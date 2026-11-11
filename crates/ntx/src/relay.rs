use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::sync::{mpsc as tmpsc, oneshot, watch};
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::protocol::{Endpoint, ErrorCode, Hello, Message, Publish, Tally};
use crate::source::Mdns;
use crate::wire::{self, Ended, Peer, lock};
use crate::{DEFAULT_PORT, Error, PROGRAM, PROTOCOL_VERSION, Result, Source, SourceConfig, display_name};

/// Most publishers a relay serves at once.
const MAX_PUBLISHERS: usize = 32;

/// Most publishers a relay serves at once from one web origin, or from programs.
const MAX_PUBLISHERS_PER_ORIGIN: usize = 8;

/// Most source ids whose port the relay remembers (clause 14.4).
const MAX_REMEMBERED_PORTS: usize = 256;

/// Settings for a [`Relay`].
#[derive(Debug, Clone)]
pub struct RelayConfig {
    /// Human-readable name sent to publishers, e.g. `CHURCH-PC (Cueword)`.
    pub name: String,
    /// Stable id. Defaults to a new random UUID.
    pub id: String,
    /// Address to listen on for publishers. Defaults to `127.0.0.1`: publishers are normally on
    /// the same computer (clause 14.1). Programs on other computers are refused either way
    /// (clause 14.5). The published sources always listen on all interfaces.
    pub bind: IpAddr,
    /// TCP port. `0` picks any free port. Defaults to 7686.
    pub port: u16,
    /// WebSocket path. Defaults to `/publish`.
    pub path: String,
    /// Require this pairing code from publishers.
    pub pairing_code: Option<String>,
    /// Web page origins allowed to publish, e.g. `https://example.com` (clause 14.5). Programs
    /// that send no `Origin` header are allowed from this computer only. The origin `null` is
    /// never allowed.
    pub allowed_origins: Vec<String>,
    /// Advertise the published sources with mDNS. Defaults to `true`.
    pub advertise: bool,
}

impl RelayConfig {
    /// Default settings with the given display name.
    pub fn new(name: impl Into<String>) -> Self {
        RelayConfig {
            name: name.into(),
            id: uuid::Uuid::new_v4().to_string(),
            bind: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: DEFAULT_PORT,
            path: "/publish".to_string(),
            pairing_code: None,
            allowed_origins: Vec::new(),
            advertise: true,
        }
    }
}

/// Something that happened to a [`Relay`].
///
/// Serialises as `{"event": "origin_requested", "data": {...}}`.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(tag = "event", content = "data", rename_all = "snake_case")]
pub enum RelayEvent {
    /// A web page from an origin that is not allowed tried to publish and was refused. Ask the
    /// user, and call [`Relay::allow_origin`] if they agree; the page's next connection then
    /// succeeds (clause 14.5). Reported once per origin, and never for the origin `null`.
    OriginRequested {
        /// The page's origin, e.g. `https://example.com`.
        origin: String,
    },
    /// A publisher finished the handshake. Its source is now served to the network.
    PublisherConnected {
        /// Number that identifies this connection in later events.
        connection: u64,
        /// The published source, as served: the name has the machine name added (clause 14.4).
        source: Endpoint,
        /// The page's origin, or `None` for a native publisher.
        origin: Option<String>,
        /// The publisher's network address.
        address: SocketAddr,
        /// The TCP port the published source listens on.
        port: u16,
    },
    /// A publisher went away, and its source with it: the source's receivers are disconnected
    /// and its advertisement withdrawn (clause 14.4).
    PublisherDisconnected {
        /// The number from [`RelayEvent::PublisherConnected`].
        connection: u64,
    },
}

/// An NTX relay: accepts publishers and serves each one to the network as an ordinary source
/// (clause 14). Publishing is off until a relay is started.
///
/// Events queue up until they are read, so read them regularly with
/// [`Relay::poll_event`] or [`Relay::wait_event`].
pub struct Relay {
    shared: Arc<Shared>,
    events: Mutex<mpsc::Receiver<RelayEvent>>,
    local_addr: SocketAddr,
    stop: watch::Sender<bool>,
}

struct Shared {
    identity: Endpoint,
    path: String,
    pairing_code: Option<String>,
    /// One mDNS daemon for all published sources, if they are advertised.
    mdns: Option<Arc<Mdns>>,
    /// Allowed origins, normalised. Connections watch it, so withdrawing an origin closes them.
    allowed: watch::Sender<HashSet<String>>,
    /// Origins already reported with [`RelayEvent::OriginRequested`].
    requested: Mutex<HashSet<String>>,
    state: Mutex<State>,
    events: mpsc::Sender<RelayEvent>,
}

#[derive(Default)]
struct State {
    next_connection: u64,
    /// The connection serving each source id. The newest connection wins (clause 14.4).
    served: HashMap<String, Served>,
    /// The port each source id last used, oldest first, so receivers find the source again after
    /// the publisher reconnects (clause 14.4).
    ports: VecDeque<(String, u16)>,
}

struct Served {
    connection: u64,
    origin: Option<String>,
    /// Dropped when a newer connection replaces this one.
    _replaced: oneshot::Sender<()>,
}

impl Relay {
    /// Starts listening for publishers.
    ///
    /// # Errors
    ///
    /// Fails if an allowed origin is invalid, the port cannot be opened, or mDNS cannot start.
    pub fn start(config: RelayConfig) -> Result<Relay> {
        let allowed = config
            .allowed_origins
            .iter()
            .map(|o| normalize_origin(o))
            .collect::<Result<HashSet<_>>>()?;
        let (listener, local_addr) = wire::listen(config.bind, config.port)?;
        let mdns = config.advertise.then(Mdns::new).transpose()?.map(Arc::new);
        let (events_tx, events_rx) = mpsc::channel();
        let shared = Arc::new(Shared {
            identity: Endpoint {
                id: config.id,
                name: config.name,
            },
            path: config.path,
            pairing_code: config.pairing_code,
            mdns,
            allowed: watch::channel(allowed).0,
            requested: Mutex::default(),
            state: Mutex::default(),
            events: events_tx,
        });
        let stop = wire::serve_connections(listener, shared.clone(), serve);
        Ok(Relay {
            shared,
            events: Mutex::new(events_rx),
            local_addr,
            stop,
        })
    }

    /// Allows web pages from `origin`, e.g. `https://example.com`, to publish (clause 14.5).
    ///
    /// # Errors
    ///
    /// Fails if `origin` is not of the form `scheme://host[:port]`.
    pub fn allow_origin(&self, origin: &str) -> Result<()> {
        let origin = normalize_origin(origin)?;
        self.shared.allowed.send_modify(|allowed| {
            allowed.insert(origin);
        });
        Ok(())
    }

    /// Withdraws permission from `origin`. Its publishers receive `not_allowed` and are
    /// disconnected (clause 14.5).
    ///
    /// # Errors
    ///
    /// Fails if `origin` is not of the form `scheme://host[:port]`.
    pub fn revoke_origin(&self, origin: &str) -> Result<()> {
        let origin = normalize_origin(origin)?;
        self.shared.allowed.send_modify(|allowed| {
            allowed.remove(&origin);
        });
        Ok(())
    }

    /// Takes the next event, if any.
    pub fn poll_event(&self) -> Option<RelayEvent> {
        lock(&self.events).try_recv().ok()
    }

    /// Waits up to `timeout` for the next event.
    pub fn wait_event(&self, timeout: Duration) -> Option<RelayEvent> {
        lock(&self.events).recv_timeout(timeout).ok()
    }

    /// The address the relay listens on.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

/// Whether a connection may publish, decided before `hello` (clause 14.5).
#[derive(Debug, PartialEq)]
enum Admission {
    /// Allowed, from this web origin (normalised) or from a program (`None`).
    Allowed(Option<String>),
    /// Refused; ask the user about this origin.
    Ask(String),
    /// Refused for this reason.
    Refused(String),
}

fn admit(origin: Option<&str>, address: IpAddr, allowed: &HashSet<String>) -> Admission {
    match origin {
        None if address.to_canonical().is_loopback() => Admission::Allowed(None),
        None => Admission::Refused(format!(
            "programs on other computers ({address}) may not publish here"
        )),
        Some("null") => Admission::Refused("pages with the origin null may not publish".into()),
        Some(origin) => match normalize_origin(origin) {
            Ok(origin) if allowed.contains(&origin) => Admission::Allowed(Some(origin)),
            Ok(origin) => Admission::Ask(origin),
            Err(e) => Admission::Refused(e.to_string()),
        },
    }
}

/// Normalises a web origin, `scheme://host[:port]`, for comparison: lowercase, without a
/// trailing slash or the scheme's default port.
fn normalize_origin(origin: &str) -> Result<String> {
    let invalid = || Error::InvalidOrigin(origin.to_string());
    let (scheme, rest) = origin.trim().split_once("://").ok_or_else(invalid)?;
    let rest = rest.strip_suffix('/').unwrap_or(rest);
    let scheme_ok = scheme.starts_with(|c: char| c.is_ascii_alphabetic())
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "+-.".contains(c));
    if !scheme_ok || rest.contains(['/', '?', '#', '@']) || rest.contains(char::is_whitespace) {
        return Err(invalid());
    }
    let (scheme, rest) = (scheme.to_ascii_lowercase(), rest.to_ascii_lowercase());
    let (host, port) = match rest.rsplit_once(':') {
        Some((host, port)) if !port.contains(']') => {
            (host, Some(port.parse::<u16>().map_err(|_| invalid())?))
        }
        _ => (rest.as_str(), None),
    };
    if host.is_empty() {
        return Err(invalid());
    }
    let default_port = matches!(
        (scheme.as_str(), port),
        ("http" | "ws", Some(80)) | ("https" | "wss", Some(443))
    );
    Ok(match port {
        Some(port) if !default_port => format!("{scheme}://{host}:{port}"),
        _ => format!("{scheme}://{host}"),
    })
}

/// Refuses a publisher with `not_allowed` (clause 14.5).
async fn refuse<S>(peer: &mut Peer<S>, reason: String)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let Ended(reason) = peer.reject(ErrorCode::NotAllowed, reason).await;
    tracing::debug!("refused a publisher: {reason}");
}

/// Serves one publisher from WebSocket upgrade to disconnect.
async fn serve(stream: TcpStream, address: SocketAddr, shared: Arc<Shared>, mut stop: watch::Receiver<bool>) {
    let (mut peer, origin) = match wire::accept(stream, &shared.path, "publish").await {
        Ok(accepted) => accepted,
        Err(Ended(e)) => return tracing::debug!("upgrade from {address} failed: {e}"),
    };
    let admission = admit(origin.as_deref(), address.ip(), &shared.allowed.borrow());
    let origin = match admission {
        Admission::Allowed(origin) => origin,
        Admission::Ask(origin) => {
            if lock(&shared.requested).insert(origin.clone()) {
                let _ = shared.events.send(RelayEvent::OriginRequested {
                    origin: origin.clone(),
                });
            }
            return refuse(&mut peer, format!("origin {origin} is not allowed to publish")).await;
        }
        Admission::Refused(reason) => return refuse(&mut peer, reason).await,
    };
    let hello = Hello {
        ntx: PROTOCOL_VERSION.to_string(),
        source: shared.identity.clone(),
        // Does not limit what the publisher may publish (clause 14.2).
        channels: vec![PROGRAM.to_string()],
        session: uuid::Uuid::new_v4().simple().to_string(),
        pairing: None,
    };
    let publish = wire::greet(
        &mut peer,
        hello,
        shared.pairing_code.as_deref(),
        |message| match message {
            Message::Publish(p) => Some((p.clone(), p.ntx, p.pairing)),
            _ => None,
        },
    );
    let publish = match publish.await {
        Ok(publish) => publish,
        Err(Ended(reason)) => return tracing::debug!("handshake with {address} failed: {reason}"),
    };
    let id = publish.source.id.clone();
    let (connection, replaced) = match register(&shared, &id, origin.as_deref()) {
        Ok(registered) => registered,
        Err(reason) => return refuse(&mut peer, reason).await,
    };
    let mut source = match start_source(&shared, publish).await {
        Ok(source) => source,
        Err(e) => {
            unregister(&shared, &id, connection);
            peer.close(Some((CLOSE_INTERNAL_ERROR, &e.to_string()))).await;
            return tracing::warn!("could not serve the publisher at {address}: {e}");
        }
    };
    source.discard_events(); // the relay watches tally directly
    let _ = shared.events.send(RelayEvent::PublisherConnected {
        connection,
        source: source.identity().clone(),
        origin: origin.clone(),
        address,
        port: source.local_addr().port(),
    });

    let (out, mut out_rx) = tmpsc::unbounded_channel();
    tokio::spawn(forward_tally(source.tally(), out.clone()));
    let mut allowed = shared.allowed.subscribe();
    let stopped = async move {
        let withdrawn = allowed.wait_for(|allowed| origin.as_ref().is_some_and(|o| !allowed.contains(o)));
        tokio::select! {
            _ = stop.changed() => None,
            _ = replaced => Some((ErrorCode::NotAllowed, "a newer connection publishes this source".into())),
            _ = withdrawn => Some((ErrorCode::NotAllowed, "permission for this origin was withdrawn".into())),
        }
    };
    let ended = wire::pump(&mut peer, &mut out_rx, stopped, |message| {
        // Forward the publisher's text to the receivers (clause 14.3).
        let forwarded = match message {
            Message::Announce(a) => source.announce(a.types),
            Message::Update(u) => source.update(&u.channel, &u.type_name, u.fields).map(drop),
            Message::Error(e) => {
                tracing::info!("publisher reported {}", e.code.as_str());
                Ok(())
            }
            _ => Ok(()),
        };
        if let Err(e) = forwarded {
            let _ = out.send(WsMessage::text(Message::Error(wire::bad_message(&e)).to_json()));
        }
    })
    .await;
    tracing::debug!("publisher {address} left: {}", ended.0);
    // Closes the receivers' connections and withdraws the advertisement (clause 14.4).
    blocking(move || drop(source)).await;
    unregister(&shared, &id, connection);
    let _ = shared
        .events
        .send(RelayEvent::PublisherDisconnected { connection });
}

/// WebSocket close code for a failure on this side (RFC 6455, section 7.4.1).
const CLOSE_INTERNAL_ERROR: u16 = 1011;

/// Records a new connection serving source `id`, within the limits. An older connection for the
/// same id is replaced (clause 14.4): the returned receiver of that one completes.
fn register(
    shared: &Shared,
    id: &str,
    origin: Option<&str>,
) -> std::result::Result<(u64, oneshot::Receiver<()>), String> {
    let mut state = lock(&shared.state);
    let others: Vec<&Served> = state
        .served
        .iter()
        .filter(|(served, _)| *served != id)
        .map(|(_, s)| s)
        .collect();
    if others.len() >= MAX_PUBLISHERS {
        return Err("this relay serves too many publishers".into());
    }
    if others.iter().filter(|s| s.origin.as_deref() == origin).count() >= MAX_PUBLISHERS_PER_ORIGIN {
        return Err("too many publishers from this origin".into());
    }
    state.next_connection += 1;
    let connection = state.next_connection;
    let (replaced_tx, replaced) = oneshot::channel();
    let served = Served {
        connection,
        origin: origin.map(String::from),
        _replaced: replaced_tx,
    };
    state.served.insert(id.to_string(), served);
    Ok((connection, replaced))
}

/// Forgets `connection`, unless a newer connection already replaced it.
fn unregister(shared: &Shared, id: &str, connection: u64) {
    let mut state = lock(&shared.state);
    if state.served.get(id).is_some_and(|s| s.connection == connection) {
        state.served.remove(id);
    }
}

/// Runs `f` on a thread for blocking work: starting and stopping a source waits for mDNS.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(f)
        .await
        .unwrap_or_else(|e| std::panic::resume_unwind(e.into_panic()))
}

/// Starts the source that serves a publisher (clause 14.4), at the address it had last time if
/// that port becomes free soon, or else on any free port.
async fn start_source(shared: &Shared, publish: Publish) -> Result<Source> {
    let mut config = SourceConfig::new(display_name(&publish.source.name));
    config.id = publish.source.id;
    config.channels = publish.channels.unwrap_or(config.channels);
    config.advertise = shared.mdns.is_some();
    let start = |config: SourceConfig| {
        let mdns = shared.mdns.clone();
        blocking(move || Source::start_with(config, mdns))
    };
    let last_port = lock(&shared.state)
        .ports
        .iter()
        .find(|(id, _)| *id == config.id)
        .map(|&(_, port)| port);
    let mut source = None;
    if let Some(port) = last_port {
        config.port = port;
        // The previous source of this id may still be closing that port.
        for _ in 0..20 {
            source = start(config.clone()).await.ok();
            if source.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    let source = match source {
        Some(source) => source,
        None => start(SourceConfig { port: 0, ..config }).await?,
    };
    let mut state = lock(&shared.state);
    let id = &source.identity().id;
    state.ports.retain(|(remembered, _)| remembered != id);
    state.ports.push_back((id.clone(), source.local_addr().port()));
    if state.ports.len() > MAX_REMEMBERED_PORTS {
        state.ports.pop_front();
    }
    drop(state);
    Ok(source)
}

/// Sends the combined tally to the publisher whenever it changes (clause 14.3).
async fn forward_tally(mut tally: watch::Receiver<Tally>, out: tmpsc::UnboundedSender<WsMessage>) {
    while tally.changed().await.is_ok() {
        let message = Message::Tally(*tally.borrow_and_update());
        if out.send(WsMessage::text(message.to_json())).is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origins_are_normalised() {
        for (given, normal) in [
            ("https://Example.COM/", "https://example.com"),
            ("http://example.com:80", "http://example.com"),
            ("https://example.com:443", "https://example.com"),
            ("https://example.com:8443", "https://example.com:8443"),
            ("http://[::1]:3000", "http://[::1]:3000"),
            ("http://[::1]", "http://[::1]"),
            ("chrome-extension://abcdef", "chrome-extension://abcdef"),
        ] {
            assert_eq!(normalize_origin(given).unwrap(), normal, "{given}");
        }
        for bad in [
            "null",
            "example.com",
            "https://",
            "https://a/b",
            "https://a:x",
            "1http://a",
            "https://a b",
        ] {
            assert!(
                matches!(normalize_origin(bad), Err(Error::InvalidOrigin(_))),
                "{bad}"
            );
        }
    }

    #[test]
    fn who_may_publish() {
        let allowed: HashSet<String> = ["https://ok.example".to_string()].into();
        let local = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let mapped: IpAddr = "::ffff:127.0.0.1".parse().unwrap();
        let remote: IpAddr = "192.168.1.20".parse().unwrap();
        assert_eq!(admit(None, local, &allowed), Admission::Allowed(None));
        assert_eq!(admit(None, mapped, &allowed), Admission::Allowed(None));
        assert!(matches!(admit(None, remote, &allowed), Admission::Refused(_)));
        assert!(matches!(
            admit(Some("null"), local, &allowed),
            Admission::Refused(_)
        ));
        assert!(matches!(
            admit(Some("nonsense"), local, &allowed),
            Admission::Refused(_)
        ));
        assert_eq!(
            admit(Some("https://OK.example:443"), remote, &allowed),
            Admission::Allowed(Some("https://ok.example".into()))
        );
        assert_eq!(
            admit(Some("https://new.example"), local, &allowed),
            Admission::Ask("https://new.example".into())
        );
    }
}
