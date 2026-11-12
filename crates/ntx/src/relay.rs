use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc as tmpsc, oneshot, watch};
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::origin::{self, normalize_origin};
use crate::protocol::{Endpoint, ErrorCode, Hello, Message, Publish, Tally, check_endpoint};
use crate::source::Mdns;
use crate::wire::{self, Ended, Incoming, lock};
use crate::{DEFAULT_PORT, Error, PROGRAM, PROTOCOL_VERSION, Result, Source, SourceConfig, display_name};

/// Most publishers a relay serves at once.
const MAX_PUBLISHERS: usize = 32;

/// Most publishers a relay serves at once from one web origin, or from programs.
const MAX_PUBLISHERS_PER_ORIGIN: usize = 8;

/// Most source ids whose port the relay remembers (clause 14.4).
const MAX_REMEMBERED_PORTS: usize = 256;

/// Most origins the app is asked about and has not answered yet (clause 14.5).
const MAX_PENDING_REQUESTS: usize = 8;

/// Least time between two questions to the app about origins.
const REQUEST_INTERVAL: Duration = Duration::from_secs(1);

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
    _stop: wire::Stopper,
}

struct Shared {
    identity: Endpoint,
    path: String,
    pairing_code: Option<String>,
    /// One mDNS daemon for all published sources, if they are advertised.
    mdns: Option<Arc<Mdns>>,
    /// Allowed origins, normalised. Connections watch it, so withdrawing an origin closes them.
    allowed: watch::Sender<HashSet<String>>,
    requests: Mutex<Requests>,
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
    /// Fails if the id or name is empty or too long (clause 18.4), an allowed origin is invalid,
    /// `bind` is not a loopback address but there is no
    /// pairing code, the port cannot be opened, or mDNS cannot start.
    pub fn start(config: RelayConfig) -> Result<Relay> {
        // Beyond this computer, anyone can send any Origin header (clause 20.2).
        if !config.bind.to_canonical().is_loopback() && config.pairing_code.is_none() {
            return Err(Error::InvalidAddress(format!(
                "{}: a relay that listens beyond this computer needs a pairing code",
                config.bind
            )));
        }
        let identity = Endpoint {
            id: config.id,
            name: config.name,
        };
        check_endpoint(&identity)?;
        crate::pairing::check_code(config.pairing_code.as_deref())?;
        let allowed = config
            .allowed_origins
            .iter()
            .map(|o| normalize_origin(o))
            .collect::<Result<HashSet<_>>>()?;
        let (listener, local_addr) = wire::listen(config.bind, config.port)?;
        let mdns = config.advertise.then(Mdns::new).transpose()?.map(Arc::new);
        let (events_tx, events_rx) = mpsc::channel();
        let shared = Arc::new(Shared {
            identity,
            path: config.path,
            pairing_code: config.pairing_code,
            mdns,
            allowed: watch::channel(allowed).0,
            requests: Mutex::default(),
            state: Mutex::default(),
            events: events_tx,
        });
        let stop = wire::serve_connections(listener, shared.clone(), serve);
        Ok(Relay {
            shared,
            events: Mutex::new(events_rx),
            local_addr,
            _stop: stop,
        })
    }

    /// Allows web pages from `origin`, e.g. `https://example.com`, to publish (clause 14.5).
    ///
    /// # Errors
    ///
    /// Fails if `origin` is not of the form `http(s)://host[:port]`.
    pub fn allow_origin(&self, origin: &str) -> Result<()> {
        let origin = normalize_origin(origin)?;
        lock(&self.shared.requests).pending.remove(&origin);
        self.shared.allowed.send_modify(|allowed| {
            allowed.insert(origin);
        });
        Ok(())
    }

    /// Withdraws permission from `origin`. Its publishers receive `not_allowed` and are
    /// disconnected (clause 14.5). Also call it when the user says no to an
    /// [`RelayEvent::OriginRequested`], so that it no longer counts as unanswered.
    ///
    /// # Errors
    ///
    /// Fails if `origin` is not of the form `http(s)://host[:port]`.
    pub fn revoke_origin(&self, origin: &str) -> Result<()> {
        let origin = normalize_origin(origin)?;
        lock(&self.shared.requests).pending.remove(&origin);
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

/// The origins the app was asked about and has not answered, and when it was last asked.
#[derive(Default)]
struct Requests {
    pending: HashSet<String>,
    last: Option<Instant>,
}

impl Requests {
    /// Whether to ask the app about `origin` now: once per origin, with few questions waiting and
    /// not too often, so that pages cannot flood the user.
    fn ask(&mut self, origin: &str) -> bool {
        let ask = !self.pending.contains(origin)
            && self.pending.len() < MAX_PENDING_REQUESTS
            && self.last.is_none_or(|last| last.elapsed() >= REQUEST_INTERVAL);
        if ask {
            self.pending.insert(origin.to_string());
            self.last = Some(Instant::now());
        }
        ask
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

/// Serves one publisher from WebSocket upgrade to disconnect.
async fn serve(incoming: Incoming, shared: Arc<Shared>, mut stop: watch::Receiver<bool>) {
    let Incoming {
        stream,
        address,
        slot,
    } = incoming;
    let (mut peer, origin) =
        match wire::unless_stopped(&mut stop, wire::accept(stream, &shared.path, "publish")).await {
            Ok(accepted) => accepted,
            Err(Ended(e)) => return tracing::debug!("upgrade from {address} failed: {e}"),
        };
    let admission = admit(origin.as_deref(), address.ip(), &shared.allowed.borrow());
    let origin = match admission {
        Admission::Allowed(origin) => origin,
        Admission::Ask(origin) => {
            if lock(&shared.requests).ask(&origin) {
                let _ = shared.events.send(RelayEvent::OriginRequested {
                    origin: origin.clone(),
                });
            }
            return origin::refuse(&mut peer, format!("origin {origin} is not allowed to publish")).await;
        }
        Admission::Refused(reason) => return origin::refuse(&mut peer, reason).await,
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
        &slot,
        hello,
        shared.pairing_code.as_deref(),
        |message| match message {
            Message::Publish(p) => Some((p.clone(), p.ntx, p.pairing)),
            _ => None,
        },
    );
    let publish = match wire::unless_stopped(&mut stop, publish).await {
        Ok(publish) => publish,
        Err(Ended(reason)) => return tracing::debug!("handshake with {address} failed: {reason}"),
    };
    let id = publish.source.id.clone();
    let (connection, replaced) = match register(&shared, &id, origin.as_deref()) {
        Ok(registered) => registered,
        Err(reason) => return origin::refuse(&mut peer, reason).await,
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

    let (out, mut out_rx) = wire::queue();
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
            let _ = out.try_send(WsMessage::text(Message::Error(wire::bad_message(&e)).to_json()));
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
    // Only the same page or program may take over its source, e.g. after a reload (clause 14.4):
    // source ids are public, so another origin must not replace it.
    if state
        .served
        .get(id)
        .is_some_and(|s| s.origin.as_deref() != origin)
    {
        return Err(format!("source {id} is already published from another origin"));
    }
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
async fn forward_tally(mut tally: watch::Receiver<Tally>, out: tmpsc::Sender<WsMessage>) {
    while tally.changed().await.is_ok() {
        let message = Message::Tally(*tally.borrow_and_update());
        if out.send(WsMessage::text(message.to_json())).await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn questions_to_the_app_are_limited() {
        let mut requests = Requests::default();
        assert!(requests.ask("https://a.example"));
        assert!(!requests.ask("https://a.example"), "asked twice");
        assert!(!requests.ask("https://b.example"), "asked too soon");
        for i in 0..MAX_PENDING_REQUESTS {
            requests.last = None;
            requests.ask(&format!("https://{i}.example"));
        }
        requests.last = None;
        assert!(!requests.ask("https://z.example"), "too many unanswered");
        requests.pending.remove("https://a.example");
        assert!(requests.ask("https://z.example"));
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
