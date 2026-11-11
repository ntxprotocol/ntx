use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use mdns_sd::{ServiceDaemon, ServiceInfo};
use tokio::sync::{mpsc as tmpsc, watch};
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::protocol::{Endpoint, Fields, Hello, Message, Subscribe, Tally, TypeDescriptor};
use crate::store::{Store, text_shortcuts};
use crate::wire::{self, Ended, Incoming, lock};
use crate::{DEFAULT_PORT, Error, PROGRAM, PROTOCOL_VERSION, Result, SERVICE_TYPE};

/// Settings for a [`Source`].
#[derive(Debug, Clone)]
pub struct SourceConfig {
    /// Human-readable name, e.g. `CHURCH-PC (Lyrics)`.
    pub name: String,
    /// Stable id. Save it and pass it again next time, so receivers find the source after a
    /// restart. Defaults to a new random UUID.
    pub id: String,
    /// Channels this source has. Must include `program`.
    pub channels: Vec<String>,
    /// Address to listen on. Defaults to all IPv4 interfaces.
    pub bind: IpAddr,
    /// TCP port. `0` picks any free port. Defaults to 7686.
    pub port: u16,
    /// WebSocket path. Defaults to `/`.
    pub path: String,
    /// Require this pairing code from receivers.
    pub pairing_code: Option<String>,
    /// Advertise with mDNS. Defaults to `true`.
    pub advertise: bool,
}

impl SourceConfig {
    /// Default settings with the given display name.
    pub fn new(name: impl Into<String>) -> Self {
        SourceConfig {
            name: name.into(),
            id: uuid::Uuid::new_v4().to_string(),
            channels: vec![PROGRAM.to_string()],
            bind: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port: DEFAULT_PORT,
            path: "/".to_string(),
            pairing_code: None,
            advertise: true,
        }
    }
}

/// Something that happened to a [`Source`].
///
/// Serialises as `{"event": "receiver_connected", "data": {...}}`.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(tag = "event", content = "data", rename_all = "snake_case")]
pub enum SourceEvent {
    /// A receiver finished the handshake.
    ReceiverConnected {
        /// Number that identifies this connection in later events.
        connection: u64,
        /// The receiver's identity from its `subscribe`.
        receiver: Endpoint,
        /// The receiver's network address.
        address: SocketAddr,
    },
    /// A receiver went away.
    ReceiverDisconnected {
        /// The number from [`SourceEvent::ReceiverConnected`].
        connection: u64,
    },
    /// The combined tally of all receivers changed (clause 10.3).
    TallyChanged(Tally),
}

/// An NTX source: holds the current text and serves it to receivers (clauses 8–11).
pub struct Source {
    shared: Arc<Shared>,
    events: Mutex<mpsc::Receiver<SourceEvent>>,
    local_addr: SocketAddr,
    stop: watch::Sender<bool>,
    /// The mDNS daemon and the registered service name.
    mdns: Option<(Arc<Mdns>, String)>,
}

struct Shared {
    identity: Endpoint,
    path: String,
    pairing_code: Option<String>,
    session: String,
    state: Mutex<State>,
    events: mpsc::Sender<SourceEvent>,
    /// The combined tally of all receivers (clause 10.3).
    tally: watch::Sender<Tally>,
}

struct State {
    store: Store,
    connections: HashMap<u64, Connection>,
    next_connection: u64,
}

struct Connection {
    channels: Vec<String>,
    tally: Tally,
    /// `None` once the receiver fell too far behind; its task then ends the connection.
    out: Option<tmpsc::Sender<WsMessage>>,
}

impl Source {
    /// Starts listening and, unless turned off, advertises the source with mDNS.
    ///
    /// # Errors
    ///
    /// Fails if the channels lack `program`, the port cannot be opened, or mDNS cannot start.
    pub fn start(config: SourceConfig) -> Result<Source> {
        Source::start_with(config, None)
    }

    /// Like [`Source::start`], advertising with `mdns` when given instead of a daemon of its own.
    pub(crate) fn start_with(config: SourceConfig, mdns: Option<Arc<Mdns>>) -> Result<Source> {
        let store = Store::new(config.channels.clone())?;
        let (listener, local_addr) = wire::listen(config.bind, config.port)?;
        let mdns = if config.advertise {
            let daemon = match mdns {
                Some(daemon) => daemon,
                None => Arc::new(Mdns::new()?),
            };
            let fullname = advertise(&daemon.0, &config, local_addr.port())?;
            Some((daemon, fullname))
        } else {
            None
        };

        let (events_tx, events_rx) = mpsc::channel();
        let shared = Arc::new(Shared {
            identity: Endpoint {
                id: config.id,
                name: config.name,
            },
            path: config.path,
            pairing_code: config.pairing_code,
            session: uuid::Uuid::new_v4().simple().to_string(),
            state: Mutex::new(State {
                store,
                connections: HashMap::new(),
                next_connection: 0,
            }),
            events: events_tx,
            tally: watch::channel(Tally::default()).0,
        });
        let stop = wire::serve_connections(listener, shared.clone(), serve);

        Ok(Source {
            shared,
            events: Mutex::new(events_rx),
            local_addr,
            stop,
            mdns,
        })
    }

    /// Replaces the item of a channel and sends it to every subscribed receiver (clause 9.3).
    /// Strings in `fields` are normalised (clause 17). Returns the sequence number.
    ///
    /// # Errors
    ///
    /// Fails for an unknown channel, an invalid type name, or a message over the size limit.
    pub fn update(&self, channel: &str, type_name: &str, fields: Fields) -> Result<u64> {
        let mut state = lock(&self.shared.state);
        let (seq, frame) = state.store.update(channel, type_name, fields)?;
        for conn in state
            .connections
            .values_mut()
            .filter(|c| c.channels.iter().any(|c| c == channel))
        {
            wire::enqueue(&mut conn.out, frame.clone());
        }
        Ok(seq)
    }

    text_shortcuts!();

    /// Sets the announced types and sends them to every receiver (clause 15.5).
    ///
    /// # Errors
    ///
    /// Fails for an invalid type descriptor or a message over the size limit.
    pub fn announce(&self, types: Vec<TypeDescriptor>) -> Result<()> {
        let mut state = lock(&self.shared.state);
        let frame = state.store.announce(types)?;
        for conn in state.connections.values_mut() {
            wire::enqueue(&mut conn.out, frame.clone());
        }
        Ok(())
    }

    /// Takes the next event, if any.
    pub fn poll_event(&self) -> Option<SourceEvent> {
        lock(&self.events).try_recv().ok()
    }

    /// Waits up to `timeout` for the next event.
    pub fn wait_event(&self, timeout: Duration) -> Option<SourceEvent> {
        lock(&self.events).recv_timeout(timeout).ok()
    }

    /// Number of connected receivers.
    #[must_use]
    pub fn receiver_count(&self) -> usize {
        lock(&self.shared.state).connections.len()
    }

    /// The address the source listens on.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The source's id and name.
    #[must_use]
    pub fn identity(&self) -> &Endpoint {
        &self.shared.identity
    }

    /// Watches the combined tally (for relays, which pass it on to the publisher).
    pub(crate) fn tally(&self) -> watch::Receiver<Tally> {
        self.shared.tally.subscribe()
    }

    /// Stops queueing events that nobody will read.
    pub(crate) fn discard_events(&mut self) {
        self.events = Mutex::new(mpsc::channel().1);
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Some((daemon, fullname)) = self.mdns.take() {
            if let Ok(done) = daemon.0.unregister(&fullname) {
                let _ = done.recv_timeout(Duration::from_secs(1));
            }
        }
    }
}

/// `MACHINE (name)`: the form of display name the spec recommends (clause 8.2).
#[must_use]
pub fn display_name(name: &str) -> String {
    let host = gethostname::gethostname().to_string_lossy().into_owned();
    let machine = host.split('.').next().unwrap_or(&host).to_uppercase();
    format!("{machine} ({name})")
}

/// An mDNS daemon, shut down when its last user is dropped. A relay shares one between the
/// sources it serves.
pub(crate) struct Mdns(ServiceDaemon);

impl Mdns {
    pub(crate) fn new() -> Result<Mdns> {
        Ok(Mdns(ServiceDaemon::new().map_err(mdns_error)?))
    }
}

impl Drop for Mdns {
    fn drop(&mut self) {
        let _ = self.0.shutdown();
    }
}

#[allow(clippy::needless_pass_by_value)] // used with map_err
fn mdns_error(e: mdns_sd::Error) -> Error {
    Error::Mdns(e.to_string())
}

/// Registers the source with DNS-SD (clause 6). Returns the registered service name.
fn advertise(daemon: &ServiceDaemon, config: &SourceConfig, port: u16) -> Result<String> {
    let mdns = mdns_error;
    let host = gethostname::gethostname()
        .to_string_lossy()
        .trim_end_matches(".local")
        .to_string();
    let mut txt = vec![
        ("txtvers", "1"),
        ("ntx", PROTOCOL_VERSION),
        ("id", config.id.as_str()),
    ];
    if config.path != "/" {
        txt.push(("path", config.path.as_str()));
    }
    if config.pairing_code.is_some() {
        txt.push(("pair", "1"));
    }
    let info = ServiceInfo::new(
        SERVICE_TYPE,
        dns_label(&config.name),
        &format!("{}.local.", dns_label(&host)),
        "",
        port,
        &txt[..],
    )
    .map_err(mdns)?
    .enable_addr_auto();
    let fullname = info.get_fullname().to_string();
    daemon.register(info).map_err(mdns)?;
    Ok(fullname)
}

/// Shortens a name to the 63-byte limit of a DNS label (clause 6.2), keeping whole characters.
/// Longer names make mDNS answers empty, so nobody would find the source.
fn dns_label(name: &str) -> &str {
    wire::cut(name, 63)
}

/// Serves one receiver from WebSocket upgrade to disconnect.
async fn serve(incoming: Incoming, shared: Arc<Shared>, mut stop: watch::Receiver<bool>) {
    let Incoming {
        stream,
        address,
        slot,
    } = incoming;
    let mut peer = match wire::accept(stream, &shared.path, "subscribe").await {
        Ok((peer, _)) => peer,
        Err(Ended(e)) => return tracing::debug!("upgrade from {address} failed: {e}"),
    };
    let hello = Hello {
        ntx: PROTOCOL_VERSION.to_string(),
        source: shared.identity.clone(),
        channels: lock(&shared.state).store.channels().to_vec(),
        session: shared.session.clone(),
        pairing: None,
    };
    let subscribe = wire::greet(
        &mut peer,
        &slot,
        hello,
        shared.pairing_code.as_deref(),
        |message| match message {
            Message::Subscribe(s) => Some((s.clone(), s.ntx, s.pairing)),
            _ => None,
        },
    );
    let subscribe = match subscribe.await {
        Ok(subscribe) => subscribe,
        Err(Ended(reason)) => return tracing::debug!("handshake with {address} failed: {reason}"),
    };

    let (connection, mut out) = register(&shared, &subscribe);
    let _ = shared.events.send(SourceEvent::ReceiverConnected {
        connection,
        receiver: subscribe.receiver,
        address,
    });
    let stopped = async move {
        let _ = stop.changed().await;
        None
    };
    let ended = wire::pump(&mut peer, &mut out, stopped, |message| match message {
        Message::Tally(tally) => set_tally(&shared, connection, tally),
        Message::Error(e) => tracing::info!("receiver reported {}", e.code.as_str()),
        _ => {}
    })
    .await;
    tracing::debug!("receiver {address} left: {}", ended.0);

    if lock(&shared.state).connections.remove(&connection).is_some() {
        recompute_tally(&shared);
        let _ = shared
            .events
            .send(SourceEvent::ReceiverDisconnected { connection });
    }
}

/// Adds the connection and queues the announcement and full state (clause 9.2) under one lock,
/// so no update is lost or sent twice.
fn register(shared: &Shared, subscribe: &Subscribe) -> (u64, tmpsc::Receiver<WsMessage>) {
    let (out, out_rx) = wire::queue();
    let mut out = Some(out);
    let mut state = lock(&shared.state);
    let channels: Vec<String> = subscribe
        .channels
        .clone()
        .unwrap_or_else(|| vec![PROGRAM.to_string()])
        .into_iter()
        .filter(|c| state.store.channels().contains(c))
        .collect();
    for frame in state.store.state(&channels) {
        wire::enqueue(&mut out, frame);
    }
    state.next_connection += 1;
    let connection = state.next_connection;
    state.connections.insert(
        connection,
        Connection {
            channels,
            tally: Tally::default(),
            out,
        },
    );
    (connection, out_rx)
}

fn set_tally(shared: &Shared, connection: u64, tally: Tally) {
    if let Some(conn) = lock(&shared.state).connections.get_mut(&connection) {
        conn.tally = tally;
    }
    recompute_tally(shared);
}

fn recompute_tally(shared: &Shared) {
    let state = lock(&shared.state);
    let combined = Tally {
        program: state.connections.values().any(|c| c.tally.program),
        preview: state.connections.values().any(|c| c.tally.preview),
    };
    if shared
        .tally
        .send_if_modified(|tally| std::mem::replace(tally, combined) != combined)
    {
        let _ = shared.events.send(SourceEvent::TallyChanged(combined));
    }
}

#[cfg(test)]
mod tests {
    use super::dns_label;

    #[test]
    fn dns_labels_fit_in_63_bytes() {
        assert_eq!(dns_label("CHURCH-PC (Lyrics)"), "CHURCH-PC (Lyrics)");
        let long = format!("{} (probe)", "A".repeat(62));
        assert_eq!(dns_label(&long), &long[..63]);
        // "Ọ" is 3 bytes: never cut a character in half.
        let accented = "Ọ".repeat(30);
        assert_eq!(dns_label(&accented), "Ọ".repeat(21));
    }
}
