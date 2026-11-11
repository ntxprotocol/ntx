use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use mdns_sd::{ServiceDaemon, ServiceInfo};
use serde_json::Value;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc as tmpsc, watch};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::{HeaderValue, StatusCode};

use crate::protocol::{
    Announce, Endpoint, ErrorCode, Fields, Hello, MAX_SEQ, Message, PairingChallenge, Subscribe, Tally,
    TypeDescriptor, Update, check_type_name,
};
use crate::runtime::runtime;
use crate::wire::{self, Ended, HANDSHAKE_TIMEOUT, HEARTBEAT_INTERVAL, Peer, lock};
use crate::{
    DEFAULT_PORT, Error, MAX_MESSAGE_BYTES, PROGRAM, PROTOCOL_VERSION, Result, SERVICE_TYPE, SUBPROTOCOL,
    pairing,
};

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
    mdns: Option<(ServiceDaemon, String)>,
}

struct Shared {
    identity: Endpoint,
    channels: Vec<String>,
    path: String,
    pairing_code: Option<String>,
    session: String,
    state: Mutex<State>,
    events: mpsc::Sender<SourceEvent>,
}

#[derive(Default)]
struct State {
    seq: u64,
    /// Current item of each channel, with the sequence number of its last change.
    items: HashMap<String, (u64, String, Fields)>,
    types: Option<Vec<TypeDescriptor>>,
    connections: HashMap<u64, Connection>,
    next_connection: u64,
    tally: Tally,
}

struct Connection {
    channels: Vec<String>,
    tally: Tally,
    out: tmpsc::UnboundedSender<WsMessage>,
}

impl Source {
    /// Starts listening and, unless turned off, advertises the source with mDNS.
    ///
    /// # Errors
    ///
    /// Fails if the channels lack `program`, the port cannot be opened, or mDNS cannot start.
    pub fn start(config: SourceConfig) -> Result<Source> {
        if !config.channels.iter().any(|c| c == PROGRAM) {
            return Err(Error::UnknownChannel(PROGRAM.to_string()));
        }
        let listener = std::net::TcpListener::bind((config.bind, config.port))?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;
        let mdns = if config.advertise {
            Some(advertise(&config, local_addr.port())?)
        } else {
            None
        };

        let (events_tx, events_rx) = mpsc::channel();
        let shared = Arc::new(Shared {
            identity: Endpoint {
                id: config.id,
                name: config.name,
            },
            channels: config.channels,
            path: config.path,
            pairing_code: config.pairing_code,
            session: uuid::Uuid::new_v4().simple().to_string(),
            state: Mutex::new(State::default()),
            events: events_tx,
        });
        let (stop, stop_rx) = watch::channel(false);
        let listener = {
            let _guard = runtime().enter();
            TcpListener::from_std(listener)?
        };
        runtime().spawn(accept_loop(listener, shared.clone(), stop_rx));

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
        if !self.shared.channels.iter().any(|c| c == channel) {
            return Err(Error::UnknownChannel(channel.to_string()));
        }
        check_type_name(type_name)?;
        let mut update = Update {
            channel: channel.to_string(),
            seq: 0,
            type_name: type_name.to_string(),
            fields: wire::normalize_fields(fields),
        };

        let mut state = lock(&self.shared.state);
        if state.seq >= MAX_SEQ {
            return Err(Error::InvalidMessage(
                "sequence numbers exhausted; restart the source".into(),
            ));
        }
        update.seq = state.seq + 1;
        let frame = frame(&Message::Update(update.clone()))?;
        state.seq = update.seq;
        state
            .items
            .insert(update.channel, (update.seq, update.type_name, update.fields));
        for conn in state
            .connections
            .values()
            .filter(|c| c.channels.iter().any(|c| c == channel))
        {
            let _ = conn.out.send(frame.clone()); // a closed connection is cleaned up by its own task
        }
        Ok(update.seq)
    }

    /// Sends plain text as an `ntx.text` item.
    ///
    /// # Errors
    ///
    /// As [`Source::update`].
    pub fn update_text(&self, channel: &str, text: &str) -> Result<u64> {
        let mut fields = Fields::new();
        fields.insert("text".into(), Value::String(text.to_string()));
        self.update(channel, "ntx.text", fields)
    }

    /// Clears a channel (clause 9.5).
    ///
    /// # Errors
    ///
    /// Fails for an unknown channel.
    pub fn clear(&self, channel: &str) -> Result<u64> {
        self.update(channel, "ntx.text", Fields::new())
    }

    /// Sets the announced types and sends them to every receiver (clause 15.5).
    ///
    /// # Errors
    ///
    /// Fails for an invalid type descriptor or a message over the size limit.
    pub fn announce(&self, types: Vec<TypeDescriptor>) -> Result<()> {
        for t in &types {
            t.validate()?;
        }
        let frame = frame(&Message::Announce(Announce { types: types.clone() }))?;
        let mut state = lock(&self.shared.state);
        state.types = Some(types);
        for conn in state.connections.values() {
            let _ = conn.out.send(frame.clone());
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
}

impl Drop for Source {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Some((daemon, fullname)) = self.mdns.take() {
            if let Ok(done) = daemon.unregister(&fullname) {
                let _ = done.recv_timeout(Duration::from_secs(1));
            }
            let _ = daemon.shutdown();
        }
    }
}

/// Serialises a message, enforcing the size limit (clause 18.1).
fn frame(message: &Message) -> Result<WsMessage> {
    let json = message.to_json();
    if json.len() > MAX_MESSAGE_BYTES {
        return Err(Error::TooLarge(json.len()));
    }
    Ok(WsMessage::text(json))
}

/// Advertises the source with DNS-SD (clause 6).
fn advertise(config: &SourceConfig, port: u16) -> Result<(ServiceDaemon, String)> {
    let mdns = |e: mdns_sd::Error| Error::Mdns(e.to_string());
    let daemon = ServiceDaemon::new().map_err(mdns)?;
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
    Ok((daemon, fullname))
}

/// Shortens a name to the 63-byte limit of a DNS label (clause 6.2), keeping whole characters.
/// Longer names make mDNS answers empty, so nobody would find the source.
fn dns_label(name: &str) -> &str {
    const MAX: usize = 63;
    let end = (0..=MAX.min(name.len()))
        .rev()
        .find(|&i| name.is_char_boundary(i))
        .unwrap_or(0);
    &name[..end]
}

async fn accept_loop(listener: TcpListener, shared: Arc<Shared>, mut stop: watch::Receiver<bool>) {
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, address)) => {
                    let _ = stream.set_nodelay(true);
                    tokio::spawn(serve(stream, address, shared.clone(), stop.clone()));
                }
                Err(e) => {
                    tracing::warn!("accept failed: {e}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
            _ = stop.changed() => break,
        }
    }
}

/// Serves one receiver from WebSocket upgrade to disconnect.
#[allow(clippy::result_large_err)] // the upgrade callback's error type comes from tungstenite
async fn serve(stream: TcpStream, address: SocketAddr, shared: Arc<Shared>, mut stop: watch::Receiver<bool>) {
    let path = shared.path.clone();
    let upgrade = tokio_tungstenite::accept_hdr_async_with_config(
        stream,
        move |request: &Request, response: Response| check_upgrade(&path, request, response),
        Some(wire::ws_config()),
    );
    let mut peer = match upgrade.await {
        Ok(ws) => Peer::new(ws, "subscribe"),
        Err(e) => return tracing::debug!("upgrade from {address} failed: {e}"),
    };
    let subscribe = match tokio::time::timeout(HANDSHAKE_TIMEOUT, handshake(&shared, &mut peer)).await {
        Ok(Ok(subscribe)) => subscribe,
        Ok(Err(Ended(reason))) => return tracing::debug!("handshake with {address} failed: {reason}"),
        Err(_) => return tracing::debug!("{address} sent no subscribe"),
    };
    peer.handshake_done();

    let (connection, mut out) = register(&shared, &subscribe);
    let _ = shared.events.send(SourceEvent::ReceiverConnected {
        connection,
        receiver: subscribe.receiver,
        address,
    });
    let ended = run(&shared, connection, &mut peer, &mut out, &mut stop).await;
    tracing::debug!("receiver {address} left: {}", ended.0);

    if lock(&shared.state).connections.remove(&connection).is_some() {
        recompute_tally(&shared);
        let _ = shared
            .events
            .send(SourceEvent::ReceiverDisconnected { connection });
    }
}

/// Accepts the upgrade only on the source's path, and confirms the `ntx` subprotocol if offered.
#[allow(clippy::result_large_err, clippy::unnecessary_wraps)] // signature required by tungstenite
fn check_upgrade(
    path: &str,
    request: &Request,
    mut response: Response,
) -> std::result::Result<Response, ErrorResponse> {
    if request.uri().path() != path {
        let mut not_found = ErrorResponse::new(Some("no NTX source at this path".into()));
        *not_found.status_mut() = StatusCode::NOT_FOUND;
        return Err(not_found);
    }
    let offered = request
        .headers()
        .get_all("sec-websocket-protocol")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|p| p.trim() == SUBPROTOCOL);
    if offered {
        response
            .headers_mut()
            .insert("sec-websocket-protocol", HeaderValue::from_static(SUBPROTOCOL));
    }
    Ok(response)
}

/// Sends `hello` and waits for a valid `subscribe` (clause 8).
async fn handshake(shared: &Shared, peer: &mut Peer<TcpStream>) -> std::result::Result<Subscribe, Ended> {
    let nonce = shared.pairing_code.as_ref().map(|_| pairing::new_nonce());
    peer.send(&Message::Hello(Hello {
        ntx: PROTOCOL_VERSION.to_string(),
        source: shared.identity.clone(),
        channels: shared.channels.clone(),
        session: shared.session.clone(),
        pairing: nonce.clone().map(|nonce| PairingChallenge { nonce }),
    }))
    .await?;
    let subscribe = loop {
        if let Message::Subscribe(s) = peer.recv().await? {
            break s;
        }
    };
    if let Err((code, reason)) = check_subscribe(shared, &subscribe, nonce.as_deref()).await {
        return Err(peer.reject(code, reason).await);
    }
    Ok(subscribe)
}

async fn check_subscribe(
    shared: &Shared,
    subscribe: &Subscribe,
    nonce: Option<&str>,
) -> std::result::Result<(), (ErrorCode, String)> {
    wire::check_version(&subscribe.ntx)?;
    if let (Some(code), Some(nonce)) = (&shared.pairing_code, nonce) {
        let Some(proof) = &subscribe.pairing else {
            return Err((
                ErrorCode::PairingRequired,
                "this source needs a pairing code".into(),
            ));
        };
        if !pairing::verify(code, nonce, &proof.proof) {
            tokio::time::sleep(Duration::from_secs(1)).await; // slow down guessing (clause 13.4)
            return Err((ErrorCode::PairingFailed, "wrong pairing code".into()));
        }
    }
    Ok(())
}

/// Adds the connection and queues the announcement and full state (clause 9.2) under one lock,
/// so no update is lost or sent twice.
fn register(shared: &Shared, subscribe: &Subscribe) -> (u64, tmpsc::UnboundedReceiver<WsMessage>) {
    let channels: Vec<String> = subscribe
        .channels
        .clone()
        .unwrap_or_else(|| vec![PROGRAM.to_string()])
        .into_iter()
        .filter(|c| shared.channels.contains(c))
        .collect();
    let (out, out_rx) = tmpsc::unbounded_channel();
    let mut state = lock(&shared.state);
    if let Some(types) = &state.types {
        let _ = out.send(WsMessage::text(
            Message::Announce(Announce { types: types.clone() }).to_json(),
        ));
    }
    for channel in &channels {
        // Carries the sequence number of the item's last change, 0 if never set.
        let (seq, type_name, fields) = state
            .items
            .get(channel)
            .cloned()
            .unwrap_or_else(|| (0, "ntx.text".to_string(), Fields::new()));
        let update = Update {
            channel: channel.clone(),
            seq,
            type_name,
            fields,
        };
        let _ = out.send(WsMessage::text(Message::Update(update).to_json()));
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

/// Forwards queued messages, handles tally and heartbeats until the connection ends.
async fn run(
    shared: &Shared,
    connection: u64,
    peer: &mut Peer<TcpStream>,
    out: &mut tmpsc::UnboundedReceiver<WsMessage>,
    stop: &mut watch::Receiver<bool>,
) -> Ended {
    let mut tick = tokio::time::interval(HEARTBEAT_INTERVAL);
    loop {
        let outcome = tokio::select! {
            frame = out.recv() => match frame {
                Some(frame) => peer.send_frame(frame).await,
                None => Err(Ended("source closed".into())),
            },
            message = peer.recv() => message.map(|m| match m {
                Message::Tally(tally) => set_tally(shared, connection, tally),
                Message::Error(e) => tracing::info!("receiver reported {}", e.code.as_str()),
                _ => {}
            }),
            _ = tick.tick() => peer.heartbeat().await,
            _ = stop.changed() => {
                peer.close().await;
                Err(Ended("source stopped".into()))
            }
        };
        if let Err(ended) = outcome {
            return ended;
        }
    }
}

fn set_tally(shared: &Shared, connection: u64, tally: Tally) {
    if let Some(conn) = lock(&shared.state).connections.get_mut(&connection) {
        conn.tally = tally;
    }
    recompute_tally(shared);
}

fn recompute_tally(shared: &Shared) {
    let mut state = lock(&shared.state);
    let combined = Tally {
        program: state.connections.values().any(|c| c.tally.program),
        preview: state.connections.values().any(|c| c.tally.preview),
    };
    if combined != state.tally {
        state.tally = combined;
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
