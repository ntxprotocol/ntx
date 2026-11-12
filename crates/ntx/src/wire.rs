//! The WebSocket side shared by every endpoint: listening, sending, receiving, heartbeats and
//! errors.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::{HeaderValue, StatusCode};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::protocol::frame::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

use crate::protocol::{
    ErrorCode, ErrorMessage, Fields, Heartbeat, Hello, MAX_ERROR_CHARS, Message, PairingChallenge,
    PairingProof,
};
use crate::runtime::runtime;
use crate::{Error, MAX_MESSAGE_BYTES, PROTOCOL_VERSION, SUBPROTOCOL, pairing, text};

/// Ping when nothing has been received for this long (clause 11.2).
const PING_AFTER: Duration = Duration::from_secs(5);

/// Give up when nothing has been received for this long (clause 11.2).
const DEAD_AFTER: Duration = Duration::from_secs(15);

/// Close code for a timeout (clause 12.5): no `subscribe` in time, or a silent peer.
pub(crate) const TIMEOUT_CLOSE: u16 = 4008;

/// How long the first message of a handshake may take (clause 8.6).
pub(crate) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Longest close reason: a close frame's payload is at most 125 bytes, two of them the code.
const MAX_CLOSE_REASON: usize = 123;

/// How long to wait for the peer to answer a close.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

/// How often [`Peer::heartbeat`] should be called.
pub(crate) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

/// WebSocket limits. They are larger than [`MAX_MESSAGE_BYTES`] so that an oversized message
/// reaches NTX code, which answers `too_large` as the spec requires.
pub(crate) fn ws_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .max_message_size(Some(4 * MAX_MESSAGE_BYTES))
        .max_frame_size(Some(4 * MAX_MESSAGE_BYTES))
}

/// Locks a mutex, ignoring poisoning: NTX state stays consistent even if a holder panicked.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Applies the text rules (clause 17) to every string in `fields`.
pub(crate) fn normalize_fields(fields: Fields) -> Fields {
    let mut value = Value::Object(fields);
    normalize_strings(&mut value);
    match value {
        Value::Object(fields) => fields,
        _ => unreachable!("normalising keeps the value an object"),
    }
}

fn normalize_strings(value: &mut Value) {
    match value {
        Value::String(s) => *s = text::normalize(s),
        Value::Array(items) => items.iter_mut().for_each(normalize_strings),
        Value::Object(map) => map.values_mut().for_each(normalize_strings),
        _ => {}
    }
}

/// Most frames waiting for one connection. State is latest-wins, so a peer that falls this far
/// behind is dropped rather than queued for; it gets the whole state again when it reconnects.
const QUEUE: usize = 256;

/// A queue of frames for one connection.
pub(crate) fn queue() -> (mpsc::Sender<WsMessage>, mpsc::Receiver<WsMessage>) {
    mpsc::channel(QUEUE)
}

/// Queues `frame` for a connection. A full or closed queue is let go, which ends that
/// connection (see [`pump`]). Returns whether the frame was queued.
pub(crate) fn enqueue(out: &mut Option<mpsc::Sender<WsMessage>>, frame: WsMessage) -> bool {
    let queued = out.as_ref().is_some_and(|out| out.try_send(frame).is_ok());
    if !queued {
        *out = None;
    }
    queued
}

/// Opens a listening TCP socket for the background runtime.
pub(crate) fn listen(bind: IpAddr, port: u16) -> crate::Result<(TcpListener, SocketAddr)> {
    let listener = std::net::TcpListener::bind((bind, port))?;
    listener.set_nonblocking(true)?;
    let local_addr = listener.local_addr()?;
    let _guard = runtime().enter();
    Ok((TcpListener::from_std(listener)?, local_addr))
}

/// Most connections one endpoint serves at once.
const MAX_CONNECTIONS: usize = 64;

/// Most connections one endpoint serves at once from one IP address. Higher than a relay's
/// per-origin limit, because every local publisher comes from the loopback address.
const MAX_CONNECTIONS_PER_ADDRESS: usize = 16;

/// Wrong pairing codes from one IP address before it is locked out (clause 13.4).
const MAX_PAIRING_FAILURES: u32 = 10;

/// How long a lockout lasts, and how long failures are remembered.
const PAIRING_LOCKOUT: Duration = Duration::from_secs(60);

/// The connections and pairing failures of one listening endpoint, per IP address.
#[derive(Default)]
struct Limits {
    connections: Mutex<HashMap<IpAddr, usize>>,
    /// Failures and the time of the last one.
    pairing_failures: Mutex<HashMap<IpAddr, (u32, Instant)>>,
}

/// A connection within the limits. Its place is freed when the slot is dropped.
pub(crate) struct Slot {
    limits: Arc<Limits>,
    ip: IpAddr,
}

impl Slot {
    /// Takes a place for a connection from `ip`, if the limits allow it.
    fn take(limits: &Arc<Limits>, ip: IpAddr) -> Option<Slot> {
        let mut connections = lock(&limits.connections);
        let total: usize = connections.values().sum();
        let from_ip = connections.entry(ip).or_default();
        if total >= MAX_CONNECTIONS || *from_ip >= MAX_CONNECTIONS_PER_ADDRESS {
            return None;
        }
        *from_ip += 1;
        Some(Slot {
            limits: limits.clone(),
            ip,
        })
    }

    /// Whether this address made too many wrong pairing attempts recently.
    fn locked_out(&self) -> bool {
        let failures = lock(&self.limits.pairing_failures);
        failures
            .get(&self.ip)
            .is_some_and(|&(count, last)| count >= MAX_PAIRING_FAILURES && last.elapsed() < PAIRING_LOCKOUT)
    }

    fn pairing_failed(&self) {
        let mut failures = lock(&self.limits.pairing_failures);
        failures.retain(|_, (_, last)| last.elapsed() < PAIRING_LOCKOUT);
        let entry = failures.entry(self.ip).or_insert((0, Instant::now()));
        *entry = (entry.0 + 1, Instant::now());
    }

    fn paired(&self) {
        lock(&self.limits.pairing_failures).remove(&self.ip);
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        let mut connections = lock(&self.limits.connections);
        if let Some(count) = connections.get_mut(&self.ip) {
            *count -= 1;
            if *count == 0 {
                connections.remove(&self.ip);
            }
        }
    }
}

/// A new TCP connection within the limits.
pub(crate) struct Incoming {
    pub(crate) stream: TcpStream,
    pub(crate) address: SocketAddr,
    pub(crate) slot: Slot,
}

/// Accepts connections on the background runtime, serving each one on its own task with
/// `serve(incoming, shared, stop)`. Connections over the limits are closed at once, before the
/// WebSocket upgrade. Stops when the returned [`Stopper`] is dropped.
pub(crate) fn serve_connections<T, Fut>(
    listener: TcpListener,
    shared: Arc<T>,
    serve: fn(Incoming, Arc<T>, watch::Receiver<bool>) -> Fut,
) -> Stopper
where
    T: Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let (stop, stop_rx) = watch::channel(false);
    runtime().spawn(accept_loop(listener, shared, serve, stop_rx));
    Stopper(stop)
}

/// How long dropping a [`Stopper`] waits for its connections to close.
const CLOSE_WAIT: Duration = Duration::from_secs(1);

/// Stops accepting and serving connections when dropped. It tells each connection to close, then
/// waits up to [`CLOSE_WAIT`] until all have sent their close frame, so that a program which
/// exits right afterwards still closes them properly instead of resetting them.
pub(crate) struct Stopper(watch::Sender<bool>);

impl Drop for Stopper {
    fn drop(&mut self) {
        let _ = self.0.send(true);
        // Each connection task and the accept loop hold a receiver until they are done.
        let deadline = Instant::now() + CLOSE_WAIT;
        while self.0.receiver_count() > 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

async fn accept_loop<T, Fut>(
    listener: TcpListener,
    shared: Arc<T>,
    serve: fn(Incoming, Arc<T>, watch::Receiver<bool>) -> Fut,
    mut stop: watch::Receiver<bool>,
) where
    Fut: Future<Output = ()> + Send + 'static,
{
    let limits = Arc::new(Limits::default());
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, address)) => {
                    let Some(slot) = Slot::take(&limits, address.ip()) else {
                        tracing::debug!("too many connections; closed one from {address}");
                        continue;
                    };
                    let _ = stream.set_nodelay(true);
                    let incoming = Incoming { stream, address, slot };
                    tokio::spawn(serve(incoming, shared.clone(), stop.clone()));
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

/// Accepts a WebSocket upgrade on `path` only. Returns the connection, expecting `handshake` as
/// its first message, and the HTTP `Origin` header if there is one (clause 14.5).
#[allow(clippy::result_large_err)] // the upgrade callback's error type comes from tungstenite
pub(crate) async fn accept(
    stream: TcpStream,
    path: &str,
    handshake: &'static str,
) -> Result<(Peer<TcpStream>, Option<String>), Ended> {
    let mut origin = None;
    let upgrade = tokio_tungstenite::accept_hdr_async_with_config(
        stream,
        |request: &Request, response: Response| {
            origin = request
                .headers()
                .get("origin")
                .and_then(|v| v.to_str().ok())
                .map(String::from);
            check_upgrade(path, request, response)
        },
        Some(ws_config()),
    );
    let ws = tokio::time::timeout(HANDSHAKE_TIMEOUT, upgrade)
        .await
        .map_err(|_| Ended("no WebSocket upgrade".into()))?
        .map_err(|e| Ended(e.to_string()))?;
    Ok((Peer::new(ws, handshake), origin))
}

/// Accepts the upgrade only on `path`, and confirms the `ntx` subprotocol if offered (clause 5.2).
#[allow(clippy::result_large_err)] // signature required by tungstenite
fn check_upgrade(path: &str, request: &Request, mut response: Response) -> Result<Response, ErrorResponse> {
    if request.uri().path() != path {
        let mut not_found = ErrorResponse::new(Some("no NTX endpoint at this path".into()));
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

/// Serialises a message, enforcing the size limit (clause 18.1).
pub(crate) fn frame(message: &Message) -> crate::Result<WsMessage> {
    let json = message.to_json();
    if json.len() > MAX_MESSAGE_BYTES {
        return Err(Error::TooLarge(json.len()));
    }
    Ok(WsMessage::text(json))
}

/// Why a connection is being stopped: `None` to close normally, or an error to send first.
pub(crate) type Refusal = Option<(ErrorCode, String)>;

/// Runs a connection after its handshake until it ends: sends queued frames, passes received
/// messages to `handle`, and keeps the heartbeat (clause 11). When `stop` completes, it sends
/// the frames still queued, then closes (with the error `stop` gives, if any).
pub(crate) async fn pump<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    peer: &mut Peer<S>,
    out: &mut mpsc::Receiver<WsMessage>,
    stop: impl Future<Output = Refusal>,
    mut handle: impl FnMut(Message),
) -> Ended {
    tokio::pin!(stop);
    let mut tick = tokio::time::interval(HEARTBEAT_INTERVAL);
    loop {
        let outcome = tokio::select! {
            frame = out.recv() => match frame {
                Some(frame) => peer.send_frame(frame).await,
                None => Err(Ended("closed".into())),
            },
            message = peer.recv() => message.map(&mut handle),
            _ = tick.tick() => peer.heartbeat().await,
            refusal = &mut stop => {
                while let Ok(frame) = out.try_recv() {
                    if peer.send_frame(frame).await.is_err() {
                        break;
                    }
                }
                if let Some((code, reason)) = refusal {
                    Err(peer.reject(code, reason).await)
                } else {
                    peer.close(None).await;
                    Err(Ended("stopped".into()))
                }
            }
        };
        if let Err(ended) = outcome {
            return ended;
        }
    }
}

/// Why a connection ended or a handshake was refused.
#[derive(Debug)]
pub(crate) struct Ended(pub(crate) String);

/// One WebSocket connection speaking NTX.
pub(crate) struct Peer<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> {
    sink: SplitSink<WebSocketStream<S>, WsMessage>,
    /// Always present; taken only when the peer is dropped.
    stream: Option<SplitStream<WebSocketStream<S>>>,
    /// A close frame has been sent.
    closing: bool,
    last_received: Instant,
    last_ping: Instant,
    pings: u64,
    /// The op expected next during the handshake (`subscribe` or `hello`); `None` afterwards.
    handshake: Option<&'static str>,
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> Peer<S> {
    pub(crate) fn new(ws: WebSocketStream<S>, handshake: &'static str) -> Self {
        let (sink, stream) = ws.split();
        Peer {
            sink,
            stream: Some(stream),
            closing: false,
            last_received: Instant::now(),
            last_ping: Instant::now(),
            pings: 0,
            handshake: Some(handshake),
        }
    }

    /// Marks the handshake as finished: from now on bad messages are answered, not fatal.
    pub(crate) fn handshake_done(&mut self) {
        self.handshake = None;
    }

    pub(crate) async fn send(&mut self, message: &Message) -> Result<(), Ended> {
        self.send_frame(WsMessage::text(message.to_json())).await
    }

    /// Sends one frame. A peer that stops reading for [`DEAD_AFTER`] is given up on, so a stalled
    /// peer cannot hold the connection open.
    pub(crate) async fn send_frame(&mut self, frame: WsMessage) -> Result<(), Ended> {
        tokio::time::timeout(DEAD_AFTER, self.sink.send(frame))
            .await
            .map_err(|_| Ended("peer stopped reading".into()))?
            .map_err(|e| Ended(e.to_string()))
    }

    /// Sends `error` and closes the connection with the matching close code (clause 12).
    pub(crate) async fn reject(&mut self, code: ErrorCode, reason: String) -> Ended {
        let error = Message::Error(ErrorMessage {
            code,
            message: Some(text::shorten(&reason, MAX_ERROR_CHARS, text::chars).to_string()),
        });
        // The connection ends either way, so failures to send are not interesting.
        let _ = self.send(&error).await;
        self.close(Some((code.close_code(), &reason))).await;
        Ended(reason)
    }

    /// Closes the connection, with a WebSocket close code and reason if given. The reason is cut
    /// to fit a close frame (RFC 6455, section 5.5). When the peer is dropped, or [`Peer::finish`]
    /// is called, it waits for the other side to close too.
    pub(crate) async fn close(&mut self, code: Option<(u16, &str)>) {
        let frame = code.map(|(code, reason)| CloseFrame {
            code: CloseCode::from(code),
            reason: text::shorten(reason, MAX_CLOSE_REASON, str::len).into(),
        });
        self.closing = self.send_frame(WsMessage::Close(frame)).await.is_ok();
    }

    /// Drops the connection, first waiting (briefly) for the other side to answer a close.
    pub(crate) async fn finish(mut self) {
        if let Some(stream) = self.stream.take().filter(|_| self.closing) {
            drain(stream).await;
        }
    }

    /// Returns the next NTX message. Pings are answered and unknown ops skipped (clause 7.2).
    /// Invalid messages are answered with `bad_message`, or end a handshake.
    pub(crate) async fn recv(&mut self) -> Result<Message, Ended> {
        loop {
            let stream = self.stream.as_mut().expect("present until dropped");
            let frame = match stream.next().await {
                Some(Ok(frame)) => frame,
                Some(Err(e)) => return Err(Ended(e.to_string())),
                None => return Err(Ended("connection closed".into())),
            };
            self.last_received = Instant::now();
            let text = match frame {
                WsMessage::Text(text) => text,
                WsMessage::Close(frame) => {
                    let reason = frame.map_or_else(
                        || "closed by peer".to_string(),
                        |f| format!("closed by peer ({}): {}", u16::from(f.code), f.reason),
                    );
                    return Err(Ended(reason));
                }
                _ => continue, // binary frames are ignored (clause 5.3); pings are handled by tungstenite
            };
            match Message::parse(&text) {
                Ok(Some(Message::Ping(Heartbeat { id }))) => {
                    self.send(&Message::Pong(Heartbeat { id })).await?;
                }
                Ok(Some(message)) => return Ok(message),
                Ok(None) => {}
                Err(Error::TooLarge(n)) => {
                    return Err(self
                        .reject(ErrorCode::TooLarge, format!("message is {n} bytes"))
                        .await);
                }
                Err(e) => match self.handshake {
                    Some(op) if is_other_version(&text, op) => {
                        let reason = format!("this endpoint speaks NTX {PROTOCOL_VERSION}");
                        return Err(self.reject(ErrorCode::UnsupportedVersion, reason).await);
                    }
                    Some(_) => return Err(self.reject(ErrorCode::BadMessage, e.to_string()).await),
                    None => self.send(&Message::Error(bad_message(&e))).await?,
                },
            }
        }
    }

    /// Call every [`HEARTBEAT_INTERVAL`]: pings a quiet peer and closes the connection to a
    /// silent one (clause 11.2).
    pub(crate) async fn heartbeat(&mut self) -> Result<(), Ended> {
        if self.last_received.elapsed() >= DEAD_AFTER {
            let reason = "nothing received for 15 seconds";
            self.close(Some((TIMEOUT_CLOSE, reason))).await;
            return Err(Ended(format!("no answer from peer: {reason}")));
        }
        if self.last_received.elapsed() >= PING_AFTER && self.last_ping.elapsed() >= PING_AFTER {
            self.pings += 1;
            self.last_ping = Instant::now();
            self.send(&Message::Ping(Heartbeat {
                id: self.pings.to_string(),
            }))
            .await?;
        }
        Ok(())
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> Drop for Peer<S> {
    /// After a close, reads until the peer closes too. Dropping a socket with unread data resets
    /// the connection on some systems (Windows), and the peer would lose its last messages, such
    /// as an `error`. This runs on its own task, so nothing can cancel it halfway.
    fn drop(&mut self) {
        if let Some(stream) = self.stream.take().filter(|_| self.closing) {
            runtime().spawn(drain(stream));
        }
    }
}

/// Reads until the other side closes, for up to [`CLOSE_TIMEOUT`].
async fn drain<S: AsyncRead + AsyncWrite + Unpin>(mut stream: SplitStream<WebSocketStream<S>>) {
    let closed = async {
        while let Some(Ok(frame)) = stream.next().await {
            if frame.is_close() {
                break;
            }
        }
    };
    let _ = tokio::time::timeout(CLOSE_TIMEOUT, closed).await;
}

/// The `bad_message` error for `e`.
pub(crate) fn bad_message(e: &Error) -> ErrorMessage {
    ErrorMessage {
        code: ErrorCode::BadMessage,
        message: Some(e.to_string()),
    }
}

/// A handshake message for another protocol version may not pass this version's checks.
fn is_other_version(text: &str, op: &str) -> bool {
    serde_json::from_str::<Value>(text).is_ok_and(|v| {
        v.get("op").and_then(Value::as_str) == Some(op)
            && v.get("ntx")
                .and_then(Value::as_str)
                .is_some_and(|n| n != PROTOCOL_VERSION)
    })
}

/// The server side of a handshake (clauses 8, 13 and 14.2): sends `hello`, with a pairing
/// challenge when `pairing_code` is set, and waits for the answer that `pick` accepts. `pick`
/// returns the answer with its version and pairing proof, which are then checked.
pub(crate) async fn greet<T>(
    peer: &mut Peer<TcpStream>,
    slot: &Slot,
    mut hello: Hello,
    pairing_code: Option<&str>,
    pick: impl Fn(Message) -> Option<(T, String, Option<PairingProof>)>,
) -> Result<T, Ended> {
    let greeting = async {
        let nonce = pairing_code.map(|_| pairing::new_nonce());
        hello.pairing = nonce.clone().map(|nonce| PairingChallenge { nonce });
        peer.send(&Message::Hello(hello)).await?;
        let (answer, ntx, proof) = loop {
            if let Some(found) = pick(peer.recv().await?) {
                break found;
            }
        };
        let pairing = pairing_code.zip(nonce.as_deref());
        if let Err((code, reason)) = check_answer(&ntx, pairing, proof.as_ref(), slot).await {
            return Err(peer.reject(code, reason).await);
        }
        peer.handshake_done();
        Ok(answer)
    };
    if let Ok(greeted) = tokio::time::timeout(HANDSHAKE_TIMEOUT, greeting).await {
        return greeted;
    }
    let reason = "no answer to hello within 10 seconds";
    peer.close(Some((TIMEOUT_CLOSE, reason))).await;
    Err(Ended(reason.into()))
}

/// Checks the version and pairing proof of a `subscribe` or `publish` (clauses 13.4, 19.3).
/// `pairing` holds the pairing code and the nonce sent in `hello`, when pairing is required.
/// Guessing is slowed down (clause 13.4): each failure waits a second, and an address with too
/// many recent failures is locked out for a while, however many connections it opens.
async fn check_answer(
    ntx: &str,
    pairing: Option<(&str, &str)>,
    proof: Option<&PairingProof>,
    slot: &Slot,
) -> Result<(), (ErrorCode, String)> {
    check_version(ntx)?;
    if let Some((code, nonce)) = pairing {
        let Some(proof) = proof.and_then(|p| p.proof.as_deref()) else {
            return Err((ErrorCode::PairingRequired, "a pairing code is needed".into()));
        };
        let locked_out = slot.locked_out();
        if locked_out || !pairing::verify(code, nonce, proof) {
            slot.pairing_failed();
            tokio::time::sleep(Duration::from_secs(1)).await;
            let reason = if locked_out {
                "too many wrong pairing codes; try again later"
            } else {
                "wrong pairing code"
            };
            return Err((ErrorCode::PairingFailed, reason.into()));
        }
        slot.paired();
    }
    Ok(())
}

/// Checks the version in a handshake message (clause 19.3).
pub(crate) fn check_version(ntx: &str) -> Result<(), (ErrorCode, String)> {
    if ntx == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err((
            ErrorCode::UnsupportedVersion,
            format!("this endpoint speaks NTX {PROTOCOL_VERSION}, not {ntx}"),
        ))
    }
}
