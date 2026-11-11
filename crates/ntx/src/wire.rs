//! The WebSocket side shared by every endpoint: listening, sending, receiving, heartbeats and
//! errors.

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
    ErrorCode, ErrorMessage, Fields, Heartbeat, Hello, Message, PairingChallenge, PairingProof,
};
use crate::runtime::runtime;
use crate::{Error, MAX_MESSAGE_BYTES, PROTOCOL_VERSION, SUBPROTOCOL, pairing, text};

/// Ping when nothing has been received for this long (clause 11.2).
const PING_AFTER: Duration = Duration::from_secs(5);

/// Give up when nothing has been received for this long (clause 11.2).
const DEAD_AFTER: Duration = Duration::from_secs(15);

/// How long the first message of a handshake may take (clause 8.6).
pub(crate) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

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

/// Opens a listening TCP socket for the background runtime.
pub(crate) fn listen(bind: IpAddr, port: u16) -> crate::Result<(TcpListener, SocketAddr)> {
    let listener = std::net::TcpListener::bind((bind, port))?;
    listener.set_nonblocking(true)?;
    let local_addr = listener.local_addr()?;
    let _guard = runtime().enter();
    Ok((TcpListener::from_std(listener)?, local_addr))
}

/// Accepts connections on the background runtime, serving each one on its own task with
/// `serve(stream, address, shared, stop)`. Stops when the returned sender sends `true`.
pub(crate) fn serve_connections<T, Fut>(
    listener: TcpListener,
    shared: Arc<T>,
    serve: fn(TcpStream, SocketAddr, Arc<T>, watch::Receiver<bool>) -> Fut,
) -> watch::Sender<bool>
where
    T: Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let (stop, stop_rx) = watch::channel(false);
    runtime().spawn(accept_loop(listener, shared, serve, stop_rx));
    stop
}

async fn accept_loop<T, Fut>(
    listener: TcpListener,
    shared: Arc<T>,
    serve: fn(TcpStream, SocketAddr, Arc<T>, watch::Receiver<bool>) -> Fut,
    mut stop: watch::Receiver<bool>,
) where
    Fut: Future<Output = ()> + Send + 'static,
{
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
    let ws = upgrade.await.map_err(|e| Ended(e.to_string()))?;
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

/// Runs a connection after its handshake until it ends: sends queued frames, passes received
/// messages to `handle`, keeps the heartbeat (clause 11) and closes when `stop` completes.
pub(crate) async fn pump<S: AsyncRead + AsyncWrite + Unpin>(
    peer: &mut Peer<S>,
    out: &mut mpsc::UnboundedReceiver<WsMessage>,
    stop: impl Future<Output = ()>,
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
            () = &mut stop => {
                peer.close().await;
                Err(Ended("stopped".into()))
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
pub(crate) struct Peer<S> {
    sink: SplitSink<WebSocketStream<S>, WsMessage>,
    stream: SplitStream<WebSocketStream<S>>,
    last_received: Instant,
    last_ping: Instant,
    pings: u64,
    /// The op expected next during the handshake (`subscribe` or `hello`); `None` afterwards.
    handshake: Option<&'static str>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Peer<S> {
    pub(crate) fn new(ws: WebSocketStream<S>, handshake: &'static str) -> Self {
        let (sink, stream) = ws.split();
        Peer {
            sink,
            stream,
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

    pub(crate) async fn send_frame(&mut self, frame: WsMessage) -> Result<(), Ended> {
        self.sink.send(frame).await.map_err(|e| Ended(e.to_string()))
    }

    /// Sends `error` and closes the connection with the matching close code (clause 12).
    pub(crate) async fn reject(&mut self, code: ErrorCode, reason: String) -> Ended {
        let error = Message::Error(ErrorMessage {
            code: code.clone(),
            message: Some(reason.clone()),
        });
        let close = WsMessage::Close(Some(CloseFrame {
            code: CloseCode::from(code.close_code()),
            reason: reason.as_str().into(),
        }));
        // The connection ends either way, so failures to send are not interesting.
        let _ = self.send(&error).await;
        self.close_with(close).await;
        Ended(reason)
    }

    /// Closes the connection without an error.
    pub(crate) async fn close(&mut self) {
        self.close_with(WsMessage::Close(None)).await;
    }

    /// Sends `close` and waits briefly for the peer to close too. Dropping a socket with unread
    /// data resets the connection on some systems (Windows), and the peer would lose the last
    /// messages, such as an `error`, before reading them.
    async fn close_with(&mut self, close: WsMessage) {
        if self.send_frame(close).await.is_ok() {
            let drain = async {
                while let Some(Ok(frame)) = self.stream.next().await {
                    if frame.is_close() {
                        break;
                    }
                }
            };
            let _ = tokio::time::timeout(CLOSE_TIMEOUT, drain).await;
        }
    }

    /// Returns the next NTX message. Pings are answered and unknown ops skipped (clause 7.2).
    /// Invalid messages are answered with `bad_message`, or end a handshake.
    pub(crate) async fn recv(&mut self) -> Result<Message, Ended> {
        loop {
            let frame = match self.stream.next().await {
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

    /// Call every [`HEARTBEAT_INTERVAL`]: pings a quiet peer and gives up on a silent one.
    pub(crate) async fn heartbeat(&mut self) -> Result<(), Ended> {
        if self.last_received.elapsed() >= DEAD_AFTER {
            return Err(Ended("no answer from peer".into()));
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
        if let Err((code, reason)) = check_answer(&ntx, pairing, proof.as_ref()).await {
            return Err(peer.reject(code, reason).await);
        }
        peer.handshake_done();
        Ok(answer)
    };
    tokio::time::timeout(HANDSHAKE_TIMEOUT, greeting)
        .await
        .unwrap_or_else(|_| Err(Ended("no answer to hello".into())))
}

/// Checks the version and pairing proof of a `subscribe` or `publish` (clauses 13.4, 19.3).
/// `pairing` holds the pairing code and the nonce sent in `hello`, when pairing is required.
async fn check_answer(
    ntx: &str,
    pairing: Option<(&str, &str)>,
    proof: Option<&PairingProof>,
) -> Result<(), (ErrorCode, String)> {
    check_version(ntx)?;
    if let Some((code, nonce)) = pairing {
        let Some(proof) = proof else {
            return Err((ErrorCode::PairingRequired, "a pairing code is needed".into()));
        };
        if !pairing::verify(code, nonce, &proof.proof) {
            tokio::time::sleep(Duration::from_secs(1)).await; // slow down guessing (clause 13.4)
            return Err((ErrorCode::PairingFailed, "wrong pairing code".into()));
        }
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
