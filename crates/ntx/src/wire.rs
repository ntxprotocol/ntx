//! The WebSocket side shared by sources and receivers: sending, receiving, heartbeats and errors.

use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::protocol::frame::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

use crate::protocol::{ErrorCode, ErrorMessage, Fields, Heartbeat, Message};
use crate::{Error, MAX_MESSAGE_BYTES, PROTOCOL_VERSION, text};

/// Ping when nothing has been received for this long (clause 11.2).
const PING_AFTER: Duration = Duration::from_secs(5);

/// Give up when nothing has been received for this long (clause 11.2).
const DEAD_AFTER: Duration = Duration::from_secs(15);

/// How long the first message of a handshake may take (clause 8.6).
pub(crate) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

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
        let _ = self.send_frame(close).await;
        Ended(reason)
    }

    /// Closes the connection without an error.
    pub(crate) async fn close(&mut self) {
        let _ = self.send_frame(WsMessage::Close(None)).await;
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

fn bad_message(e: &Error) -> ErrorMessage {
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
