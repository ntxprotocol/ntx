use std::collections::HashMap;
use std::sync::{Mutex, mpsc};
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;

use crate::protocol::{
    Endpoint, ErrorCode, Hello, Message, PairingProof, Subscribe, Tally, TypeDescriptor, Update,
};
use crate::runtime::runtime;
use crate::wire::{self, Ended, HANDSHAKE_TIMEOUT, HEARTBEAT_INTERVAL, Peer, lock};
use crate::{DEFAULT_PORT, Error, PROGRAM, PROTOCOL_VERSION, Result, SUBPROTOCOL, pairing};

type Connection = Peer<MaybeTlsStream<TcpStream>>;

/// Settings for a [`Receiver`].
#[derive(Debug, Clone)]
pub struct ReceiverConfig {
    /// Human-readable name, e.g. `MEDIA-PC (Display)`.
    pub name: String,
    /// Stable id. Defaults to a new random UUID.
    pub id: String,
    /// Channels to receive. Defaults to `program`.
    pub channels: Vec<String>,
    /// Pairing code, if the source needs one.
    pub pairing_code: Option<String>,
    /// Reconnect automatically after the connection is lost. Defaults to `true`.
    pub reconnect: bool,
}

impl ReceiverConfig {
    /// Default settings with the given display name.
    pub fn new(name: impl Into<String>) -> Self {
        ReceiverConfig {
            name: name.into(),
            id: uuid::Uuid::new_v4().to_string(),
            channels: vec![PROGRAM.to_string()],
            pairing_code: None,
            reconnect: true,
        }
    }
}

/// Something that happened to a [`Receiver`].
#[derive(Debug, Clone, PartialEq)]
pub enum ReceiverEvent {
    /// The handshake finished. Updates with the full state follow.
    Connected {
        /// The source's identity.
        source: Endpoint,
        /// The source's session; it changes when the source restarts.
        session: String,
        /// The channels the source has.
        channels: Vec<String>,
    },
    /// The source announced its types (clause 15.5).
    Announce(Vec<TypeDescriptor>),
    /// New item for a channel. Old and repeated updates are already filtered out (clause 9.4),
    /// and text is normalised (clause 17).
    Update(Update),
    /// The source reported an error.
    Error {
        /// What went wrong.
        code: ErrorCode,
        /// Optional text for logs.
        message: Option<String>,
    },
    /// The connection was lost or could not be made. The receiver retries if `reconnect` is on.
    Disconnected {
        /// Why, for logs and people.
        reason: String,
    },
}

/// An NTX receiver: connects to one source and keeps receiving its text.
pub struct Receiver {
    events: Mutex<mpsc::Receiver<ReceiverEvent>>,
    tally: watch::Sender<Option<Tally>>,
    stop: watch::Sender<bool>,
    url: String,
}

impl Receiver {
    /// Connects to a source. `address` is a `ws://` URL, `host` or `host:port` (clause 6.4).
    /// Connecting happens in the background; watch the events.
    ///
    /// # Errors
    ///
    /// Fails if `address` is not a valid address.
    pub fn connect(address: &str, config: ReceiverConfig) -> Result<Receiver> {
        let url = normalize_url(address)?;
        let (events_tx, events_rx) = mpsc::channel();
        let (tally, tally_rx) = watch::channel(None);
        let (stop, stop_rx) = watch::channel(false);
        let link = Link {
            url: url.clone(),
            config,
            events: events_tx,
            tally: tally_rx,
        };
        runtime().spawn(link.run(stop_rx));
        Ok(Receiver {
            events: Mutex::new(events_rx),
            tally,
            stop,
            url,
        })
    }

    /// Tells the source whether its text is on air or in preview (clause 10).
    pub fn set_tally(&self, tally: Tally) {
        self.tally.send_replace(Some(tally));
    }

    /// Takes the next event, if any.
    pub fn poll_event(&self) -> Option<ReceiverEvent> {
        lock(&self.events).try_recv().ok()
    }

    /// Waits up to `timeout` for the next event.
    pub fn wait_event(&self, timeout: Duration) -> Option<ReceiverEvent> {
        lock(&self.events).recv_timeout(timeout).ok()
    }

    /// The WebSocket URL this receiver connects to.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

/// Turns `host`, `host:port` or a `ws://` / `wss://` URL into a URL.
fn normalize_url(address: &str) -> Result<String> {
    let address = address.trim();
    if address.starts_with("ws://") || address.starts_with("wss://") {
        return Ok(address.to_string());
    }
    if address.is_empty() || address.contains(char::is_whitespace) || address.contains("://") {
        return Err(Error::InvalidAddress(address.to_string()));
    }
    let colons = address.matches(':').count();
    let (host, has_port) = if address.starts_with('[') {
        (address.to_string(), address.contains("]:"))
    } else if colons > 1 {
        (format!("[{address}]"), false) // bare IPv6 address
    } else {
        (address.to_string(), colons == 1)
    };
    Ok(if has_port {
        format!("ws://{host}/")
    } else {
        format!("ws://{host}:{DEFAULT_PORT}/")
    })
}

/// The background task behind a [`Receiver`].
struct Link {
    url: String,
    config: ReceiverConfig,
    events: mpsc::Sender<ReceiverEvent>,
    tally: watch::Receiver<Option<Tally>>,
}

/// Session and last applied sequence number per channel (clause 9.4).
#[derive(Default)]
struct Seen {
    session: String,
    seq: HashMap<String, u64>,
}

impl Seen {
    fn start_session(&mut self, session: &str) {
        if self.session != session {
            session.clone_into(&mut self.session);
            self.seq.clear();
        }
    }

    /// Whether an update is new; records it if so.
    fn is_new(&mut self, update: &Update) -> bool {
        let new = self
            .seq
            .get(&update.channel)
            .is_none_or(|&last| update.seq > last);
        if new {
            self.seq.insert(update.channel.clone(), update.seq);
        }
        new
    }
}

impl Link {
    /// Connects, and reconnects with backoff (clause 11.3), until stopped.
    async fn run(mut self, mut stop: watch::Receiver<bool>) {
        let mut seen = Seen::default();
        let mut backoff = Duration::from_secs(1);
        loop {
            let (Ended(reason), handshook) = tokio::select! {
                ended = self.session(&mut seen) => ended,
                _ = stop.changed() => return,
            };
            if self.events.send(ReceiverEvent::Disconnected { reason }).is_err() || !self.config.reconnect {
                return;
            }
            if handshook {
                backoff = Duration::from_secs(1);
            }
            tokio::select! {
                () = tokio::time::sleep(backoff) => {}
                _ = stop.changed() => return,
            }
            backoff = (backoff * 2).min(Duration::from_secs(10));
        }
    }

    /// One connection. Returns why it ended and whether the handshake had finished.
    async fn session(&mut self, seen: &mut Seen) -> (Ended, bool) {
        let (mut peer, hello) = match tokio::time::timeout(HANDSHAKE_TIMEOUT, self.handshake()).await {
            Ok(Ok(connected)) => connected,
            Ok(Err(ended)) => return (ended, false),
            Err(_) => return (Ended("no hello from source".into()), false),
        };
        seen.start_session(&hello.session);
        let _ = self.events.send(ReceiverEvent::Connected {
            source: hello.source,
            session: hello.session,
            channels: hello.channels,
        });
        (self.receive(&mut peer, seen).await, true)
    }

    /// Connects, waits for `hello` and answers with `subscribe` (clause 8).
    async fn handshake(&self) -> std::result::Result<(Connection, Hello), Ended> {
        let mut request = self
            .url
            .as_str()
            .into_client_request()
            .map_err(|e| Ended(e.to_string()))?;
        request
            .headers_mut()
            .insert("sec-websocket-protocol", HeaderValue::from_static(SUBPROTOCOL));
        let (ws, _) = tokio_tungstenite::connect_async_with_config(request, Some(wire::ws_config()), true)
            .await
            .map_err(|e| Ended(e.to_string()))?;
        let mut peer = Peer::new(ws, "hello");
        let hello = match peer.recv().await? {
            Message::Hello(hello) => hello,
            Message::Error(e) => return Err(Ended(format!("source error: {}", e.code.as_str()))),
            _ => return Err(peer.reject(ErrorCode::BadMessage, "expected hello".into()).await),
        };
        if let Err((code, reason)) = wire::check_version(&hello.ntx) {
            return Err(peer.reject(code, reason).await);
        }
        let pairing =
            hello
                .pairing
                .as_ref()
                .zip(self.config.pairing_code.as_ref())
                .map(|(challenge, code)| PairingProof {
                    proof: pairing::proof(code, &challenge.nonce),
                });
        peer.send(&Message::Subscribe(Subscribe {
            ntx: PROTOCOL_VERSION.to_string(),
            receiver: Endpoint {
                id: self.config.id.clone(),
                name: self.config.name.clone(),
            },
            channels: Some(self.config.channels.clone()),
            pairing,
        }))
        .await?;
        peer.handshake_done();
        Ok((peer, hello))
    }

    /// Receives until the connection ends, sending tally whenever it changes.
    async fn receive(&mut self, peer: &mut Connection, seen: &mut Seen) -> Ended {
        self.tally.mark_changed(); // send the current tally, if any, on every new connection
        let mut tick = tokio::time::interval(HEARTBEAT_INTERVAL);
        loop {
            let outcome = tokio::select! {
                message = peer.recv() => match message {
                    Ok(message) => {
                        self.handle(message, seen);
                        Ok(())
                    }
                    Err(ended) => Err(ended),
                },
                changed = self.tally.changed() => {
                    let current = *self.tally.borrow_and_update();
                    match (changed, current) {
                        (Err(_), _) => Err(Ended("receiver closed".into())),
                        (Ok(()), Some(tally)) => peer.send(&Message::Tally(tally)).await,
                        (Ok(()), None) => Ok(()),
                    }
                }
                _ = tick.tick() => peer.heartbeat().await,
            };
            if let Err(ended) = outcome {
                return ended;
            }
        }
    }

    fn handle(&self, message: Message, seen: &mut Seen) {
        let event = match message {
            Message::Update(update) if seen.is_new(&update) => ReceiverEvent::Update(Update {
                fields: wire::normalize_fields(update.fields),
                ..update
            }),
            Message::Announce(a) => ReceiverEvent::Announce(a.types),
            Message::Error(e) => ReceiverEvent::Error {
                code: e.code,
                message: e.message,
            },
            _ => return,
        };
        let _ = self.events.send(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses() {
        assert_eq!(normalize_url("192.168.1.20").unwrap(), "ws://192.168.1.20:7686/");
        assert_eq!(normalize_url(" pc.local:9000 ").unwrap(), "ws://pc.local:9000/");
        assert_eq!(normalize_url("ws://pc:1/a").unwrap(), "ws://pc:1/a");
        assert_eq!(normalize_url("wss://pc/").unwrap(), "wss://pc/");
        assert_eq!(normalize_url("fe80::1").unwrap(), "ws://[fe80::1]:7686/");
        assert_eq!(normalize_url("[fe80::1]").unwrap(), "ws://[fe80::1]:7686/");
        assert_eq!(normalize_url("[fe80::1]:9").unwrap(), "ws://[fe80::1]:9/");
        for bad in ["http://x", "", "a b"] {
            assert!(normalize_url(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn sequence_numbers() {
        let update = |channel: &str, seq| Update {
            channel: channel.into(),
            seq,
            type_name: "ntx.text".into(),
            fields: crate::Fields::new(),
        };
        let mut seen = Seen::default();
        seen.start_session("a");
        assert!(seen.is_new(&update("program", 0)));
        assert!(seen.is_new(&update("program", 5)));
        assert!(!seen.is_new(&update("program", 5)), "repeat");
        assert!(!seen.is_new(&update("program", 4)), "older");
        assert!(seen.is_new(&update("preview", 1)), "channels are independent");
        seen.start_session("a");
        assert!(!seen.is_new(&update("program", 5)), "same session keeps numbers");
        seen.start_session("b");
        assert!(seen.is_new(&update("program", 1)), "a new session starts again");
    }
}
