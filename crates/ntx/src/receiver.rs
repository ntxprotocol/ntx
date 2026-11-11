use std::collections::HashMap;
use std::sync::{Mutex, mpsc};
use std::time::Duration;

use tokio::sync::watch;

use crate::client::{self, Client, Connection, Role, Settings, Stop};
use crate::protocol::{Endpoint, ErrorCode, ErrorMessage, Hello, Message, Tally, TypeDescriptor, Update};
use crate::runtime::runtime;
use crate::wire::{self, Ended, HEARTBEAT_INTERVAL, lock};
use crate::{PROGRAM, Result};

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
///
/// Serialises as `{"event": "update", "data": {...}}`.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(tag = "event", content = "data", rename_all = "snake_case")]
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
    stop: watch::Sender<Stop>,
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
        let url = client::normalize_url(address, "/")?;
        let (events_tx, events_rx) = mpsc::channel();
        let (tally, tally_rx) = watch::channel(None);
        let (stop, stop_rx) = watch::channel(Stop::Run);
        let settings = Settings::new(
            Role::Receiver,
            url.clone(),
            config.id,
            config.name,
            config.channels,
            config.pairing_code,
            config.reconnect,
        );
        let link = Link {
            settings,
            events: events_tx,
            tally: tally_rx,
            seen: Seen::default(),
        };
        runtime().spawn(client::keep_connected(link, stop_rx));
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
        let _ = self.stop.send(Stop::Now);
    }
}

/// The background task behind a [`Receiver`].
struct Link {
    settings: Settings,
    events: mpsc::Sender<ReceiverEvent>,
    tally: watch::Receiver<Option<Tally>>,
    seen: Seen,
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

impl Client for Link {
    fn settings(&self) -> &Settings {
        &self.settings
    }

    async fn connected(&mut self, mut peer: Connection, hello: Hello, _: watch::Receiver<Stop>) -> Ended {
        self.seen.start_session(&hello.session);
        let _ = self.events.send(ReceiverEvent::Connected {
            source: hello.source,
            session: hello.session,
            channels: hello.channels,
        });
        self.receive(&mut peer).await
    }

    fn refused(&mut self, error: ErrorMessage) {
        let _ = self.events.send(ReceiverEvent::Error {
            code: error.code,
            message: error.message,
        });
    }

    fn disconnected(&mut self, reason: String) -> bool {
        self.events.send(ReceiverEvent::Disconnected { reason }).is_ok()
    }
}

impl Link {
    /// Receives until the connection ends, sending tally whenever it changes.
    async fn receive(&mut self, peer: &mut Connection) -> Ended {
        self.tally.mark_changed(); // send the current tally, if any, on every new connection
        let mut tick = tokio::time::interval(HEARTBEAT_INTERVAL);
        loop {
            let outcome = tokio::select! {
                message = peer.recv() => match message {
                    Ok(message) => {
                        self.handle(message);
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

    fn handle(&mut self, message: Message) {
        let event = match message {
            Message::Update(update) if self.seen.is_new(&update) => ReceiverEvent::Update(Update {
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
