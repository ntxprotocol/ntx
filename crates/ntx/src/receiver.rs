use std::collections::HashMap;
use std::sync::{Mutex, mpsc};
use std::time::Duration;

use tokio::sync::watch;

use crate::client::{self, Client, Connection, Progress, Role, Settings, Stop};
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
    /// Reconnect automatically after the connection is lost. Defaults to `true`. Even when on,
    /// the receiver waits for [`Receiver::reconnect`] after an error that only the user can
    /// fix (clause 11.3).
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
    /// The source confirmed the subscription by sending its first `update` (clause 8.3). That
    /// update and the rest of the state follow at once.
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
    /// The source reported an error, or this receiver found one that refuses the connection
    /// (`unsupported_version`, or `pairing_required` when it has no pairing code). After
    /// `unsupported_version`, `pairing_required`, `pairing_failed` and `not_allowed` it does not
    /// reconnect until [`Receiver::reconnect`] is called (clause 11.3).
    Error {
        /// What went wrong.
        code: ErrorCode,
        /// Optional text for logs.
        message: Option<String>,
    },
    /// The connection was lost or could not be made. The receiver retries if `reconnect` is on,
    /// waiting 1 second, then twice as long each time up to 10 seconds; the wait goes back to 1
    /// second once an `update` arrives (clause 11.3).
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
    retry: watch::Sender<()>,
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
        let (retry, retry_rx) = watch::channel(());
        let settings = Settings::new(
            Role::Receiver,
            url.clone(),
            config.id,
            config.name,
            config.channels,
            config.pairing_code,
            config.reconnect,
        )?;
        let link = Link {
            settings,
            events: events_tx,
            tally: tally_rx,
            seen: Seen::default(),
        };
        runtime().spawn(client::keep_connected(link, stop_rx, retry_rx));
        Ok(Receiver {
            events: Mutex::new(events_rx),
            tally,
            stop,
            retry,
            url,
        })
    }

    /// Tells the source whether its text is on air or in preview (clause 10).
    pub fn set_tally(&self, tally: Tally) {
        self.tally.send_replace(Some(tally));
    }

    /// Connects again now, after the connection was lost: also after an error that stopped
    /// reconnecting (clause 11.3), or when `reconnect` is off. Has no effect while connected.
    pub fn reconnect(&self) {
        self.retry.send_replace(());
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

    async fn connected(
        &mut self,
        mut peer: Connection,
        hello: Hello,
        _: watch::Receiver<Stop>,
        progress: &mut Progress,
    ) -> Ended {
        self.seen.start_session(&hello.session);
        let mut pending = Some(Pending {
            hello,
            announce: None,
        });
        self.tally.mark_changed(); // send the current tally, if any, on every new connection
        let mut tick = tokio::time::interval(HEARTBEAT_INTERVAL);
        loop {
            let outcome = tokio::select! {
                message = peer.recv() => message.map(|m| self.handle(m, &mut pending, progress)),
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

    fn error(&mut self, error: ErrorMessage) {
        let _ = self.events.send(ReceiverEvent::Error {
            code: error.code,
            message: error.message,
        });
    }

    fn disconnected(&mut self, reason: String) -> bool {
        self.events.send(ReceiverEvent::Disconnected { reason }).is_ok()
    }
}

/// A connection the source has not confirmed yet, and the announcement held until it does.
struct Pending {
    hello: Hello,
    announce: Option<Vec<TypeDescriptor>>,
}

impl Link {
    fn handle(&mut self, message: Message, pending: &mut Option<Pending>, progress: &mut Progress) {
        match message {
            Message::Update(update) => {
                if let Some(Pending { hello, announce }) = pending.take() {
                    // The first update confirms the subscription (clause 8.3).
                    progress.confirmed = true;
                    self.send(ReceiverEvent::Connected {
                        source: hello.source,
                        session: hello.session,
                        channels: hello.channels,
                    });
                    if let Some(types) = announce {
                        self.send(ReceiverEvent::Announce(types));
                    }
                }
                if self.seen.is_new(&update) {
                    self.send(ReceiverEvent::Update(Update {
                        fields: wire::normalize_fields(update.fields),
                        ..update
                    }));
                }
            }
            Message::Announce(a) => match pending {
                Some(pending) => pending.announce = Some(a.types),
                None => self.send(ReceiverEvent::Announce(a.types)),
            },
            Message::Error(e) => {
                progress.error(&e);
                self.error(e);
            }
            _ => {}
        }
    }

    fn send(&self, event: ReceiverEvent) {
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
