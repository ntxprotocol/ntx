use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
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
    events: Arc<Events>,
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
        let events = Arc::new(Events::default());
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
            events: events.clone(),
            tally: tally_rx,
            seen: Seen::default(),
        };
        runtime().spawn(client::keep_connected(link, stop_rx, retry_rx));
        Ok(Receiver {
            events,
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
    #[must_use]
    pub fn poll_event(&self) -> Option<ReceiverEvent> {
        lock(&self.events.queue).pop_front()
    }

    /// Waits up to `timeout` for the next event.
    #[must_use]
    pub fn wait_event(&self, timeout: Duration) -> Option<ReceiverEvent> {
        let queue = lock(&self.events.queue);
        let (mut queue, _) = self
            .events
            .ready
            .wait_timeout_while(queue, timeout, |queue| queue.is_empty())
            .unwrap_or_else(PoisonError::into_inner);
        queue.pop_front()
    }

    /// The WebSocket URL this receiver connects to.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.events.closed.store(true, Ordering::Relaxed);
        // Close the connection properly before the program goes on, or exits.
        let _ = self.stop.send(Stop::Flush);
        wire::wait_until_unwatched(&self.stop);
        let _ = self.stop.send(Stop::Now);
    }
}

/// Most events waiting for the app before new updates replace waiting ones.
const MAX_WAITING: usize = 1024;

/// Events waiting for the app. Once [`MAX_WAITING`] wait, an update replaces the waiting update
/// of its channel, and an announcement the waiting announcement: state is latest-wins, so an app
/// that falls behind skips what it would overwrite anyway, and a source that floods updates
/// cannot grow memory without limit.
#[derive(Default)]
struct Events {
    queue: Mutex<VecDeque<ReceiverEvent>>,
    ready: Condvar,
    /// Set when the [`Receiver`] is dropped.
    closed: AtomicBool,
}

impl Events {
    /// Queues `event`. Returns `false` once the receiver is dropped.
    fn push(&self, event: ReceiverEvent) -> bool {
        if self.closed.load(Ordering::Relaxed) {
            return false;
        }
        let mut queue = lock(&self.queue);
        if queue.len() >= MAX_WAITING {
            let replaces = |waiting: &ReceiverEvent| match (waiting, &event) {
                (ReceiverEvent::Update(old), ReceiverEvent::Update(new)) => old.channel == new.channel,
                (ReceiverEvent::Announce(_), ReceiverEvent::Announce(_)) => true,
                _ => false,
            };
            if let Some(older) = queue.iter().rposition(replaces) {
                queue.remove(older);
            }
        }
        queue.push_back(event);
        self.ready.notify_one();
        true
    }
}

/// The background task behind a [`Receiver`].
struct Link {
    settings: Settings,
    events: Arc<Events>,
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
        mut stop: watch::Receiver<Stop>,
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
                () = client::until(&mut stop, Stop::Flush) => {
                    peer.close(None).await;
                    Err(Ended("stopped".into()))
                }
            };
            if let Err(ended) = outcome {
                return ended;
            }
        }
    }

    fn error(&mut self, error: ErrorMessage) {
        self.events.push(ReceiverEvent::Error {
            code: error.code,
            message: error.message,
        });
    }

    fn disconnected(&mut self, reason: String) -> bool {
        self.events.push(ReceiverEvent::Disconnected { reason })
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
                // Only channels it subscribed to (clause 8.3): a source sending others could
                // otherwise grow `seen` and the application's state without limit.
                if !self.settings.wants(&update.channel) {
                    tracing::debug!("ignored an update for unsubscribed channel {:?}", update.channel);
                } else if self.seen.is_new(&update) {
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
        self.events.push(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn update(channel: &str, seq: u64) -> Update {
        Update {
            channel: channel.into(),
            seq,
            type_name: "ntx.text".into(),
            fields: crate::Fields::new(),
        }
    }

    #[test]
    fn an_app_that_falls_behind_gets_the_latest_state_and_bounded_memory() {
        let events = Events::default();
        events.push(ReceiverEvent::Disconnected {
            reason: "first".into(),
        });
        for seq in 0..3 * MAX_WAITING as u64 {
            events.push(ReceiverEvent::Update(update("program", seq)));
            events.push(ReceiverEvent::Announce(Vec::new()));
        }
        assert!(events.push(ReceiverEvent::Update(update("preview", 1))));
        let queue = lock(&events.queue);
        assert!(queue.len() <= MAX_WAITING + 1, "{}", queue.len());
        // Nothing but superseded updates and announcements is dropped, and order is kept.
        assert!(matches!(&queue[0], ReceiverEvent::Disconnected { .. }));
        let last_program = queue.iter().rev().find_map(|e| match e {
            ReceiverEvent::Update(u) if u.channel == "program" => Some(u.seq),
            _ => None,
        });
        assert_eq!(last_program, Some(3 * MAX_WAITING as u64 - 1));
        assert!(matches!(queue.back(), Some(ReceiverEvent::Update(u)) if u.channel == "preview"));
        drop(queue);
        events.closed.store(true, Ordering::Relaxed);
        assert!(!events.push(ReceiverEvent::Disconnected {
            reason: "late".into()
        }));
    }

    #[test]
    fn sequence_numbers() {
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
