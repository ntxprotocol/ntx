use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use tokio::sync::{mpsc as tmpsc, watch};
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::client::{self, Client, Connection, Role, Settings, Stop};
use crate::protocol::{Endpoint, ErrorCode, ErrorMessage, Fields, Hello, Message, Tally, TypeDescriptor};
use crate::runtime::runtime;
use crate::store::{Store, text_shortcuts};
use crate::wire::{self, Ended, lock};
use crate::{PROGRAM, Result};

/// The usual address of a relay: the NTX app on the same computer (clause 14.1).
pub const DEFAULT_RELAY_URL: &str = "ws://localhost:7686/publish";

/// Settings for a [`Publisher`].
#[derive(Debug, Clone)]
pub struct PublisherConfig {
    /// Name of the published source, e.g. `Song Board`. The relay adds its machine name.
    pub name: String,
    /// Stable id of the published source. Defaults to a new random UUID.
    pub id: String,
    /// Channels to publish. Must include `program`.
    pub channels: Vec<String>,
    /// Pairing code, if the relay needs one.
    pub pairing_code: Option<String>,
    /// Reconnect automatically after the connection is lost. Defaults to `true`.
    pub reconnect: bool,
}

impl PublisherConfig {
    /// Default settings with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        PublisherConfig {
            name: name.into(),
            id: uuid::Uuid::new_v4().to_string(),
            channels: vec![PROGRAM.to_string()],
            pairing_code: None,
            reconnect: true,
        }
    }
}

/// Something that happened to a [`Publisher`].
///
/// Serialises as `{"event": "tally", "data": {...}}`.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(tag = "event", content = "data", rename_all = "snake_case")]
pub enum PublisherEvent {
    /// The handshake with the relay finished and the current text has been sent again. The relay
    /// checks the pairing code after this, so an [`PublisherEvent::Error`] may still follow.
    Connected {
        /// The relay's identity.
        relay: Endpoint,
        /// The relay's session for this connection.
        session: String,
    },
    /// The combined tally of the receivers showing the published source changed (clause 14.3).
    Tally(Tally),
    /// The relay reported an error. After `not_allowed` the publisher does not reconnect: the
    /// relay refused it (clause 14.5).
    Error {
        /// What went wrong.
        code: ErrorCode,
        /// Optional text for logs.
        message: Option<String>,
    },
    /// The connection was lost, refused or could not be made. The publisher retries if
    /// `reconnect` is on.
    Disconnected {
        /// Why, for logs and people.
        reason: String,
    },
}

/// How long dropping a [`Publisher`] waits to deliver text the relay has not received yet.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(3);

/// An NTX publisher: sends text through a relay, which serves it to the network as a source
/// (clause 14). Use it where a [`Source`](crate::Source) cannot accept connections.
///
/// Dropping a publisher first delivers text the relay has not received yet, waiting up to 3
/// seconds, so a short-lived program does not lose its last update.
pub struct Publisher {
    state: Arc<Mutex<State>>,
    events: Mutex<mpsc::Receiver<PublisherEvent>>,
    stop: watch::Sender<Stop>,
    /// Disconnects when the background task ends.
    done: Mutex<mpsc::Receiver<()>>,
    url: String,
}

struct State {
    store: Store,
    /// The current connection's queue, once the handshake finished.
    out: Option<tmpsc::Sender<WsMessage>>,
    /// Some text has not reached the relay yet.
    unsent: bool,
}

impl Publisher {
    /// Connects to a relay. `address` is a `ws://` URL, `host` or `host:port`; host forms use
    /// the path `/publish`. Connecting happens in the background; watch the events.
    ///
    /// # Errors
    ///
    /// Fails if `address` is not a valid address or the channels lack `program`.
    pub fn connect(address: &str, config: PublisherConfig) -> Result<Publisher> {
        let url = client::normalize_url(address, "/publish")?;
        let state = Arc::new(Mutex::new(State {
            store: Store::new(config.channels.clone())?,
            out: None,
            unsent: false,
        }));
        let settings = Settings::new(
            Role::Publisher,
            url.clone(),
            config.id,
            config.name,
            config.channels,
            config.pairing_code,
            config.reconnect,
        )?;
        let (events_tx, events_rx) = mpsc::channel();
        let (stop, stop_rx) = watch::channel(Stop::Run);
        let (done_tx, done) = mpsc::channel();
        let link = Link {
            settings,
            state: state.clone(),
            events: events_tx,
            not_allowed: false,
        };
        runtime().spawn(async move {
            client::keep_connected(link, stop_rx).await;
            drop(done_tx);
        });
        Ok(Publisher {
            state,
            events: Mutex::new(events_rx),
            stop,
            done: Mutex::new(done),
            url,
        })
    }

    /// Replaces the item of a channel and sends it to the relay (clause 9.3). Strings in
    /// `fields` are normalised (clause 17). Returns the sequence number. While the relay is not
    /// connected the item is kept and sent on the next connection.
    ///
    /// # Errors
    ///
    /// Fails for an unknown channel, an invalid type name, or a message over the size limit.
    pub fn update(&self, channel: &str, type_name: &str, fields: Fields) -> Result<u64> {
        let mut state = lock(&self.state);
        let (seq, frame) = state.store.update(channel, type_name, fields)?;
        state.send(frame);
        Ok(seq)
    }

    text_shortcuts!();

    /// Sets the announced types and sends them to the relay (clause 15.5).
    ///
    /// # Errors
    ///
    /// Fails for an invalid type descriptor or a message over the size limit.
    pub fn announce(&self, types: Vec<TypeDescriptor>) -> Result<()> {
        let mut state = lock(&self.state);
        let frame = state.store.announce(types)?;
        state.send(frame);
        Ok(())
    }

    /// Takes the next event, if any.
    pub fn poll_event(&self) -> Option<PublisherEvent> {
        lock(&self.events).try_recv().ok()
    }

    /// Waits up to `timeout` for the next event.
    pub fn wait_event(&self, timeout: Duration) -> Option<PublisherEvent> {
        lock(&self.events).recv_timeout(timeout).ok()
    }

    /// The WebSocket URL of the relay.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        let flush = {
            let state = lock(&self.state);
            state.unsent || state.out.is_some()
        };
        if flush {
            let _ = self.stop.send(Stop::Flush);
            let _ = lock(&self.done).recv_timeout(FLUSH_TIMEOUT);
        }
        let _ = self.stop.send(Stop::Now);
    }
}

impl State {
    fn send(&mut self, frame: WsMessage) {
        // Without a connection, or when the relay fell too far behind (which ends the
        // connection), the text is sent with the whole state on the next connection.
        if !wire::enqueue(&mut self.out, frame) {
            self.unsent = true;
        }
    }
}

/// The background task behind a [`Publisher`].
struct Link {
    settings: Settings,
    state: Arc<Mutex<State>>,
    events: mpsc::Sender<PublisherEvent>,
    /// The relay refused this publisher (clause 14.5); do not reconnect.
    not_allowed: bool,
}

impl Link {
    fn error(&mut self, error: ErrorMessage) {
        self.not_allowed |= error.code == ErrorCode::NotAllowed;
        let _ = self.events.send(PublisherEvent::Error {
            code: error.code,
            message: error.message,
        });
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
    ) -> Ended {
        // The relay starts a new session for every connection (clause 14.4), so send the whole
        // state again, as a source does on connect (clause 9.2). Under one lock, so no update is
        // lost or sent twice.
        let (out, mut out_rx) = wire::queue();
        {
            let mut state = lock(&self.state);
            let frames = state.store.state(state.store.channels());
            state.out = Some(out);
            state.unsent = false;
            for frame in frames {
                state.send(frame);
            }
        }
        let _ = self.events.send(PublisherEvent::Connected {
            relay: hello.source,
            session: hello.session,
        });
        let stopping = async move {
            let _ = stop.wait_for(|s| *s != Stop::Run).await;
            None
        };
        let ended = wire::pump(&mut peer, &mut out_rx, stopping, |message| match message {
            Message::Tally(tally) => drop(self.events.send(PublisherEvent::Tally(tally))),
            Message::Error(e) => self.error(e),
            _ => {}
        })
        .await;
        {
            let mut state = lock(&self.state);
            state.out = None;
            state.unsent |= !out_rx.is_empty();
        }
        // Let the relay read everything before the socket goes away.
        peer.finish().await;
        ended
    }

    fn refused(&mut self, error: ErrorMessage) {
        self.error(error);
    }

    fn disconnected(&mut self, reason: String) -> bool {
        self.events.send(PublisherEvent::Disconnected { reason }).is_ok() && !self.not_allowed
    }
}
