use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::sync::{mpsc as tmpsc, watch};
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::protocol::{Endpoint, ErrorCode, Hello, Message, Publish, Tally};
use crate::runtime::runtime;
use crate::wire::{self, Ended, lock};
use crate::{DEFAULT_PORT, PROGRAM, PROTOCOL_VERSION, Result, Source, SourceConfig, display_name};

/// Settings for a [`Relay`].
#[derive(Debug, Clone)]
pub struct RelayConfig {
    /// Human-readable name sent to publishers, e.g. `CHURCH-PC (Cueword)`.
    pub name: String,
    /// Stable id. Defaults to a new random UUID.
    pub id: String,
    /// Address to listen on for publishers. Defaults to `127.0.0.1`: publishers are normally on
    /// the same computer (clause 14.1). The published sources always listen on all interfaces.
    pub bind: IpAddr,
    /// TCP port. `0` picks any free port. Defaults to 7686.
    pub port: u16,
    /// WebSocket path. Defaults to `/publish`.
    pub path: String,
    /// Require this pairing code from publishers.
    pub pairing_code: Option<String>,
    /// Web page origins allowed to publish, e.g. `https://example.com` (clause 14.5).
    /// Publishers that send no `Origin` header (native programs) are always allowed.
    pub allowed_origins: Vec<String>,
    /// Advertise the published sources with mDNS. Defaults to `true`.
    pub advertise: bool,
}

impl RelayConfig {
    /// Default settings with the given display name.
    pub fn new(name: impl Into<String>) -> Self {
        RelayConfig {
            name: name.into(),
            id: uuid::Uuid::new_v4().to_string(),
            bind: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: DEFAULT_PORT,
            path: "/publish".to_string(),
            pairing_code: None,
            allowed_origins: Vec::new(),
            advertise: true,
        }
    }
}

/// Something that happened to a [`Relay`].
///
/// Serialises as `{"event": "origin_requested", "data": {...}}`.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(tag = "event", content = "data", rename_all = "snake_case")]
pub enum RelayEvent {
    /// A web page from an origin that is not allowed tried to publish and was refused. Ask the
    /// user, and call [`Relay::allow_origin`] if they agree (clause 14.5). Reported once per
    /// origin.
    OriginRequested {
        /// The page's origin, e.g. `https://example.com`.
        origin: String,
    },
    /// A publisher finished the handshake. Its source is now served to the network.
    PublisherConnected {
        /// Number that identifies this connection in later events.
        connection: u64,
        /// The published source, as served: the name has the machine name added (clause 14.4).
        source: Endpoint,
        /// The page's origin, or `None` for a native publisher.
        origin: Option<String>,
        /// The publisher's network address.
        address: SocketAddr,
        /// The TCP port the published source listens on.
        port: u16,
    },
    /// A publisher went away, and its source with it.
    PublisherDisconnected {
        /// The number from [`RelayEvent::PublisherConnected`].
        connection: u64,
    },
}

/// An NTX relay: accepts publishers and serves each one to the network as an ordinary source
/// (clause 14). Publishing is off until a relay is started.
pub struct Relay {
    shared: Arc<Shared>,
    events: Mutex<mpsc::Receiver<RelayEvent>>,
    local_addr: SocketAddr,
    stop: watch::Sender<bool>,
}

struct Shared {
    identity: Endpoint,
    path: String,
    pairing_code: Option<String>,
    advertise: bool,
    /// Allowed origins. Connections watch it, so withdrawing an origin closes them.
    allowed: watch::Sender<HashSet<String>>,
    /// Origins already reported with [`RelayEvent::OriginRequested`].
    requested: Mutex<HashSet<String>>,
    /// The port each published source id last used, so receivers find it again after the
    /// publisher reconnects.
    ports: Mutex<HashMap<String, u16>>,
    next_connection: Mutex<u64>,
    events: mpsc::Sender<RelayEvent>,
}

impl Relay {
    /// Starts listening for publishers.
    ///
    /// # Errors
    ///
    /// Fails if the port cannot be opened.
    pub fn start(config: RelayConfig) -> Result<Relay> {
        let (listener, local_addr) = wire::listen(config.bind, config.port)?;
        let (events_tx, events_rx) = mpsc::channel();
        let shared = Arc::new(Shared {
            identity: Endpoint {
                id: config.id,
                name: config.name,
            },
            path: config.path,
            pairing_code: config.pairing_code,
            advertise: config.advertise,
            allowed: watch::channel(config.allowed_origins.into_iter().collect()).0,
            requested: Mutex::default(),
            ports: Mutex::default(),
            next_connection: Mutex::default(),
            events: events_tx,
        });
        let stop = wire::serve_connections(listener, shared.clone(), serve);
        Ok(Relay {
            shared,
            events: Mutex::new(events_rx),
            local_addr,
            stop,
        })
    }

    /// Allows web pages from `origin` to publish (clause 14.5).
    pub fn allow_origin(&self, origin: &str) {
        self.shared.allowed.send_modify(|allowed| {
            allowed.insert(origin.to_string());
        });
    }

    /// Withdraws permission from `origin` and disconnects its publishers (clause 14.5).
    pub fn revoke_origin(&self, origin: &str) {
        self.shared.allowed.send_modify(|allowed| {
            allowed.remove(origin);
        });
    }

    /// Takes the next event, if any.
    pub fn poll_event(&self) -> Option<RelayEvent> {
        lock(&self.events).try_recv().ok()
    }

    /// Waits up to `timeout` for the next event.
    pub fn wait_event(&self, timeout: Duration) -> Option<RelayEvent> {
        lock(&self.events).recv_timeout(timeout).ok()
    }

    /// The address the relay listens on.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

/// Serves one publisher from WebSocket upgrade to disconnect.
async fn serve(stream: TcpStream, address: SocketAddr, shared: Arc<Shared>, mut stop: watch::Receiver<bool>) {
    let (mut peer, origin) = match wire::accept(stream, &shared.path, "publish").await {
        Ok(accepted) => accepted,
        Err(Ended(e)) => return tracing::debug!("upgrade from {address} failed: {e}"),
    };
    if let Some(origin) = origin.as_ref().filter(|o| !shared.allowed.borrow().contains(*o)) {
        if lock(&shared.requested).insert(origin.clone()) {
            let _ = shared.events.send(RelayEvent::OriginRequested {
                origin: origin.clone(),
            });
        }
        let Ended(reason) = peer
            .reject(
                ErrorCode::NotAllowed,
                format!("origin {origin} is not allowed to publish"),
            )
            .await;
        return tracing::debug!("{reason}");
    }
    let hello = Hello {
        ntx: PROTOCOL_VERSION.to_string(),
        source: shared.identity.clone(),
        channels: vec![PROGRAM.to_string()],
        session: uuid::Uuid::new_v4().simple().to_string(),
        pairing: None,
    };
    let publish = wire::greet(
        &mut peer,
        hello,
        shared.pairing_code.as_deref(),
        |message| match message {
            Message::Publish(p) => Some((p.clone(), p.ntx, p.pairing)),
            _ => None,
        },
    );
    let publish = match publish.await {
        Ok(publish) => publish,
        Err(Ended(reason)) => return tracing::debug!("handshake with {address} failed: {reason}"),
    };
    let mut source = match start_source(&shared, publish).await {
        Ok(source) => source,
        Err(e) => {
            let Ended(reason) = peer.reject(ErrorCode::BadMessage, e.to_string()).await;
            return tracing::warn!("could not serve the publisher at {address}: {reason}");
        }
    };
    source.discard_events(); // the relay watches tally directly
    let connection = {
        let mut next = lock(&shared.next_connection);
        *next += 1;
        *next
    };
    let _ = shared.events.send(RelayEvent::PublisherConnected {
        connection,
        source: source.identity().clone(),
        origin: origin.clone(),
        address,
        port: source.local_addr().port(),
    });

    let (out, mut out_rx) = tmpsc::unbounded_channel();
    runtime().spawn(forward_tally(source.tally(), out.clone()));
    let mut allowed = shared.allowed.subscribe();
    let stopped = async move {
        tokio::select! {
            _ = stop.changed() => {}
            _ = allowed.wait_for(|allowed| origin.as_ref().is_some_and(|o| !allowed.contains(o))) => {}
        }
    };
    let ended = wire::pump(&mut peer, &mut out_rx, stopped, |message| {
        // Forward the publisher's text to the receivers (clause 14.3).
        let forwarded = match message {
            Message::Announce(a) => source.announce(a.types),
            Message::Update(u) => source.update(&u.channel, &u.type_name, u.fields).map(drop),
            Message::Error(e) => {
                tracing::info!("publisher reported {}", e.code.as_str());
                Ok(())
            }
            _ => Ok(()),
        };
        if let Err(e) = forwarded {
            let _ = out.send(WsMessage::text(Message::Error(wire::bad_message(&e)).to_json()));
        }
    })
    .await;
    tracing::debug!("publisher {address} left: {}", ended.0);
    drop(source);
    let _ = shared
        .events
        .send(RelayEvent::PublisherDisconnected { connection });
}

/// Starts the source that serves a publisher (clause 14.4), on the port it had last time if
/// that port becomes free soon, or else on any free port.
async fn start_source(shared: &Shared, publish: Publish) -> Result<Source> {
    let mut config = SourceConfig::new(display_name(&publish.source.name));
    config.id = publish.source.id;
    config.channels = publish.channels.unwrap_or(config.channels);
    config.advertise = shared.advertise;
    let last_port = lock(&shared.ports).get(&config.id).copied();
    let mut source = None;
    if let Some(port) = last_port {
        config.port = port;
        // The publisher's previous source may still be closing that port.
        for _ in 0..10 {
            source = Source::start(config.clone()).ok();
            if source.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    let source = match source {
        Some(source) => source,
        None => Source::start(SourceConfig { port: 0, ..config })?,
    };
    let id = source.identity().id.clone();
    lock(&shared.ports).insert(id, source.local_addr().port());
    Ok(source)
}

/// Sends the combined tally to the publisher whenever it changes (clause 14.3).
async fn forward_tally(mut tally: watch::Receiver<Tally>, out: tmpsc::UnboundedSender<WsMessage>) {
    while tally.changed().await.is_ok() {
        let message = Message::Tally(*tally.borrow_and_update());
        if out.send(WsMessage::text(message.to_json())).is_err() {
            return;
        }
    }
}
