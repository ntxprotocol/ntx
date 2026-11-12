//! The client side shared by receivers and publishers: addresses, connecting, `hello`, and
//! reconnecting with backoff.

use std::time::Duration;

use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;

use crate::protocol::{
    Endpoint, ErrorCode, ErrorMessage, Hello, Message, PairingProof, Publish, Subscribe, check_endpoint,
};
use crate::wire::{self, Ended, HANDSHAKE_TIMEOUT, Peer};
use crate::{DEFAULT_PORT, Error, PROTOCOL_VERSION, Result, SUBPROTOCOL, pairing};

pub(crate) type Connection = Peer<MaybeTlsStream<TcpStream>>;

/// Turns `host`, `host:port` or a `ws://` / `wss://` URL into a URL (clause 6.4). Host forms get
/// the default port and `path`.
pub(crate) fn normalize_url(address: &str, path: &str) -> Result<String> {
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
        format!("ws://{host}{path}")
    } else {
        format!("ws://{host}:{DEFAULT_PORT}{path}")
    })
}

/// How a client answers `hello`: a receiver with `subscribe` (clause 8.3), a publisher with
/// `publish` (clause 14.2).
pub(crate) enum Role {
    Receiver,
    Publisher,
}

/// What receivers and publishers have in common: where they connect, who they are and what they
/// want.
pub(crate) struct Settings {
    role: Role,
    url: String,
    me: Endpoint,
    channels: Vec<String>,
    pairing_code: Option<String>,
    reconnect: bool,
}

impl Settings {
    pub(crate) fn new(
        role: Role,
        url: String,
        id: String,
        name: String,
        channels: Vec<String>,
        pairing_code: Option<String>,
        reconnect: bool,
    ) -> Result<Settings> {
        let me = Endpoint { id, name };
        check_endpoint(&me)?;
        Ok(Settings {
            role,
            url,
            me,
            channels,
            pairing_code,
            reconnect,
        })
    }

    /// The answer to `hello`, with the pairing proof if one was asked for. A receiver asks only
    /// for channels the source has, and for at least one (clause 8.3): `None` when the source
    /// has none of them.
    fn answer(&self, hello: &Hello, pairing: Option<PairingProof>) -> Option<Message> {
        let (ntx, me) = (PROTOCOL_VERSION.to_string(), self.me.clone());
        Some(match self.role {
            Role::Receiver => {
                let channels: Vec<_> = self
                    .channels
                    .iter()
                    .filter(|c| hello.channels.contains(c))
                    .cloned()
                    .collect();
                if channels.is_empty() {
                    return None;
                }
                Message::Subscribe(Subscribe {
                    ntx,
                    receiver: me,
                    channels: Some(channels),
                    pairing,
                })
            }
            Role::Publisher => Message::Publish(Publish {
                ntx,
                source: me,
                channels: Some(self.channels.clone()),
                pairing,
            }),
        })
    }
}

/// Why a handshake failed.
enum Failed {
    /// The connection ended or could not be made.
    Ended(Ended),
    /// An NTX error ended it: the other side sent `error` instead of `hello` (clause 14.5), or
    /// this side found the versions incompatible or has no pairing code.
    Error(ErrorMessage),
}

impl From<Ended> for Failed {
    fn from(ended: Ended) -> Self {
        Failed::Ended(ended)
    }
}

/// Connects, waits for `hello` and answers it (clauses 8 and 14.2).
async fn handshake(settings: &Settings) -> std::result::Result<(Connection, Hello), Failed> {
    let mut peer = tokio::time::timeout(HANDSHAKE_TIMEOUT, open(&settings.url))
        .await
        .map_err(|_| Ended("no WebSocket upgrade".into()))??;
    let Ok(first) = tokio::time::timeout(HANDSHAKE_TIMEOUT, peer.recv()).await else {
        let reason = "no hello within 10 seconds";
        peer.close(Some((wire::TIMEOUT_CLOSE, reason))).await;
        return Err(Ended(reason.into()).into());
    };
    let (hello, proof) = greeted(&mut peer, first?, settings.pairing_code.as_deref()).await?;
    let Some(answer) = settings.answer(&hello, proof) else {
        let reason = "the source has none of the requested channels";
        peer.close(Some((1000, reason))).await;
        return Err(Ended(reason.into()).into());
    };
    peer.send(&answer).await?;
    peer.handshake_done();
    Ok((peer, hello))
}

/// Opens the WebSocket connection, offering the `ntx` subprotocol (clause 5.2).
async fn open(url: &str) -> std::result::Result<Connection, Ended> {
    let mut request = url.into_client_request().map_err(|e| Ended(e.to_string()))?;
    request
        .headers_mut()
        .insert("sec-websocket-protocol", HeaderValue::from_static(SUBPROTOCOL));
    let (ws, _) = tokio_tungstenite::connect_async_with_config(request, Some(wire::ws_config()), true)
        .await
        .map_err(|e| Ended(e.to_string()))?;
    Ok(Peer::new(ws, "hello"))
}

/// Checks the first message, which should be `hello`, and computes the pairing proof.
async fn greeted(
    peer: &mut Connection,
    first: Message,
    pairing_code: Option<&str>,
) -> std::result::Result<(Hello, Option<PairingProof>), Failed> {
    let hello = match first {
        Message::Hello(hello) => hello,
        Message::Error(e) => return Err(Failed::Error(e)),
        _ => {
            return Err(peer
                .reject(ErrorCode::BadMessage, "expected hello".into())
                .await
                .into());
        }
    };
    if let Err((code, reason)) = wire::check_version(&hello.ntx) {
        let _ = peer.reject(code, reason.clone()).await;
        return Err(Failed::Error(ErrorMessage {
            code,
            message: Some(reason),
        }));
    }
    let proof = match (&hello.pairing, pairing_code) {
        (None, _) => None,
        (Some(challenge), Some(code)) => Some(PairingProof {
            proof: Some(pairing::proof(code, &challenge.nonce)),
        }),
        (Some(_), None) => {
            // Give up rather than send a `subscribe` that must fail (clauses 8.3, 12.5).
            let reason = "a pairing code is needed";
            peer.close(Some((ErrorCode::PairingRequired.close_code(), reason)))
                .await;
            return Err(Failed::Error(ErrorMessage {
                code: ErrorCode::PairingRequired,
                message: Some(reason.into()),
            }));
        }
    };
    Ok((hello, proof))
}

/// What the owner of a receiver or publisher wants from its background task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stop {
    /// Keep connected.
    Run,
    /// Deliver what has not been sent yet, close cleanly, then stop.
    Flush,
    /// Stop at once.
    Now,
}

/// What one connection achieved, for the reconnect rules (clause 11.3).
#[derive(Default)]
pub(crate) struct Progress {
    /// The other side confirmed the connection, so the wait goes back to 1 second.
    pub(crate) confirmed: bool,
    /// An error that reconnecting cannot fix: wait until the app asks to try again.
    needs_user: bool,
}

impl Progress {
    /// Notes an error the other side sent.
    pub(crate) fn error(&mut self, error: &ErrorMessage) {
        self.needs_user |= error.code.needs_user();
    }
}

/// The longest wait between attempts (clause 11.3).
pub(crate) const MAX_WAIT: Duration = Duration::from_secs(10);

/// A receiver or publisher that [`keep_connected`] connects again and again.
pub(crate) trait Client: Send {
    fn settings(&self) -> &Settings;

    /// Serves one connection after its handshake. Returns why it ended. `stop` changes when the
    /// owner wants to stop; a connection that sees [`Stop::Flush`] finishes cleanly. Errors
    /// from the other side and confirmation go into `progress`.
    fn connected(
        &mut self,
        peer: Connection,
        hello: Hello,
        stop: watch::Receiver<Stop>,
        progress: &mut Progress,
    ) -> impl Future<Output = Ended> + Send;

    /// Reports an error that ended the handshake.
    fn error(&mut self, error: ErrorMessage);

    /// Reports a lost connection. Returns whether to go on.
    fn disconnected(&mut self, reason: String) -> bool;
}

/// Connects, and reconnects with backoff (clause 11.3), until stopped. After an error that
/// needs the user, or when reconnecting is off, it waits until `retry` changes.
pub(crate) async fn keep_connected(
    mut client: impl Client,
    mut stop: watch::Receiver<Stop>,
    mut retry: watch::Receiver<()>,
) {
    let mut backoff = Duration::from_secs(1);
    loop {
        let mut progress = Progress::default();
        let Ended(reason) = tokio::select! {
            ended = connection(&mut client, stop.clone(), &mut progress) => ended,
            () = until(&mut stop, Stop::Now) => return,
        };
        // Only a request made after this disconnect counts.
        retry.mark_unchanged();
        if !client.disconnected(reason) || *stop.borrow() != Stop::Run {
            return;
        }
        if progress.confirmed {
            backoff = Duration::from_secs(1);
        }
        if progress.needs_user || !client.settings().reconnect {
            tokio::select! {
                changed = retry.changed() => if changed.is_err() { return },
                () = until(&mut stop, Stop::Flush) => return,
            }
            continue;
        }
        tokio::select! {
            () = tokio::time::sleep(backoff) => backoff = (backoff * 2).min(MAX_WAIT),
            changed = retry.changed() => if changed.is_err() { return },
            // Flushing: try once more at once, then stop (checked above).
            () = until(&mut stop, Stop::Flush) => if *stop.borrow() == Stop::Now { return },
        }
    }
}

/// Waits until the owner asks for `at_least` (`Flush` also matches `Now`) or goes away.
async fn until(stop: &mut watch::Receiver<Stop>, at_least: Stop) {
    let _ = stop.wait_for(|s| *s == Stop::Now || *s == at_least).await;
}

/// One connection. Returns why it ended.
async fn connection(client: &mut impl Client, stop: watch::Receiver<Stop>, progress: &mut Progress) -> Ended {
    match handshake(client.settings()).await {
        Ok((peer, hello)) => client.connected(peer, hello, stop, progress).await,
        Err(Failed::Ended(ended)) => ended,
        Err(Failed::Error(error)) => {
            let reason = format!(
                "{} {}",
                error.code.as_str(),
                error.message.as_deref().unwrap_or_default()
            );
            progress.error(&error);
            client.error(error);
            Ended(reason)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses() {
        assert_eq!(
            normalize_url("192.168.1.20", "/").unwrap(),
            "ws://192.168.1.20:7686/"
        );
        assert_eq!(
            normalize_url(" pc.local:9000 ", "/").unwrap(),
            "ws://pc.local:9000/"
        );
        assert_eq!(normalize_url("ws://pc:1/a", "/").unwrap(), "ws://pc:1/a");
        assert_eq!(normalize_url("wss://pc/", "/").unwrap(), "wss://pc/");
        assert_eq!(normalize_url("fe80::1", "/").unwrap(), "ws://[fe80::1]:7686/");
        assert_eq!(normalize_url("[fe80::1]", "/").unwrap(), "ws://[fe80::1]:7686/");
        assert_eq!(normalize_url("[fe80::1]:9", "/").unwrap(), "ws://[fe80::1]:9/");
        assert_eq!(
            normalize_url("localhost", "/publish").unwrap(),
            "ws://localhost:7686/publish"
        );
        for bad in ["http://x", "", "a b"] {
            assert!(normalize_url(bad, "/").is_err(), "{bad}");
        }
    }
}
