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

    /// The answer to `hello`, with the pairing proof if one was asked for.
    fn answer(&self, pairing: Option<PairingProof>) -> Message {
        let (ntx, me, channels) = (
            PROTOCOL_VERSION.to_string(),
            self.me.clone(),
            Some(self.channels.clone()),
        );
        match self.role {
            Role::Receiver => Message::Subscribe(Subscribe {
                ntx,
                receiver: me,
                channels,
                pairing,
            }),
            Role::Publisher => Message::Publish(Publish {
                ntx,
                source: me,
                channels,
                pairing,
            }),
        }
    }
}

/// Why a handshake failed.
enum Failed {
    /// The connection ended or could not be made.
    Ended(Ended),
    /// The other side sent `error` instead of `hello` (clause 14.5).
    Refused(ErrorMessage),
}

impl From<Ended> for Failed {
    fn from(ended: Ended) -> Self {
        Failed::Ended(ended)
    }
}

/// Connects, waits for `hello` and answers it (clauses 8 and 14.2).
async fn handshake(settings: &Settings) -> std::result::Result<(Connection, Hello), Failed> {
    let connect = async {
        let (mut peer, hello, proof) = connect(&settings.url, settings.pairing_code.as_deref()).await?;
        peer.send(&settings.answer(proof)).await?;
        peer.handshake_done();
        Ok((peer, hello))
    };
    tokio::time::timeout(HANDSHAKE_TIMEOUT, connect)
        .await
        .unwrap_or_else(|_| Err(Ended("no hello".into()).into()))
}

async fn connect(
    url: &str,
    pairing_code: Option<&str>,
) -> std::result::Result<(Connection, Hello, Option<PairingProof>), Failed> {
    let mut request = url.into_client_request().map_err(|e| Ended(e.to_string()))?;
    request
        .headers_mut()
        .insert("sec-websocket-protocol", HeaderValue::from_static(SUBPROTOCOL));
    let (ws, _) = tokio_tungstenite::connect_async_with_config(request, Some(wire::ws_config()), true)
        .await
        .map_err(|e| Ended(e.to_string()))?;
    let mut peer = Peer::new(ws, "hello");
    let hello = match peer.recv().await? {
        Message::Hello(hello) => hello,
        Message::Error(e) => return Err(Failed::Refused(e)),
        _ => {
            return Err(peer
                .reject(ErrorCode::BadMessage, "expected hello".into())
                .await
                .into());
        }
    };
    if let Err((code, reason)) = wire::check_version(&hello.ntx) {
        return Err(peer.reject(code, reason).await.into());
    }
    let proof = hello
        .pairing
        .as_ref()
        .zip(pairing_code)
        .map(|(challenge, code)| PairingProof {
            proof: Some(pairing::proof(code, &challenge.nonce)),
        });
    Ok((peer, hello, proof))
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

/// A receiver or publisher that [`keep_connected`] connects again and again.
pub(crate) trait Client: Send {
    fn settings(&self) -> &Settings;

    /// Serves one connection after its handshake. Returns why it ended. `stop` changes when the
    /// owner wants to stop; a connection that sees [`Stop::Flush`] finishes cleanly.
    fn connected(
        &mut self,
        peer: Connection,
        hello: Hello,
        stop: watch::Receiver<Stop>,
    ) -> impl Future<Output = Ended> + Send;

    /// The other side refused the connection with `error` instead of `hello`.
    fn refused(&mut self, error: ErrorMessage);

    /// Reports a lost connection. Returns whether to connect again.
    fn disconnected(&mut self, reason: String) -> bool;
}

/// Connects, and reconnects with backoff (clause 11.3), until stopped.
pub(crate) async fn keep_connected(mut client: impl Client, mut stop: watch::Receiver<Stop>) {
    let mut backoff = Duration::from_secs(1);
    loop {
        let (Ended(reason), handshook) = tokio::select! {
            ended = connection(&mut client, stop.clone()) => ended,
            () = until(&mut stop, Stop::Now) => return,
        };
        let again = client.disconnected(reason);
        if !again || !client.settings().reconnect || *stop.borrow() != Stop::Run {
            return;
        }
        if handshook {
            backoff = Duration::from_secs(1);
        }
        tokio::select! {
            () = tokio::time::sleep(backoff) => {}
            // Flushing: try once more at once, then stop (checked above).
            () = until(&mut stop, Stop::Flush) => if *stop.borrow() == Stop::Now { return },
        }
        backoff = (backoff * 2).min(Duration::from_secs(10));
    }
}

/// Waits until the owner asks for `at_least` (`Flush` also matches `Now`) or goes away.
async fn until(stop: &mut watch::Receiver<Stop>, at_least: Stop) {
    let _ = stop.wait_for(|s| *s == Stop::Now || *s == at_least).await;
}

/// One connection. Returns why it ended and whether its handshake had finished.
async fn connection(client: &mut impl Client, stop: watch::Receiver<Stop>) -> (Ended, bool) {
    match handshake(client.settings()).await {
        Ok((peer, hello)) => (client.connected(peer, hello, stop).await, true),
        Err(Failed::Ended(ended)) => (ended, false),
        Err(Failed::Refused(error)) => {
            let reason = format!(
                "refused: {} {}",
                error.code.as_str(),
                error.message.as_deref().unwrap_or_default()
            );
            client.refused(error);
            (Ended(reason), false)
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
