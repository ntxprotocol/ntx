//! The client side shared by receivers and publishers: addresses, connecting, `hello`, and
//! reconnecting with backoff.

use std::time::Duration;

use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;

use crate::protocol::{Endpoint, ErrorCode, Hello, Message, PairingProof, Publish, Subscribe};
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
    ) -> Settings {
        Settings {
            role,
            url,
            me: Endpoint { id, name },
            channels,
            pairing_code,
            reconnect,
        }
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

/// Connects, waits for `hello` and answers it (clauses 8 and 14.2).
async fn handshake(settings: &Settings) -> std::result::Result<(Connection, Hello), Ended> {
    let connect = async {
        let (mut peer, hello, proof) = connect(&settings.url, settings.pairing_code.as_deref()).await?;
        peer.send(&settings.answer(proof)).await?;
        peer.handshake_done();
        Ok((peer, hello))
    };
    tokio::time::timeout(HANDSHAKE_TIMEOUT, connect)
        .await
        .unwrap_or_else(|_| Err(Ended("no hello".into())))
}

async fn connect(
    url: &str,
    pairing_code: Option<&str>,
) -> std::result::Result<(Connection, Hello, Option<PairingProof>), Ended> {
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
        Message::Error(e) => {
            let message = e.message.unwrap_or_default();
            return Err(Ended(format!("refused: {} {message}", e.code.as_str())));
        }
        _ => return Err(peer.reject(ErrorCode::BadMessage, "expected hello".into()).await),
    };
    if let Err((code, reason)) = wire::check_version(&hello.ntx) {
        return Err(peer.reject(code, reason).await);
    }
    let proof = hello
        .pairing
        .as_ref()
        .zip(pairing_code)
        .map(|(challenge, code)| PairingProof {
            proof: pairing::proof(code, &challenge.nonce),
        });
    Ok((peer, hello, proof))
}

/// A receiver or publisher that [`keep_connected`] connects again and again.
pub(crate) trait Client: Send {
    fn settings(&self) -> &Settings;

    /// Serves one connection after its handshake. Returns why it ended.
    fn connected(&mut self, peer: Connection, hello: Hello) -> impl Future<Output = Ended> + Send;

    /// Reports a lost connection. Returns whether anyone still listens.
    fn disconnected(&mut self, reason: String) -> bool;
}

/// Connects, and reconnects with backoff (clause 11.3), until stopped.
pub(crate) async fn keep_connected(mut client: impl Client, mut stop: watch::Receiver<bool>) {
    let mut backoff = Duration::from_secs(1);
    loop {
        let (Ended(reason), handshook) = tokio::select! {
            ended = connection(&mut client) => ended,
            _ = stop.changed() => return,
        };
        if !client.disconnected(reason) || !client.settings().reconnect {
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

/// One connection. Returns why it ended and whether its handshake had finished.
async fn connection(client: &mut impl Client) -> (Ended, bool) {
    match handshake(client.settings()).await {
        Ok((peer, hello)) => (client.connected(peer, hello).await, true),
        Err(ended) => (ended, false),
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
