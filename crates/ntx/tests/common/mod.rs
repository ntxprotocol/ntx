//! Helpers shared by the integration tests.
#![allow(dead_code, unreachable_pub)] // each test file uses a different subset

use std::net::{IpAddr, Ipv4Addr, TcpListener, TcpStream};
use std::time::Duration;

use ntx::{
    Publisher, PublisherConfig, Receiver, ReceiverConfig, ReceiverEvent, Relay, RelayConfig, Source,
    SourceConfig, Update,
};
use serde_json::Value;
use tungstenite::client::IntoClientRequest;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message as WsMessage, WebSocket};

pub const WAIT: Duration = Duration::from_secs(5);

/// A source on localhost with `program` and `preview`, not advertised.
pub fn source(pairing: Option<&str>) -> Source {
    let mut config = SourceConfig::new("TEST (source)");
    config.bind = IpAddr::V4(Ipv4Addr::LOCALHOST);
    config.port = 0;
    config.advertise = false;
    config.channels.push(ntx::PREVIEW.into());
    config.pairing_code = pairing.map(String::from);
    Source::start(config).unwrap()
}

/// A relay on localhost on any free port, not advertising, allowing `origins`.
pub fn relay(pairing: Option<&str>, origins: &[&str]) -> Relay {
    let mut config = RelayConfig::new("TEST (relay)");
    config.port = 0;
    config.advertise = false;
    config.pairing_code = pairing.map(String::from);
    config.allowed_origins = origins.iter().map(ToString::to_string).collect();
    Relay::start(config).unwrap()
}

pub fn publisher_config(id: &str, pairing: Option<&str>) -> PublisherConfig {
    let mut config = PublisherConfig::new("Song Board");
    config.id = id.to_string();
    config.channels.push(ntx::PREVIEW.into());
    config.pairing_code = pairing.map(String::from);
    config
}

pub fn publisher(relay: &Relay, id: &str, pairing: Option<&str>) -> Publisher {
    Publisher::connect(
        &format!("ws://{}/publish", relay.local_addr()),
        publisher_config(id, pairing),
    )
    .unwrap()
}

pub fn receiver_config(channels: &[&str], pairing: Option<&str>) -> ReceiverConfig {
    let mut config = ReceiverConfig::new("TEST (receiver)");
    config.channels = channels.iter().map(ToString::to_string).collect();
    config.pairing_code = pairing.map(String::from);
    config
}

pub fn receiver(source: &Source, channels: &[&str], pairing: Option<&str>) -> Receiver {
    Receiver::connect(
        &format!("ws://{}/", source.local_addr()),
        receiver_config(channels, pairing),
    )
    .unwrap()
}

/// A receiver of a source on a localhost port, such as one served by a relay.
pub fn receiver_at(port: u16, channels: &[&str]) -> Receiver {
    Receiver::connect(&format!("127.0.0.1:{port}"), receiver_config(channels, None)).unwrap()
}

/// Waits for the next update, skipping other events.
pub fn next_update(r: &Receiver) -> Update {
    loop {
        match r.wait_event(WAIT).expect("timed out waiting for an update") {
            ReceiverEvent::Update(update) => return update,
            ReceiverEvent::Disconnected { reason } => panic!("disconnected: {reason}"),
            _ => {}
        }
    }
}

/// Waits for the first event that `pick` accepts.
pub fn wait_for<E, T>(mut next: impl FnMut() -> Option<E>, mut pick: impl FnMut(E) -> Option<T>) -> T {
    loop {
        if let Some(found) = pick(next().expect("timed out")) {
            return found;
        }
    }
}

pub fn text(update: &Update) -> &str {
    update.fields.get("text").and_then(Value::as_str).unwrap_or("")
}

pub type Raw = WebSocket<MaybeTlsStream<TcpStream>>;

/// A raw WebSocket client offering the `ntx` subprotocol.
pub fn raw_connect(url: &str) -> Raw {
    raw_connect_from(url, None)
}

/// A raw WebSocket client offering the `ntx` subprotocol, sending `origin` like a web page.
pub fn raw_connect_from(url: &str, origin: Option<&str>) -> Raw {
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("sec-websocket-protocol", "ntx".parse().unwrap());
    if let Some(origin) = origin {
        request.headers_mut().insert("origin", origin.parse().unwrap());
    }
    let (ws, response) = tungstenite::connect(request).unwrap();
    assert_eq!(response.headers().get("sec-websocket-protocol").unwrap(), "ntx");
    if let MaybeTlsStream::Plain(stream) = ws.get_ref() {
        stream.set_read_timeout(Some(WAIT)).unwrap();
    }
    ws
}

pub fn send_json(ws: &mut Raw, value: &Value) {
    ws.send(WsMessage::text(value.to_string())).unwrap();
}

/// Reads the next text message as JSON.
pub fn read_json(ws: &mut Raw) -> Value {
    loop {
        match ws.read().expect("read failed") {
            WsMessage::Text(text) => return serde_json::from_str(&text).unwrap(),
            WsMessage::Close(frame) => panic!("closed: {frame:?}"),
            _ => {}
        }
    }
}

/// Reads until the close frame and returns its code. Waits up to 20 seconds, longer than the
/// spec's timeouts.
pub fn read_close(ws: &mut Raw) -> u16 {
    if let MaybeTlsStream::Plain(stream) = ws.get_ref() {
        stream.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    }
    loop {
        match ws.read() {
            Ok(WsMessage::Close(frame)) => return frame.map_or(1005, |f| f.code.into()),
            Ok(_) => {}
            Err(e) => panic!("no close frame: {e}"),
        }
    }
}

/// A fake source or relay: a listener and the URL with `path` that reaches it.
pub fn fake_server(path: &str) -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("ws://{}{path}", listener.local_addr().unwrap());
    (listener, url)
}

/// Accepts one client on a fake server, confirming the `ntx` subprotocol.
#[allow(clippy::result_large_err)] // the callback's error type comes from tungstenite
pub fn accept(listener: &TcpListener) -> WebSocket<TcpStream> {
    let (stream, _) = listener.accept().unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let confirm_ntx = |_: &tungstenite::handshake::server::Request,
                       mut response: tungstenite::handshake::server::Response| {
        response
            .headers_mut()
            .insert("sec-websocket-protocol", "ntx".parse().unwrap());
        Ok(response)
    };
    tungstenite::accept_hdr(stream, confirm_ntx).unwrap()
}

/// Reads the next text message on a fake server as JSON.
pub fn read(ws: &mut WebSocket<TcpStream>) -> Value {
    loop {
        if let WsMessage::Text(text) = ws.read().unwrap() {
            return serde_json::from_str(&text).unwrap();
        }
    }
}

pub fn write(ws: &mut WebSocket<TcpStream>, value: &Value) {
    ws.send(WsMessage::text(value.to_string())).unwrap();
}
