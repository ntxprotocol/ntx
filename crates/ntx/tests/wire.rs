//! Raw protocol checks: a hand-driven WebSocket client against a real source, and a hand-driven
//! fake source against a real receiver. These cover the error paths of clauses 7, 8, 11, 12 and 18.

mod common;

use std::time::Duration;

use common::{
    Raw, WAIT, accept, fake_server, raw_connect, read, read_close, read_json, receiver_config, send_json,
    source, wait_for, write,
};
use ntx::protocol::ErrorCode;
use ntx::{Receiver, ReceiverEvent, SourceEvent};
use serde_json::{Value, json};
use tungstenite::Message as WsMessage;

fn subscribe(ntx: &str) -> Value {
    json!({"op": "subscribe", "ntx": ntx, "receiver": {"id": "raw", "name": "RAW (test)"}, "channels": ["program", "nope"]})
}

/// Connects and completes the handshake, returning the socket after the state on connect.
fn subscribed(s: &ntx::Source) -> Raw {
    let mut ws = raw_connect(&format!("ws://{}/", s.local_addr()));
    assert_eq!(read_json(&mut ws)["op"], "hello");
    send_json(&mut ws, &subscribe("0.1"));
    let state = read_json(&mut ws);
    assert_eq!(
        (state["op"].as_str(), state["channel"].as_str()),
        (Some("update"), Some("program"))
    );
    ws
}

#[test]
fn hello_and_unknown_channels() {
    let s = source(None);
    let mut ws = raw_connect(&format!("ws://{}/", s.local_addr()));
    let hello = read_json(&mut ws);
    assert_eq!(hello["ntx"], "0.1");
    assert_eq!(hello["source"]["name"], "TEST (source)");
    assert_eq!(hello["channels"], json!(["program", "preview"]));
    assert!(hello.get("pairing").is_none());
    // A ping before subscribing is answered too.
    send_json(&mut ws, &json!({"op": "ping", "id": "early"}));
    assert_eq!(read_json(&mut ws), json!({"op": "pong", "id": "early"}));
    send_json(&mut ws, &subscribe("0.1"));
    // "nope" is ignored: only the program state arrives.
    assert_eq!(read_json(&mut ws)["channel"], "program");
    s.update_text("program", "next").unwrap();
    assert_eq!(read_json(&mut ws)["fields"]["text"], "next");
}

#[test]
fn wrong_path_is_refused() {
    let s = source(None);
    let err = tungstenite::connect(format!("ws://{}/other", s.local_addr())).unwrap_err();
    assert!(err.to_string().contains("404"), "{err}");
}

#[test]
fn handshake_errors_close_with_the_right_codes() {
    let s = source(None);
    let cases = [
        (subscribe("0.2"), "unsupported_version", 4001),
        // Another version whose subscribe does not even pass 0.1 checks.
        (
            json!({"op": "subscribe", "ntx": "9.9"}),
            "unsupported_version",
            4001,
        ),
        (json!({"op": "subscribe"}), "bad_message", 4000),
    ];
    for (message, code, close) in cases {
        let mut ws = raw_connect(&format!("ws://{}/", s.local_addr()));
        read_json(&mut ws);
        send_json(&mut ws, &message);
        assert_eq!(read_json(&mut ws)["code"], code);
        assert_eq!(read_close(&mut ws), close);
    }
}

#[test]
fn pairing_required_without_proof() {
    let s = source(Some("4321"));
    let mut ws = raw_connect(&format!("ws://{}/", s.local_addr()));
    let hello = read_json(&mut ws);
    assert_eq!(hello["pairing"]["nonce"].as_str().map(str::len), Some(32));
    send_json(&mut ws, &subscribe("0.1"));
    assert_eq!(read_json(&mut ws)["code"], "pairing_required");
    assert_eq!(read_close(&mut ws), 4003);
}

#[test]
fn bad_messages_after_the_handshake() {
    let s = source(None);
    let mut ws = subscribed(&s);

    // Unknown ops and binary frames are ignored; invalid JSON is answered but not fatal.
    send_json(&mut ws, &json!({"op": "com.example.future"}));
    ws.send(WsMessage::binary(vec![1, 2, 3])).unwrap();
    ws.send(WsMessage::text("{not json")).unwrap();
    assert_eq!(read_json(&mut ws)["code"], "bad_message");
    send_json(&mut ws, &json!({"op": "error", "code": "bad_message"}));
    send_json(&mut ws, &json!({"op": "ping", "id": "7"}));
    assert_eq!(read_json(&mut ws), json!({"op": "pong", "id": "7"}));

    // Oversized messages end the connection (clause 18.1).
    ws.send(WsMessage::text("x".repeat(ntx::MAX_MESSAGE_BYTES + 1)))
        .unwrap();
    assert_eq!(read_json(&mut ws)["code"], "too_large");
    assert_eq!(read_close(&mut ws), 4009);
    wait_for(
        || s.wait_event(WAIT),
        |e| matches!(e, SourceEvent::ReceiverDisconnected { .. }).then_some(()),
    );
}

#[test]
fn source_closes_when_dropped() {
    let s = source(None);
    let mut ws = subscribed(&s);
    drop(s);
    assert_eq!(read_close(&mut ws), 1005);
}

fn hello(ntx: &str) -> Value {
    json!({"op": "hello", "ntx": ntx, "source": {"id": "fake", "name": "FAKE"}, "channels": ["program"], "session": "s1"})
}

fn one_shot_receiver(url: &str) -> Receiver {
    let mut config = receiver_config(&["program"], None);
    config.reconnect = false;
    Receiver::connect(url, config).unwrap()
}

fn disconnect_reason(r: &Receiver) -> String {
    wait_for(
        || r.wait_event(WAIT),
        |e| match e {
            ReceiverEvent::Disconnected { reason } => Some(reason),
            _ => None,
        },
    )
}

#[test]
fn receiver_rejects_other_versions_and_bad_hellos() {
    let cases = [
        (hello("0.2"), Some("unsupported_version")),
        (json!({"op": "hello", "ntx": "7.0"}), Some("unsupported_version")),
        (
            json!({"op": "tally", "program": true, "preview": false}),
            Some("bad_message"),
        ),
        (json!({"op": "error", "code": "not_allowed"}), None),
    ];
    for (first, answer) in cases {
        let (listener, url) = fake_server("/");
        let r = one_shot_receiver(&url);
        let mut ws = accept(&listener);
        write(&mut ws, &first);
        if let Some(code) = answer {
            assert_eq!(read(&mut ws)["code"], code);
        }
        let reason = disconnect_reason(&r);
        assert_ne!(reason, "");
    }
}

#[test]
fn receiver_filters_repeats_and_reports_everything_else() {
    let (listener, url) = fake_server("/");
    let r = one_shot_receiver(&url);
    let mut ws = accept(&listener);
    write(&mut ws, &hello("0.1"));
    assert_eq!(read(&mut ws)["op"], "subscribe");

    let update = |seq: u64, text: &str| json!({"op": "update", "channel": "program", "seq": seq, "type": "ntx.text", "fields": {"text": text}});
    write(&mut ws, &update(3, "three"));
    write(&mut ws, &update(3, "three again"));
    write(&mut ws, &update(2, "older"));
    write(&mut ws, &json!({"op": "com.example.future"}));
    write(
        &mut ws,
        &json!({"op": "error", "code": "bad_message", "message": "test"}),
    );
    write(&mut ws, &json!({"op": "ping", "id": "p"}));
    assert_eq!(read(&mut ws), json!({"op": "pong", "id": "p"}));
    ws.send(WsMessage::text("{not json")).unwrap();
    assert_eq!(read(&mut ws)["code"], "bad_message");
    ws.send(WsMessage::text("x".repeat(ntx::MAX_MESSAGE_BYTES + 1)))
        .unwrap();
    assert_eq!(read(&mut ws)["code"], "too_large");

    let mut events = Vec::new();
    while let Some(event) = r.wait_event(WAIT) {
        let done = matches!(event, ReceiverEvent::Disconnected { .. });
        events.push(event);
        if done {
            break;
        }
    }
    let updates: Vec<_> = events
        .iter()
        .filter(|e| matches!(e, ReceiverEvent::Update(_)))
        .collect();
    assert_eq!(updates.len(), 1, "{events:?}");
    assert!(events.iter().any(|e| matches!(
        e,
        ReceiverEvent::Error {
            code: ErrorCode::BadMessage,
            ..
        }
    )));
}

#[test]
fn receiver_pings_a_quiet_source_and_gives_up_on_a_silent_one() {
    let (listener, url) = fake_server("/");
    let r = one_shot_receiver(&url);
    let mut ws = accept(&listener);
    write(&mut ws, &hello("0.1"));
    read(&mut ws); // subscribe
    // Say nothing: the receiver pings after 5 seconds (clause 11.1).
    let ping = read(&mut ws);
    assert_eq!(ping["op"], "ping");
    // Keep ignoring it: the receiver gives up after 15 seconds (clause 11.2).
    let reason = wait_for(
        || r.wait_event(Duration::from_secs(20)),
        |e| match e {
            ReceiverEvent::Disconnected { reason } => Some(reason),
            _ => None,
        },
    );
    assert!(reason.contains("no answer"), "{reason}");
}

#[test]
fn receiver_reports_a_refused_connection() {
    let (listener, url) = fake_server("/");
    drop(listener);
    let r = one_shot_receiver(&url);
    assert_ne!(disconnect_reason(&r), "");
}
