//! Raw protocol checks for clause 14: a hand-driven publisher against a real relay, and a
//! hand-driven fake relay against a real publisher.

mod common;

use std::time::Duration;

use common::{
    Raw, WAIT, accept, fake_server, publisher_config, raw_connect, raw_connect_from, read, read_close,
    read_json, receiver_at, relay, send_json, wait_for, write,
};
use ntx::protocol::ErrorCode;
use ntx::{Publisher, PublisherEvent, Relay, RelayEvent, Tally};
use serde_json::{Value, json};

fn publish(ntx: &str) -> Value {
    json!({"op": "publish", "ntx": ntx, "source": {"id": "raw-pub", "name": "Raw Board"}, "channels": ["program"]})
}

fn url(relay: &Relay) -> String {
    format!("ws://{}/publish", relay.local_addr())
}

/// Connects (as a web page from `origin`, if given) and publishes; returns the socket and the
/// served source's port.
fn published(relay: &Relay, origin: Option<&str>) -> (Raw, u16) {
    let mut ws = raw_connect_from(&url(relay), origin);
    assert_eq!(read_json(&mut ws)["op"], "hello");
    let mut message = publish("0.1");
    message["source"]["id"] = origin.unwrap_or("native").into(); // one source per publisher
    send_json(&mut ws, &message);
    let port = wait_for(
        || relay.wait_event(WAIT),
        |e| match e {
            RelayEvent::PublisherConnected { port, origin: o, .. } => {
                assert_eq!(o.as_deref(), origin);
                Some(port)
            }
            _ => None,
        },
    );
    (ws, port)
}

#[test]
fn relay_hello_and_forwarding() {
    let relay = relay(None, &[]);
    let mut ws = raw_connect(&url(&relay));
    let hello = read_json(&mut ws);
    assert_eq!(hello["ntx"], "0.1");
    assert_eq!(hello["source"]["name"], "TEST (relay)");
    assert_eq!(hello["channels"], json!(["program"]));
    assert!(hello["session"].as_str().is_some_and(|s| !s.is_empty()));
    assert!(hello.get("pairing").is_none());
    send_json(&mut ws, &publish("0.1"));
    let port = wait_for(
        || relay.wait_event(WAIT),
        |e| match e {
            RelayEvent::PublisherConnected { port, .. } => Some(port),
            _ => None,
        },
    );

    // The relay numbers updates itself; the publisher's numbers are not trusted.
    let update = json!({"op": "update", "channel": "program", "seq": 99, "type": "ntx.text", "fields": {"text": "hi"}});
    send_json(&mut ws, &update);
    let r = receiver_at(port, &["program"]);
    let state = common::next_update(&r);
    assert_eq!((state.seq, common::text(&state)), (1, "hi"));

    // Problems after the handshake are reported, not fatal.
    send_json(
        &mut ws,
        &json!({"op": "update", "channel": "preview", "seq": 100, "type": "ntx.text", "fields": {}}),
    );
    assert_eq!(read_json(&mut ws)["code"], "bad_message");
    send_json(&mut ws, &json!({"op": "error", "code": "bad_message"}));
    send_json(&mut ws, &json!({"op": "ping", "id": "9"}));
    assert_eq!(read_json(&mut ws), json!({"op": "pong", "id": "9"}));

    r.set_tally(Tally {
        program: false,
        preview: true,
    });
    assert_eq!(
        read_json(&mut ws),
        json!({"op": "tally", "program": false, "preview": true})
    );
}

#[test]
fn relay_handshake_errors() {
    let relay = relay(Some("2468"), &[]);
    let cases = [
        (publish("0.2"), "unsupported_version", 4001),
        (json!({"op": "publish", "ntx": "0.1"}), "bad_message", 4000),
        (publish("0.1"), "pairing_required", 4003),
    ];
    for (message, code, close) in cases {
        let mut ws = raw_connect(&url(&relay));
        let hello = read_json(&mut ws);
        assert_eq!(hello["pairing"]["nonce"].as_str().map(str::len), Some(32));
        send_json(&mut ws, &message);
        assert_eq!(read_json(&mut ws)["code"], code);
        assert_eq!(read_close(&mut ws), close);
    }
    let err = tungstenite::connect(format!("ws://{}/", relay.local_addr())).unwrap_err();
    assert!(err.to_string().contains("404"), "{err}");
}

#[test]
fn origins_are_refused_until_allowed() {
    const PAGE: &str = "https://songs.example";
    let relay = relay(None, &["https://trusted.example"]);
    for _ in 0..2 {
        let mut ws = raw_connect_from(&url(&relay), Some(PAGE));
        assert_eq!(read_json(&mut ws)["code"], "not_allowed");
        assert_eq!(read_close(&mut ws), 4003);
    }
    // The app is asked once, however often the page tries.
    assert_eq!(
        relay.wait_event(WAIT),
        Some(RelayEvent::OriginRequested { origin: PAGE.into() })
    );
    assert_eq!(relay.wait_event(Duration::from_millis(200)), None);

    // Allow-list entries and origins are compared in normal form.
    relay.allow_origin("HTTPS://Songs.Example:443/").unwrap();
    let (mut ws, _) = published(&relay, Some(PAGE));
    let (_trusted, _) = published(&relay, Some("https://trusted.example"));
    let (_native, _) = published(&relay, None);

    // Withdrawing the permission disconnects the page's publisher only, with `not_allowed`.
    relay.revoke_origin(PAGE).unwrap();
    assert_eq!(read_json(&mut ws)["code"], "not_allowed");
    assert_eq!(read_close(&mut ws), 4003);
    let left = wait_for(
        || relay.wait_event(WAIT),
        |e| match e {
            RelayEvent::PublisherDisconnected { connection } => Some(connection),
            _ => None,
        },
    );
    assert_eq!(left, 1);
    let mut again = raw_connect_from(&url(&relay), Some(PAGE));
    assert_eq!(read_json(&mut again)["code"], "not_allowed");
}

fn hello(nonce: Option<&str>) -> Value {
    let mut hello = json!({"op": "hello", "ntx": "0.1", "source": {"id": "fake", "name": "FAKE"}, "channels": ["program"], "session": "s1"});
    if let Some(nonce) = nonce {
        hello["pairing"] = json!({"nonce": nonce});
    }
    hello
}

#[test]
fn publisher_sends_publish_and_its_state() {
    let (listener, url) = fake_server("/publish");
    let p = Publisher::connect(&url, publisher_config("pub-raw", Some("123456"))).unwrap();
    p.update_text("program", "kept while offline").unwrap();
    let mut ws = accept(&listener);
    write(&mut ws, &hello(Some("q7cJ9mV2xL0aR4tY")));

    let publish = read(&mut ws);
    assert_eq!(publish["op"], "publish");
    assert_eq!(publish["ntx"], "0.1");
    assert_eq!(publish["source"], json!({"id": "pub-raw", "name": "Song Board"}));
    assert_eq!(publish["channels"], json!(["program", "preview"]));
    // examples/valid/subscribe-pairing.json in the spec repository.
    assert_eq!(
        publish["pairing"]["proof"],
        "i9tw3CG1t6+jl88QuoukjWJSEvC7mN3wlsoNQILA0mI="
    );
    let program = read(&mut ws);
    assert_eq!(
        (program["channel"].as_str(), program["seq"].as_u64()),
        (Some("program"), Some(1))
    );
    assert_eq!(program["fields"]["text"], "kept while offline");
    assert_eq!(read(&mut ws)["channel"], "preview");

    write(
        &mut ws,
        &json!({"op": "tally", "program": true, "preview": false}),
    );
    write(
        &mut ws,
        &json!({"op": "error", "code": "bad_message", "message": "test"}),
    );
    write(&mut ws, &json!({"op": "com.example.future"}));
    let events: Vec<_> = (0..3).filter_map(|_| p.wait_event(WAIT)).collect();
    assert!(
        matches!(events[0], PublisherEvent::Connected { .. }),
        "{events:?}"
    );
    assert_eq!(
        events[1],
        PublisherEvent::Tally(Tally {
            program: true,
            preview: false
        })
    );
    assert!(matches!(
        events[2],
        PublisherEvent::Error {
            code: ErrorCode::BadMessage,
            ..
        }
    ));

    let seq = p.update_text("program", "live").unwrap();
    let live = read(&mut ws);
    assert_eq!(
        (live["seq"].as_u64(), live["fields"]["text"].as_str()),
        (Some(seq), Some("live"))
    );
    let event = serde_json::to_value(PublisherEvent::Tally(Tally::default())).unwrap();
    assert_eq!(
        event,
        json!({"event": "tally", "data": {"program": false, "preview": false}})
    );
}

#[test]
fn publisher_reports_a_refusal() {
    let (listener, url) = fake_server("/publish");
    let mut config = publisher_config("pub-refused", None);
    config.reconnect = false;
    let p = Publisher::connect(&url, config).unwrap();
    let mut ws = accept(&listener);
    write(
        &mut ws,
        &json!({"op": "error", "code": "not_allowed", "message": "ask the user"}),
    );
    let reason = wait_for(
        || p.wait_event(WAIT),
        |e| match e {
            PublisherEvent::Disconnected { reason } => Some(reason),
            _ => None,
        },
    );
    assert!(reason.contains("not_allowed"), "{reason}");
}

#[test]
fn null_and_long_origins_are_refused_cleanly() {
    let relay = relay(None, &[]);
    // `null` is always refused, and the app is never asked about it (clause 14.5).
    let mut ws = raw_connect_from(&url(&relay), Some("null"));
    assert_eq!(read_json(&mut ws)["code"], "not_allowed");
    assert_eq!(read_close(&mut ws), 4003);
    assert_eq!(relay.wait_event(Duration::from_millis(200)), None);

    // The close reason must fit in a close frame (125 bytes); the error keeps the whole origin.
    let long = format!("https://{}.example", "a".repeat(200));
    let mut ws = raw_connect_from(&url(&relay), Some(&long));
    let error = read_json(&mut ws);
    assert!(error["message"].as_str().unwrap().contains(&long), "{error}");
    assert_eq!(read_close(&mut ws), 4003);
    assert_eq!(
        relay.wait_event(WAIT),
        Some(RelayEvent::OriginRequested { origin: long })
    );
}

#[test]
fn a_publisher_dropped_before_connecting_still_delivers() {
    let (listener, url) = fake_server("/publish");
    let p = Publisher::connect(&url, publisher_config("pub-early", None)).unwrap();
    p.update_text("program", "hi").unwrap();
    let dropping = std::thread::spawn(move || drop(p));
    let mut ws = accept(&listener);
    write(&mut ws, &hello(None));
    assert_eq!(read(&mut ws)["op"], "publish");
    let update = read(&mut ws);
    assert_eq!(update["fields"]["text"], "hi");
    dropping.join().unwrap();
}
