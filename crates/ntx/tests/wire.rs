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
fn pairing_proofs_missing_or_malformed() {
    // Clause 13.4: no `pairing` or no `proof` is pairing_required; a proof that is not
    // 44 characters of Base64 is pairing_failed, not bad_message.
    let s = source(Some("4321"));
    let cases = [
        (None, "pairing_required"),
        (Some(json!({})), "pairing_required"),
        (Some(json!({"proof": "not-base64"})), "pairing_failed"),
    ];
    for (pairing, code) in cases {
        let mut ws = raw_connect(&format!("ws://{}/", s.local_addr()));
        let hello = read_json(&mut ws);
        assert_eq!(hello["pairing"]["nonce"].as_str().map(str::len), Some(32));
        let mut answer = subscribe("0.1");
        if let Some(pairing) = pairing {
            answer["pairing"] = pairing;
        }
        send_json(&mut ws, &answer);
        assert_eq!(read_json(&mut ws)["code"], code);
        assert_eq!(read_close(&mut ws), 4003);
    }
}

#[test]
fn pairing_is_ignored_when_not_required() {
    let s = source(None);
    let mut ws = raw_connect(&format!("ws://{}/", s.local_addr()));
    read_json(&mut ws);
    let mut answer = subscribe("0.1");
    answer["pairing"] = json!({"proof": "not-base64"});
    send_json(&mut ws, &answer);
    assert_eq!(read_json(&mut ws)["op"], "update");
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
    // Keep ignoring it: after 15 seconds the receiver closes with 4008 (clauses 11.2, 12.5).
    ws.get_ref()
        .set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    let close = loop {
        if let WsMessage::Close(frame) = ws.read().unwrap() {
            break frame.unwrap().code;
        }
    };
    assert_eq!(u16::from(close), 4008);
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
fn source_closes_with_4008_without_subscribe_or_when_the_receiver_is_silent() {
    let s = source(None);
    // No subscribe within 10 seconds (clause 8.6).
    let mut quiet = raw_connect(&format!("ws://{}/", s.local_addr()));
    assert_eq!(read_json(&mut quiet)["op"], "hello");
    // A receiver that answers nothing, not even pings, for 15 seconds (clause 11.2).
    let mut silent = subscribed(&s);
    assert_eq!(read_close(&mut quiet), 4008);
    assert_eq!(read_close(&mut silent), 4008);
}

#[test]
fn receiver_reports_a_refused_connection() {
    let (listener, url) = fake_server("/");
    drop(listener);
    let r = one_shot_receiver(&url);
    assert_ne!(disconnect_reason(&r), "");
}

#[test]
fn idle_connections_are_limited_and_time_out() {
    use std::io::Read;
    let s = source(None);
    // Sockets that never send an upgrade request: one address may hold 16 places at most.
    let mut idle: Vec<_> = (0..16)
        .map(|_| std::net::TcpStream::connect(s.local_addr()).unwrap())
        .collect();
    let one_too_many = tungstenite::connect(format!("ws://{}/", s.local_addr()));
    assert!(one_too_many.is_err(), "a connection over the limit was served");
    // Without an upgrade within 10 seconds, the source closes them and frees their places.
    idle[0].set_read_timeout(Some(Duration::from_secs(15))).unwrap();
    assert_eq!(idle[0].read(&mut [0; 16]).unwrap(), 0);
    let r = common::receiver(&s, &["program"], None);
    common::next_update(&r);
}

#[test]
fn a_receiver_that_stops_reading_is_dropped() {
    let s = source(None);
    let _stalled = subscribed(&s); // never read again
    wait_for(
        || s.wait_event(WAIT),
        |e| matches!(e, SourceEvent::ReceiverConnected { .. }).then_some(()),
    );
    // Far more than fits in socket buffers and the queue: memory must not grow without bound.
    let big = "x".repeat(60_000);
    for _ in 0..1000 {
        s.update_text("program", &big).unwrap();
    }
    wait_for(
        || s.wait_event(Duration::from_secs(25)),
        |e| matches!(e, SourceEvent::ReceiverDisconnected { .. }).then_some(()),
    );
    assert_eq!(s.receiver_count(), 0);
}

#[test]
fn repeated_wrong_pairing_codes_lock_the_address_out() {
    let s = source(Some("1234"));
    let url = format!("ws://{}/", s.local_addr());
    let attempt = |code: &str| {
        let mut ws = raw_connect(&url);
        let nonce = read_json(&mut ws)["pairing"]["nonce"]
            .as_str()
            .unwrap()
            .to_string();
        let mut answer = subscribe("0.1");
        answer["pairing"] = json!({"proof": ntx::pairing::proof(code, &nonce)});
        send_json(&mut ws, &answer);
        read_json(&mut ws)
    };
    // Guesses in parallel are counted too.
    std::thread::scope(|scope| {
        let guesses: Vec<_> = (0..10).map(|_| scope.spawn(|| attempt("0000"))).collect();
        for guess in guesses {
            assert_eq!(guess.join().unwrap()["code"], "pairing_failed");
        }
    });
    let locked = attempt("1234");
    assert_eq!(locked["code"], "pairing_failed");
    assert!(
        locked["message"].as_str().unwrap().contains("try again later"),
        "{locked}"
    );
}

/// Whether a client connects to the fake server within `within`.
fn connects_within(listener: &std::net::TcpListener, within: Duration) -> bool {
    listener.set_nonblocking(true).unwrap();
    let deadline = std::time::Instant::now() + within;
    let connected = loop {
        match listener.accept() {
            Ok(_) => break true,
            Err(_) if std::time::Instant::now() < deadline => std::thread::sleep(Duration::from_millis(20)),
            Err(_) => break false,
        }
    };
    listener.set_nonblocking(false).unwrap();
    connected
}

fn next_error(r: &Receiver) -> ErrorCode {
    wait_for(
        || r.wait_event(WAIT),
        |e| match e {
            ReceiverEvent::Error { code, .. } => Some(code),
            _ => None,
        },
    )
}

#[test]
fn connected_only_after_the_first_update() {
    let (listener, url) = fake_server("/");
    let r = Receiver::connect(&url, receiver_config(&["program", "preview", "nope"], None)).unwrap();
    let mut ws = accept(&listener);
    let mut greeting = hello("0.1");
    greeting["channels"] = json!(["program", "preview"]);
    write(&mut ws, &greeting);
    // Only channels the source listed are asked for (clause 8.3).
    assert_eq!(read(&mut ws)["channels"], json!(["program", "preview"]));
    write(
        &mut ws,
        &json!({"op": "announce", "types": [{"ntxType": "com.example.t", "schema": {"type": "object"}}]}),
    );
    assert_eq!(r.wait_event(Duration::from_millis(300)), None);
    // The first update confirms the subscription; the announcement waited for it.
    write(
        &mut ws,
        &json!({"op": "update", "channel": "program", "seq": 0, "type": "ntx.text", "fields": {}}),
    );
    let events: Vec<_> = (0..3).filter_map(|_| r.wait_event(WAIT)).collect();
    assert!(matches!(events[0], ReceiverEvent::Connected { .. }), "{events:?}");
    assert!(matches!(events[1], ReceiverEvent::Announce(_)), "{events:?}");
    assert!(matches!(events[2], ReceiverEvent::Update(_)), "{events:?}");
}

#[test]
fn errors_that_need_the_user_stop_reconnecting() {
    // Clause 11.3: pairing_failed (and the others) from the source...
    let (listener, url) = fake_server("/");
    let r = Receiver::connect(&url, receiver_config(&["program"], Some("1234"))).unwrap();
    let mut ws = accept(&listener);
    let mut greeting = hello("0.1");
    greeting["pairing"] = json!({"nonce": "q7cJ9mV2xL0aR4tY"});
    write(&mut ws, &greeting);
    read(&mut ws);
    write(&mut ws, &json!({"op": "error", "code": "pairing_failed"}));
    drop(ws);
    assert_eq!(next_error(&r), ErrorCode::PairingFailed);
    disconnect_reason(&r);
    assert!(!connects_within(&listener, Duration::from_millis(1500)));
    // ...until the app asks to try again.
    r.reconnect();
    assert!(connects_within(&listener, WAIT));

    // A receiver without a pairing code gives up with 4003 instead of subscribing (clause 8.3).
    let (listener, url) = fake_server("/");
    let r = Receiver::connect(&url, receiver_config(&["program"], None)).unwrap();
    let mut ws = accept(&listener);
    write(&mut ws, &greeting);
    let close = loop {
        if let WsMessage::Close(frame) = ws.read().unwrap() {
            break frame.unwrap().code;
        }
    };
    assert_eq!(u16::from(close), 4003);
    assert_eq!(next_error(&r), ErrorCode::PairingRequired);
    assert!(!connects_within(&listener, Duration::from_millis(1500)));

    // So does a version this receiver does not speak.
    let (listener, url) = fake_server("/");
    let r = Receiver::connect(&url, receiver_config(&["program"], None)).unwrap();
    write(&mut accept(&listener), &hello("0.2"));
    assert_eq!(next_error(&r), ErrorCode::UnsupportedVersion);
    assert!(!connects_within(&listener, Duration::from_millis(1500)));
}

#[test]
fn the_wait_goes_back_to_one_second_only_after_an_update() {
    let (listener, url) = fake_server("/");
    let _r = Receiver::connect(&url, receiver_config(&["program"], None)).unwrap();
    // Accepts the next connection, answers its subscribe, and says how long it took to come.
    let next = |since: std::time::Instant| {
        let mut ws = accept(&listener);
        let waited = since.elapsed();
        write(&mut ws, &hello("0.1"));
        read(&mut ws);
        (ws, waited)
    };
    let (ws, _) = next(std::time::Instant::now());
    // Closed before any update: the waits are 1 second, then 2 (clause 11.3).
    drop(ws);
    let (ws, first) = next(std::time::Instant::now());
    drop(ws);
    let (mut ws, second) = next(std::time::Instant::now());
    assert!(first < Duration::from_millis(1800), "{first:?}");
    assert!(second >= Duration::from_millis(1800), "{second:?}");
    // This connection delivers an update: the next wait is 1 second again.
    write(
        &mut ws,
        &json!({"op": "update", "channel": "program", "seq": 1, "type": "ntx.text", "fields": {}}),
    );
    std::thread::sleep(Duration::from_millis(200));
    drop(ws);
    let (_ws, third) = next(std::time::Instant::now());
    assert!(third < Duration::from_millis(1800), "{third:?}");
}

#[test]
fn reconnect_connects_again_when_reconnecting_is_off() {
    let (listener, url) = fake_server("/");
    let r = one_shot_receiver(&url);
    drop(accept(&listener));
    disconnect_reason(&r);
    assert!(!connects_within(&listener, Duration::from_millis(1500)));
    r.reconnect();
    assert!(connects_within(&listener, WAIT));
}

#[test]
fn a_receiver_does_not_subscribe_to_nothing() {
    // Clause 8.3: ask for at least one channel the source has; otherwise nothing would arrive.
    let (listener, url) = fake_server("/");
    let r = Receiver::connect(&url, receiver_config(&["preview"], None)).unwrap();
    let mut ws = accept(&listener);
    write(&mut ws, &hello("0.1"));
    let close = loop {
        match ws.read().unwrap() {
            WsMessage::Close(frame) => break frame.unwrap().code,
            WsMessage::Text(text) => panic!("sent {text}"),
            _ => {}
        }
    };
    assert_eq!(u16::from(close), 1000);
    assert!(disconnect_reason(&r).contains("none of the requested channels"));
}

#[test]
fn a_receiver_closes_with_4008_without_hello() {
    let (listener, url) = fake_server("/");
    let r = one_shot_receiver(&url);
    let mut ws = accept(&listener);
    ws.get_ref()
        .set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    let close = loop {
        if let WsMessage::Close(frame) = ws.read().unwrap() {
            break frame.unwrap().code;
        }
    };
    assert_eq!(u16::from(close), 4008);
    assert!(disconnect_reason(&r).contains("no hello"));
}
