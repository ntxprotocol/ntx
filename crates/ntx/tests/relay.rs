//! Publishers sending through a real relay to real receivers over localhost (clause 14).

mod common;

use std::time::Duration;

use common::{WAIT, next_update, publisher, publisher_config, receiver_at, relay, text, wait_for};
use ntx::protocol::ErrorCode;
use ntx::{
    Browser, BrowserEvent, Error, Fields, Publisher, PublisherConfig, PublisherEvent, Receiver,
    ReceiverEvent, Relay, RelayConfig, RelayEvent, Tally, TypeDescriptor, display_name,
};
use serde_json::json;

/// Waits until the relay serves a publisher; returns the served source's port and identity.
fn served(relay: &Relay) -> (u16, ntx::Endpoint) {
    wait_for(
        || relay.wait_event(WAIT),
        |e| match e {
            RelayEvent::PublisherConnected { port, source, .. } => Some((port, source)),
            _ => None,
        },
    )
}

fn next_tally(p: &Publisher) -> Tally {
    wait_for(
        || p.wait_event(WAIT),
        |e| match e {
            PublisherEvent::Tally(t) => Some(t),
            _ => None,
        },
    )
}

fn disconnect_reason(p: &Publisher) -> String {
    wait_for(
        || p.wait_event(WAIT),
        |e| match e {
            PublisherEvent::Disconnected { reason } => Some(reason),
            _ => None,
        },
    )
}

#[test]
fn text_and_tally_flow_through_the_relay() {
    let relay = relay(None, &[]);
    let p = publisher(&relay, "pub-flow", None);
    p.update_text("program", "Before the relay answered").unwrap();
    let relay_name = wait_for(
        || p.wait_event(WAIT),
        |e| match e {
            PublisherEvent::Connected { relay, .. } => Some(relay.name),
            _ => None,
        },
    );
    assert_eq!(relay_name, "TEST (relay)");

    let (port, source) = served(&relay);
    assert_eq!(source.id, "pub-flow");
    assert_eq!(source.name, display_name("Song Board"));

    let r = receiver_at(port, &["program", "preview"]);
    let (id, channels) = wait_for(
        || r.wait_event(WAIT),
        |e| match e {
            ReceiverEvent::Connected { source, channels, .. } => Some((source.id, channels)),
            _ => None,
        },
    );
    assert_eq!(
        (id.as_str(), channels),
        ("pub-flow", vec!["program".into(), "preview".into()])
    );
    let mut state = [next_update(&r), next_update(&r)];
    state.sort_by(|a, b| a.channel.cmp(&b.channel));
    assert_eq!(text(&state[1]), "Before the relay answered");
    assert!(state[0].fields.is_empty());

    let lyrics: TypeDescriptor =
        serde_json::from_value(json!({"ntxType": "com.example.lyrics", "schema": {"type": "object"}}))
            .unwrap();
    p.announce(vec![lyrics.clone()]).unwrap();
    let announced = wait_for(
        || r.wait_event(WAIT),
        |e| match e {
            ReceiverEvent::Announce(types) => Some(types),
            _ => None,
        },
    );
    assert_eq!(announced, [lyrics]);
    p.update_text("program", "Amazing grace").unwrap();
    assert_eq!(text(&next_update(&r)), "Amazing grace");
    p.update(
        "preview",
        "com.example.lyrics",
        json!({"line": "How sweet"}).as_object().unwrap().clone(),
    )
    .unwrap();
    assert_eq!(next_update(&r).fields["line"], "How sweet");
    p.clear("program").unwrap();
    assert!(next_update(&r).fields.is_empty());

    // Tally comes back to the publisher (clause 14.3).
    r.set_tally(Tally {
        program: true,
        preview: false,
    });
    assert!(next_tally(&p).program);
    drop(r);
    assert_eq!(next_tally(&p), Tally::default());

    drop(p);
    wait_for(
        || relay.wait_event(WAIT),
        |e| matches!(e, RelayEvent::PublisherDisconnected { .. }).then_some(()),
    );
}

#[test]
fn receivers_find_the_source_again_after_the_publisher_reconnects() {
    let relay = relay(None, &[]);
    let first = publisher(&relay, "pub-again", None);
    first.update_text("program", "one").unwrap();
    let (port, _) = served(&relay);
    let r = receiver_at(port, &["program"]);
    assert_eq!(text(&next_update(&r)), "one");
    drop(first);
    wait_for(
        || relay.wait_event(WAIT),
        |e| matches!(e, RelayEvent::PublisherDisconnected { .. }).then_some(()),
    );

    // A new connection is a new session, served on the same port (clause 14.4).
    let second = publisher(&relay, "pub-again", None);
    second.update_text("program", "two").unwrap();
    let (again, _) = served(&relay);
    assert_eq!(again, port);
    let update = wait_for(
        || r.wait_event(Duration::from_secs(10)),
        |e| match e {
            ReceiverEvent::Update(u) => Some(u),
            _ => None,
        },
    );
    assert_eq!((update.seq, text(&update)), (1, "two"));

    // A newer connection for the same id wins (clause 14.4): the older publisher is told
    // `not_allowed` and does not come back; the newer one is served on the same port.
    let third = publisher(&relay, "pub-again", None);
    third.update_text("program", "three").unwrap();
    let error = wait_for(
        || second.wait_event(WAIT),
        |e| match e {
            PublisherEvent::Error { code, .. } => Some(code),
            _ => None,
        },
    );
    assert_eq!(error, ErrorCode::NotAllowed);
    assert!(matches!(
        second.wait_event(WAIT),
        Some(PublisherEvent::Disconnected { .. })
    ));
    let (newest, _) = served(&relay);
    assert_eq!(newest, port);
    assert_eq!(
        second.wait_event(Duration::from_millis(2500)),
        None,
        "reconnected"
    );
}

#[test]
fn dropping_a_publisher_delivers_its_last_update() {
    let relay = relay(None, &[]);
    let p = publisher(&relay, "pub-last", None);
    let (port, _) = served(&relay);
    let r = receiver_at(port, &["program"]);
    next_update(&r);
    p.update_text("program", "last words").unwrap();
    drop(p);
    assert_eq!(text(&next_update(&r)), "last words");
}

#[test]
fn a_published_source_stops_advertising_when_its_publisher_leaves() {
    let mut config = RelayConfig::new("TEST (relay)");
    config.port = 0;
    let relay = Relay::start(config).unwrap();
    let browser = Browser::start().unwrap();
    let p = publisher(&relay, "pub-mdns-leaves", None);
    let is_ours = |id: &str| id == "pub-mdns-leaves";
    wait_for(
        || browser.wait_event(Duration::from_secs(10)),
        |e| matches!(e, BrowserEvent::Found(ref s) if is_ours(&s.id)).then_some(()),
    );
    drop(p);
    wait_for(
        || browser.wait_event(Duration::from_secs(10)),
        |e| matches!(e, BrowserEvent::Lost { ref id, .. } if is_ours(id)).then_some(()),
    );
}

#[test]
fn programs_are_limited_per_origin() {
    let relay = relay(None, &[]);
    let publishers: Vec<_> = (0..8)
        .map(|i| publisher(&relay, &format!("pub-limit-{i}"), None))
        .collect();
    for _ in &publishers {
        served(&relay);
    }
    let one_too_many = publisher(&relay, "pub-limit-8", None);
    let error = wait_for(
        || one_too_many.wait_event(WAIT),
        |e| match e {
            PublisherEvent::Error { code, message } => Some((code, message)),
            _ => None,
        },
    );
    assert_eq!(error.0, ErrorCode::NotAllowed);
    assert!(error.1.unwrap().contains("too many"));
}

#[test]
fn publisher_reconnects_after_the_relay_restarts() {
    let first = relay(None, &[]);
    let address = first.local_addr();
    let p = publisher(&first, "pub-restart", None);
    served(&first);
    drop(first);
    assert_ne!(disconnect_reason(&p), "");

    let mut config = RelayConfig::new("TEST (relay)");
    config.port = address.port();
    config.advertise = false;
    // The port can take a moment to free up.
    let second = (0..100)
        .find_map(|_| {
            Relay::start(config.clone())
                .inspect_err(|_| std::thread::sleep(Duration::from_millis(50)))
                .ok()
        })
        .expect("could not restart the relay");
    p.update_text("program", "after restart").unwrap();
    let (port, _) = wait_for(
        || second.wait_event(Duration::from_secs(10)),
        |e| match e {
            RelayEvent::PublisherConnected { port, source, .. } => Some((port, source)),
            _ => None,
        },
    );
    let r = receiver_at(port, &["program"]);
    assert_eq!(text(&next_update(&r)), "after restart");
}

#[test]
fn pairing() {
    let relay = relay(Some("2468"), &[]);
    for (code, expected) in [(None, "pairing_required"), (Some("1357"), "pairing_failed")] {
        let mut config = publisher_config("pub-pair", code);
        config.reconnect = false;
        let p = Publisher::connect(&format!("ws://{}/publish", relay.local_addr()), config).unwrap();
        let mut error = None;
        while let Some(event) = p.wait_event(WAIT) {
            match event {
                PublisherEvent::Error { code, .. } => error = Some(code),
                PublisherEvent::Disconnected { .. } => break,
                _ => {}
            }
        }
        assert_eq!(error.map(|c| c.as_str().to_string()).as_deref(), Some(expected));
    }
    let p = publisher(&relay, "pub-pair", Some("2468"));
    p.update_text("program", "paired").unwrap();
    let (port, _) = served(&relay);
    assert_eq!(text(&next_update(&receiver_at(port, &["program"]))), "paired");
}

#[test]
fn invalid_use_is_refused() {
    let invalid = Publisher::connect("http://x", PublisherConfig::new("x"));
    assert!(matches!(invalid, Err(Error::InvalidAddress(_))));
    let mut config = PublisherConfig::new("x");
    config.channels = vec!["preview".into()];
    assert!(matches!(
        Publisher::connect("localhost", config),
        Err(Error::UnknownChannel(_))
    ));

    let mut config = PublisherConfig::new("x");
    config.reconnect = false;
    let p = Publisher::connect("127.0.0.1:1", config).unwrap();
    assert_eq!(p.url(), "ws://127.0.0.1:1/publish");
    assert!(matches!(
        p.update_text("nope", "x"),
        Err(Error::UnknownChannel(_))
    ));
    assert!(matches!(
        p.update("program", "Bad Name", Fields::new()),
        Err(Error::InvalidTypeName(_))
    ));
    let bad: TypeDescriptor =
        serde_json::from_value(json!({"ntxType": "x", "schema": {"type": "object"}})).unwrap();
    assert!(p.announce(vec![bad]).is_err());
    assert_ne!(disconnect_reason(&p), "");
    assert_eq!(p.poll_event(), None);

    let mut config = RelayConfig::new("TEST");
    config.allowed_origins = vec!["example.com".into()];
    assert!(matches!(Relay::start(config), Err(Error::InvalidOrigin(_))));

    let taken = relay(None, &[]);
    assert!(matches!(taken.allow_origin("null"), Err(Error::InvalidOrigin(_))));
    assert!(matches!(taken.revoke_origin("x"), Err(Error::InvalidOrigin(_))));
    let mut config = RelayConfig::new("TEST");
    config.port = taken.local_addr().port();
    assert!(matches!(Relay::start(config), Err(Error::Io(_))));
    assert_eq!(taken.poll_event(), None);
}

#[test]
fn receiver_with_reconnect_off_stops_after_the_publisher_leaves() {
    let relay = relay(None, &[]);
    let p = publisher(&relay, "pub-leave", None);
    let (port, _) = served(&relay);
    let mut config = common::receiver_config(&["program"], None);
    config.reconnect = false;
    let r = Receiver::connect(&format!("127.0.0.1:{port}"), config).unwrap();
    next_update(&r);
    drop(p);
    wait_for(
        || r.wait_event(WAIT),
        |e| matches!(e, ReceiverEvent::Disconnected { .. }).then_some(()),
    );
}
