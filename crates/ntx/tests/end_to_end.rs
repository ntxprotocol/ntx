//! Real sources and receivers talking over localhost through the public API.

mod common;

use std::time::Duration;

use common::{WAIT, next_update, receiver, receiver_config, source, text, wait_for};
use ntx::protocol::ErrorCode;
use ntx::{
    Browser, BrowserEvent, Error, Fields, Receiver, ReceiverConfig, ReceiverEvent, Source, SourceConfig,
    SourceEvent, Tally, TypeDescriptor,
};
use serde_json::json;

fn fields(value: &serde_json::Value) -> Fields {
    value.as_object().unwrap().clone()
}

fn next_announce(r: &Receiver) -> Vec<TypeDescriptor> {
    wait_for(
        || r.wait_event(WAIT),
        |e| match e {
            ReceiverEvent::Announce(types) => Some(types),
            _ => None,
        },
    )
}

fn next_tally(s: &Source) -> Tally {
    wait_for(
        || s.wait_event(WAIT),
        |e| match e {
            SourceEvent::TallyChanged(t) => Some(t),
            _ => None,
        },
    )
}

#[test]
fn state_on_connect_then_changes() {
    let s = source(None);
    s.update_text("program", "Before anyone connected").unwrap();

    let r = receiver(&s, &["program"], None);
    let (name, channels) = wait_for(
        || r.wait_event(WAIT),
        |e| match e {
            ReceiverEvent::Connected { source, channels, .. } => Some((source.name, channels)),
            _ => None,
        },
    );
    assert_eq!(name, "TEST (source)");
    assert_eq!(channels, ["program", "preview"]);
    let first = next_update(&r);
    assert_eq!(
        (first.channel.as_str(), first.type_name.as_str()),
        ("program", "ntx.text")
    );
    assert_eq!(text(&first), "Before anyone connected");

    let seq = s.update_text("program", "Amazing grace").unwrap();
    let second = next_update(&r);
    assert_eq!((second.seq, text(&second)), (seq, "Amazing grace"));
    assert!(second.seq > first.seq);

    // Updates on a channel the receiver did not subscribe to are not sent.
    s.update_text("preview", "next").unwrap();
    s.clear("program").unwrap();
    let cleared = next_update(&r);
    assert_eq!(cleared.channel, "program");
    assert!(cleared.fields.is_empty());
}

#[test]
fn empty_channels_custom_types_and_announcements() {
    let s = source(None);
    let lyrics: TypeDescriptor = serde_json::from_value(json!({
        "ntxType": "com.example.lyrics",
        "schema": {"type": "object", "properties": {"line": {"type": "string"}}}
    }))
    .unwrap();
    s.announce(vec![lyrics.clone()]).unwrap();

    let r = receiver(&s, &["program", "preview"], None);
    assert_eq!(next_announce(&r), [lyrics]);
    let mut snapshot = [next_update(&r), next_update(&r)];
    snapshot.sort_by(|a, b| a.channel.cmp(&b.channel));
    assert_eq!(
        [&snapshot[0].channel, &snapshot[1].channel],
        ["preview", "program"]
    );
    assert!(
        snapshot
            .iter()
            .all(|u| u.seq == 0 && u.type_name == "ntx.text" && u.fields.is_empty())
    );

    let scripture = json!({"reference": "John 3:16", "verses": [{"text": "For God so loved the world"}]});
    s.update("preview", "com.example.scripture", fields(&scripture))
        .unwrap();
    let update = next_update(&r);
    assert_eq!(
        (update.channel.as_str(), update.type_name.as_str()),
        ("preview", "com.example.scripture")
    );
    assert_eq!(
        ntx::binding::field_text(&update.fields, "verses.0.text"),
        "For God so loved the world"
    );

    s.announce(vec![]).unwrap();
    assert_eq!(next_announce(&r), []);
}

#[test]
fn invalid_updates_are_refused() {
    let s = source(None);
    assert!(matches!(
        s.update("program", "Bad Name", Fields::new()),
        Err(Error::InvalidTypeName(_))
    ));
    assert!(matches!(
        s.update_text("nope", "x"),
        Err(Error::UnknownChannel(_))
    ));
    let huge = "x".repeat(ntx::MAX_MESSAGE_BYTES);
    assert!(matches!(s.update_text("program", &huge), Err(Error::TooLarge(_))));
    // At most 16 levels of nesting in fields (clause 18.2).
    let deep = (0..16).fold(json!(1), |inner, _| json!([inner]));
    assert!(matches!(
        s.update("program", "ntx.text", fields(&json!({"deep": deep}))),
        Err(Error::InvalidMessage(_))
    ));
    let bad: TypeDescriptor =
        serde_json::from_value(json!({"ntxType": "x", "schema": {"type": "object"}})).unwrap();
    assert!(s.announce(vec![bad]).is_err());
    // Each description is within its 4096 characters; together they are too large.
    let big: TypeDescriptor = serde_json::from_value(json!({
        "ntxType": "com.example.big", "description": "d".repeat(4096), "schema": {"type": "object"}
    }))
    .unwrap();
    let types = vec![big; ntx::MAX_MESSAGE_BYTES / 4096 + 1];
    assert!(matches!(s.announce(types), Err(Error::TooLarge(_))));
    assert!(s.poll_event().is_none());
}

#[test]
fn start_errors() {
    let mut config = SourceConfig::new("TEST");
    config.channels = vec!["preview".into()];
    assert!(matches!(Source::start(config), Err(Error::UnknownChannel(_))));
    // Channel lists must be valid (clause 18.4), or `hello` would break the schema.
    for channels in [vec!["program", "program"], vec!["program", ""]] {
        let mut config = SourceConfig::new("TEST");
        config.channels = channels.into_iter().map(String::from).collect();
        assert!(matches!(Source::start(config), Err(Error::InvalidMessage(_))));
    }
    let mut config = SourceConfig::new("TEST");
    config.pairing_code = Some(String::new());
    assert!(matches!(Source::start(config), Err(Error::InvalidMessage(_))));
    let mut config = ReceiverConfig::new("TEST");
    config.channels = vec!["program".into(), "program".into()];
    assert!(matches!(
        Receiver::connect("127.0.0.1:1", config),
        Err(Error::InvalidMessage(_))
    ));

    let taken = source(None);
    let mut config = SourceConfig::new("TEST");
    config.bind = taken.local_addr().ip();
    config.port = taken.local_addr().port();
    config.advertise = false;
    assert!(matches!(Source::start(config), Err(Error::Io(_))));

    let invalid = Receiver::connect("http://x", receiver_config(&[], None));
    assert!(matches!(invalid, Err(Error::InvalidAddress(_))));
}

#[test]
fn text_is_normalised() {
    let s = source(None);
    let r = receiver(&s, &["program"], None);
    next_update(&r);
    s.update_text("program", "line one\r\nO\u{0323}lo\u{0301}run")
        .unwrap();
    assert_eq!(text(&next_update(&r)), "line one\n\u{1ECC}l\u{00F3}run");
}

#[test]
fn tally_and_receiver_count() {
    let s = source(None);
    let r = receiver(&s, &["program"], None);
    r.set_tally(Tally {
        program: true,
        preview: false,
    });
    next_update(&r);
    assert_eq!(
        next_tally(&s),
        Tally {
            program: true,
            preview: false
        }
    );
    assert_eq!(s.receiver_count(), 1);
    assert_eq!(r.poll_event(), None);

    drop(r);
    assert_eq!(next_tally(&s), Tally::default());
    wait_for(
        || s.wait_event(WAIT),
        |e| matches!(e, SourceEvent::ReceiverDisconnected { .. }).then_some(()),
    );
    assert_eq!(s.receiver_count(), 0);
}

#[test]
fn pairing() {
    let s = source(Some("123456"));
    s.update_text("program", "secret service order").unwrap();
    let good = receiver(&s, &["program"], Some("123456"));
    assert_eq!(text(&next_update(&good)), "secret service order");

    for (code, expected) in [
        (None, ErrorCode::PairingRequired),
        (Some("000000"), ErrorCode::PairingFailed),
    ] {
        let mut config = receiver_config(&["program"], code);
        config.reconnect = false;
        let bad = Receiver::connect(&format!("ws://{}/", s.local_addr()), config).unwrap();
        let mut got = None;
        while let Some(event) = bad.wait_event(WAIT) {
            match event {
                ReceiverEvent::Error { code, .. } => got = Some(code),
                ReceiverEvent::Update(_) => panic!("received text without the right pairing code"),
                ReceiverEvent::Disconnected { .. } => break,
                ReceiverEvent::Connected { .. } | ReceiverEvent::Announce(_) => {}
            }
        }
        assert_eq!(got, Some(expected));
    }
}

#[test]
fn reconnects_after_source_restart() {
    let s = source(None);
    let address = s.local_addr();
    let r = receiver(&s, &["program"], None);
    next_update(&r);
    s.update_text("program", "one").unwrap();
    next_update(&r);
    drop(s);

    // Same port, new session: sequence numbers start again and must not be ignored.
    let mut config = SourceConfig::new("TEST (source)");
    config.bind = address.ip();
    config.port = address.port();
    config.advertise = false;
    let restarted = (0..100).find_map(|_| {
        let started = Source::start(config.clone()).ok();
        if started.is_none() {
            std::thread::sleep(Duration::from_millis(50)); // the port can take a moment to free up
        }
        started
    });
    let s2 = restarted.expect("could not restart the source");
    s2.update_text("program", "after restart").unwrap();

    let mut disconnected = false;
    let update = wait_for(
        || r.wait_event(Duration::from_secs(10)),
        |e| match e {
            ReceiverEvent::Disconnected { .. } => {
                disconnected = true;
                None
            }
            ReceiverEvent::Update(u) => Some(u),
            _ => None,
        },
    );
    assert!(disconnected);
    assert_eq!((update.seq, text(&update)), (1, "after restart"));
}

#[test]
fn discovery_finds_and_loses_a_source() {
    let mut config = SourceConfig::new("TEST (discovery)");
    config.port = 0;
    config.path = "/ntx".into();
    config.pairing_code = Some("1".into());
    let id = config.id.clone();
    let s = Source::start(config).unwrap();
    let browser = Browser::start().unwrap();

    let found = wait_for(
        || browser.wait_event(Duration::from_secs(10)),
        |e| match e {
            BrowserEvent::Found(found) if found.id == id => Some(found),
            _ => None,
        },
    );
    assert_eq!(found.name, "TEST (discovery)");
    assert_eq!(found.version, ntx::PROTOCOL_VERSION);
    assert!(found.pairing_required);
    assert_eq!(found.port, s.local_addr().port());
    assert!(
        found.url.ends_with(&format!(":{}/ntx", found.port)),
        "{}",
        found.url
    );
    assert_ne!(found.addresses, Vec::<std::net::IpAddr>::new());

    drop(s);
    wait_for(
        || browser.wait_event(Duration::from_secs(10)),
        |e| matches!(e, BrowserEvent::Lost { id: ref lost, .. } if *lost == id).then_some(()),
    );
    assert!(
        browser
            .poll_event()
            .is_none_or(|e| !matches!(e, BrowserEvent::Found(f) if f.id == id))
    );
}

#[test]
fn names_and_ids_must_fit_hello() {
    // Clause 18.4: names are 1 to 256 characters, ids 1 to 128.
    let mut config = SourceConfig::new("n".repeat(257));
    config.advertise = false;
    assert!(matches!(Source::start(config), Err(Error::InvalidMessage(_))));
    let mut config = receiver_config(&["program"], None);
    config.id = String::new();
    assert!(Receiver::connect("127.0.0.1:1", config).is_err());
    let mut config = common::publisher_config("p", None);
    config.name = "n".repeat(257);
    assert!(ntx::Publisher::connect("127.0.0.1:1", config).is_err());
    let mut config = ntx::RelayConfig::new("n".repeat(300));
    config.port = 0;
    config.advertise = false;
    assert!(matches!(ntx::Relay::start(config), Err(Error::InvalidMessage(_))));
}
