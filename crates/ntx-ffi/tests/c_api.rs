//! The C ABI called the way a binding calls it: raw pointers, JSON strings and polling.
//!
//! Each C function is wrapped once in a small safe helper, the same way a language binding
//! would, so every `unsafe` block has one operation and one reason.
#![allow(unsafe_code)] // calling a C ABI needs unsafe; each block says why it is sound

use std::ffi::{CStr, CString, c_char, c_int};
use std::ptr;

use ntx_ffi::*;
use serde_json::{Value, json};

const LYRICS: &str = r#"[{"ntxType": "com.example.lyrics", "schema": {"type": "object"}}]"#;

fn c(s: &str) -> CString {
    CString::new(s).unwrap()
}

/// Takes ownership of a returned string (`None` for null).
fn take_string(s: *mut c_char) -> Option<String> {
    if s.is_null() {
        return None;
    }
    // SAFETY: a non-null string returned by the library.
    let text = unsafe { CStr::from_ptr(s) }.to_str().unwrap().to_owned();
    // SAFETY: returned by the library and not freed yet.
    unsafe { ntx_string_free(s) };
    Some(text)
}

fn take_json(s: *mut c_char) -> Option<Value> {
    take_string(s).map(|s| serde_json::from_str(&s).unwrap())
}

fn static_str(s: *const c_char) -> String {
    // SAFETY: the library's static and thread-local strings are valid and NUL-terminated.
    unsafe { CStr::from_ptr(s) }.to_str().unwrap().to_owned()
}

fn last_error() -> String {
    static_str(ntx_last_error())
}

// Safe wrappers, one per C function. `config` strings may be invalid on purpose.

fn source_start(config: &CStr) -> Result<*mut NtxSource, c_int> {
    let mut source = ptr::null_mut();
    // SAFETY: a valid string and out-pointer.
    let code = unsafe { ntx_source_start(config.as_ptr(), &raw mut source) };
    if code == NTX_OK { Ok(source) } else { Err(code) }
}

fn start(config: &Value) -> *mut NtxSource {
    source_start(&c(&config.to_string())).unwrap_or_else(|code| panic!("{code}: {}", last_error()))
}

// Sources and publishers share these functions; the C function is passed in.
type UpdateFn<T> =
    unsafe extern "C" fn(*const T, *const c_char, *const c_char, *const c_char, *mut u64) -> c_int;
type TextFn<T> = unsafe extern "C" fn(*const T, *const c_char, *const c_char) -> c_int;
type StringFn<T> = unsafe extern "C" fn(*const T, *const c_char) -> c_int;
type EventFn<T> = unsafe extern "C" fn(*const T, u32) -> *mut c_char;
type HandleFn<T> = unsafe extern "C" fn(*const T) -> c_int;

fn update<T>(f: UpdateFn<T>, handle: *const T, type_name: &str, fields: &str) -> Result<u64, c_int> {
    let (channel, type_name, fields) = (c("program"), c(type_name), c(fields));
    let mut seq = 0;
    // SAFETY: valid strings, a live or null handle, and a writable `seq`.
    let code = unsafe {
        f(
            handle,
            channel.as_ptr(),
            type_name.as_ptr(),
            fields.as_ptr(),
            &raw mut seq,
        )
    };
    if code == NTX_OK { Ok(seq) } else { Err(code) }
}

fn update_text<T>(f: TextFn<T>, handle: *const T, text: &str) -> c_int {
    let (channel, text) = (c("program"), c(text));
    // SAFETY: valid strings and a live or null handle.
    unsafe { f(handle, channel.as_ptr(), text.as_ptr()) }
}

/// Calls a function that takes a handle and one string: clear, announce, allow or revoke.
fn with_string<T>(f: StringFn<T>, handle: *const T, text: &str) -> c_int {
    let text = c(text);
    // SAFETY: a valid string and a live or null handle.
    unsafe { f(handle, text.as_ptr()) }
}

/// Calls a function that takes only a handle: reconnect.
fn with_handle<T>(f: HandleFn<T>, handle: *const T) -> c_int {
    // SAFETY: a live or null handle.
    unsafe { f(handle) }
}

fn event<T>(f: EventFn<T>, handle: *const T, timeout_ms: u32) -> Option<Value> {
    // SAFETY: a live or null handle.
    take_json(unsafe { f(handle, timeout_ms) })
}

fn source_info(source: *const NtxSource) -> Value {
    // SAFETY: a live handle.
    take_json(unsafe { ntx_source_info(source) }).unwrap()
}

fn source_free(source: *mut NtxSource) {
    // SAFETY: a handle from this library (or null), freed once.
    unsafe { ntx_source_free(source) };
}

fn connect(address: &str, config: &str) -> Result<*mut NtxReceiver, c_int> {
    let (address, config) = (c(address), c(config));
    let mut receiver = ptr::null_mut();
    // SAFETY: valid strings and out-pointer.
    let code = unsafe { ntx_receiver_connect(address.as_ptr(), config.as_ptr(), &raw mut receiver) };
    if code == NTX_OK { Ok(receiver) } else { Err(code) }
}

fn set_tally(receiver: *const NtxReceiver, program: bool) -> c_int {
    // SAFETY: a live or null handle.
    unsafe { ntx_receiver_set_tally(receiver, program, false) }
}

fn receiver_free(receiver: *mut NtxReceiver) {
    // SAFETY: a handle from this library (or null), freed once.
    unsafe { ntx_receiver_free(receiver) };
}

fn relay_start(config: &Value) -> *mut NtxRelay {
    let config = c(&config.to_string());
    let mut relay = ptr::null_mut();
    // SAFETY: a valid string and out-pointer.
    let code = unsafe { ntx_relay_start(config.as_ptr(), &raw mut relay) };
    assert_eq!(code, NTX_OK, "{}", last_error());
    relay
}

fn publisher_connect(address: &str, config: &str) -> Result<*mut NtxPublisher, c_int> {
    let (address, config) = (c(address), c(config));
    let mut publisher = ptr::null_mut();
    // SAFETY: valid strings and out-pointer.
    let code = unsafe { ntx_publisher_connect(address.as_ptr(), config.as_ptr(), &raw mut publisher) };
    if code == NTX_OK { Ok(publisher) } else { Err(code) }
}

fn render(fields: &str, pattern: &str) -> Option<String> {
    let (fields, pattern) = (c(fields), c(pattern));
    // SAFETY: valid strings.
    take_string(unsafe { ntx_render(fields.as_ptr(), pattern.as_ptr()) })
}

/// Polls until an event of `kind` arrives and returns its data.
fn wait_for(mut next: impl FnMut() -> Option<Value>, kind: &str) -> Value {
    loop {
        let event = next().expect("timed out");
        if event["event"] == kind {
            return event["data"].clone();
        }
    }
}

#[test]
fn versions() {
    assert_eq!(static_str(ntx_version()), env!("CARGO_PKG_VERSION"));
    assert_eq!(static_str(ntx_protocol_version()), ntx::PROTOCOL_VERSION);
}

#[test]
fn source_and_receiver_round_trip() {
    let source = start(&json!({
        "name": "C API (source)", "id": "c-api", "channels": ["program", "preview"],
        "bind": "127.0.0.1", "port": 0, "path": "/", "advertise": false
    }));
    let info = source_info(source);
    assert_eq!(
        (info["id"].as_str(), info["receivers"].as_u64()),
        (Some("c-api"), Some(0))
    );
    let address = format!("ws://{}/", info["address"].as_str().unwrap());

    assert_eq!(with_string(ntx_source_announce, source, LYRICS), NTX_OK);
    assert_eq!(update_text(ntx_source_update_text, source, "Welcome"), NTX_OK);

    let config = r#"{"name": "C API (receiver)", "id": "r", "channels": ["program"], "reconnect": true}"#;
    let receiver = connect(&address, config).unwrap();
    let next = || event(ntx_receiver_wait_event, receiver, 5000);
    assert_eq!(wait_for(next, "connected")["source"]["id"], "c-api");
    assert_eq!(wait_for(next, "announce")[0]["ntxType"], "com.example.lyrics");
    assert_eq!(wait_for(next, "update")["fields"]["text"], "Welcome");

    let seq = update(
        ntx_source_update,
        source,
        "com.example.lyrics",
        r#"{"line": "Amazing grace"}"#,
    )
    .unwrap();
    let update = wait_for(next, "update");
    assert_eq!(
        (update["seq"].as_u64(), update["fields"]["line"].as_str()),
        (Some(seq), Some("Amazing grace"))
    );

    assert_eq!(set_tally(receiver, true), NTX_OK);
    assert_eq!(
        wait_for(|| event(ntx_source_wait_event, source, 5000), "tally_changed"),
        json!({"program": true, "preview": false})
    );

    assert_eq!(with_string(ntx_source_clear, source, "program"), NTX_OK);
    assert_eq!(wait_for(next, "update")["fields"], json!({}));
    assert_eq!(event(ntx_receiver_wait_event, receiver, 0), None);

    receiver_free(receiver);
    source_free(source);
}

#[test]
fn errors_are_reported() {
    // SAFETY: a null config is allowed and reported.
    let code = unsafe { ntx_source_start(ptr::null(), ptr::null_mut()) };
    assert_eq!(code, NTX_ERR_ARGUMENT);
    assert!(last_error().contains("null"));
    let valid = c(r#"{"name":"x","advertise":false,"port":0}"#);
    // SAFETY: a valid string; the null out-pointer is reported.
    let code = unsafe { ntx_source_start(valid.as_ptr(), ptr::null_mut()) };
    assert_eq!(code, NTX_ERR_ARGUMENT);
    assert_eq!(
        source_start(&c(r#"{"name":"x","colour":"red"}"#)),
        Err(NTX_ERR_ARGUMENT)
    );
    assert!(last_error().contains("colour"));
    assert_eq!(
        source_start(&c(r#"{"name":"x","channels":["preview"]}"#)),
        Err(NTX_ERR_FAILED)
    );
    assert_eq!(source_start(c"\xff"), Err(NTX_ERR_ARGUMENT));
    assert!(last_error().contains("UTF-8"));

    let source = start(&json!({"name": "errors", "bind": "127.0.0.1", "port": 0, "advertise": false}));
    assert_eq!(
        update(ntx_source_update, source, "Bad Name", "{}"),
        Err(NTX_ERR_FAILED)
    );
    assert!(last_error().contains("invalid type name"));
    assert_eq!(
        with_string(ntx_source_clear, ptr::null(), "program"),
        NTX_ERR_ARGUMENT
    );
    assert_eq!(with_string(ntx_source_announce, source, "[1]"), NTX_ERR_ARGUMENT);
    assert_eq!(event(ntx_source_wait_event, ptr::null(), 0), None);

    assert_eq!(connect("http://x", r#"{"name":"r"}"#), Err(NTX_ERR_FAILED));
    assert_eq!(set_tally(ptr::null(), true), NTX_ERR_ARGUMENT);

    // SAFETY: freeing null is allowed.
    unsafe { ntx_string_free(ptr::null_mut()) };
    receiver_free(ptr::null_mut());
    // SAFETY: freeing null is allowed.
    unsafe { ntx_browser_free(ptr::null_mut()) };
    source_free(source);
}

#[test]
fn render_patterns() {
    let fields = r#"{"reference":"John 3:16","version":"KJV"}"#;
    assert_eq!(
        render(fields, "{reference} · {version}").as_deref(),
        Some("John 3:16 · KJV")
    );
    assert_eq!(render(fields, "{oops"), None);
    assert!(last_error().contains("pattern"));
}

#[test]
fn browser_finds_a_source() {
    let source = start(&json!({"name": "C API (browse)", "id": "c-api-browse", "port": 0}));
    let mut browser = ptr::null_mut();
    // SAFETY: a valid out-pointer.
    assert_eq!(unsafe { ntx_browser_start(&raw mut browser) }, NTX_OK);
    let found = loop {
        // SAFETY: a live handle.
        let event = take_json(unsafe { ntx_browser_wait_event(browser, 10_000) }).expect("not found");
        if event["event"] == "found" && event["data"]["id"] == "c-api-browse" {
            break event["data"].clone();
        }
    };
    assert_eq!(found["name"], "C API (browse)");
    // SAFETY: a handle from this library, freed once.
    unsafe { ntx_browser_free(browser) };
    source_free(source);
}

#[test]
fn publisher_through_relay() {
    let relay = relay_start(&json!({
        "name": "C API (relay)", "id": "c-relay", "bind": "127.0.0.1", "port": 0, "path": "/publish",
        "allowed_origins": ["https://a.example"], "advertise": false
    }));
    // SAFETY: a live handle.
    let info = take_json(unsafe { ntx_relay_info(relay) }).unwrap();
    let address = info["address"].as_str().unwrap();
    assert_eq!(
        with_string(ntx_relay_allow_origin, relay, "https://b.example"),
        NTX_OK
    );
    assert_eq!(
        with_string(ntx_relay_revoke_origin, relay, "https://a.example"),
        NTX_OK
    );

    let config =
        r#"{"name": "C API (publisher)", "id": "c-pub", "channels": ["program"], "reconnect": true}"#;
    let publisher = publisher_connect(&format!("ws://{address}/publish"), config).unwrap();
    assert_eq!(with_string(ntx_publisher_announce, publisher, LYRICS), NTX_OK);
    assert_eq!(
        update_text(ntx_publisher_update_text, publisher, "Welcome"),
        NTX_OK
    );
    assert_eq!(
        wait_for(|| event(ntx_publisher_wait_event, publisher, 5000), "connected")["relay"]["id"],
        "c-relay"
    );
    let served = wait_for(|| event(ntx_relay_wait_event, relay, 5000), "publisher_connected");
    assert_eq!(served["source"]["id"], "c-pub");

    let receiver = connect(
        &format!("127.0.0.1:{}", served["port"]),
        r#"{"name": "C API (receiver)"}"#,
    )
    .unwrap();
    let next = || event(ntx_receiver_wait_event, receiver, 5000);
    assert_eq!(wait_for(next, "announce")[0]["ntxType"], "com.example.lyrics");
    assert_eq!(wait_for(next, "update")["fields"]["text"], "Welcome");
    let seq = update(
        ntx_publisher_update,
        publisher,
        "com.example.lyrics",
        r#"{"line": "Amazing grace"}"#,
    );
    assert!(seq.is_ok());
    assert_eq!(wait_for(next, "update")["fields"]["line"], "Amazing grace");
    assert_eq!(with_string(ntx_publisher_clear, publisher, "program"), NTX_OK);
    assert_eq!(wait_for(next, "update")["fields"], json!({}));

    assert_eq!(set_tally(receiver, true), NTX_OK);
    assert_eq!(
        wait_for(|| event(ntx_publisher_wait_event, publisher, 5000), "tally"),
        json!({"program": true, "preview": false})
    );

    receiver_free(receiver);
    // SAFETY: handles from this library, freed once.
    unsafe { ntx_publisher_free(publisher) };
    assert_eq!(
        wait_for(
            || event(ntx_relay_wait_event, relay, 5000),
            "publisher_disconnected"
        )["connection"],
        1
    );
    // SAFETY: as above.
    unsafe { ntx_relay_free(relay) };
}

#[test]
fn publisher_and_relay_errors() {
    assert_eq!(
        publisher_connect("http://x", r#"{"name":"p"}"#),
        Err(NTX_ERR_FAILED)
    );
    assert_eq!(
        publisher_connect("localhost", r#"{"nme":"p"}"#),
        Err(NTX_ERR_ARGUMENT)
    );
    assert_eq!(
        with_string(ntx_relay_allow_origin, ptr::null(), "x"),
        NTX_ERR_ARGUMENT
    );
    let relay = relay_start(&json!({"name": "errors", "port": 0, "advertise": false}));
    assert_eq!(with_string(ntx_relay_allow_origin, relay, "null"), NTX_ERR_FAILED);
    assert!(last_error().contains("invalid origin"));
    assert_eq!(with_string(ntx_relay_revoke_origin, relay, "x"), NTX_ERR_FAILED);
    // SAFETY: a handle from this library, freed once.
    unsafe { ntx_relay_free(relay) };
    assert_eq!(event(ntx_relay_wait_event, ptr::null(), 0), None);
    // SAFETY: a null config is allowed and reported.
    let code = unsafe { ntx_relay_start(ptr::null(), ptr::null_mut()) };
    assert_eq!(code, NTX_ERR_ARGUMENT);
    // SAFETY: freeing null is allowed.
    unsafe { ntx_publisher_free(ptr::null_mut()) };
    // SAFETY: freeing null is allowed.
    unsafe { ntx_relay_free(ptr::null_mut()) };
}

/// Clause 11.3: after `pairing_required` a client waits until it is asked to try again.
fn stops_until_reconnect<T>(events: EventFn<T>, reconnect: HandleFn<T>, handle: *const T) {
    let next = || event(events, handle, 5000);
    assert_eq!(wait_for(next, "error")["code"], "pairing_required");
    wait_for(next, "disconnected");
    assert_eq!(event(events, handle, 1500), None);
    assert_eq!(with_handle(reconnect, ptr::null()), NTX_ERR_ARGUMENT);
    assert_eq!(with_handle(reconnect, handle), NTX_OK);
    assert_eq!(wait_for(next, "error")["code"], "pairing_required");
}

#[test]
fn clients_without_a_pairing_code_wait_for_reconnect() {
    let source = start(&json!({
        "name": "C API (paired)", "bind": "127.0.0.1", "port": 0, "pairing_code": "2468", "advertise": false
    }));
    let address = source_info(source)["address"].as_str().unwrap().to_string();
    let receiver = connect(&address, r#"{"name": "C API (no code)"}"#).unwrap();
    stops_until_reconnect(ntx_receiver_wait_event, ntx_receiver_reconnect, receiver);
    receiver_free(receiver);
    source_free(source);

    let relay =
        relay_start(&json!({"name": "paired", "port": 0, "pairing_code": "1357", "advertise": false}));
    // SAFETY: a live handle.
    let info = take_json(unsafe { ntx_relay_info(relay) }).unwrap();
    let url = format!("ws://{}/publish", info["address"].as_str().unwrap());
    let publisher = publisher_connect(&url, r#"{"name": "no code"}"#).unwrap();
    stops_until_reconnect(ntx_publisher_wait_event, ntx_publisher_reconnect, publisher);
    // SAFETY: handles from this library, freed once.
    unsafe { ntx_publisher_free(publisher) };
    // SAFETY: as above.
    unsafe { ntx_relay_free(relay) };
}
