//! The C ABI called the way a binding calls it: raw pointers, JSON strings and polling.
//!
//! Each C function is wrapped once in a small safe helper, the same way a language binding
//! would, so every `unsafe` block has one operation and one reason.
#![allow(unsafe_code)] // calling a C ABI needs unsafe; each block says why it is sound

use std::ffi::{CStr, CString, c_char, c_int};
use std::ptr;

use ntx_ffi::*;
use serde_json::{Value, json};

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

fn update(source: *const NtxSource, type_name: &str, fields: &str) -> Result<u64, c_int> {
    let (channel, type_name, fields) = (c("program"), c(type_name), c(fields));
    let mut seq = 0;
    // SAFETY: valid strings, a live or null handle, and a writable `seq`.
    let code = unsafe {
        ntx_source_update(
            source,
            channel.as_ptr(),
            type_name.as_ptr(),
            fields.as_ptr(),
            &raw mut seq,
        )
    };
    if code == NTX_OK { Ok(seq) } else { Err(code) }
}

fn update_text(source: *const NtxSource, text: &str) -> c_int {
    let (channel, text) = (c("program"), c(text));
    // SAFETY: valid strings and a live or null handle.
    unsafe { ntx_source_update_text(source, channel.as_ptr(), text.as_ptr()) }
}

fn clear(source: *const NtxSource) -> c_int {
    let channel = c("program");
    // SAFETY: a valid string and a live or null handle.
    unsafe { ntx_source_clear(source, channel.as_ptr()) }
}

fn announce(source: *const NtxSource, types: &str) -> c_int {
    let types = c(types);
    // SAFETY: a valid string and a live or null handle.
    unsafe { ntx_source_announce(source, types.as_ptr()) }
}

fn source_event(source: *const NtxSource, timeout_ms: u32) -> Option<Value> {
    // SAFETY: a live or null handle.
    take_json(unsafe { ntx_source_wait_event(source, timeout_ms) })
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

fn receiver_event(receiver: *const NtxReceiver, timeout_ms: u32) -> Option<Value> {
    // SAFETY: a live handle.
    take_json(unsafe { ntx_receiver_wait_event(receiver, timeout_ms) })
}

fn receiver_free(receiver: *mut NtxReceiver) {
    // SAFETY: a handle from this library (or null), freed once.
    unsafe { ntx_receiver_free(receiver) };
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

    assert_eq!(
        announce(
            source,
            r#"[{"ntxType": "com.example.lyrics", "schema": {"type": "object"}}]"#
        ),
        NTX_OK
    );
    assert_eq!(update_text(source, "Welcome"), NTX_OK);

    let config = r#"{"name": "C API (receiver)", "id": "r", "channels": ["program"], "reconnect": true}"#;
    let receiver = connect(&address, config).unwrap();
    let next = || receiver_event(receiver, 5000);
    assert_eq!(wait_for(next, "connected")["source"]["id"], "c-api");
    assert_eq!(wait_for(next, "announce")[0]["ntxType"], "com.example.lyrics");
    assert_eq!(wait_for(next, "update")["fields"]["text"], "Welcome");

    let seq = update(source, "com.example.lyrics", r#"{"line": "Amazing grace"}"#).unwrap();
    let update = wait_for(next, "update");
    assert_eq!(
        (update["seq"].as_u64(), update["fields"]["line"].as_str()),
        (Some(seq), Some("Amazing grace"))
    );

    assert_eq!(set_tally(receiver, true), NTX_OK);
    assert_eq!(
        wait_for(|| source_event(source, 5000), "tally_changed"),
        json!({"program": true, "preview": false})
    );

    assert_eq!(clear(source), NTX_OK);
    assert_eq!(wait_for(next, "update")["fields"], json!({}));
    assert_eq!(receiver_event(receiver, 0), None);

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
    assert_eq!(update(source, "Bad Name", "{}"), Err(NTX_ERR_FAILED));
    assert!(last_error().contains("invalid type name"));
    assert_eq!(clear(ptr::null()), NTX_ERR_ARGUMENT);
    assert_eq!(announce(source, "[1]"), NTX_ERR_ARGUMENT);
    assert_eq!(source_event(ptr::null(), 0), None);

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
