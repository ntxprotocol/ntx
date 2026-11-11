//! C ABI for NTX (Network Text Exchange).
//!
//! This is the one native interface that the C/C++, Python, Node, Java and .NET bindings wrap.
//! It is kept small and stable:
//!
//! - Objects are opaque handles: [`NtxSource`], [`NtxReceiver`], [`NtxBrowser`].
//! - Settings, fields and events are UTF-8 **JSON strings**, so the ABI does not change when a
//!   field is added, and every language can read them with its own JSON parser.
//! - Events are **polled** with `*_wait_event`; nothing calls back into the host language.
//! - Functions return [`NTX_OK`] or a negative code; [`ntx_last_error`] explains the failure.
//! - Strings returned as `char *` belong to the caller and are freed with [`ntx_string_free`].
//!
//! # Safety
//!
//! Every pointer argument must be null or valid for the call: strings are NUL-terminated UTF-8,
//! handles come from this library and have not been freed, and out-pointers point to writable
//! memory. Handles may be used from any thread.
#![allow(unsafe_code)] // a C ABI needs raw pointers; every unsafe block has a SAFETY comment

use std::cell::RefCell;
use std::ffi::{CStr, CString, c_char, c_int};
use std::net::IpAddr;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::time::Duration;

use ntx::{Browser, Fields, Receiver, ReceiverConfig, Source, SourceConfig, Tally, TypeDescriptor};
use serde::Deserialize;
use serde::de::DeserializeOwned;

/// Success.
pub const NTX_OK: c_int = 0;
/// A null pointer, invalid UTF-8 or invalid JSON was passed.
pub const NTX_ERR_ARGUMENT: c_int = -1;
/// The operation failed; see [`ntx_last_error`].
pub const NTX_ERR_FAILED: c_int = -2;

/// A running source.
pub struct NtxSource(Source);

/// A receiver connected (or connecting) to one source.
pub struct NtxReceiver(Receiver);

/// An mDNS browser looking for sources.
pub struct NtxBrowser(Browser);

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::default());
}

/// A failed call: the result code and a message for [`ntx_last_error`].
struct Failure(c_int, String);

impl From<ntx::Error> for Failure {
    fn from(e: ntx::Error) -> Self {
        Failure(NTX_ERR_FAILED, e.to_string())
    }
}

fn argument(message: impl Into<String>) -> Failure {
    Failure(NTX_ERR_ARGUMENT, message.into())
}

fn set_last_error(message: &str) {
    let message = CString::new(message.replace('\0', " ")).unwrap_or_default();
    LAST_ERROR.with(|e| *e.borrow_mut() = message);
}

/// Runs `body`, turning failures and panics into a result code.
fn call(body: impl FnOnce() -> Result<(), Failure>) -> c_int {
    let failure = match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(())) => return NTX_OK,
        Ok(Err(failure)) => failure,
        Err(_) => Failure(NTX_ERR_FAILED, "internal error (panic)".into()),
    };
    set_last_error(&failure.1);
    failure.0
}

/// Like [`call`] for functions that return a string, or null on failure.
fn call_string(body: impl FnOnce() -> Result<Option<String>, Failure>) -> *mut c_char {
    let mut result = ptr::null_mut();
    call(|| {
        result = body()?.map_or(ptr::null_mut(), |s| into_c_string(&s));
        Ok(())
    });
    result
}

fn into_c_string(s: &str) -> *mut c_char {
    CString::new(s.replace('\0', " ")).unwrap_or_default().into_raw()
}

/// # Safety
///
/// `ptr` is null or a NUL-terminated string valid for `'a`.
unsafe fn str_arg<'a>(ptr: *const c_char, what: &str) -> Result<&'a str, Failure> {
    if ptr.is_null() {
        return Err(argument(format!("{what} is null")));
    }
    // SAFETY: not null, and the caller guarantees a valid NUL-terminated string.
    let bytes = unsafe { CStr::from_ptr(ptr) };
    bytes
        .to_str()
        .map_err(|_| argument(format!("{what} is not valid UTF-8")))
}

/// # Safety
///
/// As [`str_arg`].
unsafe fn json_arg<T: DeserializeOwned>(ptr: *const c_char, what: &str) -> Result<T, Failure> {
    // SAFETY: forwarded from the caller.
    let text = unsafe { str_arg(ptr, what) }?;
    serde_json::from_str(text).map_err(|e| argument(format!("{what}: {e}")))
}

/// # Safety
///
/// `handle` is null or a live handle from this library.
unsafe fn handle_arg<'a, T>(handle: *const T) -> Result<&'a T, Failure> {
    // SAFETY: the caller guarantees the handle is null or live.
    unsafe { handle.as_ref() }.ok_or_else(|| argument("handle is null"))
}

/// # Safety
///
/// `out` is null or points to writable memory for one pointer.
unsafe fn write_out<T>(out: *mut *mut T, value: T) -> Result<(), Failure> {
    if out.is_null() {
        return Err(argument("out pointer is null"));
    }
    // SAFETY: not null, and the caller guarantees it is writable.
    unsafe { out.write(Box::into_raw(Box::new(value))) };
    Ok(())
}

fn to_json(value: &impl serde::Serialize) -> String {
    serde_json::to_string(value).expect("NTX events always serialise")
}

fn millis(timeout_ms: u32) -> Duration {
    Duration::from_millis(u64::from(timeout_ms))
}

// ---------------------------------------------------------------------------------------------
// Library

/// The library version, e.g. `"0.1.0"`. The string is static; do not free it.
#[unsafe(no_mangle)]
pub extern "C" fn ntx_version() -> *const c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr().cast()
}

/// The protocol version this library speaks, e.g. `"0.1"`. Static; do not free it.
#[unsafe(no_mangle)]
pub extern "C" fn ntx_protocol_version() -> *const c_char {
    c"0.1".as_ptr()
}

/// The message of the last failed call on this thread. Valid until the next call on this
/// thread; do not free it.
#[unsafe(no_mangle)]
pub extern "C" fn ntx_last_error() -> *const c_char {
    LAST_ERROR.with(|e| e.borrow().as_ptr())
}

/// Frees a string returned by this library. Null is ignored.
///
/// # Safety
///
/// `s` is null or a string returned by this library that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_string_free(s: *mut c_char) {
    if !s.is_null() {
        // SAFETY: the caller guarantees `s` came from `CString::into_raw` in this library.
        drop(unsafe { CString::from_raw(s) });
    }
}

/// Renders a binding (spec clause 16): `pattern` such as `"{reference} · {version}"` filled
/// from `fields_json`. Returns null on failure.
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_render(fields_json: *const c_char, pattern: *const c_char) -> *mut c_char {
    call_string(|| {
        // SAFETY: pointer rules are the caller's, as documented.
        let fields: Fields = unsafe { json_arg(fields_json, "fields_json") }?;
        // SAFETY: as above.
        let pattern = ntx::binding::Pattern::parse(unsafe { str_arg(pattern, "pattern") }?)?;
        Ok(Some(pattern.render(&fields)))
    })
}

// ---------------------------------------------------------------------------------------------
// Source

/// Source settings, as JSON. Only `name` is required.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceOptions {
    name: String,
    id: Option<String>,
    channels: Option<Vec<String>>,
    bind: Option<IpAddr>,
    port: Option<u16>,
    path: Option<String>,
    pairing_code: Option<String>,
    advertise: Option<bool>,
}

impl From<SourceOptions> for SourceConfig {
    fn from(o: SourceOptions) -> Self {
        let mut config = SourceConfig::new(o.name);
        config.id = o.id.unwrap_or(config.id);
        config.channels = o.channels.unwrap_or(config.channels);
        config.bind = o.bind.unwrap_or(config.bind);
        config.port = o.port.unwrap_or(config.port);
        config.path = o.path.unwrap_or(config.path);
        config.pairing_code = o.pairing_code;
        config.advertise = o.advertise.unwrap_or(config.advertise);
        config
    }
}

/// Starts a source. `config_json` is an object with `name` and optionally `id`, `channels`,
/// `bind`, `port`, `path`, `pairing_code` and `advertise` (see `SourceConfig` in the Rust docs).
/// On success `*out` receives the handle; free it with [`ntx_source_free`].
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_source_start(config_json: *const c_char, out: *mut *mut NtxSource) -> c_int {
    call(|| {
        // SAFETY: pointer rules are the caller's, as documented.
        let options: SourceOptions = unsafe { json_arg(config_json, "config_json") }?;
        let source = Source::start(options.into())?;
        // SAFETY: as above.
        unsafe { write_out(out, NtxSource(source)) }
    })
}

/// Replaces the item of `channel` with `fields_json` (an object) of type `type_name`.
/// If `seq_out` is not null it receives the sequence number.
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_source_update(
    source: *const NtxSource,
    channel: *const c_char,
    type_name: *const c_char,
    fields_json: *const c_char,
    seq_out: *mut u64,
) -> c_int {
    call(|| {
        // SAFETY: pointer rules are the caller's, as documented.
        let source = unsafe { handle_arg(source) }?;
        // SAFETY: as above.
        let channel = unsafe { str_arg(channel, "channel") }?;
        // SAFETY: as above.
        let type_name = unsafe { str_arg(type_name, "type_name") }?;
        // SAFETY: as above.
        let fields: Fields = unsafe { json_arg(fields_json, "fields_json") }?;
        let seq = source.0.update(channel, type_name, fields)?;
        if !seq_out.is_null() {
            // SAFETY: not null, and the caller guarantees it is writable.
            unsafe { seq_out.write(seq) };
        }
        Ok(())
    })
}

/// Sends plain text on `channel` as an `ntx.text` item.
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_source_update_text(
    source: *const NtxSource,
    channel: *const c_char,
    text: *const c_char,
) -> c_int {
    call(|| {
        // SAFETY: pointer rules are the caller's, as documented.
        let source = unsafe { handle_arg(source) }?;
        // SAFETY: as above.
        let channel = unsafe { str_arg(channel, "channel") }?;
        // SAFETY: as above.
        let text = unsafe { str_arg(text, "text") }?;
        source.0.update_text(channel, text)?;
        Ok(())
    })
}

/// Clears `channel`.
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_source_clear(source: *const NtxSource, channel: *const c_char) -> c_int {
    call(|| {
        // SAFETY: pointer rules are the caller's, as documented.
        let source = unsafe { handle_arg(source) }?;
        // SAFETY: as above.
        let channel = unsafe { str_arg(channel, "channel") }?;
        source.0.clear(channel)?;
        Ok(())
    })
}

/// Sets the announced types: `types_json` is an array of type descriptors (spec clause 15.4).
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_source_announce(source: *const NtxSource, types_json: *const c_char) -> c_int {
    call(|| {
        // SAFETY: pointer rules are the caller's, as documented.
        let source = unsafe { handle_arg(source) }?;
        // SAFETY: as above.
        let types: Vec<TypeDescriptor> = unsafe { json_arg(types_json, "types_json") }?;
        source.0.announce(types)?;
        Ok(())
    })
}

/// Waits up to `timeout_ms` (0 = just check) for the next event, as JSON such as
/// `{"event":"receiver_connected","data":{...}}`. Returns null when there is none.
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_source_wait_event(source: *const NtxSource, timeout_ms: u32) -> *mut c_char {
    call_string(|| {
        // SAFETY: pointer rules are the caller's, as documented.
        let source = unsafe { handle_arg(source) }?;
        Ok(source.0.wait_event(millis(timeout_ms)).map(|e| to_json(&e)))
    })
}

/// The source's identity, address and receiver count, as JSON:
/// `{"id":"…","name":"…","address":"0.0.0.0:7686","receivers":0}`.
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_source_info(source: *const NtxSource) -> *mut c_char {
    call_string(|| {
        // SAFETY: pointer rules are the caller's, as documented.
        let source = unsafe { handle_arg(source) }?;
        let identity = source.0.identity();
        Ok(Some(to_json(&serde_json::json!({
            "id": identity.id,
            "name": identity.name,
            "address": source.0.local_addr(),
            "receivers": source.0.receiver_count(),
        }))))
    })
}

/// Stops the source and frees the handle. Null is ignored.
///
/// # Safety
///
/// `source` is null or a handle from [`ntx_source_start`] that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_source_free(source: *mut NtxSource) {
    if !source.is_null() {
        // SAFETY: the caller guarantees the handle came from `Box::into_raw` here.
        drop(unsafe { Box::from_raw(source) });
    }
}

// ---------------------------------------------------------------------------------------------
// Receiver

/// Receiver settings, as JSON. Only `name` is required.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiverOptions {
    name: String,
    id: Option<String>,
    channels: Option<Vec<String>>,
    pairing_code: Option<String>,
    reconnect: Option<bool>,
}

impl From<ReceiverOptions> for ReceiverConfig {
    fn from(o: ReceiverOptions) -> Self {
        let mut config = ReceiverConfig::new(o.name);
        config.id = o.id.unwrap_or(config.id);
        config.channels = o.channels.unwrap_or(config.channels);
        config.pairing_code = o.pairing_code;
        config.reconnect = o.reconnect.unwrap_or(config.reconnect);
        config
    }
}

/// Connects to the source at `address` (`host`, `host:port` or a `ws://` URL). `config_json`
/// is an object with `name` and optionally `id`, `channels`, `pairing_code` and `reconnect`.
/// On success `*out` receives the handle; free it with [`ntx_receiver_free`].
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_receiver_connect(
    address: *const c_char,
    config_json: *const c_char,
    out: *mut *mut NtxReceiver,
) -> c_int {
    call(|| {
        // SAFETY: pointer rules are the caller's, as documented.
        let address = unsafe { str_arg(address, "address") }?;
        // SAFETY: as above.
        let options: ReceiverOptions = unsafe { json_arg(config_json, "config_json") }?;
        let receiver = Receiver::connect(address, options.into())?;
        // SAFETY: as above.
        unsafe { write_out(out, NtxReceiver(receiver)) }
    })
}

/// Tells the source whether its text is on air (`program`) or in preview.
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_receiver_set_tally(
    receiver: *const NtxReceiver,
    program: bool,
    preview: bool,
) -> c_int {
    call(|| {
        // SAFETY: pointer rules are the caller's, as documented.
        let receiver = unsafe { handle_arg(receiver) }?;
        receiver.0.set_tally(Tally { program, preview });
        Ok(())
    })
}

/// Waits up to `timeout_ms` (0 = just check) for the next event, as JSON such as
/// `{"event":"update","data":{"channel":"program","seq":1,"type":"ntx.text","fields":{…}}}`.
/// Returns null when there is none.
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_receiver_wait_event(
    receiver: *const NtxReceiver,
    timeout_ms: u32,
) -> *mut c_char {
    call_string(|| {
        // SAFETY: pointer rules are the caller's, as documented.
        let receiver = unsafe { handle_arg(receiver) }?;
        Ok(receiver.0.wait_event(millis(timeout_ms)).map(|e| to_json(&e)))
    })
}

/// Disconnects and frees the handle. Null is ignored.
///
/// # Safety
///
/// `receiver` is null or a handle from [`ntx_receiver_connect`] that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_receiver_free(receiver: *mut NtxReceiver) {
    if !receiver.is_null() {
        // SAFETY: the caller guarantees the handle came from `Box::into_raw` here.
        drop(unsafe { Box::from_raw(receiver) });
    }
}

// ---------------------------------------------------------------------------------------------
// Browser

/// Starts looking for sources with mDNS. On success `*out` receives the handle; free it with
/// [`ntx_browser_free`].
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_browser_start(out: *mut *mut NtxBrowser) -> c_int {
    call(|| {
        let browser = Browser::start()?;
        // SAFETY: pointer rules are the caller's, as documented.
        unsafe { write_out(out, NtxBrowser(browser)) }
    })
}

/// Waits up to `timeout_ms` (0 = just check) for the next event, as JSON such as
/// `{"event":"found","data":{"id":"…","name":"…","url":"ws://…",…}}`. Returns null when there
/// is none.
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_browser_wait_event(browser: *const NtxBrowser, timeout_ms: u32) -> *mut c_char {
    call_string(|| {
        // SAFETY: pointer rules are the caller's, as documented.
        let browser = unsafe { handle_arg(browser) }?;
        Ok(browser.0.wait_event(millis(timeout_ms)).map(|e| to_json(&e)))
    })
}

/// Stops looking and frees the handle. Null is ignored.
///
/// # Safety
///
/// `browser` is null or a handle from [`ntx_browser_start`] that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_browser_free(browser: *mut NtxBrowser) {
    if !browser.is_null() {
        // SAFETY: the caller guarantees the handle came from `Box::into_raw` here.
        drop(unsafe { Box::from_raw(browser) });
    }
}
