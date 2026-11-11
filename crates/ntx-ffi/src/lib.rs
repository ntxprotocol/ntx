//! C ABI for NTX (Network Text Exchange).
//!
//! This is the one native interface that the C/C++, Python, Node, Java and .NET bindings wrap.
//! It is kept small and stable:
//!
//! - Objects are opaque handles: [`NtxSource`], [`NtxReceiver`], [`NtxBrowser`],
//!   [`NtxPublisher`], [`NtxRelay`].
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

use ntx::{
    Browser, Fields, Publisher, PublisherConfig, Receiver, ReceiverConfig, Relay, RelayConfig, Source,
    SourceConfig, Tally, TypeDescriptor,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

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

/// A publisher connected (or connecting) to a relay.
pub struct NtxPublisher(Publisher);

/// A running relay.
pub struct NtxRelay(Relay);

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

fn to_json(value: &impl Serialize) -> String {
    serde_json::to_string(value).expect("NTX events always serialise")
}

/// Handles whose events are read with `*_wait_event`.
trait Events {
    type Event: Serialize;
    fn wait_event(&self, timeout: Duration) -> Option<Self::Event>;
}

/// Handles that hold text and send it: sources and publishers.
trait Sends {
    fn update(&self, channel: &str, type_name: &str, fields: Fields) -> ntx::Result<u64>;
    fn update_text(&self, channel: &str, text: &str) -> ntx::Result<u64>;
    fn clear(&self, channel: &str) -> ntx::Result<u64>;
    fn announce(&self, types: Vec<TypeDescriptor>) -> ntx::Result<()>;
}

macro_rules! events {
    ($($handle:ty => $event:ty),*) => {$(
        impl Events for $handle {
            type Event = $event;
            fn wait_event(&self, timeout: Duration) -> Option<$event> {
                self.0.wait_event(timeout)
            }
        }
    )*};
}

events!(
    NtxSource => ntx::SourceEvent,
    NtxReceiver => ntx::ReceiverEvent,
    NtxBrowser => ntx::BrowserEvent,
    NtxPublisher => ntx::PublisherEvent,
    NtxRelay => ntx::RelayEvent
);

macro_rules! sends {
    ($($handle:ty),*) => {$(
        impl Sends for $handle {
            fn update(&self, channel: &str, type_name: &str, fields: Fields) -> ntx::Result<u64> {
                self.0.update(channel, type_name, fields)
            }
            fn update_text(&self, channel: &str, text: &str) -> ntx::Result<u64> {
                self.0.update_text(channel, text)
            }
            fn clear(&self, channel: &str) -> ntx::Result<u64> {
                self.0.clear(channel)
            }
            fn announce(&self, types: Vec<TypeDescriptor>) -> ntx::Result<()> {
                self.0.announce(types)
            }
        }
    )*};
}

sends!(NtxSource, NtxPublisher);

/// Waits up to `timeout_ms` for the next event of `handle`, as JSON, or null.
///
/// # Safety
///
/// `handle` is null or a live handle from this library.
unsafe fn wait_event<T: Events>(handle: *const T, timeout_ms: u32) -> *mut c_char {
    call_string(|| {
        // SAFETY: forwarded from the caller.
        let handle = unsafe { handle_arg(handle) }?;
        let timeout = Duration::from_millis(u64::from(timeout_ms));
        Ok(handle.wait_event(timeout).map(|e| to_json(&e)))
    })
}

/// Frees a handle. Null is ignored.
///
/// # Safety
///
/// `handle` is null or a handle from this library that has not been freed.
unsafe fn free<T>(handle: *mut T) {
    if !handle.is_null() {
        // SAFETY: the caller guarantees the handle came from `Box::into_raw` here.
        drop(unsafe { Box::from_raw(handle) });
    }
}

/// # Safety
///
/// Pointers are null or valid: NUL-terminated UTF-8 strings, a live handle, a writable `seq_out`.
unsafe fn update<T: Sends>(
    handle: *const T,
    channel: *const c_char,
    type_name: *const c_char,
    fields_json: *const c_char,
    seq_out: *mut u64,
) -> c_int {
    call(|| {
        // SAFETY: forwarded from the caller.
        let handle = unsafe { handle_arg(handle) }?;
        // SAFETY: as above.
        let channel = unsafe { str_arg(channel, "channel") }?;
        // SAFETY: as above.
        let type_name = unsafe { str_arg(type_name, "type_name") }?;
        // SAFETY: as above.
        let fields: Fields = unsafe { json_arg(fields_json, "fields_json") }?;
        let seq = handle.update(channel, type_name, fields)?;
        if !seq_out.is_null() {
            // SAFETY: not null, and the caller guarantees it is writable.
            unsafe { seq_out.write(seq) };
        }
        Ok(())
    })
}

/// # Safety
///
/// Pointers are null or valid: NUL-terminated UTF-8 strings and a live handle.
unsafe fn update_text<T: Sends>(handle: *const T, channel: *const c_char, text: *const c_char) -> c_int {
    call(|| {
        // SAFETY: forwarded from the caller.
        let handle = unsafe { handle_arg(handle) }?;
        // SAFETY: as above.
        let channel = unsafe { str_arg(channel, "channel") }?;
        // SAFETY: as above.
        let text = unsafe { str_arg(text, "text") }?;
        handle.update_text(channel, text)?;
        Ok(())
    })
}

/// # Safety
///
/// Pointers are null or valid: a NUL-terminated UTF-8 string and a live handle.
unsafe fn clear<T: Sends>(handle: *const T, channel: *const c_char) -> c_int {
    call(|| {
        // SAFETY: forwarded from the caller.
        let handle = unsafe { handle_arg(handle) }?;
        // SAFETY: as above.
        let channel = unsafe { str_arg(channel, "channel") }?;
        handle.clear(channel)?;
        Ok(())
    })
}

/// # Safety
///
/// Pointers are null or valid: a NUL-terminated UTF-8 string and a live handle.
unsafe fn announce<T: Sends>(handle: *const T, types_json: *const c_char) -> c_int {
    call(|| {
        // SAFETY: forwarded from the caller.
        let handle = unsafe { handle_arg(handle) }?;
        // SAFETY: as above.
        let types: Vec<TypeDescriptor> = unsafe { json_arg(types_json, "types_json") }?;
        handle.announce(types)?;
        Ok(())
    })
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
    // SAFETY: pointer rules are the caller's, as documented.
    unsafe { update(source, channel, type_name, fields_json, seq_out) }
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
    // SAFETY: pointer rules are the caller's, as documented.
    unsafe { update_text(source, channel, text) }
}

/// Clears `channel`.
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_source_clear(source: *const NtxSource, channel: *const c_char) -> c_int {
    // SAFETY: pointer rules are the caller's, as documented.
    unsafe { clear(source, channel) }
}

/// Sets the announced types: `types_json` is an array of type descriptors (spec clause 15.4).
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_source_announce(source: *const NtxSource, types_json: *const c_char) -> c_int {
    // SAFETY: pointer rules are the caller's, as documented.
    unsafe { announce(source, types_json) }
}

/// Waits up to `timeout_ms` (0 = just check) for the next event, as JSON such as
/// `{"event":"receiver_connected","data":{...}}`. Returns null when there is none.
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_source_wait_event(source: *const NtxSource, timeout_ms: u32) -> *mut c_char {
    // SAFETY: pointer rules are the caller's, as documented.
    unsafe { wait_event(source, timeout_ms) }
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
    // SAFETY: as documented, a handle from this library that has not been freed.
    unsafe { free(source) }
}

// ---------------------------------------------------------------------------------------------
// Receiver

/// Receiver or publisher settings, as JSON. Only `name` is required.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClientOptions {
    name: String,
    id: Option<String>,
    channels: Option<Vec<String>>,
    pairing_code: Option<String>,
    reconnect: Option<bool>,
}

macro_rules! from_client_options {
    ($($config:ty),*) => {$(
        impl From<ClientOptions> for $config {
            fn from(o: ClientOptions) -> Self {
                let mut config = <$config>::new(o.name);
                config.id = o.id.unwrap_or(config.id);
                config.channels = o.channels.unwrap_or(config.channels);
                config.pairing_code = o.pairing_code;
                config.reconnect = o.reconnect.unwrap_or(config.reconnect);
                config
            }
        }
    )*};
}

from_client_options!(ReceiverConfig, PublisherConfig);

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
        let options: ClientOptions = unsafe { json_arg(config_json, "config_json") }?;
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
    // SAFETY: pointer rules are the caller's, as documented.
    unsafe { wait_event(receiver, timeout_ms) }
}

/// Disconnects and frees the handle. Null is ignored.
///
/// # Safety
///
/// `receiver` is null or a handle from [`ntx_receiver_connect`] that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_receiver_free(receiver: *mut NtxReceiver) {
    // SAFETY: as documented, a handle from this library that has not been freed.
    unsafe { free(receiver) }
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
    // SAFETY: pointer rules are the caller's, as documented.
    unsafe { wait_event(browser, timeout_ms) }
}

/// Stops looking and frees the handle. Null is ignored.
///
/// # Safety
///
/// `browser` is null or a handle from [`ntx_browser_start`] that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_browser_free(browser: *mut NtxBrowser) {
    // SAFETY: as documented, a handle from this library that has not been freed.
    unsafe { free(browser) }
}

// ---------------------------------------------------------------------------------------------
// Publisher

/// Connects to the relay at `address` (a `ws://` URL, or `host[:port]` for the path
/// `/publish`). `config_json` is an object with `name` and optionally `id`, `channels`,
/// `pairing_code` and `reconnect`. On success `*out` receives the handle; free it with
/// [`ntx_publisher_free`].
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_publisher_connect(
    address: *const c_char,
    config_json: *const c_char,
    out: *mut *mut NtxPublisher,
) -> c_int {
    call(|| {
        // SAFETY: pointer rules are the caller's, as documented.
        let address = unsafe { str_arg(address, "address") }?;
        // SAFETY: as above.
        let options: ClientOptions = unsafe { json_arg(config_json, "config_json") }?;
        let publisher = Publisher::connect(address, options.into())?;
        // SAFETY: as above.
        unsafe { write_out(out, NtxPublisher(publisher)) }
    })
}

/// Replaces the item of `channel` with `fields_json` (an object) of type `type_name`, and sends
/// it to the relay. If `seq_out` is not null it receives the sequence number.
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_publisher_update(
    publisher: *const NtxPublisher,
    channel: *const c_char,
    type_name: *const c_char,
    fields_json: *const c_char,
    seq_out: *mut u64,
) -> c_int {
    // SAFETY: pointer rules are the caller's, as documented.
    unsafe { update(publisher, channel, type_name, fields_json, seq_out) }
}

/// Sends plain text on `channel` as an `ntx.text` item.
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_publisher_update_text(
    publisher: *const NtxPublisher,
    channel: *const c_char,
    text: *const c_char,
) -> c_int {
    // SAFETY: pointer rules are the caller's, as documented.
    unsafe { update_text(publisher, channel, text) }
}

/// Clears `channel`.
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_publisher_clear(
    publisher: *const NtxPublisher,
    channel: *const c_char,
) -> c_int {
    // SAFETY: pointer rules are the caller's, as documented.
    unsafe { clear(publisher, channel) }
}

/// Sets the announced types: `types_json` is an array of type descriptors (spec clause 15.4).
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_publisher_announce(
    publisher: *const NtxPublisher,
    types_json: *const c_char,
) -> c_int {
    // SAFETY: pointer rules are the caller's, as documented.
    unsafe { announce(publisher, types_json) }
}

/// Waits up to `timeout_ms` (0 = just check) for the next event, as JSON such as
/// `{"event":"tally","data":{"program":true,"preview":false}}`. Returns null when there is none.
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_publisher_wait_event(
    publisher: *const NtxPublisher,
    timeout_ms: u32,
) -> *mut c_char {
    // SAFETY: pointer rules are the caller's, as documented.
    unsafe { wait_event(publisher, timeout_ms) }
}

/// Disconnects and frees the handle. Null is ignored.
///
/// # Safety
///
/// `publisher` is null or a handle from [`ntx_publisher_connect`] that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_publisher_free(publisher: *mut NtxPublisher) {
    // SAFETY: as documented, a handle from this library that has not been freed.
    unsafe { free(publisher) }
}

// ---------------------------------------------------------------------------------------------
// Relay

/// Relay settings, as JSON. Only `name` is required.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RelayOptions {
    name: String,
    id: Option<String>,
    bind: Option<IpAddr>,
    port: Option<u16>,
    path: Option<String>,
    pairing_code: Option<String>,
    allowed_origins: Option<Vec<String>>,
    advertise: Option<bool>,
}

impl From<RelayOptions> for RelayConfig {
    fn from(o: RelayOptions) -> Self {
        let mut config = RelayConfig::new(o.name);
        config.id = o.id.unwrap_or(config.id);
        config.bind = o.bind.unwrap_or(config.bind);
        config.port = o.port.unwrap_or(config.port);
        config.path = o.path.unwrap_or(config.path);
        config.pairing_code = o.pairing_code;
        config.allowed_origins = o.allowed_origins.unwrap_or_default();
        config.advertise = o.advertise.unwrap_or(config.advertise);
        config
    }
}

/// Starts a relay, which turns publishing on (spec clause 14). `config_json` is an object with
/// `name` and optionally `id`, `bind`, `port`, `path`, `pairing_code`, `allowed_origins` and
/// `advertise` (see `RelayConfig` in the Rust docs). On success `*out` receives the handle; free
/// it with [`ntx_relay_free`].
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_relay_start(config_json: *const c_char, out: *mut *mut NtxRelay) -> c_int {
    call(|| {
        // SAFETY: pointer rules are the caller's, as documented.
        let options: RelayOptions = unsafe { json_arg(config_json, "config_json") }?;
        let relay = Relay::start(options.into())?;
        // SAFETY: as above.
        unsafe { write_out(out, NtxRelay(relay)) }
    })
}

/// Allows web pages from `origin` (e.g. `"https://example.com"`) to publish. Call it when the
/// user agrees to an `origin_requested` event.
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_relay_allow_origin(relay: *const NtxRelay, origin: *const c_char) -> c_int {
    call(|| {
        // SAFETY: pointer rules are the caller's, as documented.
        let relay = unsafe { handle_arg(relay) }?;
        // SAFETY: as above.
        relay.0.allow_origin(unsafe { str_arg(origin, "origin") }?);
        Ok(())
    })
}

/// Withdraws permission from `origin` and disconnects its publishers.
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_relay_revoke_origin(relay: *const NtxRelay, origin: *const c_char) -> c_int {
    call(|| {
        // SAFETY: pointer rules are the caller's, as documented.
        let relay = unsafe { handle_arg(relay) }?;
        // SAFETY: as above.
        relay.0.revoke_origin(unsafe { str_arg(origin, "origin") }?);
        Ok(())
    })
}

/// Waits up to `timeout_ms` (0 = just check) for the next event, as JSON such as
/// `{"event":"origin_requested","data":{"origin":"https://example.com"}}`. Returns null when
/// there is none.
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_relay_wait_event(relay: *const NtxRelay, timeout_ms: u32) -> *mut c_char {
    // SAFETY: pointer rules are the caller's, as documented.
    unsafe { wait_event(relay, timeout_ms) }
}

/// The address the relay listens on, as JSON: `{"address":"127.0.0.1:7686"}`.
///
/// # Safety
///
/// Pointers must be null or valid: NUL-terminated UTF-8 strings, live handles (see `ntx.h`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_relay_info(relay: *const NtxRelay) -> *mut c_char {
    call_string(|| {
        // SAFETY: pointer rules are the caller's, as documented.
        let relay = unsafe { handle_arg(relay) }?;
        Ok(Some(to_json(
            &serde_json::json!({ "address": relay.0.local_addr() }),
        )))
    })
}

/// Stops the relay, disconnects its publishers and frees the handle. Null is ignored.
///
/// # Safety
///
/// `relay` is null or a handle from [`ntx_relay_start`] that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ntx_relay_free(relay: *mut NtxRelay) {
    // SAFETY: as documented, a handle from this library that has not been freed.
    unsafe { free(relay) }
}
