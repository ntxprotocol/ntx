//! Python bindings for NTX (Network Text Exchange), imported as `ntxprotocol._ntx`.
//!
//! This module is thin on purpose. Fields, type descriptors and events cross as JSON strings,
//! and the `ntxprotocol` package turns them into typed Python objects.
//!
//! - Events are **polled** with `wait_event`; nothing calls back into Python.
//! - Every call that can wait or block releases the GIL.
//! - `close()` stops the object at once; later calls raise `ClosedError`.

use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{PoisonError, RwLock};
use std::time::{Duration, Instant};

use ntx::{Browser, Fields, Receiver, ReceiverConfig, Source, SourceConfig, Tally, TypeDescriptor};
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;

create_exception!(_ntx, NtxError, PyException, "Base class of every NTX error.");
create_exception!(_ntx, ClosedError, NtxError, "The object was closed.");
create_exception!(_ntx, NetworkError, NtxError, "A network or OS error.");
create_exception!(_ntx, MdnsError, NtxError, "mDNS could not start or register.");
create_exception!(
    _ntx,
    InvalidTypeNameError,
    NtxError,
    "Invalid type name (clause 15.2)."
);
create_exception!(_ntx, UnknownChannelError, NtxError, "No such channel.");
create_exception!(
    _ntx,
    MessageTooLargeError,
    NtxError,
    "Over the size limit (clause 18.1)."
);
create_exception!(_ntx, InvalidAddressError, NtxError, "Invalid address.");
create_exception!(
    _ntx,
    InvalidPatternError,
    NtxError,
    "Invalid pattern (clause 16.3)."
);
create_exception!(_ntx, InvalidMessageError, NtxError, "A message is not valid NTX.");

/// Maps each [`ntx::Error`] to its Python exception class.
#[allow(clippy::needless_pass_by_value)] // takes ownership so it reads as `.map_err(error)`
fn error(e: ntx::Error) -> PyErr {
    let message = e.to_string();
    match e {
        ntx::Error::Io(_) => NetworkError::new_err(message),
        ntx::Error::Mdns(_) => MdnsError::new_err(message),
        ntx::Error::InvalidTypeName(_) => InvalidTypeNameError::new_err(message),
        ntx::Error::UnknownChannel(_) => UnknownChannelError::new_err(message),
        ntx::Error::TooLarge(_) => MessageTooLargeError::new_err(message),
        ntx::Error::InvalidAddress(_) => InvalidAddressError::new_err(message),
        ntx::Error::InvalidPattern(_) => InvalidPatternError::new_err(message),
        ntx::Error::InvalidMessage(_) => InvalidMessageError::new_err(message),
    }
}

fn from_json<T: DeserializeOwned>(json: &str, what: &str) -> PyResult<T> {
    serde_json::from_str(json).map_err(|e| PyValueError::new_err(format!("{what}: {e}")))
}

/// How long one slice of a wait lasts. Between slices `close()` can take the object and
/// Ctrl-C can interrupt the wait.
const SLICE: Duration = Duration::from_millis(50);

/// A native object that `close()` can stop while another thread waits on it.
///
/// Waits hold the read lock for one [`SLICE`] at a time, so `close()` (which sets `closed` and
/// then takes the write lock) returns within one slice and the waiter then gets `ClosedError`.
struct Handle<T> {
    object: RwLock<Option<T>>,
    closed: AtomicBool,
}

impl<T: Send + Sync> Handle<T> {
    fn new(value: T) -> Self {
        Handle {
            object: RwLock::new(Some(value)),
            closed: AtomicBool::new(false),
        }
    }

    /// Runs `body` on the object with the GIL released.
    fn with<R: Send>(&self, py: Python<'_>, body: impl FnOnce(&T) -> ntx::Result<R> + Send) -> PyResult<R> {
        py.detach(|| {
            if self.closed.load(Ordering::Acquire) {
                return None;
            }
            let guard = self.object.read().unwrap_or_else(PoisonError::into_inner);
            guard.as_ref().map(body)
        })
        .ok_or_else(|| ClosedError::new_err("closed"))?
        .map_err(error)
    }

    /// Waits up to `timeout` seconds (0 = just check, infinity = no limit) for the next event,
    /// as JSON. Waits in slices, checking for Ctrl-C and `close()` between them.
    fn wait_event<E: Serialize + Send>(
        &self,
        py: Python<'_>,
        timeout: f64,
        wait: impl Fn(&T, Duration) -> Option<E> + Sync,
    ) -> PyResult<Option<String>> {
        // No deadline means wait forever: for infinity, and for timeouts too large for `Instant`.
        let deadline = if timeout == f64::INFINITY {
            None
        } else {
            let timeout = Duration::try_from_secs_f64(timeout)
                .map_err(|e| PyValueError::new_err(format!("timeout: {e}")))?;
            Instant::now().checked_add(timeout)
        };
        loop {
            let slice = deadline.map_or(SLICE, |d| d.saturating_duration_since(Instant::now()).min(SLICE));
            if let Some(event) = self.with(py, |object| Ok(wait(object, slice)))? {
                return Ok(Some(
                    serde_json::to_string(&event).expect("NTX events always serialise"),
                ));
            }
            if deadline.is_some_and(|d| Instant::now() >= d) {
                return Ok(None);
            }
            py.check_signals()?;
        }
    }

    /// Stops the object, at most one [`SLICE`] after a waiting thread's current slice began.
    /// Stopping mDNS can take a moment, so the GIL is released.
    fn close(&self, py: Python<'_>) {
        self.closed.store(true, Ordering::Release);
        py.detach(|| drop(self.object.write().unwrap_or_else(PoisonError::into_inner).take()));
    }
}

/// Renders a binding pattern such as `"{reference} · {version}"` from fields (clause 16).
#[pyfunction]
fn render(fields_json: &str, pattern: &str) -> PyResult<String> {
    let fields: Fields = from_json(fields_json, "fields")?;
    let pattern = ntx::binding::Pattern::parse(pattern).map_err(error)?;
    Ok(pattern.render(&fields))
}

/// A running source.
#[pyclass(frozen, name = "Source", module = "ntxprotocol._ntx")]
struct PySource(Handle<Source>);

#[pymethods]
impl PySource {
    #[new]
    #[pyo3(signature = (name, *, id, channels, bind, port, path, pairing_code, advertise))]
    #[allow(clippy::too_many_arguments)] // one keyword argument per `SourceConfig` field
    fn start(
        py: Python<'_>,
        name: String,
        id: Option<String>,
        channels: Vec<String>,
        bind: &str,
        port: u16,
        path: String,
        pairing_code: Option<String>,
        advertise: bool,
    ) -> PyResult<Self> {
        let bind: IpAddr = bind
            .parse()
            .map_err(|_| error(ntx::Error::InvalidAddress(bind.to_string())))?;
        let defaults = SourceConfig::new(name);
        let config = SourceConfig {
            id: id.unwrap_or(defaults.id),
            channels,
            bind,
            port,
            path,
            pairing_code,
            advertise,
            ..defaults
        };
        let source = py.detach(|| Source::start(config)).map_err(error)?;
        Ok(PySource(Handle::new(source)))
    }

    fn update(&self, py: Python<'_>, channel: &str, type_name: &str, fields_json: &str) -> PyResult<u64> {
        let fields: Fields = from_json(fields_json, "fields")?;
        self.0.with(py, |s| s.update(channel, type_name, fields))
    }

    fn update_text(&self, py: Python<'_>, channel: &str, text: &str) -> PyResult<u64> {
        self.0.with(py, |s| s.update_text(channel, text))
    }

    fn clear(&self, py: Python<'_>, channel: &str) -> PyResult<u64> {
        self.0.with(py, |s| s.clear(channel))
    }

    fn announce(&self, py: Python<'_>, types_json: &str) -> PyResult<()> {
        let types: Vec<TypeDescriptor> = from_json(types_json, "types")?;
        self.0.with(py, |s| s.announce(types))
    }

    fn wait_event(&self, py: Python<'_>, timeout: f64) -> PyResult<Option<String>> {
        self.0.wait_event(py, timeout, Source::wait_event)
    }

    #[getter]
    fn id(&self, py: Python<'_>) -> PyResult<String> {
        self.0.with(py, |s| Ok(s.identity().id.clone()))
    }

    #[getter]
    fn name(&self, py: Python<'_>) -> PyResult<String> {
        self.0.with(py, |s| Ok(s.identity().name.clone()))
    }

    #[getter]
    fn address(&self, py: Python<'_>) -> PyResult<String> {
        self.0.with(py, |s| Ok(s.local_addr().to_string()))
    }

    #[getter]
    fn port(&self, py: Python<'_>) -> PyResult<u16> {
        self.0.with(py, |s| Ok(s.local_addr().port()))
    }

    #[getter]
    fn receiver_count(&self, py: Python<'_>) -> PyResult<usize> {
        self.0.with(py, |s| Ok(s.receiver_count()))
    }

    fn close(&self, py: Python<'_>) {
        self.0.close(py);
    }
}

/// A receiver connected (or connecting) to one source.
#[pyclass(frozen, name = "Receiver", module = "ntxprotocol._ntx")]
struct PyReceiver(Handle<Receiver>);

#[pymethods]
impl PyReceiver {
    #[new]
    #[pyo3(signature = (address, name, *, id, channels, pairing_code, reconnect))]
    fn connect(
        address: &str,
        name: String,
        id: Option<String>,
        channels: Vec<String>,
        pairing_code: Option<String>,
        reconnect: bool,
    ) -> PyResult<Self> {
        let defaults = ReceiverConfig::new(name);
        let config = ReceiverConfig {
            id: id.unwrap_or(defaults.id),
            channels,
            pairing_code,
            reconnect,
            ..defaults
        };
        let receiver = Receiver::connect(address, config).map_err(error)?;
        Ok(PyReceiver(Handle::new(receiver)))
    }

    fn set_tally(&self, py: Python<'_>, program: bool, preview: bool) -> PyResult<()> {
        self.0.with(py, |r| {
            r.set_tally(Tally { program, preview });
            Ok(())
        })
    }

    fn wait_event(&self, py: Python<'_>, timeout: f64) -> PyResult<Option<String>> {
        self.0.wait_event(py, timeout, Receiver::wait_event)
    }

    #[getter]
    fn url(&self, py: Python<'_>) -> PyResult<String> {
        self.0.with(py, |r| Ok(r.url().to_string()))
    }

    fn close(&self, py: Python<'_>) {
        self.0.close(py);
    }
}

/// An mDNS browser looking for sources.
#[pyclass(frozen, name = "Browser", module = "ntxprotocol._ntx")]
struct PyBrowser(Handle<Browser>);

#[pymethods]
impl PyBrowser {
    #[new]
    fn start(py: Python<'_>) -> PyResult<Self> {
        let browser = py.detach(Browser::start).map_err(error)?;
        Ok(PyBrowser(Handle::new(browser)))
    }

    fn wait_event(&self, py: Python<'_>, timeout: f64) -> PyResult<Option<String>> {
        self.0.wait_event(py, timeout, Browser::wait_event)
    }

    fn close(&self, py: Python<'_>) {
        self.0.close(py);
    }
}

#[pymodule]
#[pyo3(name = "_ntx")]
fn ntx_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("PROTOCOL_VERSION", ntx::PROTOCOL_VERSION)?;
    m.add("DEFAULT_PORT", ntx::DEFAULT_PORT)?;
    m.add("PROGRAM", ntx::PROGRAM)?;
    m.add("PREVIEW", ntx::PREVIEW)?;
    m.add_function(wrap_pyfunction!(render, m)?)?;
    m.add_class::<PySource>()?;
    m.add_class::<PyReceiver>()?;
    m.add_class::<PyBrowser>()?;
    m.add("NtxError", py.get_type::<NtxError>())?;
    m.add("ClosedError", py.get_type::<ClosedError>())?;
    m.add("NetworkError", py.get_type::<NetworkError>())?;
    m.add("MdnsError", py.get_type::<MdnsError>())?;
    m.add("InvalidTypeNameError", py.get_type::<InvalidTypeNameError>())?;
    m.add("UnknownChannelError", py.get_type::<UnknownChannelError>())?;
    m.add("MessageTooLargeError", py.get_type::<MessageTooLargeError>())?;
    m.add("InvalidAddressError", py.get_type::<InvalidAddressError>())?;
    m.add("InvalidPatternError", py.get_type::<InvalidPatternError>())?;
    m.add("InvalidMessageError", py.get_type::<InvalidMessageError>())?;
    Ok(())
}
