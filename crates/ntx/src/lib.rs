//! NTX (Network Text Exchange): send text, not video, between apps on a local network.
//!
//! This crate implements NTX 0.1 as described in <https://github.com/ntxprotocol/spec>.
//!
//! - [`Source`] serves text to receivers and advertises itself with mDNS.
//! - [`Receiver`] connects to a source, keeps its state and reconnects when needed.
//! - [`Browser`] finds sources on the local network.
//! - [`Publisher`] sends text through a [`Relay`], which serves it to the network as a source.
//!   This is for programs that cannot accept connections, such as web pages (clause 14).
//! - [`binding`] turns fields into display text (field paths and patterns).
//!
//! The networking objects run on a background runtime. Events are queued and read with
//! `poll_event` or `wait_event`, so the API works the same from Rust, C and other languages.
//! The queues are not bounded: read the events regularly, or drop the object when done.
//!
//! ```no_run
//! let source = ntx::Source::start(ntx::SourceConfig::new("Song Board")).unwrap();
//! source.update_text("program", "Amazing grace, how sweet the sound").unwrap();
//! ```

pub mod binding;
mod client;
mod discovery;
mod error;
mod origin;
pub mod pairing;
pub mod protocol;
mod publisher;
mod receiver;
mod relay;
mod runtime;
mod source;
mod store;
pub mod text;
mod wire;

pub use discovery::{Browser, BrowserEvent, DiscoveredSource};
pub use error::{Error, Result};
pub use protocol::{Endpoint, Fields, Tally, TypeDescriptor, Update};
pub use publisher::{DEFAULT_RELAY_URL, Publisher, PublisherConfig, PublisherEvent};
pub use receiver::{Receiver, ReceiverConfig, ReceiverEvent};
pub use relay::{Relay, RelayConfig, RelayEvent};
pub use source::{Source, SourceConfig, SourceEvent, display_name};

/// Protocol version implemented by this crate.
pub const PROTOCOL_VERSION: &str = "0.1";

/// Default TCP port of a source (spec clause 5.6).
pub const DEFAULT_PORT: u16 = 7686;

/// WebSocket subprotocol name (spec clause 5.2).
pub const SUBPROTOCOL: &str = "ntx";

/// DNS-SD service type, including the mDNS domain (spec clause 6.1).
pub const SERVICE_TYPE: &str = "_ntxp._tcp.local.";

/// Largest allowed message, in bytes of UTF-8 (spec clause 18.1).
pub const MAX_MESSAGE_BYTES: usize = 65_536;

/// The channel every source has (spec clause 3.5).
pub const PROGRAM: &str = "program";

/// The optional preview channel (spec clause 3.5).
pub const PREVIEW: &str = "preview";
