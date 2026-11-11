use std::io;

/// Errors returned by the NTX API.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A network or operating system error.
    #[error("network error: {0}")]
    Io(#[from] io::Error),

    /// mDNS could not start or register.
    #[error("mDNS error: {0}")]
    Mdns(String),

    /// A type name is not valid (clause 15.2).
    #[error("invalid type name {0:?} (expected reverse-DNS, e.g. com.example.lyrics)")]
    InvalidTypeName(String),

    /// The source has no such channel.
    #[error("this source has no channel {0:?}")]
    UnknownChannel(String),

    /// A message is over the size limit (clause 18.1).
    #[error("message is {0} bytes, the limit is {limit}", limit = crate::MAX_MESSAGE_BYTES)]
    TooLarge(usize),

    /// An address could not be understood.
    #[error("invalid address {0:?}")]
    InvalidAddress(String),

    /// A web origin is not of the form `scheme://host[:port]` (clause 14.5).
    #[error("invalid origin {0:?} (expected scheme://host[:port], e.g. https://example.com)")]
    InvalidOrigin(String),

    /// A binding pattern is not valid (clause 16.3).
    #[error("invalid pattern: {0}")]
    InvalidPattern(String),

    /// A message is not valid NTX.
    #[error("invalid message: {0}")]
    InvalidMessage(String),
}

/// Result with an NTX [`Error`].
pub type Result<T> = std::result::Result<T, Error>;
