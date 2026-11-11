"""Native module (Rust, PyO3). Use the typed wrappers in :mod:`ntxprotocol` instead."""

from typing import final

__all__ = [
    "DEFAULT_PORT",
    "PREVIEW",
    "PROGRAM",
    "PROTOCOL_VERSION",
    "Browser",
    "ClosedError",
    "InvalidAddressError",
    "InvalidMessageError",
    "InvalidPatternError",
    "InvalidTypeNameError",
    "MdnsError",
    "MessageTooLargeError",
    "NetworkError",
    "NtxError",
    "Receiver",
    "Source",
    "UnknownChannelError",
    "__version__",
    "render",
]

__version__: str
PROTOCOL_VERSION: str
DEFAULT_PORT: int
PROGRAM: str
PREVIEW: str

class NtxError(Exception):
    """Base class of every NTX error."""

class ClosedError(NtxError):
    """The object was closed."""

class NetworkError(NtxError):
    """A network or OS error."""

class MdnsError(NtxError):
    """mDNS could not start or register."""

class InvalidTypeNameError(NtxError):
    """Invalid type name (clause 15.2)."""

class UnknownChannelError(NtxError):
    """No such channel."""

class MessageTooLargeError(NtxError):
    """Over the size limit (clause 18.1)."""

class InvalidAddressError(NtxError):
    """Invalid address."""

class InvalidPatternError(NtxError):
    """Invalid pattern (clause 16.3)."""

class InvalidMessageError(NtxError):
    """A message is not valid NTX."""

def render(fields_json: str, pattern: str) -> str: ...

@final
class Source:
    def __new__(
        cls,
        name: str,
        *,
        id: str | None,
        channels: list[str],
        bind: str,
        port: int,
        path: str,
        pairing_code: str | None,
        advertise: bool,
    ) -> Source: ...
    def update(self, channel: str, type_name: str, fields_json: str) -> int: ...
    def update_text(self, channel: str, text: str) -> int: ...
    def clear(self, channel: str) -> int: ...
    def announce(self, types_json: str) -> None: ...
    def wait_event(self, timeout: float) -> str | None: ...
    def close(self) -> None: ...
    @property
    def id(self) -> str: ...
    @property
    def name(self) -> str: ...
    @property
    def address(self) -> str: ...
    @property
    def port(self) -> int: ...
    @property
    def receiver_count(self) -> int: ...

@final
class Receiver:
    def __new__(
        cls,
        address: str,
        name: str,
        *,
        id: str | None,
        channels: list[str],
        pairing_code: str | None,
        reconnect: bool,
    ) -> Receiver: ...
    def set_tally(self, program: bool, preview: bool) -> None: ...
    def wait_event(self, timeout: float) -> str | None: ...
    def close(self) -> None: ...
    @property
    def url(self) -> str: ...

@final
class Browser:
    def __new__(cls) -> Browser: ...
    def wait_event(self, timeout: float) -> str | None: ...
    def close(self) -> None: ...
