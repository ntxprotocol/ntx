"""NTX (Network Text Exchange): send text, not video, between apps on a local network.

- :class:`Source` serves text to receivers and advertises itself with mDNS.
- :class:`Receiver` connects to a source, keeps its state and reconnects when needed.
- :class:`Browser` finds sources on the local network.
- :func:`render` turns fields into display text with a binding pattern.

Events are polled with ``poll_event()`` or ``wait_event(timeout)``; nothing calls back from
another thread. Close objects with ``close()`` or a ``with`` block.

See https://github.com/ntxprotocol/spec for the protocol.
"""

from ._events import (
    Announced,
    BrowserEvent,
    Connected,
    Disconnected,
    DiscoveredSource,
    Endpoint,
    ErrorReport,
    ReceiverConnected,
    ReceiverDisconnected,
    ReceiverEvent,
    SourceEvent,
    SourceFound,
    SourceLost,
    Tally,
    TallyChanged,
    TypeDescriptor,
    Update,
)
from ._ntx import (
    DEFAULT_PORT,
    PREVIEW,
    PROGRAM,
    PROTOCOL_VERSION,
    ClosedError,
    InvalidAddressError,
    InvalidMessageError,
    InvalidPatternError,
    InvalidTypeNameError,
    MdnsError,
    MessageTooLargeError,
    NetworkError,
    NtxError,
    UnknownChannelError,
    __version__,
)
from ._objects import Browser, Receiver, Source, render

__all__ = [
    "DEFAULT_PORT",
    "PREVIEW",
    "PROGRAM",
    "PROTOCOL_VERSION",
    "Announced",
    "Browser",
    "BrowserEvent",
    "ClosedError",
    "Connected",
    "Disconnected",
    "DiscoveredSource",
    "Endpoint",
    "ErrorReport",
    "InvalidAddressError",
    "InvalidMessageError",
    "InvalidPatternError",
    "InvalidTypeNameError",
    "MdnsError",
    "MessageTooLargeError",
    "NetworkError",
    "NtxError",
    "Receiver",
    "ReceiverConnected",
    "ReceiverDisconnected",
    "ReceiverEvent",
    "Source",
    "SourceEvent",
    "SourceFound",
    "SourceLost",
    "Tally",
    "TallyChanged",
    "TypeDescriptor",
    "UnknownChannelError",
    "Update",
    "__version__",
    "render",
]
