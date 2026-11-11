"""Typed values and events, built from the JSON that the native module returns."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Union


@dataclass(frozen=True)
class Endpoint:
    """The identity of a source or receiver."""

    id: str
    name: str


@dataclass(frozen=True)
class Tally:
    """Whether a receiver shows a source's text (clause 10)."""

    program: bool = False
    """Shown on air."""
    preview: bool = False
    """Shown in preview."""


@dataclass(frozen=True)
class TypeDescriptor:
    """Describes an item type to receivers (clause 15.4)."""

    type: str
    """The type name, e.g. ``com.example.lyrics``."""
    schema: dict[str, Any]
    """JSON Schema of the fields."""
    title: str | None = None
    """Short name for people."""
    description: str | None = None
    """Longer explanation for people."""

    def _to_wire(self) -> dict[str, Any]:
        """Returns the descriptor as it is sent on the wire."""
        wire: dict[str, Any] = {"ntxType": self.type, "schema": self.schema}
        if self.title is not None:
            wire["title"] = self.title
        if self.description is not None:
            wire["description"] = self.description
        return wire


@dataclass(frozen=True)
class DiscoveredSource:
    """A source found on the network with mDNS (clause 6)."""

    id: str
    name: str
    """The DNS-SD instance name, normally the source's display name."""
    url: str
    """WebSocket URL to connect to."""
    version: str
    """Protocol version from the TXT record."""
    pairing_required: bool
    host: str
    port: int
    addresses: tuple[str, ...]
    """All known addresses; the first one is used in ``url``."""


# Source events


@dataclass(frozen=True)
class ReceiverConnected:
    """A receiver finished the handshake."""

    connection: int
    """Number that identifies this connection in later events."""
    receiver: Endpoint
    address: str
    """The receiver's network address, ``host:port``."""


@dataclass(frozen=True)
class ReceiverDisconnected:
    """A receiver went away."""

    connection: int


@dataclass(frozen=True)
class TallyChanged:
    """The combined tally of all receivers changed (clause 10.3)."""

    tally: Tally


# Receiver events


@dataclass(frozen=True)
class Connected:
    """The handshake finished. Updates with the full state follow."""

    source: Endpoint
    session: str
    """The source's session; it changes when the source restarts."""
    channels: tuple[str, ...]


@dataclass(frozen=True)
class Announced:
    """The source announced its types (clause 15.5)."""

    types: tuple[TypeDescriptor, ...]


@dataclass(frozen=True)
class Update:
    """New item for a channel; empty fields mean "clear" (clause 9).

    Old and repeated updates are already filtered out, and text is normalised.
    """

    channel: str
    seq: int
    type: str
    fields: dict[str, Any]


@dataclass(frozen=True)
class ErrorReport:
    """The source reported an error (clause 12), e.g. ``pairing_failed``."""

    code: str
    message: str | None


@dataclass(frozen=True)
class Disconnected:
    """The connection was lost or could not be made. The receiver retries if ``reconnect`` is on."""

    reason: str


# Browser events


@dataclass(frozen=True)
class SourceFound:
    """A new source, or a change to a source already found."""

    source: DiscoveredSource


@dataclass(frozen=True)
class SourceLost:
    """A source went away."""

    id: str
    name: str


SourceEvent = Union[ReceiverConnected, ReceiverDisconnected, TallyChanged]
"""Something that happened to a :class:`~ntxprotocol.Source`."""

ReceiverEvent = Union[Connected, Announced, Update, ErrorReport, Disconnected]
"""Something that happened to a :class:`~ntxprotocol.Receiver`."""

BrowserEvent = Union[SourceFound, SourceLost]
"""Something a :class:`~ntxprotocol.Browser` noticed."""


def _endpoint(data: dict[str, Any]) -> Endpoint:
    return Endpoint(id=data["id"], name=data["name"])


def _type_descriptor(data: dict[str, Any]) -> TypeDescriptor:
    return TypeDescriptor(
        type=data["ntxType"],
        schema=data["schema"],
        title=data.get("title"),
        description=data.get("description"),
    )


def _discovered_source(data: dict[str, Any]) -> DiscoveredSource:
    return DiscoveredSource(
        id=data["id"],
        name=data["name"],
        url=data["url"],
        version=data["version"],
        pairing_required=data["pairing_required"],
        host=data["host"],
        port=data["port"],
        addresses=tuple(data["addresses"]),
    )


# Each table maps the native `{"event": name, "data": ...}` to the Python event.
SOURCE_EVENTS: dict[str, Callable[[Any], SourceEvent]] = {
    "receiver_connected": lambda d: ReceiverConnected(
        d["connection"], _endpoint(d["receiver"]), d["address"]
    ),
    "receiver_disconnected": lambda d: ReceiverDisconnected(d["connection"]),
    "tally_changed": lambda d: TallyChanged(Tally(program=d["program"], preview=d["preview"])),
}

RECEIVER_EVENTS: dict[str, Callable[[Any], ReceiverEvent]] = {
    "connected": lambda d: Connected(_endpoint(d["source"]), d["session"], tuple(d["channels"])),
    "announce": lambda d: Announced(tuple(_type_descriptor(t) for t in d)),
    "update": lambda d: Update(d["channel"], d["seq"], d["type"], d["fields"]),
    "error": lambda d: ErrorReport(d["code"], d.get("message")),
    "disconnected": lambda d: Disconnected(d["reason"]),
}

BROWSER_EVENTS: dict[str, Callable[[Any], BrowserEvent]] = {
    "found": lambda d: SourceFound(_discovered_source(d)),
    "lost": lambda d: SourceLost(d["id"], d["name"]),
}
