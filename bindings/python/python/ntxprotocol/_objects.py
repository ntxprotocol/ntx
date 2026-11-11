"""Sources, receivers and browsers: typed wrappers around the native objects."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from types import TracebackType
from typing import Any, Callable, Generic, Protocol, TypeVar

from . import _ntx
from ._events import (
    BROWSER_EVENTS,
    RECEIVER_EVENTS,
    SOURCE_EVENTS,
    BrowserEvent,
    ReceiverEvent,
    SourceEvent,
    Tally,
    TypeDescriptor,
)

E = TypeVar("E")
H = TypeVar("H", bound="_Handle[Any]")


class _Native(Protocol):
    def wait_event(self, timeout: float) -> str | None: ...
    def close(self) -> None: ...


def _json(value: object) -> str:
    return json.dumps(value, allow_nan=False)


def _channels(channels: Sequence[str]) -> list[str]:
    # A plain string is a sequence too, but of single characters.
    if isinstance(channels, str):
        raise TypeError(f"channels must be a sequence of names, not a str: use ({channels!r},)")
    return list(channels)


class _Handle(Generic[E]):
    """What sources, receivers and browsers share: events and closing."""

    def __init__(self, native: _Native, events: Mapping[str, Callable[[Any], E]]) -> None:
        self._native = native
        self._events = events

    def poll_event(self) -> E | None:
        """Takes the next event without waiting.

        Returns:
            The event, or ``None`` if there is none.
        """
        return self.wait_event(0)

    def wait_event(self, timeout: float) -> E | None:
        """Waits for the next event.

        Other Python threads keep running while it waits, Ctrl-C interrupts it, and
        :meth:`close` from another thread ends it with :class:`ClosedError`.

        Args:
            timeout: Longest wait in seconds. ``0`` just checks; ``math.inf`` waits until an
                event comes.

        Returns:
            The event, or ``None`` if none came in time.
        """
        message = self._native.wait_event(timeout)
        if message is None:
            return None
        event = json.loads(message)
        return self._events[event["event"]](event["data"])

    def close(self) -> None:
        """Stops the object. Later calls raise :class:`ClosedError`. Closing twice is fine.

        Safe to call from another thread while one waits: it returns within about 50 ms.
        """
        self._native.close()

    def __enter__(self: H) -> H:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


class Source(_Handle[SourceEvent]):
    """An NTX source: holds the current text and serves it to receivers (clauses 8-11).

    It starts listening at once and, unless ``advertise`` is off, advertises itself with mDNS.

    Args:
        name: Human-readable name, e.g. ``CHURCH-PC (Lyrics)``.
        id: Stable id. Save it and pass it again next time, so receivers find the source after a
            restart. Defaults to a new random UUID.
        channels: Channels this source has, e.g. ``("program", "preview")``. Must include
            ``program``.
        bind: Address to listen on. Defaults to all IPv4 interfaces.
        port: TCP port. ``0`` picks any free port.
        path: WebSocket path.
        pairing_code: Require this pairing code from receivers.
        advertise: Advertise with mDNS.

    Raises:
        TypeError: ``channels`` is a single ``str``.
        UnknownChannelError: ``channels`` lacks ``program``.
        InvalidAddressError: ``bind`` is not an IP address.
        NetworkError: The port cannot be opened.
        MdnsError: mDNS cannot start.
    """

    def __init__(
        self,
        name: str,
        *,
        id: str | None = None,
        channels: Sequence[str] = (_ntx.PROGRAM,),
        bind: str = "0.0.0.0",  # noqa: S104 - the same default as the Rust library
        port: int = _ntx.DEFAULT_PORT,
        path: str = "/",
        pairing_code: str | None = None,
        advertise: bool = True,
    ) -> None:
        self._source = _ntx.Source(
            name,
            id=id,
            channels=_channels(channels),
            bind=bind,
            port=port,
            path=path,
            pairing_code=pairing_code,
            advertise=advertise,
        )
        super().__init__(self._source, SOURCE_EVENTS)

    def update(self, channel: str, type_name: str, fields: Mapping[str, Any]) -> int:
        """Replaces the item of a channel (clause 9).

        Args:
            channel: The channel, e.g. ``program``.
            type_name: The item's type, e.g. ``com.example.lyrics``.
            fields: The item's fields. They must be JSON-compatible.

        Returns:
            The update's sequence number.

        Raises:
            UnknownChannelError: The source has no such channel.
            InvalidTypeNameError: ``type_name`` is not reverse-DNS.
            MessageTooLargeError: The update is over 64 KiB.
        """
        return self._source.update(channel, type_name, _json(fields))

    def update_text(self, channel: str, text: str) -> int:
        """Sends plain text on a channel as an ``ntx.text`` item.

        Returns:
            The update's sequence number.
        """
        return self._source.update_text(channel, text)

    def clear(self, channel: str) -> int:
        """Clears a channel (clause 9.5).

        Returns:
            The update's sequence number.
        """
        return self._source.clear(channel)

    def announce(self, types: Iterable[TypeDescriptor]) -> None:
        """Tells receivers which item types this source sends (clause 15)."""
        self._source.announce(_json([t._to_wire() for t in types]))

    @property
    def id(self) -> str:
        """The source id."""
        return self._source.id

    @property
    def name(self) -> str:
        """The display name."""
        return self._source.name

    @property
    def address(self) -> str:
        """The address it listens on, ``host:port``."""
        return self._source.address

    @property
    def port(self) -> int:
        """The TCP port it listens on, useful after ``port=0``."""
        return self._source.port

    @property
    def receiver_count(self) -> int:
        """How many receivers are connected."""
        return self._source.receiver_count


class Receiver(_Handle[ReceiverEvent]):
    """An NTX receiver: connects to one source and keeps receiving its text.

    Connecting happens in the background; watch the events.

    Args:
        address: A ``ws://`` URL, ``host`` or ``host:port`` (clause 6.4).
        name: Human-readable name, e.g. ``MEDIA-PC (Display)``.
        id: Stable id. Defaults to a new random UUID.
        channels: Channels to receive.
        pairing_code: Pairing code, if the source needs one.
        reconnect: Reconnect automatically after the connection is lost.

    Raises:
        TypeError: ``channels`` is a single ``str``.
        InvalidAddressError: ``address`` is not a valid address.
    """

    def __init__(
        self,
        address: str,
        name: str,
        *,
        id: str | None = None,
        channels: Sequence[str] = (_ntx.PROGRAM,),
        pairing_code: str | None = None,
        reconnect: bool = True,
    ) -> None:
        self._receiver = _ntx.Receiver(
            address,
            name,
            id=id,
            channels=_channels(channels),
            pairing_code=pairing_code,
            reconnect=reconnect,
        )
        super().__init__(self._receiver, RECEIVER_EVENTS)

    def set_tally(self, tally: Tally) -> None:
        """Tells the source whether its text is on air or in preview (clause 10)."""
        self._receiver.set_tally(tally.program, tally.preview)

    @property
    def url(self) -> str:
        """The WebSocket URL this receiver connects to."""
        return self._receiver.url


class Browser(_Handle[BrowserEvent]):
    """Finds NTX sources on the local network with mDNS (clause 6).

    Raises:
        MdnsError: mDNS cannot start.
    """

    def __init__(self) -> None:
        super().__init__(_ntx.Browser(), BROWSER_EVENTS)


def render(fields: Mapping[str, Any], pattern: str) -> str:
    """Renders a binding pattern from fields (clause 16).

    Example:
        >>> render({"reference": "John 3:16", "version": "KJV"}, "{reference} · {version}")
        'John 3:16 · KJV'

    Raises:
        InvalidPatternError: The pattern is not valid.
    """
    return _ntx.render(_json(fields), pattern)
