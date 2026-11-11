"""Shared fixtures: real sources and receivers on localhost."""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any, TypeVar

import pytest

import ntxprotocol as ntx

T = TypeVar("T")

TIMEOUT = 10.0
"""Seconds to wait for an event that should come."""


def expect(handle: Any, kind: type[T], timeout: float = TIMEOUT) -> T:
    """Waits for the next event of type ``kind``, skipping other events."""
    deadline = time.monotonic() + timeout
    while (left := deadline - time.monotonic()) > 0:
        event = handle.wait_event(left)
        if isinstance(event, kind):
            return event
    pytest.fail(f"no {kind.__name__} event within {timeout} s")


def local_source(**options: Any) -> ntx.Source:
    """A source on a free localhost port, not advertised."""
    settings: dict[str, Any] = {"bind": "127.0.0.1", "port": 0, "advertise": False, **options}
    return ntx.Source("Test Source", **settings)


def receiver_for(source: ntx.Source, **options: Any) -> ntx.Receiver:
    """A receiver connecting to ``source``."""
    return ntx.Receiver(f"127.0.0.1:{source.port}", "Test Receiver", **options)


@pytest.fixture
def source() -> Iterator[ntx.Source]:
    with local_source() as source:
        yield source


@pytest.fixture
def receiver(source: ntx.Source) -> Iterator[ntx.Receiver]:
    with receiver_for(source) as receiver:
        expect(receiver, ntx.Connected)
        yield receiver
