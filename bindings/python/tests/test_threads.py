"""Waiting releases the GIL, Ctrl-C interrupts a wait, and close() ends one from another thread."""

from __future__ import annotations

import _thread
import math
import threading
import time
from typing import Callable

import pytest

import ntxprotocol as ntx
from conftest import expect

LONG = 10.0
"""A wait that the test always cuts short."""

QUICK = 1.0
"""Generous upper bound for something that should take about one 50 ms slice."""


def in_thread(target: Callable[[], object]) -> tuple[threading.Thread, list[object]]:
    """Runs ``target`` in a thread and keeps what it returned or raised."""
    result: list[object] = []

    def run() -> None:
        try:
            result.append(target())
        except BaseException as e:
            result.append(e)

    thread = threading.Thread(target=run)
    thread.start()
    return thread, result


def test_other_threads_run_during_a_wait(source: ntx.Source) -> None:
    waiter, _ = in_thread(lambda: source.wait_event(1.0))
    start = time.monotonic()
    count = 0
    while waiter.is_alive():
        count += 1
    assert count > 1000, "the waiting thread held the GIL"
    assert time.monotonic() - start >= 0.9


def test_calls_from_another_thread_during_a_wait(source: ntx.Source, receiver: ntx.Receiver) -> None:
    waiter, _ = in_thread(lambda: source.wait_event(LONG))
    start = time.monotonic()
    seq = source.update_text(ntx.PROGRAM, "Hello")
    assert source.port > 0
    assert source.receiver_count == 1
    assert time.monotonic() - start < QUICK
    while (update := expect(receiver, ntx.Update)).seq < seq:  # the empty state comes first
        pass
    assert update.fields == {"text": "Hello"}
    source.close()
    waiter.join(QUICK)


def test_close_from_another_thread_ends_a_wait(source: ntx.Source) -> None:
    waiter, result = in_thread(lambda: source.wait_event(LONG))
    time.sleep(0.2)
    start = time.monotonic()
    source.close()
    assert time.monotonic() - start < QUICK
    waiter.join(QUICK)
    assert not waiter.is_alive()
    assert isinstance(result[0], ntx.ClosedError)


def test_ctrl_c_interrupts_a_wait(source: ntx.Source) -> None:
    threading.Timer(0.2, _thread.interrupt_main).start()
    start = time.monotonic()
    with pytest.raises(KeyboardInterrupt):
        source.wait_event(LONG)
    assert time.monotonic() - start < QUICK + 0.2


def test_infinite_wait(source: ntx.Source, receiver: ntx.Receiver) -> None:
    safety = threading.Timer(LONG, receiver.close)  # fails the test instead of hanging it
    safety.start()
    threading.Timer(0.2, lambda: source.update_text(ntx.PROGRAM, "Later")).start()
    try:
        while not isinstance(event := receiver.wait_event(math.inf), ntx.Update) or not event.fields:
            pass
    finally:
        safety.cancel()
    assert event.fields == {"text": "Later"}
