"""Errors from the core map to typed Python exceptions."""

from __future__ import annotations

import math

import pytest

import ntxprotocol as ntx
from conftest import local_source, receiver_for
from ntxprotocol import _ntx


def test_every_error_is_an_ntx_error() -> None:
    for name in ntx.__all__:
        if name.endswith("Error") and name != "NtxError":
            assert issubclass(getattr(ntx, name), ntx.NtxError)


def test_unknown_channel(source: ntx.Source) -> None:
    with pytest.raises(ntx.UnknownChannelError, match="preview"):
        source.update_text("preview", "text")


def test_source_needs_program() -> None:
    with pytest.raises(ntx.UnknownChannelError, match="program"):
        local_source(channels=["preview"])


def test_invalid_type_name(source: ntx.Source) -> None:
    with pytest.raises(ntx.InvalidTypeNameError):
        source.update("program", "Not A Type", {})


def test_too_large(source: ntx.Source) -> None:
    with pytest.raises(ntx.MessageTooLargeError):
        source.update_text("program", "x" * 70_000)


def test_invalid_type_descriptor(source: ntx.Source) -> None:
    with pytest.raises(ntx.InvalidMessageError, match="object"):
        source.announce([ntx.TypeDescriptor("com.example.lyrics", {"type": "string"})])


def test_invalid_addresses() -> None:
    with pytest.raises(ntx.InvalidAddressError):
        local_source(bind="not-an-ip")
    with pytest.raises(ntx.InvalidAddressError):
        ntx.Receiver("http://example.com", "R")


def test_port_in_use(source: ntx.Source) -> None:
    with pytest.raises(ntx.NetworkError):
        local_source(port=source.port)


def test_invalid_pattern() -> None:
    with pytest.raises(ntx.InvalidPatternError):
        ntx.render({}, "{unclosed")


def test_closed_objects_raise() -> None:
    source = local_source()
    receiver = receiver_for(source)
    browser = ntx.Browser()
    for handle in (source, receiver, browser):
        handle.close()
        handle.close()  # closing twice is fine
        with pytest.raises(ntx.ClosedError):
            handle.poll_event()
    with pytest.raises(ntx.ClosedError):
        source.update_text("program", "late")
    with pytest.raises(ntx.ClosedError):
        receiver.set_tally(ntx.Tally())


def test_invalid_arguments(source: ntx.Source) -> None:
    with pytest.raises(ValueError, match="timeout"):
        source.wait_event(-1)
    with pytest.raises(ValueError, match="Out of range float"):
        source.update("program", "com.example.number", {"value": math.nan})
    with pytest.raises(ValueError, match="fields"):
        _ntx.render("not json", "{a}")
    with pytest.raises(ValueError, match="fields"):
        source._source.update("program", "com.example.list", "[]")
    with pytest.raises(ValueError, match="types"):
        source._source.announce("{}")
