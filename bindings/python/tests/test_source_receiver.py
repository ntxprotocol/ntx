"""A source and receivers exchanging text over real sockets on localhost."""

from __future__ import annotations

import ntxprotocol as ntx
from conftest import expect, local_source, receiver_for


def test_update_reaches_the_receiver(source: ntx.Source, receiver: ntx.Receiver) -> None:
    seq = source.update("program", "com.example.lyrics", {"line": "Amazing grace", "verse": 1})

    update = expect(receiver, ntx.Update)
    while update.seq < seq:  # the current (empty) state comes first after connecting
        update = expect(receiver, ntx.Update)
    assert update == ntx.Update("program", seq, "com.example.lyrics", {"line": "Amazing grace", "verse": 1})


def test_update_text_and_clear(source: ntx.Source, receiver: ntx.Receiver) -> None:
    first = source.update_text("program", "Hello")
    cleared = source.clear("program")
    assert cleared > first

    updates = [expect(receiver, ntx.Update) for _ in range(3)]
    assert updates[-2:] == [
        ntx.Update("program", first, "ntx.text", {"text": "Hello"}),
        ntx.Update("program", cleared, "ntx.text", {}),
    ]


def test_preview_channel() -> None:
    with local_source(channels=[ntx.PROGRAM, ntx.PREVIEW]) as source:
        with receiver_for(source, channels=[ntx.PREVIEW]) as receiver:
            connected = expect(receiver, ntx.Connected)
            assert connected.channels == ("program", "preview")
            seq = source.update_text(ntx.PREVIEW, "Next slide")
            update = expect(receiver, ntx.Update)
            while update.seq < seq:
                update = expect(receiver, ntx.Update)
            assert (update.channel, update.fields) == ("preview", {"text": "Next slide"})


def test_connection_events() -> None:
    with local_source(id="source-1") as source:
        receiver = receiver_for(source, id="receiver-1")
        connected = expect(receiver, ntx.Connected)
        assert connected.source == ntx.Endpoint("source-1", "Test Source")
        assert connected.session

        event = expect(source, ntx.ReceiverConnected)
        assert event.receiver == ntx.Endpoint("receiver-1", "Test Receiver")
        assert event.address.startswith("127.0.0.1:")
        assert source.receiver_count == 1

        receiver.close()
        gone = expect(source, ntx.ReceiverDisconnected)
        assert gone.connection == event.connection


def test_tally(source: ntx.Source, receiver: ntx.Receiver) -> None:
    receiver.set_tally(ntx.Tally(program=True))
    assert expect(source, ntx.TallyChanged).tally == ntx.Tally(program=True, preview=False)

    receiver.set_tally(ntx.Tally(preview=True))
    assert expect(source, ntx.TallyChanged).tally == ntx.Tally(program=False, preview=True)


def test_announce(source: ntx.Source, receiver: ntx.Receiver) -> None:
    lyrics = ntx.TypeDescriptor(
        "com.example.lyrics",
        {"type": "object", "properties": {"line": {"type": "string"}}},
        title="Lyrics",
        description="One line of a song",
    )
    plain = ntx.TypeDescriptor("com.example.plain", {"type": "object"})
    source.announce([lyrics, plain])

    assert expect(receiver, ntx.Announced).types == (lyrics, plain)


def test_pairing() -> None:
    with local_source(pairing_code="1234") as source:
        with receiver_for(source, pairing_code="1234") as receiver:
            expect(receiver, ntx.Connected)

        with receiver_for(source, pairing_code="9999", reconnect=False) as receiver:
            assert expect(receiver, ntx.ErrorReport).code == "pairing_failed"
            expect(receiver, ntx.Disconnected)

        with receiver_for(source, reconnect=False) as receiver:
            assert expect(receiver, ntx.ErrorReport).code == "pairing_required"


def test_disconnected_when_the_source_stops() -> None:
    source = local_source()
    with receiver_for(source, reconnect=False) as receiver:
        expect(receiver, ntx.Connected)
        source.close()
        assert expect(receiver, ntx.Disconnected).reason


def test_properties() -> None:
    with local_source(id="fixed-id", path="/ntx") as source:
        assert (source.id, source.name) == ("fixed-id", "Test Source")
        assert source.address == f"127.0.0.1:{source.port}"
        assert source.receiver_count == 0
        assert source.poll_event() is None
        with ntx.Receiver(f"ws://127.0.0.1:{source.port}/ntx", "R") as receiver:
            assert receiver.url == f"ws://127.0.0.1:{source.port}/ntx"
            expect(receiver, ntx.Connected)


def test_default_port_in_url() -> None:
    with ntx.Receiver("example.invalid", "R", reconnect=False) as receiver:
        assert receiver.url == f"ws://example.invalid:{ntx.DEFAULT_PORT}/"


def test_render() -> None:
    fields = {"reference": "John 3:16", "version": "KJV", "lines": ["For God", "so loved"]}
    assert ntx.render(fields, "{reference} · {version}") == "John 3:16 · KJV"
    assert ntx.render(fields, "{lines}") == "For God\nso loved"


def test_display_name() -> None:
    machine, rest = ntx.display_name("Lyrics").split(" (", 1)
    assert machine
    assert machine == machine.upper()
    assert rest == "Lyrics)"


def test_versions() -> None:
    assert ntx.PROTOCOL_VERSION == "0.1"
    assert ntx.__version__.count(".") == 2
