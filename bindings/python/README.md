# ntx-protocol

Python bindings for **NTX (Network Text Exchange)**, an open protocol for sending text, not
video, between apps on a local network. A source sends structured text (a type name plus JSON
fields) and each receiver shows it with its own templates.

> **Status: early development**, following the NTX 0.1 Pre-Draft
> [specification](https://github.com/ntxprotocol/spec). APIs will change. Not on PyPI yet.

- Distribution: `ntx-protocol`. Import: `ntxprotocol`.
- Python 3.9 or newer (one `abi3` wheel per platform), on Linux, macOS and Windows.
- Built on the Rust [`ntx`](../../crates/ntx) library with [PyO3](https://pyo3.rs).
- Fully typed (`py.typed`), checked with `mypy --strict`.

## Install from source

You need [Rust](https://rustup.rs) 1.87 or newer.

```bash
pip install ./bindings/python
```

## Send and receive

```python
import ntxprotocol as ntx

with ntx.Source("CHURCH-PC (Lyrics)") as source:
    source.update_text(ntx.PROGRAM, "Amazing grace, how sweet the sound")
    source.update("program", "com.example.lyrics", {"line": "Amazing grace", "verse": 1})
    ...
```

```python
import ntxprotocol as ntx

with ntx.Receiver("192.168.1.20", "MEDIA-PC (Display)") as receiver:
    while True:
        event = receiver.wait_event(timeout=1.0)
        if isinstance(event, ntx.Update):
            print(event.channel, event.type, event.fields)
```

A complete example that runs on one computer is in [`examples/hello.py`](examples/hello.py).

## Find sources

```python
import ntxprotocol as ntx

with ntx.Browser() as browser:
    while event := browser.wait_event(timeout=5.0):
        if isinstance(event, ntx.SourceFound):
            print(event.source.name, event.source.url)
```

## How it works

| Topic | Rule |
| --- | --- |
| Objects | `Source`, `Receiver`, `Browser`. Close them with `close()` or a `with` block |
| Settings | Keyword arguments, e.g. `Source(name, port=0, pairing_code="1234", advertise=False)` |
| Channels | A sequence of names, e.g. `channels=("program", "preview")`. A single `str` raises `TypeError` |
| Events | Frozen dataclasses, read with `poll_event()` or `wait_event(timeout)` (`math.inf` waits with no limit). No callbacks |
| Threads | Waiting releases the GIL and Ctrl-C interrupts it. Objects may be shared; `close()` from another thread ends a wait with `ClosedError` |
| Errors | Exceptions derived from `NtxError`, e.g. `UnknownChannelError`, `ClosedError` |
| Binding | `render(fields, "{reference} · {version}")` turns fields into text (clause 16) |

Events by object:

| Object | Events |
| --- | --- |
| `Source` | `ReceiverConnected`, `ReceiverDisconnected`, `TallyChanged` |
| `Receiver` | `Connected`, `Announced`, `Update`, `ErrorReport`, `Disconnected` |
| `Browser` | `SourceFound`, `SourceLost` |

Receivers can tell the source whether its text is on air with
`receiver.set_tally(ntx.Tally(program=True))`, and sources can describe their item types with
`source.announce([ntx.TypeDescriptor("com.example.lyrics", schema)])`.

## Develop

From the repository root:

```bash
python -m venv .venv && . .venv/bin/activate
pip install maturin mypy pytest pytest-cov
cd bindings/python
maturin develop
pytest
```

`./check.sh` checks the type stub against the built module (`mypy.stubtest`), runs the tests
against an instrumented build and checks that both the Python package and the Rust binding code
([`src/lib.rs`](src/lib.rs)) have at least 90% line coverage. It needs mypy and
[`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov). The pre-push hook runs it.

## Licence

MIT OR Apache-2.0. Copyright © 2026 CamelCase Technologies Ltd and the NTX contributors.
