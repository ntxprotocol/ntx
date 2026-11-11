<!-- markdownlint-disable-file MD041 -- the logo comes before the title -->
<!-- markdownlint-disable MD033 -->
<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/ntx-lockup-reverse.svg">
    <img src="assets/ntx-lockup.svg" alt="NTX — Network Text Exchange" height="64">
  </picture>
</p>
<!-- markdownlint-enable MD033 -->

# NTX — Network Text Exchange

[![ci](https://github.com/ntxprotocol/ntx/actions/workflows/ci.yml/badge.svg)](https://github.com/ntxprotocol/ntx/actions/workflows/ci.yml)
[![Coverage: at least 90%](https://img.shields.io/badge/coverage-%E2%89%A590%25-0B7A72)](https://github.com/ntxprotocol/.github/blob/main/CODE_STANDARDS.md)
[![Licence: MIT OR Apache-2.0](https://img.shields.io/badge/licence-MIT%20OR%20Apache--2.0-0B7A72)](#licence)
[![Rust 1.87+](https://img.shields.io/badge/rust-1.87%2B-0B7A72)](https://rustup.rs)

The core library and command-line tool for **NTX**, an open protocol for
sending text, not video, between apps on a local network.

> **Status: early development**, following the NTX 0.1 Pre-Draft
> [specification](https://github.com/ntxprotocol/spec). APIs will change.

## What's here

| Crate | What it is |
| --- | --- |
| [`crates/ntx`](crates/ntx) | Rust library: sources, receivers, publishers, relays, mDNS discovery, pairing, binding |
| [`crates/ntx-cli`](crates/ntx-cli) | `ntx` command: a test sender, receiver, publisher and relay |
| [`crates/ntx-ffi`](crates/ntx-ffi) | C library (`libntx_ffi`) and header [`ntx.h`](crates/ntx-ffi/include/ntx.h) |

Planned in this repository: thin wrappers for Python, Node, Java and .NET on top of the C library.

## Try it

You need [Rust](https://rustup.rs) 1.87 or newer.

Send text (each line you type is sent; an empty line clears):

```bash
cargo run -p ntx-cli -- send --name "Test"
```

In another terminal, find sources on the network:

```bash
cargo run -p ntx-cli -- list
```

Receive and print the text:

```bash
cargo run -p ntx-cli -- recv 127.0.0.1 --field text
```

Useful options: `--pair 1234` (pairing code), `--preview` (also offer a preview channel),
`--json --type com.example.lyrics` (send JSON fields), `--pattern "{reference} · {version}"`.

Programs that cannot accept connections, such as web pages, publish through a relay
([spec clause 14](https://github.com/ntxprotocol/spec)). Run a relay, then publish to it; the relay
serves the text to the network as an ordinary source:

```bash
cargo run -p ntx-cli -- relay --allow-origin https://example.com
cargo run -p ntx-cli -- publish --name "Song Board"
```

Web pages may publish only from origins allowed with `--allow-origin` (never from the origin
`null`). Programs that send no `Origin` header may publish only from the same computer. Publishing
is off unless a relay runs.

## Use the library

```rust
use std::time::Duration;
use ntx::{Receiver, ReceiverConfig, ReceiverEvent, Source, SourceConfig};

let source = Source::start(SourceConfig::new("CHURCH-PC (Lyrics)"))?;
source.update_text("program", "Amazing grace, how sweet the sound")?;

let receiver = Receiver::connect("127.0.0.1", ReceiverConfig::new("MEDIA-PC (Display)"))?;
while let Some(event) = receiver.wait_event(Duration::from_secs(5)) {
    if let ReceiverEvent::Update { fields, .. } = event {
        println!("{}", ntx::binding::field_text(&fields, "text"));
    }
}
# Ok::<(), ntx::Error>(())
```

Everything networked runs on a background thread. Events are **queued and read with
`poll_event` / `wait_event`**, never pushed through callbacks, so the same design works from C,
Python, Node, Java and .NET.

## Development

All checks run automatically before each commit and push, and again in CI. Set up once:

```bash
brew install pre-commit cargo-llvm-cov cargo-deny cbindgen   # or your platform's equivalent
cargo install cargo-machete
pre-commit install
```

Run everything by hand:

```bash
pre-commit run --all-files --hook-stage pre-commit   # format, lint, secrets
pre-commit run --all-files --hook-stage pre-push     # tests, coverage >= 90%, docs, licences, duplication
```

Tests include **surface tests** that run the real `ntx` binary and real sockets
(`crates/ntx-cli/tests/`, `crates/ntx/tests/`). They also check the spec's examples when the
spec is next to this repository (`../spec`) or `NTX_SPEC_DIR` points to it.

## Contributing

See the organisation's [contributing guide](https://github.com/ntxprotocol/.github/blob/main/CONTRIBUTING.md).
Every commit needs a DCO sign-off (`git commit -s`). Protocol changes start in the
[spec repository](https://github.com/ntxprotocol/spec).

## Licence

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT)
at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in
this work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without
any additional terms or conditions.

Copyright © 2026 CamelCase Technologies Ltd and the NTX contributors.

The NTX logo in `assets/` is a trademark and is not covered by the code licences; see the
[NTX trademark policy](https://github.com/ntxprotocol/.github/blob/main/TRADEMARKS.md).
