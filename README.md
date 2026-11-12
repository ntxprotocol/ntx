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

The language SDKs live in their own repositories and use this core: `ntx-python`, `ntx-node`,
`ntx-dotnet`, `ntx-java` and `ntx-web` (TypeScript for browsers). They are not public yet.

## Words used here

| Word | Meaning |
| --- | --- |
| **Source** | An app that sends text, e.g. lyrics or presentation software |
| **Receiver** | An app that shows a source's text, e.g. OBS, vMix or a stage display |
| **Channel** | A source's text stream: **program** is what is live, **preview** is what comes next |
| **Tally** | A receiver telling the source it is showing it: red when on air (program), green in preview |
| **Relay** | A program that takes text from publishers and serves it to the network as ordinary sources |
| **Publisher** | An app that cannot accept connections, such as a web page, sending its text through a relay |
| **Origin** | The web address a page comes from, e.g. `https://example.com`; a relay accepts only allowed ones |
| **mDNS** | Multicast DNS: how sources announce themselves on the local network, so `list` finds them |

## Try it

You need [Rust](https://rustup.rs) 1.87 or newer and [Git](https://git-scm.com). Get the code:

```bash
git clone https://github.com/ntxprotocol/ntx
cd ntx
```

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

**On two computers:** run `send` on one, and on the other use `list` to find it, then
`recv <its IP address>`. Both must be on the same network, and the sender's firewall must allow
incoming connections on TCP port 7686 (and mDNS, UDP port 5353, for `list`). Windows asks the first
time `send` runs: allow it on private networks.

Useful options (`--help` on each command lists them all):

| Command | Options |
| --- | --- |
| `send` | `--name`, `--port`, `--pair 1234` (pairing code), `--preview` (also a preview channel), `--json --type com.example.lyrics` (send JSON fields), `--channel`, `--no-mdns` |
| `recv` | `--pair`, `--field text`, `--pattern "{reference} · {version}"`, `--channel`, `--on-air`, `--count` |
| `list` | `--seconds` |
| `relay` | `--port`, `--pair`, `--allow-origin`, `--no-mdns` |
| `publish` | `--name`, `--pair`, `--preview`, `--json --type …`, `--channel` |

Programs that cannot accept connections, such as web pages, publish through a relay
([spec clause 14](https://github.com/ntxprotocol/spec/blob/main/ntx.md#14-publishing-through-a-relay)). Run a relay, then publish to it; the relay
serves the text to the network as an ordinary source:

```bash
cargo run -p ntx-cli -- relay --allow-origin https://example.com
cargo run -p ntx-cli -- publish --name "Song Board"
```

Each published source gets a port of its own, not the relay's: the relay prints it
(`serving "PC (Song Board)" at ws://127.0.0.1:50670/`), and `ntx list` shows it too. Receive it
with `recv 127.0.0.1:<port>`; `recv 127.0.0.1` would reach the relay's port, which only takes
publishers.

`ntx relay` and `ntx send` both use port 7686 by default (spec clauses 5.6 and 14.1), so only one
of them can run on a computer at a time. To run both, give `send` another port, for example
`send --port 7687`.

Web pages may publish only from origins allowed with `--allow-origin` (never from the origin
`null`). Programs that send no `Origin` header may publish only from the same computer. Publishing
is off unless a relay runs.

## Use the library

Add it to your `Cargo.toml` (it is not on crates.io yet):

```toml
[dependencies]
ntx = { git = "https://github.com/ntxprotocol/ntx" }
```

Send a line of text and receive it on the same computer
([`crates/ntx/examples/hello.rs`](crates/ntx/examples/hello.rs); run it with
`cargo run -p ntx --example hello`):

```rust
use std::time::Duration;

use ntx::{Receiver, ReceiverConfig, ReceiverEvent, Source, SourceConfig, display_name};

fn main() -> Result<(), ntx::Error> {
    let source = Source::start(SourceConfig::new(display_name("Lyrics")))?;
    source.update_text("program", "Amazing grace, how sweet the sound")?;

    let receiver = Receiver::connect("127.0.0.1", ReceiverConfig::new(display_name("Display")))?;
    while let Some(event) = receiver.wait_event(Duration::from_secs(5)) {
        if let ReceiverEvent::Update(update) = event {
            println!("{}", ntx::binding::field_text(&update.fields, "text"));
            break;
        }
    }
    Ok(())
}
```

Everything networked runs on a background thread. Events are **queued and read with
`poll_event` / `wait_event`**, never pushed through callbacks, so the same design works from C,
Python, Node, Java and .NET.

## Development

All checks run automatically before each commit and push, and again in CI. Set up once:

```bash
brew install pre-commit cargo-llvm-cov cargo-deny cbindgen   # or your platform's equivalent
cargo install cargo-machete
rustup toolchain install 1.87
rustup target add x86_64-unknown-linux-gnu x86_64-pc-windows-msvc
pre-commit install
```

Run everything by hand:

```bash
pre-commit run --all-files --hook-stage pre-commit   # format, lint, secrets
pre-commit run --all-files --hook-stage pre-push     # tests, coverage >= 90%, docs, licences, duplication,
                                                     # Linux and Windows lint, Rust 1.87
```

Only CI runs the tests on Linux and Windows; the hooks lint those platforms' code from any computer.

### Releases

`scripts/release.sh` builds the `ntx` command and the C library for macOS (Apple silicon and Intel),
Linux (x86-64 and ARM64, glibc 2.17 or newer) and Windows (x86-64), as archives with `ntx.h`, the
licences and `SHA256SUMS`, in `dist/`. Linux and Windows are cross-built with
[zig](https://ziglang.org) and [cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild).
Pushing a tag such as `v0.1.0-alpha.1` runs the same script in CI (`release.yml`) and creates a
*draft* GitHub release for a maintainer to check and publish. Nothing goes to a package registry.

On **Windows**, use the MSVC Rust toolchain (rustup's default) with the
[Visual Studio Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)
("Desktop development with C++").

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
