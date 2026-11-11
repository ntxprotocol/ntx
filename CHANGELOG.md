# Changelog

All notable changes are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## Unreleased

### Added

- `ntx` crate implementing NTX 0.1: `Source`, `Receiver`, `Browser` (mDNS), pairing, binding
  (field paths and patterns), text normalisation, and message validation matching the spec's
  JSON Schemas.
- `ntx` command-line tool: `ntx send`, `ntx recv`, `ntx list`.
- Surface tests for the CLI and raw-protocol tests; line coverage at least 90%, enforced.
- pre-commit / pre-push hooks and CI running the same checks.
- `ntx-ffi`: C ABI (opaque handles, JSON settings and events, polling), `ntx.h` header generated
  by cbindgen, and a C example that is compiled and run by the tests.
- Events serialise to JSON (`{"event": ..., "data": ...}`).
- Publishing through a relay (spec clause 14): `Publisher` sends text through a relay, with
  pairing, tally events and reconnection; `Relay` serves each publisher to the network as a
  source, checks web page origins against an allow-list (`origin_requested` events,
  `allow_origin`, `revoke_origin`; `null` always refused), accepts programs without an `Origin`
  header from loopback only, limits publishers (32, and 8 per origin), lets the newest connection
  for a source id win, and keeps each published source on the same port when its publisher
  reconnects. Dropping a publisher first delivers unsent text (up to 3 seconds). Also in the C ABI
  (`ntx_publisher_*`, `ntx_relay_*`) and the CLI (`ntx publish`, `ntx relay`).
- Security hardening for sources and relays: at most 64 connections per endpoint and 16 per IP
  address, refused before the WebSocket upgrade; the upgrade must finish within 10 seconds; frame
  queues are bounded (a peer that falls 256 frames behind is dropped and gets the full state when
  it reconnects); a send that stalls for 15 seconds ends the connection; 10 wrong pairing codes
  from one address lock it out for a minute. Relays accept only `http(s)` origins, ask the app
  about at most 8 unanswered origins and at most one per second, and refuse to listen beyond
  loopback without a pairing code.
- Closing a connection waits briefly for the other side's close, so a final `error` is not lost
  (Windows resets connections closed with unread data). Close reasons are cut to fit a close
  frame.
- Python bindings (`bindings/python`): package `ntx-protocol`, import `ntxprotocol`, built with
  PyO3 as one `abi3` wheel for Python 3.9+. Typed `Source`, `Receiver`, `Browser`, event
  dataclasses, `render`, and exceptions for each error. Events are polled and waiting releases
  the GIL. Tests on real sockets, including mDNS discovery and the example script; Python and
  Rust binding coverage at least 90%, enforced. Wheels are built in CI but not published.
