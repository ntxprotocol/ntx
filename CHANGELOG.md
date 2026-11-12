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
- `Receiver::reconnect` and `Publisher::reconnect` (C ABI: `ntx_receiver_reconnect`,
  `ntx_publisher_reconnect`): connect again now, also after an error that stopped reconnecting.

### Changed

Following the spec's interop errata (spec commit `d76e9ba`):

- Clause 11.3: after `unsupported_version`, `pairing_required`, `pairing_failed` or
  `not_allowed`, receivers and publishers report the error and wait for `reconnect` instead of
  retrying. With `reconnect: false` they now also wait for `reconnect()` after a lost connection
  (before, their background task ended). The wait goes back to 1 second only after an `update`
  (a publisher: after a connection that lasted 10 seconds). `ntx recv` and `ntx publish` exit
  with an error in these cases. The new `ErrorCode::needs_user` tells these errors apart.
- Clause 8.3: `ReceiverEvent::Connected` comes with the first `update`, which confirms the
  subscription (an earlier `announce` is held until then). Receivers ask only for channels listed
  in `hello` (and close instead of subscribing to none of them), and close with 4003 instead of
  subscribing when a pairing code is needed and they have none.
- Clauses 8.6, 11.2, 12.5: timeouts close with 4008: no `subscribe` or `publish` within 10
  seconds, no `hello` within 10 seconds, or nothing received for 15 seconds.
- Clause 12.4: an unknown error code is received as `bad_message`, with the code at the start of
  the message. `ErrorCode::Other`, `impl Deserialize for ErrorCode` and
  `impl From<&str> for ErrorCode` are removed, and `ErrorCode` is `Copy`.
- Clauses 13.3, 13.4: pairing codes are compared in NFC. A malformed proof is `pairing_failed` and
  a missing one `pairing_required`; `PairingProof::proof` is now optional.
- Clause 16.4: numbers become text as ECMAScript `Number.prototype.toString` writes them (`3.0` →
  `3`, `1e21` → `1e+21`).
- Clauses 16.3, 16.5: only spaces and tabs around a field path are ignored; a pattern renders
  empty text for a cleared channel.
- Clause 6.2: names are shortened without splitting a grapheme cluster; display names are cut to
  256 characters, and sources, relays, receivers and publishers refuse ids and names outside
  the clause 18.4 limits.
- Clauses 7.3, 18.4: type descriptors check `title` and `description` lengths and the value of
  every allowed schema keyword. Optional members may be absent but not `null`, as in the
  schemas (`{"proof": null}` is `bad_message`).
