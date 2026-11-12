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
- `ntx_display_name` in the C ABI: a name with the computer's name in front (spec clause 8.2),
  as `ntx::display_name` does in Rust, so every SDK can name sources the same way.
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
- C ABI error codes for each kind of core error, so bindings can tell them apart:
  `NTX_ERR_IO` (-3), `NTX_ERR_MDNS` (-4), `NTX_ERR_INVALID_TYPE_NAME` (-5),
  `NTX_ERR_UNKNOWN_CHANNEL` (-6), `NTX_ERR_TOO_LARGE` (-7), `NTX_ERR_INVALID_ADDRESS` (-8),
  `NTX_ERR_INVALID_ORIGIN` (-9), `NTX_ERR_INVALID_PATTERN` (-10) and `NTX_ERR_INVALID_MESSAGE`
  (-11). Before, all of them were `NTX_ERR_FAILED` (-2), which now means an internal error only.

### Added

- `Pattern::fields()`: the field paths a pattern uses, so apps can warn about a path a type does
  not have.

### Changed

- A `Receiver` holds at most 1,024 waiting events before a new update replaces the waiting
  update of its channel (and an announcement the waiting announcement). An app that falls far
  behind skips what it would overwrite anyway, and a flooding source cannot grow its memory
  without limit.

Following the spec's review proposals (spec PR #10, issues #7–#9):

- Clause 8.7: a source refuses web pages from internet origins with `not_allowed`, unless
  `SourceConfig::allowed_origins` lists them or the source requires pairing. Programs, local files
  (`null`) and pages on this computer or the local network are accepted as before. Also in the C
  ABI (`allowed_origins`) and the CLI (`ntx send --allow-origin`).
- Clause 14.4: a relay refuses a publisher that publishes a source id already published from
  another origin, instead of replacing it.
- Clause 16.2: array indexes have no leading zeros: `verses.01.text` finds nothing.
- Clause 19.1: a malformed version such as `00.1` is answered with `bad_message`, not
  `unsupported_version`.

### Fixed

- From the review on 12 Nov 2026:
  - Receivers ignore updates for channels they did not subscribe to (clause 8.3), so a
    hostile source cannot grow their state without limit.
  - `seq` accepts a zero fraction, as in `42.0`, like the schema does (clause 7.3).
  - Invalid channel lists and an empty pairing code are refused when starting.
  - Messages over the WebSocket limit get `too_large` (clause 18.1) instead of a reset.
  - Dropping a `Receiver` closes its connection properly; so does a peer dropped for falling
    behind (close code 1008).
  - Wrong pairing codes from one address slow its answers down (3 seconds) but no longer lock
    out the right code, so another program on the same computer cannot lock out a receiver.
    Wrong codes are logged.
  - At most 4 connections per address may be in their handshake, and the WebSocket upgrade
    must finish within 3 seconds, so idle connections from a few addresses cannot take every
    place. Stopping a source or relay no longer waits for connections still in their handshake.
  - A failed connection says "timed out connecting to …" instead of "no WebSocket upgrade".
  - `ntx send` and `ntx publish` report a line they cannot send and go on; Ctrl-C stops every
    command cleanly.
- Discovery never keeps a link-local IPv6 address (such as `fe80::1`) in a source's URL; it
  falls back to the host name. Before, Preview could fail with "No route to host".
- Dropping a `Source` or `Relay` waits (up to 1 second) until its connections have sent their
  close frames, so a program that exits right away no longer resets them.
- Discovery prefers an address on the network of the default route, so a source's URL no longer
  flips to a virtual machine bridge (such as 192.168.64.1) between runs. When one of two mDNS
  records with the same source id goes away, `Browser` reports the source `Found` under the other
  one instead of `Lost`. `ntx list` collects what it finds and lists each source id once, with
  its latest address; it no longer prints `gone:` lines.
- Clause 18.2: `fields` nested deeper than 16 levels are refused: `bad_message` when received,
  `Error::InvalidMessage` when sent. Before, only serde's limit of 128 levels applied.

- The README links spec clause 14 itself, not the spec repository, and explains the words it
  uses (source, receiver, channel, tally, relay, publisher, origin, mDNS).
- The `ntx` and `ntx-ffi` packages include the licence files (`LICENSE-MIT`, `LICENSE-APACHE`)
  and leave out the tests.

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
