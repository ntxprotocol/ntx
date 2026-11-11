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
