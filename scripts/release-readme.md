# NTX: the `ntx` command and the C library

NTX (Network Text Exchange) is an open protocol for sending text, not video, between apps on a
local network. See <https://github.com/ntxprotocol>.

## What is here

| File | What it is |
| --- | --- |
| `ntx` (`ntx.exe` on Windows) | A test tool: send, receive, list, publish and relay text. Run `ntx --help`. |
| `lib/` | The C library: a shared library (`.dylib`, `.so` or `.dll`) and a static one (`.a`) |
| `include/ntx.h` | The C header |

## Try it

In one terminal:

```sh
./ntx send --name "Test"
```

Type a line and press Enter. In a second terminal:

```sh
./ntx recv 127.0.0.1 --field text
```

On macOS, the files are not signed yet. If macOS refuses to open them, run
`xattr -d com.apple.quarantine ntx lib/*` in this folder first.

## Licence

MIT OR Apache-2.0, at your option. See `LICENSE-MIT` and `LICENSE-APACHE`.
Copyright © 2026 CamelCase Technologies Ltd and the NTX contributors.
