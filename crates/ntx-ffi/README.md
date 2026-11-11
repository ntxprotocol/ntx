# ntx-ffi

The C library for [NTX (Network Text Exchange)](https://github.com/ntxprotocol/spec). It is the one
native interface that the C/C++, Python, Node, Java and .NET bindings wrap.

- Header: [`include/ntx.h`](include/ntx.h), generated from [`src/lib.rs`](src/lib.rs) by cbindgen.
- Library: `libntx_ffi` (`.so` / `.dylib` / `.dll`, and a static `.a` / `.lib`).
- Example: [`examples/hello.c`](examples/hello.c).

## How it works

| Topic | Rule |
| --- | --- |
| Objects | Opaque handles: `NtxSource`, `NtxReceiver`, `NtxBrowser`, `NtxPublisher`, `NtxRelay`, freed with `ntx_*_free` |
| Data | Settings, fields and events are UTF-8 JSON strings |
| Events | Polled with `ntx_*_wait_event(handle, timeout_ms)`; no callbacks into your code |
| Errors | `NTX_OK` (0) or a negative code; `ntx_last_error()` explains the failure |
| Strings | A returned `char *` is yours: free it with `ntx_string_free` |
| Threads | Handles may be used from any thread |

Events look like `{"event": "update", "data": {"channel": "program", "seq": 1, "type": "ntx.text",
"fields": {"text": "Hello"}}}`.

## Build and try

```bash
cargo build -p ntx-ffi
cc crates/ntx-ffi/examples/hello.c -I crates/ntx-ffi/include -L target/debug -lntx_ffi -o hello
DYLD_LIBRARY_PATH=target/debug ./hello    # LD_LIBRARY_PATH on Linux
```

After changing `src/lib.rs`, regenerate the header (a pre-commit hook checks it):

```bash
cd crates/ntx-ffi && cbindgen --config cbindgen.toml --output include/ntx.h
```
