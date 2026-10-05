# C bridge

`crates/reiny-ffi/include/reiny.h` is the additive C ABI to the same synchronous
facade exported with UniFFI. It is not the generated UniFFI scaffolding ABI.
The header specifies handle ownership, pointer lifetimes, and error handling.
All owned objects, messages, lists, and buffers use `reiny_release`; never use
the C allocator on library memory. Payloads are length-delimited binary bytes.

Build the shared library first from the repository root:

```sh
cargo build -p reiny-ffi
cc -std=c11 -Wall -Wextra -Werror -Icrates/reiny-ffi/include bindings/c/smoke.c \
  -Ltarget/debug -lreiny_ffi -o target/debug/c-smoke
LD_LIBRARY_PATH=target/debug target/debug/c-smoke
```

On macOS use `DYLD_LIBRARY_PATH=target/debug`. With an initialized Windows
MSVC developer prompt:

```bat
cl /nologo /W4 /WX /Icrates\reiny-ffi\include bindings\c\smoke.c /Fetarget\debug\c-smoke.exe /Fotarget\debug\c-smoke.obj /link target\debug\reiny_ffi.dll.lib
target\debug\c-smoke.exe
```

`reiny_session_open` opens the network backend; pass NULL config for defaults
or a NUL-terminated UTF-8 path to a Zenoh JSON5/JSON/YAML file. `LocalBus` handles instead
connect sessions to the same in-process bus. Presence uses
`reiny_session_publishers`, whose list entries are independent UTF-8 buffers.
