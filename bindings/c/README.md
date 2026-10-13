# C bridge

`crates/reiny-ffi/include/reiny.h` is the ergonomic C ABI to the same synchronous
facade exported with UniFFI. It is not the generated UniFFI scaffolding ABI.
The header specifies handle ownership, pointer lifetimes, and error handling.
All owned objects, messages, and buffers use `reiny_release`; never use
the C allocator on library memory. Payloads are length-delimited binary bytes.

Build the shared library first from the repository root:

```sh
cargo build -p reiny-ffi
cc -std=c11 -Wall -Wextra -Werror -Icrates/reiny-ffi/include bindings/c/smoke.c \
  -Ltarget/debug -lreiny_ffi -o target/debug/c-smoke
LD_LIBRARY_PATH=target/debug target/debug/c-smoke "$PWD/bindings/fixtures/sender.json" "$PWD/bindings/fixtures/receiver.json"
```

On macOS use `DYLD_LIBRARY_PATH=target/debug`. With an initialized Windows
MSVC developer prompt:

```bat
cl /nologo /W4 /WX /Icrates\reiny-ffi\include bindings\c\smoke.c /Fetarget\debug\c-smoke.exe /Fotarget\debug\c-smoke.obj /link target\debug\reiny_ffi.dll.lib
target\debug\c-smoke.exe "%CD%\bindings\fixtures\sender.json" "%CD%\bindings\fixtures\receiver.json"
```

`reiny_session_open` opens the network backend; pass NULL config for defaults
or a NUL-terminated UTF-8 path to a Zenoh JSON5/JSON/YAML file. `LocalBus` handles instead
connect sessions to the same in-process bus.

Both session constructors require an absolute prepared module-bindings JSON path.
Publisher and subscriber registration take the declared port name, type name, and
mandatory uint64 schema. Inputs use exact sources from the prepared contract.
Register all declared ports before `reiny_session_ready`. The smoke uses schema 42.

`bindings\c\check-windows.cmd compile-only` initializes MSVC and compiles both
C and C++ smokes without linking or running the native library.
