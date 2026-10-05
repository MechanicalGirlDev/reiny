# reiny-ffi

The multilingual reiny SDK. One Rust library exposes a synchronous UniFFI
facade and an additive, handle-based C ABI. Both call the same reiny engine:
there is no separate wire protocol or subprocess adapter.

| Language | Binding |
| --- | --- |
| Rust | Existing typed `reiny` SDK; or the `reiny-ffi` Rust library |
| Python | UniFFI-generated module, using standard-library `ctypes` |
| Kotlin | UniFFI-generated JVM/Android classes, using JNA |
| Swift | UniFFI-generated Swift module and C module map |
| Ruby | UniFFI-generated module, using the `ffi` gem |
| C | [`include/reiny.h`](include/reiny.h) |
| C++ | [`include/reiny.hpp`](include/reiny.hpp), C++17 RAII |
| JavaScript | [`../../bindings/javascript`](../../bindings/javascript), Node.js ESM |
| TypeScript | The same runtime with declarations and a TypeScript example |
| C# | [`../../bindings/csharp`](../../bindings/csharp), .NET |

UniFFI 0.32.2 supplies Python, Kotlin, Swift, and Ruby generation. It does not
supply an ergonomic stable C, C++, Node.js, or C# SDK. Those bindings use our
documented C bridge, not UniFFI's internal symbols. The native library works on
Windows, Linux, and macOS when built for the host architecture. JavaScript runs
in Node.js, not in a browser; Kotlin targets JVM/Android, not Kotlin/JS.

## Message contract

Pass the **type segment** from the generated Rust `Topic::TYPE`, for example
`Ping`, not a Zenoh path or necessarily the fully qualified Protobuf name.
Compile the same `.proto` files for each language and serialize their messages
with that language's Protobuf implementation. The FFI sends those wire bytes
unchanged, including empty payloads and embedded NULs. It does not JSON-encode
messages or generate your application-specific Protobuf classes.

Publishing goes to `reiny/<domain>/<id>/<type>` and subscribing to that type
receives all sources in the domain unless a source filter is supplied.
`Message` includes the original bytes, source id, optional schema fingerprint,
and optional Unix-nanosecond engine timestamp. Supply the generated
`Topic::SCHEMA` when available; an absent fingerprint is allowed just as it is
for handwritten Rust topics. This raw foreign receive API reports fingerprints
rather than validating them or decoding Protobuf.

The initial foreign API covers launch creation, binary publish/subscribe,
source filtering, publisher discovery, and cooperative shutdown. Typed Rust
services, latched/QoS builders, descriptor advertisement, link transports, and
ROS bridges remain in the Rust SDK; they are not foreign API methods.

## Build

From the repository root:

```sh
cargo build -p reiny-ffi --features bindgen
```

This builds the shared library and the matching `reiny-bindgen` CLI. For
distribution, add `--release` and use `target/release` everywhere below.

| Host | Library | C linker artifact |
| --- | --- | --- |
| Windows MSVC | `target/debug/reiny_ffi.dll` | `target/debug/reiny_ffi.dll.lib` |
| Linux | `target/debug/libreiny_ffi.so` | the `.so` |
| macOS | `target/debug/libreiny_ffi.dylib` | the `.dylib` |

`--no-default-features` builds a local-only SDK, with no Zenoh dependency.
`Session.open` then returns an error; it never silently substitutes a local bus.
The `lib`, `cdylib`, and `staticlib` crate types support Rust, dynamic foreign
loading, and native static linking respectively.

## Generate UniFFI bindings

Use the generator built from this crate so its version exactly matches the
library. Example for Windows (replace `LIB` with the host library above):

```sh
LIB=target/debug/reiny_ffi.dll
for LANG in python kotlin swift ruby; do
  target/debug/reiny-bindgen generate \
    --library "$LIB" --language "$LANG" \
    --out-dir "bindings/generated/$LANG" --no-format
done
```

The generator locates this crate's `uniffi.toml` through Cargo metadata.
UniFFI 0.32's `--config` option is for a separate global configuration file,
not this per-crate file.

The output is intentionally ignored by Git: regenerate it whenever the Rust
interface changes and ship the generated source and the matching library
together. Do not reuse bindings generated with another UniFFI version.

### Python

Place the library beside `bindings/generated/python/reiny_ffi.py`, then:

```sh
cp "$LIB" bindings/generated/python/
PYTHONPATH=bindings/generated/python python bindings/python/smoke.py
```

```python
from reiny_ffi import LocalBus

bus = LocalBus()
sender = bus.connect("sender", "lab")
receiver = bus.connect("receiver", "lab")
subscription = receiver.subscriber("Ping", None)
publisher = sender.publisher("Ping", None)
publisher.send(b"\x0a\x03\x00\x01\xff")  # Ping { bytes data = 1; }
message = subscription.receive(1000)
assert message is not None
assert message.source == "sender"
```

For cross-process communication, use
`Session.open("sender", "lab", "zenoh.json5")`. The third argument is an optional
Zenoh configuration **file path**, not inline JSON. Pass `None` for defaults.
LocalBus sessions communicate only with sessions attached to that same object.

### Kotlin

Use the generated `.kt` source in package `dev.mechanicalgirl.reiny`, the
native library for each target architecture, and JNA 5. The generated source
specifies its imports; Kotlin objects support deterministic `.use { ... }`
cleanup. Compile `bindings/kotlin/Smoke.kt` together with the generated source:

```sh
kotlinc bindings/generated/kotlin/dev/mechanicalgirl/reiny/*.kt bindings/kotlin/Smoke.kt \
  -cp "$JNA_JAR" -include-runtime -d target/reiny-kotlin-smoke.jar
java -Djna.library.path=target/debug \
  -cp "target/reiny-kotlin-smoke.jar:$JNA_JAR" SmokeKt
```

On Windows, use `;` instead of `:` as the Java classpath separator. On Android,
package the library under `jniLibs/<abi>/` and include JNA's Android native
dependency.

### Swift

Compile the generated `Reiny.swift` with the generated `ReinyFFI` header and
module map accessible on the Swift import path, linking `reiny_ffi`. For example,
on macOS:

```sh
swiftc -emit-library -emit-module -module-name Reiny \
  bindings/generated/swift/Reiny.swift \
  -I bindings/generated/swift \
  -Xcc -fmodule-map-file=bindings/generated/swift/ReinyFFI.modulemap \
  -L target/debug -lreiny_ffi \
  -o bindings/generated/swift/libReiny.dylib
swiftc bindings/swift/Smoke.swift \
  -I bindings/generated/swift -L bindings/generated/swift -lReiny \
  -Xcc -fmodule-map-file=bindings/generated/swift/ReinyFFI.modulemap \
  -L target/debug -lreiny_ffi -o target/reiny-swift-smoke
DYLD_LIBRARY_PATH=target/debug:bindings/generated/swift target/reiny-swift-smoke
```

For iOS, build the Rust library for each iOS target and package the generated
Swift module with those native artifacts. Generating Swift on another host
does not prove an iOS build.

### Ruby

Install the `ffi` gem, make the native library available to the dynamic loader
(`PATH` on Windows, `LD_LIBRARY_PATH` on Linux, `DYLD_LIBRARY_PATH` on macOS),
then:

```sh
ruby -I bindings/generated/ruby bindings/ruby/smoke.rb
```

### C, C++, JavaScript, TypeScript, and C#

See each directory under [`../../bindings`](../../bindings) for build and smoke
commands. C callers must release owned handles exactly once, including
message/list/buffer outputs. C++ and managed wrappers provide deterministic
resource cleanup. The C header documents borrowed pointers, buffer lifetime,
error retrieval, timeout results, and concurrency contracts.

## Lifetimes, concurrency, and shutdown

Children retain their session and runtime, so releasing the language-side
session variable does not invalidate an existing publisher or subscription.
Publisher presence disappears when its last handle is released. Explicitly
dispose bridge objects; an unreleased C handle stays alive in the registry.
Generated UniFFI wrappers use their language's reference lifetime conventions.

All methods are synchronous. Use a worker thread for blocking receives in a GUI,
async Python application, or Node.js event loop. Rust async callers should use
`spawn_blocking` or the existing async typed SDK. Methods that drive the runtime
reject calls inside an active Tokio runtime.

Each subscription uses a 256-message FIFO with backpressure, like the Rust
default. Drain it to avoid stalling delivery. `receive(timeout_ms)` returns no
message on timeout or shutdown. Concurrent receivers compete for the same
FIFO; each sample goes to only one receiver. `Session.shutdown()` wakes pending
receives and prevents further receive delivery. It is cooperative shutdown,
not forced disposal; it does not revoke publisher handles or remove presence.

## Checks

```sh
cargo test -p reiny-ffi
cargo test -p reiny-ffi --no-default-features
cargo clippy -p reiny-ffi --all-targets --features bindgen -- -D warnings
cargo fmt --all --check
```

Rust regression tests exchange actual Protobuf messages between the typed SDK
and the foreign facade on the same engine, and exercise two network sessions
over an ephemeral TCP listener after an explicit subscriber-ready event.
Language smoke examples load the real shared library. After local-only feature
tests, rebuild with `cargo build -p reiny-ffi --features bindgen` before running
network examples: Cargo may have replaced the shared library with the
local-only variant. Kotlin, Swift, and Ruby generation is separate from
execution: run their smoke examples on hosts with those toolchains installed.
