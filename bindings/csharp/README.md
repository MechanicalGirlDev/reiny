# reiny for C#

The `Reiny` library and `Smoke` executable target .NET 10 (`net10.0`).
Building requires the .NET 10 SDK; running requires the .NET 10 runtime
(included with the SDK). The bindings use portable Cdecl `DllImport` calls
to the ergonomic C API in the same native library used by UniFFI.

Set `REINY_FFI_LIBRARY` to an absolute path to `reiny_ffi.dll`,
`libreiny_ffi.so`, or `libreiny_ffi.dylib`. Without it, .NET's native library
search resolves `reiny_ffi`.

```csharp
using Reiny;

using var bus = LocalBus.New();
using var sender = bus.Connect("sender", "demo", Path.GetFullPath("../fixtures/sender.json"));
using var receiver = bus.Connect("receiver", "demo", Path.GetFullPath("../fixtures/receiver.json"));
using var subscription = receiver.Subscriber("in", "Binary", 42UL);
using var publisher = sender.Publisher("out", "Binary", 42UL);
sender.Ready();
receiver.Ready();
publisher.Send([0, 1, 0]);
Message? message = subscription.Receive(1000);
```

`Session.Open(id, domain, zenohConfig, moduleBindingsPath)` opens a network session. The optional
configuration is a path to a Zenoh JSON5/JSON/YAML file. Sessions from one local bus share an
in-process bus; domains and prepared input source routes isolate traffic.

Every native failure throws `InvalidOperationException` with the native
error text. A receive deadline returns `null`. Payloads are copied byte arrays;
schema and timestamp are nullable `ulong`. No native message or buffer
allocation remains after returning a message.

All resources require deterministic disposal (`using` is recommended);
dispose children before sessions. Disposal is idempotent, and use after
disposal throws `ObjectDisposedException`. Operations on one resource serialize
with its disposal. `Shutdown()` shuts down the session; disposal still releases
its owned native handle. Calls are blocking, including receive.

From this directory run:

```
dotnet build Smoke/Smoke.csproj
dotnet run --project Smoke/Smoke.csproj --no-build
```

The smoke uses real local pub/sub, embedded NUL payloads, schema
values, provenance, readiness, multiple sessions, source/domain isolation,
invalid input, and deterministic disposal. It requires the native library.

Session constructors require an absolute prepared version-2 module-bindings JSON path.
Ports take a declared name, the contract type name, and a mandatory uint64 schema
fingerprint. Source routing and policies come from the prepared contract, not caller
overrides. Register all declared ports, then call `Ready()` before sending.
Publisher discovery is not exposed by this managed facade.
