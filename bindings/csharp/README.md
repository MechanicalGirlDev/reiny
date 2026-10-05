# reiny for C#

The `Reiny` project targets .NET 8 and uses portable Cdecl `DllImport` calls
to the additive C API in the same native library used by UniFFI.

Set `REINY_FFI_LIBRARY` to an absolute path to `reiny_ffi.dll`,
`libreiny_ffi.so`, or `libreiny_ffi.dylib`. Without it, .NET's native library
search resolves `reiny_ffi`.

```csharp
using Reiny;

using var bus = LocalBus.New();
using var sender = bus.Connect("sender", "demo");
using var receiver = bus.Connect("receiver", "demo");
using var subscription = receiver.Subscriber("Binary");
using var publisher = sender.Publisher("Binary", 42UL);
publisher.Send([0, 1, 0]);
Message? message = subscription.Receive(1000);
```

`Session.Open(id, domain, zenohConfig)` opens a network session. The optional
configuration is a path to a Zenoh JSON5/JSON/YAML file. Sessions from one local bus share an
in-process bus; domains and optional subscriber source filters isolate traffic.

Every native failure throws `InvalidOperationException` with the native
error text. A receive deadline returns `null`. Payloads are copied byte arrays;
schema and timestamp are nullable `ulong`. No native message or buffer
allocation remains after returning a message.

All resources require deterministic disposal (`using` is recommended);
dispose children before sessions. Disposal is idempotent, and use after
disposal throws `ObjectDisposedException`. Operations on one resource serialize
with its disposal. `Shutdown()` shuts down the session; disposal still releases
its owned native handle. Calls are blocking, including receive and discovery.

From this directory run:

```
dotnet build Smoke/Smoke.csproj
dotnet run --project Smoke/Smoke.csproj --no-build
```

The smoke uses real local pub/sub, embedded NUL payloads, full-width schema
values, provenance, discovery, multiple sessions, source/domain isolation,
invalid input, and deterministic disposal. It requires the native library.
