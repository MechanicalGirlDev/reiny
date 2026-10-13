# reiny for JavaScript and TypeScript

This ESM package runs on Node.js 20 or newer. It uses Koffi to call the ergonomic
C API in the same `reiny_ffi` library used by UniFFI.

Install dependencies with `npm ci`. Set `REINY_FFI_LIBRARY` to the absolute
path of `reiny_ffi.dll`, `libreiny_ffi.so`, or `libreiny_ffi.dylib`, or call
`loadLibrary(path)` before constructing any objects. Without an explicit path,
the platform's native library search path is used.

```js
import { LocalBus } from "@reiny/ffi";

const bus = LocalBus.new();
const sender = bus.connect("sender", "demo", "/absolute/path/to/prepared/sender.json");
const receiver = bus.connect("receiver", "demo", "/absolute/path/to/prepared/receiver.json");
const subscription = receiver.subscriber("in", "Binary", 42n);
const publisher = sender.publisher("out", "Binary", 42n);
sender.ready();
receiver.ready();
try {
  publisher.send(Uint8Array.of(0, 1, 0));
  console.log(subscription.receive(1000));
} finally {
  publisher.dispose();
  subscription.dispose();
  receiver.dispose();
  sender.dispose();
  bus.dispose();
}
```

`Session.open(id, domain, zenohConfig, moduleBindingsPath)` opens a network session; `zenohConfig`
is an optional path to a Zenoh JSON5/JSON/YAML configuration file. Local sessions connected through
one `LocalBus` share an in-process bus. Domains isolate traffic.

Calls are synchronous, including `receive(timeoutMs)`.
Use a Node worker thread when blocking must not pause an application's event
loop. A receive deadline returns `undefined`; every native error throws
`ReinyError`, which exposes the native status and error text.
Payloads are copied binary bytes, including embedded NULs. Schema fingerprints
and timestamps use `bigint`, never lossy JavaScript numbers.

Dispose every resource deterministically, preferably children before their
session. `dispose()` is idempotent; use after disposal throws. `shutdown()`
shuts down a session and can throw if the native operation fails.

Run `npm run check`, `npm test`, `npm run smoke`, and
`npm run smoke:typescript`. Both examples require the native library.

Session constructors require an absolute prepared version-2 module-bindings JSON path.
Ports take a declared name, the contract type name, and a mandatory uint64 schema
fingerprint. Source routing and policies come from the prepared contract, not caller
overrides. Register all declared ports, then call `ready()` before sending.
Publisher discovery is not exposed by this managed facade.
