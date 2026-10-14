# reiny-iceoryx2

[reiny](https://github.com/MechanicalGirlDev/reiny) on
[iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2): launches on one host
talk over shared memory instead of zenoh. Same `ModuleContext`, same types, same
`main.yaml` schema; only the engine changes:

```rust
use std::sync::Arc;
use reiny::{ModuleContext, RuntimeOptions};

let mut opts = RuntimeOptions::from_args("motor");
opts.engine = Some(Arc::new(reiny_iceoryx2::Iceoryx2::new()?));
reiny::run_with(opts, |cloudy: ModuleContext| async move { /* … */ Ok(()) })
```

What it maps to (design: `docs/design/0.5.0.md` §5):

| reiny                          | iceoryx2                                                                 |
| ------------------------------ | ------------------------------------------------------------------------ |
| publish `reiny/v3/<d>/<namespace>/@out/<port>/<T>` | pub-sub service `reiny/<d>/<T>`; the complete named key rides in a user header |
| subscribe to a declared source endpoint | one subscriber on that service; exact namespace and port are header filters |
| latched / services / `@schema` | request-response service `reiny/<d>/<T>/q`                               |
| presence                       | a `reiny-alive/<key>` service held open; `watch` polls every 200 ms      |

Sharing a native service by protobuf type does not connect same-type ports.
Stream samples, RPC requests, and successful or error replies must match the
declared endpoint target. Old key epochs are rejected.

iceoryx2 is single-host. To see these launches from another machine (or from
`reiny topic` / `reiny bag`, which stay on zenoh) run one `reiny bridge iceoryx2`.

## Building

- Linux: nothing extra (iceoryx2 binds libc directly).
- Windows / macOS: `iceoryx2-pal-posix` runs bindgen, so **libclang** must be
  findable — install LLVM or point `LIBCLANG_PATH` at a directory containing
  `libclang.dll` / `libclang.dylib`. The crate is excluded from the workspace's
  `default-members` for this reason; build it explicitly with
  `cargo test -p reiny-iceoryx2`.

License: MIT
