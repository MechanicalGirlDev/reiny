# reiny-iceoryx2

[reiny](https://github.com/MechanicalGirlDev/reiny) on
[iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2): grains on one host
talk over shared memory instead of zenoh. Same `Cloudy`, same types, same
`Reiny.toml` — only the engine changes:

```rust
use std::sync::Arc;
use reiny::{Cloudy, RuntimeOptions};

let mut opts = RuntimeOptions::from_args("motor");
opts.engine = Some(Arc::new(reiny_iceoryx2::Iceoryx2::new()?));
reiny::run_with(opts, |cloudy: Cloudy| async move { /* … */ Ok(()) })
```

What it maps to (design: `docs/design/0.5.0.md` §5):

| reiny                          | iceoryx2                                                                 |
| ------------------------------ | ------------------------------------------------------------------------ |
| publish `reiny/<d>/<id>/<T>`   | pub-sub service `reiny/<d>/<T>`; the source id rides in a user header    |
| subscribe `reiny/<d>/*/<T>`    | one subscriber on that service; a named source is a header filter        |
| latched / services / `@schema` | request-response service `reiny/<d>/<T>/q`                               |
| presence                       | a `reiny-alive/<key>` service held open; `watch` polls every 200 ms      |

iceoryx2 is single-host. To see these grains from another machine (or from
`reiny topic` / `reiny bag`, which stay on zenoh) run one `reiny bridge iceoryx2`.

## Building

- Linux: nothing extra (iceoryx2 binds libc directly).
- Windows / macOS: `iceoryx2-pal-posix` runs bindgen, so **libclang** must be
  findable — install LLVM or point `LIBCLANG_PATH` at a directory containing
  `libclang.dll` / `libclang.dylib`. The crate is excluded from the workspace's
  `default-members` for this reason; build it explicitly with
  `cargo test -p reiny-iceoryx2`.

License: MIT
