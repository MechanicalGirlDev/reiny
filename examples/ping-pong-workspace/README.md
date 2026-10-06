# ping-pong-workspace

The same Ping/Pong cycle as the ring example, with one shared proto catalog.
The root `main.yaml` contains `schema.internals` and `schema.projects`; each leaf
contains runtime ports and a process run declaration, without a schema block.
Codegen skips those leaves and inherits the root catalog. Proto paths remain relative
to the catalog, not to the leaf directory.

Both applications generate the messages independently and import
`crate::internals::{Ping, Pong}`. The ring example instead puts public catalogs and
protos in each leaf.

```rust
let pings = cloudy.output::<Ping>("ping")?;
let mut pongs = cloudy.input::<Pong>("pong")?;
cloudy.ready()?;
```

## Run

From this independent Cargo workspace:

```sh
cargo build --locked
reiny run main.yaml
```

The root process provider resolves binaries in `target/debug`. Each runtime leaf has its own
`main.yaml`, explicit `in`/`out` contracts, and a Cargo build declaration. Runtime module
paths determine identities under the deployment namespace. Ports are created synchronously;
`cloudy.ready()?` is called only after every named port has been created.
