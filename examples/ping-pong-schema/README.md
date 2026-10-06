# ping-pong-schema

A shared catalog generated once in the `pingpong-schema` library. The root
`main.yaml` selects it with `schema.schema.crate: pingpong-schema`.
Codegen uses the current Cargo package to choose its role: the schema package
compiles the protos and implements `Topic`, while ping and pong re-export its
`internals` without recompiling protos. `schema/src/lib.rs` includes the output
with `reiny::schema!()`.

Both applications use `crate::internals::{Ping, Pong}` through Cargo dependencies
on the shared schema crate. The library is not a runtime module; only ping and
pong appear in the root `modules` mapping.

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
