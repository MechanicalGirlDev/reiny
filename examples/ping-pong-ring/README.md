# ping-pong-ring

Two modules exchange Ping and Pong in a cycle. Ping sends the first message and sends
the next whenever a Pong arrives; Pong replies with the same sequence number.

Each leaf owns its `schema.project`, `schema.publications`, and relative
`schema.dependencies`. Schema references may be cyclic: codegen reads only the
dependency public catalog, without recursively resolving its dependencies. Runtime
wiring is separate and uses `ping.ping` and `pong.pong`; no mutual startup dependency
is introduced. Generated types are `crate::publications::Ping` and
`crate::dependencies::pong::Pong`. The root `main.yaml` imports both directories
and forwards their outputs.

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
