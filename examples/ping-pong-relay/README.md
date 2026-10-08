# ping-pong-relay

A source/transform/sink pipeline: `ping.ping` feeds relay, and `relay.relayed`
feeds pong. Ping produces once per second. Relay records its canonical identity
and a hop count. Pong consumes and logs elapsed time without declaring an output.

`apps/pipeline/main.yaml` owns the reusable composition. Both the root
`main.yaml` and `projects/local/main.yaml` call it rather than copying its
leaves. Calls provide only wiring: input types come from app definitions, and
all declared child outputs are visible without caller redeclarations.

Reusable leaf definitions live in `apps/ping`, `apps/relay`, and `apps/pong`.
The build-time public catalogs remain beside their Rust packages (`ping`,
`relay`, `pong`). This preserves upward schema discovery, schema dependency
paths, and protobuf imports without duplicating runtime definitions. Sources
in the pipeline app are relative to its own manifest (`../ping`, for example).
App builds select the independent workspace with `../../Cargo.toml`.

## Run

From this independent Cargo workspace:

```sh
cargo build --locked
reiny run main.yaml
# Or run the project instance:
reiny run projects/local/main.yaml
```

The root process provider resolves binaries in `target/debug`; the project
instance uses its root-relative `../../target/debug`. Each runtime leaf has its own
`main.yaml`, explicit `in`/`out` contracts, and a Cargo build declaration. Runtime module
paths determine identities under the deployment namespace. Ports are created synchronously;
`cloudy.ready()?` is called only after every named port has been created.
The app introduces a `pipeline` namespace component, so identities are
`ping-pong-relay/pipeline/ping` and `ping-pong-relay/pipeline/relay` at the root
entry point, or `ping-pong-relay-local/pipeline/...` in the local project.
