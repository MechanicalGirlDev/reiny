# reiny-build

Build-time code generation helper for
[reiny](https://github.com/MechanicalGirlDev/reiny). Call it from each launch's
`build.rs`.

```rust,ignore
// build.rs
fn main() {
    reiny_build::compile().unwrap();
}
```

`compile()` searches upward for the nearest `schema.yaml` and reads its required
`schema` block. It never reads `main.yaml` or falls back past an invalid catalog.
Runtime providers and module wiring are never evaluated by codegen.
Catalogs and runtime manifests require `version: 3`; version 1 is rejected.
It compiles the declared
protos with prost, and writes `$OUT_DIR/reiny_generated.rs`. The output contains
re-exports of the exported/shared/dependency types, a `reiny::MessageType` impl for each
type (with a mandatory fingerprint and its descriptor), and - if `schema.config` is present - a typed
`config::Config`.

The schema block supports both per-project catalogs (`project`, `exported_types`,
`dependencies`) and workspace catalogs (`shared_types`, `projects`, optional `schema_crates`
crates and `services`). Proto and dependency paths are relative to the directory
containing that catalog. Dependency projects provide their public catalog in
their own `schema.yaml`.

Shared schema crates use named parts under `schema.schema_crates`, even when there is
only one crate. Each part declares `crate`, a nonempty list of owned `protos`,
and optional `depends_on` part names. The legacy `schema.schema.crate` shortcut
is rejected.

```yaml
version: 3
schema:
  shared_types:
    Ping: { proto: proto/ping.proto, message: ping.Ping }
  schema_crates:
    messages:
      crate: pingpong-schema
      protos: [proto/ping.proto]
  projects:
    ping: { types: [Ping] }
```

```yaml
version: 3
schema:
  project: { name: ping }
  exported_types:
    Ping: { proto: proto/ping.proto, message: ping.Ping }
  dependencies:
    pong: { path: ../pong }
```

`protoc` is vendored via `protoc-bin-vendored`, so no system protoc install is
required.

## Explicit static port contracts

For a `no_std` app, call `reiny_build::compile_ports("ports.yaml")` from
`build.rs` alongside `compile()`. The path is explicit and relative to the
Cargo package, never discovered from an ancestor. Include the separate output
at the crate root, alongside the generated schema modules:

```rust,ignore
include!(concat!(env!("OUT_DIR"), "/reiny_ports.rs"));
```

```yaml
version: 3
in:
  measurements: { type: sensors.Sample }
out:
  commands: { type: control.Command }
rpc_clients:
  calculate: { type: calc.Request, response: calc.Response }
rpc_servers:
  reset: { type: control.Reset, response: control.ResetResult }
```

FQNs resolve through `schema.yaml` to generated Rust types. The resulting
`PORTS` slice contains `reiny::StaticPort` entries with `MessageType::TYPE_NAME` wire names
and `MessageType::SCHEMA_FINGERPRINT` fingerprints. `in` / `out` generate `Input` / `Output`;
`rpc_clients` / `rpc_servers` generate `RpcClient` / `RpcServer`, including response metadata.
Runtime-style `in` / `out` entries with `kind: rpc` also generate client / service ports.
The catalog alone grants no ports. Pass `PORTS` to `Link::new` or
`HostLink::host`; port operations use the YAML names.

License: MIT
