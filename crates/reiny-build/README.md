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

`compile()` searches upward for the nearest `main.yaml` containing a `schema`
block, skipping runtime-only module manifests. It reads only that build-time
catalog; runtime providers and module wiring are never evaluated by codegen.
Catalogs accept versions 1 and 2 because their schema shape is unchanged.
Managed runtime manifests and bindings still require version 2.
It compiles the declared
protos with prost, and writes `$OUT_DIR/reiny_generated.rs`. The output contains
re-exports of the publication/dependency types, a `reiny::Topic` impl for each
type (with the topic string embedded), and - if `schema.config` is present - a typed
`config::Config`.

The schema block supports both per-project catalogs (`project`, `publications`,
`dependencies`) and workspace catalogs (`internals`, `projects`, optional `schema`
crates and `services`). Proto and dependency paths are relative to the directory
containing that catalog. Dependency projects provide their public catalog in
their own `main.yaml`.

```yaml
version: 2
schema:
  project: { name: ping }
  publications:
    Ping: { proto: proto/ping.proto, message: ping.Ping }
  dependencies:
    pong: { path: ../pong }
```

`protoc` is vendored via `protoc-bin-vendored`, so no system protoc install is
required.

License: MIT
