# reiny-build

Build-time code generation helper for
[reiny](https://github.com/MechanicalGirlDev/reiny). Call it from each grain's
`build.rs`.

```rust,ignore
// build.rs
fn main() {
    reiny_build::compile().unwrap();
}
```

`compile()` searches upward for the nearest `Reiny.toml`, compiles the declared
protos with prost, and writes `$OUT_DIR/reiny_generated.rs`. The output contains
re-exports of the publication/dependency types, a `reiny::Topic` impl for each
type (with the topic string embedded), and — if `[config]` is present — a typed
`config::Config`.

`protoc` is vendored via `protoc-bin-vendored`, so no system protoc install is
required.

License: MIT
