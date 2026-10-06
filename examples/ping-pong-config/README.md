# ping-pong-config

Pong reads its reply text and delay from typed configuration. Defaults live in
`pong/main.yaml` under `schema.config`: `reply: pong` and `delay_ms: 0`.
Codegen generates `crate::config::Config`, available through `cloudy.config()`.

The leaf `run.config: ../pong.config.yaml` resolves relative to that leaf and
overlays defaults with `reply: PONG!` and `delay_ms: 250`. Overrides may also
use TOML or JSON by extension. Unknown keys and mismatched value types warn
and leave the default in place. The override is application configuration,
not a second primary manifest.

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
