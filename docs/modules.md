# Modules and `main.yaml`

This is the normative guide for describing and running a reiny deployment.
Every directory that takes part has exactly one `main.yaml`. The same file
format covers three roles:

- a **root**, which names the deployment and configures providers,
- a **composite module**, which calls child modules and wires their ports,
- a **leaf module**, which declares one native executable with `run`.

A machine-readable description of the fields lives in
[`main.schema.yaml`](main.schema.yaml) (JSON Schema written as YAML). The Rust
structs in `crates/reiny-launch/src/modules.rs`, `acquire.rs` and
`artifacts.rs` are the source of truth if the two ever disagree.

Older design records under [`design/`](design) describe `Reiny.toml` and
`launch.yaml` launch configs. Those formats are gone; read those files as
history only.

## Root

```yaml
version: 1
deployment: "ping-pong-relay"      # deployment identity
# domain: "lab"                    # optional, defaults to the deployment name
providers:
  process:
    type: "process"
    bin_dir: "target/debug"        # prebuilt executable search dir, root-relative
    # zenoh_config: zenoh.json5     # optional, passed through unchanged
    # connect: ["tcp/10.0.0.2:7447"]
in: {}
out:
  relay_relayed: { type: "relay.Relayed", from: "relay.relayed" }
modules:
  ping:
    source: "ping"
    out:
      ping: { type: "ping.Ping" }
  relay:
    source: "relay"
    in:
      ping: { type: "ping.Ping", from: "ping.ping" }
    out:
      relayed: { type: "relay.Relayed" }
```

`version` is always `1`. Only the root sets `deployment`, `domain` and
`providers`.

### Providers

Two built-in provider types ship today:

| `type` | What it does |
| --- | --- |
| `process` | Spawns and supervises native executable modules. Fields: `bin_dir` (default `.`), `zenoh_config`, `connect`. |
| `artifact` | Materializes immutable local files declared under `resources` with `type: artifact.file`. |

There is no plugin mechanism for external providers. A child module may rename
the root's provider slots through its call's `providers` map
(`child_slot: root_slot`), but it can't define new providers.

## Composite modules

A module call has this shape:

```yaml
modules:
  <name>:
    source: "relative/dir"          # or { git, ref, path }
    providers: { process: process } # optional slot aliases
    in:
      <port>: { type: "pkg.Message", from: "<sibling>.<port>" }  # or "in.<port>"
    out:
      <port>: { type: "pkg.Message" }
```

- A local `source` string is resolved relative to the `main.yaml` that
  contains the call, not to the root.
- A Git source is `{ git: <url>, ref: <branch|tag|commit>, path: <subdir> }`.
  `path` defaults to the repository root. The first resolution pins the commit
  in the root's `lock.yaml`, and later runs reuse that pin. Only
  `reiny update` moves it. Fetching a module never runs a build, and Reiny
  never falls back to a newer ref or an older executable when a fetch or build
  fails.
- `in.<port>.from` names either a sibling's output (`relay.relayed`) or an
  input of the enclosing module (`in.ping`). Wiring is always explicit. Reiny
  doesn't guess a connection from matching types.
- The `out` entries on a module's own top level may forward a child endpoint
  with `from`. That's a reference to the existing endpoint, not a second
  publisher.
- A nested module's `in` and `out` are its public contract. The parent wires
  those ports; it can't reach inside.

## Leaf modules

```yaml
version: 1
in:
  ping: { type: "ping.Ping" }
out:
  relayed: { type: "relay.Relayed" }
run:
  provider: "process"
  bin: "relay"
  # config: config.yaml   # optional app config, module-relative
  # args: []
  # restart: manual        # manual | on_failure | always
  # on_failure: report     # report | suspend_deployment
build:
  type: "cargo"
  manifest: "../Cargo.toml"  # default Cargo.toml, module-relative
  package: "relay"
  profile: "debug"           # default release
  features: []
  default_features: true
  locked: true
schema:
  # build-time catalog read by reiny-build, see crates/reiny-build/README.md
```

`run` and `modules` are mutually exclusive in one file. `build` is only
allowed next to `run`. Without `build`, the executable must already exist in
the provider's `bin_dir`.

Cargo output defaults to the root's `.reiny/build/cargo`, keeping build outputs
outside revision-addressed Git checkouts. An explicit `CARGO_TARGET_DIR` is
honored. Prepared executables and runtime libraries are staged separately and
never replaced while a module is running.

The optional `schema` block is the build-time type catalog that
`reiny_build::compile()` reads. Runtime tooling keeps it intact but never
executes anything from it.

## Namespaces and topics

Each executable gets a logical namespace `deployment/module/nested...`. Topic
keys still carry the short `Topic::TYPE`, so Link hashes and existing wire
keys don't change. The fully qualified message name and schema fingerprint are
checked locally when a port opens, and again across the reports of connected
modules.

## Writing a managed executable

```rust,ignore
#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let out = cloudy.output::<Relayed>("relayed")?;
    let mut incoming = cloudy.input::<Ping>("ping")?;
    cloudy.ready()?;           // sync; ports can't be created afterwards
    while let Some(ping) = incoming.recv().await { /* ... */ }
    Ok(())
}
```

- `input` subscribes only to the source namespace that the deployment wired.
- `output` publishes in the module's own namespace. Two outputs with the same
  transport type in one executable are rejected.
- `ready()` publishes `@ready`, which is separate from the `@launch` presence
  token. `apply` waits for it.
- A `@stop` request is acknowledged on receipt. The ack means "stop
  requested", not "process exited".
- Managed mode needs an engine with query, liveliness and attachment support.
  `LinkEngine` doesn't have attachments, so it's refused in managed mode.
  Standalone link routing still works.
- A namespaced ID is capped at 32 bytes on a link and an iceoryx2 key at 224
  bytes. Longer values fail; they're never truncated.

The raw `publish`, `subscribe`, `serve` and `call` APIs still exist for
**standalone** programs that run outside a deployment.

## CLI lifecycle

| Command | Effect |
| --- | --- |
| `reiny plan [path] [--json]` | Resolve sources and show modules and connections. Builds nothing. |
| `reiny apply [path] [--detach]` | Prepare artifacts (declared builds, staged files), start or reconcile managed processes, wait for explicit readiness and compare reported port schemas. |
| `reiny run [path]` | Same contract as `apply`, in the foreground. Ctrl+C stops the deployment. |
| `reiny status [path] [--json]` | Read state from the live owner. Retained state may show `owner_alive: false`. |
| `reiny stop [path] [--json]` | Ask modules to stop cooperatively, then reap. Remaining owned process trees are killed. |
| `reiny update [path] [--json]` | Move Git refs and rewrite `lock.yaml`. |
| `reiny compress [path] [--out dist]` | Build and bundle a self-contained deployment. |
| `reiny new` / `init` / `add` | Scaffold a leaf module, or add a local schema dependency. |
| `reiny check [path]` | Validate composition or describe the schema without compiling. |
| `reiny build [--release]` | Prepare declared build artifacts without starting processes. |

`apply` and `run` share `--bin-dir`, `--log-level` and `--ready-timeout`
(seconds, default 30). A bare `reiny <path>` or `reiny --config <path>` is
shorthand for a foreground `apply` of that root. `apply --detach` returns once readiness is confirmed.
A foreground run registers its stop subscription before it handles Ctrl+C.

Deployment state is global per deployment namespace. Only the authenticated
local owner controls it, a second owner is never created for the same
deployment, and `apply` keeps the existing process (and pid) of a module whose
fingerprint didn't change. Runtime files live under the root's `.reiny/`
directory.
