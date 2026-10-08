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
version: 2
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
  relay_relayed: { from: "relay.relayed" }
modules:
  ping:
    source: "ping"
  relay:
    source: "relay"
    in:
      ping: { from: "ping.ping" }
```

`version` is always `2`. Only the root sets `deployment`, `domain` and
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
      <port>: { from: "<sibling>.<port>" }  # or "in.<port>"
    config: "instances/robot.yaml"  # optional, caller-relative
    args: ["--mode", "lab"]         # optional replacement, not append
```

- A local `source` string is resolved relative to the `main.yaml` that
  contains the call, not to the root.
- The app's own `main.yaml` defines its ports, policies, executable and build.
  A call supplies connections and optional instance values. Input `type` may
  be omitted; if supplied, it must match the app. Every declared child output
  is visible by default, so callers need not repeat `out`. Calls cannot
  redefine endpoint policies.
- Call `config` paths are relative to the caller; default `run.config` paths
  are relative to the app. Call `args` replaces the app list, including with
  `[]`. These overrides apply only to a called executable, not descendants of
  a composite.
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
version: 2
in:
  ping: { type: "ping.Ping" }
out:
  relayed: { type: "relay.Relayed" }
run:
  kind: "service"            # default; task for finite successful work
  provider: "process"
  bin: "relay"
  # config: config.yaml   # optional app config, module-relative
  # companions: [worker]  # explicit companion binary names, no .exe suffix
  # config_assets: [assets/model.bin, ../shared/calibration.yaml]
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

`run.kind: service` treats natural exit as failure; `task` treats successful
exit as completed work. Readiness still requires the app to open its declared
named endpoints and call `ready()`.

## App-owned endpoint contracts

Stream definitions require `type` on executable inputs and outputs. Output
policies are `qos: reliable` (default) or `sensor`, and `retention: volatile`
(default) or `last`. Input policies are `replay: live` (default) or `last`,
and `buffer: fifo` (default) or `{ latest: 4 }`, with a positive depth.
Policies belong to the app definition, never to its invocation.

```yaml
in:
  readings: { type: sensor.Reading, replay: last, buffer: { latest: 4 } }
  calculate: { kind: rpc, type: math.Request, response: math.Response }
out:
  status: { type: robot.Status, qos: reliable, retention: last }
  calculate: { kind: rpc, type: math.Request, response: math.Response }
```

RPC inputs call a server; RPC outputs serve requests. Request and response
types must both agree. Stream `from` may be a single reference or an explicit
list such as `[left.reading, right.reading]`; RPC has exactly one source.
Duplicate sources are rejected. Composite output aliases may omit `type`:
`status: { from: robot.status }` preserves the child's endpoint and policies.

An executable host may delegate exclusive subsets of its declared endpoints:

```yaml
owned_children:
  worker:
    inputs: [readings]
    outputs: [status]
```

Each endpoint has one owner: the host or one named owned child. A child gets
a distinct runtime identity, but its publication/server endpoint source
remains the host's source identity. Delegation does not create another public
publisher or allow undeclared endpoints.

## Reusable apps and project instances

Keep reusable definitions in `apps/<name>/main.yaml` and deployment values in
`projects/<project>/main.yaml`:

```yaml
# projects/lab/main.yaml
version: 2
deployment: lab
providers:
  process: { type: process }
modules:
  robot:
    source: ../../apps/robot
    config: instances/robot.yaml
    args: ["--mode", "lab"]
```

That project calls the app rather than copying its leaf definition.
[`ping-pong-relay`](../examples/ping-pong-relay/README.md) demonstrates a reusable
pipeline app with a project instance while retaining the Rust package catalogs
beside their source files.

Cargo output defaults to the root's `.reiny/build/cargo`, keeping build outputs
outside revision-addressed Git checkouts. An explicit `CARGO_TARGET_DIR` is
honored. Prepared executables and runtime libraries are staged separately and
never replaced while a module is running.

`run.companions` explicitly names helper binaries to build and stage alongside
the host and runtime libraries in one prepared bundle directory. Both
`companions` and `config_assets` default to `[]`. Configuration is frozen and
fingerprinted during preparation. `config_assets` lists relative paths from
the selected configuration's origin, including a caller override. An explicit
`../shared/calibration.yaml` retains its relative tree shape; absolute asset
paths are rejected.

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

- `input` subscribes only to the exact source namespaces that the deployment wired.
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
Their builders reject managed contexts before declaring transport resources.
`engine()` returns an error and `session()` returns `None` in managed mode;
`with_engine()` and standalone reopening of a managed engine are rejected.
The owner report environment also prevents a deployment-owned process from
opening an unbound second context on a fresh engine, even if application code
clears its inherited report option.
Changing a schema catalog to `version: 2` does not turn a standalone executable
into a managed app. See the [0.8 migration guide](design/0.8.0.md).

### Named RPC and owned processes

`cloudy.uses::<Request>("calculate")?` opens a named RPC input using its single
configured server. `cloudy.provides::<Request>("calculate")?` implements an RPC
output. Both request and response types need compiled schema fingerprints.

For an `owned_children` entry, resolve the helper with
`cloudy.artifact("worker")?`, build a `tokio::process::Command`, and pass it to
`cloudy.spawn_owned_child("worker", &mut command, runtime_directory)?`.
The runtime directory must be absolute and writable. Keep the returned
`OwnedChild` alive, await `child.wait_ready()`, initialize the host, then call
`cloudy.ready()`. The SDK creates dedicated bindings and report files and
aggregates the child's verified endpoints into the host report.
`child.kill().await` waits for reaping, not merely acknowledgement.
Host shutdown first requests the child's cooperative stop and waits up to one
second for exit before forcing termination. `child.wait().await` observes that
reaped exit; explicit `kill()` remains a forced termination request.

The owner supplies `REINY_BUNDLE_DIR` and, when applicable, `REINY_CONFIG_DIR`.
`artifact()` accepts only declared executable names from the frozen bundle.
`config_dir()` returns the frozen configuration's origin for relative assets;
neither API searches a development directory. See
[`owned-child`](../examples/owned-child/README.md) for an actual delegated
publication and RPC server with a standalone verification probe.

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
Finite task deployments also return successfully when every task is completed
and reaped; the owner remains available for later `apply`. Task completion
does not restart even with `restart: always`. Failed tasks and unexpected
service exits obey the restart policy.

Deployment state is global per deployment namespace. Only the authenticated
local owner controls it, a second owner is never created for the same
deployment, and `apply` keeps the existing process (and pid) of a module whose
fingerprint didn't change. Runtime files live under the root's `.reiny/`
directory.
