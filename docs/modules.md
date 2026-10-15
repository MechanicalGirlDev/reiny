# Modules and `main.yaml`

This is the normative guide for describing and running a reiny deployment.
A module definition is reusable; an instance is one invocation of it. Each
executable instance is a graph node whose process belongs to a runtime generation.
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
version: 3
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

`version` is always `3`. Only the root sets `deployment`, `domain` and
`providers`. The logical domain is a Reiny key-space segment, independent of
`ROS_DOMAIN_ID`; it is not an ACL or an authorization boundary.

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
    config_assets: ["assets/model.bin"] # optional replacement, config-parent-relative
    args: ["--mode", "lab"]         # optional replacement, not append
```

- A local `source` string is resolved relative to the `main.yaml` that
  contains the call, not to the root.
- The app's own `main.yaml` defines its ports, policies, executable and build.
  A call supplies connections and optional instance values. Input `type` may
  be omitted; if supplied, it must match the app. Every declared child output
  is visible by default, so callers need not repeat `out`. Calls cannot
  redefine endpoint policies.
- Invocation ports are connections and optional type assertions, not endpoint
  definitions. Previously serialized default policy fields remain accepted,
  but nondefault policies are rejected during parsing. Rust `ModuleCall` input
  and output maps use `PortConnection`; app definitions use `PortSpec`.
- Call `config` paths are relative to the caller; default `run.config` paths
  are relative to the app. Call `config_assets` and `args` replace their app
  lists, including with `[]`; omission retains the app defaults.
  Asset paths are relative to the selected config's parent, whether that config
  comes from the app or caller. Nonempty assets require a selected config.
  These overrides apply only to a called executable, not descendants of a composite.
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
version: 3
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
  profile: "dev"             # default release
  features: []
  default_features: true
  locked: true
  cwd: ".."                  # module-relative; defaults to "."
  target: "x86_64-pc-windows-msvc" # optional Cargo target triple
  env: { RUSTFLAGS: "-C debuginfo=1" }
```

Every runtime manifest must declare `run` or a nonempty `modules` mapping.
`run` and `modules` are mutually exclusive in one file. `build` is only
allowed next to `run`. Without `build`, the executable must already exist in
the provider's `bin_dir`.

Cargo builds accept `manifest`, `package`, `profile`, `features`,
`default_features`, `locked`, `cwd`, `target`, and string-valued `env`.
Relative `manifest` and `cwd` paths are resolved against the declaring
`main.yaml`; Cargo runs in `cwd`, retaining applicable ancestor `.cargo`
configuration. Unless `--build-cache` is supplied, Cargo's persistent target
directory uses `CARGO_TARGET_DIR` when set, otherwise
`<deployment-root>/.reiny/build`. Compatible requests share a target only when
their resolved manifest, package, cwd, profile, normalized feature set,
default-feature and lock settings, target, compiler environment, and cache
root match. `--jobs` bounds independent groups; work sharing a Cargo target is
serialized.

An explicit command builder can produce executables and bundle files without
an implicit shell:

```yaml
build:
  type: command
  argv: ["sh", "build.sh"]       # shell use is explicit
  cwd: "."
  env: { BUILD_MODE: release }
  outputs:
    app: "out/app"               # logical names have no platform suffix
    helper: "out/helper"
  files:
    "data/table.bin": "out/table.bin" # bundle destination: source path
```

On Windows, a project can explicitly invoke its command interpreter, for
example `argv: ["cmd.exe", "/d", "/c", "build.cmd"]`, with an output source
such as `out/app.exe`. `outputs` must declare `run.bin` and every
`run.companions` executable. Reiny stages each under its logical name with the
platform executable suffix. `files` maps safe relative bundle destinations to
source files relative to `cwd`; executable and file sources must exist as
files. Only declared `outputs` and `files` are frozen; list every additional
runtime file the prepared module needs rather than relying on other files in
the builder's output directory. Reiny rejects missing, duplicate or
case-conflicting outputs and unsafe destinations rather than guessing a
binary path. Command builders run on each prepare; they are not treated as
cached when their output bytes happen to match. Prebuilt modules omit `build`
and use the installed provider binaries. `--release` affects Cargo profiles
only, and `--bin-dir` affects only modules without a build declaration.

`reiny bundle` preserves declared command `files` when the distribution is
prepared again as prebuilt. Each bundled binary directory contains a private
`.reiny-bundle.json` file list for its frozen executables, companions, runtime
libraries and declared files. That name is reserved at the binary directory
root. Preparation rejects invalid versions, unsafe paths, missing executables
or files, and does not copy unrelated neighbors from the binary directory.

Preparation can expose progress, cancel owned builders, and create a
source-independent verified snapshot:

```sh
reiny capabilities --json
reiny prepare main.yaml --out prepared.json --events jsonl \
  --build-cache .reiny-build-cache --jobs 2
reiny apply --prepared prepared.json
```

The JSON capability response identifies `reiny_version`,
`prepare_protocol`, `prepared_file`, `apply_protocol`, `build_kinds`, and
`controlled_apply`. Consumers should require the expected version and protocol
values instead of inferring support from the version string alone.
For this CLI, `reiny capabilities --json` returns:

```json
{"reiny_version":"0.11.0","prepare_protocol":1,"prepared_file":1,"apply_protocol":1,"build_kinds":["cargo","command"],"controlled_apply":true}
```

The prepared output must be new; Reiny does not replace an existing file.
Apply verifies the versioned envelope, payload hash, bindings, frozen
executables, companions, configuration/assets and resources before starting
the owner. It does not resolve sources or rebuild. This SHA-256 integrity
check detects changed or missing bytes; it is not a signature. Frozen files
are independent of later source or Cargo-output changes, while the declared
application working directory must still exist. Use `--events jsonl` for
newline-terminated, sequenced progress events on stdout; diagnostics remain
available in the retained preparation log and on stderr. Add
`--control-stdin` when a supervising process will send
`{"v":1,"cmd":"cancel"}` or when stdin EOF should cancel. Ctrl+C also cancels
and reaps builder descendants before prepare exits. `prepare` without
`--control-stdin` does not treat an already-closed stdin as cancellation.
JSONL records are UTF-8 objects with `v: 1`, a strictly increasing `seq`, and
an event payload. Preparation emits `started`, phase changes, `planned`, group
state transitions, Cargo `compiled` records, diagnostics, and exactly one
terminal `finished` record. Groups cover Cargo, command, prebuilt executable,
configuration, and resource work; Cargo progress reports observed compiler
artifacts rather than an estimated total. A controlled apply stays attached to
the owner for its lifetime and reports owner/readiness/status events through
the same sequence envelope. Its cancel control is the same JSON line; EOF
cancels only when `--control-stdin` is selected. `--control-stdin` and
`--detach` cannot be combined.

`run.kind: service` treats natural exit as failure; `task` treats successful
exit as completed work. Readiness still requires the app to open its declared
named endpoints and call `ready()`.

## App-owned endpoint contracts

Stream definitions require `type` on executable inputs and outputs. Output
policies are `qos: default` (backend congestion default), `command` (block under
congestion), or `sensor` (drop under congestion), and `retention: volatile`
(default) or `last`. Input policies are `replay: live` (default) or `last`,
and `buffer: fifo` (default) or `{ latest: 4 }`, with a positive depth.
Policies belong to the app definition, never to its invocation.
Congestion control does not guarantee delivery or retransmission. Local and
Link accept only `default`; Zenoh and iceoryx2 also support `command` and
`sensor`. Zenoh's default publishing policy drops under congestion unless
overridden by backend configuration. Iceoryx2 uses its configured publisher
backpressure default (normally retry until buffer space is available).
Local queues sends without a bound; Link reports a full send buffer as an
error. Receive buffering and retained history remain separate from congestion.
The former `reliable` profile spelling is rejected. RPC permits only the
`default` profile; its request/response behavior is not a stream QoS policy.

```yaml
in:
  readings: { type: sensor.Reading, replay: last, buffer: { latest: 4 } }
  calculate: { kind: rpc, type: math.Request, response: math.Response }
out:
  status: { type: robot.Status, qos: default, retention: last }
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
version: 3
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

### Separate schema catalogs

`schema.yaml` is the build-time catalog that `reiny_build::compile()` discovers
by searching upward from the Rust package. Runtime tooling reads only
`main.yaml`, which rejects `schema`. A catalog directory need not be a runtime
module: shared schema libraries and the relay example's Rust packages have
catalogs without executable definitions.

```yaml
# schema.yaml
version: 3
schema:
  shared_types:
    Ping: {proto: proto/ping.proto, message: ping.Ping}
  projects:
    ping:
      types: [Ping]
```

Catalog-relative proto and dependency paths keep their meaning when moving
an existing `schema` block to a sibling `schema.yaml`. Catalogs supply payload
types and fingerprints, never endpoint permissions or runtime connections.

Build-time catalogs also require `version: 3`. Shared schema crates are named
parts under `schema.schema_crates.<name>`, with `crate`, a nonempty `protos` list, and
optional `depends_on` part names. This applies to one-crate catalogs too; the
legacy `schema.schema.crate` shortcut is rejected.

## Namespaces and topics

Each executable instance gets a logical namespace `deployment/module/nested...`.
Message keys carry the complete protobuf `MessageType::TYPE_NAME`, including its
package; package-less names remain valid. Link hashes use that same full name.
Ports and connected module reports compare exact type names and fingerprints.

## Writing a managed executable

`ModuleContext::open` requires the resolved bindings supplied by `apply` / `run`, even
for an executable with no ports. Missing bindings fail before opening the engine.
Schema generation does not authorize publishing, subscribing or RPC. The SDK
exposes only named data endpoints and no underlying engine/session.

```rust,ignore
#[reiny::main]
async fn main(cloudy: ModuleContext) -> reiny::Result<()> {
    let out = cloudy.output::<Relayed>("relayed")?;
    let mut incoming = cloudy.input::<Ping>("ping")?;
    cloudy.ready()?;           // sync; ports can't be created afterwards
    while let Some(ping) = incoming.recv().await { /* ... */ }
    Ok(())
}
```

For a cycle whose first publication is volatile, register
`cloudy.watch_input_ready("port")` before calling `ready()`. The watch replays
readiness only for that input's exact configured source modules.

Dynamic-language applications use `raw_input` / `raw_output` with the declared
port name, actual protobuf type name and mandatory compiled fingerprint. Their
handles participate in readiness just like typed ports.

Engine bridges use complementary declared inputs and outputs on the two
contexts. `bridge::forward(a, b, schemas)` takes the compiled fingerprints,
opens those named routes and registers their live handles before readiness.
Data is published under the output's declared namespace and policy; RPC checks
request/response stamps and addresses replies under that same namespace.
Only control-plane launch/readiness/stop routes retain the original namespaces.

- `input` subscribes only to the exact source namespaces that the deployment wired.
- `output` publishes in the module's own namespace. Two outputs with the same
  transport type in one executable are rejected.
- `ready()` publishes `@ready`, which is separate from the `@launch` presence
  token. `apply` waits for it.
- A `@stop` request is acknowledged on receipt. The ack means "stop
  requested", not "process exited".
- Module communication needs an engine with query, liveliness and attachment support.
  `LinkEngine` preserves fingerprints for bound SDK contexts. Static Link
  applications use their own YAML-generated named port tables.
- A namespaced ID is capped at 32 bytes on a link and an iceoryx2 key at 224
  bytes. Longer values fail; they're never truncated.

`ModuleContext::open` requires resolved `ModuleBindings`; `apply` and `run` supply them
to executable modules. Opening an unbound SDK context is rejected. Applications
use only named `input`, `output`, `rpc_client`, and `rpc_server` for communication.
There are no type-only publish/subscribe/RPC APIs or engine/session escape hatches.
Do not substitute a schema catalog for an executable definition and explicit
composition wiring.

### Named RPC and owned processes

`cloudy.rpc_client::<Request>("calculate")?` opens a named RPC input using its single
configured server. `cloudy.rpc_server::<Request>("calculate")?` implements an RPC
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
publication and RPC server with a wired verification task.

## CLI lifecycle

| Command | Effect |
| --- | --- |
| `reiny capabilities --json` | Report the exact CLI version and supported preparation, prepared-file, apply, builder and controlled-apply protocols. |
| `reiny prepare <path> --out <new-file> [--events jsonl] [--control-stdin] [--bin-dir <dir>] [--release] [--build-cache <dir>] [--jobs <n>]` | Resolve and freeze a deployment without starting applications. `--bin-dir` applies only to prebuilt modules; `--release` overrides Cargo profiles only. |
| `reiny plan [path] [--json]` | Resolve sources and show modules and connections. Builds nothing. |
| `reiny apply [path] [--detach]` | Prepare artifacts (declared builds, staged files), start or reconcile managed processes, wait for explicit readiness and compare reported port schemas. |
| `reiny apply --prepared <file> [--detach] [--ready-timeout <seconds>] [--events jsonl] [--control-stdin]` | Verify and apply frozen artifacts without resolving sources or running builders. This form is mutually exclusive with a source path and build options. |
| `reiny run [path]` | Same contract as `apply`, in the foreground. Ctrl+C stops the deployment. |
| `reiny status [path] [--json]` | Read state from the live owner. Retained state may show `owner_alive: false`. |
| `reiny stop [path] [--json]` | Ask modules to stop cooperatively, then reap. Remaining owned process trees are killed. |
| `reiny update [path] [--json]` | Move Git refs and rewrite `lock.yaml`. |
| `reiny bundle [path] [--out dist]` | Build and bundle a self-contained deployment. |
| `reiny new` / `init` / `add` | Scaffold a leaf module, or add a local schema dependency. |
| `reiny check [path]` | Validate composition or describe the schema without compiling. |
| `reiny build [--release]` | Prepare declared build artifacts without starting processes. |

`reiny check main.yaml` checks only the runtime definition. Deployment roots
are fully resolved; reusable app definitions are checked without requiring
their caller's providers, sources, or executable files. Instance-dependent
checks still run when the deployment is resolved. `reiny check schema.yaml`
checks only the type catalog. Directory checks retain nearest-manifest
discovery and describe an available ancestor catalog after checking runtime
definitions.

`reiny plan --json` includes resolved `connections`, with named executable
`source` and `destination` addresses, intermediate alias addresses in `via`,
and the source endpoint's concrete `contract`. `via` runs from destination
toward source, excluding both endpoints. The human-readable plan uses the
same graph to show named sources and alias paths. These records describe the
existing wiring; they do not create additional communication grants.

Source `apply` and `run` share `--bin-dir`, `--release`, `--build-cache`,
`--jobs`, `--log-level` and `--ready-timeout` (seconds, default 30). Prepared
apply instead accepts only `--prepared`, `--detach`, `--ready-timeout`,
`--events`, and `--control-stdin`; `--control-stdin` conflicts with `--detach`.
A bare `reiny <path>` or `reiny --config <path>` is
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
