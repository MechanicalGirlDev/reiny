# Modules and `main.yaml`

This is the normative guide for describing and running a reiny deployment.
A module definition is reusable; an instance is one invocation of it. Each
executable instance is a graph node whose process belongs to a runtime generation.
Directory inputs and child module sources use `main.yaml`. A root can instead
be selected by an explicit filename, such as `runtime/simulation/deployment.yaml`;
the loader reads that exact file, even beside a different `main.yaml`. The same
file format covers three roles:

- a **root**, which names the deployment and configures providers,
- a **composite module**, which calls child modules and wires their ports,
- a **leaf module**, which declares one native executable with `run`, or a
  registered hosted component with `component` in version 4.

Relative paths and source acquisition state (`lock.yaml` and `.reiny`) stay
anchored to the selected manifest's directory. `plan`, `check`, `prepare`,
`apply`, `run`, `update`, and `bundle` accept explicit runtime files; schema
catalogs remain separate. Bundles write their resolved root as `main.yaml`.
`DeploymentPlan.root` and `module_sources` remain directory paths;
`root_manifest` and `manifest_path(namespace)` preserve the selected declaration.

A machine-readable description of the fields lives in
[`main.schema.yaml`](main.schema.yaml) (JSON Schema written as YAML). The Rust
structs in `crates/reiny-launch/src/modules.rs`, `acquire.rs` and
`artifacts.rs` are the source of truth if the two ever disagree.

Older design records under [`design/`](design) describe `Reiny.toml` and
`launch.yaml` launch configs. Those formats are gone; read those files as
history only.

## Application-owned endpoint semantics

Executable definitions may add `semantics` to an input or output. It is a map
of nonblank strings to nonblank strings, not discovery metadata:

```yaml
out:
  observation:
    type: example.Observation
    semantics:
      example.units: example.si.v1
      example.identity: example.target-a.v1
      example.ordering: example.xy.v1
      example.time: example.monotonic-observation.v1
      example.update: example.every-step.v1
```

The application owns the meaning and immutable revision of every assertion.
These keys are examples, not core vocabulary. A value may identify an
application specification covering per-field units, a concrete target, ordered
elements, clock/epoch/timestamp meaning, or freshness and update requirements.
Change the value when that meaning changes. RPC assertions describe the complete
request/response interaction; applications can use distinct request/response keys.
No implicit unit conversion, clock conversion, reordering or compatibility
negotiation occurs. An explicit application component must implement conversions
and declare the respective input and output contracts.

The resolver compares assertions on declared typed boundaries. Compiled graph
admission compares the entire map on every exact input-source edge, including
each fan-in member. A same-type, same-semantics alternative endpoint is never
selected instead. Caller connections and composite output aliases cannot
replace assertions. Composite input declarations must agree with their source;
output aliases preserve the original executable contract.

Opened-port reports retain these declarations alongside independently compiled
protobuf names and fingerprints. Admission rejects a report that erases or
changes its binding's assertions. This proves agreement of declarations, not
application correctness: the SDK does not inspect payload units, target IDs,
array order, timestamps, freshness or actual update frequency. Authors remain
responsible for those behaviors, and for choosing truthful contract revisions.
Existing transport policies and step execution rules remain independent.

This is an additive field in manifest versions 3/4 and bindings/report version 3,
not a change to protobuf fingerprints or wire keys. An absent or empty map means
unspecified and is compatible only with another empty map; one-sided semantics
are rejected. Empty maps are omitted when serialized, preserving legacy files.
Older binaries' strict endpoint parsers reject nonempty `semantics` as an unknown
field. Upgrade every owner and endpoint consuming such contracts together; do
not strip the field for mixed-version operation. Rust callers constructing a
complete struct literal must add the field or use the existing default.

The static `compile_ports`/Link port-table format does not support this field and
rejects it rather than claiming semantic admission. This contract applies to
resolved module bindings and their compiled-report graph admission.

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

Legacy manifests use `version: 3`; `version: 4` also supports explicit hosted
component planning and execution. A version 4 root may reuse version 3 modules. Version 3
rejects `component`, `component_hosts` and `placement`. Only the root sets `deployment`, `domain` and
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

Every runtime manifest must declare exactly one of `run`, `component` (version 4),
or a nonempty `modules` mapping. `build` at the module top level is only
allowed next to `run`. Without `build`, the executable must already exist in
the provider's `bin_dir`.

### Hosted component plans (version 4)

A component leaf names a factory registration and its application-defined revision:

```yaml
version: 4
component:
  factory: sensor
  factory_revision: "1"
  config: sensor.yaml
  config_assets: [calibration.bin]
out:
  state: {type: robot.State, retention: last}
```

The root supplies process hosts and assigns every full instance namespace:

```yaml
version: 4
deployment: robot
providers:
  local:
    type: process
    bin_dir: target/debug
    connect: ["tcp/127.0.0.1:7447"]
component_hosts:
  sensors: {provider: local, bin: robot-component-host, on_failure: report}
placement:
  robot/sensor: sensors
modules:
  sensor: {source: sensor}
```

`component_hosts` and `placement` belong only to the deployment root. Each host
uses a root `process` provider, a binary name, optional `build` (with the same
builder schema, paths relative to the root), `restart: manual`, and
`on_failure: report` or `suspend_deployment`. Planning neither runs a builder
nor requires its executable outputs to exist.

Placement must contain exactly the instantiated component namespaces, not
definition paths or composite namespaces. Missing, duplicate, unknown and empty
assignments fail, as do unused hosts and unknown or non-process providers.
Factory identifiers must be valid names and factory revisions must be nonempty.
The staged owner checks registration availability and revision agreement against
each host's authenticated compiled report before Run. Planning does not execute
factories.

Components cannot declare `run`, a module-level `build`, `owned_children`,
executable companions or argv. Calls may replace `config` and `config_assets`
using the existing caller-relative and selected-config-origin rules, but may
not supply `args`, even `[]`. Endpoint policies remain definition-owned.
Resolved bindings retain their independent version and their original exact
source namespace/port pairs; placement grants no executable or child authority.

`reiny plan --json` exposes separate `components` and
`component_hosts` collections. Legacy processes remain in `nodes`.
The human-readable CLI renderer lists hosts, factory revisions and placements.
Mixing hosted components with legacy `run` processes,
automatic hosted restart, rolling replacement, and in-process hosts is unsupported.

Hosted preparation freezes host executables, component settings, declared config
assets and exact placements in prepared-file version 2. Execution starts actual
SDK ProcessHosts through private inherited bootstrap channels. The owner validates
every compiled report and completes the Prepare/Commit barrier before sending
Run. Identical re-apply retains live host PIDs and generations; changes require
manual whole-graph recovery. Status retains host exits and component failures.
Cooperative stop waits for owned hosts to exit and be reaped; a dead owner's
retained status is not a postmortem cleanup receipt.

Legacy-only v3/v4 preparation and prepared-file version 1 remain supported.
Hosted bundles are unsupported and fail explicitly. Hosted execution sends the
protocol's `routed_zenoh` transport variant with the complete verified
namespace-to-host placement map and explicit Zenoh settings. Each ProcessHost
checks that the map assigns exactly its received instances to its authenticated
bootstrap host ID before constructing components. Placement selects Local for
same-host reads and Zenoh for remote reads; the original named bindings still
grant all endpoint authority. Unknown namespaces fail rather than falling back.
Map size is bounded by the host's instance limit and the protocol frame limit.

Owned publications remain mirrored non-atomically to Local and Zenoh, including
when all declared consumers are local. Shared native values retain their Arc
identity on local delivery, but the mirrored network publication still encodes.
RoutedZenoh does **not** provide zero-serialization hosted publication. A failed publish
may already have reached one transport and must not be retried as a transaction.
Control presence, retained queries and RPC responders remain visible on both
transports; current liveliness is never used to infer absence of remote readers.

Manual ProcessHost owners may still use the original `zenoh` variant. The
control envelope remains version 1: this is not capability negotiation.
Older host binaries reject the unknown `routed_zenoh` variant while decoding
Prepare, before construction or commit. The owner aborts admission and cleans
up the graph; it never falls back to pure Zenoh. Rebuild old host binaries to
use public hosted execution. Unsupported transport configuration fields fail
rather than being silently discarded. This path does not provide remote-machine
supervision or distributed synchronous step execution.

Applications that already own authenticated process connections can use the
separate [native process-step API](process-steps.md). That explicit owner does
not add step execution to manifests or provide remote process supervision.

### Explicit Local-only hosts

Set `component_hosts.<name>.transport: local` to opt into one isolated Local
data bus. Omitting `transport` retains `routed_zenoh` and its mirrored network
publication behavior. This is a host setting, not a new provider:

```yaml
providers:
  native: {type: process, bin_dir: target/debug}
component_hosts:
  control: {provider: native, bin: control-host, transport: local}
```

Every input edge touching a Local-only host must have its source and destination
on that same host, including stream fan-in and RPC. Planning and prepared-file
verification reject crossing edges before any host is spawned. Local hosts also
reject provider `connect` endpoints or `zenoh_config`; network intent is never
silently ignored. The host independently validates the bounded placement map,
its exact assigned namespaces against its bootstrap identity, and that its own
input sources are local before constructing components.

Local-only data, presence and RPC endpoints are deliberately **not visible to
network bus tools or other hosts**. Owner control uses its existing private
channel. This explicit isolation is not automatic route thinning. Local-only
ProcessHosts work without the SDK's `zenoh` feature and never open a Zenoh
session. Older host binaries reject the protocol's `local` variant; no fallback
or capability negotiation is attempted.

Within such a host, matching native SDK `send_shared` / `recv_shared` types can
retain the same immutable Arc without protobuf serialization. Owned sends,
byte readers, or incompatible Rust representations still require encoding.
The public process fixture proves the shared two-component path with both Arc
identity and an instrumented zero encoding count; the identical default-routed
fixture encodes its two publications as a positive control.

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

Without that inventory, PE executables (Windows) include their local import
dependency closure, including delay-loaded imports. Sibling runtime libraries
and Cargo build-script linked runtime libraries remain roots, including plugins
loaded dynamically; their imports are followed too. Cargo `deps` is a lookup
directory, not an inventory: unrelated compiler proc-macro DLLs are not bundled.
Resolution uses these local files only, never the host's `PATH` or Windows system
directories. Libraries supplied only by the target environment remain its
responsibility. Dynamically loaded libraries kept only in `deps` must instead be
placed beside the executable or declared through the bundle/command inventory.
Malformed PE imports fail preparation. Non-PE inputs retain the existing sibling
and `deps` runtime-library discovery.

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
