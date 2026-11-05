# Explicit native process steps

`ProcessStepOwner` runs one synchronous graph across explicitly supplied
`ProcessHost` connections. It uses the same scheduler, logical clock and
admission/commit gate as Local execution. Each host executes its assigned
callbacks; hosts do not run independent subgraph schedulers.

This is a Rust API for applications that already own their processes and
authenticated bootstrap connections. It is not an additional manifest execution
mode, a deployment-control client socket, or an SSH process owner. Ordinary
stream/RPC bindings remain independently declared and validated.

## Preparation and admission

1. Register immutable schemas with `ProcessRegistry::register_step`. Use
   `register_step_prepared` only when specific output ports may supply initial
   state from the actual prepared object.
2. Construct a `ProcessStepPlan` with explicit External, SameTick and PreviousTick
   routes. A delayed initial value is either explicit data or an authorized
   prepared output; it is never a fabricated zero or a second factory instance.
3. Supply each `ProcessStepPeer` with its exact assignments, bootstrap identity,
   one-use nonce and connected stream. Keep the corresponding process handles.
4. Call `ProcessStepOwner::prepare` with the initial logical time, one startup
   budget and an application validator of actual reports and the resolved graph.

Every peer negotiates step capability before any receives Prepare. The owner
checks ordinary reports, immutable step schemas, actual prepared contracts and
seeds, then binds the finalized graph and initial clock by digest. Every host
acknowledges Bind, Commit and Running before client admission opens.

Schemas include explicit semantic assertion maps. Their equality verifies
declarations, not the physical units or truth of application payloads. Preparation
does not grant ordinary ports from a step schema.

## Execution and retained results

`ProcessStepClient::try_step` allocates a request ID. `start` accepts an explicit
session-local ID. An exact retained duplicate returns the original run; a
conflicting or released ID never dispatches again. Clones share one admission
identity. At most one invocation is in flight globally.

Dropping a waiter or releasing a retained record does not cancel its work.
Cancellation, deadline, stop and logical-clock commit meet at the existing gate.
A failure after dispatch faults the epoch and retains its attempted/completed
prefix. Callback errors, panics, invalid outputs and worker loss remain distinct.
No failure implies rollback of callback side effects.

Results must match the complete outstanding session, epoch, run, tick, ordinal
and instance. A partial or interrupted control frame closes that connection;
the owner does not replay it or resume the epoch.

The protocol bounds each frame to 8 MiB, each protobuf value to 1 MiB, nodes and
ports per node to 1,024, accepted retained requests to 64, and aggregate retained
protobuf snapshots to 64 MiB. External inputs reserve envelope space before
admission. An oversized callback output fails after dispatch. There is no bulk
transfer or chunking fallback.

Preparation charges caller-supplied literal seeds before collecting reports,
then charges every authorized prepared output, even when no route selects it.
Resolving a prepared output into a bootstrap seed also consumes that shared
budget while both snapshots coexist. The original reports are released after
application validation. These payload limits are not an RSS limit; schema
metadata and temporary control-frame encoding have separate bounds.

## Stop and ownership

`ProcessStepOwner::stop` revokes admission, joins its coordinator and returns
retained host cleanup observations. An absent observation is Unknown. Native
callbacks may ignore cancellation; an Unresponsive host must terminate.

A host outcome or channel EOF does not prove that an OS process tree was reaped.
The application must separately join its local children or preserve remote
uncertainty and fencing. Prepared objects, configuration origins and factory
assets remain under the existing staged host lifetime through cleanup.

## Runnable Linux example

```sh
cargo run --locked -p reiny --no-default-features --example process_steps
cargo run --locked -p reiny --no-default-features --example process_steps -- --cancel-blocked
cargo run --locked -p reiny --no-default-features --example process_steps -- --kill-blocked
```

The normal example places A/C in one process and B in another, compares complete
outputs and logical intervals with Local execution, and uses C's actual prepared
seed. The fault examples wait for a native mutation event before cancelling or
killing B. They retain the failure prefix and old clock, then separately reap
their direct children. They do not create descendants or prove remote-tree
containment, WAN timing, or arbitrary solver equivalence.
