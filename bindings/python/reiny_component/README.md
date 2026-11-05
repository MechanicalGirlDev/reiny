# Explicit Python service components

`reiny_component` is the handwritten executable-component adapter for the
additive `reiny_component.h` ABI v1. The generated `reiny_ffi` UniFFI session
bindings remain separate and unchanged. This package uses only Python's
standard library; put `bindings/python` on `PYTHONPATH`.

Load the exact matching native build explicitly:

```python
from reiny_component import Library, Component

native = Library("/absolute/path/to/libreiny_ffi.so")

class Worker(Component):
    def prepare(self, context):
        self.identity = context.info

    def run(self, context):
        context.wait_stop(60_000_000_000)

with native.registry() as registry:
    registry.register("worker", "application-v1", lambda context: Worker())
    registry.seal()
    with native.graph({
        "version": 1, "domain": "example",
        "startup_timeout_ns": 5_000_000_000,
        "stop_timeout_ns": 5_000_000_000,
        "instances": [{
            "factory_id": "worker",
            "bindings": {
                "version": 3, "namespace": "worker",
                "endpoint_namespace": "worker", "inputs": {}, "outputs": {}
            }
        }]
    }) as graph:
        with registry.launch_local(graph) as host:
            host.wait_running(5_000_000_000)
            host.request_stop()
            outcome = host.wait_terminal(5_000_000_000)
            host.wait_joined(5_000_000_000)
native.collect_factories()
```

All waits take **positive integer nanoseconds**, not seconds. Wait timeout raises
`DeadlineError` and cancels only that wait. The example's service wait also has a
deadline: an application should choose its own service budget, and must not let
`run` return before shutdown. No arbitrary Python thread can be killed safely.

`Context.info` copies instance ID, domain, config and captured config origin.
In `prepare`, open only declared named endpoints with `input`, `output`,
`rpc_client`, `rpc_server`, and optionally `watch_input_ready`. Each schema is an
owned `native.schema(protobuf_name, fingerprint, fingerprint_version=2)` handle.
Derive fingerprints from Reiny's descriptor tooling; the adapter does not hash
messages or infer authority from a schema. Payloads are copied opaque protobuf
`bytes`; matching fingerprints do not prove valid decoding.

`Publisher.send`, `Subscription.receive`/`stats`, `Message.payload`/`source`/
`source_port`/`info`, `RpcClient.call_start`, `Operation.wait`, and
`RpcServer.receive`/`Request.payload`/`reply`/`reply_error` invoke native endpoints.
Messages, requests, operations, schemas, endpoints, observers, contexts, graphs
and hosts own handles and support `with`/`close`. `clone()` creates another claim;
Python copying is rejected. A request's first reply wins; a second raises
`AlreadyCompletedError`. Closing its last unanswered claim produces NoReply.
RPC operation results are retained for repeated waits and cloned observers.
Closing the last operation abandons observation, **not server execution**.
Never retry a mutating call automatically after a deadline.

`Status` preserves every ABI status; `ReinyError.status` is machine-readable.
Deadline, closed, remote application error, no reply, schema and already-completed
statuses have distinct exception subclasses. Host terminal waits return a copied
document even for FAILED or UNRESPONSIVE: inspect `phase`, `primary`, `cleanup`
and `unjoined`. Native validation currently groups several graph/port failures
under FAILURE; Python does not classify them by parsing diagnostic prose.

## Threading, shutdown and ownership

Native create/prepare/run/stop/destroy callbacks are serialized on one OS thread
per instance, while independent factories can run concurrently. `ctypes.CDLL`
releases the GIL during native waits and `CFUNCTYPE` acquires it for callbacks.
Do not depend on `threading.local` surviving distinct foreign callbacks.
Factory callables must synchronize any shared mutable application state.
All `BaseException`s in lifecycle callbacks become copied native callback errors.
`destroy` cannot return an error through this ABI: its exceptions are retained in
the diagnostics list returned by `Registry.register`.

Callback context objects are borrowed and expire at **each callback return**.
Store `context.clone()` if context metadata/shutdown observation must outlive the
callback; explicitly close that clone afterward. Release retained endpoints in
`stop` or `destroy`. Prepare may declare endpoints but cannot send or dispatch
RPC. Stopping closes new admission; an effect already admitted can still finish.
Synchronous send/reply backpressure has no hard execution deadline.

Subscribe to host updates before the triggering action. Updates can coalesce.
`request_stop` is a request, `wait_terminal` is an outcome, and `wait_joined`
proves actual callback teardown. Stop never runs concurrently with an active
callback. UNRESPONSIVE may be reported while that callback is still running;
later `wait_joined` can succeed after the application releases its gate.
Callbacks and their factory data are globally pinned through native factory
release, including after registry/host handles are closed or collected.
Registration uses `register_notified` with an owned atomic release token and
**NULL `release_factory`**: the last native owner signals only native atomic
state, with no foreign callback, GIL acquisition or application cleanup.
After joining hosts and closing registries, call `native.collect_factories()`
to query those tokens and remove released callback/factory roots on the caller's
thread. Unreleased registrations remain pinned; omitting collection intentionally
retains them until interpreter exit. The native CDLL itself remains pinned for
**process lifetime**, because its shared runtime is process-scoped.

ctypes lifecycle callbacks acquire the GIL and **may block under arbitrary
Python thread contention**; they are not nonblocking callbacks. The wrapper
does not install any Python factory-finalizer callback, so that limitation is
absent from the native last-owner path. Keep callback roots through release-token
confirmation; do not use collection as a substitute for lifecycle observations.

Closing a host or leaving its context manager releases its claim and requests
stop where the native owner requires it; **it does not wait for join**. Explicitly
wait for join before releasing the last host observation claim. Never finalize
the Python interpreter or unload callback code with an unjoined host. User-created
detached threads are not tracked. Concurrent `close` can race an operation before
native lookup (invalid handle) or after native admission (operation may finish).
The adapter never holds a Python handle lock across native waits/callbacks.

`Registry.process_host_run(startup_ns, stop_ns, frame_ns, max_instances)` reads
the private inherited bootstrap and invokes the native revisioned process host.
It is executable entry, not reusable embedding or process spawn/reap ownership.
The executable **must exit after it returns**, including error/unresponsive
outcomes. Python does not expose bootstrap secrets or global engine pointers.

## Scope and checks

Only explicitly registered trusted **service** components, Local host graphs,
named raw streams/RPC, readiness, lifecycle and the app-owned process-host entry
are exposed. There are no step/job methods, distributed scheduling, deployment
owner, remote-machine spawn/reap, sandbox, automatic plugin loading, protobuf
codec generation or cross-language zero-copy claims.

Run actual native integration (no dependency installation):

```sh
PYTHONPATH=bindings/python \
REINY_COMPONENT_LIBRARY=/absolute/path/to/libreiny_ffi.so \
python3 -m unittest discover -s bindings/python -p test_component.py -v
```

The native artifact is mandatory; missing it fails rather than skipping tests.
Tests use events and real native completion deadlines, never fixed sleeps.
They cover local service execution, binary/empty streams and provenance,
RPC success/remote error/no reply, one-shot reply gates, owned clones,
registration failure, callback exceptions and unresponsive-then-joined cleanup.
Rust/Python codec interoperability and production deployment-owner integration
still require their separate owner-driven language matrix.

### Actual Python executable process acceptance

`component_process_entry.py` is an app-owned executable entry using
`Registry.process_host_run`, not an embedded interpreter or alternate runtime.
`test_component_process.py` is a Linux test-only protocol owner, based on the
real native host-control DTOs. It writes an actual framed private Bootstrap to
each child's inherited stdin, authenticates exact nonce/revision/compatibility,
asserts exact compiled fixture reports, collects every Commit acknowledgement
before Run, and explicitly waits/reaps each owned child.

```sh
PYTHONPATH=bindings/python \
REINY_COMPONENT_LIBRARY=/absolute/path/to/frozen/libreiny_ffi.so \
python3 -m unittest discover -s bindings/python -p test_component_process.py -v
```

These tests require the hash-pinned frozen native library and matching header.
They verify each live child's mapped library through `/proc/<pid>/maps` and
retain per-child stdout, stderr, events, outcomes and reap evidence in test
output. Two distinct actual Python host processes exchange binary/empty streams
with exact provenance and RPC success, remote application error and NoReply over
native Zenoh TCP. The server listens on a kernel-selected port; the fixture
discovers its actual owned listener through Linux `/proc`, avoiding a
reserve-close-bind free-port race. Readiness and a real RPC gate synchronize
delivery; there are no discovery sleeps.

Other cases reject Run before Commit without running callbacks, propagate a
Python prepare exception through the native structured outcome, and exit/reap
an UNRESPONSIVE host without falsely claiming stop/destroy callbacks completed.
The executable uses immediate `os._exit` after the native entry returns, avoiding
Python interpreter finalization alongside a blocked foreign callback.

This test-only owner is not a deployment supervisor or public graph validator.
Exact fixed-fixture report/edge assertions do not replace Rust `validate_graph`
for arbitrary application graphs. Direct child reaping is verified; descendant
process-tree containment, production owner reconciliation and mixed-language
protobuf codec interoperability are not claimed.
