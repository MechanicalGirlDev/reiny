# Additive Python Local managed steps

`reiny_component.steps` wraps the actual `reiny_component_step.h` ABI. It uses
the native Local scheduler and the service wrapper's library, handle store,
callback exception boundary and factory ownership roots. It is not another
scheduler or a stream-based simulation of steps.

The extension is opt-in:

```python
from reiny_component import Library
from reiny_component.steps import LocalSteps

native = Library("/absolute/path/to/notified-step/libreiny_ffi.so")
api = LocalSteps(native)
```

Importing `reiny_component.steps` resolves no native step symbols. The existing
service `Library` and service/process APIs remain unchanged. Constructing
`LocalSteps` against the old service-only library raises `StepsUnavailableError`
with service status UNSUPPORTED, rather than failing service imports. The step
library must export **notified** registration; the adapter never falls back to a
Python `release_factory` callback.

## Minimal actual step execution

This fixture uses opaque protobuf bytes for a tiny `Number { uint32 value = 1; }`
codec. Fingerprint 42 is a test identity, not an algorithm for application
fingerprints: production applications must use Reiny's matching descriptor
tooling and generated codecs.

```python
from reiny_component.steps import Duration, StepComponent

class Increment(StepComponent):
    def step(self, context, tick, inputs, outputs):
        with inputs.get("x") as value:
            payload = value.encoded(number_type)
        if len(payload) != 2 or payload[0] != 8 or payload[1] >= 127:
            raise ValueError("invalid fixture Number payload")
        with api.value(number_type, bytes([8, payload[1] + 1])) as result:
            outputs.set("out", result)

with native.schema("test.Number", 42) as number_type:
    with api.schema({"x": number_type}, {"out": number_type}) as node_schema:
        with native.registry() as registry:
            diagnostics = api.register(
                registry, "increment", "application-v1", node_schema,
                lambda context: Increment())
            registry.seal()
            with native.graph({
                "version": 1, "domain": "python-steps",
                "startup_timeout_ns": 5_000_000_000,
                "stop_timeout_ns": 5_000_000_000,
                "instances": [{
                    "factory_id": "increment",
                    "bindings": {
                        "version": 3, "namespace": "node",
                        "endpoint_namespace": "node", "inputs": {}, "outputs": {}
                    }
                }]
            }) as components, api.graph({"node": node_schema}, [
                {"instance": "node", "port": "x", "kind": "external"}
            ]) as graph:
                host, scheduler = api.launch_local(
                    registry, components, graph, Duration(20_000_000_000))
                with host, scheduler:
                    host.wait_running(5_000_000_000)
                    with api.value(number_type, b"\x08\x01") as input_value:
                        with api.request(Duration(nanos=7), [
                            ("node", "x", input_value)
                        ]) as request:
                            with scheduler.try_step(request, 5_000_000_000) as run:
                                with run.wait(5_000_000_000) as result:
                                    with result.output("node", "out") as output:
                                        assert output.encoded(number_type) == b"\x08\x02"
                    host.request_stop()
                    outcome = host.wait_terminal(5_000_000_000)
                    host.wait_joined(5_000_000_000)
            registry.close()
            native.collect_factories()
            assert diagnostics == []
```

In application code, inspect `result.kind` before accessing committed outputs
and inspect host terminal `phase`, primary and cleanup separately. A successful
wait is an observation, not a claim that the step committed.

## Values, schemas, routes and clocks

`api.schema(inputs, outputs)` copies encoded type identities from owned
`native.schema` handles. `api.value(schema, bytes)` constructs an immutable
opaque encoded snapshot; `Value.encoded(schema)` performs exact schema equality
before copying. It does not validate protobuf contents or domain meaning.

`api.graph(nodes, routes)` uses the header's route shape, with Python handles
instead of numeric IDs:

```python
routes = [
    {"instance": "a", "port": "x", "kind": "external"},
    {"instance": "b", "port": "x", "kind": "same_tick",
     "source": {"instance": "a", "port": "out"}},
    {"instance": "a", "port": "previous", "kind": "previous_tick",
     "source": {"instance": "b", "port": "out"}, "initial": initial_value},
]
```

Every input has exactly one route. Same-tick cycles, schema mismatches and
invalid assignments fail native graph validation synchronously at launch, before
any component is created. Previous-tick routes require a correctly typed seed.
The separate service graph supplies per-instance bindings, config/origin and
lifecycle budgets; step routes do not grant stream/RPC authority.

`api.request(dt, [(instance, port, value), ...])` copies external input values.
Imports own copies, so source handles can be closed after import. Callback code
that calls `Value.encoded` still needs its own live schema handle for the exact
identity check. `import_schema`, `import_graph` and `import_request` expose the
full versioned JSON forms; mappings accept owned handles, and raw `bytes` reach
native duplicate/unknown-field validation unchanged.

Logical `Duration(secs, nanos)` preserves the native u64-seconds range, including
times above accumulated-u64-nanoseconds range. Nanos must be below one billion;
normalization is not guessed. Zero logical dt is rejected by native request
admission. Logical time is not a wall-clock execution budget.

`scheduler.clock` copies `Clock(epoch, next_index, time, phase)`. `run.id` copies
`RunId(epoch, sequence, tick_index)`; epoch zero is valid. Sequence is an
acceptance identity, not the logical tick index. A committed `result.tick`
contains the exact start/dt/end and canonical tick identity.

## Admission, results, cancellation and failure

`scheduler.try_step(request, timeout_ns)` is synchronous native admission.
`StepError`, which is also a service `ReinyError`, preserves a separate
`StepStatus`: NOT_RUNNING, BUSY, INVALID_REQUEST, INVALID_TIMEOUT, ID_EXHAUSTED,
FAULTED, STOPPED and SUPERVISOR_GONE. Each has a distinct exception subclass.
The zero execution budget is intentionally passed to native admission and
raises `InvalidTimeoutError`; invalid Python integers fail before ctypes.

`run.wait(positive_timeout_ns)` returns an owned retained `Result`, including
failed attempts. A wait deadline raises the existing `DeadlineError` and does
not cancel work. Repeated waits and cloned run/result observers preserve the
same outcome. Dropping observation handles does not cancel execution.

`result.kind` is `ResultKind`: COMMITTED, CANCELLED_BEFORE_DISPATCH,
TIMED_OUT_BEFORE_DISPATCH, FAILED_AFTER_DISPATCH or SUPERVISOR_GONE.
`result.info` copies the native structured document. `result.failure` returns
a copied `Failure` with typed `FailureKind`, instance/port, attempted/completed
tuples and diagnostic detail. Tick/output getters require a committed result;
they fail with service INVALID_STATE on failed attempts.

`run.cancel()` returns `CancelStatus`: REQUESTED, ALREADY_REQUESTED, FINISHED
or SUPERVISOR_GONE. It acknowledges the request, not callback termination.
Pre-dispatch cancellation can leave the tick uncommitted; after dispatch,
cancellation/deadline/host stop fault the attempt/epoch according to the native
contract. Successful receipt commit can win a cancellation race. No effects
are rolled back, and mutating work must not be retried automatically.

`host.request_stop()` is asynchronous; return is not an admission barrier.
`wait_terminal` can report UNRESPONSIVE while Python code or destruction is
still running. Only `host.wait_joined` proves actual callback/adapter teardown.
The scheduler is an observation/admission capability and does not keep the host
running or own its lifecycle. Neither cancelled results nor terminal failure
authorize releasing active callback code.

## Callback and factory lifetime

Factory create/prepare/step/stop/destroy hooks run on the same serialized
ordinary OS lane for each instance. Independent instances can run concurrently.
ctypes callback entry acquires the GIL and can block under arbitrary Python
thread contention; these lifecycle callbacks are not promised nonblocking.
All application `BaseException`s, including KeyboardInterrupt, are converted
through the shared copied native error sink and cannot unwind into C.

The tick is a copied immutable Python dataclass. Context, `Inputs` and `Outputs`
are borrowed and the wrapper expires them at callback return. Clone a context
or immutable `Inputs` handle explicitly if retaining it. `inputs.get` returns
an independently owned `Value`. `outputs.set` retains an immutable value copy;
output sinks cannot be cloned or written from another thread. Every declared
output must be inserted exactly once; duplicate insertion and stale sink use
are errors, and the real scheduler validates the final output set/schema.

Step registration calls `reiny_step_register_notified` with NULL
`release_factory` and a caller-owned existing native release token. Failed
registration transfers no factory claim and leaves token binding unchanged;
the adapter releases its unused token. Successful registration pins callback
trampolines, factory captures and instance state in the **same roots used by
the service wrapper**. No GIL callback runs at last-owner finalization.

Release registry/host claims only according to their ownership rules. After
actual host join and owner release, `native.collect_factories()` queries native
tokens, closes released token claims and removes released Python roots on the
caller thread. Roots remain pinned while a step callback or destructor is
blocked. Explicit close/clone semantics match the service wrapper; finalizers
are fallback releases, never join certificates. The native CDLL remains loaded
for process lifetime. Untracked application threads are outside SDK quiescence.

## Real acceptance

The new test module leaves all existing service/process behavioral assertions
unchanged:

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=bindings/python \
REINY_STEP_LIBRARY=/absolute/path/to/frozen-notified-step/libreiny_ffi.so \
REINY_COMPONENT_SERVICE_LIBRARY=/absolute/path/to/original-service/libreiny_ffi.so \
python3 -m unittest discover -s bindings/python -p test_component_step.py -v
```

Artifact/header hashes are mandatory assertions. Tests execute real native
Local scheduling: three ticks with external/same/previous-tick routes, exact
clock advancement from 20,000,000,000 seconds, schema/value/import errors,
native Busy/invalid admission, repeated immutable results, callback exceptions,
wrong output schemas and structured attempted/completed progress. Event-gated
cancellation/deadline/stop tests distinguish UNRESPONSIVE from actual join;
an independent destructor gate verifies callback return is not enough.
Weak references and native tokens verify roots survive both gates and become
collectible only after joined teardown and release. There are no scheduler
mocks, fixed sleeps or skipped tests.

The extension supports **Local managed steps only**. No process/distributed
steps, jobs, deployment owner, logical-clock distribution, rollback, zero-copy
or forcible Python callback termination is provided.
