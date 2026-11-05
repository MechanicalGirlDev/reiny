# C++ Local managed steps

`../include/reiny_component_step.hpp` adds the C++17 adapter in
`reiny::component::step`. It includes, but does not modify, the existing service
adapter `reiny_component.hpp`. Both consume the same native handle space.
The project style follows `AGENTS.md` and the existing C++ headers: header-only
classes, move-only owned claims, explicit registration, and exceptions for ABI
operation errors.
Install or copy the complete `include/` directory, including
`reiny_component_step_values.hpp` and the service header helpers.

## Public surface

- `component::Schema` identifies an encoded protobuf payload. Step `Schema`
  describes named input/output maps; step `Graph` describes explicit external,
  same-tick, and previous-tick routes; `Request` describes one tick's external
  values and `dt`. Their `import` methods take the strict JSON forms documented
  by `reiny_component_step.h`. All referenced native handles must be live
  through import. Values and schema definitions are copied synchronously;
  imports do not retain a borrowed C++ object or JSON string.
- `Value::create(payload_schema, bytes)` copies an encoded snapshot.
  `value.bytes(payload_schema)` checks exact schema identity before copying.
  These operations do not decode protobuf or validate application values.
  Schema/graph/request/value handles are immutable, move-only, and explicitly
  cloneable into independent ownership claims.
- Derive from step `Component` and implement
  `step(ContextView, const Tick&, InputsView, OutputsView)`. `prepare` and `stop`
  are optional overrides. Register a factory returning `unique_ptr<Component>`
  using `register_factory(registry, name, revision, step_schema, factory)`,
  then seal the existing `component::Registry`.
- `launch_local(registry, service_graph, step_graph, initial_time)` returns a
  `Launch` containing the existing `Host` and a step `Scheduler`. The service
  graph still supplies exact instance bindings, config, and lifecycle budgets.
  Semantic graph validation occurs before component construction.
- `Scheduler::try_step(request, timeout)` returns typed `AdmissionResult`,
  with an owned `Run` only for `Admission::accepted`. Rejections distinguish
  Busy, invalid request/timeout, not running, faulted, stopped, exhausted IDs,
  and supervisor loss. Unexpected ABI errors throw `component::Error` with
  their original status. Zero/negative C++ timeouts become typed invalid-timeout
  admission rather than an unsigned duration wrap.
- `Scheduler::clock()` returns canonical epoch, next index, logical time, and
  typed phase. `Run::id()` returns epoch, acceptance sequence, and tick index.
  Epoch zero is valid; logical tick identity is not acceptance sequence.
- `Run::wait` returns an owned retained `Result`, including failed attempts.
  `Result::kind()` and `Run::cancel()` return typed result/cancellation values.
  `Result::tick()` and `output(instance, port)` require a committed receipt;
  failure is an error, never a fabricated tick or empty successful value.
  `Result::info()` preserves the ABI's structured JSON failure document,
  including numeric failure kind and attempted/completed node lists.

Logical `Duration` uses normalized `secs` plus `nanos` and is not converted into
an accumulated `uint64_t` nanosecond count. The native boundary validates the
range and normalization. Wall-clock operation/wait budgets use positive
`component::Timeout` (`std::chrono::nanoseconds`).

## Callback and ownership contract

All per-instance create, prepare, step, stop, and destruction run serially on
one ordinary native callback thread. Independent instances have distinct lanes;
factory captures must support concurrent creation. Standard and unknown C++
exceptions from create/prepare/step/stop are caught in `noexcept` trampolines
and become copied callback-error diagnostics. Destructors must not throw.

`InputsView`, `OutputsView`, context, and tick are borrowed for the callback.
`InputsView::get` returns an owned immutable value; `retain()` creates an owned
immutable input map. Output sinks cannot be cloned or released. Copying a C++
output view neither extends its lifetime nor permits another thread to write.
Write each declared output once on the callback lane; the scheduler validates
the complete output set and schema.

Factory callable/capture destruction runs synchronously on the last-owner
thread, not necessarily an instance lane. It must not block or unwind. The
adapter uses native C++ finalization, not the Python notified-registration
path. Registration failure transfers no ownership; success retains the
factory until its native claims are gone.

Dropping run/result observers and timing out a wait never cancel execution.
`Run::cancel()` only acknowledges the cancellation request. A committed receipt
commits scheduler bookkeeping, not arbitrary component effects; errors, stop,
and cancellation cannot promise rollback.

`Host::request_stop()` is asynchronous and its return is not an admission
barrier. Always inspect typed admission. A scheduler handle does not keep its
host alive. Terminal failure or `Unresponsive` may precede callback/destructor
completion. Use `Host::wait_joined` for actual native quiescence and keep callback
code and factory roots alive until the required owners are gone. Keep the native
ABI library loaded for process lifetime because its shared runtime is
process-scoped. Detached application threads remain outside host certification.

Jobs, process/distributed steps, distributed clocks, deployment ownership,
hard termination of in-process callbacks, foreign zero-copy, and protobuf
codec generation are not supplied by this adapter.

## Independent native acceptance

`component_step_cpp_client.cpp` and its `component_step_cpp_callbacks.inc`,
`component_step_cpp_fixture.inc`, and `component_step_cpp_lifecycle.inc` exercise
the real native ABI:

- Two nodes over three ticks, explicit external/same-tick/previous-tick edges,
  outputs 12/14/16, distinct acceptance sequences, epoch/tick identity, and
  canonical clock advancement starting at 20,000,000,000 seconds.
- Immutable imports and explicit clones after source handles are released;
  schema mismatch, invalid graph before construction, unsupported service-only
  launch, typed invalid request/timeout and Busy admission, retained results,
  and drop-without-cancel behavior.
- Distinct ordinary lanes, copied registration tables, factory ownership on
  failed registration, public registry release during execution, retained input
  snapshots, wrong-thread and stale output-sink rejection, one-shot outputs,
  standard/unknown exceptions, preparation/stop errors, and invalid output sets.
- Callback gates for cancel and stop, wait timeout without cancellation,
  structured attempted/completed progress, no rollback of an observed effect,
  terminal Unresponsive separate from actual join, and a second destructor gate
  proving callback return is not enough for quiescence.

There are no sleeps or polling loops. Future waits subscribe before actions and
have bounded failure guards. The tiny join/wait budgets deliberately test a
gated operation that cannot yet complete; they do not assume scheduling speed.

The historical `component_step_cpp_acceptance.py` snapshot file list predates
the internal header and caller fragments. Keep that runner unchanged for its
historical source snapshot; for the current source tree, copy the complete
`include/` directory and C/C++ test fragments before using the strict native
commands in `component_cpp_README.md`. See `cpp_decomposition_README.md`.
The historical invocation takes the final ABI
owner's exact library, both header hashes, and official receipt/hash:

```sh
python3 crates/reiny-ffi/tests/component_step_cpp_acceptance.py \
  --label final \
  --library /absolute/frozen/libreiny_ffi.so \
  --library-sha256 "$LIBRARY_SHA256" \
  --step-header-sha256 "$STEP_HEADER_SHA256" \
  --service-header-sha256 "$SERVICE_HEADER_SHA256" \
  --native-receipt /absolute/native/verification.md \
  --native-receipt-sha256 "$NATIVE_RECEIPT_SHA256" \
  --output /absolute/new/private/acceptance-directory
```

The runner creates a private source snapshot and separate target, compiles
with `g++ -std=c++17 -Wall -Wextra -Werror -pedantic -pthread`, and uses an
absolute library link instead of a loader search fallback. On Linux the client
checks `/proc/self/maps` for the exact frozen library. It preserves command
arguments, separate compiler/client exit codes, stdout/stderr logs and their
hashes, source/header/executable hashes, before/after library hashes, and
timestamps in `receipt.json`. Subprocesses have a 60-second failure bound.
Failed attempts remain preserved too; a retry needs a new output directory.
Retained evidence targets are not source files or a claim of a process-host test.
