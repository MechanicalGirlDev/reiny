# Explicitly owned Python jobs

`reiny_component.jobs` is an additive wrapper for the actual `reiny_job.h`
boundary and native `JobOwner`. It does not implement another scheduler.
Importing the module resolves no job symbols. Explicit `OwnedJobs(library)`
opt-in binds them; an old service-only or step-only library raises typed
`JobsUnavailableError` with service status UNSUPPORTED. The existing service
and step modules, bindings and behavioral assertions are unchanged.

## Minimal independent worker

```python
from reiny_component import Library
from reiny_component.jobs import OwnedJobs, Worker, ResultKind

native = Library("/absolute/path/to/owned-jobs/libreiny_ffi.so")
api = OwnedJobs(native)
input_bytes = bytes(b"independently owned application input")

class Compute(Worker):
    def __init__(self, payload):
        self.payload = payload

    def run(self, context, success):
        context.report(b"computing")
        success.set(self.payload.upper())

with api.owner(capacity=1) as owner:
    with api.work(lambda context: Compute(input_bytes)) as work:
        with owner.start(work) as job:
            work.close()  # Release this data claim, not execution or observation.
            with job.wait(5_000_000_000) as result:
                if result.kind == ResultKind.SUCCEEDED:
                    output = result.success
                elif result.kind == ResultKind.APPLICATION_ERROR:
                    failure = result.error
                # Cancelled/panicked/interrupted have no successful payload.
            job.wait_joined(5_000_000_000)
    report = owner.shutdown(5_000_000_000)
    # A nonempty report.unresponsive requires keeping the owner and retrying.
    assert report.unresponsive == () and report.join_failures == ()
native.collect_factories()
```

Production workers should choose an explicit cancellation policy and return
promptly after observing cancellation. Setting success copies bytes into a
lane-local sink; it is **not** a completion operation. Core terminal commit is
possible only after `run` returns and native `destroy`/OS-thread join finishes.
If cancellation wins, the core discards the example's successful bytes instead
of treating them as a successful completion.

There is no native job input accessor/schema/request field in this ABI.
Factories explicitly capture independent application data, such as immutable
copied bytes. Do not capture borrowed component objects or mutable storage whose
lifetime can end before the worker joins.

## Owner, work, observation and join are separate

`api.work(factory)` copies a callback table and creates one independent native
factory-data claim. The factory receives a borrowed job `Context` and returns
an independent `Worker`. No instance is created merely by making the work.
Each work is single-use after successful admission; failed admission leaves it
unused. `work.clone()` creates another ownership claim, not another job.

`api.owner(capacity)` requires a positive capacity. `owner.start(work)` schedules
actual work without waiting for callbacks. Native capacity counts through the
actual worker join, including a blocked destructor; it cannot reopen just
because bytes were written or a callback result was observed.

Start failures preserve the separate `StartStatus` values and distinct
`StartError` subclasses: `OwnerClosedError`, `CapacityError`,
`IdExhaustedError`, `RuntimeUnavailableError`. These also inherit service
`ReinyError`. Existing invalid-argument/handle/state/deadline statuses remain
service exceptions; native error prose is not parsed to classify failures.

`owner.close_admission()` permanently closes admission and requests cancellation
of owned jobs; it is idempotent and does not release or join.
`owner.shutdown(grace_ns)` does the same, then waits one shared grace deadline.
Zero grace is valid. It returns an immutable `ShutdownReport(version,
unresponsive, join_failures)` containing actual job-ID tuples. Nonempty
`unresponsive` is a real timeout report, **not** a fabricated terminal result or
a thrown observation deadline. Roots and unfinished workers remain owned.
Keep the owner, or an explicit owner clone, and retry shutdown after cooperative
work exits. Admission remains closed.

`owner.close()` and leaving its context manager release that ownership claim.
Only release of the last owner requests cancellation, and it cannot await or
certify join. Finalizers are fallback releases, never shutdown certificates.
User-created detached work is not owned or certified by these APIs.

Dropping a job or observer abandons observation, not execution.
`job.wait(timeout_ns)` observes the retained terminal result.
`job.wait_joined(timeout_ns)` independently certifies native worker-thread join;
it is not factory-data release and is not a replacement for owner shutdown.
Wait budgets are positive integer nanoseconds. Timeout abandons that wait only,
never cancels work. Native owner capacity bookkeeping can still be retained
until its core task is reaped; no immediate post-join scheduling deadline is
promised.

After terminal success and actual join, a retained **work handle** can still
keep callback captures alive. Close all work claims before collecting roots.
A native release token says the last data owner is gone, not that an owner was
shut down or that a join was observed. Keep the required owner/job observations
until their corresponding join/shutdown proof is obtained.

## Progress, results and application errors

Register `observer = job.subscribe()` before triggering progress. Its first
`changed(positive_timeout_ns)` waits for a revision after the current snapshot.
Progress is latest-value state, not an event delivery queue; updates may
coalesce. `job.snapshot()` copies current state without advancing an observer.

Owned `Snapshot` handles are immutable. `snapshot.info` copies typed
`SnapshotInfo(id, revision, phase, cancellation_requested, has_progress)`.
`Phase` is RUNNING, CANCELLING or COMPLETE. `snapshot.progress` copies bytes;
absent progress is INVALID_STATE. `snapshot.result()` requires a terminal
snapshot and returns an independently owned retained result. Keeping an old
snapshot does not make it update when the job advances.

`job.cancel()` acknowledges REQUESTED, ALREADY_REQUESTED or FINISHED.
Cancellation is not native join or terminal completion. If cancellation wins
before successful commit, the core discards successful bytes and retains
CANCELLED. A real application error remains APPLICATION_ERROR after cancellation
and wins over bytes already written to the success sink.

`Result.kind` is SUCCEEDED, APPLICATION_ERROR, CANCELLED, PANICKED or INTERRUPTED.
Only SUCCEEDED permits `result.success`; only APPLICATION_ERROR permits
`result.error`, an immutable `ApplicationFailure(code, message)`. Inapplicable
getters raise service INVALID_STATE. Results survive release of their job,
owner, observer and work handles.

Raise `ApplicationError(negative_int32_code, utf8_message)` from create/run to
preserve the copied application code/message. Embedded NUL and invalid code
ranges are rejected at that Python boundary. Other `BaseException`s, including
KeyboardInterrupt/SystemExit, become copied CALLBACK_ERROR (-10) outcomes
rather than unwinding into C. They do not become native Rust PANICKED outcomes.
A successful callback that omits its success bytes is a native application
error, not an implicit empty success.

`Worker.destroy()` is a void native hook and cannot change the native result
through an error sink. Its exceptions are caught and retained in
`work.finalizer_errors`, shared by work clones. Inspect this list separately:
the ABI cannot promise that a throwing Python destroy hook changes an otherwise
successful outcome. Native join still waits for that hook's return.

## Callback and payload ownership

For each admitted job, create/run/destroy are serialized on one ordinary native
OS thread, independent of component lifecycle lanes. ctypes acquires the GIL
on callback entry and releases it in native waits; arbitrary Python contention
can delay callbacks. No hard execution or hard cancellation bound is promised.

Context/success/error handles are borrowed per callback. The wrapper expires
them when that callback returns. Explicitly clone a `Context` to retain
report/cancellation observation; it does not own execution. `Context.report`
copies progress and returns False when cancellation/completion or revision
exhaustion rejects it. `wait_cancelled` follows the native contract: it resolves
on cancellation **or terminal completion**; inspect `is_cancelled` afterward.

The success sink cannot be cloned/released, is lane-confined, expires at return
and accepts exactly one `set(bytes)`. Native progress, success and diagnostic
inputs are copied immediately. Public payload getters return immutable Python
bytes and copied strings; no cross-language zero-copy or decode validation is
claimed. Join all application child threads before returning from work.

The wrapper uses only `reiny_job_work_new_notified` with NULL
`release_factory`, and retains the existing caller-owned native release token.
Rejected work import binds/transfers nothing; successful import binds once and
transfers one data claim. No Python/GIL callback runs on native last-owner
finalization. Shared service-wrapper roots retain callbacks, instances and
captures while native work owns them.

After real worker/owner join and release of retained work claims, call
`native.collect_factories()` on the application thread. It queries native
tokens, closes released token claims and removes released roots. It is not a
join operation. Unused work requires only last-claim release (no thread ever
started). The native CDLL remains loaded for process lifetime.

## Real acceptance and limits

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=bindings/python \
REINY_JOB_LIBRARY=/absolute/path/to/frozen-owned-job/libreiny_ffi.so \
REINY_COMPONENT_SERVICE_LIBRARY=/absolute/path/to/original-service/libreiny_ffi.so \
REINY_COMPONENT_STEP_LIBRARY=/absolute/path/to/original-notified-step/libreiny_ffi.so \
python3 -m unittest discover -s bindings/python -p test_component_job.py -v
```

The suite requires exact frozen artifact/header hashes. It uses real native
work and event-gated callbacks/destructors, never mocked scheduling or sleeps.
It covers success and capacity remaining pending through both gates; immutable
progress/terminal snapshots and mutable C-source copying; cancellation
preserving application errors; timeout reports and shutdown retry through a
retained owner; observer abandonment; owner drop without false join/free;
retained work after terminal/native join; unused work; notified token
transactions; callback exceptions and missing-success errors.

The extension is standalone and **explicitly owned**, not automatically bound
to a component or step. There are no distributed jobs/steps, named job RPC
protocol, durable remote IDs, process-tree ownership, rollback or forcible
callback termination. Applications must explicitly coordinate any captured
component state with both job shutdown and component host lifetime.
