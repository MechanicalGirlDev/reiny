# Standalone explicitly owned C++ jobs

`../include/reiny_job.hpp` exposes `reiny::job` over the real native `JobOwner`
ABI. It reuses the existing executable adapter's handle/error/byte utilities;
it does not borrow, retain, or automatically attach to a component instance.
Existing service and managed-step wrappers are unchanged.
Install or copy the complete `include/` directory, including
`reiny_job_work.hpp` and the service header helpers.

## Ownership and work

Derive from `job::Task`, implement `run(ContextView, SuccessView)`, and call
`Work::create(factory)` with a factory returning `unique_ptr<Task>`. Capture
independently owned data, not borrowed component/stack state. The factory's
copied callback table and captures transfer to native ownership only after
successful work construction. Native execution retains them after public
work/job/observer handles are dropped.

Each work item is single-use after successful start. `Work::clone()` gives
another ownership claim to the same work, not another admission. A rejected
start leaves the work unused and does not call create/run. Retaining a work
claim after result/native join may still retain its callback data.

Create an explicit `Owner::create(capacity)` and call `owner.start(work)`.
`StartResult` contains an owned `Job` only for `StartStatus::accepted`.
Typed rejections include capacity, closed owner, already-admitted work,
ID exhaustion, and unavailable runtime. Other ABI errors throw `job::Error`
with their original status. Call blocking APIs on ordinary caller threads.

Per-job create, run, and task destruction run serially on one ordinary native
thread. Capacity covers the core worker through actual native join, including
the task destructor. Publication of terminal state and the core task's final
return are separate; the wrapper does not promise immediate capacity reopening
merely because a result/native-join observation succeeded.

Task destructors must not unwind. Factory callable/capture destruction runs
synchronously on the last-owner thread and must not block or unwind. The C++
adapter uses that native finalizer path, not Python's release-notification
entry point. Join all foreign child threads before callback return; detached
work is outside native cleanup certification.

## Progress, results, and errors

`ContextView::report(bytes)` copies latest-value progress and returns whether
the core accepted it. Rejection after cancellation/completion is `false`, not
an ABI failure. `cancelled()` is a snapshot. `wait_cancelled(timeout)` follows
the native contract: notification resolves on cancellation **or terminal
completion**; inspect `cancelled()` to distinguish them.

Callback context/success/error sinks are borrowed. `context.retain()` makes a
separate owned context claim. `SuccessView::set(bytes)` copies success bytes
once on the native lane; copying that C++ view does not retain the sink.
Writing success is not completion. Native callback return, destruction, and
join must happen before the core can commit a terminal result.

Throw `ApplicationError(negative_code, message)` to return a copied application
failure. Standard and unknown C++ exceptions from create/run are caught in
`noexcept` trampolines and become callback errors. A missing success write is
also a native callback error. An error return wins over an earlier success
write.

`Job::snapshot` returns an immutable copied snapshot with typed phase,
cancellation flag, revision, and job ID. `progress()` requires actual progress;
`result()` requires terminal state. `Job::subscribe` returns a progress/status
observer; register it before triggering the action and call `changed(timeout)`.
Updates may coalesce. Retained snapshots never change into later snapshots.

`Job::wait` returns an owned retained `Result`. `kind()` distinguishes success,
application error, cancellation, panic, and interruption. `success()` is only
valid for success, and `error()` returns the native application code/message
only for application errors. Immutable snapshots/results and jobs can be
explicitly cloned into separate owned claims.

## Cancellation and shutdown

Observer/job release and wait timeout only abandon observation. `Job::cancel`
returns a typed acknowledgement, not completion or a successful join.
Cancellation before success commit discards successful bytes and produces
Cancelled; application failures remain failures after cancellation.

`Owner::close_admission()` closes admission and requests cancellation.
Inherited `close()` releases that ownership claim. Dropping the **last** owner
requests cancellation without waiting or certifying cleanup. Keep an explicit
owner clone for bounded shutdown and retry.

`owner.shutdown(grace)` allows zero grace and returns the native JSON report
with `unresponsive` and `join_failures` ID arrays. A successful ABI call with
nonempty arrays is not successful cleanup. Live work and roots remain owned,
so repeat shutdown on the retained owner after a timeout. Negative C++ grace
is rejected rather than wrapped into an unsigned budget.

`Job::wait_joined(timeout)` independently observes actual native worker join.
Retained work claims can still retain callback data afterward. Keep callback
code loaded while native ownership claims survive, and keep the native ABI
library loaded for process lifetime because its runtime is process-scoped.
There is no hard termination, rollback, remote job capability, detached-thread
certificate, or automatic binding to a component's lifetime.

## Real native acceptance and preserved compatibility

`job_cpp_client.cpp` and `job_cpp_cases.inc` run actual factories and work:

- Capacity-one rejection without construction, single-use clones, capacity
  remaining occupied through a separately gated destructor, and reuse of
  rejected work under another explicit owner.
- Copied binary progress and success after mutation of callback source buffers;
  retained immutable snapshots/results after completion and handle release.
- Success written while run is still gated, cancelled success disposal,
  application error preservation after cancellation, and standard/unknown/
  missing-result/factory exception handling.
- Dropped observers/job handles without cancellation; last-owner drop requesting
  cancellation without joining; callback-data roots retained until real teardown.
- Zero-grace shutdown reporting the exact unresponsive job ID, retained owner
  retry with actual join, retained work keeping data after join, and once-only
  native cleanup of unused work without constructing an instance.

All gates use retained future notifications established before the actions,
with bounded failure guards. There are no sleeps or polling. Tiny timeouts
probe work that cannot finish until an explicit gate opens.

The historical `job_cpp_acceptance.py` has an explicit snapshot file list
that predates the internal header and caller fragments. Preserve that runner
for its historical source snapshot. Current native checks must copy the
complete `include/` directory and C/C++ test fragments; see
`cpp_decomposition_README.md`.

For its historical tree, `job_cpp_acceptance.py` builds in a copied source snapshot and separate
target. It compiles the new job client and the **unchanged** service and step
C++ clients with `-std=c++17 -Wall -Wextra -Werror -pedantic -pthread`.
It absolute-links each to the exact frozen library, records `readelf -d`
proof, and uses the job/step clients' `/proc/self/maps` verification.

```sh
python3 crates/reiny-ffi/tests/job_cpp_acceptance.py \
  --library /absolute/frozen/libreiny_ffi.so \
  --library-sha256 "$LIBRARY_SHA256" \
  --native-receipt /absolute/native/verification.md \
  --native-receipt-sha256 "$NATIVE_RECEIPT_SHA256" \
  --job-header-sha256 "$JOB_HEADER_SHA256" \
  --step-header-sha256 "$STEP_HEADER_SHA256" \
  --service-header-sha256 "$SERVICE_HEADER_SHA256" \
  --output /absolute/new/private/acceptance-directory
```

`receipt.json` retains exact commands, independent compiler/client exit codes,
stdout/stderr logs and hashes, source/header/executable hashes, official native
receipt hash, before/after library hashes, and timestamps. Every subprocess has
a 60-second failure bound. Failed attempts remain as evidence too; reruns need
a new output directory. Successful runs reap each test process; evidence
snapshots, binaries, and logs remain intentionally retained for review.
