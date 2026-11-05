#ifndef REINY_JOB_H
#define REINY_JOB_H
#include "reiny_component.h"
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Standalone Local jobs, not automatically component-bound or distributed.
 * Uses the executable handle/status/copy conventions of reiny_component.h.
 * Owners must be explicitly retained and shut down before unloading code.
 * The process-lifetime native runtime must remain loaded.
 *
 * A work handle owns one independent factory-data claim, copied callback table
 * and strings/bytes supplied to APIs. Do not borrow component state whose
 * lifetime can end before this work. Each work can be admitted once; failed
 * admission leaves it unused. Capacity rejection never invokes create/run.
 * Releasing a job/observer abandons observation, not execution. Last owner
 * release requests cancellation but cannot join. Retain an owner clone to
 * retry shutdown after a deadline.
 */
typedef struct {
    uint32_t struct_size, abi_major, flags, reserved;
    void *factory_data;
    int32_t (*create)(void *, uint64_t context, void **instance, uint64_t error_sink);
    int32_t (*run)(void *, uint64_t context, uint64_t result_sink, uint64_t error_sink);
    void (*destroy_instance)(void *);
    void (*release_factory)(void *);
} reiny_job_work_v1;
/*
 * Exact size, abi_major=1, flags=reserved=0; create/run/destroy are required.
 * Per job, create/run/destroy run sequentially on one ordinary OS thread.
 * create must write a non-NULL independent instance on success; on failure it
 * cleans its own partial construction. destroy runs once after successful
 * create, including failed/cancelled execution. No foreign unwind is allowed.
 * Join any foreign child threads before returning; detached foreign work is
 * not tracked by the owner and cannot be certified by native join.
 * release_factory is optional, runs on the last-owner thread, must not block
 * or unwind. Python MUST use work_new_notified with release_factory=NULL:
 * existing native release tokens notify without acquiring a GIL.
 * work_new success transfers one factory-data claim; failure transfers none.
 * Tokens bind only on success, remain caller-owned, and do not retain work.
 * Keep roots until token notification AND actual worker/owner join.
 */
int32_t reiny_job_work_new(const reiny_job_work_v1 *, uint64_t *work);
int32_t reiny_job_work_new_notified(const reiny_job_work_v1 *, uint64_t token, uint64_t *work);
enum {
    REINY_JOB_OWNER_CLOSED = -201, REINY_JOB_AT_CAPACITY = -202,
    REINY_JOB_ID_EXHAUSTED = -203, REINY_JOB_RUNTIME_UNAVAILABLE = -204
};
int32_t reiny_job_owner_new(size_t capacity, uint64_t *owner);
/* Blocking ABI calls require ordinary caller threads. start serializes with
 * shutdown; successful admission schedules work but does not wait for it.
 * Capacity counts the core worker through actual native join, including
 * destroy_instance. Retained data/result handles do not consume capacity.
 */
int32_t reiny_job_start(uint64_t owner, uint64_t work, uint64_t *job);
int32_t reiny_job_owner_close(uint64_t owner);
/* Closes admission, requests cancellation and waits one shared grace deadline.
 * grace_ns may be zero. Success returns a JSON buffer:
 * {"version":1,"unresponsive":[job IDs],"join_failures":[job IDs]}.
 * Nonempty unresponsive is a timeout report, not a fabricated terminal result.
 * Workers/roots remain owned; another shutdown call can join them later.
 */
int32_t reiny_job_owner_shutdown(uint64_t owner, uint64_t grace_ns, uint64_t *report);
/* 1 requested, 2 already requested, 3 finished. Cancellation is not completion. */
int32_t reiny_job_cancel(uint64_t job, uint32_t *status);
/* Wait deadlines only abandon observation; they never cancel work. */
int32_t reiny_job_wait(uint64_t job, uint64_t timeout_ns, uint64_t *result);
int32_t reiny_job_wait_joined(uint64_t job, uint64_t timeout_ns);
typedef struct {
    uint64_t id, revision;
    uint32_t phase; /* 1 running, 2 cancelling, 3 complete */
    uint32_t cancellation_requested, has_progress;
} reiny_job_snapshot_info;
int32_t reiny_job_snapshot(uint64_t job, uint64_t *snapshot);
int32_t reiny_job_subscribe(uint64_t job, uint64_t *observer);
int32_t reiny_job_changed(uint64_t observer, uint64_t timeout_ns, uint64_t *snapshot);
int32_t reiny_job_snapshot_read(uint64_t snapshot, reiny_job_snapshot_info *out);
int32_t reiny_job_snapshot_progress(uint64_t snapshot, uint64_t *buffer);
int32_t reiny_job_snapshot_result(uint64_t snapshot, uint64_t *result);
/* 1 succeeded, 2 application error, 3 cancelled, 4 panicked, 5 interrupted.
 * Success is committed only after run returns and native work is joined.
 * Cancellation before commit discards successful bytes; application errors
 * remain errors after cancellation. Terminal result and data release are
 * distinct: retained work handles can still retain callback roots.
 */
int32_t reiny_job_result_kind(uint64_t result, uint32_t *kind);
int32_t reiny_job_result_success(uint64_t result, uint64_t *buffer);
int32_t reiny_job_result_error(uint64_t result, int32_t *code, uint64_t *message);
/*
 * Callback context/result/error sinks are borrowed. Context may be cloned;
 * sinks cannot be cloned/released and expire at callback return.
 * report copies progress; accepted=0 means cancellation/completion or revision
 * exhaustion rejected it. Progress is latest-value, not a delivery queue.
 * succeed copies bytes into the lane-local sink exactly once, NOT a completion
 * operation. Return zero from run to use it; missing success is an error.
 * Return nonzero to fail; reiny_exec_error_set(error_sink, negative_code, text)
 * supplies a copied application diagnostic. A return error wins over success.
 */
int32_t reiny_job_report(uint64_t context, reiny_exec_bytes progress, uint32_t *accepted);
int32_t reiny_job_is_cancelled(uint64_t context, uint32_t *out);
/* Follows core JobContext: resolves at cancellation OR terminal completion.
 * Inspect is_cancelled to distinguish an actual cancellation request. */
int32_t reiny_job_wait_cancelled(uint64_t context, uint64_t timeout_ns);
int32_t reiny_job_succeed(uint64_t result_sink, reiny_exec_bytes value);
#ifdef __cplusplus
}
#endif
#endif
