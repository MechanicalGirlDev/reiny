#ifndef REINY_COMPONENT_STEP_H
#define REINY_COMPONENT_STEP_H
#include "reiny_component.h"
#ifdef __cplusplus
extern "C" {
#endif

/*
 * Additive Local managed steps, version 1. The service table is unchanged.
 * No jobs, process/distributed steps, rollback, or hard callback termination.
 * All ownership, pointer, callback and error rules of reiny_component.h apply.
 * Durations use normalized seconds/nanoseconds, never accumulated u64 nanos.
 */
typedef struct { uint64_t secs; uint32_t nanos; } reiny_step_duration;
typedef struct {
    uint64_t epoch, index;
    reiny_step_duration start, dt, end;
} reiny_step_tick;
typedef struct {
    uint64_t epoch, next_index;
    reiny_step_duration time;
    uint32_t phase; /* 1 starting, 2 ready, 3 running, 4 faulted, 5 stopped */
} reiny_step_clock;

typedef int32_t (*reiny_step_callback)(
    void *, reiny_exec_handle context, const reiny_step_tick *,
    reiny_exec_handle inputs, reiny_exec_handle outputs, reiny_exec_handle error_sink);
typedef struct {
    uint32_t struct_size, abi_major, flags, reserved;
    void *factory_data;
    int32_t (*create)(void *, uint64_t, void **, uint64_t);
    int32_t (*prepare)(void *, uint64_t, uint64_t);
    reiny_step_callback step;
    int32_t (*stop)(void *, uint64_t, uint32_t, uint64_t);
    void (*destroy_instance)(void *);
    void (*release_factory)(void *);
} reiny_step_factory_v1;

/*
 * Table size must match exactly, abi_major=1, flags=reserved=0.
 * All instance hooks are required and run on one ordinary OS lane per instance.
 * step returns after filling every declared output exactly once. The immutable
 * inputs handle may be cloned; outputs is borrowed, noncloneable and expires
 * at return. Outputs may only be written from that callback's lane.
 * Factory claim transfers only on successful registration; release_factory has
 * the service finalizer contract. Schema is copied into the registration.
 * Failed/cancelled receipts do not imply callback, destroy, or adapter teardown.
 * Use host_wait_joined for actual quiescence, including after Unresponsive.
 */
int32_t reiny_step_register(uint64_t registry, reiny_exec_str name,
    reiny_exec_str revision, uint64_t schema, const reiny_step_factory_v1 *);
/*
 * Interpreter-safe registration: use an existing caller-owned token from
 * reiny_exec_factory_release_token_new. release_factory MUST be NULL.
 * Success binds the token once and transfers one factory-data claim; failure
 * binds/transfers nothing. Registration never consumes the caller's token.
 * reiny_exec_factory_released returns 1 only after the last native factory
 * owner is gone, including blocked callback/destroy and adapter teardown.
 * No foreign finalizer runs. Keep callback roots pinned until notification,
 * then collect them on the caller/interpreter thread. Cloning/releasing tokens
 * changes observation ownership only; a token does not keep the factory alive.
 */
int32_t reiny_step_register_notified(uint64_t registry, reiny_exec_str name,
    reiny_exec_str revision, uint64_t schema, const reiny_step_factory_v1 *,
    uint64_t token);

/*
 * JSON imports copy handles' values immediately. Referenced handles must remain
 * valid through the call; imported objects are immutable and independently owned.
 * schema: {"version":1,"inputs":{"name":SCHEMA_HANDLE},"outputs":{...}}
 * A port may instead be {"schema":SCHEMA_HANDLE,"semantics":{"units":"rad"}}.
 * Assertions are opaque application identifiers, not payload validation or
 * conversion. Keys/values must not be blank. Maps must match exactly, including
 * empty versus nonempty. They confer no stream/RPC or other routing authority.
 * graph: {"version":1,"nodes":{"namespace":STEP_SCHEMA_HANDLE},"routes":[
 *   {"instance":"dst","port":"in","kind":"external"},
 *   {"instance":"dst","port":"in","kind":"same_tick",
 *    "source":{"instance":"src","port":"out"}},
 *   {"instance":"dst","port":"in","kind":"previous_tick",
 *    "source":{"instance":"src","port":"out"},"initial":VALUE_HANDLE}]}
 * request: {"version":1,"dt":{"secs":0,"nanos":1},"inputs":[
 *   {"instance":"dst","port":"in","value":VALUE_HANDLE}]}
 * Duplicate keys/routes and unknown fields are rejected. Graph semantic checks
 * occur synchronously at launch, before component construction.
 */
int32_t reiny_step_schema_import(reiny_exec_bytes json, uint64_t *out);
int32_t reiny_step_graph_import(reiny_exec_bytes json, uint64_t *out);
int32_t reiny_step_request_import(reiny_exec_bytes json, uint64_t *out);
int32_t reiny_step_value_new(uint64_t schema, reiny_exec_bytes bytes, uint64_t *out);
/* Explicit assertions are copied from a JSON string-to-string object.
 * Existing value_new asserts an empty map. No ABI struct layout changes.
 */
int32_t reiny_step_value_new_with_semantics(uint64_t schema,
    reiny_exec_bytes semantics, reiny_exec_bytes bytes, uint64_t *out);
/* Return an independently owned JSON buffer containing the assertion map. */
int32_t reiny_step_value_semantics(uint64_t value, uint64_t *json_buffer);
/* Exact protobuf identity check precedes copying opaque protobuf bytes.
 * Like native typed decode, this does not compare application assertions.
 */
int32_t reiny_step_value_copy(uint64_t value, uint64_t schema,
    uint8_t *dest, size_t capacity, size_t *required);
int32_t reiny_step_input_get(uint64_t inputs, reiny_exec_str port, uint64_t *out);
int32_t reiny_step_output_set(uint64_t outputs, reiny_exec_str port, uint64_t value);
/* Existing service graph supplies bindings/config/timeouts; step graph is separate.
 * Returned host uses existing lifecycle calls; scheduler does not keep host alive.
 * Service-only/process launch rejects registries containing step factories.
 */
int32_t reiny_step_host_launch_local(uint64_t registry, uint64_t service_graph,
    uint64_t step_graph, reiny_step_duration initial_time,
    uint64_t *host, uint64_t *scheduler);
int32_t reiny_step_clock_read(uint64_t scheduler, reiny_step_clock *out);

enum {
    REINY_STEP_NOT_RUNNING = -101, REINY_STEP_BUSY = -102,
    REINY_STEP_INVALID_REQUEST = -103, REINY_STEP_INVALID_TIMEOUT = -104,
    REINY_STEP_ID_EXHAUSTED = -105, REINY_STEP_FAULTED = -106,
    REINY_STEP_STOPPED = -107, REINY_STEP_SUPERVISOR_GONE = -108
};
/* Synchronous typed admission. Dropping handles or wait timeout never cancels. */
int32_t reiny_step_try(uint64_t scheduler, uint64_t request,
    uint64_t timeout_ns, uint64_t *run);
int32_t reiny_step_run_id(uint64_t run, uint64_t *epoch, uint64_t *sequence,
    uint64_t *tick_index);
/* Acknowledgement only: 1 requested, 2 already requested, 3 finished, 4 gone. */
int32_t reiny_step_cancel(uint64_t run, uint32_t *status);
/* Wait timeout is REINY_EXEC_TIMEOUT; successful wait yields an owned result,
 * including failed attempts. Repeated observation returns the retained outcome.
 */
int32_t reiny_step_wait(uint64_t run, uint64_t timeout_ns, uint64_t *result);
/* 0 committed, 1 cancelled before dispatch, 2 timed out before dispatch,
 * 3 failed after dispatch, 4 supervisor gone. */
int32_t reiny_step_result_kind(uint64_t result, uint32_t *out);
int32_t reiny_step_result_tick(uint64_t result, reiny_step_tick *out);
int32_t reiny_step_result_output(uint64_t result, reiny_exec_str instance,
    reiny_exec_str port, uint64_t *value);
/* JSON buffer: version, kind, epoch, index and failure (null or object with
 * kind, instance, port, attempted, completed, detail). Failure kind is:
 * 1 node_error, 2 node_panicked, 3 invalid_outputs, 4 cancelled, 5 timed_out,
 * 6 host_stopped, 7 worker_lost, 8 advance_dropped.
 * Bookkeeping commit is not rollback of external effects. Stop request delivery
 * is asynchronous; a return from host_request_stop is not an admission barrier.
 */
int32_t reiny_step_result_info(uint64_t result, uint64_t *json_buffer);
#ifdef __cplusplus
}
#endif
#endif
