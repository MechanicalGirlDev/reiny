#ifndef REINY_COMPONENT_H
#define REINY_COMPONENT_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Executable ABI v1: explicitly registered, trusted service components.
 * This API shares the legacy reiny.h handle ID space but NOT its status codes.
 * IDs are never recycled; zero, released IDs, and wrong object kinds fail.
 * No dynamic loading, sandbox, steps, jobs, deployment ownership, distributed
 * scheduling, or cross-language zero-copy is provided by this ABI.
 *
 * Input slices are borrowed for the call and copied before asynchronous use.
 * A nonempty slice must refer to one readable allocation (length <= PTRDIFF_MAX).
 * Text is UTF-8 without embedded NUL; lengths exclude a terminating NUL.
 * Output pointers must be aligned, writable and disjoint from all other inputs
 * and outputs. Forged non-NULL addresses remain the caller's responsibility.
 * Outputs are unchanged on failure EXCEPT copy functions write required length
 * on BUFFER_TOO_SMALL. (NULL,0) queries required size. Text copies include NUL;
 * buffer copies and JSON documents are bytes, without a terminating NUL.
 * Each successful output handle is owned and must be released exactly once.
 *
 * All calls can be made concurrently. Blocking waits require ordinary threads,
 * not Tokio runtime threads. No registry lock is held across foreign code,
 * engine calls, destructors, or waits. Sends/replies may block on backpressure:
 * this ABI does not promise a deadline for synchronous engine effects.
 * One process-lifetime shared runtime drives local hosts and async operations.
 */
typedef uint64_t reiny_exec_handle;
typedef int32_t reiny_exec_status;
typedef struct { const uint8_t *data; size_t len; } reiny_exec_bytes;
typedef reiny_exec_bytes reiny_exec_str;

enum {
    REINY_EXEC_OK = 0,
    REINY_EXEC_INVALID_ARGUMENT = -1,
    REINY_EXEC_INVALID_HANDLE = -2,
    REINY_EXEC_WRONG_KIND = -3,
    REINY_EXEC_INVALID_STATE = -4,
    REINY_EXEC_TIMEOUT = -5,
    REINY_EXEC_CLOSED = -6,
    REINY_EXEC_REMOTE_ERROR = -7,
    REINY_EXEC_NO_REPLY = -8,
    REINY_EXEC_SCHEMA_ERROR = -9,
    REINY_EXEC_CALLBACK_ERROR = -10,
    REINY_EXEC_BUFFER_TOO_SMALL = -12,
    REINY_EXEC_PANIC = -13,
    REINY_EXEC_UNSUPPORTED = -14,
    REINY_EXEC_ALREADY_COMPLETED = -15,
    REINY_EXEC_FAILURE = -16
};
enum {
    REINY_EXEC_STARTING = 1, REINY_EXEC_PREPARED = 2,
    REINY_EXEC_COMMITTED = 3, REINY_EXEC_RUNNING = 4,
    REINY_EXEC_STOPPING = 5, REINY_EXEC_STOPPED = 6,
    REINY_EXEC_FAILED = 7, REINY_EXEC_UNRESPONSIVE = 8
};
enum { REINY_EXEC_SERVICE = 1 };
enum { REINY_EXEC_INPUT = 1, REINY_EXEC_OUTPUT = 2 };
enum { REINY_EXEC_RPC_CLIENT = 1, REINY_EXEC_RPC_SERVER = 2 };
enum { REINY_EXEC_STOP_REQUESTED = 1, REINY_EXEC_STARTUP_FAILURE = 2, REINY_EXEC_COMPONENT_FAILURE = 3 };
enum { REINY_EXEC_PAYLOAD = 0, REINY_EXEC_SOURCE = 1, REINY_EXEC_SOURCE_PORT = 2, REINY_EXEC_MESSAGE_INFO = 3 };

/*
 * struct_size must be sizeof(reiny_exec_factory_v1), abi_major=1, flags=0,
 * mode=REINY_EXEC_SERVICE. All instance callbacks are required.
 *
 * Registration copies the table and strings. Success transfers exactly one
 * factory_data ownership claim. Failure transfers nothing. Factory data must
 * support concurrent creates on independent lanes. release_factory is optional;
 * when present, it is called once after every registry/instance claim is gone.
 * This finalizer may run on any thread, must not block, and must not unwind.
 *
 * Each instance has one dedicated ordinary OS thread. create, prepare, run,
 * stop and destroy_instance are serialized on that thread. create success must
 * write a non-NULL instance. Failure must clean partial construction itself.
 * prepare opens declared endpoints; it must not send, call RPC, or detach work.
 * run serves until context_wait_stop returns. Returning early fails the host.
 * stop follows an interrupted/failed prepare or run, never concurrently.
 * destroy_instance follows stop exactly once, including failed preparation.
 * No exception, panic, longjmp or foreign unwind may cross any callback boundary.
 * C++/Python wrappers must catch exceptions and report through error_set.
 *
 * context and error_sink arguments are BORROWED handles. release rejects them.
 * Explicit clone of context creates an owned claim. Sinks cannot be cloned and
 * expire at callback return. error_set copies diagnostics immediately.
 * Context clones retain native transport resources, not running authority.
 *
 * Admission and stop linearize at the context phase mutex: an operation that
 * observed running may finish after stop. Later admissions fail CLOSED.
 * Prepare permits only endpoint declarations and metadata. Already admitted
 * effects are not rolled back. Native endpoint lifetime is not revocation.
 *
 * Terminal UNRESPONSIVE means cleanup is unconfirmed, not callback termination.
 * host_wait_joined waits for actual callback teardown, even after timeout.
 * Never unload callback machine code or finalize its interpreter until every
 * host has joined AND every registry/factory claim has been released. Retained
 * contexts/endpoints must also be released to reclaim transport resources.
 * Arbitrary detached foreign threads are not tracked or certified by the SDK.
 */
typedef struct {
    uint32_t struct_size, abi_major, flags, mode;
    void *factory_data;
    reiny_exec_status (*create)(void *, reiny_exec_handle, void **, reiny_exec_handle);
    reiny_exec_status (*prepare)(void *, reiny_exec_handle, reiny_exec_handle);
    reiny_exec_status (*run)(void *, reiny_exec_handle, reiny_exec_handle);
    reiny_exec_status (*stop)(void *, reiny_exec_handle, uint32_t, reiny_exec_handle);
    void (*destroy_instance)(void *);
    void (*release_factory)(void *);
} reiny_exec_factory_v1;

uint32_t reiny_exec_abi_version(void);
reiny_exec_status reiny_exec_last_error_copy(uint8_t *, size_t, size_t *);
reiny_exec_status reiny_exec_release(reiny_exec_handle);
reiny_exec_status reiny_exec_clone(reiny_exec_handle, reiny_exec_handle *);
reiny_exec_status reiny_exec_registry_new(reiny_exec_handle *);
reiny_exec_status reiny_exec_register(reiny_exec_handle, reiny_exec_str factory,
    reiny_exec_str revision, const reiny_exec_factory_v1 *);
/*
 * Interpreter-safe factory release: no callback runs at native finalization.
 * token_new creates an owned, initially unbound atomic notification. A successful
 * register_notified binds it exactly once; failure leaves it unchanged. The table
 * MUST have release_factory=NULL. factory_released returns 1 only when the last
 * native factory owner is gone, so no callback can access factory_data again.
 * The caller then destroys factory_data/callback roots on its own thread.
 * A token alone never retains the factory; releasing it loses observation only.
 *
 * Python/ctypes finalizers cannot satisfy a nonblocking native callback contract:
 * even an empty Python body acquires the GIL. Python adapters must use this
 * notification path, keep callback roots alive while pending, and collect only
 * after observing release. Keep the native ABI library loaded for process
 * lifetime because its shared runtime is process-scoped.
 */
reiny_exec_status reiny_exec_factory_release_token_new(reiny_exec_handle *);
reiny_exec_status reiny_exec_register_notified(reiny_exec_handle, reiny_exec_str factory,
    reiny_exec_str revision, const reiny_exec_factory_v1 *, reiny_exec_handle token);
reiny_exec_status reiny_exec_factory_released(reiny_exec_handle token, uint32_t *);
reiny_exec_status reiny_exec_registry_seal(reiny_exec_handle);
reiny_exec_status reiny_exec_schema_new(reiny_exec_str protobuf_name,
    uint16_t fingerprint_version, uint64_t fingerprint, reiny_exec_handle *);
reiny_exec_status reiny_exec_error_set(reiny_exec_handle sink, int32_t code, reiny_exec_str);
reiny_exec_status reiny_exec_buffer_copy(reiny_exec_handle, uint8_t *, size_t, size_t *);

/*
 * Strict JSON graph: duplicate and unknown fields are rejected, including in
 * the versioned owner-resolved bindings. No code runs during import.
 * {
 *   "version":1, "domain":"app",
 *   "startup_timeout_ns":5000000000, "stop_timeout_ns":5000000000,
 *   "instances":[{
 *     "factory_id":"registered-name", "bindings":{...ModuleBindings v3...},
 *     "config":null, "config_origin":"/absolute/config/directory"
 *   }]
 * }
 * config/config_origin may be omitted or null. Graphs are immutable/reusable.
 * Schema identity grants no authority. The closed graph uses exact named ports,
 * independent request/response fingerprints, policies and source addresses.
 */
reiny_exec_status reiny_exec_graph_import(reiny_exec_bytes json, reiny_exec_handle *);
reiny_exec_status reiny_exec_host_launch_local(reiny_exec_handle registry,
    reiny_exec_handle graph, reiny_exec_handle *);
reiny_exec_status reiny_exec_host_request_stop(reiny_exec_handle);
reiny_exec_status reiny_exec_host_wait_running(reiny_exec_handle, uint64_t timeout_ns);
reiny_exec_status reiny_exec_host_wait_joined(reiny_exec_handle, uint64_t timeout_ns);
reiny_exec_status reiny_exec_host_phase(reiny_exec_handle, uint32_t *);
reiny_exec_status reiny_exec_host_snapshot(reiny_exec_handle, reiny_exec_handle *json_buffer);
reiny_exec_status reiny_exec_host_subscribe(reiny_exec_handle, reiny_exec_handle *observer);
/* Terminal wait returns OK with a retained outcome document, including FAILED
 * and UNRESPONSIVE. Inspect phase, primary, cleanup, unjoined separately. */
reiny_exec_status reiny_exec_host_wait_terminal(reiny_exec_handle, uint64_t timeout_ns,
    reiny_exec_handle *json_buffer);
/* Subscribe before the triggering action. Updates may coalesce. */
reiny_exec_status reiny_exec_observer_next(reiny_exec_handle, uint64_t timeout_ns,
    reiny_exec_handle *json_buffer);

reiny_exec_status reiny_exec_context_info(reiny_exec_handle, reiny_exec_handle *json_buffer);
reiny_exec_status reiny_exec_context_is_stopping(reiny_exec_handle, uint32_t *);
reiny_exec_status reiny_exec_context_wait_stop(reiny_exec_handle, uint64_t timeout_ns);
reiny_exec_status reiny_exec_context_open_stream(reiny_exec_handle, reiny_exec_str name,
    reiny_exec_handle schema, uint32_t direction, reiny_exec_handle *);
reiny_exec_status reiny_exec_context_open_rpc(reiny_exec_handle, reiny_exec_str name,
    reiny_exec_handle request_schema, reiny_exec_handle response_schema,
    uint32_t direction, reiny_exec_handle *);
reiny_exec_status reiny_exec_context_watch_input_ready(reiny_exec_handle, reiny_exec_str,
    reiny_exec_handle *);
reiny_exec_status reiny_exec_presence_next(reiny_exec_handle, uint64_t timeout_ns,
    reiny_exec_handle *json_buffer);
reiny_exec_status reiny_exec_publisher_send(reiny_exec_handle, reiny_exec_bytes);
reiny_exec_status reiny_exec_subscription_receive(reiny_exec_handle, uint64_t timeout_ns,
    reiny_exec_handle *message);
reiny_exec_status reiny_exec_subscription_stats(reiny_exec_handle, uint64_t timeout_ns,
    reiny_exec_handle *json_buffer);
reiny_exec_status reiny_exec_message_copy(reiny_exec_handle, uint32_t field,
    uint8_t *, size_t, size_t *);

/* The result operation is reserved before dispatch. Timeout/last-observer
 * release abandons observation, not server execution; never auto-retry.
 * All timeout_ns budgets must be positive. Waiting timeouts do not cancel.
 * Operation results remain retained for multiple independent waiters.
 * Raw protobuf bytes are opaque: fingerprint equality does not prove decoding. */
reiny_exec_status reiny_exec_rpc_call_start(reiny_exec_handle, reiny_exec_bytes,
    uint64_t timeout_ns, reiny_exec_handle *operation);
reiny_exec_status reiny_exec_operation_wait(reiny_exec_handle, uint64_t timeout_ns,
    reiny_exec_handle *result_buffer);
reiny_exec_status reiny_exec_rpc_server_receive(reiny_exec_handle, uint64_t timeout_ns,
    reiny_exec_handle *request);
reiny_exec_status reiny_exec_request_copy(reiny_exec_handle, uint8_t *, size_t, size_t *);
/* error_reply=0: opaque response bytes; error_reply=1: UTF-8 application error.
 * The first reply atomically consumes the request; subsequent replies return
 * ALREADY_COMPLETED. Releasing the last unanswered claim finalizes NoReply. */
reiny_exec_status reiny_exec_request_reply(reiny_exec_handle, reiny_exec_bytes,
    uint32_t error_reply);

/* App-owned executable entry, NOT deployment spawn/reap ownership.
 * Reads one private framed bootstrap from inherited stdin, then uses the real
 * revisioned ProcessRegistry/ProcessHost protocol. The owner must close stdin;
 * reading an arbitrary inherited pipe has no timeout. The caller MUST exit the
 * executable after this function returns, even on error or UNRESPONSIVE.
 * With Zenoh disabled, transport preparation rejects unsupported assignments.
 */
reiny_exec_status reiny_exec_process_host_run(reiny_exec_handle registry,
    uint64_t startup_ns, uint64_t stop_ns, uint64_t frame_ns, size_t max_instances,
    reiny_exec_handle *json_buffer);

#ifdef __cplusplus
}
#endif
#endif
