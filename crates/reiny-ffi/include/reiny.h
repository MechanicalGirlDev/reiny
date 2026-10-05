#ifndef REINY_H
#define REINY_H
#include <stddef.h>
#include <stdint.h>

#if defined(_WIN32)
#define REINY_API __declspec(dllimport)
#else
#define REINY_API
#endif
#ifdef __cplusplus
extern "C" {
#endif

/* Additive ergonomic bridge to the UniFFI facade; not UniFFI's generated ABI.
 * Every function returns 0 on success, -1 on error. Errors are UTF-8, NUL
 * terminated, thread-local, and valid until the next call on that thread.
 * reiny_last_error itself does not clear the error.
 *
 * Handles are nonzero, typed, never reused, and owned by the caller.
 * Release each successful output exactly once using reiny_release.
 * Invalid, released, and wrong-type handles are errors, never pointers.
 * Calls may be concurrent. A call retains its object while executing.
 *
 * Input strings are borrowed NUL-terminated UTF-8 for the duration of a call.
 * Optional strings use NULL for absent. Required strings cannot be NULL.
 * Byte inputs are borrowed for the call: NULL is allowed only with size 0.
 * All output pointers must be writable, aligned, and non-NULL.
 * Outputs are only changed on success; receive returns handle 0 on timeout.
 *
 * Buffers are owned handles. Their data is read-only and NOT NUL terminated.
 * Pointers from buffer_data remain valid until that buffer is released.
 * Do not free them with free/delete or release concurrently with a read.
 * Message and list getters create independent owned buffer handles.
 * Schema/timestamp has flags are 0 or 1, with value 0 when absent.
 * Panics become errors in unwind builds; panic=abort cannot be caught.
 */
typedef uint64_t reiny_handle;
REINY_API const char *reiny_last_error(void);
REINY_API int32_t reiny_release(reiny_handle handle);
REINY_API int32_t reiny_local_bus_new(reiny_handle *out);
REINY_API int32_t reiny_local_bus_connect(reiny_handle bus, const char *id, const char *domain, reiny_handle *out);
REINY_API int32_t reiny_session_open(const char *id, const char *domain, const char *zenoh_config, reiny_handle *out);
REINY_API int32_t reiny_session_publisher(reiny_handle session, const char *topic, uint8_t has_schema, uint64_t schema, reiny_handle *out);
REINY_API int32_t reiny_session_subscriber(reiny_handle session, const char *topic, const char *source, reiny_handle *out);
REINY_API int32_t reiny_session_publishers(reiny_handle session, const char *topic, uint64_t timeout_ms, reiny_handle *out);
REINY_API int32_t reiny_session_shutdown(reiny_handle session);
REINY_API int32_t reiny_publisher_send(reiny_handle publisher, const uint8_t *data, size_t size);
REINY_API int32_t reiny_subscription_receive(reiny_handle subscription, uint64_t timeout_ms, reiny_handle *out);
REINY_API int32_t reiny_message_payload(reiny_handle message, reiny_handle *out);
REINY_API int32_t reiny_message_source(reiny_handle message, reiny_handle *out);
REINY_API int32_t reiny_message_schema(reiny_handle message, uint8_t *has, uint64_t *value);
REINY_API int32_t reiny_message_timestamp(reiny_handle message, uint8_t *has, uint64_t *value);
REINY_API int32_t reiny_list_len(reiny_handle list, size_t *out);
REINY_API int32_t reiny_list_get(reiny_handle list, size_t index, reiny_handle *out);
REINY_API int32_t reiny_buffer_len(reiny_handle buffer, size_t *out);
REINY_API int32_t reiny_buffer_data(reiny_handle buffer, const uint8_t **out);

#ifdef __cplusplus
}
#endif
#endif
