#define _POSIX_C_SOURCE 200809L
#include "reiny_component.h"
#include "reiny.h"
#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define NS UINT64_C(5000000000)
#define CHECK(call) do { \
    int32_t code_ = (call); \
    if (code_ != REINY_EXEC_OK) { \
        uint8_t error_[2048]; size_t length_; \
        reiny_exec_last_error_copy(error_, sizeof(error_), &length_); \
        fprintf(stderr, "%s:%d: %s returned %d: %s\n", __FILE__, __LINE__, #call, code_, error_); \
        abort(); \
    } \
} while (0)

static reiny_exec_bytes text(const char *value) {
    return (reiny_exec_bytes){(const uint8_t *)value, strlen(value)};
}
static reiny_exec_bytes bytes(const uint8_t *value, size_t len) {
    return (reiny_exec_bytes){value, len};
}

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t changed;
    int role, entered, gate, done, stopped, destroyed, released;
    int finalizer_entered, finalizer_gate;
    pthread_t lane;
    reiny_exec_handle stream, rpc, context, borrowed, presence;
} state;

static void init(state *s, int role) {
    memset(s, 0, sizeof(*s));
    s->role = role;
    assert(pthread_mutex_init(&s->mutex, NULL) == 0);
    assert(pthread_cond_init(&s->changed, NULL) == 0);
}
static void finish(state *s) {
    assert(pthread_cond_destroy(&s->changed) == 0);
    assert(pthread_mutex_destroy(&s->mutex) == 0);
}
static void signal_value(state *s, int *value) {
    assert(pthread_mutex_lock(&s->mutex) == 0);
    *value = 1;
    assert(pthread_cond_broadcast(&s->changed) == 0);
    assert(pthread_mutex_unlock(&s->mutex) == 0);
}
static void await_value(state *s, const int *value) {
    struct timespec deadline;
    assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
    deadline.tv_sec += 10;
    assert(pthread_mutex_lock(&s->mutex) == 0);
    while (!*value) {
        assert(pthread_cond_timedwait(&s->changed, &s->mutex, &deadline) == 0);
    }
    assert(pthread_mutex_unlock(&s->mutex) == 0);
}
static void on_lane(state *s) { assert(pthread_equal(s->lane, pthread_self())); }

#include "component_callbacks.inc"

static uint64_t graph(const char *instances, uint64_t stop_ns, uint64_t startup_ns) {
    char json[8192];
    int n = snprintf(json, sizeof(json),
        "{\"version\":1,\"domain\":\"ffi\",\"startup_timeout_ns\":%llu,\"stop_timeout_ns\":%llu,\"instances\":[%s]}",
        (unsigned long long)startup_ns, (unsigned long long)stop_ns, instances);
    assert(n > 0 && (size_t)n < sizeof(json));
    uint64_t result;
    CHECK(reiny_exec_graph_import(text(json), &result));
    return result;
}
static uint32_t terminal(uint64_t host) {
    uint64_t outcome; uint32_t phase;
    CHECK(reiny_exec_host_wait_terminal(host, NS, &outcome));
    CHECK(reiny_exec_release(outcome));
    CHECK(reiny_exec_host_phase(host, &phase));
    return phase;
}

static void real_ports(void) {
    state producer, consumer;
    init(&producer, 0); init(&consumer, 1);
    uint64_t registry, host;
    CHECK(reiny_exec_registry_new(&registry));
    register_one(registry, "producer", &producer);
    register_one(registry, "consumer", &consumer);
    CHECK(reiny_exec_registry_seal(registry));
    uint64_t input = graph(
        "{\"factory_id\":\"producer\",\"bindings\":{\"version\":3,\"namespace\":\"producer\",\"endpoint_namespace\":\"producer\","
        "\"inputs\":{\"echo\":{\"contract\":{\"type\":\"fixture.Packet\",\"kind\":\"rpc\",\"response\":\"fixture.Response\"},\"sources\":[{\"namespace\":\"consumer\",\"port\":\"echo\"}]}},"
        "\"outputs\":{\"outgoing\":{\"contract\":{\"type\":\"fixture.Packet\"}}}}},"
        "{\"factory_id\":\"consumer\",\"bindings\":{\"version\":3,\"namespace\":\"consumer\",\"endpoint_namespace\":\"consumer\","
        "\"inputs\":{\"incoming\":{\"contract\":{\"type\":\"fixture.Packet\"},\"sources\":[{\"namespace\":\"producer\",\"port\":\"outgoing\"}]}},"
        "\"outputs\":{\"echo\":{\"contract\":{\"type\":\"fixture.Packet\",\"kind\":\"rpc\",\"response\":\"fixture.Response\"}}}}}",
        NS, NS);
    CHECK(reiny_exec_host_launch_local(registry, input, &host));
    CHECK(reiny_exec_release(input));
    CHECK(reiny_exec_release(registry));
    CHECK(reiny_exec_host_wait_running(host, NS));
    await_value(&producer, &producer.done);
    uint64_t observer;
    CHECK(reiny_exec_host_subscribe(host, &observer));
    CHECK(reiny_exec_host_request_stop(host));
    uint64_t update;
    CHECK(reiny_exec_observer_next(observer, NS, &update));
    CHECK(reiny_exec_release(update));
    assert(terminal(host) == REINY_EXEC_STOPPED);
    CHECK(reiny_exec_host_wait_joined(host, NS));
    assert(producer.destroyed == 1 && consumer.destroyed == 1);
    uint32_t stale = 99;
    assert(reiny_exec_context_is_stopping(producer.borrowed, &stale) == REINY_EXEC_INVALID_HANDLE);
    assert(stale == 99);
    CHECK(reiny_exec_context_is_stopping(producer.context, &stale));
    assert(stale == 1);
    CHECK(reiny_exec_release(producer.context));
    CHECK(reiny_exec_release(consumer.context));
    CHECK(reiny_exec_release(observer));
    CHECK(reiny_exec_release(host));
    await_value(&producer, &producer.released);
    await_value(&consumer, &consumer.released);
    finish(&producer); finish(&consumer);
}

#include "component_lifecycle.inc"

typedef struct { uint64_t handle; pthread_barrier_t *gate; int32_t result; } race;
static void *release_race(void *data) {
    race *r = data;
    int result = pthread_barrier_wait(r->gate);
    assert(result == 0 || result == PTHREAD_BARRIER_SERIAL_THREAD);
    r->result = reiny_exec_release(r->handle);
    return NULL;
}
static void invalid_and_racing_handles(void) {
    uint64_t out = UINT64_C(123456);
    assert(reiny_exec_schema_new((reiny_exec_bytes){NULL, 1}, 2, 0, &out) == REINY_EXEC_INVALID_ARGUMENT);
    const uint8_t invalid[] = {0xff};
    assert(reiny_exec_schema_new(bytes(invalid, 1), 2, 0, &out) == REINY_EXEC_INVALID_ARGUMENT);
    assert(reiny_exec_schema_new(text("fixture.Packet"), 1, 0, &out) == REINY_EXEC_SCHEMA_ERROR);
    assert(out == UINT64_C(123456));
    uint8_t diagnostic[512]; size_t required;
    CHECK(reiny_exec_last_error_copy(NULL, 0, &required));
    assert(required > 1 && required <= sizeof(diagnostic));
    CHECK(reiny_exec_last_error_copy(diagnostic, sizeof(diagnostic), &required));
    assert(diagnostic[required - 1] == 0);
    assert(reiny_exec_graph_import(text("{\"version\":1,\"version\":1}"), &out) == REINY_EXEC_INVALID_ARGUMENT);
    assert(reiny_exec_graph_import(text(
        "{\"version\":1,\"domain\":\"ffi\",\"startup_timeout_ns\":1,\"stop_timeout_ns\":1,\"instances\":["
        "{\"factory_id\":\"x\",\"config_origin\":\"/tmp/\\u0000\",\"bindings\":{\"version\":3,\"namespace\":\"x\",\"endpoint_namespace\":\"x\",\"inputs\":{},\"outputs\":{}}}]}"),
        &out) == REINY_EXEC_INVALID_ARGUMENT);
    CHECK(reiny_exec_schema_new(text("fixture.Packet"), 2, 0, &out));
    assert(reiny_exec_registry_seal(out) == REINY_EXEC_WRONG_KIND);
    pthread_barrier_t gate;
    assert(pthread_barrier_init(&gate, NULL, 2) == 0);
    race a = {out, &gate, 0}, b = {out, &gate, 0};
    pthread_t first, second;
    assert(pthread_create(&first, NULL, release_race, &a) == 0);
    assert(pthread_create(&second, NULL, release_race, &b) == 0);
    assert(pthread_join(first, NULL) == 0);
    assert(pthread_join(second, NULL) == 0);
    assert((a.result == 0 && b.result == REINY_EXEC_INVALID_HANDLE) ||
           (b.result == 0 && a.result == REINY_EXEC_INVALID_HANDLE));
    assert(pthread_barrier_destroy(&gate) == 0);
    assert(reiny_exec_release(out) == REINY_EXEC_INVALID_HANDLE);
}

int main(void) {
    assert(reiny_exec_abi_version() == 1);
    invalid_and_racing_handles();
    real_ports();
    interrupted_callback(2);
    interrupted_callback(3);
    interrupted_callback(4);
    release_during_callback();
    joined_includes_factory_finalization();
    notified_factory_has_no_foreign_finalizer();
    puts("C service ABI: named stream/RPC, errors, lifecycle, stale handles and cancellation passed");
    return 0;
}
