/* Included by the C service client: stop, finalization and late join scenarios. */
static void release_during_callback(void) {
    state s; init(&s, 2);
    uint64_t registry, host;
    CHECK(reiny_exec_registry_new(&registry));
    register_one(registry, "blocked", &s);
    CHECK(reiny_exec_registry_seal(registry));
    uint64_t input = graph(
        "{\"factory_id\":\"blocked\",\"bindings\":{\"version\":3,\"namespace\":\"blocked\",\"endpoint_namespace\":\"blocked\",\"inputs\":{},\"outputs\":{}}}",
        UINT64_C(20000000), NS);
    CHECK(reiny_exec_host_launch_local(registry, input, &host));
    CHECK(reiny_exec_release(input));
    CHECK(reiny_exec_host_wait_running(host, NS));
    await_value(&s, &s.entered);
    CHECK(reiny_exec_release(registry));
    CHECK(reiny_exec_release(host));
    CHECK(reiny_exec_context_wait_stop(s.context, NS));
    assert(s.stopped == 0 && s.destroyed == 0 && s.released == 0);
    signal_value(&s, &s.gate);
    await_value(&s, &s.destroyed);
    await_value(&s, &s.released);
    CHECK(reiny_exec_release(s.context));
    finish(&s);
}

static void joined_includes_factory_finalization(void) {
    state s; init(&s, 6);
    uint64_t registry, host;
    CHECK(reiny_exec_registry_new(&registry));
    register_one(registry, "finalizer", &s);
    CHECK(reiny_exec_registry_seal(registry));
    uint64_t input = graph(
        "{\"factory_id\":\"finalizer\",\"bindings\":{\"version\":3,\"namespace\":\"finalizer\",\"endpoint_namespace\":\"finalizer\",\"inputs\":{},\"outputs\":{}}}",
        UINT64_C(20000000), NS);
    CHECK(reiny_exec_host_launch_local(registry, input, &host));
    CHECK(reiny_exec_release(input));
    CHECK(reiny_exec_host_wait_running(host, NS));
    await_value(&s, &s.entered);
    CHECK(reiny_exec_release(registry));
    CHECK(reiny_exec_host_request_stop(host));
    assert(terminal(host) == REINY_EXEC_UNRESPONSIVE);
    assert(s.destroyed == 0 && s.released == 0);
    assert(reiny_exec_host_wait_joined(host, UINT64_C(1000000)) == REINY_EXEC_TIMEOUT);
    signal_value(&s, &s.gate);
    await_value(&s, &s.finalizer_entered);
    assert(s.destroyed == 1 && s.released == 0);
    assert(reiny_exec_host_wait_joined(host, UINT64_C(1000000)) == REINY_EXEC_TIMEOUT);
    signal_value(&s, &s.finalizer_gate);
    CHECK(reiny_exec_host_wait_joined(host, NS));
    assert(s.destroyed == 1 && s.released == 1);
    CHECK(reiny_exec_release(s.context));
    CHECK(reiny_exec_release(host));
    finish(&s);
}

static void notified_factory_has_no_foreign_finalizer(void) {
    state s; init(&s, 2);
    uint64_t registry, token, host;
    uint32_t released = 99;
    CHECK(reiny_exec_registry_new(&registry));
    CHECK(reiny_exec_factory_release_token_new(&token));
    reiny_exec_factory_v1 callbacks = table(&s);
    assert(reiny_exec_register_notified(registry, text("notified"), text("1"),
        &callbacks, token) == REINY_EXEC_INVALID_ARGUMENT);
    CHECK(reiny_exec_factory_released(token, &released));
    assert(released == 0);
    callbacks.release_factory = NULL;
    CHECK(reiny_exec_register_notified(registry, text("notified"), text("1"), &callbacks, token));
    assert(reiny_exec_register_notified(registry, text("other"), text("1"),
        &callbacks, token) == REINY_EXEC_INVALID_STATE);
    uint64_t observer;
    CHECK(reiny_exec_clone(token, &observer));
    CHECK(reiny_exec_release(token));
    released = 99;
    assert(reiny_exec_factory_released(token, &released) == REINY_EXEC_INVALID_HANDLE);
    assert(released == 99);
    token = observer;
    CHECK(reiny_exec_registry_seal(registry));
    uint64_t input = graph(
        "{\"factory_id\":\"notified\",\"bindings\":{\"version\":3,\"namespace\":\"notified\",\"endpoint_namespace\":\"notified\",\"inputs\":{},\"outputs\":{}}}",
        UINT64_C(20000000), NS);
    CHECK(reiny_exec_host_launch_local(registry, input, &host));
    CHECK(reiny_exec_release(input));
    CHECK(reiny_exec_host_wait_running(host, NS));
    await_value(&s, &s.entered);
    CHECK(reiny_exec_release(registry));
    CHECK(reiny_exec_host_request_stop(host));
    assert(terminal(host) == REINY_EXEC_UNRESPONSIVE);
    CHECK(reiny_exec_factory_released(token, &released));
    assert(released == 0 && s.destroyed == 0);
    signal_value(&s, &s.gate);
    CHECK(reiny_exec_host_wait_joined(host, NS));
    CHECK(reiny_exec_factory_released(token, &released));
    assert(released == 1 && s.destroyed == 1 && s.released == 0);
    CHECK(reiny_exec_release(s.context));
    CHECK(reiny_exec_release(host));
    CHECK(reiny_exec_release(token));
    finish(&s);
}

static void interrupted_callback(int role) {
    state s; init(&s, role);
    uint64_t registry, host;
    CHECK(reiny_exec_registry_new(&registry));
    register_one(registry, "blocked", &s);
    CHECK(reiny_exec_registry_seal(registry));
    uint64_t input = graph(
        "{\"factory_id\":\"blocked\",\"bindings\":{\"version\":3,\"namespace\":\"blocked\",\"endpoint_namespace\":\"blocked\",\"inputs\":{},\"outputs\":{}}}",
        UINT64_C(20000000), role == 3 ? UINT64_C(20000000) : NS);
    CHECK(reiny_exec_host_launch_local(registry, input, &host));
    CHECK(reiny_exec_release(input));
    if (role == 2) CHECK(reiny_exec_host_wait_running(host, NS));
    if (role != 4) {
        await_value(&s, &s.entered);
        CHECK(reiny_exec_host_request_stop(host));
        assert(terminal(host) == REINY_EXEC_UNRESPONSIVE);
        assert(s.stopped == 0 && s.destroyed == 0);
        assert(reiny_exec_host_wait_joined(host, UINT64_C(1000000)) == REINY_EXEC_TIMEOUT);
        signal_value(&s, &s.gate);
    } else {
        assert(terminal(host) == REINY_EXEC_FAILED);
    }
    CHECK(reiny_exec_host_wait_joined(host, NS));
    assert(s.stopped == 1 && s.destroyed == 1);
    CHECK(reiny_exec_release(s.context));
    CHECK(reiny_exec_release(host));
    CHECK(reiny_exec_release(registry));
    await_value(&s, &s.released);
    finish(&s);
}
