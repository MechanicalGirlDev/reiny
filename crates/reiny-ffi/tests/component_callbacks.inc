/* Included by the C service client: serialized native callback fixture. */
static int32_t create(void *data, uint64_t context, void **instance, uint64_t sink) {
    (void)sink;
    state *s = data;
    s->lane = pthread_self();
    s->borrowed = context;
    assert(reiny_exec_release(context) == REINY_EXEC_INVALID_HANDLE);
    assert(reiny_release(context) != 0);
    CHECK(reiny_exec_clone(context, &s->context));
    uint64_t info;
    CHECK(reiny_exec_context_info(context, &info));
    CHECK(reiny_exec_release(info));
    *instance = s;
    return 0;
}

static int32_t prepare(void *instance, uint64_t context, uint64_t sink) {
    state *s = instance;
    on_lane(s);
    if (s->role == 3) {
        signal_value(s, &s->entered);
        await_value(s, &s->gate);
        return 0;
    }
    if (s->role == 4) {
        CHECK(reiny_exec_error_set(sink, REINY_EXEC_CALLBACK_ERROR, text("intentional preparation failure")));
        return REINY_EXEC_CALLBACK_ERROR;
    }
    if (s->role >= 2) return 0;
    uint64_t schema = 0, response = 0, wrong = 0, untouched = UINT64_C(123456);
    CHECK(reiny_exec_schema_new(text("fixture.Packet"), 2, 42, &schema));
    CHECK(reiny_exec_schema_new(text("fixture.Response"), 2, 99, &response));
    CHECK(reiny_exec_schema_new(text("fixture.Wrong"), 2, 42, &wrong));
    const char *stream = s->role == 0 ? "outgoing" : "incoming";
    uint32_t direction = s->role == 0 ? REINY_EXEC_OUTPUT : REINY_EXEC_INPUT;
    assert(reiny_exec_context_open_stream(context, text("undeclared"), schema, direction, &untouched) != 0);
    assert(untouched == UINT64_C(123456));
    assert(reiny_exec_context_open_stream(context, text(stream), wrong, direction, &untouched) != 0);
    assert(untouched == UINT64_C(123456));
    CHECK(reiny_exec_context_open_stream(context, text(stream), schema, direction, &s->stream));
    CHECK(reiny_exec_context_open_rpc(context, text("echo"), schema, response,
        s->role == 0 ? REINY_EXEC_RPC_CLIENT : REINY_EXEC_RPC_SERVER, &s->rpc));
    if (s->role == 0) {
        assert(reiny_exec_publisher_send(s->stream, text("not before admission")) == REINY_EXEC_INVALID_STATE);
    } else {
        CHECK(reiny_exec_context_watch_input_ready(context, text("incoming"), &s->presence));
    }
    CHECK(reiny_exec_release(wrong));
    CHECK(reiny_exec_release(response));
    CHECK(reiny_exec_release(schema));
    return 0;
}

static void check_reply(uint64_t operation, int32_t expected) {
    uint64_t result = UINT64_C(123456);
    int32_t code = reiny_exec_operation_wait(operation, NS, &result);
    assert(code == expected);
    if (code == 0) {
        uint8_t payload[4] = {0}; size_t size = 0;
        CHECK(reiny_exec_buffer_copy(result, payload, sizeof(payload), &size));
        const uint8_t expected_bytes[] = {8, 7};
        assert(size == sizeof(expected_bytes));
        assert(memcmp(payload, expected_bytes, size) == 0);
        CHECK(reiny_exec_release(result));
    } else {
        assert(result == UINT64_C(123456));
    }
    CHECK(reiny_exec_release(operation));
}

static int32_t run(void *instance, uint64_t context, uint64_t sink) {
    (void)sink;
    state *s = instance;
    on_lane(s);
    if (s->role == 2 || s->role == 6) {
        signal_value(s, &s->entered);
        await_value(s, &s->gate);
    } else if (s->role == 0) {
        const uint8_t payload[] = {10, 3, 0, 255, 0};
        CHECK(reiny_exec_publisher_send(s->stream, bytes(payload, sizeof(payload))));
        for (unsigned i = 0; i != 3; ++i) {
            uint64_t operation;
            CHECK(reiny_exec_rpc_call_start(s->rpc, bytes(payload, sizeof(payload)), NS, &operation));
            check_reply(operation, i == 0 ? 0 : (i == 1 ? REINY_EXEC_REMOTE_ERROR : REINY_EXEC_NO_REPLY));
        }
        signal_value(s, &s->done);
    } else if (s->role == 1) {
        uint64_t event, message;
        CHECK(reiny_exec_presence_next(s->presence, NS, &event));
        CHECK(reiny_exec_release(event));
        CHECK(reiny_exec_subscription_receive(s->stream, NS, &message));
        uint8_t payload[16]; size_t size;
        CHECK(reiny_exec_message_copy(message, REINY_EXEC_PAYLOAD, payload, sizeof(payload), &size));
        const uint8_t expected[] = {10, 3, 0, 255, 0};
        assert(size == sizeof(expected) && memcmp(payload, expected, size) == 0);
        CHECK(reiny_exec_message_copy(message, REINY_EXEC_SOURCE, payload, sizeof(payload), &size));
        assert(strcmp((const char *)payload, "producer") == 0);
        CHECK(reiny_exec_message_copy(message, REINY_EXEC_SOURCE_PORT, payload, sizeof(payload), &size));
        assert(strcmp((const char *)payload, "outgoing") == 0);
        CHECK(reiny_exec_release(message));
        for (unsigned i = 0; i != 3; ++i) {
            uint64_t request;
            CHECK(reiny_exec_rpc_server_receive(s->rpc, NS, &request));
            CHECK(reiny_exec_request_copy(request, payload, sizeof(payload), &size));
            assert(size == sizeof(expected) && memcmp(payload, expected, size) == 0);
            if (i != 2) {
                const uint8_t response[] = {8, 7};
                CHECK(reiny_exec_request_reply(request, i == 0 ? bytes(response, sizeof(response)) : text("application failure"), i));
                assert(reiny_exec_request_reply(request, text("second reply"), 1) == REINY_EXEC_ALREADY_COMPLETED);
            }
            CHECK(reiny_exec_release(request));
        }
    } else {
        assert(!"unexpected run after failed/interrupted prepare");
    }
    CHECK(reiny_exec_context_wait_stop(context, NS));
    return 0;
}

static int32_t stop(void *instance, uint64_t context, uint32_t reason, uint64_t sink) {
    (void)reason; (void)sink;
    state *s = instance;
    on_lane(s);
    uint32_t stopping = 0;
    CHECK(reiny_exec_context_is_stopping(context, &stopping));
    assert(stopping == 1);
    if (s->role == 0) {
        assert(reiny_exec_publisher_send(s->stream, text("after stop")) == REINY_EXEC_CLOSED);
    }
    if (s->stream) CHECK(reiny_exec_release(s->stream));
    if (s->rpc) CHECK(reiny_exec_release(s->rpc));
    if (s->presence) CHECK(reiny_exec_release(s->presence));
    assert(s->stopped++ == 0);
    return 0;
}

static void destroy(void *instance) {
    state *s = instance;
    on_lane(s);
    assert(s->stopped == 1);
    assert(s->destroyed == 0);
    signal_value(s, &s->destroyed);
}
static void release_factory(void *data) {
    state *s = data;
    if (s->role == 6) {
        /* An adversarial finalizer exceeds the public nonblocking contract.
         * The test checks only that quiescence never certifies it early. */
        signal_value(s, &s->finalizer_entered);
        await_value(s, &s->finalizer_gate);
    }
    signal_value(s, &s->released);
}
static reiny_exec_factory_v1 table(state *s) {
    return (reiny_exec_factory_v1){
        sizeof(reiny_exec_factory_v1), 1, 0, REINY_EXEC_SERVICE, s,
        create, prepare, run, stop, destroy, release_factory
    };
}
static void register_one(uint64_t registry, const char *name, state *s) {
    reiny_exec_factory_v1 callbacks = table(s);
    CHECK(reiny_exec_register(registry, text(name), text("fixture-v1"), &callbacks));
}
