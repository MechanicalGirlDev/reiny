// Included by the C++ step client: registered graph fixture and admission.
static std::string id(const rc::Handle& handle) { return std::to_string(handle.get()); }
static rs::Request request(const rc::Schema& schema, unsigned n) {
    auto value = rs::Value::create(schema, number_bytes(n));
    auto result = rs::Request::import(
        "{\"version\":1,\"dt\":{\"secs\":0,\"nanos\":7},\"inputs\":["
        "{\"instance\":\"a\",\"port\":\"x\",\"value\":" + id(value) + "}]}");
    // Import copies both encoded bytes and schema. No source-handle lease.
    value.close();
    return result;
}
struct Fixture {
    rc::Registry registry;
    std::shared_ptr<rc::Schema> schema =
        std::make_shared<rc::Schema>(rc::Schema::create("test.Number", 2, 42));
    std::shared_ptr<rc::Schema> wrong =
        std::make_shared<rc::Schema>(rc::Schema::create("test.Other", 2, 42));
    std::shared_ptr<State> a = std::make_shared<State>(), b = std::make_shared<State>();
    std::shared_ptr<std::atomic<unsigned>> released = std::make_shared<std::atomic<unsigned>>(0);
    unsigned expected_released = 2;
    rs::Launch execution;
    rs::Request input;

    Fixture(Mode mode = Mode::normal, bool block_destroy = false, bool validate = false) {
        a->first = true;
        a->mode = mode;
        a->block_destroy = block_destroy;
        for (auto& state : {a, b}) { state->schema = schema; state->wrong = wrong; }
        auto sa = rs::Schema::import("{\"version\":1,\"inputs\":{\"x\":" + id(*schema)
            + ",\"prev\":" + id(*schema) + "},\"outputs\":{\"out\":" + id(*schema) + "}}");
        auto sb = rs::Schema::import("{\"version\":1,\"inputs\":{\"x\":" + id(*schema)
            + "},\"outputs\":{\"out\":" + id(*schema) + "}}");
        for (const auto& entry : {std::make_pair("a", a), std::make_pair("b", b)}) {
            auto capture = std::make_shared<FactoryCapture>();
            capture->released = released;
            rs::register_factory(registry, entry.first, "cpp-step-v1",
                entry.first == std::string_view("a") ? sa : sb,
                [state = entry.second, capture = std::move(capture)](rc::ContextView context) {
                    return std::make_unique<Node>(state, context);
                });
        }
        if (validate) {
            ++expected_released;
            auto capture = std::make_shared<FactoryCapture>();
            capture->released = released;
            expect(REINY_EXEC_INVALID_STATE, [&] {
                rs::register_factory(registry, "a", "duplicate", sa,
                    [capture = std::move(capture)](rc::ContextView) {
                        return std::unique_ptr<rs::Component>{};
                    });
            });
            assert(*released == 1); // Failed registration transferred no capture.
        }
        registry.seal();
        auto seed = rs::Value::create(*schema, number_bytes(10));
        auto graph = rs::Graph::import("{\"version\":1,\"nodes\":{\"a\":" + id(sa)
            + ",\"b\":" + id(sb) + "},\"routes\":["
            "{\"instance\":\"a\",\"port\":\"x\",\"kind\":\"external\"},"
            "{\"instance\":\"a\",\"port\":\"prev\",\"kind\":\"previous_tick\","
            "\"source\":{\"instance\":\"b\",\"port\":\"out\"},\"initial\":" + id(seed) + "},"
            "{\"instance\":\"b\",\"port\":\"x\",\"kind\":\"same_tick\","
            "\"source\":{\"instance\":\"a\",\"port\":\"out\"}}]}");
        auto instances = rc::Graph::import(
            "{\"version\":1,\"domain\":\"cpp-step\",\"startup_timeout_ns\":5000000000,"
            "\"stop_timeout_ns\":30000000,\"instances\":["
            "{\"factory_id\":\"a\",\"bindings\":{\"version\":3,\"namespace\":\"a\","
            "\"endpoint_namespace\":\"a\",\"inputs\":{},\"outputs\":{}}},"
            "{\"factory_id\":\"b\",\"bindings\":{\"version\":3,\"namespace\":\"b\","
            "\"endpoint_namespace\":\"b\",\"inputs\":{},\"outputs\":{}}}]}");
        if (validate) {
            expect(REINY_EXEC_UNSUPPORTED, [&] { rc::Host::launch_local(registry, instances); });
            auto invalid = rs::Graph::import("{\"version\":1,\"nodes\":{\"a\":" + id(sb)
                + ",\"b\":" + id(sb) + "},\"routes\":[]}");
            expect(REINY_EXEC_FAILURE, [&] {
                rs::launch_local(registry, instances, invalid, {origin, 0});
            });
            assert(a->created == 0 && b->created == 0);
            auto clone = graph.clone();
            graph = std::move(clone);
            auto definition = sa.clone();
            assert(definition.get() != sa.get());
        }
        execution = rs::launch_local(registry, instances, graph, {origin, 0});
        sa.close(); sb.close(); graph.close(); instances.close(); seed.close();
        input = request(*schema, 1);
        if (mode != Mode::prepare_exception) {
            execution.host.wait_running(timeout);
            assert(a->lane != std::this_thread::get_id() && a->lane != b->lane);
        }
        registry.close(); // Native execution retains factory/adapter ownership.
    }
    void finish() {
        execution.host.request_stop();
        execution.host.wait_terminal(timeout);
        execution.host.wait_joined(timeout);
        assert(a->stopped == 1 && a->destroyed == 1);
        // Preparation failure may prevent the other instance from being created.
        if (b->created) assert(b->stopped == 1 && b->destroyed == 1);
        a->retained.close(); b->retained.close();
        a->context.close(); b->context.close();
        execution.host.close(); execution.scheduler.close();
        input.close();
        assert(*released == expected_released);
    }
};
static rs::Run admitted(Fixture& fixture) {
    auto attempt = fixture.execution.scheduler.try_step(fixture.input, timeout);
    assert(attempt.status == rs::Admission::accepted && attempt.run);
    return std::move(*attempt.run);
}
