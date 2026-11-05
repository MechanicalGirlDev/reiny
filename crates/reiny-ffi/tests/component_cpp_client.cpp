#include "reiny_component.hpp"
#include "reiny.hpp" // Both adapters must coexist without name collisions.
#include <atomic>
#include <cassert>
#include <future>
#include <iostream>
#include <thread>
#include <type_traits>

namespace rc = reiny::component;
using namespace std::chrono_literals;
static constexpr auto timeout = 5s;
static const rc::Bytes payload{10, 3, 0, 255, 0};
static const rc::Bytes response{8, 7};

template<class Function>
void expect_status(int32_t expected, Function&& function) {
    try {
        function();
        assert(false && "expected executable ABI error");
    } catch (const rc::Error& error) {
        assert(error.status() == expected);
    }
}
template<class Future>
void ready(Future& future) {
    assert(future.wait_for(timeout) == std::future_status::ready);
    future.get();
}

struct State {
    std::promise<void> done, entered, gate;
    std::shared_future<void> release = gate.get_future().share();
    std::atomic<unsigned> stopped{0}, destroyed{0};
    rc::Context retained;
};
class Fixture : public rc::Component {
    std::shared_ptr<State> state_;
    int role_;
    std::thread::id lane_ = std::this_thread::get_id();
    rc::Publisher publisher_;
    rc::Subscription subscriber_;
    rc::RpcClient client_;
    rc::RpcServer server_;
    rc::Presence presence_;
    void on_lane() const { assert(lane_ == std::this_thread::get_id()); }
public:
    Fixture(std::shared_ptr<State> state, int role, rc::ContextView context)
        : state_(std::move(state)), role_(role) {
        state_->retained = context.retain();
        assert(!context.info().text().empty());
    }
    ~Fixture() noexcept override {
        on_lane();
        assert(state_->stopped == 1);
        ++state_->destroyed;
    }
    void prepare(rc::ContextView context) override {
        on_lane();
        if (role_ == 3) throw std::runtime_error("intentional C++ prepare failure");
        if (role_ == 4) throw 17; // Unknown exceptions must also stay inside C++.
        if (role_ == 2) return;
        auto packet = rc::Schema::create("fixture.Packet", 2, 42);
        auto reply = rc::Schema::create("fixture.Response", 2, 99);
        expect_status(REINY_EXEC_FAILURE, [&] { context.output("undeclared", packet); });
        if (role_ == 0) {
            publisher_ = context.output("outgoing", packet);
            client_ = context.rpc_client("echo", packet, reply);
            expect_status(REINY_EXEC_INVALID_STATE, [&] { publisher_.send(payload); });
        } else {
            subscriber_ = context.input("incoming", packet);
            server_ = context.rpc_server("echo", packet, reply);
            presence_ = context.watch_input_ready("incoming");
        }
    }
    void run(rc::ContextView context) override {
        on_lane();
        if (role_ == 0) {
            publisher_.send(payload);
            auto operation = client_.call(payload, timeout);
            assert(operation.wait(timeout).bytes() == response);
            // Results are retained; independent waits do not consume them.
            assert(operation.wait(timeout).bytes() == response);
            expect_status(REINY_EXEC_REMOTE_ERROR, [&] { client_.call(payload, timeout).wait(timeout); });
            expect_status(REINY_EXEC_NO_REPLY, [&] { client_.call({}, timeout).wait(timeout); });
            state_->done.set_value();
        } else if (role_ == 1) {
            assert(!presence_.next(timeout).text().empty());
            auto message = subscriber_.receive(timeout);
            assert(message.payload() == payload);
            assert(message.source() == "producer");
            assert(message.source_port() == "outgoing");
            assert(!message.info_json().empty());
            assert(!subscriber_.stats(timeout).text().empty());
            auto first = server_.receive(timeout);
            assert(first.payload() == payload);
            first.reply(response);
            expect_status(REINY_EXEC_ALREADY_COMPLETED, [&] { first.reply(response); });
            auto second = server_.receive(timeout);
            assert(second.payload() == payload);
            second.reply_error("application failure");
            { auto third = server_.receive(timeout); assert(third.payload().empty()); }
            state_->done.set_value();
        } else {
            assert(role_ == 2);
            state_->entered.set_value();
            ready(state_->release);
        }
        context.wait_stop(timeout);
    }
    void stop(rc::ContextView context, uint32_t) override {
        on_lane();
        assert(context.stopping());
        if (role_ == 0)
            expect_status(REINY_EXEC_CLOSED, [&] { publisher_.send(payload); });
        assert(state_->stopped++ == 0);
    }
};

static rc::Graph graph(std::string_view instances, uint64_t stop_ns = 5000000000ULL) {
    return rc::Graph::import(
        "{\"version\":1,\"domain\":\"cpp\",\"startup_timeout_ns\":5000000000,\"stop_timeout_ns\":"
        + std::to_string(stop_ns) + ",\"instances\":[" + std::string(instances) + "]}");
}
static void add(rc::Registry& registry, std::string_view name,
                const std::shared_ptr<State>& state, int role) {
    registry.add(name, "cpp-fixture-v1", [state, role](rc::ContextView context) {
        return std::make_unique<Fixture>(state, role, context);
    });
}
static constexpr auto empty_instance =
    "{\"factory_id\":\"fixture\",\"bindings\":{\"version\":3,\"namespace\":\"fixture\","
    "\"endpoint_namespace\":\"fixture\",\"inputs\":{},\"outputs\":{}}}";

static void real_ports() {
    auto producer = std::make_shared<State>(), consumer = std::make_shared<State>();
    auto sent = producer->done.get_future(), received = consumer->done.get_future();
    rc::Registry registry;
    add(registry, "producer", producer, 0);
    add(registry, "consumer", consumer, 1);
    // Failure must destroy the untransferred C++ callable and its captures.
    auto marker = std::make_shared<int>(0);
    std::weak_ptr<int> weak = marker;
    expect_status(REINY_EXEC_INVALID_STATE, [&] {
        registry.add("producer", "duplicate", [capture = std::move(marker)](rc::ContextView) {
            return std::unique_ptr<rc::Component>{};
        });
    });
    assert(weak.expired());
    registry.seal();
    auto input = graph(
        "{\"factory_id\":\"producer\",\"bindings\":{\"version\":3,\"namespace\":\"producer\",\"endpoint_namespace\":\"producer\","
        "\"inputs\":{\"echo\":{\"contract\":{\"type\":\"fixture.Packet\",\"kind\":\"rpc\",\"response\":\"fixture.Response\"},\"sources\":[{\"namespace\":\"consumer\",\"port\":\"echo\"}]}},"
        "\"outputs\":{\"outgoing\":{\"contract\":{\"type\":\"fixture.Packet\"}}}}},"
        "{\"factory_id\":\"consumer\",\"bindings\":{\"version\":3,\"namespace\":\"consumer\",\"endpoint_namespace\":\"consumer\","
        "\"inputs\":{\"incoming\":{\"contract\":{\"type\":\"fixture.Packet\"},\"sources\":[{\"namespace\":\"producer\",\"port\":\"outgoing\"}]}},"
        "\"outputs\":{\"echo\":{\"contract\":{\"type\":\"fixture.Packet\",\"kind\":\"rpc\",\"response\":\"fixture.Response\"}}}}}");
    auto host = rc::Host::launch_local(registry, input);
    // Execution retains the registry/factory state independently of public IDs.
    registry.close();
    input.close();
    host.wait_running(timeout);
    ready(sent);
    ready(received);
    auto observer = host.subscribe(); // Register before triggering stop.
    host.request_stop();
    assert(!observer.next(timeout).text().empty());
    assert(!host.wait_terminal(timeout).text().empty());
    assert(host.phase() == REINY_EXEC_STOPPED);
    host.wait_joined(timeout);
    assert(producer->destroyed == 1 && consumer->destroyed == 1);
    assert(producer->retained.view().stopping());
    producer->retained.close();
    consumer->retained.close();
}

static void interrupted_and_throwing() {
    for (int role : {2, 3, 4}) {
        auto state = std::make_shared<State>();
        auto entered = state->entered.get_future();
        rc::Registry registry;
        add(registry, "fixture", state, role);
        registry.seal();
        auto host = rc::Host::launch_local(registry, graph(empty_instance, 20000000));
        if (role == 2) {
            host.wait_running(timeout);
            ready(entered);
            host.request_stop();
            host.wait_terminal(timeout);
            assert(host.phase() == REINY_EXEC_UNRESPONSIVE);
            assert(state->stopped == 0 && state->destroyed == 0);
            expect_status(REINY_EXEC_TIMEOUT, [&] { host.wait_joined(1ms); });
            state->gate.set_value(); // Real callback completion, not future cancellation.
        } else {
            host.wait_terminal(timeout);
            assert(host.phase() == REINY_EXEC_FAILED);
        }
        host.wait_joined(timeout);
        assert(state->stopped == 1 && state->destroyed == 1);
        state->retained.close();
    }
    rc::Registry registry;
    registry.add("fixture", "cpp-v1", [](rc::ContextView) -> std::unique_ptr<rc::Component> {
        throw std::runtime_error("factory exception");
    });
    registry.seal();
    auto host = rc::Host::launch_local(registry, graph(empty_instance));
    host.wait_terminal(timeout);
    assert(host.phase() == REINY_EXEC_FAILED);
    host.wait_joined(timeout);
}

int main() {
    static_assert(!std::is_copy_constructible<rc::Host>::value, "ownership must not be copied");
    static_assert(std::is_nothrow_move_constructible<rc::Host>::value, "moves transfer ownership");
    real_ports();
    interrupted_and_throwing();
    std::cout << "C++ component ABI: RAII, named stream/RPC, exceptions, stop and join passed\n";
}
