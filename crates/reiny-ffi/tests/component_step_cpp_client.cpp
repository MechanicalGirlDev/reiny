#include "reiny_component_step.hpp"
#include <atomic>
#include <cassert>
#include <cstdlib>
#include <fstream>
#include <future>
#include <iostream>
#include <thread>
#include <type_traits>

namespace rc = reiny::component;
namespace rs = reiny::component::step;
using namespace std::chrono_literals;
static constexpr auto timeout = 5s;
static constexpr uint64_t origin = 20000000000ULL;

template<class Function>
static void expect(int32_t status, Function&& function) {
    try {
        function();
        assert(false && "expected ABI error");
    } catch (const rc::Error& error) {
        assert(error.status() == status);
    }
}
template<class Future>
static void ready(Future& future) {
    assert(future.wait_for(timeout) == std::future_status::ready);
    future.get();
}
struct Gate {
    std::promise<void> entered, release;
    std::shared_future<void> permitted = release.get_future().share();
    void block() {
        entered.set_value();
        if (permitted.wait_for(timeout) != std::future_status::ready) std::abort();
        permitted.get();
    }
};
#include "component_step_cpp_callbacks.inc"
#include "component_step_cpp_fixture.inc"

static void multi_tick() {
    Fixture fixture(Mode::normal, false, true);
    auto& scheduler = fixture.execution.scheduler;
    auto wrong = request(*fixture.wrong, 1);
    auto rejected = scheduler.try_step(wrong, timeout);
    assert(rejected.status == rs::Admission::invalid_request && !rejected.run);
    rejected = scheduler.try_step(fixture.input, 0ns);
    assert(rejected.status == rs::Admission::invalid_timeout && !rejected.run);
    rejected = scheduler.try_step(fixture.input, -1ns);
    assert(rejected.status == rs::Admission::invalid_timeout && !rejected.run);
    auto cloned_request = fixture.input.clone();
    fixture.input = std::move(cloned_request);
    uint64_t sequence = 0;
    for (uint64_t index = 0; index != 3; ++index) {
        const auto before = scheduler.clock();
        assert(before.phase == rs::ClockPhase::ready && before.next_index == index);
        auto run = admitted(fixture);
        auto observer = run.clone();
        const auto run_id = observer.id();
        assert(run_id.epoch == before.epoch && run_id.tick_index == index);
        if (index) assert(run_id.sequence > sequence);
        sequence = run_id.sequence;
        run.close(); // A dropped observer does not cancel accepted work.
        auto result = observer.wait(timeout);
        auto retained = result.clone();
        result.close();
        assert(retained.kind() == rs::ResultKind::committed);
        const auto tick = retained.tick();
        assert(tick.epoch == before.epoch && tick.index == index);
        assert(tick.start.nanos == index * 7 && tick.end.nanos == (index + 1) * 7);
        auto output = retained.output("b", "out");
        auto output_clone = output.clone();
        output.close();
        assert(number(output_clone, *fixture.schema) == 12 + index * 2);
        expect(REINY_EXEC_SCHEMA_ERROR, [&] { output_clone.bytes(*fixture.wrong); });
        assert(number(fixture.a->retained.view().get("x"), *fixture.schema) == 1);
        auto next = scheduler.clock();
        assert(next.next_index == index + 1 && next.time.secs == origin);
        assert(next.time.nanos == (index + 1) * 7);
        assert(observer.wait(timeout).kind() == rs::ResultKind::committed);
        assert(observer.cancel() == rs::Cancellation::finished);
    }
    auto value = rs::Value::create(*fixture.schema, number_bytes(1));
    expect(REINY_EXEC_INVALID_HANDLE, [&] { fixture.a->expired_output->set("out", value); });
    fixture.finish();
}

#include "component_step_cpp_lifecycle.inc"

static void semantic_values() {
    auto schema = rc::Schema::create("test.Number", 2, 42);
    auto wrong = rc::Schema::create("test.Number", 2, 43);
    std::string assertions = "{\"units\":\"rad\"}";
    auto payload = number_bytes(42);
    auto value = rs::Value::create(schema, payload, assertions);
    assertions.clear();
    payload.clear();
    auto retained = value.clone();
    value.close();
    assert(number(retained, schema) == 42);
    expect(REINY_EXEC_SCHEMA_ERROR, [&] { retained.bytes(wrong); });
    auto map = retained.semantics();
    retained.close();
    assert(map.text() == "{\"units\":\"rad\"}");
}

int main() {
    semantic_values();
    static_assert(!std::is_copy_constructible<rs::Value>::value, "immutable claims are move-only");
    static_assert(std::is_nothrow_move_constructible<rs::Launch>::value, "launch transfers ownership");
    static_assert(!std::is_base_of<rc::Handle, rs::OutputsView>::value, "sink is borrowed");
    if (const auto* library = std::getenv("REINY_STEP_CPP_LIBRARY")) {
        std::ifstream maps("/proc/self/maps");
        assert(maps.is_open());
        bool found = false;
        for (std::string line; std::getline(maps, line);) {
            if (line.find("libreiny_ffi.so") == std::string::npos) continue;
            assert(line.find(library) != std::string::npos);
            found = true;
        }
        assert(found);
        std::cout << "CPP_STEP_LIBRARY " << library << '\n';
    }
    multi_tick();
    gated_lifecycle(false);
    gated_lifecycle(true);
    exception_and_output_failures();
    std::cout << "C++ Local managed steps: multi-tick, admission, snapshots, lanes, "
                 "exceptions, cancel, stop and joined lifetime passed\n";
}
