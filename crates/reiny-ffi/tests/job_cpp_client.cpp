#include "reiny_job.hpp"
#include "reiny_component_step.hpp"
#include "reiny.hpp"
#include <atomic>
#include <cassert>
#include <cstdlib>
#include <fstream>
#include <future>
#include <iostream>
#include <thread>
#include <type_traits>

namespace rj = reiny::job;
using namespace std::chrono_literals;
static constexpr auto timeout = 5s;
static const rj::Bytes progress_bytes{1, 0, 255};
static const rj::Bytes success_bytes{7, 0, 255};

template<class Function>
static void expect(int32_t status, Function&& function) {
    try {
        function();
        assert(false && "expected ABI error");
    } catch (const rj::Error& error) {
        assert(error.status() == status);
    }
}
struct Event {
    std::promise<void> signal;
    std::shared_future<void> ready = signal.get_future().share();
    void open() { signal.set_value(); }
    void wait() const {
        assert(ready.wait_for(timeout) == std::future_status::ready);
        ready.get();
    }
};
enum class Mode { success, cancelled_success, cancelled_error, standard_error, unknown_error, missing_success };
struct Data;
struct State {
    Mode mode = Mode::success;
    bool gated_destroy = false;
    Event entered, progress_gate, progressed, finish, destroy_entered, destroy_gate;
    std::atomic<unsigned> created{0}, destroyed{0}, data_destroyed{0};
    std::thread::id lane;
    rj::Context retained;
    std::optional<rj::SuccessView> expired_success;
    std::weak_ptr<Data> data;
};
struct Data {
    std::shared_ptr<State> state;
    explicit Data(std::shared_ptr<State> value) : state(std::move(value)) {}
    ~Data() noexcept { ++state->data_destroyed; }
};
class Task : public rj::Task {
    std::shared_ptr<Data> data_;
    void lane() const { assert(data_->state->lane == std::this_thread::get_id()); }
public:
    Task(std::shared_ptr<Data> data, rj::ContextView context) : data_(std::move(data)) {
        auto& state = *data_->state;
        state.lane = std::this_thread::get_id();
        assert(state.created++ == 0);
        state.retained = context.retain();
    }
    ~Task() noexcept override {
        lane();
        auto& state = *data_->state;
        state.destroy_entered.open();
        if (state.gated_destroy) state.destroy_gate.wait();
        assert(state.destroyed++ == 0);
    }
    void run(rj::ContextView context, rj::SuccessView success) override {
        lane();
        auto& state = *data_->state;
        state.expired_success = success;
        state.entered.open();
        state.progress_gate.wait();
        auto progress = progress_bytes;
        assert(context.report(progress));
        progress.assign(progress.size(), 9); // Native progress is a copied snapshot.
        if (state.mode != Mode::missing_success) {
            auto result = success_bytes;
            success.set(result);
            expect(REINY_EXEC_INVALID_STATE, [&] { success.set(result); });
            result.assign(result.size(), 9); // Sink bytes must be independent.
        }
        state.progressed.open();
        state.finish.wait();
        if (state.mode == Mode::cancelled_success || state.mode == Mode::cancelled_error) {
            assert(context.cancelled());
            context.wait_cancelled(timeout);
            assert(!context.report({9})); // Rejected progress is not an ABI failure.
        } else {
            assert(!context.cancelled());
        }
        if (state.mode == Mode::cancelled_error) {
            std::string message = "job-error";
            rj::ApplicationError error(-88, message);
            message.assign("mutated-source");
            throw error;
        }
        if (state.mode == Mode::standard_error) throw std::runtime_error("C++ job exception");
        if (state.mode == Mode::unknown_error) throw 17;
    }
};
static rj::Work work(const std::shared_ptr<State>& state) {
    auto data = std::make_shared<Data>(state);
    state->data = data;
    return rj::Work::create([data = std::move(data)](rj::ContextView context) {
        return std::make_unique<Task>(data, context);
    });
}
static rj::Job start(const rj::Owner& owner, const rj::Work& item) {
    auto result = owner.start(item);
    assert(result.status == rj::StartStatus::accepted && result.job);
    return std::move(*result.job);
}
static void rejected(rj::StartResult result, rj::StartStatus status) {
    assert(result.status == status && !result.job);
}
static rj::Snapshot progress(const std::shared_ptr<State>& state, const rj::Job& job) {
    auto observer = job.subscribe(); // Subscribe before reporting is triggered.
    state->progress_gate.open();
    auto snapshot = observer.changed(timeout);
    state->progressed.wait();
    assert(snapshot.progress().bytes() == progress_bytes);
    const auto info = snapshot.info();
    assert(info.phase == rj::Phase::running && info.has_progress);
    assert(info.revision == 1 && !info.cancellation_requested);
    return snapshot;
}
static void joined_report(const rj::Buffer& report) {
    const auto json = report.text();
    assert(json.find("\"unresponsive\":[]") != std::string::npos);
    assert(json.find("\"join_failures\":[]") != std::string::npos);
}
static void timed_out_report(const rj::Buffer& report, uint64_t id) {
    const auto json = report.text();
    assert(json.find("\"unresponsive\":[" + std::to_string(id) + "]") != std::string::npos);
    assert(json.find("\"join_failures\":[]") != std::string::npos);
}
static void completed(const std::shared_ptr<State>& state, const rj::Job& job) {
    state->entered.wait();
    assert(state->lane != std::this_thread::get_id());
    auto snapshot = progress(state, job);
    state->finish.open();
    auto result = job.wait(timeout);
    assert(result.kind() == rj::Kind::succeeded);
    assert(result.success().bytes() == success_bytes);
    job.wait_joined(timeout);
}

static void capacity_counts_destruction() {
    auto a = std::make_shared<State>(), b = std::make_shared<State>();
    a->gated_destroy = true;
    auto owner = rj::Owner::create(1);
    auto wa = work(a), wb = work(b);
    auto alias = wa.clone();
    auto job = start(owner, wa);
    rejected(owner.start(alias), rj::StartStatus::work_already_admitted);
    wa.close(); alias.close(); // Actual native work retains both factory and instance.
    a->entered.wait();
    assert(a->lane != std::this_thread::get_id() && !a->data.expired());
    rejected(owner.start(wb), rj::StartStatus::at_capacity);
    assert(b->created == 0);
    auto initial = job.snapshot();
    assert(!initial.info().has_progress);
    expect(REINY_EXEC_INVALID_STATE, [&] { initial.progress(); });
    expect(REINY_EXEC_INVALID_STATE, [&] { initial.result(); });
    auto retained = progress(a, job);
    auto snapshot_alias = retained.clone();
    expect(REINY_EXEC_TIMEOUT, [&] { job.wait(1ns); });
    assert(!a->retained.view().cancelled()); // Wait timeout is observation only.
    a->finish.open();
    a->destroy_entered.wait();
    assert(a->destroyed == 0 && !a->data.expired());
    expect(REINY_EXEC_TIMEOUT, [&] { job.wait_joined(1ns); });
    rejected(owner.start(wb), rj::StartStatus::at_capacity);
    assert(b->created == 0); // Capacity remains held through native destruction.
    a->destroy_gate.open();
    auto result = job.wait(timeout);
    job.wait_joined(timeout);
    assert(a->destroyed == 1 && a->data_destroyed == 1 && a->data.expired());
    assert(result.kind() == rj::Kind::succeeded);
    assert(result.success().bytes() == success_bytes);
    assert(job.cancel() == rj::Cancellation::finished);
    auto terminal = job.snapshot();
    assert(terminal.info().phase == rj::Phase::complete);
    assert(terminal.result().kind() == rj::Kind::succeeded);
    assert(retained.info().phase == rj::Phase::running && retained.info().revision == 1);
    assert(snapshot_alias.progress().bytes() == progress_bytes);
    a->retained.view().wait_cancelled(timeout); // Terminal notification is not cancellation.
    assert(!a->retained.view().cancelled());
    expect(REINY_EXEC_INVALID_HANDLE, [&] { a->expired_success->set({1}); });
    joined_report(owner.shutdown(timeout));
    // Terminal publication need not mean the core task has returned yet.
    // Use another explicit owner, not a timing-sensitive immediate re-admission.
    auto second_owner = rj::Owner::create(1);
    auto second = start(second_owner, wb); // Capacity rejection left this work unused.
    wb.close();
    completed(b, second);
    assert(b->data.expired() && b->data_destroyed == 1);
    joined_report(second_owner.shutdown(timeout));
    auto result_alias = result.clone();
    result.close(); job.close(); owner.close();
    assert(result_alias.success().bytes() == success_bytes);
    assert(retained.progress().bytes() == progress_bytes);
    a->retained.close(); b->retained.close();
}

#include "job_cpp_cases.inc"

int main() {
    static_assert(!std::is_copy_constructible<rj::Owner>::value, "owners require explicit clone");
    static_assert(std::is_nothrow_move_constructible<rj::Work>::value, "work transfers ownership");
    static_assert(!std::is_base_of<rj::Handle, rj::SuccessView>::value, "result sink is borrowed");
    if (const auto* library = std::getenv("REINY_JOB_CPP_LIBRARY")) {
        std::ifstream maps("/proc/self/maps");
        assert(maps.is_open());
        bool found = false;
        for (std::string line; std::getline(maps, line);) {
            if (line.find("libreiny_ffi.so") == std::string::npos) continue;
            assert(line.find(library) != std::string::npos);
            found = true;
        }
        assert(found);
        std::cout << "CPP_JOB_LIBRARY " << library << '\n';
    }
    expect(REINY_EXEC_INVALID_ARGUMENT, [&] { rj::Owner::create(0); });
    capacity_counts_destruction();
    cancelled_shutdown_retry(Mode::cancelled_success);
    cancelled_shutdown_retry(Mode::cancelled_error);
    dropped_observers_and_owner();
    retained_and_unused_work();
    callback_errors();
    std::cout << "C++ owned jobs: capacity through destruction, copied progress/results, "
                 "cancel/error, owner drop, data retention and shutdown retry joined passed\n";
}
