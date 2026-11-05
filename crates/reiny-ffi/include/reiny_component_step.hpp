#ifndef REINY_COMPONENT_STEP_HPP
#define REINY_COMPONENT_STEP_HPP

#include "reiny_component_step_values.hpp"
#include <optional>

// Additive C++17 Local managed steps. The service adapter remains independent.
// Keep the native library loaded for process lifetime. No process/distributed
// steps, jobs, hard callback termination, or rollback of component effects.
namespace reiny {
namespace component {
namespace step {

class Component {
public:
    virtual ~Component() noexcept = default;
    virtual void prepare(ContextView) {}
    virtual void step(ContextView, const Tick&, InputsView, OutputsView) = 0;
    virtual void stop(ContextView, uint32_t) {}
};

namespace detail {
using Factory = std::function<std::unique_ptr<Component>(ContextView)>;
inline reiny_exec_status create(void* data, reiny_exec_handle context,
                                void** instance, reiny_exec_handle sink) noexcept {
    return component::detail::callback(sink, [&] {
        auto result = (*static_cast<Factory*>(data))(ContextView(context));
        if (!result) throw std::runtime_error("step factory returned null");
        *instance = result.release();
    });
}
inline reiny_exec_status prepare(void* instance, reiny_exec_handle context,
                                 reiny_exec_handle sink) noexcept {
    return component::detail::callback(sink, [&] {
        static_cast<Component*>(instance)->prepare(ContextView(context));
    });
}
inline reiny_exec_status invoke(void* instance, reiny_exec_handle context,
                                const reiny_step_tick* tick, reiny_exec_handle inputs,
                                reiny_exec_handle outputs, reiny_exec_handle sink) noexcept {
    return component::detail::callback(sink, [&] {
        static_cast<Component*>(instance)->step(ContextView(context), *tick,
                                                InputsView(inputs), OutputsView(outputs));
    });
}
inline reiny_exec_status stop(void* instance, reiny_exec_handle context,
                              uint32_t reason, reiny_exec_handle sink) noexcept {
    return component::detail::callback(sink, [&] {
        static_cast<Component*>(instance)->stop(ContextView(context), reason);
    });
}
inline void destroy(void* instance) noexcept { delete static_cast<Component*>(instance); }
inline void release_factory(void* data) noexcept { delete static_cast<Factory*>(data); }
} // namespace detail

// C++ uses the existing native finalizer registration. Callable/capture
// destructors MUST NOT block or unwind: they run on the last-owner thread,
// not necessarily an instance lane. Creates may run concurrently for different
// instances; their captures must support that. All per-instance hooks, including
// destruction, execute serially on one ordinary OS callback lane.
inline void register_factory(const Registry& registry, std::string_view name,
                             std::string_view revision, const Schema& schema,
                             detail::Factory factory) {
    auto owned = std::make_unique<detail::Factory>(std::move(factory));
    const reiny_step_factory_v1 table{
        sizeof(reiny_step_factory_v1), 1, 0, 0, owned.get(),
        detail::create, detail::prepare, detail::invoke, detail::stop,
        detail::destroy, detail::release_factory
    };
    check(reiny_step_register(registry.get(), text(name), text(revision), schema.get(), &table));
    (void)owned.release(); // Failure leaves the callable owned by this stack.
}

class Result : public Handle {
public:
    using Handle::Handle;
    Result clone() const { return Result(detail::clone(get())); }
    ResultKind kind() const {
        uint32_t out = 0;
        check(reiny_step_result_kind(get(), &out));
        return static_cast<ResultKind>(out);
    }
    // Only a committed result has a tick/output map. Errors remain errors.
    Tick tick() const {
        Tick out{};
        check(reiny_step_result_tick(get(), &out));
        return out;
    }
    Value output(std::string_view instance, std::string_view port) const {
        reiny_exec_handle out = 0;
        check(reiny_step_result_output(get(), text(instance), text(port), &out));
        return Value(out);
    }
    // JSON preserves structured failure kind, instance, port, attempted and
    // completed nodes. Do not classify failures by parsing diagnostic prose.
    Buffer info() const {
        reiny_exec_handle out = 0;
        check(reiny_step_result_info(get(), &out));
        return Buffer(out);
    }
};
class Run : public Handle {
public:
    using Handle::Handle;
    Run clone() const { return Run(detail::clone(get())); }
    RunId id() const {
        RunId out{};
        check(reiny_step_run_id(get(), &out.epoch, &out.sequence, &out.tick_index));
        return out;
    }
    Cancellation cancel() const {
        uint32_t out = 0;
        check(reiny_step_cancel(get(), &out));
        return static_cast<Cancellation>(out);
    }
    Result wait(Timeout timeout) const {
        reiny_exec_handle out = 0;
        check(reiny_step_wait(get(), budget(timeout), &out));
        return Result(out);
    }
    // Dropping any run/result or timing out a wait does not cancel execution.
    // Cancellation is an acknowledgement, not callback or destructor teardown.
};
struct AdmissionResult {
    Admission status;
    std::optional<Run> run;
};
class Scheduler : public Handle {
public:
    using Handle::Handle;
    Clock clock() const {
        reiny_step_clock out{};
        check(reiny_step_clock_read(get(), &out));
        return {out.epoch, out.next_index, out.time, static_cast<ClockPhase>(out.phase)};
    }
    AdmissionResult try_step(const Request& request, Timeout timeout) const {
        reiny_exec_handle out = 0;
        const auto ns = timeout.count() > 0 ? static_cast<uint64_t>(timeout.count()) : 0;
        const auto code = reiny_step_try(get(), request.get(), ns, &out);
        switch (code) {
        case REINY_EXEC_OK:
            return {Admission::accepted, Run(out)};
        case REINY_STEP_NOT_RUNNING:
        case REINY_STEP_BUSY:
        case REINY_STEP_INVALID_REQUEST:
        case REINY_STEP_INVALID_TIMEOUT:
        case REINY_STEP_ID_EXHAUSTED:
        case REINY_STEP_FAULTED:
        case REINY_STEP_STOPPED:
        case REINY_STEP_SUPERVISOR_GONE:
            return {static_cast<Admission>(code), std::nullopt};
        default:
            check(code);
            throw std::logic_error("unreachable step admission status");
        }
    }
    // Scheduler ownership does not keep the host alive. Host request_stop is
    // asynchronous, not an admission barrier. Always inspect typed admission.
};
struct Launch {
    Host host;
    Scheduler scheduler;
};
inline Launch launch_local(const Registry& registry, const component::Graph& instances,
                           const Graph& graph, Duration initial_time) {
    reiny_exec_handle host = 0, scheduler = 0;
    check(reiny_step_host_launch_local(registry.get(), instances.get(), graph.get(),
                                       initial_time, &host, &scheduler));
    return {Host(host), Scheduler(scheduler)};
}

// A committed receipt advances scheduler bookkeeping, not arbitrary effects.
// Failed/cancelled results can precede callback/destructor completion. Observe
// Host::wait_terminal and Host::wait_joined separately, including Unresponsive.
// Never unload callback code while native factory/lane/adapter owners survive.
} // namespace step
} // namespace component
} // namespace reiny
#endif
