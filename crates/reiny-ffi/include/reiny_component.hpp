#ifndef REINY_COMPONENT_HPP
#define REINY_COMPONENT_HPP

#include "reiny_component_ports.hpp"

// C++17, header-only adapter for executable ABI v1, independent of reiny.hpp.
// Only trusted service components are supported: no steps, jobs, deployment
// owner, dynamic loading, protobuf codec, or cross-language zero-copy.
// Keep the native ABI library loaded for process lifetime: its shared runtime
// is process-scoped, even after all public handles have been released.
namespace reiny {
namespace component {

class Component {
public:
    virtual ~Component() noexcept = default;
    virtual void prepare(ContextView) {}
    virtual void run(ContextView) = 0;
    virtual void stop(ContextView, uint32_t) {}
};

namespace detail {
// All exception conversion, including allocation failure, stays inside the
// noexcept trampoline. Diagnostics are copied synchronously into this sink.
template<class Function>
reiny_exec_status callback(reiny_exec_handle sink, Function&& function) noexcept {
    try {
        function();
        return REINY_EXEC_OK;
    } catch (const std::exception& error) {
        (void)reiny_exec_error_set(sink, REINY_EXEC_CALLBACK_ERROR, text(error.what()));
    } catch (...) {
        (void)reiny_exec_error_set(sink, REINY_EXEC_CALLBACK_ERROR, text("unknown C++ exception"));
    }
    return REINY_EXEC_CALLBACK_ERROR;
}
using Factory = std::function<std::unique_ptr<Component>(ContextView)>;
inline reiny_exec_status create(void* data, reiny_exec_handle context,
                                void** instance, reiny_exec_handle sink) noexcept {
    return callback(sink, [&] {
        auto result = (*static_cast<Factory*>(data))(ContextView(context));
        if (!result) throw std::runtime_error("component factory returned null");
        *instance = result.release();
    });
}
inline reiny_exec_status prepare(void* instance, reiny_exec_handle context,
                                 reiny_exec_handle sink) noexcept {
    return callback(sink, [&] { static_cast<Component*>(instance)->prepare(ContextView(context)); });
}
inline reiny_exec_status run(void* instance, reiny_exec_handle context,
                             reiny_exec_handle sink) noexcept {
    return callback(sink, [&] { static_cast<Component*>(instance)->run(ContextView(context)); });
}
inline reiny_exec_status stop(void* instance, reiny_exec_handle context,
                              uint32_t reason, reiny_exec_handle sink) noexcept {
    return callback(sink, [&] { static_cast<Component*>(instance)->stop(ContextView(context), reason); });
}
inline void destroy(void* instance) noexcept { delete static_cast<Component*>(instance); }
// Runs on the thread releasing the last native factory owner, not necessarily
// an instance lane. Callable/capture destruction MUST NOT block or unwind.
inline void release_factory(void* data) noexcept { delete static_cast<Factory*>(data); }
} // namespace detail

class Registry : public Handle {
public:
    Registry() {
        if (reiny_exec_abi_version() != 1)
            throw Error(REINY_EXEC_UNSUPPORTED, "executable ABI major is not 1");
        reiny_exec_handle out = 0;
        check(reiny_exec_registry_new(&out));
        Handle::operator=(Handle(out));
    }
    // Callable/captures must support concurrent creates for independent
    // instances; capture owned state, not stack references that can expire.
    // Per-instance create/prepare/run/stop/destruction use one serialized
    // ordinary OS lane. Factory finalization runs on the last-owner thread:
    // callable/capture destructors MUST NOT block or unwind. A noexcept
    // trampoline cannot make a throwing or blocking destructor safe.
    void add(std::string_view name, std::string_view revision, detail::Factory factory) const {
        auto owned = std::make_unique<detail::Factory>(std::move(factory));
        reiny_exec_factory_v1 table{
            sizeof(reiny_exec_factory_v1), 1, 0, REINY_EXEC_SERVICE, owned.get(),
            detail::create, detail::prepare, detail::run, detail::stop,
            detail::destroy, detail::release_factory
        };
        check(reiny_exec_register(get(), text(name), text(revision), &table));
        (void)owned.release(); // Ownership transfers only on successful registration.
    }
    void seal() const { check(reiny_exec_registry_seal(get())); }
    // For app-owned executables only: the caller MUST exit after return,
    // including exceptions/UNRESPONSIVE. This does not spawn or reap a process.
    Buffer run_process(Timeout startup, Timeout stop, Timeout frame, size_t max_instances) const {
        reiny_exec_handle out = 0;
        check(reiny_exec_process_host_run(get(), budget(startup), budget(stop),
                                         budget(frame), max_instances, &out));
        return Buffer(out);
    }
};
class Graph : public Handle {
public:
    using Handle::Handle;
    static Graph import(std::string_view json) {
        reiny_exec_handle out = 0;
        check(reiny_exec_graph_import(text(json), &out));
        return Graph(out);
    }
};
class Observer : public Handle {
public:
    using Handle::Handle;
    Buffer next(Timeout timeout) const {
        reiny_exec_handle out = 0;
        check(reiny_exec_observer_next(get(), budget(timeout), &out));
        return Buffer(out);
    }
};
class Host : public Handle {
public:
    using Handle::Handle;
    static Host launch_local(const Registry& registry, const Graph& graph) {
        reiny_exec_handle out = 0;
        check(reiny_exec_host_launch_local(registry.get(), graph.get(), &out));
        return Host(out);
    }
    void wait_running(Timeout timeout) const {
        check(reiny_exec_host_wait_running(get(), budget(timeout)));
    }
    void request_stop() const { check(reiny_exec_host_request_stop(get())); }
    uint32_t phase() const {
        uint32_t out = 0;
        check(reiny_exec_host_phase(get(), &out));
        return out;
    }
    Buffer snapshot() const {
        reiny_exec_handle out = 0;
        check(reiny_exec_host_snapshot(get(), &out));
        return Buffer(out);
    }
    Observer subscribe() const {
        reiny_exec_handle out = 0;
        check(reiny_exec_host_subscribe(get(), &out));
        return Observer(out);
    }
    // Returns outcomes for FAILED/UNRESPONSIVE too; inspect primary, cleanup
    // and unjoined in the returned JSON. Success here is NOT a join certificate.
    Buffer wait_terminal(Timeout timeout) const {
        reiny_exec_handle out = 0;
        check(reiny_exec_host_wait_terminal(get(), budget(timeout), &out));
        return Buffer(out);
    }
    void wait_joined(Timeout timeout) const {
        check(reiny_exec_host_wait_joined(get(), budget(timeout)));
    }
    // Releasing a host requests stop without joining. Factory-capture
    // destruction must obey the nonblocking finalizer contract above.
    // Keep callback code
    // loaded until explicit wait_joined succeeds and all factory claims go.
    // Retained contexts/endpoints must also be released to reclaim transport.
    // New communication after stop is CLOSED; effects admitted before stop can
    // still finish. No handle release rolls effects back or revokes native work.
};

} // namespace component
} // namespace reiny
#endif
