#ifndef REINY_JOB_WORK_HPP
#define REINY_JOB_WORK_HPP

#include "reiny_component.hpp"
#include "reiny_job.h"

namespace reiny {
namespace job {

using Bytes = component::Bytes;
using Timeout = component::Timeout;
using Error = component::Error;
using Buffer = component::Buffer;
using Handle = component::Handle;

enum class Phase : uint32_t { running = 1, cancelling, complete };
enum class Kind : uint32_t { succeeded = 1, application_error, cancelled, panicked, interrupted };
enum class Cancellation : uint32_t { requested = 1, already_requested, finished };
enum class StartStatus : int32_t {
    accepted = REINY_EXEC_OK,
    work_already_admitted = REINY_EXEC_INVALID_STATE,
    owner_closed = REINY_JOB_OWNER_CLOSED,
    at_capacity = REINY_JOB_AT_CAPACITY,
    id_exhausted = REINY_JOB_ID_EXHAUSTED,
    runtime_unavailable = REINY_JOB_RUNTIME_UNAVAILABLE
};
struct SnapshotInfo {
    uint64_t id, revision;
    Phase phase;
    bool cancellation_requested, has_progress;
};
struct ApplicationFailure { int32_t code; std::string message; };

// Throw from create/run to supply an explicit copied application diagnostic.
class ApplicationError : public std::runtime_error {
    int32_t code_;
public:
    ApplicationError(int32_t code, std::string message)
        : std::runtime_error(std::move(message)), code_(code) {
        if (code >= 0) throw std::invalid_argument("job application error code must be negative");
    }
    int32_t code() const noexcept { return code_; }
};

namespace detail {
inline reiny_exec_handle clone(reiny_exec_handle value) {
    reiny_exec_handle out = 0;
    component::check(reiny_exec_clone(value, &out));
    return out;
}
template<class Function>
inline reiny_exec_status callback(reiny_exec_handle sink, Function&& function) noexcept {
    try {
        function();
        return REINY_EXEC_OK;
    } catch (const ApplicationError& error) {
        (void)reiny_exec_error_set(sink, error.code(), component::text(error.what()));
        return error.code();
    } catch (const std::exception& error) {
        (void)reiny_exec_error_set(sink, REINY_EXEC_CALLBACK_ERROR, component::text(error.what()));
    } catch (...) {
        (void)reiny_exec_error_set(sink, REINY_EXEC_CALLBACK_ERROR,
                                    component::text("unknown C++ job exception"));
    }
    return REINY_EXEC_CALLBACK_ERROR;
}
} // namespace detail

class Context;
// Borrowed only through this callback. Retention never extends a component
// instance: a job context is a separate native JobContext capability.
class ContextView {
    reiny_exec_handle value_;
public:
    explicit ContextView(reiny_exec_handle value) noexcept : value_(value) {}
    Context retain() const;
    bool report(const Bytes& progress) const {
        uint32_t accepted = 0;
        component::check(reiny_job_report(value_, component::bytes(progress), &accepted));
        return accepted != 0;
    }
    bool cancelled() const {
        uint32_t out = 0;
        component::check(reiny_job_is_cancelled(value_, &out));
        return out != 0;
    }
    // Native notification resolves on cancellation OR terminal completion.
    // Inspect cancelled() rather than treating notification as cancellation.
    void wait_cancelled(Timeout timeout) const {
        component::check(reiny_job_wait_cancelled(value_, component::budget(timeout)));
    }
};
class Context : public Handle {
public:
    using Handle::Handle;
    ContextView view() const noexcept { return ContextView(get()); }
    Context clone() const { return Context(detail::clone(get())); }
};
inline Context ContextView::retain() const { return Context(detail::clone(value_)); }

// Borrowed, lane-confined and noncloneable native success sink. This copies
// bytes once; it does NOT complete the job. Callback return and destruction,
// native join, and then core commit determine the terminal result.
class SuccessView {
    reiny_exec_handle value_;
public:
    explicit SuccessView(reiny_exec_handle value) noexcept : value_(value) {}
    void set(const Bytes& value) const {
        component::check(reiny_job_succeed(value_, component::bytes(value)));
    }
};
class Task {
public:
    virtual ~Task() noexcept = default;
    virtual void run(ContextView, SuccessView) = 0;
};

namespace detail {
using Factory = std::function<std::unique_ptr<Task>(ContextView)>;
inline reiny_exec_status create(void* data, reiny_exec_handle context,
                                void** instance, reiny_exec_handle sink) noexcept {
    return callback(sink, [&] {
        auto task = (*static_cast<Factory*>(data))(ContextView(context));
        if (!task) throw std::runtime_error("job factory returned null");
        *instance = task.release();
    });
}
inline reiny_exec_status run(void* instance, reiny_exec_handle context,
                             reiny_exec_handle success, reiny_exec_handle sink) noexcept {
    return callback(sink, [&] {
        static_cast<Task*>(instance)->run(ContextView(context), SuccessView(success));
    });
}
inline void destroy(void* instance) noexcept { delete static_cast<Task*>(instance); }
inline void release_factory(void* factory) noexcept { delete static_cast<Factory*>(factory); }
} // namespace detail

class Work : public Handle {
public:
    using Handle::Handle;
    // Capture independently owned data. Never capture a borrowed component,
    // stack object, callback view, or detached thread's lifetime.
    // create/run/destroy use one ordinary native lane per admitted work item.
    // Factory/capture destruction runs on the last-owner thread and MUST NOT
    // block or unwind; task destruction must not unwind.
    static Work create(detail::Factory factory) {
        auto owned = std::make_unique<detail::Factory>(std::move(factory));
        const reiny_job_work_v1 table{
            sizeof(reiny_job_work_v1), 1, 0, 0, owned.get(),
            detail::create, detail::run, detail::destroy, detail::release_factory
        };
        reiny_exec_handle out = 0;
        component::check(reiny_job_work_new(&table, &out));
        (void)owned.release(); // Native ownership begins only on success.
        return Work(out);
    }
    Work clone() const { return Work(detail::clone(get())); }
    // Clones share ONE single-use work item, not independent work admissions.
    // Retaining work after terminal/native join can retain callback data.
};

} // namespace job
} // namespace reiny
#endif
