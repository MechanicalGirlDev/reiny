#ifndef REINY_JOB_HPP
#define REINY_JOB_HPP

#include "reiny_job_work.hpp"
#include <optional>

// Standalone, explicitly owned Local jobs. No component instance is borrowed.
// The native JobOwner owns admission, cancellation, progress and actual joining.
// Keep the native ABI library loaded for process lifetime.
namespace reiny {
namespace job {

class Result : public Handle {
public:
    using Handle::Handle;
    Result clone() const { return Result(detail::clone(get())); }
    Kind kind() const {
        uint32_t out = 0;
        component::check(reiny_job_result_kind(get(), &out));
        return static_cast<Kind>(out);
    }
    Buffer success() const {
        reiny_exec_handle out = 0;
        component::check(reiny_job_result_success(get(), &out));
        return Buffer(out);
    }
    ApplicationFailure error() const {
        int32_t code = 0;
        reiny_exec_handle message = 0;
        component::check(reiny_job_result_error(get(), &code, &message));
        return {code, Buffer(message).text()};
    }
};
class Snapshot : public Handle {
public:
    using Handle::Handle;
    Snapshot clone() const { return Snapshot(detail::clone(get())); }
    SnapshotInfo info() const {
        reiny_job_snapshot_info out{};
        component::check(reiny_job_snapshot_read(get(), &out));
        return {out.id, out.revision, static_cast<Phase>(out.phase),
                out.cancellation_requested != 0, out.has_progress != 0};
    }
    Buffer progress() const {
        reiny_exec_handle out = 0;
        component::check(reiny_job_snapshot_progress(get(), &out));
        return Buffer(out);
    }
    Result result() const {
        reiny_exec_handle out = 0;
        component::check(reiny_job_snapshot_result(get(), &out));
        return Result(out);
    }
};
class Observer : public Handle {
public:
    using Handle::Handle;
    // Subscribe before the triggering action. Progress is latest-value state,
    // not an event queue; retained snapshots remain immutable.
    Snapshot changed(Timeout timeout) const {
        reiny_exec_handle out = 0;
        component::check(reiny_job_changed(get(), component::budget(timeout), &out));
        return Snapshot(out);
    }
};
class Job : public Handle {
public:
    using Handle::Handle;
    Job clone() const { return Job(detail::clone(get())); }
    Snapshot snapshot() const {
        reiny_exec_handle out = 0;
        component::check(reiny_job_snapshot(get(), &out));
        return Snapshot(out);
    }
    Observer subscribe() const {
        reiny_exec_handle out = 0;
        component::check(reiny_job_subscribe(get(), &out));
        return Observer(out);
    }
    Cancellation cancel() const {
        uint32_t out = 0;
        component::check(reiny_job_cancel(get(), &out));
        return static_cast<Cancellation>(out);
    }
    Result wait(Timeout timeout) const {
        reiny_exec_handle out = 0;
        component::check(reiny_job_wait(get(), component::budget(timeout), &out));
        return Result(out);
    }
    void wait_joined(Timeout timeout) const {
        component::check(reiny_job_wait_joined(get(), component::budget(timeout)));
    }
    // Observer release and wait timeout do not cancel work. Native join does
    // not prove release of retained Work/callback roots or detached child work.
};
struct StartResult { StartStatus status; std::optional<Job> job; };
class Owner : public Handle {
public:
    using Handle::Handle;
    static Owner create(size_t capacity) {
        reiny_exec_handle out = 0;
        component::check(reiny_job_owner_new(capacity, &out));
        return Owner(out);
    }
    Owner clone() const { return Owner(detail::clone(get())); }
    StartResult start(const Work& work) const {
        reiny_exec_handle out = 0;
        const auto code = reiny_job_start(get(), work.get(), &out);
        switch (code) {
        case REINY_EXEC_OK:
            return {StartStatus::accepted, Job(out)};
        case REINY_EXEC_INVALID_STATE:
        case REINY_JOB_OWNER_CLOSED:
        case REINY_JOB_AT_CAPACITY:
        case REINY_JOB_ID_EXHAUSTED:
        case REINY_JOB_RUNTIME_UNAVAILABLE:
            return {static_cast<StartStatus>(code), std::nullopt};
        default:
            component::check(code);
            throw std::logic_error("unreachable job start status");
        }
    }
    void close_admission() const { component::check(reiny_job_owner_close(get())); }
    // Returns the native JSON report, including unresponsive and join_failures
    // job-ID arrays. Successful API return is NOT a successful-join certificate.
    // Zero grace is valid; retain this owner and retry after live work exits.
    Buffer shutdown(Timeout grace) const {
        if (grace.count() < 0) throw std::invalid_argument("shutdown grace must not be negative");
        reiny_exec_handle out = 0;
        component::check(reiny_job_owner_shutdown(get(), static_cast<uint64_t>(grace.count()), &out));
        return Buffer(out);
    }
    // RAII releases this claim only. Last owner drop requests cancellation,
    // never waits for native work or certifies cleanup. Keep an owner clone
    // when bounded shutdown/retry is required. Work/data stay retained natively
    // until actual lane destruction even if all public observations are gone.
};

} // namespace job
} // namespace reiny
#endif
