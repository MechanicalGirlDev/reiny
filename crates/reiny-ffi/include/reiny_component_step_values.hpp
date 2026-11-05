#ifndef REINY_COMPONENT_STEP_VALUES_HPP
#define REINY_COMPONENT_STEP_VALUES_HPP

#include "reiny_component.hpp"
#include "reiny_component_step.h"

namespace reiny {
namespace component {
namespace step {

using Duration = reiny_step_duration;
using Tick = reiny_step_tick;

enum class ClockPhase : uint32_t { starting = 1, ready, running, faulted, stopped };
struct Clock {
    uint64_t epoch, next_index;
    Duration time;
    ClockPhase phase;
};
struct RunId { uint64_t epoch, sequence, tick_index; };
enum class Admission : int32_t {
    accepted = REINY_EXEC_OK,
    not_running = REINY_STEP_NOT_RUNNING,
    busy = REINY_STEP_BUSY,
    invalid_request = REINY_STEP_INVALID_REQUEST,
    invalid_timeout = REINY_STEP_INVALID_TIMEOUT,
    id_exhausted = REINY_STEP_ID_EXHAUSTED,
    faulted = REINY_STEP_FAULTED,
    stopped = REINY_STEP_STOPPED,
    supervisor_gone = REINY_STEP_SUPERVISOR_GONE
};
enum class Cancellation : uint32_t { requested = 1, already_requested, finished, gone };
enum class ResultKind : uint32_t {
    committed = 0, cancelled_before_dispatch, timed_out_before_dispatch,
    failed_after_dispatch, supervisor_gone
};

namespace detail {
inline reiny_exec_handle clone(reiny_exec_handle value) {
    reiny_exec_handle out = 0;
    check(reiny_exec_clone(value, &out));
    return out;
}
} // namespace detail

// Imports copy referenced handles' values synchronously; the JSON and source
// handles need only remain valid through import. Every clone is a new claim.
class Schema : public Handle {
public:
    using Handle::Handle;
    static Schema import(std::string_view json) {
        reiny_exec_handle out = 0;
        check(reiny_step_schema_import(text(json), &out));
        return Schema(out);
    }
    Schema clone() const { return Schema(detail::clone(get())); }
};
class Graph : public Handle {
public:
    using Handle::Handle;
    static Graph import(std::string_view json) {
        reiny_exec_handle out = 0;
        check(reiny_step_graph_import(text(json), &out));
        return Graph(out);
    }
    Graph clone() const { return Graph(detail::clone(get())); }
};
class Request : public Handle {
public:
    using Handle::Handle;
    static Request import(std::string_view json) {
        reiny_exec_handle out = 0;
        check(reiny_step_request_import(text(json), &out));
        return Request(out);
    }
    Request clone() const { return Request(detail::clone(get())); }
};
class Value : public Handle {
public:
    using Handle::Handle;
    static Value create(const component::Schema& schema, const Bytes& payload) {
        reiny_exec_handle out = 0;
        check(reiny_step_value_new(schema.get(), component::bytes(payload), &out));
        return Value(out);
    }
    static Value create(const component::Schema& schema, const Bytes& payload,
                        std::string_view semantics_json) {
        reiny_exec_handle out = 0;
        check(reiny_step_value_new_with_semantics(
            schema.get(), text(semantics_json), component::bytes(payload), &out));
        return Value(out);
    }
    Buffer semantics() const {
        reiny_exec_handle out = 0;
        check(reiny_step_value_semantics(get(), &out));
        return Buffer(out);
    }
    Bytes bytes(const component::Schema& schema) const {
        return copied([&](uint8_t* out, size_t capacity, size_t* size) {
            return reiny_step_value_copy(get(), schema.get(), out, capacity, size);
        });
    }
    Value clone() const { return Value(detail::clone(get())); }
};

class Inputs;
// Callback-scoped, borrowed view. get() returns an independently owned value;
// retain() explicitly owns the immutable input map beyond callback return.
class InputsView {
    reiny_exec_handle value_;
public:
    explicit InputsView(reiny_exec_handle value) noexcept : value_(value) {}
    Value get(std::string_view port) const {
        reiny_exec_handle out = 0;
        check(reiny_step_input_get(value_, text(port), &out));
        return Value(out);
    }
    Inputs retain() const;
};
class Inputs : public Handle {
public:
    using Handle::Handle;
    InputsView view() const noexcept { return InputsView(get()); }
    Inputs clone() const { return Inputs(detail::clone(get())); }
};
inline Inputs InputsView::retain() const { return Inputs(detail::clone(value_)); }

// Borrowed, noncloneable native sink. Use only on the current callback lane;
// no view copy prolongs its lifetime or permits writes from another thread.
class OutputsView {
    reiny_exec_handle value_;
public:
    explicit OutputsView(reiny_exec_handle value) noexcept : value_(value) {}
    void set(std::string_view port, const Value& value) const {
        check(reiny_step_output_set(value_, text(port), value.get()));
    }
};

} // namespace step
} // namespace component
} // namespace reiny
#endif
