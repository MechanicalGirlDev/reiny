#ifndef REINY_COMPONENT_PORTS_HPP
#define REINY_COMPONENT_PORTS_HPP

#include "reiny_component_base.hpp"

namespace reiny {
namespace component {

class Message : public Handle {
    Bytes field(uint32_t which) const {
        return copied([&](uint8_t* out, size_t capacity, size_t* size) {
            return reiny_exec_message_copy(get(), which, out, capacity, size);
        });
    }
    std::string text_field(uint32_t which) const {
        auto value = field(which);
        if (!value.empty()) value.pop_back(); // ABI text includes the trailing NUL.
        return string_bytes(value);
    }
public:
    using Handle::Handle;
    Bytes payload() const { return field(REINY_EXEC_PAYLOAD); }
    std::string source() const { return text_field(REINY_EXEC_SOURCE); }
    std::string source_port() const { return text_field(REINY_EXEC_SOURCE_PORT); }
    std::string info_json() const { return string_bytes(field(REINY_EXEC_MESSAGE_INFO)); }
};

class Publisher : public Handle {
public:
    using Handle::Handle;
    void send(const Bytes& payload) const { check(reiny_exec_publisher_send(get(), bytes(payload))); }
};
class Subscription : public Handle {
public:
    using Handle::Handle;
    Message receive(Timeout timeout) const {
        reiny_exec_handle out = 0;
        check(reiny_exec_subscription_receive(get(), budget(timeout), &out));
        return Message(out);
    }
    Buffer stats(Timeout timeout) const {
        reiny_exec_handle out = 0;
        check(reiny_exec_subscription_stats(get(), budget(timeout), &out));
        return Buffer(out);
    }
};
class Operation : public Handle {
public:
    using Handle::Handle;
    // TIMEOUT abandons this wait only. Neither timeout nor release cancels the
    // server's effects; never automatically retry an outcome-unknown RPC.
    Buffer wait(Timeout timeout) const {
        reiny_exec_handle out = 0;
        check(reiny_exec_operation_wait(get(), budget(timeout), &out));
        return Buffer(out);
    }
};
class RpcClient : public Handle {
public:
    using Handle::Handle;
    Operation call(const Bytes& payload, Timeout timeout) const {
        reiny_exec_handle out = 0;
        check(reiny_exec_rpc_call_start(get(), bytes(payload), budget(timeout), &out));
        return Operation(out);
    }
};
class Request : public Handle {
public:
    using Handle::Handle;
    Bytes payload() const {
        return copied([&](uint8_t* out, size_t capacity, size_t* size) {
            return reiny_exec_request_copy(get(), out, capacity, size);
        });
    }
    void reply(const Bytes& payload) const {
        check(reiny_exec_request_reply(get(), bytes(payload), 0));
    }
    void reply_error(std::string_view message) const {
        check(reiny_exec_request_reply(get(), text(message), 1));
    }
    // Reply is one-shot; a second attempt raises ALREADY_COMPLETED. Destruction
    // of the last unanswered claim finalizes NoReply rather than success.
};
class RpcServer : public Handle {
public:
    using Handle::Handle;
    Request receive(Timeout timeout) const {
        reiny_exec_handle out = 0;
        check(reiny_exec_rpc_server_receive(get(), budget(timeout), &out));
        return Request(out);
    }
};
class Presence : public Handle {
public:
    using Handle::Handle;
    Buffer next(Timeout timeout) const {
        reiny_exec_handle out = 0;
        check(reiny_exec_presence_next(get(), budget(timeout), &out));
        return Buffer(out);
    }
};

class Context;
// Borrowed only for a callback's duration. Never store this view; retain() makes
// a separate owned claim, preserving resources but not authority after stop.
class ContextView {
    reiny_exec_handle value_;
    reiny_exec_handle stream(std::string_view name, const Schema& schema, uint32_t direction) const {
        reiny_exec_handle out = 0;
        check(reiny_exec_context_open_stream(value_, text(name), schema.get(), direction, &out));
        return out;
    }
    reiny_exec_handle rpc(std::string_view name, const Schema& request,
                          const Schema& response, uint32_t direction) const {
        reiny_exec_handle out = 0;
        check(reiny_exec_context_open_rpc(value_, text(name), request.get(), response.get(), direction, &out));
        return out;
    }
public:
    explicit ContextView(reiny_exec_handle value) noexcept : value_(value) {}
    Context retain() const;
    Buffer info() const {
        reiny_exec_handle out = 0;
        check(reiny_exec_context_info(value_, &out));
        return Buffer(out);
    }
    bool stopping() const {
        uint32_t out = 0;
        check(reiny_exec_context_is_stopping(value_, &out));
        return out != 0;
    }
    void wait_stop(Timeout timeout) const {
        check(reiny_exec_context_wait_stop(value_, budget(timeout)));
    }
    Publisher output(std::string_view name, const Schema& schema) const {
        return Publisher(stream(name, schema, REINY_EXEC_OUTPUT));
    }
    Subscription input(std::string_view name, const Schema& schema) const {
        return Subscription(stream(name, schema, REINY_EXEC_INPUT));
    }
    RpcClient rpc_client(std::string_view name, const Schema& request, const Schema& response) const {
        return RpcClient(rpc(name, request, response, REINY_EXEC_RPC_CLIENT));
    }
    RpcServer rpc_server(std::string_view name, const Schema& request, const Schema& response) const {
        return RpcServer(rpc(name, request, response, REINY_EXEC_RPC_SERVER));
    }
    Presence watch_input_ready(std::string_view name) const {
        reiny_exec_handle out = 0;
        check(reiny_exec_context_watch_input_ready(value_, text(name), &out));
        return Presence(out);
    }
};
class Context : public Handle {
public:
    using Handle::Handle;
    ContextView view() const noexcept { return ContextView(get()); }
};
inline Context ContextView::retain() const {
    reiny_exec_handle out = 0;
    check(reiny_exec_clone(value_, &out));
    return Context(out);
}

} // namespace component
} // namespace reiny
#endif
