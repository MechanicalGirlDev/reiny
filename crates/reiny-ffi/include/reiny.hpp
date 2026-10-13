#ifndef REINY_HPP
#define REINY_HPP
#include "reiny.h"
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace reiny {
inline void check(int32_t status) {
    if (status != 0) throw std::runtime_error(reiny_last_error());
}

class Handle {
    reiny_handle value_ = 0;
public:
    explicit Handle(reiny_handle value = 0) noexcept : value_(value) {}
    Handle(const Handle&) = delete;
    Handle& operator=(const Handle&) = delete;
    Handle(Handle&& other) noexcept : value_(std::exchange(other.value_, 0)) {}
    Handle& operator=(Handle&& other) noexcept {
        if (this != &other) {
            if (value_) reiny_release(value_);
            value_ = std::exchange(other.value_, 0);
        }
        return *this;
    }
    ~Handle() { if (value_) reiny_release(value_); }
    reiny_handle get() const noexcept { return value_; }
};

class Buffer : public Handle {
public:
    using Handle::Handle;
    std::vector<uint8_t> bytes() const {
        size_t size = 0;
        const uint8_t* data = nullptr;
        check(reiny_buffer_len(get(), &size));
        check(reiny_buffer_data(get(), &data));
        if (!size) return {};
        return {data, data + size};
    }
    std::string text() const {
        auto value = bytes();
        if (value.empty()) return {};
        return {reinterpret_cast<const char*>(value.data()), value.size()};
    }
};

class Message : public Handle {
public:
    using Handle::Handle;
    Buffer payload() const {
        reiny_handle out = 0;
        check(reiny_message_payload(get(), &out));
        return Buffer(out);
    }
    std::string source() const {
        reiny_handle out = 0;
        check(reiny_message_source(get(), &out));
        return Buffer(out).text();
    }
    std::optional<uint64_t> schema() const {
        uint8_t has = 0;
        uint64_t value = 0;
        check(reiny_message_schema(get(), &has, &value));
        if (has) return value;
        return std::nullopt;
    }
    std::optional<uint64_t> timestamp() const {
        uint8_t has = 0;
        uint64_t value = 0;
        check(reiny_message_timestamp(get(), &has, &value));
        if (has) return value;
        return std::nullopt;
    }
};

class Subscription : public Handle {
public:
    using Handle::Handle;
    std::optional<Message> receive(uint64_t timeout_ms) const {
        reiny_handle out = 0;
        check(reiny_subscription_receive(get(), timeout_ms, &out));
        if (out) return Message(out);
        return std::nullopt;
    }
};

class Publisher : public Handle {
public:
    using Handle::Handle;
    void send(const std::vector<uint8_t>& payload) const {
        check(reiny_publisher_send(get(), payload.data(), payload.size()));
    }
};

class Session : public Handle {
public:
    using Handle::Handle;
    static Session open(const std::string& id, const std::string& domain,
                        const std::optional<std::string>& config, const std::string& module_bindings_path) {
        reiny_handle out = 0;
        check(reiny_session_open(id.c_str(), domain.c_str(), config ? config->c_str() : nullptr, module_bindings_path.c_str(), &out));
        return Session(out);
    }
    Publisher publisher(const std::string& name, const std::string& type_name, uint64_t schema) const {
        reiny_handle out = 0;
        check(reiny_session_publisher(get(), name.c_str(), type_name.c_str(), schema, &out));
        return Publisher(out);
    }
    Subscription subscriber(const std::string& name, const std::string& type_name, uint64_t schema) const {
        reiny_handle out = 0;
        check(reiny_session_subscriber(get(), name.c_str(), type_name.c_str(), schema, &out));
        return Subscription(out);
    }
    void ready() const { check(reiny_session_ready(get())); }
    void shutdown() const { check(reiny_session_shutdown(get())); }
};

class LocalBus : public Handle {
public:
    LocalBus() {
        reiny_handle out = 0;
        check(reiny_local_bus_new(&out));
        Handle::operator=(Handle(out));
    }
    Session connect(const std::string& id, const std::string& domain, const std::string& module_bindings_path) const {
        reiny_handle out = 0;
        check(reiny_local_bus_connect(get(), id.c_str(), domain.c_str(), module_bindings_path.c_str(), &out));
        return Session(out);
    }
};
} // namespace reiny
#endif
