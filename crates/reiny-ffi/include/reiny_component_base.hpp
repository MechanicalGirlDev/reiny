#ifndef REINY_COMPONENT_BASE_HPP
#define REINY_COMPONENT_BASE_HPP

#include "reiny_component.h"
#include <chrono>
#include <exception>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace reiny {
namespace component {

using Bytes = std::vector<uint8_t>;
using Timeout = std::chrono::nanoseconds;

inline reiny_exec_str text(std::string_view value) noexcept {
    return {reinterpret_cast<const uint8_t*>(value.data()), value.size()};
}
inline reiny_exec_bytes bytes(const Bytes& value) noexcept {
    return {value.data(), value.size()};
}
inline uint64_t budget(Timeout timeout) {
    if (timeout.count() <= 0) throw std::invalid_argument("timeout must be positive");
    return static_cast<uint64_t>(timeout.count());
}

class Error : public std::runtime_error {
    reiny_exec_status status_;
public:
    Error(reiny_exec_status status, std::string message)
        : std::runtime_error(std::move(message)), status_(status) {}
    reiny_exec_status status() const noexcept { return status_; }
};

inline void check(reiny_exec_status status) {
    if (status == REINY_EXEC_OK) return;
    // Copy immediately on this thread, before another ABI operation can replace it.
    size_t size = 0;
    std::string message = "Reiny executable ABI error";
    if (reiny_exec_last_error_copy(nullptr, 0, &size) == REINY_EXEC_OK && size) {
        Bytes data(size);
        if (reiny_exec_last_error_copy(data.data(), data.size(), &size) == REINY_EXEC_OK)
            message.assign(reinterpret_cast<const char*>(data.data()), size - 1);
    }
    throw Error(status, std::move(message));
}

// Move-only ownership claim. A numeric bit-copy is NOT an independent claim.
// Destruction releases resources; it never certifies successful host cleanup.
class Handle {
    reiny_exec_handle value_ = 0;
public:
    explicit Handle(reiny_exec_handle value = 0) noexcept : value_(value) {}
    Handle(const Handle&) = delete;
    Handle& operator=(const Handle&) = delete;
    Handle(Handle&& other) noexcept : value_(std::exchange(other.value_, 0)) {}
    Handle& operator=(Handle&& other) noexcept {
        if (this != &other) {
            if (value_) (void)reiny_exec_release(value_);
            value_ = std::exchange(other.value_, 0);
        }
        return *this;
    }
    ~Handle() { if (value_) (void)reiny_exec_release(value_); }
    reiny_exec_handle get() const noexcept { return value_; }
    void close() {
        if (value_) {
            check(reiny_exec_release(value_));
            value_ = 0;
        }
    }
};

template<class Copy>
inline Bytes copied(Copy copy) {
    size_t size = 0;
    check(copy(nullptr, 0, &size));
    Bytes result(size);
    check(copy(result.data(), result.size(), &size));
    return result;
}
inline std::string string_bytes(const Bytes& value) {
    if (value.empty()) return {};
    return {reinterpret_cast<const char*>(value.data()), value.size()};
}

class Buffer : public Handle {
public:
    using Handle::Handle;
    Bytes bytes() const {
        return copied([&](uint8_t* out, size_t capacity, size_t* size) {
            return reiny_exec_buffer_copy(get(), out, capacity, size);
        });
    }
    std::string text() const { return string_bytes(bytes()); }
};

class Schema : public Handle {
public:
    using Handle::Handle;
    static Schema create(std::string_view name, uint16_t version, uint64_t fingerprint) {
        reiny_exec_handle out = 0;
        check(reiny_exec_schema_new(text(name), version, fingerprint, &out));
        return Schema(out);
    }
};

} // namespace component
} // namespace reiny
#endif
