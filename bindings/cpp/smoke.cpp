#include "reiny.hpp"
#include <iostream>
#include <type_traits>

static_assert(!std::is_copy_constructible_v<reiny::Session>);
static_assert(std::is_nothrow_move_constructible_v<reiny::Session>);

int main() {
    try {
        reiny::LocalBus bus;
        auto sender = bus.connect("sender", "cpp-smoke");
        auto receiver = bus.connect("receiver", "cpp-smoke");
        auto sub = receiver.subscriber("Binary", std::string("sender"));
        auto pub = sender.publisher("Binary", 42);
        const std::vector<uint8_t> binary = {0, 255, 1, 0, 128};
        if (receiver.publishers("Binary", 1000) != std::vector<std::string>{"sender"})
            throw std::runtime_error("presence mismatch");
        pub.send(binary);
        auto message = sub.receive(1000);
        if (!message || message->payload().bytes() != binary ||
            message->source() != "sender" || message->schema() != 42)
            throw std::runtime_error("message mismatch");
        auto retained = message->payload();
        message.reset();
        auto moved = std::move(retained);
        if (moved.bytes() != binary) throw std::runtime_error("buffer ownership mismatch");
        pub.send({});
        message = sub.receive(1000);
        if (!message || !message->payload().bytes().empty())
            throw std::runtime_error("empty message mismatch");
        if (sub.receive(1)) throw std::runtime_error("expected timeout");
        sender.shutdown();
        receiver.shutdown();
        std::cout << "C++ RAII smoke passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
