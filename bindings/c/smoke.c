#include "reiny.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define REQUIRE(expr) do { if (!(expr)) { \
    fprintf(stderr, "%s failed: %s\n", #expr, reiny_last_error()); exit(1); \
} } while (0)
#define OK(expr) REQUIRE((expr) == 0)

int main(void) {
    reiny_handle bus = 0, a = 0, b = 0, pub = 0, sub = 0, msg = 0;
    reiny_handle payload = 0, source = 0, presence = 0, name = 0;
    const uint8_t binary[] = {0, 255, 1, 0, 128};
    const uint8_t *data = NULL;
    size_t size = 0;
    uint8_t has = 0;
    uint64_t schema = 0;
    OK(reiny_local_bus_new(&bus));
    REQUIRE(reiny_session_shutdown(bus) == -1);
    REQUIRE(reiny_local_bus_new(NULL) == -1);
    REQUIRE(reiny_local_bus_connect(bus, NULL, "c-smoke", &a) == -1);
    OK(reiny_local_bus_connect(bus, "sender", "c-smoke", &a));
    OK(reiny_local_bus_connect(bus, "receiver", "c-smoke", &b));
    OK(reiny_session_subscriber(b, "Binary", "sender", &sub));
    REQUIRE(reiny_session_publisher(a, "Binary", 2, 42, &pub) == -1);
    OK(reiny_session_publisher(a, "Binary", 1, 42, &pub));
    OK(reiny_session_publishers(b, "Binary", 1000, &presence));
    OK(reiny_list_len(presence, &size));
    REQUIRE(size == 1);
    OK(reiny_list_get(presence, 0, &name));
    REQUIRE(reiny_list_get(presence, size, &source) == -1);
    OK(reiny_buffer_len(name, &size));
    OK(reiny_buffer_data(name, &data));
    REQUIRE(size == 6 && memcmp(data, "sender", size) == 0);
    REQUIRE(reiny_publisher_send(pub, NULL, 1) == -1);
    OK(reiny_publisher_send(pub, binary, sizeof binary));
    OK(reiny_subscription_receive(sub, 1000, &msg));
    REQUIRE(msg != 0);
    OK(reiny_message_schema(msg, &has, &schema));
    REQUIRE(has == 1 && schema == 42);
    OK(reiny_message_payload(msg, &payload));
    OK(reiny_message_source(msg, &source));
    OK(reiny_release(msg));
    OK(reiny_buffer_len(payload, &size));
    OK(reiny_buffer_data(payload, &data));
    REQUIRE(size == sizeof binary && memcmp(data, binary, size) == 0);
    OK(reiny_buffer_len(source, &size));
    OK(reiny_buffer_data(source, &data));
    REQUIRE(size == 6 && memcmp(data, "sender", size) == 0);
    OK(reiny_release(payload));
    REQUIRE(reiny_release(payload) == -1);
    REQUIRE(reiny_buffer_len(payload, &size) == -1);
    REQUIRE(strlen(reiny_last_error()) > 0);
    OK(reiny_publisher_send(pub, NULL, 0));
    OK(reiny_subscription_receive(sub, 1000, &msg));
    REQUIRE(msg != 0);
    OK(reiny_message_payload(msg, &payload));
    OK(reiny_buffer_len(payload, &size));
    REQUIRE(size == 0);
    OK(reiny_release(payload));
    OK(reiny_release(msg));
    OK(reiny_subscription_receive(sub, 1, &msg));
    REQUIRE(msg == 0);
    OK(reiny_release(name));
    OK(reiny_release(presence));
    OK(reiny_release(source));
    OK(reiny_release(pub));
    OK(reiny_release(sub));
    OK(reiny_session_shutdown(a));
    OK(reiny_session_shutdown(b));
    OK(reiny_release(a));
    OK(reiny_release(b));
    OK(reiny_release(bus));
    REQUIRE(reiny_release(0) == -1);
    puts("C ABI smoke passed");
    return 0;
}
