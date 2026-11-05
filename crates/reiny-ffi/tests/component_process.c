#include "reiny_component.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static reiny_exec_str text(const char *s) {
    return (reiny_exec_str){(const uint8_t *)s, strlen(s)};
}
static int32_t create(void *factory, uint64_t context, void **instance, uint64_t sink) {
    (void)factory; (void)context; (void)sink;
    int *state = malloc(sizeof(*state));
    if (!state) return REINY_EXEC_FAILURE;
    *state = 0;
    *instance = state;
    puts("created");
    return 0;
}
static int32_t prepare(void *instance, uint64_t context, uint64_t sink) {
    (void)context; (void)sink;
    int *state = instance;
    assert(*state == 0);
    *state = 1;
    puts("prepared");
    return 0;
}
static int32_t run(void *instance, uint64_t context, uint64_t sink) {
    (void)sink;
    int *state = instance;
    assert(*state == 1);
    *state = 2;
    puts("running");
    return reiny_exec_context_wait_stop(context, UINT64_C(30000000000));
}
static int32_t stop(void *instance, uint64_t context, uint32_t reason, uint64_t sink) {
    (void)context; (void)reason; (void)sink;
    int *state = instance;
    assert(*state == 1 || *state == 2);
    *state = 3;
    puts("stopped");
    return 0;
}
static void destroy(void *instance) {
    int *state = instance;
    assert(*state == 3);
    puts("destroyed");
    free(state);
}
int main(void) {
    assert(setvbuf(stdout, NULL, _IOLBF, 0) == 0);
    uint64_t registry, outcome;
    reiny_exec_factory_v1 factory = {
        sizeof(factory), 1, 0, REINY_EXEC_SERVICE, NULL,
        create, prepare, run, stop, destroy, NULL
    };
    assert(reiny_exec_registry_new(&registry) == 0);
    assert(reiny_exec_register(registry, text("foreign"), text("1"), &factory) == 0);
    assert(reiny_exec_registry_seal(registry) == 0);
    int32_t result = reiny_exec_process_host_run(registry,
        UINT64_C(10000000000), UINT64_C(1000000000), UINT64_C(1000000000), 8, &outcome);
    if (result != 0) return 2;
    uint8_t json[4096]; size_t length = 0;
    assert(reiny_exec_buffer_copy(outcome, json, sizeof(json), &length) == 0);
    assert(fwrite(json, 1, length, stdout) == length);
    puts("");
    /* This is an executable entry: exit immediately after the endpoint returns. */
    return 0;
}
