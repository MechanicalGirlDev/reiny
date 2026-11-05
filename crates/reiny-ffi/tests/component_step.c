#define _POSIX_C_SOURCE 200809L
#include "reiny_component_step.h"
#include <assert.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define NS UINT64_C(5000000000)
#define CHECK(call) do { int32_t c_ = (call); if(c_) { \
    uint8_t e_[2048]; size_t n_; reiny_exec_last_error_copy(e_,sizeof(e_),&n_); \
    fprintf(stderr,"%d: %s => %d: %s\n",__LINE__,#call,c_,e_); abort(); } } while(0)
static reiny_exec_bytes text(const char *s) {
    return (reiny_exec_bytes){(const uint8_t *)s, strlen(s)};
}
#include "component_step_callbacks.inc"

typedef struct {
    state a,b;
    uint64_t registry,host,scheduler,schema,wrong,request;
    uint64_t tokens[2];
} fixture;
static void init_state(state *s,int role,uint64_t schema,uint64_t wrong) {
    memset(s,0,sizeof(*s)); s->role=role; s->schema=schema; s->wrong=wrong;
    assert(!pthread_mutex_init(&s->mutex,NULL));
    assert(!pthread_cond_init(&s->changed,NULL));
}
static uint64_t request(uint64_t schema,uint8_t n) {
    uint64_t v=value(schema,n),r; char json[512];
    snprintf(json,sizeof(json),"{\"version\":1,\"dt\":{\"secs\":0,\"nanos\":7},"
        "\"inputs\":[{\"instance\":\"a\",\"port\":\"x\",\"value\":%" PRIu64 "}]}",v);
    CHECK(reiny_step_request_import(text(json),&r)); CHECK(reiny_exec_release(v)); return r;
}
static void setup(fixture *f,int blocked,int bad_output) {
    memset(f,0,sizeof(*f));
    CHECK(reiny_exec_schema_new(text("test.Number"),2,42,&f->schema));
    CHECK(reiny_exec_schema_new(text("test.Other"),2,42,&f->wrong));
    init_state(&f->a,0,f->schema,f->wrong); init_state(&f->b,1,f->schema,f->wrong);
    f->a.blocked=blocked; f->b.bad_output=bad_output;
    CHECK(reiny_exec_registry_new(&f->registry));
    uint64_t schemas[2]; char json[4096];
    for(int i=0;i<2;i++) {
        snprintf(json,sizeof(json),"{\"version\":1,\"inputs\":{\"x\":%" PRIu64
            "%s},\"outputs\":{\"out\":%" PRIu64 "}}",f->schema,"",f->schema);
        if(i==0) snprintf(json,sizeof(json),"{\"version\":1,\"inputs\":{\"x\":%" PRIu64
            ",\"prev\":%" PRIu64 "},\"outputs\":{\"out\":%" PRIu64 "}}",f->schema,f->schema,f->schema);
        CHECK(reiny_step_schema_import(text(json),&schemas[i]));
        reiny_step_factory_v1 table={sizeof(table),1,0,0,i ? &f->b : &f->a,
            create,prepare,step,stop,destroy,release_factory};
        if(blocked==2) {
            uint64_t token;
            CHECK(reiny_exec_factory_release_token_new(&token));
            assert(reiny_step_register_notified(f->registry,text(i ? "b":"a"),text("1"),schemas[i],&table,token)==REINY_EXEC_INVALID_ARGUMENT);
            table.release_factory=NULL;
            CHECK(reiny_step_register_notified(f->registry,text(i ? "b":"a"),text("1"),schemas[i],&table,token));
            CHECK(reiny_exec_clone(token,&f->tokens[i]));
            CHECK(reiny_exec_release(token));
        } else CHECK(reiny_step_register(f->registry,text(i ? "b":"a"),text("1"),schemas[i],&table));
        memset(&table,0,sizeof(table));
    }
    CHECK(reiny_exec_registry_seal(f->registry));
    uint64_t graph,steps,initial=value(f->schema,10);
    snprintf(json,sizeof(json),"{\"version\":1,\"nodes\":{\"a\":%" PRIu64 ",\"b\":%" PRIu64
        "},\"routes\":[{\"instance\":\"a\",\"port\":\"x\",\"kind\":\"external\"},"
        "{\"instance\":\"a\",\"port\":\"prev\",\"kind\":\"previous_tick\","
        "\"source\":{\"instance\":\"b\",\"port\":\"out\"},\"initial\":%" PRIu64 "},"
        "{\"instance\":\"b\",\"port\":\"x\",\"kind\":\"same_tick\","
        "\"source\":{\"instance\":\"a\",\"port\":\"out\"}}]}",schemas[0],schemas[1],initial);
    CHECK(reiny_step_graph_import(text(json),&steps));
    CHECK(reiny_exec_graph_import(text("{\"version\":1,\"domain\":\"step-c\","
        "\"startup_timeout_ns\":5000000000,\"stop_timeout_ns\":30000000,\"instances\":["
        "{\"factory_id\":\"a\",\"bindings\":{\"version\":3,\"namespace\":\"a\","
        "\"endpoint_namespace\":\"a\",\"inputs\":{},\"outputs\":{}}},"
        "{\"factory_id\":\"b\",\"bindings\":{\"version\":3,\"namespace\":\"b\","
        "\"endpoint_namespace\":\"b\",\"inputs\":{},\"outputs\":{}}}]}"),&graph));
    uint64_t untouched=123;
    assert(reiny_exec_host_launch_local(f->registry,graph,&untouched)==REINY_EXEC_UNSUPPORTED);
    assert(untouched==123);
    uint64_t invalid_graph, untouched_step=456;
    snprintf(json,sizeof(json),"{\"version\":1,\"nodes\":{\"a\":%" PRIu64
        ",\"b\":%" PRIu64 "},\"routes\":[]}",schemas[1],schemas[1]);
    CHECK(reiny_step_graph_import(text(json),&invalid_graph));
    assert(reiny_step_host_launch_local(f->registry,graph,invalid_graph,
        (reiny_step_duration){0,0},&untouched,&untouched_step)==REINY_EXEC_FAILURE);
    assert(untouched==123 && untouched_step==456);
    CHECK(reiny_exec_release(invalid_graph));
    CHECK(reiny_step_host_launch_local(f->registry,graph,steps,
        (reiny_step_duration){UINT64_C(20000000000),0},&f->host,&f->scheduler));
    CHECK(reiny_exec_release(graph)); CHECK(reiny_exec_release(steps));
    CHECK(reiny_exec_release(initial));
    for(int i=0;i<2;i++) CHECK(reiny_exec_release(schemas[i]));
    CHECK(reiny_exec_host_wait_running(f->host,NS));
    assert(!pthread_equal(pthread_self(),f->a.lane));
    assert(!pthread_equal(f->a.lane,f->b.lane));
    f->request=request(f->schema,1);
}
static void cleanup(fixture *f) {
    CHECK(reiny_exec_host_request_stop(f->host));
    CHECK(reiny_exec_host_wait_joined(f->host,NS));
    assert(f->a.stopped && f->b.stopped && f->a.destroyed && f->b.destroyed);
    CHECK(reiny_exec_release(f->host));
    if(f->registry) CHECK(reiny_exec_release(f->registry));
    if(f->tokens[0]) {
        for(int i=0;i<2;i++) {
            uint32_t released=0;
            CHECK(reiny_exec_factory_released(f->tokens[i],&released)); assert(released==1);
            CHECK(reiny_exec_release(f->tokens[i]));
        }
        assert(!f->a.released && !f->b.released);
    } else {
        await_flag(&f->a,&f->a.released); await_flag(&f->b,&f->b.released);
    }
    CHECK(reiny_exec_release(f->scheduler)); CHECK(reiny_exec_release(f->request));
    CHECK(reiny_exec_release(f->schema)); CHECK(reiny_exec_release(f->wrong));
    assert(!pthread_cond_destroy(&f->a.changed)); assert(!pthread_mutex_destroy(&f->a.mutex));
    assert(!pthread_cond_destroy(&f->b.changed)); assert(!pthread_mutex_destroy(&f->b.mutex));
}
static void mult_tick(void) {
    fixture f; setup(&f,0,0);
    CHECK(reiny_exec_release(f.registry)); f.registry=0;
    uint64_t bad=request(f.wrong,1),run=123,result,v;
    assert(reiny_step_try(f.scheduler,bad,NS,&run)==REINY_STEP_INVALID_REQUEST && run==123);
    CHECK(reiny_exec_release(bad));
    assert(reiny_step_try(f.scheduler,f.request,0,&run)==REINY_STEP_INVALID_TIMEOUT);
    for(uint64_t i=0;i<2;i++) {
        reiny_step_clock before; CHECK(reiny_step_clock_read(f.scheduler,&before));
        CHECK(reiny_step_try(f.scheduler,f.request,NS,&run));
        uint64_t epoch,sequence,index;
        CHECK(reiny_step_run_id(run,&epoch,&sequence,&index)); assert(epoch==before.epoch && index==i);
        CHECK(reiny_step_wait(run,NS,&result));
        uint32_t kind; CHECK(reiny_step_result_kind(result,&kind)); assert(kind==0);
        CHECK(reiny_step_result_output(result,text("b"),text("out"),&v));
        assert(number(v,f.schema)==(i==0 ? 12:14));
        size_t required=987;
        assert(reiny_step_value_copy(v,f.wrong,NULL,0,&required)==REINY_EXEC_SCHEMA_ERROR && required==987);
        CHECK(reiny_exec_release(v));
        reiny_step_tick tick; CHECK(reiny_step_result_tick(result,&tick));
        assert(tick.epoch==epoch && tick.index==i && tick.end.nanos==7*(i+1));
        reiny_step_clock clock; CHECK(reiny_step_clock_read(f.scheduler,&clock));
        assert(clock.next_index==i+1 && clock.time.secs==UINT64_C(20000000000) && clock.time.nanos==7*(i+1));
        CHECK(reiny_exec_release(result)); CHECK(reiny_step_wait(run,NS,&result));
        CHECK(reiny_step_cancel(run,&kind)); assert(kind==3);
        CHECK(reiny_exec_release(result)); CHECK(reiny_exec_release(run));
    }
    v=value(f.schema,1);
    assert(reiny_step_output_set(f.a.stale_sink,text("out"),v)==REINY_EXEC_INVALID_HANDLE);
    CHECK(reiny_exec_release(v));
    cleanup(&f);
}
#include "component_step_lifecycle.inc"
static void semantic_values(void) {
    uint64_t schema, wrong, value, retained, map;
    CHECK(reiny_exec_schema_new(text("test.Number"),2,42,&schema));
    CHECK(reiny_exec_schema_new(text("test.Number"),2,43,&wrong));
    char assertions[] = "{\"units\":\"rad\"}";
    uint8_t bytes[] = {8, 42};
    CHECK(reiny_step_value_new_with_semantics(schema,text(assertions),
        (reiny_exec_bytes){bytes,sizeof(bytes)},&value));
    CHECK(reiny_exec_clone(value,&retained));
    CHECK(reiny_exec_release(value));
    memset(assertions,0,sizeof(assertions));
    memset(bytes,0,sizeof(bytes));
    uint8_t copied[64]; size_t required;
    CHECK(reiny_step_value_copy(retained,schema,copied,sizeof(copied),&required));
    assert(required==2 && copied[0]==8 && copied[1]==42);
    assert(reiny_step_value_copy(retained,wrong,copied,sizeof(copied),&required)
        == REINY_EXEC_SCHEMA_ERROR);
    CHECK(reiny_step_value_semantics(retained,&map));
    CHECK(reiny_exec_release(retained));
    CHECK(reiny_exec_buffer_copy(map,copied,sizeof(copied),&required));
    const char *expected="{\"units\":\"rad\"}";
    assert(required==strlen(expected) && !memcmp(copied,expected,required));
    CHECK(reiny_exec_release(map));
    CHECK(reiny_exec_release(wrong));
    CHECK(reiny_exec_release(schema));
}
int main(void) {
    semantic_values();
    mult_tick();
    lifecycle(0); lifecycle(1); lifecycle(2); invalid_output();
    puts("C managed steps passed"); return 0;
}
