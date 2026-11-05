#define _POSIX_C_SOURCE 200809L
#include "reiny_job.h"
#include <assert.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#define NS UINT64_C(5000000000)
#define CHECK(call) do { int32_t c_=(call); if(c_) { \
    uint8_t e_[2048]; size_t n_; reiny_exec_last_error_copy(e_,sizeof(e_),&n_); \
    fprintf(stderr,"%d: %s => %d: %s\n",__LINE__,#call,c_,e_); abort(); } } while(0)
static reiny_exec_bytes text(const char *s) {
    return (reiny_exec_bytes){(const uint8_t *)s,strlen(s)};
}
typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t changed;
    pthread_t lane;
    int entered, progress_gate, progressed, finish, destroy_entered, destroy_gate;
    int created, destroyed, released, mode;
    uint64_t context, sink;
} state;
static void init(state *s,int mode,int gated_destroy) {
    memset(s,0,sizeof(*s)); s->mode=mode; s->destroy_gate=!gated_destroy;
    assert(!pthread_mutex_init(&s->mutex,NULL));
    assert(!pthread_cond_init(&s->changed,NULL));
}
static void signal_flag(state *s,int *flag) {
    assert(!pthread_mutex_lock(&s->mutex)); *flag=1;
    assert(!pthread_cond_broadcast(&s->changed));
    assert(!pthread_mutex_unlock(&s->mutex));
}
static void await_flag(state *s,const int *flag) {
    struct timespec end; assert(!clock_gettime(CLOCK_REALTIME,&end)); end.tv_sec+=10;
    assert(!pthread_mutex_lock(&s->mutex));
    while(!*flag) assert(!pthread_cond_timedwait(&s->changed,&s->mutex,&end));
    assert(!pthread_mutex_unlock(&s->mutex));
}
static void lane(state *s) { assert(pthread_equal(s->lane,pthread_self())); }
static int32_t create(void *data,uint64_t context,void **instance,uint64_t errors) {
    (void)errors; state *s=data;
    s->lane=pthread_self(); s->created++;
    CHECK(reiny_exec_clone(context,&s->context));
    *instance=s; return 0;
}
static int32_t run(void *instance,uint64_t context,uint64_t sink,uint64_t errors) {
    state *s=instance; lane(s); s->sink=sink;
    uint64_t clone=123;
    assert(reiny_exec_clone(sink,&clone)==REINY_EXEC_INVALID_STATE && clone==123);
    assert(reiny_exec_release(sink)==REINY_EXEC_INVALID_HANDLE);
    signal_flag(s,&s->entered); await_flag(s,&s->progress_gate);
    uint8_t progress[]={1,0,255}; uint32_t accepted=99;
    CHECK(reiny_job_report(context,(reiny_exec_bytes){progress,3},&accepted));
    assert(accepted==1); memset(progress,9,sizeof(progress));
    uint8_t success[]={7,0,255};
    CHECK(reiny_job_succeed(sink,(reiny_exec_bytes){success,3}));
    assert(reiny_job_succeed(sink,text("duplicate"))==REINY_EXEC_INVALID_STATE);
    memset(success,9,sizeof(success));
    signal_flag(s,&s->progressed); await_flag(s,&s->finish);
    uint32_t cancelled;
    CHECK(reiny_job_is_cancelled(context,&cancelled));
    if(s->mode) {
        assert(cancelled==1);
        CHECK(reiny_job_wait_cancelled(context,NS));
        CHECK(reiny_job_report(context,text("too late"),&accepted)); assert(accepted==0);
    } else assert(cancelled==0);
    if(s->mode==2) {
        char diagnostic[]="job-error";
        CHECK(reiny_exec_error_set(errors,-88,text(diagnostic)));
        memset(diagnostic,'x',sizeof(diagnostic)-1);
        return -88;
    }
    return 0;
}
static void destroy(void *data) {
    state *s=data; lane(s);
    signal_flag(s,&s->destroy_entered); await_flag(s,&s->destroy_gate);
    signal_flag(s,&s->destroyed);
}
static void release_data(void *data) { state *s=data; signal_flag(s,&s->released); }
static uint64_t work(state *s,uint64_t *token) {
    reiny_job_work_v1 table={sizeof(table),1,0,0,s,create,run,destroy,release_data};
    uint64_t result=123;
    CHECK(reiny_exec_factory_release_token_new(token));
    assert(reiny_job_work_new_notified(&table,*token,&result)==REINY_EXEC_INVALID_ARGUMENT);
    assert(result==123);
    table.release_factory=NULL;
    CHECK(reiny_job_work_new_notified(&table,*token,&result));
    memset(&table,0,sizeof(table));
    return result;
}
static void released(uint64_t token,uint32_t expected) {
    uint32_t actual=99;
    CHECK(reiny_exec_factory_released(token,&actual)); assert(actual==expected);
}
static void bytes_equal(uint64_t buffer,const uint8_t *expected,size_t size) {
    uint8_t copy[256]; size_t required;
    CHECK(reiny_exec_buffer_copy(buffer,copy,sizeof(copy),&required));
    assert(required==size && !memcmp(copy,expected,size));
    CHECK(reiny_exec_release(buffer));
}
static uint64_t snapshot(uint64_t job,reiny_job_snapshot_info *info) {
    uint64_t snap; CHECK(reiny_job_snapshot(job,&snap));
    CHECK(reiny_job_snapshot_read(snap,info)); return snap;
}
static void progress(state *s,uint64_t job) {
    uint64_t observer,snap,buffer;
    CHECK(reiny_job_subscribe(job,&observer));
    signal_flag(s,&s->progress_gate);
    CHECK(reiny_job_changed(observer,NS,&snap));
    CHECK(reiny_job_snapshot_progress(snap,&buffer));
    const uint8_t expected[]={1,0,255}; bytes_equal(buffer,expected,3);
    CHECK(reiny_exec_release(snap)); CHECK(reiny_exec_release(observer));
    await_flag(s,&s->progressed);
}
static void shutdown(uint64_t owner,uint64_t grace,uint64_t unresponsive) {
    uint64_t report; CHECK(reiny_job_owner_shutdown(owner,grace,&report));
    char expected[256];
    if(unresponsive) snprintf(expected,sizeof(expected),
        "{\"join_failures\":[],\"unresponsive\":[%" PRIu64 "],\"version\":1}",unresponsive);
    else snprintf(expected,sizeof(expected),"{\"join_failures\":[],\"unresponsive\":[],\"version\":1}");
    bytes_equal(report,(const uint8_t *)expected,strlen(expected));
}
static void finish_state(state *s) {
    if(s->context) CHECK(reiny_exec_release(s->context));
    assert(!pthread_cond_destroy(&s->changed)); assert(!pthread_mutex_destroy(&s->mutex));
}
#include "job_cases.inc"
int main(void) {
    successful_job_counts_native_teardown();
    cancelled_job(1); cancelled_job(2);
    owner_drop_is_cancellation_not_join();
    retained_work_keeps_data_after_terminal_join();
    unused_native_data_is_released_once();
    puts("C owned jobs: progress, capacity, cancellation, errors and native join passed");
    return 0;
}
