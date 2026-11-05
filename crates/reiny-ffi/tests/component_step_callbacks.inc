/* Included by the C step client: callback state, gates and lane operations. */
typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t changed;
    pthread_t lane;
    int role, blocked, entered, gate, stopped, destroyed, released, bad_output;
    int destroy_blocked, destroy_entered, destroy_gate;
    uint64_t schema, wrong, stale_sink;
} state;
static void signal_flag(state *s, int *flag) {
    assert(!pthread_mutex_lock(&s->mutex));
    *flag=1;
    assert(!pthread_cond_broadcast(&s->changed));
    assert(!pthread_mutex_unlock(&s->mutex));
}
static void await_flag(state *s, const int *flag) {
    struct timespec deadline;
    assert(!clock_gettime(CLOCK_REALTIME,&deadline));
    deadline.tv_sec+=10;
    assert(!pthread_mutex_lock(&s->mutex));
    while(!*flag) assert(!pthread_cond_timedwait(&s->changed,&s->mutex,&deadline));
    assert(!pthread_mutex_unlock(&s->mutex));
}
static void lane(state *s) { assert(pthread_equal(s->lane,pthread_self())); }
static uint64_t value(uint64_t schema, uint8_t n) {
    uint64_t out;
    CHECK(reiny_step_value_new(schema,(reiny_exec_bytes){&n,1},&out));
    return out;
}
static uint8_t number(uint64_t val, uint64_t schema) {
    uint8_t n=0; size_t size=0;
    CHECK(reiny_step_value_copy(val,schema,&n,1,&size));
    assert(size==1);
    return n;
}
static uint8_t input(uint64_t inputs, const char *port, uint64_t schema) {
    uint64_t v; CHECK(reiny_step_input_get(inputs,text(port),&v));
    uint8_t n=number(v,schema); CHECK(reiny_exec_release(v)); return n;
}
static int32_t create(void *data,uint64_t ctx,void **instance,uint64_t error) {
    (void)ctx; (void)error;
    state *s=data; s->lane=pthread_self(); *instance=s; return 0;
}
static int32_t prepare(void *data,uint64_t ctx,uint64_t error) {
    (void)ctx; (void)error; lane(data); return 0;
}
static int32_t step(void *data,uint64_t ctx,const reiny_step_tick *tick,
                    uint64_t inputs,uint64_t outputs,uint64_t error) {
    (void)ctx; (void)error;
    state *s=data; lane(s);
    assert(tick->start.secs>=UINT64_C(20000000000));
    assert(tick->dt.secs==0 && tick->dt.nanos==7);
    assert(tick->end.secs==tick->start.secs);
    uint64_t clone=123;
    assert(reiny_exec_clone(outputs,&clone)==REINY_EXEC_INVALID_STATE && clone==123);
    assert(reiny_exec_release(outputs)==REINY_EXEC_INVALID_HANDLE);
    CHECK(reiny_exec_clone(inputs,&clone)); CHECK(reiny_exec_release(clone));
    s->stale_sink=outputs;
    if(s->blocked) {
        signal_flag(s,&s->entered);
        await_flag(s,&s->gate);
    }
    uint8_t n=input(inputs,"x",s->schema);
    n+=(s->role==0 ? input(inputs,"prev",s->schema) : 1);
    uint64_t v=value(s->bad_output ? s->wrong : s->schema,n);
    CHECK(reiny_step_output_set(outputs,text("out"),v));
    assert(reiny_step_output_set(outputs,text("out"),v)==REINY_EXEC_INVALID_STATE);
    CHECK(reiny_exec_release(v));
    return 0;
}
static int32_t stop(void *data,uint64_t ctx,uint32_t reason,uint64_t error) {
    (void)ctx; (void)reason; (void)error;
    state *s=data; lane(s); signal_flag(s,&s->stopped); return 0;
}
static void destroy(void *data) {
    state *s=data; lane(s);
    if(s->destroy_blocked) {
        signal_flag(s,&s->destroy_entered);
        await_flag(s,&s->destroy_gate);
    }
    signal_flag(s,&s->destroyed);
}
static void release_factory(void *data) { state *s=data; signal_flag(s,&s->released); }
