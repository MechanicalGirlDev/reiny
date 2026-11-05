/* Included by the C client so lifecycle cases use the same real graph fixture. */
static void lifecycle(int mode) {
    fixture f; setup(&f,mode==0 ? 2:1,0);
    f.a.destroy_blocked=mode==0;
    uint64_t run,result,untouched=123;
    CHECK(reiny_step_try(f.scheduler,f.request,mode==1 ? UINT64_C(1000000000):NS,&run));
    await_flag(&f.a,&f.a.entered);
    if(mode==0) {
        CHECK(reiny_exec_release(f.registry)); f.registry=0;
        uint32_t released=99;
        CHECK(reiny_exec_factory_released(f.tokens[0],&released)); assert(released==0);
    }
    if(mode!=1) {
        assert(reiny_step_try(f.scheduler,f.request,NS,&untouched)==REINY_STEP_BUSY && untouched==123);
        assert(reiny_step_wait(run,1,&untouched)==REINY_EXEC_TIMEOUT && untouched==123);
    }
    uint32_t cancellation;
    if(mode==0) {
        CHECK(reiny_step_cancel(run,&cancellation)); assert(cancellation==1);
    } else if(mode==2) CHECK(reiny_exec_host_request_stop(f.host));
    CHECK(reiny_step_wait(run,NS,&result));
    uint32_t kind; CHECK(reiny_step_result_kind(result,&kind)); assert(kind==3);
    reiny_step_tick tick;
    assert(reiny_step_result_tick(result,&tick)==REINY_EXEC_INVALID_STATE);
    uint64_t info; CHECK(reiny_step_result_info(result,&info));
    char json[4096]={0}; size_t size;
    CHECK(reiny_exec_buffer_copy(info,(uint8_t *)json,sizeof(json)-1,&size));
    char expected[32]; snprintf(expected,sizeof(expected),"\"kind\":%d",mode==0?4:mode==1?5:6);
    assert(strstr(json,expected));
    assert(strstr(json,"\"attempted\":[\"a\"]") && strstr(json,"\"completed\":[]"));
    CHECK(reiny_exec_release(info));
    CHECK(reiny_exec_host_wait_terminal(f.host,NS,&info));
    uint32_t phase; CHECK(reiny_exec_host_phase(f.host,&phase));
    assert(phase==REINY_EXEC_UNRESPONSIVE);
    CHECK(reiny_exec_release(info));
    assert(reiny_exec_host_wait_joined(f.host,1)==REINY_EXEC_TIMEOUT);
    assert(!f.a.stopped && !f.a.destroyed);
    if(mode==0) {
        uint32_t released=99;
        CHECK(reiny_exec_factory_released(f.tokens[0],&released)); assert(released==0);
    }
    /* Dropping result/run is observation only; the lane still owns callback data. */
    CHECK(reiny_exec_release(result)); CHECK(reiny_exec_release(run));
    signal_flag(&f.a,&f.a.gate);
    if(mode==0) {
        await_flag(&f.a,&f.a.destroy_entered);
        assert(reiny_exec_host_wait_joined(f.host,1)==REINY_EXEC_TIMEOUT);
        uint32_t released=99;
        CHECK(reiny_exec_factory_released(f.tokens[0],&released)); assert(released==0);
        signal_flag(&f.a,&f.a.destroy_gate);
    }
    cleanup(&f);
}

static void invalid_output(void) {
    fixture f; setup(&f,0,1);
    uint64_t run,result,info;
    CHECK(reiny_step_try(f.scheduler,f.request,NS,&run));
    CHECK(reiny_step_wait(run,NS,&result));
    uint32_t kind; CHECK(reiny_step_result_kind(result,&kind)); assert(kind==3);
    CHECK(reiny_step_result_info(result,&info));
    char json[4096]={0}; size_t size;
    CHECK(reiny_exec_buffer_copy(info,(uint8_t *)json,sizeof(json)-1,&size));
    assert(strstr(json,"\"kind\":3") && strstr(json,"\"instance\":\"b\""));
    assert(strstr(json,"\"attempted\":[\"a\",\"b\"]") && strstr(json,"\"completed\":[\"a\"]"));
    reiny_step_clock clock; CHECK(reiny_step_clock_read(f.scheduler,&clock));
    assert(clock.next_index==0 && clock.time.nanos==0);
    CHECK(reiny_exec_release(info)); CHECK(reiny_exec_release(result)); CHECK(reiny_exec_release(run));
    cleanup(&f);
}
