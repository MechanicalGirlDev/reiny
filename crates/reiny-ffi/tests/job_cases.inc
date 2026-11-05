static void successful_job_counts_native_teardown(void) {
    state a,b; init(&a,0,1); init(&b,0,0);
    uint64_t owner,ta,tb,wa=work(&a,&ta),wb=work(&b,&tb),job,untouched=123;
    CHECK(reiny_job_owner_new(1,&owner));
    CHECK(reiny_job_start(owner,wa,&job));
    assert(reiny_job_start(owner,wa,&untouched)==REINY_EXEC_INVALID_STATE && untouched==123);
    CHECK(reiny_exec_release(wa));
    await_flag(&a,&a.entered); assert(!pthread_equal(a.lane,pthread_self()));
    assert(reiny_job_start(owner,wb,&untouched)==REINY_JOB_AT_CAPACITY && untouched==123);
    assert(b.created==0);
    progress(&a,job);
    reiny_job_snapshot_info info; uint64_t retained=snapshot(job,&info);
    assert(info.phase==1 && info.revision==1 && info.has_progress==1 && !info.cancellation_requested);
    assert(reiny_job_wait(job,1,&untouched)==REINY_EXEC_TIMEOUT && untouched==123);
    signal_flag(&a,&a.finish); await_flag(&a,&a.destroy_entered);
    released(ta,0);
    assert(reiny_job_wait_joined(job,1)==REINY_EXEC_TIMEOUT);
    assert(reiny_job_start(owner,wb,&untouched)==REINY_JOB_AT_CAPACITY && b.created==0);
    signal_flag(&a,&a.destroy_gate);
    uint64_t result,buffer;
    CHECK(reiny_job_wait(job,NS,&result)); CHECK(reiny_job_wait_joined(job,NS));
    released(ta,1); assert(a.destroyed && !a.released);
    uint32_t kind; CHECK(reiny_job_result_kind(result,&kind)); assert(kind==1);
    CHECK(reiny_job_cancel(job,&kind)); assert(kind==3);
    CHECK(reiny_job_snapshot_read(retained,&info)); assert(info.phase==1 && info.revision==1);
    CHECK(reiny_exec_release(retained));
    shutdown(owner,NS,0);
    CHECK(reiny_exec_release(owner)); CHECK(reiny_exec_release(job));
    CHECK(reiny_job_result_success(result,&buffer));
    const uint8_t expected[]={7,0,255}; bytes_equal(buffer,expected,3);
    CHECK(reiny_exec_release(result));
    assert(reiny_job_succeed(a.sink,text("stale"))==REINY_EXEC_INVALID_HANDLE);
    CHECK(reiny_exec_release(wb)); released(tb,1); assert(!b.created && !b.destroyed);
    CHECK(reiny_exec_release(ta)); CHECK(reiny_exec_release(tb));
    finish_state(&a); finish_state(&b);
}

static void cancelled_job(int mode) {
    state s; init(&s,mode,0);
    uint64_t owner,token,w=work(&s,&token),job;
    CHECK(reiny_job_owner_new(1,&owner)); CHECK(reiny_job_start(owner,w,&job));
    CHECK(reiny_exec_release(w)); await_flag(&s,&s.entered); progress(&s,job);
    uint32_t status; CHECK(reiny_job_cancel(job,&status)); assert(status==1);
    CHECK(reiny_job_cancel(job,&status)); assert(status==2);
    reiny_job_snapshot_info info; uint64_t snap=snapshot(job,&info);
    assert(info.phase==2 && info.cancellation_requested==1 && info.revision==2);
    CHECK(reiny_exec_release(snap));
    shutdown(owner,0,info.id);
    released(token,0);
    uint64_t rejected,unused=work(&s,&rejected),out=123;
    assert(reiny_job_start(owner,unused,&out)==REINY_JOB_OWNER_CLOSED && out==123);
    CHECK(reiny_exec_release(unused)); released(rejected,1); CHECK(reiny_exec_release(rejected));
    assert(s.created==1);
    assert(reiny_job_wait(job,1,&out)==REINY_EXEC_TIMEOUT);
    assert(reiny_job_wait_joined(job,1)==REINY_EXEC_TIMEOUT);
    signal_flag(&s,&s.finish);
    uint64_t result; CHECK(reiny_job_wait(job,NS,&result));
    CHECK(reiny_job_result_kind(result,&status)); assert(status==(mode==2 ? 2u:3u));
    if(mode==2) {
        int32_t code; uint64_t message;
        CHECK(reiny_job_result_error(result,&code,&message)); assert(code==-88);
        bytes_equal(message,(const uint8_t *)"job-error",9);
    } else assert(reiny_job_result_success(result,&out)==REINY_EXEC_INVALID_STATE);
    shutdown(owner,NS,0); CHECK(reiny_job_wait_joined(job,NS));
    released(token,1); assert(s.destroyed && !s.released);
    CHECK(reiny_exec_release(result)); CHECK(reiny_exec_release(job)); CHECK(reiny_exec_release(owner));
    CHECK(reiny_exec_release(token)); finish_state(&s);
}

static void owner_drop_is_cancellation_not_join(void) {
    state s; init(&s,1,0);
    uint64_t owner,token,w=work(&s,&token),job,observer,retained;
    CHECK(reiny_job_owner_new(1,&owner)); CHECK(reiny_job_start(owner,w,&job));
    CHECK(reiny_exec_release(w)); await_flag(&s,&s.entered); progress(&s,job);
    CHECK(reiny_job_subscribe(job,&observer));
    CHECK(reiny_exec_clone(job,&retained)); CHECK(reiny_exec_release(observer));
    CHECK(reiny_exec_release(job));
    uint32_t cancelled; CHECK(reiny_job_is_cancelled(s.context,&cancelled)); assert(cancelled==0);
    CHECK(reiny_exec_release(owner));
    CHECK(reiny_job_wait_cancelled(s.context,NS));
    CHECK(reiny_job_is_cancelled(s.context,&cancelled)); assert(cancelled==1);
    released(token,0);
    assert(reiny_job_wait_joined(retained,1)==REINY_EXEC_TIMEOUT);
    signal_flag(&s,&s.finish);
    CHECK(reiny_job_wait_joined(retained,NS));
    uint64_t result; CHECK(reiny_job_wait(retained,NS,&result));
    CHECK(reiny_job_result_kind(result,&cancelled)); assert(cancelled==3);
    released(token,1);
    CHECK(reiny_exec_release(result)); CHECK(reiny_exec_release(retained));
    CHECK(reiny_exec_release(token)); finish_state(&s);
}

static void retained_work_keeps_data_after_terminal_join(void) {
    state s; init(&s,0,0);
    uint64_t owner,token,w=work(&s,&token),job,result;
    CHECK(reiny_job_owner_new(1,&owner)); CHECK(reiny_job_start(owner,w,&job));
    await_flag(&s,&s.entered); progress(&s,job); signal_flag(&s,&s.finish);
    CHECK(reiny_job_wait(job,NS,&result)); CHECK(reiny_job_wait_joined(job,NS));
    shutdown(owner,NS,0);
    CHECK(reiny_exec_release(job)); CHECK(reiny_exec_release(owner));
    released(token,0); assert(s.destroyed && !s.released);
    CHECK(reiny_exec_release(w)); released(token,1);
    uint32_t kind; CHECK(reiny_job_result_kind(result,&kind)); assert(kind==1);
    CHECK(reiny_exec_release(result)); CHECK(reiny_exec_release(token)); finish_state(&s);
}

static void unused_native_data_is_released_once(void) {
    state s; init(&s,0,0);
    reiny_job_work_v1 table={sizeof(table),1,0,0,&s,create,run,destroy,release_data};
    uint64_t w,retained;
    CHECK(reiny_job_work_new(&table,&w)); CHECK(reiny_exec_clone(w,&retained));
    CHECK(reiny_exec_release(w)); assert(!s.released);
    CHECK(reiny_exec_release(retained)); assert(s.released==1 && !s.created && !s.destroyed);
    finish_state(&s);
}
