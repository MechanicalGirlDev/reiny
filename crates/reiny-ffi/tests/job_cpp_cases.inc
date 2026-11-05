// Included only by the independent C++ job client.
static void cancelled_shutdown_retry(Mode mode) {
    auto state = std::make_shared<State>();
    state->mode = mode;
    auto owner = rj::Owner::create(1);
    auto retained_owner = owner.clone();
    auto item = work(state);
    auto job = start(owner, item);
    item.close();
    state->entered.wait();
    auto snapshot = progress(state, job);
    owner.close(); // A surviving explicit owner claim keeps ownership alive.
    assert(!state->retained.view().cancelled());
    assert(job.cancel() == rj::Cancellation::requested);
    assert(job.cancel() == rj::Cancellation::already_requested);
    auto cancelling = job.snapshot();
    const auto info = cancelling.info();
    assert(info.phase == rj::Phase::cancelling && info.cancellation_requested);
    assert(info.revision == 2);
    timed_out_report(retained_owner.shutdown(0ns), info.id);
    assert(state->destroyed == 0 && !state->data.expired());
    auto unused_state = std::make_shared<State>();
    auto unused = work(unused_state);
    rejected(retained_owner.start(unused), rj::StartStatus::owner_closed);
    assert(unused_state->created == 0);
    unused.close();
    assert(unused_state->data.expired() && unused_state->data_destroyed == 1);
    expect(REINY_EXEC_TIMEOUT, [&] { job.wait(1ns); });
    expect(REINY_EXEC_TIMEOUT, [&] { job.wait_joined(1ns); });
    state->finish.open();
    auto result = job.wait(timeout);
    if (mode == Mode::cancelled_error) {
        assert(result.kind() == rj::Kind::application_error);
        const auto error = result.error();
        assert(error.code == -88 && error.message == "job-error");
    } else {
        assert(result.kind() == rj::Kind::cancelled);
    }
    expect(REINY_EXEC_INVALID_STATE, [&] { result.success(); });
    joined_report(retained_owner.shutdown(timeout)); // Same owner retries actual join.
    job.wait_joined(timeout);
    assert(state->destroyed == 1 && state->data.expired() && state->data_destroyed == 1);
    assert(snapshot.info().phase == rj::Phase::running && snapshot.progress().bytes() == progress_bytes);
    retained_owner.close(); job.close();
    if (mode == Mode::cancelled_error) assert(result.error().code == -88);
    state->retained.close();
}

static void dropped_observers_and_owner() {
    auto state = std::make_shared<State>();
    state->mode = Mode::cancelled_success;
    auto owner = rj::Owner::create(1);
    auto item = work(state);
    auto job = start(owner, item);
    item.close();
    state->entered.wait();
    auto snapshot = progress(state, job);
    auto observer = job.subscribe();
    auto retained_job = job.clone();
    observer.close(); job.close();
    assert(!state->retained.view().cancelled());
    owner.close(); // Last owner drop requests cancellation, not join.
    state->retained.view().wait_cancelled(timeout);
    assert(state->retained.view().cancelled());
    assert(!state->data.expired() && state->destroyed == 0);
    expect(REINY_EXEC_TIMEOUT, [&] { retained_job.wait_joined(1ns); });
    state->finish.open();
    retained_job.wait_joined(timeout);
    auto result = retained_job.wait(timeout);
    assert(result.kind() == rj::Kind::cancelled && state->data.expired());
    assert(snapshot.progress().bytes() == progress_bytes);
    state->retained.close();
}

static void retained_and_unused_work() {
    {
        auto state = std::make_shared<State>();
        auto item = work(state);
        auto retained = item.clone();
        item.close();
        assert(!state->data.expired());
        retained.close();
        assert(state->data.expired() && state->data_destroyed == 1);
        assert(state->created == 0 && state->destroyed == 0);
    }
    {
        auto state = std::make_shared<State>();
        auto owner = rj::Owner::create(1);
        auto item = work(state);
        auto job = start(owner, item);
        completed(state, job);
        joined_report(owner.shutdown(timeout));
        assert(state->destroyed == 1 && state->data_destroyed == 0 && !state->data.expired());
        auto result = job.wait(timeout);
        owner.close(); job.close();
        auto retained = item.clone();
        item.close();
        assert(!state->data.expired());
        retained.close();
        assert(state->data.expired() && state->data_destroyed == 1);
        assert(result.success().bytes() == success_bytes);
        state->retained.close();
    }
    {
        auto owner = rj::Owner::create(1);
        owner.close_admission();
        auto state = std::make_shared<State>();
        auto item = work(state);
        rejected(owner.start(item), rj::StartStatus::owner_closed);
        assert(state->created == 0);
        auto other = rj::Owner::create(1); // Failed start did not consume this work.
        auto job = start(other, item);
        item.close();
        completed(state, job);
        joined_report(other.shutdown(timeout));
        state->retained.close();
    }
}

static void callback_errors() {
    for (auto mode : {Mode::standard_error, Mode::unknown_error, Mode::missing_success}) {
        auto state = std::make_shared<State>();
        state->mode = mode;
        auto owner = rj::Owner::create(1);
        auto item = work(state);
        auto job = start(owner, item);
        item.close();
        state->entered.wait();
        auto snapshot = progress(state, job);
        state->finish.open();
        auto result = job.wait(timeout);
        assert(result.kind() == rj::Kind::application_error);
        assert(result.error().code == REINY_EXEC_CALLBACK_ERROR);
        job.wait_joined(timeout);
        joined_report(owner.shutdown(timeout));
        assert(state->destroyed == 1 && state->data.expired());
        state->retained.close();
    }
    for (unsigned mode : {0u, 1u, 2u}) {
        auto owner = rj::Owner::create(1);
        auto item = rj::Work::create([mode](rj::ContextView) -> std::unique_ptr<rj::Task> {
            if (mode == 0) throw std::runtime_error("C++ job factory exception");
            if (mode == 1) throw 17;
            return nullptr;
        });
        auto job = start(owner, item);
        auto result = job.wait(timeout);
        assert(result.kind() == rj::Kind::application_error);
        assert(result.error().code == REINY_EXEC_CALLBACK_ERROR);
        job.wait_joined(timeout);
        joined_report(owner.shutdown(timeout));
    }
}
