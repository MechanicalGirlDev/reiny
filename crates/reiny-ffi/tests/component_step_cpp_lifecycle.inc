// Included only by the independent native C++ step client.
static void gated_lifecycle(bool stop) {
    Fixture fixture(Mode::blocked, !stop);
    auto entered = fixture.a->callback_gate.entered.get_future();
    auto destroyed = fixture.a->destroy_gate.entered.get_future();
    auto run = admitted(fixture);
    ready(entered);
    assert(fixture.a->effects == 1);
    const auto busy = fixture.execution.scheduler.try_step(fixture.input, timeout);
    assert(busy.status == rs::Admission::busy && !busy.run);
    expect(REINY_EXEC_TIMEOUT, [&] { run.wait(1ns); });
    assert(fixture.execution.scheduler.clock().phase == rs::ClockPhase::running);
    if (stop) {
        fixture.execution.host.request_stop();
    } else {
        assert(run.cancel() == rs::Cancellation::requested);
    }
    auto result = run.wait(timeout);
    assert(result.kind() == rs::ResultKind::failed_after_dispatch);
    const auto info = result.info().text();
    assert(info.find(stop ? "\"kind\":6" : "\"kind\":4") != std::string::npos);
    assert(info.find("\"attempted\":[\"a\"]") != std::string::npos);
    assert(info.find("\"completed\":[]") != std::string::npos);
    expect(REINY_EXEC_INVALID_STATE, [&] { result.tick(); });
    expect(REINY_EXEC_INVALID_STATE, [&] { result.output("a", "out"); });
    assert(run.wait(timeout).kind() == result.kind());
    fixture.execution.host.wait_terminal(timeout);
    assert(fixture.execution.host.phase() == REINY_EXEC_UNRESPONSIVE);
    expect(REINY_EXEC_TIMEOUT, [&] { fixture.execution.host.wait_joined(1ns); });
    assert(fixture.a->stopped == 0 && fixture.a->destroyed == 0);
    auto rejected = fixture.execution.scheduler.try_step(fixture.input, timeout);
    assert(!rejected.run);
    assert(rejected.status == rs::Admission::faulted || rejected.status == rs::Admission::stopped);
    result.close(); run.close(); // Neither handle owns callback termination.
    fixture.a->callback_gate.release.set_value();
    if (!stop) {
        ready(destroyed);
        expect(REINY_EXEC_TIMEOUT, [&] { fixture.execution.host.wait_joined(1ns); });
        assert(fixture.a->destroyed == 0);
        fixture.a->destroy_gate.release.set_value();
    }
    fixture.finish();
    assert(fixture.a->effects == 1); // No false rollback after cancellation/stop.
}

static void exception_and_output_failures() {
    for (const auto mode : {Mode::standard_exception, Mode::unknown_exception,
                            Mode::invalid_output, Mode::missing_output}) {
        Fixture fixture(mode);
        auto run = admitted(fixture);
        auto result = run.wait(timeout);
        assert(result.kind() == rs::ResultKind::failed_after_dispatch);
        const auto info = result.info().text();
        assert(info.find(mode == Mode::standard_exception || mode == Mode::unknown_exception
            ? "\"kind\":1" : "\"kind\":3") != std::string::npos);
        assert(info.find("\"attempted\":[\"a\"]") != std::string::npos);
        assert(info.find("\"completed\":[]") != std::string::npos);
        const auto clock = fixture.execution.scheduler.clock();
        assert(clock.next_index == 0 && clock.time.secs == origin && clock.time.nanos == 0);
        fixture.finish();
    }
    {
        Fixture fixture(Mode::prepare_exception);
        fixture.execution.host.wait_terminal(timeout);
        assert(fixture.execution.host.phase() == REINY_EXEC_FAILED);
        fixture.finish();
    }
    {
        Fixture fixture(Mode::stop_exception);
        auto run = admitted(fixture);
        assert(run.wait(timeout).kind() == rs::ResultKind::committed);
        fixture.finish(); // Stop exceptions must not skip lane destruction.
    }
}
