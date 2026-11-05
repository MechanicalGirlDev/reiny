"""Local-step lifecycle acceptance cases."""


import reiny_component as base
from reiny_component import steps

from component_step_node import (
    NS, INITIAL, number,
)
from component_step_support import Fixture


class LifecycleCases:
    def test_callback_exception_and_invalid_outputs_have_typed_progress(self):
        for mode, role, expected, attempted, completed in (
                ("exception", "a", steps.FailureKind.NODE_ERROR, ("a",), ()),
                ("invalid_output", "b", steps.FailureKind.INVALID_OUTPUTS, ("a", "b"), ("a",))):
            with self.subTest(mode=mode):
                fixture = Fixture(self, **{f"mode_{role}": mode})
                with fixture.request() as request, fixture.scheduler.try_step(request, NS) as run, \
                        run.wait(NS) as result:
                    self.assertEqual(result.kind, steps.ResultKind.FAILED_AFTER_DISPATCH)
                    failure = result.failure
                    self.assertEqual((failure.kind, failure.instance), (expected, role))
                    self.assertEqual((failure.attempted, failure.completed), (attempted, completed))
                    self.assertEqual(fixture.scheduler.clock.next_index, 0)
                    self.assertEqual(fixture.scheduler.clock.time, INITIAL)
                    with self.assertRaises(steps.FaultedError):
                        fixture.scheduler.try_step(request, NS)
                    with self.assertRaises(base.ReinyError):
                        result.tick
                    with self.assertRaises(base.ReinyError):
                        result.output("b", "out")
                fixture.close()

    def test_gated_cancel_deadline_stop_and_destroy_retain_roots_until_join(self):
        for mode in ("cancel", "deadline", "stop"):
            with self.subTest(mode=mode):
                fixture = Fixture(self, mode_a="blocked", destroy_blocked=(mode == "cancel"),
                                  stop_ns=30_000_000)
                request = fixture.request()
                self.addCleanup(request.close)
                run = fixture.scheduler.try_step(request, 1_000_000_000 if mode == "deadline" else NS)
                self.addCleanup(run.close)
                state = fixture.states[0]
                self.assertTrue(state.entered.wait(10))
                if mode != "deadline":
                    with self.assertRaises(steps.BusyError):
                        fixture.scheduler.try_step(request, NS)
                    with self.assertRaises(base.DeadlineError):
                        run.wait(1)
                self.assertFalse(state.stopped.is_set())
                fixture.assert_a_pinned()
                if mode == "cancel":
                    self.assertEqual(run.cancel(), steps.CancelStatus.REQUESTED)
                elif mode == "stop":
                    fixture.host.request_stop()
                with run.wait(NS) as result:
                    self.assertEqual(result.kind, steps.ResultKind.FAILED_AFTER_DISPATCH)
                    self.assertEqual(result.failure.kind, {
                        "cancel": steps.FailureKind.CANCELLED,
                        "deadline": steps.FailureKind.TIMED_OUT,
                        "stop": steps.FailureKind.HOST_STOPPED,
                    }[mode])
                    self.assertEqual((result.failure.attempted, result.failure.completed), (("a",), ()))
                    retained_info = result.info
                with run.wait(NS) as result:
                    self.assertEqual(result.info, retained_info)
                terminal = fixture.host.wait_terminal(NS)
                self.assertEqual(terminal["phase"], base.Phase.UNRESPONSIVE)
                self.assertIn("a", terminal["unjoined"])
                with self.assertRaises(base.DeadlineError):
                    fixture.host.wait_joined(1)
                self.assertFalse(state.stopped.is_set())
                self.assertFalse(state.destroyed.is_set())
                run.close()  # Observation release cannot free the active callback.
                fixture.assert_a_pinned()
                state.gate.set()
                if mode == "cancel":
                    self.assertTrue(state.destroy_entered.wait(10))
                    self.assertFalse(state.destroyed.is_set())
                    with self.assertRaises(base.DeadlineError):
                        fixture.host.wait_joined(1)
                    fixture.assert_a_pinned()
                    state.destroy_gate.set()
                fixture.host.wait_joined(NS)
                self.assertTrue(state.destroyed.is_set())
                fixture.close()

    def test_releasing_run_observer_does_not_cancel_real_execution(self):
        fixture = Fixture(self, mode_a="blocked")
        scheduler_observer = fixture.scheduler.clone()
        self.addCleanup(scheduler_observer.close)
        with fixture.request() as request:
            run = fixture.scheduler.try_step(request, NS)
            observer = run.clone()
            self.addCleanup(observer.close)
            self.assertTrue(fixture.states[0].entered.wait(10))
            run.close()
            with self.assertRaises(steps.BusyError):
                fixture.scheduler.try_step(request, NS)
            fixture.states[0].gate.set()
            with observer.wait(NS) as result, result.output("b", "out") as value:
                self.assertEqual(result.kind, steps.ResultKind.COMMITTED)
                self.assertEqual(number(value, fixture.schema), 12)
            fixture.close()
            self.assertEqual(scheduler_observer.clock.phase, steps.ClockPhase.STOPPED)
            with self.assertRaises(steps.StoppedError):
                scheduler_observer.try_step(request, NS)
