"""Owned-job completion acceptance cases."""


import reiny_component as base
from reiny_component import jobs

from component_job_worker import (
    NS, PROGRESS, SUCCESS,
)
from component_job_support import Fixture


class CompletionCases:
    def test_success_and_capacity_wait_for_run_and_native_destroy_join(self):
        fixture = Fixture(self)
        owner = fixture.owner()
        work, state = fixture.work(destroy_blocked=True, raw_copies=True)
        spare, unused = fixture.work()
        job = fixture.start(owner, work, state)
        with self.assertRaises(base.ReinyError) as duplicate:
            owner.start(work)
        self.assertEqual(duplicate.exception.status, base.Status.INVALID_STATE)
        work.close()
        with job.snapshot() as initial:
            self.assertEqual(initial.info.phase, jobs.Phase.RUNNING)
            self.assertFalse(initial.info.has_progress)
            with self.assertRaises(base.ReinyError):
                initial.progress
            with self.assertRaises(base.ReinyError):
                initial.result()
        retained = fixture.progress(job, state)
        retained_info = retained.info
        self.assertEqual(retained.progress, PROGRESS)
        self.assertEqual(retained_info.phase, jobs.Phase.RUNNING)
        self.assertFalse(retained_info.cancellation_requested)
        with self.assertRaises(base.DeadlineError):
            job.wait(1)
        with self.assertRaises(base.DeadlineError):
            job.wait_joined(1)
        with self.assertRaises(jobs.CapacityError) as capacity:
            owner.start(spare)
        self.assertEqual(capacity.exception.status, jobs.StartStatus.AT_CAPACITY)
        self.assertFalse(unused.create_called.is_set())
        fixture.pinned()
        state.finish.set()
        self.assertTrue(state.destroy_entered.wait(10))
        self.assertFalse(state.destroyed.is_set())
        with self.assertRaises(base.DeadlineError):
            job.wait(1)
        with self.assertRaises(base.DeadlineError):
            job.wait_joined(1)
        with self.assertRaises(jobs.CapacityError):
            owner.start(spare)
        fixture.pinned()
        state.destroy_gate.set()
        with job.wait(NS) as result:
            job.wait_joined(NS)
            self.assertEqual(result.kind, jobs.ResultKind.SUCCEEDED)
            self.assertEqual(result.success, SUCCESS)
            with self.assertRaises(base.ReinyError):
                result.error
            self.assertEqual(job.cancel(), jobs.CancelStatus.FINISHED)
            self.assertEqual(retained.info, retained_info)
            self.assertEqual(retained.progress, PROGRESS)
            with job.snapshot() as final, final.result() as copied:
                self.assertEqual(final.info.phase, jobs.Phase.COMPLETE)
                self.assertEqual(copied.success, SUCCESS)
                self.assertGreater(final.info.revision, retained_info.revision)
            state.context.wait_cancelled(NS)  # Completion also resolves this wait.
            self.assertFalse(state.context.is_cancelled)
            self.assertFalse(state.context.report(b"after completion"))
            for borrowed in (state.borrowed_create, state.borrowed_run, state.borrowed_success):
                with self.assertRaises(base.ReinyError):
                    borrowed._id()
            with self.assertRaises(base.ReinyError) as stale:
                self.api._call("succeed", state.native_sink, base._bytes(b"stale"))
            self.assertEqual(stale.exception.status, base.Status.INVALID_HANDLE)
            self.assertEqual(owner.shutdown(NS), jobs.ShutdownReport(1, (), ()))
            owner.close()
            job.close()
            self.assertEqual(result.success, SUCCESS)  # Retained immutable result is independent.
        spare.close()
        self.assertFalse(unused.create_called.is_set())
        fixture.close()

    def test_cancel_timeout_retry_preserves_application_error_after_success_write(self):
        for mode, expected in (("cancel_success", jobs.ResultKind.CANCELLED),
                               ("application_error", jobs.ResultKind.APPLICATION_ERROR)):
            with self.subTest(mode=mode):
                fixture = Fixture(self)
                owner = fixture.owner()
                retry = fixture.resources.enter_context(owner.clone())
                work, state = fixture.work(mode)
                job = fixture.start(owner, work, state)
                work.close()
                fixture.progress(job, state)
                self.assertEqual(job.cancel(), jobs.CancelStatus.REQUESTED)
                self.assertEqual(job.cancel(), jobs.CancelStatus.ALREADY_REQUESTED)
                with job.snapshot() as snapshot:
                    self.assertEqual(snapshot.info.phase, jobs.Phase.CANCELLING)
                    self.assertTrue(snapshot.info.cancellation_requested)
                    job_id = snapshot.info.id
                report = owner.shutdown(0)
                self.assertEqual(report, jobs.ShutdownReport(1, (job_id,), ()))
                owner.close()  # Retained clone owns the explicit shutdown retry.
                blocked, unused = fixture.work()
                with self.assertRaises(jobs.OwnerClosedError):
                    retry.start(blocked)
                self.assertFalse(unused.create_called.is_set())
                with self.assertRaises(base.DeadlineError):
                    job.wait(1)
                with self.assertRaises(base.DeadlineError):
                    job.wait_joined(1)
                fixture.pinned()
                state.finish.set()
                with job.wait(NS) as result:
                    self.assertEqual(result.kind, expected)
                    if expected == jobs.ResultKind.APPLICATION_ERROR:
                        self.assertEqual(result.error, jobs.ApplicationFailure(-88, "job-error"))
                    else:
                        with self.assertRaises(base.ReinyError):
                            result.error
                    with self.assertRaises(base.ReinyError):
                        result.success
                self.assertEqual(retry.shutdown(NS), jobs.ShutdownReport(1, (), ()))
                job.wait_joined(NS)
                retry.close()
                fixture.close()
