"""Owned-job ownership acceptance cases."""

import ctypes as C
import gc

import reiny_component as base
from reiny_component import jobs

from component_job_worker import (
    NS, SUCCESS,
)
from component_job_support import Fixture


class OwnershipCases:
    def test_abandoned_observers_and_owner_drop_do_not_join_or_free_live_work(self):
        fixture = Fixture(self)
        owner = fixture.owner()
        work, state = fixture.work("owner_drop")
        job = fixture.start(owner, work, state)
        work.close()
        fixture.progress(job, state)
        observer = job.subscribe()
        retained = fixture.track_job(job.clone())
        observer.close()
        job.close()
        self.assertFalse(state.context.is_cancelled)
        owner.close()  # Last owner requests cancellation but cannot await native join.
        state.context.wait_cancelled(NS)
        self.assertTrue(state.context.is_cancelled)
        self.assertFalse(state.destroyed.is_set())
        with self.assertRaises(base.DeadlineError):
            retained.wait_joined(1)
        with retained.snapshot() as snapshot:
            self.assertEqual(snapshot.info.phase, jobs.Phase.CANCELLING)
        fixture.pinned()
        state.finish.set()
        retained.wait_joined(NS)
        with retained.wait(NS) as result:
            self.assertEqual(result.kind, jobs.ResultKind.CANCELLED)
        fixture.close()

    def test_retained_work_handle_keeps_roots_after_result_and_native_join(self):
        fixture = Fixture(self)
        owner = fixture.owner()
        work, state = fixture.work()
        retained_work = fixture.resources.enter_context(work.clone())
        self.assertIs(retained_work.finalizer_errors, work.finalizer_errors)
        job = fixture.start(owner, work, state)
        work.close()
        fixture.progress(job, state)
        state.finish.set()
        with job.wait(NS) as result:
            job.wait_joined(NS)
            self.assertTrue(state.destroyed.is_set())
            self.assertEqual(owner.shutdown(NS), jobs.ShutdownReport(1, (), ()))
            owner.close()
            job.close()
            fixture.pinned()  # Token observes data ownership, not merely join.
            retained_work.close()
            self.assertTrue(fixture.references[0]().released)
            self.library.collect_factories()
            gc.collect()
            self.assertIsNone(fixture.references[0]())
            self.assertIsNone(fixture.callbacks[0]())
            self.assertEqual(result.success, SUCCESS)
        fixture.close()

    def test_unused_work_and_failed_import_preserve_owned_token_transaction(self):
        fixture = Fixture(self)
        work, state = fixture.work()
        clone = fixture.resources.enter_context(work.clone())
        work.close()
        fixture.pinned()
        self.assertFalse(state.create_called.is_set())
        clone.close()
        self.assertTrue(fixture.references[0]().released)
        fixture.close()
        registration = jobs._WorkRegistration(self.library, lambda context: jobs.Worker())
        self.assertFalse(registration.table.release_factory)
        token = registration.release_token
        alias = token.clone()
        try:
            output = base._U64(123)
            registration.table.flags = 1
            with self.assertRaises(base.ReinyError) as invalid:
                self.api._call("work_new_notified", C.byref(registration.table),
                               token._id(), C.byref(output))
            self.assertEqual(invalid.exception.status, base.Status.INVALID_ARGUMENT)
            self.assertEqual(output.value, 123)
            self.assertFalse(token.released)
            registration.table.flags = 0
            self.api._call("work_new_notified", C.byref(registration.table),
                           token._id(), C.byref(output))
            with jobs.Work(self.library, output.value):
                token.close()
                self.assertFalse(alias.released)
                unchanged = base._U64(456)
                with self.assertRaises(base.ReinyError):
                    self.api._call("work_new_notified", C.byref(registration.table),
                                   alias._id(), C.byref(unchanged))
                self.assertEqual(unchanged.value, 456)
            self.assertTrue(alias.released)
        finally:
            alias.close()
            token.close()
