"""Owned-job error acceptance cases."""

import ctypes as C
import hashlib
import os
from pathlib import Path

import reiny_component as base
from reiny_component import jobs

from component_job_worker import (
    NS, SERVICE_SHA, STEP_SHA,
)
from component_job_support import Fixture


class ErrorCases:
    def test_create_run_and_missing_success_errors_are_retained(self):
        for mode, code in (("create_error", -77), ("exception", -10), ("missing_success", -10)):
            with self.subTest(mode=mode):
                fixture = Fixture(self)
                owner = fixture.owner()
                work, state = fixture.work(mode)
                job = fixture.track_job(owner.start(work))
                work.close()
                self.assertTrue(state.create_called.wait(10))
                if mode != "create_error":
                    self.assertTrue(state.entered.wait(10))
                    fixture.progress(job, state)
                    state.finish.set()
                with job.wait(NS) as result:
                    job.wait_joined(NS)
                    self.assertEqual(result.kind, jobs.ResultKind.APPLICATION_ERROR)
                    self.assertEqual(result.error.code, code)
                    self.assertTrue(result.error.message)
                    with job.wait(NS) as retained:
                        self.assertEqual(retained.error, result.error)
                fixture.close()

    def test_explicit_close_types_and_old_library_lazy_opt_in(self):
        fixture = Fixture(self)
        owner = fixture.owner()
        work, state = fixture.work()
        owner.close_admission()
        owner.close_admission()
        with self.assertRaises(jobs.OwnerClosedError) as closed:
            owner.start(work)
        self.assertEqual(closed.exception.status, jobs.StartStatus.OWNER_CLOSED)
        self.assertIsInstance(closed.exception, base.ReinyError)
        self.assertFalse(state.create_called.is_set())
        self.assertEqual(owner.shutdown(0), jobs.ShutdownReport(1, (), ()))
        fixture.close()
        with self.assertRaises(base.ReinyError) as invalid_capacity:
            self.api.owner(0)
        self.assertEqual(invalid_capacity.exception.status, base.Status.INVALID_ARGUMENT)
        for code in (0, -(1 << 31) - 1, True):
            with self.assertRaises(ValueError):
                jobs.ApplicationError(code, "invalid")
        for variable, expected in (("REINY_COMPONENT_SERVICE_LIBRARY", SERVICE_SHA),
                                   ("REINY_COMPONENT_STEP_LIBRARY", STEP_SHA)):
            path = Path(os.environ[variable]).resolve(strict=True)
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), expected)
            old = base.Library(path)
            with old.registry():
                pass
            with self.assertRaises(jobs.JobsUnavailableError) as unavailable:
                jobs.OwnedJobs(old)
            self.assertEqual(unavailable.exception.status, base.Status.UNSUPPORTED)
        self.assertEqual(C.sizeof(jobs._WorkTable), 16 + 5 * C.sizeof(C.c_void_p))
        self.assertEqual(C.sizeof(jobs._SnapshotInfo), 32)
