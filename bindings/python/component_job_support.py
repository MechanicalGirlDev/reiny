"""Explicit resource ownership for native job acceptance."""

from contextlib import ExitStack
import gc
import threading
import weakref

import reiny_component as base

from component_job_worker import (
    NS, State, GatedWorker,
)


class Fixture:
    def __init__(self, test):
        self.test, self.library, self.api = test, test.library, test.api
        self.resources = ExitStack()
        self.owners, self.job_handles, self.states = [], [], []
        self.references, self.callbacks, self.tokens, self.errors = [], [], [], []
        self.closed = False
        test.addCleanup(self.close)

    def owner(self, capacity=1):
        owner = self.resources.enter_context(self.api.owner(capacity))
        self.owners.append(owner)
        return owner

    def work(self, mode="success", **options):
        state = State(mode, **options)
        self.states.append(state)
        with base._registration_lock:
            before = set(base._registrations)
        work = self.resources.enter_context(self.api.work(
            lambda context: GatedWorker(context, state, self.api)))
        with base._registration_lock:
            token, = set(base._registrations) - before
            registration = base._registrations[token]
        self.test.assertFalse(registration.table.release_factory)
        self.test.assertFalse(registration.released)
        self.tokens.append(token)
        self.references.append(weakref.ref(registration))
        self.callbacks.append(weakref.ref(registration.callbacks[1]))
        self.errors.append(work.finalizer_errors)
        return work, state

    def track_job(self, job):
        self.resources.enter_context(job)
        self.job_handles.append(job)
        return job

    def start(self, owner, work, state):
        job = self.track_job(owner.start(work))
        self.test.assertTrue(state.entered.wait(10))
        self.test.assertNotEqual(state.lane, threading.get_ident())
        return job

    def progress(self, job, state):
        with job.subscribe() as observer:
            state.progress_gate.set()
            self.test.assertTrue(state.progressed.wait(10))
            snapshot = observer.changed(NS)
        return self.resources.enter_context(snapshot)

    def pinned(self, index=0):
        self.library.collect_factories()
        gc.collect()
        self.test.assertIsNotNone(self.references[index]())
        self.test.assertIsNotNone(self.callbacks[index]())
        self.test.assertFalse(self.references[index]().released)

    def close(self):
        if self.closed:
            return
        self.closed = True
        for state in self.states:
            state.progress_gate.set()
            state.finish.set()
            state.destroy_gate.set()
        for owner in self.owners:
            if owner._handle:
                report = owner.shutdown(NS)
                self.test.assertEqual((report.unresponsive, report.join_failures), ((), ()))
        for job in self.job_handles:
            if job._handle:
                job.wait_joined(NS)
        for state in self.states:
            if state.created_count:
                self.test.assertTrue(state.destroyed.wait(10))
                self.test.assertEqual(state.destroyed_count, 1)
            else:
                self.test.assertEqual(state.destroyed_count, 0)
            if state.context is not None:
                state.context.close()
        self.resources.close()
        self.library.collect_factories()
        gc.collect()
        self.test.assertTrue(all(not errors for errors in self.errors))
        for reference in self.references + self.callbacks:
            self.test.assertIsNone(reference(), "native work roots survived joined teardown/release")
