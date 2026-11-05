"""Local-step registration acceptance cases."""

import ctypes as C
import hashlib
import os
from pathlib import Path

import reiny_component as base
from reiny_component import steps

from component_step_node import (
    SERVICE_SHA,
)


class RegistrationCases:
    def test_old_service_library_import_remains_lazy(self):
        path = Path(os.environ["REINY_COMPONENT_SERVICE_LIBRARY"]).resolve(strict=True)
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), SERVICE_SHA)
        old = base.Library(path)
        with old.registry():
            pass
        with self.assertRaises(steps.StepsUnavailableError) as unavailable:
            steps.LocalSteps(old)
        self.assertEqual(unavailable.exception.status, base.Status.UNSUPPORTED)

    def test_registration_failure_preserves_native_token_ownership(self):
        with self.library.schema("test.Number", 42) as schema, \
                self.api.schema({}, {"out": schema}) as node_schema:
            registration = steps._StepRegistration(self.library, lambda ctx: steps.StepComponent())
            self.assertFalse(registration.table.release_factory)
            with self.library.registry() as registry:
                registry.seal()
                with self.assertRaises(base.ReinyError):
                    self.api._call("register_notified", registry._id(), base._text("node"),
                                   base._text("1"), node_schema._id(),
                                   C.byref(registration.table), registration.release_token._id())
                self.assertFalse(registration.released)
            with self.library.registry() as registry:
                self.api._call("register_notified", registry._id(), base._text("node"),
                               base._text("1"), node_schema._id(),
                               C.byref(registration.table), registration.release_token._id())
                self.assertFalse(registration.released)
                with self.assertRaises(base.ReinyError):
                    self.api._call("register_notified", registry._id(), base._text("another"),
                                   base._text("1"), node_schema._id(),
                                   C.byref(registration.table), registration.release_token._id())
            self.assertTrue(registration.released)
            registration.release_token.close()
