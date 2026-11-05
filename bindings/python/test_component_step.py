"""Real Local-step ABI acceptance; no scheduler mocks or service-test edits."""

import ctypes as C
import hashlib
import os
from pathlib import Path

import reiny_component as base
from reiny_component import steps
import unittest

from component_step_node import (
    NS, INITIAL, DT, LIBRARY_SHA, HEADER_SHA, SERVICE_SHA, SERVICE_HEADER_SHA, encoded,
    number, NodeState, Node,
)
from component_step_support import Fixture
from component_step_value_cases import ValueCases
from component_step_registration_cases import RegistrationCases
from component_step_lifecycle_cases import LifecycleCases


class ManagedStepTests(ValueCases, RegistrationCases, LifecycleCases, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.path = Path(os.environ["REINY_STEP_LIBRARY"]).resolve(strict=True)
        if hashlib.sha256(cls.path.read_bytes()).hexdigest() != LIBRARY_SHA:
            raise RuntimeError("requires the exact frozen notified-step library")
        root = Path(__file__).resolve().parents[2]
        headers = root / "crates/reiny-ffi/include"
        for name, expected in (("reiny_component_step.h", HEADER_SHA),
                               ("reiny_component.h", SERVICE_HEADER_SHA)):
            if hashlib.sha256((headers / name).read_bytes()).hexdigest() != expected:
                raise RuntimeError(f"frozen header mismatch: {name}")
        cls.library = base.Library(cls.path)
        cls.api = steps.LocalSteps(cls.library)
        print(f"PYTHON_STEP_LIBRARY sha256={LIBRARY_SHA} path={cls.path}", flush=True)
        print(f"PYTHON_STEP_HEADER sha256={HEADER_SHA}", flush=True)


if __name__ == "__main__":
    unittest.main()
