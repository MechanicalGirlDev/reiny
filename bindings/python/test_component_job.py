"""Actual owned native jobs: gates, joins and immutable retained observations."""

import ctypes as C
import gc
import hashlib
import os
from pathlib import Path

import reiny_component as base
from reiny_component import jobs
import unittest

from component_job_worker import (
    NS, LIBRARY_SHA, HEADER_SHA, SERVICE_SHA, STEP_SHA, PROGRESS, SUCCESS, State,
    GatedWorker,
)
from component_job_support import Fixture
from component_job_completion_cases import CompletionCases
from component_job_ownership_cases import OwnershipCases
from component_job_error_cases import ErrorCases


class OwnedJobTests(CompletionCases, OwnershipCases, ErrorCases, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.path = Path(os.environ["REINY_JOB_LIBRARY"]).resolve(strict=True)
        if hashlib.sha256(cls.path.read_bytes()).hexdigest() != LIBRARY_SHA:
            raise RuntimeError("requires the exact frozen owned-job native library")
        include = Path(__file__).resolve().parents[2] / "crates/reiny-ffi/include"
        hashes = {
            "reiny_job.h": HEADER_SHA,
            "reiny_component.h": "978db314fea91fe62800565890d19bad315a68e3440c4d08c0ce9522752a75ed",
            "reiny_component_step.h": "58c973c95ad53a08d9de21a6ceaae4bc05c22c60a6fb9ad6e0d0c5a4431f0d1b",
        }
        for name, expected in hashes.items():
            if hashlib.sha256((include / name).read_bytes()).hexdigest() != expected:
                raise RuntimeError(f"frozen header mismatch: {name}")
        cls.library = base.Library(cls.path)
        cls.api = jobs.OwnedJobs(cls.library)
        print(f"PYTHON_JOB_LIBRARY sha256={LIBRARY_SHA} path={cls.path}", flush=True)
        print(f"PYTHON_JOB_HEADER sha256={HEADER_SHA}", flush=True)


if __name__ == "__main__":
    unittest.main()
