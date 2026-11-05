"""Opt-in explicitly owned Local jobs over reiny_job.h.

Importing resolves no job symbols. No automatic component binding, distributed
execution, alternate scheduler or Python last-owner finalizer is provided.
"""

import ctypes as C
from dataclasses import dataclass
from enum import IntEnum
import json
import threading
import traceback

from . import (
    Buffer, Handle, Library, ReinyError, Status, _Bytes, _Create, _Destroy,
    _FactoryReleaseToken, _OUT, _Registration, _U32, _U64, _bytes, _registration_lock,
    _registrations, _text, _tokens, _uint,
)


from ._job_models import (
    StartStatus, StartError, OwnerClosedError, CapacityError, IdExhaustedError,
    RuntimeUnavailableError, JobsUnavailableError, _START_ERRORS, Phase, CancelStatus,
    ResultKind, ApplicationError, ApplicationFailure, ShutdownReport, SnapshotInfo,
    _SnapshotInfo, _RunCallback, _WorkTable,
)
from ._job_handles import (
    _JobHandle, Work, Owner, Job, Observer, Snapshot, Result, Context, Success,
)
from ._job_callbacks import (
    Worker, _WorkRegistration,
)


class OwnedJobs:
    """Explicit lazy opt-in, sharing the service Library and ownership roots."""

    def __init__(self, library):
        if not isinstance(library, Library):
            raise TypeError("OwnedJobs requires an existing reiny_component.Library")
        self.library = library
        signatures = {
            "work_new_notified": [C.POINTER(_WorkTable), _U64, _OUT],
            "owner_new": [C.c_size_t, _OUT],
            "start": [_U64, _U64, _OUT],
            "owner_close": [_U64],
            "owner_shutdown": [_U64, _U64, _OUT],
            "cancel": [_U64, C.POINTER(_U32)],
            "wait": [_U64, _U64, _OUT],
            "wait_joined": [_U64, _U64],
            "snapshot": [_U64, _OUT],
            "subscribe": [_U64, _OUT],
            "changed": [_U64, _U64, _OUT],
            "snapshot_read": [_U64, C.POINTER(_SnapshotInfo)],
            "snapshot_progress": [_U64, _OUT],
            "snapshot_result": [_U64, _OUT],
            "result_kind": [_U64, C.POINTER(_U32)],
            "result_success": [_U64, _OUT],
            "result_error": [_U64, C.POINTER(C.c_int32), _OUT],
            "report": [_U64, _Bytes, C.POINTER(_U32)],
            "is_cancelled": [_U64, C.POINTER(_U32)],
            "wait_cancelled": [_U64, _U64],
            "succeed": [_U64, _Bytes],
        }
        try:
            functions = [(getattr(library._dll, "reiny_job_" + name), args)
                         for name, args in signatures.items()]
        except AttributeError as error:
            raise JobsUnavailableError(
                Status.UNSUPPORTED, "native library lacks the notified owned-jobs ABI") from error
        for function, args in functions:
            function.restype, function.argtypes = C.c_int32, args
        library._reiny_owned_jobs = self

    def _check(self, status):
        error_type = _START_ERRORS.get(status)
        if error_type is not None:
            diagnostic = self.library._copy("last_error_copy").rstrip(b"\0").decode("utf-8")
            raise error_type(status, diagnostic)
        self.library._check(status)

    def _call(self, name, *args):
        self._check(getattr(self.library._dll, "reiny_job_" + name)(*args))

    def _new(self, cls, name, *args):
        output = _U64()
        self._call(name, *args, C.byref(output))
        return cls(self.library, output.value)

    def _bytes_result(self, name, *args):
        with self._new(Buffer, name, *args) as buffer:
            return buffer.bytes

    def owner(self, capacity):
        return self._new(Owner, "owner_new", _uint(capacity, C.sizeof(C.c_size_t) * 8))

    def work(self, factory):
        if not callable(factory):
            raise TypeError("factory must be callable")
        registration = _WorkRegistration(self.library, factory)
        with _registration_lock:
            _registrations[registration.token] = registration
        output = _U64()
        try:
            self._call("work_new_notified", C.byref(registration.table),
                       registration.release_token._id(), C.byref(output))
        except BaseException:
            with _registration_lock:
                _registrations.pop(registration.token, None)
            registration.release_token.close()
            raise
        return Work(self.library, output.value, finalizer_errors=registration.finalizer_errors)

# Keep the published module identity for pickles and class introspection.
for _export in (
    StartStatus, StartError, OwnerClosedError, CapacityError, IdExhaustedError,
    RuntimeUnavailableError, JobsUnavailableError, Phase, CancelStatus, ResultKind,
    ApplicationError, ApplicationFailure, ShutdownReport, SnapshotInfo, _SnapshotInfo,
    _WorkTable, Worker, _WorkRegistration, _JobHandle, Work, Owner, Job, Observer,
    Snapshot, Result, Context, Success,
):
    _export.__module__ = __name__
del _export
