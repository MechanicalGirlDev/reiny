"""Owned-job handles and retained observations."""

import ctypes as C
import json

from ._abi import ReinyError, Status, _U32, _U64, _bytes, _uint
from ._handles import Buffer, Handle
from ._job_models import (
    ApplicationFailure, CancelStatus, Phase, ResultKind, ShutdownReport, SnapshotInfo,
    _SnapshotInfo,
)


class _JobHandle(Handle):
    @property
    def _jobs(self):
        return self._lib._reiny_owned_jobs


class Work(_JobHandle):
    """An independent single-use work claim; retained handles retain data."""

    def __init__(self, library, handle, owned=True, finalizer_errors=None):
        super().__init__(library, handle, owned=owned)
        self.finalizer_errors = [] if finalizer_errors is None else finalizer_errors

    def clone(self):
        clone = super().clone()
        clone.finalizer_errors = self.finalizer_errors
        return clone


class Owner(_JobHandle):
    def start(self, work):
        return self._jobs._new(Job, "start", self._id(), self._lib._id(work))

    def close_admission(self):
        """Close admission and request cancellation; do not release or join."""
        self._jobs._call("owner_close", self._id())

    def shutdown(self, grace_ns):
        document = json.loads(self._jobs._bytes_result(
            "owner_shutdown", self._id(), _uint(grace_ns)))
        return ShutdownReport(document["version"], tuple(document["unresponsive"]),
                              tuple(document["join_failures"]))


class Job(_JobHandle):
    def cancel(self):
        status = _U32()
        self._jobs._call("cancel", self._id(), C.byref(status))
        return CancelStatus(status.value)

    def wait(self, timeout_ns):
        return self._jobs._new(Result, "wait", self._id(), _uint(timeout_ns, positive=True))

    def wait_joined(self, timeout_ns):
        self._jobs._call("wait_joined", self._id(), _uint(timeout_ns, positive=True))

    def snapshot(self):
        return self._jobs._new(Snapshot, "snapshot", self._id())

    def subscribe(self):
        return self._jobs._new(Observer, "subscribe", self._id())


class Observer(_JobHandle):
    def changed(self, timeout_ns):
        return self._jobs._new(Snapshot, "changed", self._id(), _uint(timeout_ns, positive=True))


class Snapshot(_JobHandle):
    @property
    def info(self):
        info = _SnapshotInfo()
        self._jobs._call("snapshot_read", self._id(), C.byref(info))
        return SnapshotInfo(info.id, info.revision, Phase(info.phase),
                            bool(info.cancellation_requested), bool(info.has_progress))

    @property
    def progress(self):
        return self._jobs._bytes_result("snapshot_progress", self._id())

    def result(self):
        return self._jobs._new(Result, "snapshot_result", self._id())


class Result(_JobHandle):
    @property
    def kind(self):
        kind = _U32()
        self._jobs._call("result_kind", self._id(), C.byref(kind))
        return ResultKind(kind.value)

    @property
    def success(self):
        return self._jobs._bytes_result("result_success", self._id())

    @property
    def error(self):
        code, message = C.c_int32(), _U64()
        self._jobs._call("result_error", self._id(), C.byref(code), C.byref(message))
        with Buffer(self._lib, message.value) as buffer:
            return ApplicationFailure(code.value, buffer.bytes.decode("utf-8"))


class Context(_JobHandle):
    def report(self, progress):
        accepted = _U32()
        self._jobs._call("report", self._id(), _bytes(progress), C.byref(accepted))
        return bool(accepted.value)

    @property
    def is_cancelled(self):
        cancelled = _U32()
        self._jobs._call("is_cancelled", self._id(), C.byref(cancelled))
        return bool(cancelled.value)

    def wait_cancelled(self, timeout_ns):
        """Wait for cancellation OR completion; inspect is_cancelled afterward."""
        self._jobs._call("wait_cancelled", self._id(), _uint(timeout_ns, positive=True))


class Success(_JobHandle):
    def clone(self):
        raise ReinyError(Status.INVALID_STATE, "job success sink cannot be cloned")

    def set(self, value):
        """Copy bytes, not completion; return from run and native join commit."""
        self._jobs._call("succeed", self._id(), _bytes(value))
