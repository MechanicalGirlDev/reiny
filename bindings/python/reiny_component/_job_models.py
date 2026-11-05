"""Owned-job status, immutable observations and ABI layouts."""

import ctypes as C
from dataclasses import dataclass
from enum import IntEnum

from ._abi import ReinyError, _Create, _Destroy, _U32, _U64, _text


class StartStatus(IntEnum):
    OWNER_CLOSED = -201
    AT_CAPACITY = -202
    ID_EXHAUSTED = -203
    RUNTIME_UNAVAILABLE = -204


class StartError(ReinyError):
    def __init__(self, status, message):
        self.status = StartStatus(status)
        Exception.__init__(self, message)


class OwnerClosedError(StartError):
    pass


class CapacityError(StartError):
    pass


class IdExhaustedError(StartError):
    pass


class RuntimeUnavailableError(StartError):
    pass


class JobsUnavailableError(ReinyError):
    pass


_START_ERRORS = {
    StartStatus.OWNER_CLOSED: OwnerClosedError,
    StartStatus.AT_CAPACITY: CapacityError,
    StartStatus.ID_EXHAUSTED: IdExhaustedError,
    StartStatus.RUNTIME_UNAVAILABLE: RuntimeUnavailableError,
}


class Phase(IntEnum):
    RUNNING = 1
    CANCELLING = 2
    COMPLETE = 3


class CancelStatus(IntEnum):
    REQUESTED = 1
    ALREADY_REQUESTED = 2
    FINISHED = 3


class ResultKind(IntEnum):
    SUCCEEDED = 1
    APPLICATION_ERROR = 2
    CANCELLED = 3
    PANICKED = 4
    INTERRUPTED = 5


class ApplicationError(Exception):
    """Raise from create/run to return a copied negative application error."""

    def __init__(self, code, message):
        if type(code) is not int or not -(1 << 31) <= code < 0:
            raise ValueError("application error code must be a negative int32")
        _text(message)
        self.code, self.message = code, message
        super().__init__(message)


@dataclass(frozen=True)
class ApplicationFailure:
    code: int
    message: str


@dataclass(frozen=True)
class ShutdownReport:
    version: int
    unresponsive: tuple
    join_failures: tuple


@dataclass(frozen=True)
class SnapshotInfo:
    id: int
    revision: int
    phase: Phase
    cancellation_requested: bool
    has_progress: bool


class _SnapshotInfo(C.Structure):
    _fields_ = [("id", _U64), ("revision", _U64), ("phase", _U32),
                ("cancellation_requested", _U32), ("has_progress", _U32)]


_RunCallback = C.CFUNCTYPE(C.c_int32, C.c_void_p, _U64, _U64, _U64)


class _WorkTable(C.Structure):
    _fields_ = [
        ("struct_size", _U32), ("abi_major", _U32), ("flags", _U32),
        ("reserved", _U32), ("factory_data", C.c_void_p),
        ("create", _Create), ("run", _RunCallback),
        ("destroy_instance", _Destroy), ("release_factory", _Destroy),
    ]
