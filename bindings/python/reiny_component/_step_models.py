"""Local-step statuses, logical time models and ABI layouts."""

import ctypes as C
from dataclasses import dataclass
from enum import IntEnum

from ._abi import ReinyError, _Create, _Destroy, _Hook, _Stop, _U32, _U64, _uint


class StepStatus(IntEnum):
    NOT_RUNNING = -101
    BUSY = -102
    INVALID_REQUEST = -103
    INVALID_TIMEOUT = -104
    ID_EXHAUSTED = -105
    FAULTED = -106
    STOPPED = -107
    SUPERVISOR_GONE = -108


class StepError(ReinyError):
    def __init__(self, status, message):
        self.status = StepStatus(status)
        Exception.__init__(self, message)


class NotRunningError(StepError):
    pass


class BusyError(StepError):
    pass


class InvalidRequestError(StepError):
    pass


class InvalidTimeoutError(StepError):
    pass


class IdExhaustedError(StepError):
    pass


class FaultedError(StepError):
    pass


class StoppedError(StepError):
    pass


class SupervisorGoneError(StepError):
    pass


class StepsUnavailableError(ReinyError):
    pass


_ADMISSION_ERRORS = {
    StepStatus.NOT_RUNNING: NotRunningError,
    StepStatus.BUSY: BusyError,
    StepStatus.INVALID_REQUEST: InvalidRequestError,
    StepStatus.INVALID_TIMEOUT: InvalidTimeoutError,
    StepStatus.ID_EXHAUSTED: IdExhaustedError,
    StepStatus.FAULTED: FaultedError,
    StepStatus.STOPPED: StoppedError,
    StepStatus.SUPERVISOR_GONE: SupervisorGoneError,
}


class ClockPhase(IntEnum):
    STARTING = 1
    READY = 2
    RUNNING = 3
    FAULTED = 4
    STOPPED = 5


class CancelStatus(IntEnum):
    REQUESTED = 1
    ALREADY_REQUESTED = 2
    FINISHED = 3
    SUPERVISOR_GONE = 4


class ResultKind(IntEnum):
    COMMITTED = 0
    CANCELLED_BEFORE_DISPATCH = 1
    TIMED_OUT_BEFORE_DISPATCH = 2
    FAILED_AFTER_DISPATCH = 3
    SUPERVISOR_GONE = 4


class FailureKind(IntEnum):
    NODE_ERROR = 1
    NODE_PANICKED = 2
    INVALID_OUTPUTS = 3
    CANCELLED = 4
    TIMED_OUT = 5
    HOST_STOPPED = 6
    WORKER_LOST = 7
    ADVANCE_DROPPED = 8


class _Duration(C.Structure):
    _fields_ = [("secs", _U64), ("nanos", _U32)]


class _Tick(C.Structure):
    _fields_ = [("epoch", _U64), ("index", _U64), ("start", _Duration),
                ("dt", _Duration), ("end", _Duration)]


class _Clock(C.Structure):
    _fields_ = [("epoch", _U64), ("next_index", _U64), ("time", _Duration),
                ("phase", _U32)]


@dataclass(frozen=True)
class Duration:
    secs: int = 0
    nanos: int = 0

    def __post_init__(self):
        _uint(self.secs)
        _uint(self.nanos, 32)
        if self.nanos >= 1_000_000_000:
            raise ValueError("nanoseconds must be normalized below one billion")

    def _native(self):
        return _Duration(self.secs, self.nanos)

    @classmethod
    def _copy(cls, value):
        return cls(value.secs, value.nanos)


@dataclass(frozen=True)
class Tick:
    epoch: int
    index: int
    start: Duration
    dt: Duration
    end: Duration

    @classmethod
    def _copy(cls, value):
        return cls(value.epoch, value.index, Duration._copy(value.start),
                   Duration._copy(value.dt), Duration._copy(value.end))


@dataclass(frozen=True)
class Clock:
    epoch: int
    next_index: int
    time: Duration
    phase: ClockPhase


@dataclass(frozen=True)
class RunId:
    epoch: int
    sequence: int
    tick_index: int


@dataclass(frozen=True)
class Failure:
    kind: FailureKind
    instance: str | None
    port: str | None
    attempted: tuple
    completed: tuple
    detail: str | None


_StepCallback = C.CFUNCTYPE(C.c_int32, C.c_void_p, _U64, C.POINTER(_Tick),
                           _U64, _U64, _U64)


class _StepFactory(C.Structure):
    _fields_ = [
        ("struct_size", _U32), ("abi_major", _U32), ("flags", _U32),
        ("reserved", _U32), ("factory_data", C.c_void_p),
        ("create", _Create), ("prepare", _Hook), ("step", _StepCallback),
        ("stop", _Stop), ("destroy_instance", _Destroy),
        ("release_factory", _Destroy),
    ]
