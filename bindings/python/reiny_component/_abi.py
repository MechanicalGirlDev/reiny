"""Service ABI declarations and boundary conversions."""

import ctypes as C
from enum import IntEnum


class Status(IntEnum):
    OK = 0
    INVALID_ARGUMENT = -1
    INVALID_HANDLE = -2
    WRONG_KIND = -3
    INVALID_STATE = -4
    TIMEOUT = -5
    CLOSED = -6
    REMOTE_ERROR = -7
    NO_REPLY = -8
    SCHEMA_ERROR = -9
    CALLBACK_ERROR = -10
    BUFFER_TOO_SMALL = -12
    PANIC = -13
    UNSUPPORTED = -14
    ALREADY_COMPLETED = -15
    FAILURE = -16


class Phase(IntEnum):
    STARTING = 1
    PREPARED = 2
    COMMITTED = 3
    RUNNING = 4
    STOPPING = 5
    STOPPED = 6
    FAILED = 7
    UNRESPONSIVE = 8


class StopReason(IntEnum):
    REQUESTED = 1
    STARTUP_FAILURE = 2
    COMPONENT_FAILURE = 3


class ReinyError(Exception):
    def __init__(self, status, message):
        self.status = Status(status)
        super().__init__(message)


class DeadlineError(ReinyError):
    pass


class ClosedError(ReinyError):
    pass


class RemoteError(ReinyError):
    pass


class NoReplyError(ReinyError):
    pass


class SchemaError(ReinyError):
    pass


class AlreadyCompletedError(ReinyError):
    pass


_ERRORS = {
    Status.TIMEOUT: DeadlineError,
    Status.CLOSED: ClosedError,
    Status.REMOTE_ERROR: RemoteError,
    Status.NO_REPLY: NoReplyError,
    Status.SCHEMA_ERROR: SchemaError,
    Status.ALREADY_COMPLETED: AlreadyCompletedError,
}
_U64 = C.c_uint64
_U32 = C.c_uint32
_PTR = C.POINTER(C.c_uint8)
_OUT = C.POINTER(_U64)


class _Bytes(C.Structure):
    _fields_ = [("data", _PTR), ("len", C.c_size_t)]


_Create = C.CFUNCTYPE(C.c_int32, C.c_void_p, _U64, C.POINTER(C.c_void_p), _U64)
_Hook = C.CFUNCTYPE(C.c_int32, C.c_void_p, _U64, _U64)
_Stop = C.CFUNCTYPE(C.c_int32, C.c_void_p, _U64, _U32, _U64)
_Destroy = C.CFUNCTYPE(None, C.c_void_p)


class _Factory(C.Structure):
    _fields_ = [
        ("struct_size", _U32), ("abi_major", _U32), ("flags", _U32),
        ("mode", _U32), ("factory_data", C.c_void_p),
        ("create", _Create), ("prepare", _Hook), ("run", _Hook),
        ("stop", _Stop), ("destroy_instance", _Destroy),
        ("release_factory", _Destroy),
    ]


def _bytes(value):
    if not isinstance(value, bytes):
        raise TypeError("payload must be bytes")
    storage = (C.c_uint8 * len(value)).from_buffer_copy(value)
    result = _Bytes(storage, len(value))
    result._storage = storage
    return result


def _text(value):
    if not isinstance(value, str):
        raise TypeError("text must be str")
    if "\0" in value:
        raise ValueError("text contains NUL")
    return _bytes(value.encode("utf-8"))


def _uint(value, bits=64, positive=False):
    if type(value) is not int or not (int(positive) <= value < 1 << bits):
        raise ValueError(f"expected {'positive ' if positive else ''}uint{bits}")
    return value
