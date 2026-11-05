"""Owned native handles and copied service buffers."""

import ctypes as C
import threading

from ._abi import ReinyError, Status, _U32


class Handle:
    def __init__(self, library, handle, owned=True):
        self._lib = library
        self._handle = handle
        self._owned = owned
        self._lock = threading.RLock()

    def _id(self):
        with self._lock:
            if not self._handle:
                raise ReinyError(Status.INVALID_HANDLE, "handle is closed or callback expired")
            return self._handle

    def clone(self):
        return self._lib._new(type(self), "clone", self._id())

    def close(self):
        with self._lock:
            if not self._handle:
                return
            if not self._owned:
                raise ReinyError(Status.INVALID_STATE, "borrowed callback context cannot be closed")
            handle, self._handle = self._handle, 0
        self._lib._call("release", handle)

    def __enter__(self):
        self._id()
        return self

    def __exit__(self, *_):
        self.close()

    def __del__(self):
        try:
            if self._owned:
                self.close()
        except BaseException:
            # Finalization is only a fallback release, never a join certificate.
            pass

    def __copy__(self):
        raise TypeError("use clone() for another owned native handle")

    def __deepcopy__(self, memo):
        raise TypeError("use clone() for another owned native handle")


class Buffer(Handle):
    @property
    def bytes(self):
        return self._lib._copy("buffer_copy", self._id())


class Schema(Handle):
    pass


class _FactoryReleaseToken(Handle):
    @property
    def released(self):
        result = _U32()
        self._lib._call("factory_released", self._id(), C.byref(result))
        return bool(result.value)


class Graph(Handle):
    pass
