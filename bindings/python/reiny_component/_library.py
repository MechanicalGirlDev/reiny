"""Explicit service library loading, ABI calls and callback collection."""

import ctypes as C
import json
import threading

from ._abi import (
    ReinyError, _Bytes, _ERRORS, _Factory, _OUT, _PTR, _U32, _U64, _bytes, _text,
    _uint,
)
from ._handles import Buffer, Graph, Handle, Schema
from ._lifecycle import Registry
from ._ownership import _libraries, _registration_lock, _registrations


class Library:
    """A specific native ABI library; never searches for a stale build."""

    def __init__(self, path):
        # CDLL releases the GIL during native calls. CFUNCTYPE reacquires it on
        # foreign instance lanes. threading.local state must not span callbacks.
        self._dll = C.CDLL(str(path))
        self._collection_lock = threading.Lock()
        self._bind()
        if self._dll.reiny_exec_abi_version() != 1:
            raise RuntimeError("unsupported Reiny component ABI major")
        _libraries.append(self._dll)

    def _bind(self):
        signatures = {
            "abi_version": (C.c_uint32, []),
            "last_error_copy": (C.c_int32, [_PTR, C.c_size_t, C.POINTER(C.c_size_t)]),
            "release": (C.c_int32, [_U64]),
            "clone": (C.c_int32, [_U64, _OUT]),
            "registry_new": (C.c_int32, [_OUT]),
            "register": (C.c_int32, [_U64, _Bytes, _Bytes, C.POINTER(_Factory)]),
            "factory_release_token_new": (C.c_int32, [_OUT]),
            "register_notified": (C.c_int32, [_U64, _Bytes, _Bytes, C.POINTER(_Factory), _U64]),
            "factory_released": (C.c_int32, [_U64, C.POINTER(_U32)]),
            "registry_seal": (C.c_int32, [_U64]),
            "schema_new": (C.c_int32, [_Bytes, C.c_uint16, _U64, _OUT]),
            "error_set": (C.c_int32, [_U64, C.c_int32, _Bytes]),
            "buffer_copy": (C.c_int32, [_U64, _PTR, C.c_size_t, C.POINTER(C.c_size_t)]),
            "graph_import": (C.c_int32, [_Bytes, _OUT]),
            "host_launch_local": (C.c_int32, [_U64, _U64, _OUT]),
            "host_request_stop": (C.c_int32, [_U64]),
            "host_wait_running": (C.c_int32, [_U64, _U64]),
            "host_wait_joined": (C.c_int32, [_U64, _U64]),
            "host_phase": (C.c_int32, [_U64, C.POINTER(_U32)]),
            "host_snapshot": (C.c_int32, [_U64, _OUT]),
            "host_subscribe": (C.c_int32, [_U64, _OUT]),
            "host_wait_terminal": (C.c_int32, [_U64, _U64, _OUT]),
            "observer_next": (C.c_int32, [_U64, _U64, _OUT]),
            "context_info": (C.c_int32, [_U64, _OUT]),
            "context_is_stopping": (C.c_int32, [_U64, C.POINTER(_U32)]),
            "context_wait_stop": (C.c_int32, [_U64, _U64]),
            "context_open_stream": (C.c_int32, [_U64, _Bytes, _U64, _U32, _OUT]),
            "context_open_rpc": (C.c_int32, [_U64, _Bytes, _U64, _U64, _U32, _OUT]),
            "context_watch_input_ready": (C.c_int32, [_U64, _Bytes, _OUT]),
            "presence_next": (C.c_int32, [_U64, _U64, _OUT]),
            "publisher_send": (C.c_int32, [_U64, _Bytes]),
            "subscription_receive": (C.c_int32, [_U64, _U64, _OUT]),
            "subscription_stats": (C.c_int32, [_U64, _U64, _OUT]),
            "message_copy": (C.c_int32, [_U64, _U32, _PTR, C.c_size_t, C.POINTER(C.c_size_t)]),
            "rpc_call_start": (C.c_int32, [_U64, _Bytes, _U64, _OUT]),
            "operation_wait": (C.c_int32, [_U64, _U64, _OUT]),
            "rpc_server_receive": (C.c_int32, [_U64, _U64, _OUT]),
            "request_copy": (C.c_int32, [_U64, _PTR, C.c_size_t, C.POINTER(C.c_size_t)]),
            "request_reply": (C.c_int32, [_U64, _Bytes, _U32]),
            "process_host_run": (C.c_int32, [_U64, _U64, _U64, _U64, C.c_size_t, _OUT]),
        }
        for name, (result, args) in signatures.items():
            fn = getattr(self._dll, "reiny_exec_" + name)
            fn.restype, fn.argtypes = result, args

    def _check(self, status):
        if status:
            # Copy thread-local diagnostics immediately, before any other ABI
            # call can overwrite them on this thread.
            message = self._copy("last_error_copy").rstrip(b"\0").decode("utf-8")
            raise _ERRORS.get(status, ReinyError)(status, message)

    def _call(self, name, *args):
        self._check(getattr(self._dll, "reiny_exec_" + name)(*args))

    def _copy(self, name, *args):
        fn = getattr(self._dll, "reiny_exec_" + name)
        size = C.c_size_t()
        status = fn(*args, None, 0, C.byref(size))
        if name == "last_error_copy":
            if status:
                raise RuntimeError("cannot read native error")
        else:
            self._check(status)
        storage = (C.c_uint8 * size.value)()
        status = fn(*args, storage, len(storage), C.byref(size))
        if name == "last_error_copy":
            if status:
                raise RuntimeError("cannot copy native error")
        else:
            self._check(status)
        return bytes(storage[:size.value])

    def _new(self, cls, name, *args):
        handle = _U64()
        self._call(name, *args, C.byref(handle))
        return cls(self, handle.value)

    def _document(self, name, *args):
        with self._new(Buffer, name, *args) as buffer:
            return json.loads(buffer.bytes)

    def registry(self):
        return self._new(Registry, "registry_new")

    def collect_factories(self):
        """Reclaim released callback roots on the caller's thread.

        Call after joining hosts and closing registries. Registrations still
        owned by native work remain pinned, including unresponsive instances.
        """
        released = []
        with self._collection_lock:
            with _registration_lock:
                candidates = [registration for registration in _registrations.values()
                              if registration.library is self]
            for registration in candidates:
                if registration.released:
                    with _registration_lock:
                        released.append(_registrations.pop(registration.token))
                    registration.release_token.close()
        count = len(released)
        # Drop application factory captures outside the lock on this thread.
        released.clear()
        return count

    def schema(self, protobuf_name, fingerprint, fingerprint_version=2):
        return self._new(Schema, "schema_new", _text(protobuf_name),
                         _uint(fingerprint_version, 16), _uint(fingerprint))

    def graph(self, document):
        """Import a mapping or strict JSON bytes (duplicates reach native validation)."""
        payload = document if isinstance(document, bytes) else json.dumps(
            document, allow_nan=False, separators=(",", ":")).encode("utf-8")
        return self._new(Graph, "graph_import", _bytes(payload))

    def _id(self, handle):
        if not isinstance(handle, Handle) or handle._lib is not self:
            raise ValueError("handle belongs to a different native library")
        return handle._id()
