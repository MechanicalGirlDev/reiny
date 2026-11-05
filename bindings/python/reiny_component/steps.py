"""Opt-in Local managed steps over the additive reiny_component_step.h ABI.

Importing this module does not bind native step symbols. Construct LocalSteps
with an existing service Library to opt in. Jobs and process/distributed steps
are unsupported. Factory teardown uses native tokens, never a Python finalizer.
"""

import ctypes as C
from dataclasses import dataclass
from enum import IntEnum
import json
import threading
from collections.abc import Mapping

from . import (
    Buffer, Component, Handle, Host, Library, ReinyError, Status, _Bytes, _Create,
    _Destroy, _FactoryReleaseToken, _Hook, _OUT, _PTR, _Registration, _Stop, _U32,
    _U64, _bytes, _registration_lock, _registrations, _text, _tokens, _uint,
)


from ._step_models import (
    StepStatus, StepError, NotRunningError, BusyError, InvalidRequestError,
    InvalidTimeoutError, IdExhaustedError, FaultedError, StoppedError,
    SupervisorGoneError, StepsUnavailableError, _ADMISSION_ERRORS, ClockPhase,
    CancelStatus, ResultKind, FailureKind, _Duration, _Tick, _Clock, Duration, Tick,
    Clock, RunId, Failure, _StepCallback, _StepFactory,
)
from ._step_handles import (
    _StepHandle, StepSchema, StepGraph, Request, Value, Inputs, Outputs, Scheduler,
    Run, Result,
)
from ._step_callbacks import (
    StepComponent, _StepRegistration,
)


class LocalSteps:
    """Bind only on explicit opt-in, using the same Library and handle store."""

    def __init__(self, library):
        if not isinstance(library, Library):
            raise TypeError("LocalSteps requires an existing reiny_component.Library")
        self.library = library
        signatures = {
            "register_notified": [_U64, _Bytes, _Bytes, _U64, C.POINTER(_StepFactory), _U64],
            "schema_import": [_Bytes, _OUT],
            "graph_import": [_Bytes, _OUT],
            "request_import": [_Bytes, _OUT],
            "value_new": [_U64, _Bytes, _OUT],
            "value_copy": [_U64, _U64, _PTR, C.c_size_t, C.POINTER(C.c_size_t)],
            "input_get": [_U64, _Bytes, _OUT],
            "output_set": [_U64, _Bytes, _U64],
            "host_launch_local": [_U64, _U64, _U64, _Duration, _OUT, _OUT],
            "clock_read": [_U64, C.POINTER(_Clock)],
            "try": [_U64, _U64, _U64, _OUT],
            "run_id": [_U64, _OUT, _OUT, _OUT],
            "cancel": [_U64, C.POINTER(_U32)],
            "wait": [_U64, _U64, _OUT],
            "result_kind": [_U64, C.POINTER(_U32)],
            "result_tick": [_U64, C.POINTER(_Tick)],
            "result_output": [_U64, _Bytes, _Bytes, _OUT],
            "result_info": [_U64, _OUT],
        }
        try:
            functions = [(getattr(library._dll, "reiny_step_" + name), args)
                         for name, args in signatures.items()]
        except AttributeError as error:
            raise StepsUnavailableError(
                Status.UNSUPPORTED, "native library lacks the notified Local-step ABI") from error
        for function, args in functions:
            function.restype, function.argtypes = C.c_int32, args
        library._reiny_local_steps = self

    def _check(self, status):
        error_type = _ADMISSION_ERRORS.get(status)
        if error_type is not None:
            diagnostic = self.library._copy("last_error_copy").rstrip(b"\0").decode("utf-8")
            raise error_type(status, diagnostic)
        self.library._check(status)

    def _call(self, name, *args):
        self._check(getattr(self.library._dll, "reiny_step_" + name)(*args))

    def _new(self, cls, name, *args):
        output = _U64()
        self._call(name, *args, C.byref(output))
        return cls(self.library, output.value)

    def _import(self, cls, name, document):
        payload = document if isinstance(document, bytes) else json.dumps(
            document, default=self.library._id, allow_nan=False,
            separators=(",", ":")).encode("utf-8")
        return self._new(cls, name, _bytes(payload))

    def import_schema(self, document):
        return self._import(StepSchema, "schema_import", document)

    def import_graph(self, document):
        return self._import(StepGraph, "graph_import", document)

    def import_request(self, document):
        return self._import(Request, "request_import", document)

    def schema(self, inputs, outputs):
        """Ports accept a schema handle or {"schema": handle, "semantics": map}."""
        return self.import_schema({"version": 1, "inputs": inputs, "outputs": outputs})

    def graph(self, nodes, routes):
        return self.import_graph({"version": 1, "nodes": nodes, "routes": routes})

    def request(self, dt, inputs=()):
        if not isinstance(dt, Duration):
            raise TypeError("dt must be a Duration")
        return self.import_request({
            "version": 1, "dt": {"secs": dt.secs, "nanos": dt.nanos},
            "inputs": [{"instance": instance, "port": port, "value": value}
                       for instance, port, value in inputs],
        })

    def value(self, schema, payload, *, semantics: Mapping[str, str] | None = None):
        """Snapshot bytes with optional explicit application-owned assertions."""
        if semantics is not None:
            function = self.library._dll.reiny_step_value_new_with_semantics
            function.restype = C.c_int32
            function.argtypes = [_U64, _Bytes, _Bytes, _OUT]
            assertions = json.dumps(dict(semantics), allow_nan=False).encode("utf-8")
            return self._new(Value, "value_new_with_semantics",
                             self.library._id(schema), _bytes(assertions), _bytes(payload))
        return self._new(Value, "value_new", self.library._id(schema), _bytes(payload))

    def register(self, registry, factory_id, revision, schema, factory):
        if not callable(factory):
            raise TypeError("factory must be callable")
        registration = _StepRegistration(self.library, factory)
        with _registration_lock:
            _registrations[registration.token] = registration
        try:
            self._call("register_notified", self.library._id(registry),
                       _text(factory_id), _text(revision), self.library._id(schema),
                       C.byref(registration.table), registration.release_token._id())
        except BaseException:
            with _registration_lock:
                _registrations.pop(registration.token, None)
            registration.release_token.close()
            raise
        return registration.finalizer_errors

    def launch_local(self, registry, service_graph, step_graph, initial_time=Duration()):
        if not isinstance(initial_time, Duration):
            raise TypeError("initial_time must be a Duration")
        host, scheduler = _U64(), _U64()
        self._call("host_launch_local", self.library._id(registry),
                   self.library._id(service_graph), self.library._id(step_graph),
                   initial_time._native(), C.byref(host), C.byref(scheduler))
        return Host(self.library, host.value), Scheduler(self.library, scheduler.value)

# Keep the published module identity for pickles and class introspection.
for _export in (
    StepStatus, StepError, NotRunningError, BusyError, InvalidRequestError,
    InvalidTimeoutError, IdExhaustedError, FaultedError, StoppedError,
    SupervisorGoneError, StepsUnavailableError, ClockPhase, CancelStatus, ResultKind,
    FailureKind, _Duration, _Tick, _Clock, Duration, Tick, Clock, RunId, Failure,
    _StepFactory, StepComponent, _StepRegistration, _StepHandle, StepSchema, StepGraph,
    Request, Value, Inputs, Outputs, Scheduler, Run, Result,
):
    _export.__module__ = __name__
del _export
