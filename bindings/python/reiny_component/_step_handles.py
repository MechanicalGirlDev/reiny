"""Local-step values, scheduler runs and retained results."""

import ctypes as C
import json
from collections.abc import Mapping

from ._abi import ReinyError, Status, _OUT, _U32, _U64, _text, _uint
from ._handles import Buffer, Handle
from ._step_models import (
    Clock, ClockPhase, Duration, Tick, RunId, CancelStatus, ResultKind, Failure,
    FailureKind, _Clock, _Tick,
)


class _StepHandle(Handle):
    @property
    def _steps(self):
        return self._lib._reiny_local_steps


class StepSchema(_StepHandle):
    pass


class StepGraph(_StepHandle):
    pass


class Request(_StepHandle):
    pass


class Value(_StepHandle):
    @property
    def semantics(self) -> Mapping[str, str]:
        """Return a copy of the native snapshot's application assertions."""
        function = self._lib._dll.reiny_step_value_semantics
        function.restype = C.c_int32
        function.argtypes = [_U64, _OUT]
        with self._steps._new(Buffer, "value_semantics", self._id()) as buffer:
            return json.loads(buffer.bytes)

    def encoded(self, schema):
        required = C.c_size_t()
        schema_id = self._lib._id(schema)
        self._steps._call("value_copy", self._id(), schema_id, None, 0, C.byref(required))
        storage = (C.c_uint8 * required.value)()
        self._steps._call("value_copy", self._id(), schema_id, storage,
                          len(storage), C.byref(required))
        return bytes(storage[:required.value])


class Inputs(_StepHandle):
    def get(self, port):
        return self._steps._new(Value, "input_get", self._id(), _text(port))


class Outputs(_StepHandle):
    def clone(self):
        raise ReinyError(Status.INVALID_STATE, "step output sink cannot be cloned")

    def set(self, port, value):
        self._steps._call("output_set", self._id(), _text(port), self._lib._id(value))


class Scheduler(_StepHandle):
    @property
    def clock(self):
        value = _Clock()
        self._steps._call("clock_read", self._id(), C.byref(value))
        return Clock(value.epoch, value.next_index, Duration._copy(value.time), ClockPhase(value.phase))

    def try_step(self, request, timeout_ns):
        # Zero is a native INVALID_TIMEOUT admission error, not a wait timeout.
        return self._steps._new(Run, "try", self._id(), self._lib._id(request), _uint(timeout_ns))


class Run(_StepHandle):
    @property
    def id(self):
        epoch, sequence, index = _U64(), _U64(), _U64()
        self._steps._call("run_id", self._id(), C.byref(epoch), C.byref(sequence), C.byref(index))
        return RunId(epoch.value, sequence.value, index.value)

    def cancel(self):
        status = _U32()
        self._steps._call("cancel", self._id(), C.byref(status))
        return CancelStatus(status.value)

    def wait(self, timeout_ns):
        return self._steps._new(Result, "wait", self._id(), _uint(timeout_ns, positive=True))


class Result(_StepHandle):
    @property
    def kind(self):
        result = _U32()
        self._steps._call("result_kind", self._id(), C.byref(result))
        return ResultKind(result.value)

    @property
    def tick(self):
        tick = _Tick()
        self._steps._call("result_tick", self._id(), C.byref(tick))
        return Tick._copy(tick)

    @property
    def info(self):
        with self._steps._new(Buffer, "result_info", self._id()) as buffer:
            return json.loads(buffer.bytes)

    @property
    def failure(self):
        failure = self.info["failure"]
        if failure is None:
            return None
        return Failure(FailureKind(failure["kind"]), failure["instance"], failure["port"],
                       tuple(failure["attempted"]), tuple(failure["completed"]), failure["detail"])

    def output(self, instance, port):
        return self._steps._new(Value, "result_output", self._id(), _text(instance), _text(port))
