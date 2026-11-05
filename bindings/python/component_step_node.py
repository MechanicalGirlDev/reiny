"""Gated component nodes for Local-step acceptance."""

import ctypes as C
import queue
import threading

import reiny_component as base
from reiny_component import steps

NS = 5_000_000_000
INITIAL = steps.Duration(20_000_000_000)
DT = steps.Duration(nanos=7)
LIBRARY_SHA = "5ca3df75cff391f44a01138589c35d364368ebc8a32a44f60765adcc49a416d4"
HEADER_SHA = "58c973c95ad53a08d9de21a6ceaae4bc05c22c60a6fb9ad6e0d0c5a4431f0d1b"
SERVICE_SHA = "8e8341ada1f5b8399d5413ba42074ae48daffc24ff6bf028d2fcdce157816b6c"
SERVICE_HEADER_SHA = "978db314fea91fe62800565890d19bad315a68e3440c4d08c0ce9522752a75ed"




def encoded(number):
    # Fixture Number { uint32 value = 1; }, restricted to one-byte varints.
    assert 0 <= number < 128
    return bytes([8, number])


def number(value, schema):
    payload = value.encoded(schema)
    assert len(payload) == 2 and payload[0] == 8
    return payload[1]


class NodeState:
    def __init__(self, role, mode="normal", destroy_blocked=False):
        self.role, self.mode = role, mode
        self.destroy_blocked = destroy_blocked
        self.created = threading.Event()
        self.entered = threading.Event()
        self.gate = threading.Event()
        self.destroy_entered = threading.Event()
        self.destroy_gate = threading.Event()
        self.stopped = threading.Event()
        self.destroyed = threading.Event()
        self.stop_count = 0
        self.destroy_count = 0
        self.lane = None
        self.ticks = []
        self.retained_inputs = []
        self.borrowed_inputs = None
        self.borrowed_outputs = None
        self.borrowed_context = None
        self.native_sink = None


class Node(steps.StepComponent):
    def __init__(self, context, state, api, schema, wrong):
        self.state, self.api, self.schema, self.wrong = state, api, schema, wrong
        state.lane = threading.get_ident()
        assert context.info["id"] == state.role
        state.created.set()

    def lane(self):
        assert threading.get_ident() == self.state.lane

    def prepare(self, context):
        self.lane()

    def step(self, context, tick, inputs, outputs):
        self.lane()
        state = self.state
        state.borrowed_inputs, state.borrowed_outputs = inputs, outputs
        state.borrowed_context, state.native_sink = context, outputs._id()
        state.ticks.append(tick)
        assert tick.start.secs == INITIAL.secs
        assert tick.dt == DT
        assert tick.end == steps.Duration(INITIAL.secs, tick.start.nanos + DT.nanos)
        with self.api.value(self.schema, encoded(1)) as probe:
            with self.api.library._new(base.Handle, "clone", inputs._id()):
                pass
            try:
                outputs.clone()
            except base.ReinyError as error:
                assert error.status == base.Status.INVALID_STATE
            else:
                raise AssertionError("borrowed output sink cloned")
            unchanged = base._U64(123)
            try:
                self.api.library._call("clone", outputs._id(), C.byref(unchanged))
            except base.ReinyError as error:
                assert error.status == base.Status.INVALID_STATE
                assert unchanged.value == 123
            else:
                raise AssertionError("native output sink cloned")
            if state.mode == "thread_probe":
                completion = queue.Queue()

                def wrong_thread():
                    try:
                        outputs.set("other", probe)
                    except base.ReinyError as error:
                        completion.put(error.status)
                    except BaseException as error:
                        completion.put(error)
                    else:
                        completion.put(None)

                worker = threading.Thread(target=wrong_thread)
                worker.start()
                assert completion.get(timeout=10) == base.Status.INVALID_STATE
                worker.join(timeout=10)
                assert not worker.is_alive()
        state.retained_inputs.append(inputs.clone())
        if state.mode == "blocked":
            state.entered.set()
            assert state.gate.wait(10), "test never released step gate"
        if state.mode == "exception":
            raise KeyboardInterrupt("intentional Python step exception")
        with inputs.get("x") as incoming:
            result = number(incoming, self.schema)
        if state.role == "a":
            with inputs.get("prev") as previous:
                result += number(previous, self.schema)
        else:
            result += 1
        schema = self.wrong if state.mode == "invalid_output" else self.schema
        with self.api.value(schema, encoded(result)) as value:
            outputs.set("out", value)
            try:
                outputs.set("out", value)
            except base.ReinyError as error:
                assert error.status == base.Status.INVALID_STATE
            else:
                raise AssertionError("duplicate output accepted")

    def stop(self, context, reason):
        self.lane()
        assert context.is_stopping
        self.state.stop_count += 1
        assert self.state.stop_count == 1
        self.state.stopped.set()

    def destroy(self):
        self.lane()
        state = self.state
        state.destroy_entered.set()
        if state.destroy_blocked:
            assert state.destroy_gate.wait(10), "test never released destroy gate"
        state.destroy_count += 1
        assert state.destroy_count == 1
        state.destroyed.set()
