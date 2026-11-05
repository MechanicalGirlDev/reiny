"""Gated native worker fixtures for owned-job acceptance."""

import ctypes as C
import threading

import reiny_component as base
from reiny_component import jobs

NS = 5_000_000_000
LIBRARY_SHA = "8d11f369520261cb8fc4409c7e4c61b52337294f9fc71fcee7e324b80c78dcc6"
HEADER_SHA = "0e19eae5843cb7b4440656fa90aed09704963a4f5db1ceec7e324f14b9db4400"
SERVICE_SHA = "8e8341ada1f5b8399d5413ba42074ae48daffc24ff6bf028d2fcdce157816b6c"
STEP_SHA = "5ca3df75cff391f44a01138589c35d364368ebc8a32a44f60765adcc49a416d4"
PROGRESS = b"\x01\x00\xff"
SUCCESS = b"\x07\x00\xff"




class State:
    def __init__(self, mode, destroy_blocked=False, raw_copies=False):
        self.mode, self.destroy_blocked, self.raw_copies = mode, destroy_blocked, raw_copies
        self.create_called = threading.Event()
        self.entered = threading.Event()
        self.progress_gate = threading.Event()
        self.progressed = threading.Event()
        self.finish = threading.Event()
        self.destroy_entered = threading.Event()
        self.destroy_gate = threading.Event()
        self.destroyed = threading.Event()
        self.created_count = self.destroyed_count = 0
        self.lane = None
        self.context = None
        self.borrowed_create = self.borrowed_run = self.borrowed_success = None
        self.native_sink = None


class GatedWorker(jobs.Worker):
    def __init__(self, context, state, api):
        self.state, self.api = state, api
        state.lane = threading.get_ident()
        state.borrowed_create = context
        state.create_called.set()
        if state.mode == "create_error":
            raise jobs.ApplicationError(-77, "create-error")
        state.created_count += 1
        state.context = context.clone()

    def lane(self):
        assert threading.get_ident() == self.state.lane

    def run(self, context, success):
        self.lane()
        state = self.state
        state.borrowed_run, state.borrowed_success = context, success
        state.native_sink = success._id()
        try:
            success.clone()
        except base.ReinyError as error:
            assert error.status == base.Status.INVALID_STATE
        else:
            raise AssertionError("borrowed success sink cloned")
        untouched = base._U64(123)
        try:
            self.api.library._call("clone", success._id(), C.byref(untouched))
        except base.ReinyError as error:
            assert error.status == base.Status.INVALID_STATE and untouched.value == 123
        else:
            raise AssertionError("native success sink cloned")
        try:
            self.api.library._call("release", success._id())
        except base.ReinyError as error:
            assert error.status == base.Status.INVALID_HANDLE
        else:
            raise AssertionError("borrowed success sink released")
        state.entered.set()
        assert state.progress_gate.wait(10), "progress gate was not released"
        assert context.report(PROGRESS)
        if state.raw_copies:
            # Exercise the real native copied-buffer boundary with mutable C
            # storage, in addition to the ordinary public bytes API.
            progress = (C.c_uint8 * 3)(*PROGRESS)
            accepted = base._U32()
            self.api._call("report", context._id(), base._Bytes(progress, 3), C.byref(accepted))
            assert accepted.value == 1
            progress[:] = [9, 9, 9]
        if state.mode != "missing_success":
            if state.raw_copies:
                result = (C.c_uint8 * 3)(*SUCCESS)
                self.api._call("succeed", success._id(), base._Bytes(result, 3))
                result[:] = [9, 9, 9]
            else:
                success.set(SUCCESS)
            try:
                success.set(b"duplicate")
            except base.ReinyError as error:
                assert error.status == base.Status.INVALID_STATE
            else:
                raise AssertionError("duplicate success write accepted")
        state.progressed.set()
        assert state.finish.wait(10), "run gate was not released"
        if state.mode in ("cancel_success", "application_error", "owner_drop"):
            assert context.is_cancelled
            context.wait_cancelled(NS)
            assert not context.report(b"too late")
        if state.mode == "application_error":
            raise jobs.ApplicationError(-88, "job-error")
        if state.mode == "exception":
            raise KeyboardInterrupt("intentional job callback exception")

    def destroy(self):
        self.lane()
        state = self.state
        state.destroy_entered.set()
        if state.destroy_blocked:
            assert state.destroy_gate.wait(10), "destroy gate was not released"
        state.destroyed_count += 1
        assert state.destroyed_count == 1
        state.destroyed.set()
