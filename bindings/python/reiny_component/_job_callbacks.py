"""Owned-job worker callbacks and borrowed callback lifetime."""

import ctypes as C
import threading
import traceback

from ._abi import Status, _Create, _Destroy, _text
from ._handles import _FactoryReleaseToken
from ._lifecycle import _Registration
from ._ownership import _tokens
from ._job_models import ApplicationError, _RunCallback, _WorkTable
from ._job_handles import Context, Success


class Worker:
    """Implement synchronous run(context, success_sink), then destroy."""

    def run(self, context, success):
        raise NotImplementedError("run must set success or raise an application error")

    def destroy(self):
        pass


class _WorkRegistration(_Registration):
    def __init__(self, library, factory):
        self.library, self.factory = library, factory
        self.instances = {}
        self.lock = threading.RLock()
        self.finalizer_errors = []
        self.token = next(_tokens)
        self.callbacks = (_Create(self.create), _RunCallback(self.run_job), _Destroy(self.destroy))
        self.table = _WorkTable(C.sizeof(_WorkTable), 1, 0, 0, self.token,
                                *self.callbacks, _Destroy())
        self.release_token = library._new(_FactoryReleaseToken, "factory_release_token_new")

    def diagnostic(self, sink, code, text):
        try:
            self.library._call("error_set", sink, code, _text(text))
        except BaseException:
            with self.lock:
                self.finalizer_errors.append(traceback.format_exc())

    def invoke(self, context_id, sink, action):
        context = Context(self.library, context_id, owned=False)
        try:
            action(context)
            return 0
        except ApplicationError as error:
            self.diagnostic(sink, error.code, error.message)
            return error.code
        except BaseException:
            self.diagnostic(sink, Status.CALLBACK_ERROR, traceback.format_exc().replace("\0", "\\0"))
            return Status.CALLBACK_ERROR
        finally:
            context._handle = 0

    def run_job(self, instance, context, sink, errors):
        success = Success(self.library, sink, owned=False)
        try:
            return self.invoke(context, errors, lambda ctx: self.instance(instance).run(ctx, success))
        finally:
            success._handle = 0
