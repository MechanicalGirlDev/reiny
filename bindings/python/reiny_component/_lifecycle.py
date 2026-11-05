"""Component callback adaptation and host lifecycle."""

import ctypes as C
import threading
import traceback

from ._abi import (
    Phase, Status, StopReason, _Create, _Destroy, _Factory, _Hook, _Stop, _U32, _text,
    _uint,
)
from ._handles import Handle, _FactoryReleaseToken
from ._ownership import _registration_lock, _registrations, _tokens
from ._ports import Context


class Component:
    """Subclass prepare/run/stop/destroy; factory(context) returns an instance."""

    def prepare(self, context):
        pass

    def run(self, context):
        raise NotImplementedError("run must serve until shutdown")

    def stop(self, context, reason):
        pass

    def destroy(self):
        pass


class _Registration:
    def __init__(self, library, factory):
        self.library, self.factory = library, factory
        self.instances = {}
        self.lock = threading.RLock()
        # Void callbacks cannot report through a sink. Retain errors for explicit
        # inspection rather than allowing ctypes to print and ignore exceptions.
        self.finalizer_errors = []
        self.token = next(_tokens)
        self.callbacks = (
            _Create(self.create), _Hook(self.prepare), _Hook(self.run),
            _Stop(self.stop), _Destroy(self.destroy),
        )
        # NULL release_factory: native teardown NEVER enters the interpreter.
        self.table = _Factory(C.sizeof(_Factory), 1, 0, 1, self.token,
                              *self.callbacks, _Destroy())
        self.release_token = library._new(_FactoryReleaseToken, "factory_release_token_new")

    @property
    def released(self):
        return self.release_token.released

    def invoke(self, context_id, sink, action):
        context = Context(self.library, context_id, owned=False)
        try:
            action(context)
            return 0
        except BaseException:
            # Catch SystemExit/KeyboardInterrupt as well: none may unwind through C.
            diagnostic = traceback.format_exc().replace("\0", "\\0")
            try:
                self.library._call("error_set", sink, Status.CALLBACK_ERROR, _text(diagnostic))
            except BaseException:
                with self.lock:
                    self.finalizer_errors.append(traceback.format_exc())
            return Status.CALLBACK_ERROR
        finally:
            context._handle = 0

    def create(self, _, context, output, sink):
        def action(ctx):
            instance = self.factory(ctx)
            if instance is None:
                raise TypeError("factory must return a component instance")
            token = next(_tokens)
            with self.lock:
                self.instances[token] = instance
            output[0] = token
        return self.invoke(context, sink, action)

    def instance(self, token):
        with self.lock:
            return self.instances[token]

    def prepare(self, token, context, sink):
        return self.invoke(context, sink, lambda ctx: self.instance(token).prepare(ctx))

    def run(self, token, context, sink):
        return self.invoke(context, sink, lambda ctx: self.instance(token).run(ctx))

    def stop(self, token, context, reason, sink):
        return self.invoke(context, sink, lambda ctx: self.instance(token).stop(ctx, StopReason(reason)))

    def destroy(self, token):
        try:
            with self.lock:
                instance = self.instances.pop(token)
            instance.destroy()
        except BaseException:
            with self.lock:
                self.finalizer_errors.append(traceback.format_exc())

class Registry(Handle):
    def register(self, factory_id, revision, factory):
        """Register callable(context)->Component; returns finalizer diagnostics."""
        if not callable(factory):
            raise TypeError("factory must be callable")
        registration = _Registration(self._lib, factory)
        with _registration_lock:
            _registrations[registration.token] = registration
        try:
            self._lib._call("register_notified", self._id(), _text(factory_id), _text(revision),
                            C.byref(registration.table), registration.release_token._id())
        except BaseException:
            with _registration_lock:
                _registrations.pop(registration.token, None)
            registration.release_token.close()
            raise
        return registration.finalizer_errors

    def seal(self):
        self._lib._call("registry_seal", self._id())

    def launch_local(self, graph):
        return self._lib._new(Host, "host_launch_local", self._id(), self._lib._id(graph))

    def process_host_run(self, startup_ns, stop_ns, frame_ns, max_instances):
        """Executable entry only. Caller MUST exit the process after return."""
        return self._lib._document("process_host_run", self._id(),
                                   _uint(startup_ns, positive=True),
                                   _uint(stop_ns, positive=True),
                                   _uint(frame_ns, positive=True),
                                   _uint(max_instances, C.sizeof(C.c_size_t) * 8, True))


class Host(Handle):
    def request_stop(self):
        self._lib._call("host_request_stop", self._id())

    def wait_running(self, timeout_ns):
        self._lib._call("host_wait_running", self._id(), _uint(timeout_ns, positive=True))

    def wait_joined(self, timeout_ns):
        self._lib._call("host_wait_joined", self._id(), _uint(timeout_ns, positive=True))

    def wait_terminal(self, timeout_ns):
        return self._lib._document("host_wait_terminal", self._id(),
                                   _uint(timeout_ns, positive=True))

    @property
    def phase(self):
        result = _U32()
        self._lib._call("host_phase", self._id(), C.byref(result))
        return Phase(result.value)

    @property
    def snapshot(self):
        return self._lib._document("host_snapshot", self._id())

    def subscribe(self):
        return self._lib._new(Observer, "host_subscribe", self._id())


class Observer(Handle):
    def next(self, timeout_ns):
        return self._lib._document("observer_next", self._id(), _uint(timeout_ns, positive=True))
