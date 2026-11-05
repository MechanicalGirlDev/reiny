"""Local-step component callbacks and borrowed input/output lifetime."""

import ctypes as C
import threading

from ._abi import _Create, _Destroy, _Hook, _Stop
from ._handles import _FactoryReleaseToken
from ._lifecycle import Component, _Registration
from ._ownership import _tokens
from ._step_models import Tick, _StepCallback, _StepFactory
from ._step_handles import Inputs, Outputs


class StepComponent(Component):
    """Implement step(context, copied_tick, borrowed_inputs, borrowed_outputs)."""

    def step(self, context, tick, inputs, outputs):
        raise NotImplementedError("step must fill all declared outputs")


class _StepRegistration(_Registration):
    def __init__(self, library, factory):
        # Share the service wrapper's ownership roots, exception boundary and
        # callback lane adaptation; do not invent a second Python runtime.
        self.library, self.factory = library, factory
        self.instances = {}
        self.lock = threading.RLock()
        self.finalizer_errors = []
        self.token = next(_tokens)
        self.callbacks = (
            _Create(self.create), _Hook(self.prepare), _StepCallback(self.invoke_step),
            _Stop(self.stop), _Destroy(self.destroy),
        )
        self.table = _StepFactory(C.sizeof(_StepFactory), 1, 0, 0, self.token,
                                  *self.callbacks, _Destroy())
        self.release_token = library._new(_FactoryReleaseToken, "factory_release_token_new")

    def invoke_step(self, instance, context, tick, inputs_id, outputs_id, sink):
        inputs = Inputs(self.library, inputs_id, owned=False)
        outputs = Outputs(self.library, outputs_id, owned=False)
        try:
            return self.invoke(context, sink, lambda ctx: self.instance(instance).step(
                ctx, Tick._copy(tick.contents), inputs, outputs))
        finally:
            inputs._handle = 0
            outputs._handle = 0
