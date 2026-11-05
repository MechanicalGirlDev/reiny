use super::{
    Adapter, CLOSED, Command, Component, ComponentContext, ComponentFuture, Error, INVALID, Lane,
    Mutex, Object, Registered, Result, StopReason, insert, lock, object, oneshot, variant, wrong,
};
use reiny::component::{StepComponent, StepComponentFactory};
use reiny::step::{NodeError, StepFuture, StepInputs, StepOutputs, StepTick};

impl StepComponentFactory for Registered {
    fn create(&self, _config: Option<&toml::Table>) -> reiny::Result<Box<dyn StepComponent>> {
        Ok(Box::new(Adapter {
            factory: self.factory.clone(),
            lanes: self.lanes.clone(),
            sender: None,
            context: None,
            _claim: self.lanes.add()?,
        }))
    }
}

impl StepComponent for Adapter {
    fn prepare<'a>(&'a mut self, context: &'a ComponentContext) -> ComponentFuture<'a> {
        Component::prepare(self, context)
    }

    fn step<'a>(
        &'a mut self,
        _context: &'a ComponentContext,
        tick: StepTick,
        inputs: StepInputs,
    ) -> StepFuture<'a> {
        Box::pin(async move {
            let result = async {
                let (reply, result) = oneshot::channel();
                self.sender
                    .as_ref()
                    .ok_or_else(|| Error::new(CLOSED, "step lane not started"))?
                    .send(Command::Step(tick, inputs, reply))
                    .map_err(|_| Error::new(CLOSED, "step lane closed"))?;
                result
                    .await
                    .map_err(|_| Error::new(CLOSED, "step lane terminated"))?
            }
            .await;
            result.map_err(|error| NodeError {
                message: error.to_string(),
            })
        })
    }

    fn stop<'a>(
        &'a mut self,
        context: &'a ComponentContext,
        reason: StopReason,
    ) -> ComponentFuture<'a> {
        Component::stop(self, context, reason)
    }
}

impl Lane {
    pub(super) fn step(&self, tick: StepTick, inputs: StepInputs) -> Result<StepOutputs> {
        {
            let mut phase = lock(&self.context.phase)?;
            if *phase == 2 {
                return Err(Error::new(CLOSED, "step context closed"));
            }
            *phase = 1;
        }
        let registration = self
            .factory
            .step
            .as_ref()
            .ok_or_else(|| Error::new(INVALID, "not a step factory"))?;
        let inputs = Borrowed(insert(Object::StepInputs(inputs), true)?);
        let sink = Borrowed(insert(
            Object::StepSink(Mutex::new(super::super::steps::Sink {
                owner: std::thread::current().id(),
                outputs: StepOutputs::new(),
            })),
            true,
        )?);
        let tick = super::super::steps::Tick::from(tick);
        Self::invoke(|errors| {
            // SAFETY: [FFI/lifetime] instance is lane-confined; the table owner
            // remains alive. Tick and borrowed handles live through the call.
            unsafe {
                (registration.callback)(
                    self.instance,
                    self.id,
                    &raw const tick,
                    inputs.0,
                    sink.0,
                    errors,
                )
            }
        })?;
        let value = object(sink.0)?;
        Ok(std::mem::take(
            &mut lock(variant!(value, StepSink))?.outputs,
        ))
    }
}

struct Borrowed(u64);

impl Drop for Borrowed {
    fn drop(&mut self) {
        if let Err(error) = crate::c_api::remove(self.0, true) {
            tracing::error!(%error, "cannot release borrowed step handle");
        }
    }
}
