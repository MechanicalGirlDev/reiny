use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::sync::{mpsc, oneshot};

use super::StepComponentFactory;
use crate::{
    component::{Component, ComponentContext, ComponentFactory, ComponentFuture, StopReason},
    step::{
        NodeError, StepExecutor, StepFailureKind, StepFuture, StepInputs, StepOutputs, StepTick,
    },
};

pub(in crate::component) struct Call {
    pub tick: StepTick,
    pub inputs: StepInputs,
    pub reply: oneshot::Sender<Result<StepOutputs, (StepFailureKind, String)>>,
}

pub(in crate::component) type Prepared = Arc<std::sync::OnceLock<(StepOutputs, Vec<u8>)>>;

/// A distinct adapter per instance, never a component shared between factories.
pub(in crate::component) struct Factory {
    pub implementation: Arc<dyn StepComponentFactory>,
    pub calls: Mutex<Option<mpsc::UnboundedReceiver<Call>>>,
    pub prepared: Option<Prepared>,
}

impl ComponentFactory for Factory {
    fn create(&self, config: Option<&toml::Table>) -> crate::Result<Box<dyn Component>> {
        let component = self.implementation.create(config)?;
        let calls = self
            .calls
            .lock()
            .map_err(|_| anyhow::anyhow!("step factory poisoned"))?
            .take()
            .ok_or_else(|| anyhow::anyhow!("step instance already constructed"))?;
        Ok(Box::new(Adapter {
            component,
            calls,
            panicked: false,
            prepared: self.prepared.clone(),
        }))
    }
}

struct Adapter {
    component: Box<dyn super::StepComponent>,
    calls: mpsc::UnboundedReceiver<Call>,
    panicked: bool,
    prepared: Option<Prepared>,
}

impl Component for Adapter {
    fn prepare<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            self.component.prepare(ctx).await?;
            if let Some(prepared) = self.prepared.take() {
                let outputs = self.component.prepared_outputs()?;
                let contract = self.component.prepared_contract()?;
                prepared
                    .set((outputs, contract))
                    .map_err(|_| anyhow::anyhow!("prepared object already captured"))?;
            }
            Ok(())
        })
    }

    fn run<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            while let Some(call) = self.calls.recv().await {
                let result = crate::step::catch_node(async {
                    self.component.step(ctx, call.tick, call.inputs).await
                })
                .await;
                self.panicked = matches!(&result, Err((StepFailureKind::NodePanicked, _)));
                let failed = result.is_err();
                // Wake the scheduler only after callback disposal. It retains the
                // precise error; host cleanup remains the existing worker's job.
                let _ = call.reply.send(result);
                if failed {
                    // Do not accept another callback on a failed object. The
                    // scheduler records progress and asks the host to stop.
                    std::future::pending::<()>().await;
                }
            }
            std::future::pending().await
        })
    }

    fn stop<'a>(
        &'a mut self,
        ctx: &'a ComponentContext,
        reason: StopReason,
    ) -> ComponentFuture<'a> {
        if self.panicked {
            // Never enter user code on an object whose step unwound. The ordinary
            // worker still owns admission withdrawal, joining, and disposal.
            Box::pin(async { Ok(()) })
        } else {
            self.component.stop(ctx, reason)
        }
    }
}

pub(super) struct Executor {
    pub calls: BTreeMap<String, mpsc::UnboundedSender<Call>>,
    pub failure: Option<StepFailureKind>,
}

impl super::driver::DriverExecutor for Executor {
    fn begin(&mut self, _: &super::control::Run) {
        self.failure = None;
    }

    fn failure(&self) -> Option<StepFailureKind> {
        self.failure
    }
}

impl StepExecutor for Executor {
    fn invoke<'a>(
        &'a mut self,
        instance: &'a str,
        tick: StepTick,
        inputs: StepInputs,
    ) -> StepFuture<'a> {
        Box::pin(async move {
            let (reply, received) = oneshot::channel();
            let result = match self.calls.get(instance) {
                Some(calls) => {
                    if calls
                        .send(Call {
                            tick,
                            inputs,
                            reply,
                        })
                        .is_ok()
                    {
                        received.await.unwrap_or_else(|_| {
                            Err((
                                StepFailureKind::WorkerLost,
                                "step worker disappeared; effects unknown".into(),
                            ))
                        })
                    } else {
                        Err((
                            StepFailureKind::WorkerLost,
                            "step worker unavailable; effects unknown".into(),
                        ))
                    }
                }
                _ => Err((
                    StepFailureKind::WorkerLost,
                    "step worker unavailable; effects unknown".into(),
                )),
            };
            result.map_err(|(kind, message)| {
                self.failure = Some(kind);
                NodeError { message }
            })
        })
    }
}
