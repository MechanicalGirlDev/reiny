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

pub(super) struct Call {
    tick: StepTick,
    inputs: StepInputs,
    reply: oneshot::Sender<Result<StepOutputs, (StepFailureKind, String)>>,
}

/// A distinct adapter per instance, never a component shared between factories.
pub(super) struct Factory {
    pub factory: Arc<dyn StepComponentFactory>,
    pub calls: Mutex<Option<mpsc::UnboundedReceiver<Call>>>,
}

impl ComponentFactory for Factory {
    fn create(&self, config: Option<&toml::Table>) -> crate::Result<Box<dyn Component>> {
        let component = self.factory.create(config)?;
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
        }))
    }
}

struct Adapter {
    component: Box<dyn super::StepComponent>,
    calls: mpsc::UnboundedReceiver<Call>,
    panicked: bool,
}

impl Component for Adapter {
    fn prepare<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a> {
        self.component.prepare(ctx)
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
