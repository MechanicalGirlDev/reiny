//! Configurable real callbacks used by process-step scenarios.

use super::{Arc, AtomicUsize, BTreeMap, Ordering, mpsc, watch};
use crate::{
    component::{
        ComponentContext, ComponentFuture, StopReason,
        step::{StepComponent, StepComponentFactory},
    },
    step::{NodeError, StepFuture, StepInputs, StepOutputs, StepTick, StepValue},
};

#[derive(Clone, PartialEq, prost::Message)]
pub(in crate::component::process) struct Number {
    #[prost(sint64, tag = "1")]
    pub value: i64,
}
impl crate::MessageType for Number {
    const TYPE_NAME: &'static str = "process_test.Number";
    const SCHEMA_FINGERPRINT: u64 = 42;
    const DESCRIPTOR: Option<crate::Descriptor> = None;
}
#[derive(Clone, Copy)]
pub(in crate::component::process) enum Mode {
    Sum,
    Wait,
    Error,
    Panic,
    Invalid,
}
pub(super) struct Factory {
    pub(super) mode: Mode,
    pub(super) entered: mpsc::UnboundedSender<()>,
    pub(super) release: watch::Receiver<bool>,
    pub(super) constructions: Arc<AtomicUsize>,
}
struct Node {
    mode: Mode,
    entered: mpsc::UnboundedSender<()>,
    release: watch::Receiver<bool>,
    state: i64,
}
impl StepComponentFactory for Factory {
    fn create(&self, _: Option<&toml::Table>) -> crate::Result<Box<dyn StepComponent>> {
        self.constructions.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Node {
            mode: self.mode,
            entered: self.entered.clone(),
            release: self.release.clone(),
            state: 17,
        }))
    }
}
impl StepComponent for Node {
    fn prepare<'a>(&'a mut self, _: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async { Ok(()) })
    }
    fn prepared_outputs(&self) -> Result<StepOutputs, NodeError> {
        Ok(BTreeMap::from([(
            "out".into(),
            StepValue::encode(&Number { value: self.state }),
        )]))
    }
    fn step<'a>(
        &'a mut self,
        _: &'a ComponentContext,
        _: StepTick,
        inputs: StepInputs,
    ) -> StepFuture<'a> {
        Box::pin(async move {
            let _ = self.entered.send(());
            match self.mode {
                Mode::Wait => {
                    self.release
                        .wait_for(|ready| *ready)
                        .await
                        .map_err(|e| NodeError {
                            message: e.to_string(),
                        })?;
                }
                Mode::Error => {
                    return Err(NodeError {
                        message: "precise application cause".into(),
                    });
                }
                Mode::Panic => std::panic::resume_unwind(Box::new("precise panic cause")),
                Mode::Invalid => return Ok(BTreeMap::new()),
                Mode::Sum => {}
            }
            let previous = inputs["previous"]
                .decode::<Number>()
                .map_err(|e| NodeError {
                    message: e.to_string(),
                })?;
            self.state = previous.value + 1;
            self.prepared_outputs()
        })
    }
    fn stop<'a>(&'a mut self, _: &'a ComponentContext, _: StopReason) -> ComponentFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}
