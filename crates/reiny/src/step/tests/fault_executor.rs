use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use super::*;
use crate::step::StepOutputs;

#[derive(Clone, Copy, Debug)]
pub(super) enum Fault {
    Error,
    InvokePanic,
    PollPanic,
    DropPanic,
    MissingOutput,
    ExtraOutput,
    WrongName,
    WrongFingerprint,
    Pending,
}

pub(super) struct Faulting {
    pub(super) arithmetic: Arithmetic,
    pub(super) effects: usize,
    pub(super) fault: Fault,
}

impl Faulting {
    pub(super) fn new(fault: Fault) -> Self {
        Self {
            arithmetic: Arithmetic::default(),
            effects: 0,
            fault,
        }
    }
}

impl StepExecutor for Faulting {
    fn invoke<'a>(
        &'a mut self,
        instance: &'a str,
        tick: StepTick,
        inputs: StepInputs,
    ) -> StepFuture<'a> {
        if instance != "physics" {
            return self.arithmetic.invoke(instance, tick, inputs);
        }
        self.effects += 1;
        match self.fault {
            Fault::InvokePanic => std::panic::resume_unwind(Box::new("synchronous node panic")),
            Fault::PollPanic => {
                Box::pin(async { std::panic::resume_unwind(Box::new("node polling panic")) })
            }
            Fault::DropPanic => Box::pin(PanicOnDrop),
            Fault::Pending => Box::pin(std::future::pending()),
            Fault::Error => Box::pin(async {
                Err(NodeError {
                    message: "failed after effect".into(),
                })
            }),
            Fault::MissingOutput => Box::pin(async { Ok(BTreeMap::new()) }),
            Fault::ExtraOutput => Box::pin(async move {
                let mut outputs = self.arithmetic.invoke(instance, tick, inputs).await?;
                outputs.insert("extra".into(), value(9));
                Ok(outputs)
            }),
            Fault::WrongName => Box::pin(async {
                Ok(BTreeMap::from([(
                    "out".into(),
                    StepValue::encode(&OtherNumber { value: 2 }),
                )]))
            }),
            Fault::WrongFingerprint => Box::pin(async {
                Ok(BTreeMap::from([(
                    "out".into(),
                    StepValue::encode(&ChangedNumber { value: 2 }),
                )]))
            }),
        }
    }
}

struct PanicOnDrop;

impl Future for PanicOnDrop {
    type Output = Result<StepOutputs, NodeError>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(BTreeMap::from([("out".into(), value(5))])))
    }
}

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        std::panic::resume_unwind(Box::new("node disposal panic"));
    }
}
