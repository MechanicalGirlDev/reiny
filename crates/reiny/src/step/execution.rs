use std::{
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, catch_unwind},
    task::Poll,
};

use super::{NodeError, SchedulerError, StepFailure, StepFailureKind, StepId, StepOutputs};

pub(super) struct AdvanceGuard<'a> {
    fault: &'a mut Option<StepFailure>,
    pub(super) failure: StepFailure,
    armed: bool,
}

impl<'a> AdvanceGuard<'a> {
    pub(super) fn new(fault: &'a mut Option<StepFailure>, tick: StepId) -> Self {
        Self {
            fault,
            failure: StepFailure {
                tick,
                kind: StepFailureKind::AdvanceDropped,
                instance: None,
                port: None,
                attempted: Vec::new(),
                completed: Vec::new(),
                detail: "advance dropped after dispatch; component effects may persist".to_owned(),
            },
            armed: false,
        }
    }

    pub(super) fn dispatch(&mut self, instance: &str) {
        self.failure.instance = Some(instance.to_owned());
        self.failure.attempted.push(instance.to_owned());
        self.armed = true;
    }

    pub(super) fn fail(mut self) -> SchedulerError {
        *self.fault = Some(self.failure.clone());
        self.armed = false;
        SchedulerError::Failed(Box::new(self.failure.clone()))
    }

    pub(super) fn commit(&mut self) {
        self.armed = false;
    }
}

impl Drop for AdvanceGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            if std::thread::panicking() {
                self.failure.kind = StepFailureKind::NodePanicked;
                self.failure.detail =
                    "unwound after dispatch; component effects may persist".into();
            }
            *self.fault = Some(self.failure.clone());
        }
    }
}

pub(crate) async fn catch_node(
    future: impl Future<Output = Result<StepOutputs, NodeError>>,
) -> Result<StepOutputs, (StepFailureKind, String)> {
    // Include synchronous invoke(), future polling, and normal future disposal
    // inside unwind boundaries. AssertUnwindSafe is justified because any unwind
    // permanently faults this epoch rather than reusing potentially damaged state.
    let mut future = Box::pin(future);
    let result = poll_fn(
        |cx| match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(result)) => Poll::Ready(Ok(result)),
            Err(payload) => Poll::Ready(Err(payload)),
        },
    )
    .await;
    let disposal = catch_unwind(AssertUnwindSafe(|| drop(future)));
    match disposal.and(result) {
        Ok(Ok(outputs)) => Ok(outputs),
        Ok(Err(error)) => Err((StepFailureKind::NodeError, error.message)),
        Err(payload) => {
            let detail = if let Some(message) = payload.downcast_ref::<String>() {
                message.clone()
            } else if let Some(message) = payload.downcast_ref::<&str>() {
                (*message).to_owned()
            } else {
                "node panicked with a non-string payload".to_owned()
            };
            Err((StepFailureKind::NodePanicked, detail))
        }
    }
}

/// Only crate-owned implementations may serialize the bookkeeping move. The
/// callback must only move prepared values and assign primitives; its returned
/// retired state is disposed after the gate unlocks.
pub(crate) trait Commit: Sync {
    fn commit<T>(
        &self,
        receipt: &std::sync::Arc<super::StepReceipt>,
        clock: &std::sync::Arc<super::ClockSnapshot>,
        apply: impl FnOnce() -> T,
    ) -> Result<T, StepFailureKind>;
}

pub(super) struct DirectCommit;

pub(super) fn invalid_output<'a>(
    schema: &'a std::collections::BTreeMap<String, super::StepType>,
    outputs: &'a StepOutputs,
) -> Option<(&'a str, &'static str)> {
    schema
        .iter()
        .find_map(|(port, expected)| match outputs.get(port) {
            None => Some((port.as_str(), "missing declared output")),
            Some(value) if value.step_type() != expected => {
                Some((port.as_str(), "output schema mismatch"))
            }
            Some(_) => None,
        })
        .or_else(|| {
            outputs
                .keys()
                .find(|port| !schema.contains_key(*port))
                .map(|port| (port.as_str(), "undeclared output"))
        })
}

impl Commit for DirectCommit {
    fn commit<T>(
        &self,
        _: &std::sync::Arc<super::StepReceipt>,
        _: &std::sync::Arc<super::ClockSnapshot>,
        apply: impl FnOnce() -> T,
    ) -> Result<T, StepFailureKind> {
        Ok(apply())
    }
}
