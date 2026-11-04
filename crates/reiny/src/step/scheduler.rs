use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use super::{
    ClockPhase, ClockSnapshot, EpochId, SchedulerError, StepExecutor, StepFailure, StepFailureKind,
    StepGraph, StepOutput, StepPlanError, StepReceipt, StepRequest, StepSource, StepValue,
    execution::{AdvanceGuard, Commit, DirectCommit, catch_node, invalid_output},
    plan::Plan,
};

static NEXT_EPOCH: AtomicU64 = AtomicU64::new(0);

/// Sequential local executor with explicit logical time and delayed feedback.
///
/// Graph compilation and request validation are side-effect free. Once any node
/// is dispatched, a failed or abandoned tick faults the epoch permanently.
pub struct StepScheduler {
    pub(crate) plan: Arc<Plan>,
    epoch: EpochId,
    next_index: u64,
    time: Duration,
    previous: BTreeMap<StepOutput, StepValue>,
    fault: Option<StepFailure>,
}

impl StepScheduler {
    /// Compile a graph and allocate a unique epoch within this process.
    pub fn new(graph: StepGraph, initial_time: Duration) -> Result<Self, StepPlanError> {
        let plan = Plan::compile(graph)?;
        let epoch = NEXT_EPOCH
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map(EpochId)
            .map_err(|_| StepPlanError::EpochExhausted)?;
        Ok(Self {
            plan: Arc::new(plan),
            epoch,
            next_index: 0,
            time: initial_time,
            previous: BTreeMap::new(),
            fault: None,
        })
    }

    /// Read the last committed clock and any permanent fault.
    ///
    /// An in-flight advancement exclusively borrows this scheduler; concurrent
    /// observation and host lifecycle phases belong to a host adapter.
    #[must_use]
    pub fn clock(&self) -> ClockSnapshot {
        ClockSnapshot {
            epoch: self.epoch,
            next_index: self.next_index,
            time: self.time,
            phase: match &self.fault {
                Some(failure) => ClockPhase::Faulted(failure.clone()),
                None => ClockPhase::Ready,
            },
        }
    }

    /// Execute one tick in lexical topological order, then commit bookkeeping.
    ///
    /// # Cancel safety
    ///
    /// Dropping this future before dispatch leaves the scheduler usable. Dropping
    /// it after dispatch faults the epoch, even if no node acknowledged the call.
    /// No component state or external side effect is rolled back. Unwind panics
    /// become `NodePanicked`; `panic=abort` still terminates the process.
    pub async fn advance(
        &mut self,
        executor: &mut dyn StepExecutor,
        request: StepRequest,
    ) -> Result<StepReceipt, SchedulerError> {
        self.advance_controlled(executor, request, &DirectCommit)
            .await
            .map(Arc::unwrap_or_clone)
    }

    pub(crate) async fn advance_controlled(
        &mut self,
        executor: &mut dyn StepExecutor,
        request: StepRequest,
        control: &impl Commit,
    ) -> Result<Arc<StepReceipt>, SchedulerError> {
        if let Some(failure) = &self.fault {
            return Err(SchedulerError::Faulted(Box::new(failure.clone())));
        }
        let request = request
            .validate(&self.plan, &self.clock())
            .map_err(SchedulerError::Request)?;
        let mut guard = AdvanceGuard::new(&mut self.fault, request.tick.id);
        let mut outputs = BTreeMap::<StepOutput, StepValue>::new();
        for instance in &self.plan.order {
            let inputs = self
                .plan
                .graph
                .inputs
                .iter()
                .filter(|(input, _)| &input.instance == instance)
                .map(|(input, source)| {
                    let value = match source {
                        StepSource::External => &request.inputs[input],
                        StepSource::SameTick(source) => &outputs[source],
                        StepSource::PreviousTick { source, initial } => {
                            if self.next_index == 0 {
                                initial
                            } else {
                                &self.previous[source]
                            }
                        }
                    };
                    (input.port.clone(), value.clone())
                })
                .collect();
            guard.dispatch(instance);
            let result =
                catch_node(async { executor.invoke(instance, request.tick, inputs).await }).await;
            let node_outputs = match result {
                Ok(outputs) => outputs,
                Err((kind, detail)) => {
                    guard.failure.kind = kind;
                    guard.failure.detail = detail;
                    return Err(guard.fail());
                }
            };
            let schema = &self.plan.graph.nodes[instance].outputs;
            if let Some((port, detail)) = invalid_output(schema, &node_outputs) {
                guard.failure.kind = StepFailureKind::InvalidOutputs;
                guard.failure.port = Some(port.to_owned());
                guard.failure.detail = detail.to_owned();
                return Err(guard.fail());
            }
            outputs.extend(node_outputs.into_iter().map(|(port, value)| {
                (
                    StepOutput {
                        instance: instance.clone(),
                        port,
                    },
                    value,
                )
            }));
            guard.failure.completed.push(instance.clone());
        }
        // Prepare the delayed store before changing any committed field. Only
        // declared delayed sources survive; fan-out retains a source once.
        let previous = self
            .plan
            .graph
            .inputs
            .values()
            .filter_map(|source| match source {
                StepSource::PreviousTick { source, .. } => {
                    Some((source.clone(), outputs[source].clone()))
                }
                StepSource::External | StepSource::SameTick(_) => None,
            })
            .collect();
        let receipt = Arc::new(StepReceipt {
            tick: request.tick,
            outputs,
        });
        let clock = Arc::new(ClockSnapshot {
            epoch: self.epoch,
            next_index: request.next_index,
            time: request.tick.end,
            phase: ClockPhase::Ready,
        });
        match control.commit(&receipt, &clock, || {
            let retired = std::mem::replace(&mut self.previous, previous);
            self.time = request.tick.end;
            self.next_index = request.next_index;
            guard.commit();
            retired
        }) {
            Ok(retired) => drop(retired),
            Err(kind) => {
                guard.failure.kind = kind;
                guard.failure.detail =
                    "host rejected bookkeeping commit; effects may persist".into();
                return Err(guard.fail());
            }
        }
        Ok(receipt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::step::{
        StepRequestError,
        tests::{Arithmetic, graph, request},
    };

    #[tokio::test]
    async fn index_overflow_is_rejected_before_dispatch() -> Result<(), StepPlanError> {
        let mut scheduler = StepScheduler::new(graph(), Duration::ZERO)?;
        scheduler.next_index = u64::MAX;
        let mut executor = Arithmetic::default();
        assert_eq!(
            scheduler.advance(&mut executor, request(2)).await,
            Err(SchedulerError::Request(StepRequestError::TickOverflow))
        );
        assert_eq!(executor.visits, Vec::new());
        assert_eq!(scheduler.clock().next_index, u64::MAX);
        assert_eq!(scheduler.clock().phase, ClockPhase::Ready);
        Ok(())
    }
}
