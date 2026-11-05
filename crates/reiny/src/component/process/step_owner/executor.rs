//! Sequential remote dispatch and retained-result accounting.

use super::super::{
    protocol::{
        HostMessage, Milliseconds, OwnerMessage, StopReason,
        step::{
            InvokeStep, StepCallKey, StepHostMessage, StepOwnerMessage, StepSessionKey, WireTick,
        },
    },
    step_wire::{export_values, import_failure, import_values},
};
use super::Channel;
use crate::{
    component::step::{
        DispatchFailure, ManagedStepExecutor, ManagedStepFuture, StepDispatchContext,
    },
    step::{StepFailureKind, StepInputs},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};
use tokio::time::Instant;

pub(super) struct Executor {
    pub(super) channels: tokio::sync::oneshot::Receiver<Vec<Channel>>,
    pub(super) ready: Option<Vec<Channel>>,
    pub(super) placement: BTreeMap<String, usize>,
    pub(super) key: StepSessionKey,
    pub(super) cleanup:
        tokio::sync::watch::Sender<BTreeMap<String, Option<super::super::protocol::HostOutcome>>>,
    pub(super) retention: std::sync::Arc<super::super::step_client::Retention>,
    pub(super) delayed: BTreeSet<crate::step::StepOutput>,
    pub(super) final_ordinal: u32,
    pub(super) run_sequence: Option<u64>,
    pub(super) retained: usize,
    pub(super) next_previous: usize,
}

impl ManagedStepExecutor for Executor {
    fn shutdown(&mut self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            if self.ready.is_none()
                && let Ok(channels) = self.channels.try_recv()
            {
                self.ready = Some(channels);
            }
            if let Some(channels) = self.ready.take() {
                let deadline = Instant::now() + Duration::from_secs(5);
                for mut channel in channels {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    let remaining_budget = Milliseconds(
                        remaining.as_secs() * 1000 + u64::from(remaining.subsec_millis()),
                    );
                    // An interrupted frame cannot safely carry another command.
                    // Dropping that exact connection requests owner-loss cleanup.
                    if channel.healthy
                        && channel
                            .send(
                                OwnerMessage::Stop {
                                    reason: StopReason::Requested,
                                    remaining_budget,
                                },
                                deadline,
                            )
                            .await
                            .is_ok()
                    {
                        loop {
                            match channel.receive(deadline).await {
                                Ok(HostMessage::Failure(_)) => {}
                                Ok(HostMessage::Outcome(outcome)) => {
                                    self.cleanup.send_modify(|outcomes| {
                                        outcomes.insert(
                                            channel.identity.host_id.0.clone(),
                                            Some(outcome),
                                        );
                                    });
                                    break;
                                }
                                _ => break,
                            }
                        }
                    }
                }
            }
        })
    }
    fn invoke<'a>(
        &'a mut self,
        context: StepDispatchContext,
        instance: &'a str,
        inputs: StepInputs,
    ) -> ManagedStepFuture<'a> {
        Box::pin(async move {
            let lost = |message: String| DispatchFailure {
                kind: StepFailureKind::WorkerLost,
                message,
            };
            if self.ready.is_none() {
                self.ready = Some(
                    (&mut self.channels)
                        .await
                        .map_err(|_| lost("owner admission disappeared".into()))?,
                );
            }
            let channels = self
                .ready
                .as_mut()
                .ok_or_else(|| lost("missing host channels".into()))?;
            let index = self
                .placement
                .get(instance)
                .ok_or_else(|| lost("unassigned node".into()))?;
            let channel = &mut channels[*index];
            let tick = context.tick;
            let call = StepCallKey {
                session: self.key.clone(),
                epoch: tick.id.epoch.get(),
                run_sequence: context.run.sequence,
                tick_index: tick.id.index,
                node_ordinal: context.ordinal,
                instance: instance.into(),
            };
            let invocation = InvokeStep {
                call: call.clone(),
                tick: WireTick {
                    epoch: tick.id.epoch.get(),
                    index: tick.id.index,
                    start: tick.start.into(),
                    dt: tick.dt.into(),
                    end: tick.end.into(),
                },
                inputs: export_values(inputs).map_err(|e| lost(e.to_string()))?,
                remaining_budget: context
                    .deadline
                    .saturating_duration_since(Instant::now())
                    .into(),
            };
            channel
                .send(
                    OwnerMessage::Steps(Box::new(StepOwnerMessage::Invoke(invocation))),
                    context.deadline,
                )
                .await
                .map_err(|e| lost(e.to_string()))?;
            match channel
                .receive(context.deadline)
                .await
                .map_err(|e| lost(e.to_string()))?
            {
                HostMessage::Steps(message) => match *message {
                    StepHostMessage::Result(result) if result.call == call => {
                        let outputs = import_values(result.result.map_err(import_failure)?)
                            .map_err(|e| lost(e.to_string()))?;
                        self.retain_outputs(context, instance, &outputs)?;
                        Ok(outputs)
                    }
                    _ => Err(lost("unsolicited or mismatched step result".into())),
                },
                _ => Err(lost("host disappeared during invocation".into())),
            }
        })
    }
}

impl Executor {
    fn retain_outputs(
        &mut self,
        context: StepDispatchContext,
        instance: &str,
        outputs: &crate::step::StepOutputs,
    ) -> Result<(), DispatchFailure> {
        let bounds = || DispatchFailure {
            kind: StepFailureKind::InvalidOutputs,
            message: "aggregate retained step bytes exceeded".into(),
        };
        if self.run_sequence != Some(context.run.sequence) {
            self.run_sequence = Some(context.run.sequence);
            self.retained = 0;
            self.next_previous = 0;
        }
        for (port, value) in outputs {
            self.retained = self
                .retained
                .checked_add(value.as_encoded().len())
                .ok_or_else(bounds)?;
            if self.delayed.contains(&crate::step::StepOutput {
                instance: instance.into(),
                port: port.clone(),
            }) {
                self.next_previous = self
                    .next_previous
                    .checked_add(value.as_encoded().len())
                    .ok_or_else(bounds)?;
            }
        }
        let used = self
            .retained
            .checked_add(self.next_previous)
            .ok_or_else(bounds)?;
        if used
            > self
                .retention
                .available
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(bounds());
        }
        if context.ordinal == self.final_ordinal {
            self.retention
                .previous
                .store(self.next_previous, std::sync::atomic::Ordering::Release);
        }
        Ok(())
    }
}
