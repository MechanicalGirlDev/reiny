use super::{
    ProcessError,
    protocol::step::{BoundSteps, InvokeStep, StepSessionKey},
    step_wire::invalid,
};
use crate::step::{EpochId, StepId, StepSchema, StepTick};
use std::{collections::BTreeMap, time::Duration};

/// Reserved before scheduling user code. An invalid call ends the host session.
pub(super) struct ReplayGuard {
    session: StepSessionKey,
    epoch: u64,
    order: Vec<String>,
    assigned: Vec<String>,
    cursor: usize,
    active_run: Option<u64>,
    progress: BTreeMap<String, (u64, Duration, Option<u64>)>,
}
impl ReplayGuard {
    pub(super) fn new(
        bound: &BoundSteps,
        schemas: &BTreeMap<String, StepSchema>,
    ) -> Result<Self, ProcessError> {
        let initial = bound.initial_time.duration().map_err(|_| invalid())?;
        let assigned: Vec<_> = bound
            .order
            .iter()
            .filter(|id| schemas.contains_key(*id))
            .cloned()
            .collect();
        if assigned.is_empty() {
            return Err(invalid());
        }
        Ok(Self {
            session: bound.session.clone(),
            epoch: bound.epoch,
            order: bound.order.clone(),
            assigned,
            cursor: 0,
            active_run: None,
            progress: schemas
                .keys()
                .map(|id| (id.clone(), (0, initial, None)))
                .collect(),
        })
    }
    pub(super) fn reserve(&mut self, invocation: &InvokeStep) -> Result<StepTick, ProcessError> {
        invocation.tick.validate().map_err(|_| invalid())?;
        let call = &invocation.call;
        let (index, time, sequence) = self.progress.get_mut(&call.instance).ok_or_else(invalid)?;
        if call.session != self.session
            || call.run_sequence == u64::MAX
            || call.epoch != self.epoch
            || invocation.tick.epoch != self.epoch
            || call.tick_index != *index
            || invocation.tick.index != *index
            || invocation.tick.start.duration().map_err(|_| invalid())? != *time
            || sequence.is_some_and(|previous| call.run_sequence <= previous)
            || self.order.get(call.node_ordinal as usize) != Some(&call.instance)
            || self.assigned.get(self.cursor) != Some(&call.instance)
            || (self.cursor != 0 && self.active_run != Some(call.run_sequence))
        {
            return Err(invalid());
        }
        let tick = StepTick {
            id: StepId {
                epoch: EpochId::from_owner(self.epoch),
                index: call.tick_index,
            },
            start: *time,
            dt: invocation.tick.dt.duration().map_err(|_| invalid())?,
            end: invocation.tick.end.duration().map_err(|_| invalid())?,
        };
        *index = index.checked_add(1).ok_or_else(invalid)?;
        *time = tick.end;
        *sequence = Some(call.run_sequence);
        self.active_run = Some(call.run_sequence);
        self.cursor = (self.cursor + 1) % self.assigned.len();
        Ok(tick)
    }
}

#[cfg(test)]
mod tests {
    use super::super::protocol::{
        GraphGeneration, OwnerGeneration, PreparedDigest,
        step::{StepCallKey, WireGraph, WireTick},
    };
    use super::*;
    fn fixture() -> crate::Result<(ReplayGuard, InvokeStep)> {
        let session = StepSessionKey {
            incarnation: [1; 32],
            owner_generation: OwnerGeneration(1),
            graph_generation: GraphGeneration(2),
            graph_digest: PreparedDigest("graph".into()),
            plan_digest: PreparedDigest("plan".into()),
        };
        let bound = BoundSteps {
            session: session.clone(),
            epoch: 9,
            initial_time: Duration::ZERO.into(),
            order: vec!["a".into(), "b".into(), "c".into()],
            graph: WireGraph {
                nodes: BTreeMap::new(),
                inputs: Vec::new(),
            },
        };
        let schema = StepSchema {
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
        };
        let guard = ReplayGuard::new(
            &bound,
            &BTreeMap::from([("a".into(), schema.clone()), ("c".into(), schema)]),
        )?;
        let invoke = InvokeStep {
            call: StepCallKey {
                session,
                epoch: 9,
                run_sequence: 0,
                tick_index: 0,
                node_ordinal: 0,
                instance: "a".into(),
            },
            tick: WireTick {
                epoch: 9,
                index: 0,
                start: Duration::ZERO.into(),
                dt: Duration::from_secs(1).into(),
                end: Duration::from_secs(1).into(),
            },
            inputs: BTreeMap::new(),
            remaining_budget: Duration::from_secs(5).into(),
        };
        Ok((guard, invoke))
    }
    #[test]
    fn replay_order_and_attempt_identity_reject_before_second_callback() -> crate::Result<()> {
        let (mut guard, first) = fixture()?;
        guard.reserve(&first)?;
        assert!(guard.reserve(&first).is_err());
        let mut last = first.clone();
        last.call.instance = "c".into();
        last.call.node_ordinal = 2;
        last.call.run_sequence = 1;
        assert!(guard.reserve(&last).is_err());
        last.call.run_sequence = 0;
        guard.reserve(&last)?;
        assert!(guard.reserve(&last).is_err());
        Ok(())
    }
    #[test]
    fn forged_identity_ordinal_duration_and_overflow_reject() -> crate::Result<()> {
        for change in 0..9 {
            let (mut guard, mut invoke) = fixture()?;
            match change {
                0 => invoke.call.session.incarnation[0] = 2,
                1 => invoke.call.epoch = 8,
                2 => invoke.call.node_ordinal = 2,
                3 => invoke.call.tick_index = 1,
                4 => invoke.tick.dt.nanos = 1_000_000_000,
                5 => invoke.tick.dt = Duration::ZERO.into(),
                6 => invoke.tick.index = u64::MAX,
                7 => {
                    invoke.tick.start.secs = u64::MAX;
                    invoke.tick.dt.secs = 1;
                }
                _ => invoke.call.run_sequence = u64::MAX,
            }
            assert!(guard.reserve(&invoke).is_err());
        }
        Ok(())
    }
}
