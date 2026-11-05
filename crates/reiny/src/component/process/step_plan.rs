use super::{
    protocol::step::{MAX_ENTRIES, MAX_VALUE_BYTES, StepPreparedReport},
    step_budget::SnapshotBudget,
    step_wire::import_values,
};
use crate::step::{
    Plan, PlanRoute, StepGraph, StepInput, StepOutput, StepSchema, StepSource, StepValue,
};
use std::collections::BTreeMap;

/// Explicit initial state, never a guessed substitute for prepared native data.
#[derive(Clone, Debug)]
pub enum ProcessStepSeed {
    /// Caller-owned immutable literal.
    Literal(StepValue),
    /// Snapshot from the actual paused object retained in its process host.
    PreparedOutput(StepOutput),
}

/// Exactly one declared source for each named input.
#[derive(Clone, Debug)]
pub enum ProcessStepSource {
    /// Supplied by the admission client on each tick.
    External,
    /// Output from a preceding node in the same tick.
    SameTick(StepOutput),
    /// Last committed output, initialized from an explicit seed.
    PreviousTick {
        /// Source retained after every successful tick.
        source: StepOutput,
        /// Literal or actual prepared state.
        initial: ProcessStepSeed,
    },
}

/// Draft validated without constructing an epoch or fabricating seed bytes.
#[derive(Clone, Debug)]
pub struct ProcessStepPlan {
    /// Exact schema for every independent node.
    pub nodes: BTreeMap<String, StepSchema>,
    /// Exactly one source per input; no type-derived routes.
    pub inputs: BTreeMap<StepInput, ProcessStepSource>,
}

impl ProcessStepPlan {
    /// Use the authoritative scheduler's topology/type checks before preparation.
    pub fn validate(&self) -> crate::Result<()> {
        anyhow::ensure!(self.nodes.len() <= MAX_ENTRIES, "too many nodes");
        self.literal_budget()?;
        let mut routes = BTreeMap::new();
        for (input, source) in &self.inputs {
            let route = match source {
                ProcessStepSource::External => PlanRoute {
                    output: None,
                    initial: None,
                    same_tick: false,
                },
                ProcessStepSource::SameTick(output) => PlanRoute {
                    output: Some(output.clone()),
                    initial: None,
                    same_tick: true,
                },
                ProcessStepSource::PreviousTick { source, initial } => {
                    let ty = match initial {
                        ProcessStepSeed::Literal(value) => {
                            anyhow::ensure!(
                                value.as_encoded().len() <= MAX_VALUE_BYTES,
                                "oversized initial value"
                            );
                            value.step_type().clone()
                        }
                        ProcessStepSeed::PreparedOutput(output) => self
                            .nodes
                            .get(&output.instance)
                            .and_then(|s| s.outputs.get(&output.port))
                            .ok_or_else(|| anyhow::anyhow!("unknown prepared seed output"))?
                            .clone(),
                    };
                    PlanRoute {
                        output: Some(source.clone()),
                        initial: Some(ty),
                        same_tick: false,
                    }
                }
            };
            routes.insert(input.clone(), route);
        }
        Plan::validate_routes(&self.nodes, &routes)?;
        Ok(())
    }

    pub(super) fn literal_budget(&self) -> crate::Result<SnapshotBudget> {
        let mut budget = SnapshotBudget::default();
        for source in self.inputs.values() {
            if let ProcessStepSource::PreviousTick {
                initial: ProcessStepSeed::Literal(value),
                ..
            } = source
            {
                budget.charge(value.as_encoded().len())?;
            }
        }
        Ok(budget)
    }

    pub(super) fn resolve(
        self,
        reports: &BTreeMap<String, StepPreparedReport>,
    ) -> crate::Result<StepGraph> {
        let mut budget = SnapshotBudget::default();
        for report in reports.values() {
            for value in report.outputs.values() {
                budget.charge(value.bytes.len())?;
            }
        }
        let inputs = self
            .inputs
            .into_iter()
            .map(|(input, source)| {
                let source = match source {
                    ProcessStepSource::External => StepSource::External,
                    ProcessStepSource::SameTick(source) => StepSource::SameTick(source),
                    ProcessStepSource::PreviousTick { source, initial } => {
                        let initial = match initial {
                            ProcessStepSeed::Literal(value) => {
                                budget.charge(value.as_encoded().len())?;
                                value
                            }
                            ProcessStepSeed::PreparedOutput(output) => {
                                let value = reports
                                    .get(&output.instance)
                                    .and_then(|r| r.outputs.get(&output.port))
                                    .ok_or_else(|| {
                                        anyhow::anyhow!("missing authorized prepared seed")
                                    })?;
                                budget.charge(value.bytes.len())?;
                                import_values(BTreeMap::from([(
                                    output.port.clone(),
                                    value.clone(),
                                )]))?
                                .remove(&output.port)
                                .ok_or_else(|| anyhow::anyhow!("missing prepared value"))?
                            }
                        };
                        StepSource::PreviousTick { source, initial }
                    }
                };
                Ok((input, source))
            })
            .collect::<crate::Result<_>>()?;
        Ok(StepGraph {
            nodes: self.nodes,
            inputs,
        })
    }
}
