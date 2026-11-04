use std::{collections::BTreeMap, fmt, time::Duration};

use super::{
    ClockSnapshot, StepId, StepInput, StepSource, StepTick, StepType, StepValue, plan::Plan,
};

/// Caller-supplied duration and values for external inputs only.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StepRequest {
    /// Positive logical duration.
    pub dt: Duration,
    /// Exactly the external routes declared in the graph.
    pub inputs: BTreeMap<StepInput, StepValue>,
}

/// A request rejected before dispatching any node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StepRequestError {
    /// Logical duration must be positive.
    ZeroDt,
    /// Logical end time cannot be represented.
    TimeOverflow,
    /// The next tick index cannot be represented.
    TickOverflow,
    /// A required external value is absent.
    MissingInput(StepInput),
    /// A supplied value has no external route.
    UnexpectedInput(StepInput),
    /// An external value has a different exact schema.
    TypeMismatch {
        /// External input address.
        input: StepInput,
        /// Declared schema.
        expected: StepType,
        /// Supplied schema.
        actual: StepType,
    },
}

impl fmt::Display for StepRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid step request: {self:?}")
    }
}

impl std::error::Error for StepRequestError {}

pub(crate) struct ValidatedRequest {
    pub(super) tick: StepTick,
    pub(super) next_index: u64,
    pub(super) inputs: BTreeMap<StepInput, StepValue>,
}

impl StepRequest {
    pub(crate) fn validate(
        self,
        plan: &Plan,
        clock: &ClockSnapshot,
    ) -> Result<ValidatedRequest, StepRequestError> {
        if self.dt.is_zero() {
            return Err(StepRequestError::ZeroDt);
        }
        let end = clock
            .time
            .checked_add(self.dt)
            .ok_or(StepRequestError::TimeOverflow)?;
        let next_index = clock
            .next_index
            .checked_add(1)
            .ok_or(StepRequestError::TickOverflow)?;
        for input in self.inputs.keys() {
            match plan.graph.inputs.get(input) {
                Some(StepSource::External) => {}
                Some(StepSource::SameTick(_) | StepSource::PreviousTick { .. }) | None => {
                    return Err(StepRequestError::UnexpectedInput(input.clone()));
                }
            }
        }
        for (input, source) in &plan.graph.inputs {
            match source {
                StepSource::External => {
                    let value = self
                        .inputs
                        .get(input)
                        .ok_or_else(|| StepRequestError::MissingInput(input.clone()))?;
                    let expected = &plan.graph.nodes[&input.instance].inputs[&input.port];
                    if value.step_type() != expected {
                        return Err(StepRequestError::TypeMismatch {
                            input: input.clone(),
                            expected: expected.clone(),
                            actual: value.step_type().clone(),
                        });
                    }
                }
                StepSource::SameTick(_) | StepSource::PreviousTick { .. } => {}
            }
        }
        Ok(ValidatedRequest {
            tick: StepTick {
                id: StepId {
                    epoch: clock.epoch,
                    index: clock.next_index,
                },
                start: clock.time,
                dt: self.dt,
                end,
            },
            next_index,
            inputs: self.inputs,
        })
    }
}
