use std::{collections::BTreeMap, fmt};

use super::{StepType, StepValue};

/// Exact destination of one step value.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct StepInput {
    /// Component instance namespace.
    pub instance: String,
    /// Declared input name.
    pub port: String,
}

/// Exact source of one step value.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct StepOutput {
    /// Component instance namespace.
    pub instance: String,
    /// Declared output name.
    pub port: String,
}

/// Immutable input/output contracts for a node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StepSchema {
    /// Required input names and exact schemas.
    pub inputs: BTreeMap<String, StepType>,
    /// Required output names and exact schemas.
    pub outputs: BTreeMap<String, StepType>,
}

/// The sole authorized source for a declared input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StepSource {
    /// A value supplied in each advancement request.
    External,
    /// An output produced earlier in the same logical tick.
    SameTick(StepOutput),
    /// A successful output from the preceding tick, or an explicit initial value.
    PreviousTick {
        /// Output to retain for the next tick.
        source: StepOutput,
        /// Value consumed at tick zero.
        initial: StepValue,
    },
}

/// A local operation-specific graph; it grants no stream or RPC permissions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StepGraph {
    /// Node schemas, indexed by exact instance namespace.
    pub nodes: BTreeMap<String, StepSchema>,
    /// Exactly one route for every declared input.
    pub inputs: BTreeMap<StepInput, StepSource>,
}

/// Graph compilation failed before any node was dispatched.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StepPlanError {
    /// At least one node is required.
    EmptyGraph,
    /// Instance does not satisfy Reiny namespace rules.
    InvalidInstance {
        /// Rejected instance.
        instance: String,
    },
    /// Port does not satisfy Reiny endpoint-name rules.
    InvalidPort {
        /// Declaring instance.
        instance: String,
        /// Rejected port.
        port: String,
    },
    /// A declared input has no source.
    MissingInput {
        /// Unwired input.
        input: StepInput,
    },
    /// A route names an undeclared input.
    UnknownInput {
        /// Unrecognized input.
        input: StepInput,
    },
    /// A route names an undeclared output.
    UnknownSource {
        /// Unrecognized output.
        output: StepOutput,
    },
    /// Source and destination schemas differ.
    TypeMismatch {
        /// Destination.
        input: StepInput,
        /// Destination schema.
        expected: StepType,
        /// Source schema.
        actual: StepType,
    },
    /// A delayed edge's initial schema differs.
    InitialTypeMismatch {
        /// Destination.
        input: StepInput,
        /// Destination schema.
        expected: StepType,
        /// Initial value schema.
        actual: StepType,
    },
    /// Same-tick edges cannot be topologically ordered.
    SameTickCycle {
        /// Lexically ordered nodes blocked by the cycle.
        instances: Vec<String>,
    },
    /// This process has exhausted its unique epoch identifiers.
    EpochExhausted,
}

impl fmt::Display for StepPlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid step graph: {self:?}")
    }
}

impl std::error::Error for StepPlanError {}
