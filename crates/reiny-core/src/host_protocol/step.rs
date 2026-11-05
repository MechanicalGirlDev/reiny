//! Additive, explicitly negotiated synchronous-step extension.
use super::{GraphGeneration, OwnerGeneration, PreparedDigest};
use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::String,
    vec::Vec,
};
use serde::{Deserialize, Serialize};

/// Negotiated owner commands, never ordinary RPC.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum StepOwnerMessage {
    /// Explicit extension acceptance before any preparation.
    Authenticated {
        /// Exact supported extension.
        version: StepProtocol,
        /// Exact linked contracts.
        compatibility: super::Compatibility,
        /// Required registered factory revisions.
        required_factories: Vec<super::FactoryRegistration>,
    },
    /// Ordinary and step assignments validated independently.
    Prepare {
        /// Existing ordinary preparation contract.
        ordinary: super::Prepare,
        /// Exact step-node subset indexed by namespace.
        steps: BTreeMap<String, StepAssignment>,
    },
    /// Seal the final epoch before any commit.
    Bind(BoundSteps),
    /// Execute one node.
    Invoke(InvokeStep),
}

/// Host extension events; the first event explicitly advertises capability.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum StepHostMessage {
    /// Bootstrap proof plus explicit step protocol support.
    Authenticate {
        /// Existing authentication evidence.
        ordinary: super::Authenticate,
        /// Exact extension contract.
        version: StepProtocol,
        /// Registered step schemas, keyed by factory ID.
        schemas: BTreeMap<String, WireSchema>,
    },
    /// Snapshot from the same prepared object that will execute.
    Report(StepPreparedReport),
    /// Acknowledge the immutable final epoch.
    Bound(BoundSteps),
    /// One exact invocation result.
    Result(StepResult),
}

/// Maximum nodes or ports in an admitted step graph.
pub const MAX_ENTRIES: usize = 1024;
/// Maximum immutable protobuf payload.
pub const MAX_VALUE_BYTES: usize = 1024 * 1024;
/// Maximum aggregate retained values.
pub const MAX_RETAINED_BYTES: usize = 64 * 1024 * 1024;

/// Exact step-extension contract; unsupported versions reject during decoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u16", into = "u16")]
pub struct StepProtocol;
impl TryFrom<u16> for StepProtocol {
    type Error = &'static str;
    fn try_from(value: u16) -> Result<Self, Self::Error> {
        if value == 1 {
            Ok(Self)
        } else {
            Err("unsupported step protocol")
        }
    }
}
impl From<StepProtocol> for u16 {
    fn from(_: StepProtocol) -> Self {
        1
    }
}

/// Normalized logical or remaining wall duration, never an absolute clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireDuration {
    /// Whole seconds.
    pub secs: u64,
    /// Fractional nanoseconds, strictly below one billion.
    pub nanos: u32,
}
impl WireDuration {
    /// Reject normalization and arithmetic overflow rather than saturating.
    pub fn duration(self) -> Result<core::time::Duration, &'static str> {
        if self.nanos >= 1_000_000_000 {
            return Err("non-normalized duration");
        }
        Ok(core::time::Duration::new(self.secs, self.nanos))
    }
}
impl From<core::time::Duration> for WireDuration {
    fn from(value: core::time::Duration) -> Self {
        Self {
            secs: value.as_secs(),
            nanos: value.subsec_nanos(),
        }
    }
}

/// Globally distinguished admission, independent of process-local epoch IDs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepSessionKey {
    /// Fresh owner-generated entropy, never reused.
    pub incarnation: [u8; 32],
    /// Exact deployment owner.
    pub owner_generation: OwnerGeneration,
    /// Exact graph admission.
    pub graph_generation: GraphGeneration,
    /// Frozen assignment and asset identity.
    pub graph_digest: PreparedDigest,
    /// Final graph including actual prepared snapshots.
    pub plan_digest: PreparedDigest,
}

/// Full invocation correlation; reserve before dispatch and never replay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepCallKey {
    /// Admitted session.
    pub session: StepSessionKey,
    /// Authoritative scheduler epoch.
    pub epoch: u64,
    /// Accepted owner attempt.
    pub run_sequence: u64,
    /// Logical tick index.
    pub tick_index: u64,
    /// Position in the authoritative node order.
    pub node_ordinal: u32,
    /// Exact assigned namespace.
    pub instance: String,
}

/// Caller-clocked logical interval.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireTick {
    /// Owner epoch.
    pub epoch: u64,
    /// Logical index.
    pub index: u64,
    /// Previous committed time.
    pub start: WireDuration,
    /// Positive delta.
    pub dt: WireDuration,
    /// Checked start plus delta.
    pub end: WireDuration,
}
impl WireTick {
    /// Validate interval arithmetic before invoking user code.
    pub fn validate(&self) -> Result<(), &'static str> {
        let dt = self.dt.duration()?;
        if dt.is_zero() || self.start.duration()?.checked_add(dt) != Some(self.end.duration()?) {
            return Err("invalid tick interval");
        }
        self.index.checked_add(1).ok_or("tick index exhausted")?;
        Ok(())
    }
}

/// Protobuf identity plus exact canonical port assertions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireStepType {
    /// Complete protobuf name.
    pub type_name: String,
    /// Compiled fingerprint algorithm.
    pub fingerprint_version: u16,
    /// Transitive fingerprint.
    pub fingerprint: u64,
    /// Explicit producer/consumer assertions, never inferred from bytes.
    pub assertions: BTreeMap<String, String>,
}

/// Owned immutable snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireValue {
    /// Exact schema and semantics.
    pub ty: WireStepType,
    /// Protobuf bytes, not a foreign handle.
    pub bytes: Vec<u8>,
}

/// Immutable node port declaration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireSchema {
    /// Exact required input set.
    pub inputs: BTreeMap<String, WireStepType>,
    /// Exact output set.
    pub outputs: BTreeMap<String, WireStepType>,
}

/// Prepared-object evidence; ordinary reports remain independently validated.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepPreparedReport {
    /// Exact assigned instance.
    pub instance: String,
    /// Exact registered immutable schema.
    pub schema: WireSchema,
    /// Actual prepared instance evidence, compared to the owner's expectation.
    pub contract: Vec<u8>,
    /// Only explicitly authorized and requested prepared output snapshots.
    pub outputs: BTreeMap<String, WireValue>,
}

/// Per-instance step assignment, separate from ordinary port authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepAssignment {
    /// Exact registered schema.
    pub schema: WireSchema,
    /// Expected actual preparation evidence.
    pub contract: Vec<u8>,
    /// Explicitly requested prepared snapshot ports.
    pub seed_outputs: BTreeSet<String>,
}

/// Bound final plan and its authoritative node order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundSteps {
    /// Complete epoch identity.
    pub session: StepSessionKey,
    /// Owner-local scheduler epoch.
    pub epoch: u64,
    /// Initial committed time.
    pub initial_time: WireDuration,
    /// Full authoritative order, including other hosts.
    pub order: Vec<String>,
    /// Complete immutable graph whose schemas each host checks against its assignment.
    pub graph: WireGraph,
}

/// Exact output endpoint; matching types alone do not select one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireOutput {
    /// Component namespace.
    pub instance: String,
    /// Output port.
    pub port: String,
}

/// One complete input source descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WireSource {
    /// Explicit per-request input.
    External,
    /// Earlier output in this tick.
    SameTick {
        /// Exact source endpoint.
        source: WireOutput,
    },
    /// Last committed output or immutable finalized bootstrap.
    PreviousTick {
        /// Exact source endpoint.
        source: WireOutput,
        /// Final actual bootstrap value.
        initial: WireValue,
    },
}

/// One exact destination/source pair; duplicates reject before map construction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireRoute {
    /// Destination component.
    pub instance: String,
    /// Destination input.
    pub port: String,
    /// Sole authorized source.
    pub source: WireSource,
}

/// Final graph shared by the authoritative scheduler and bound process hosts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireGraph {
    /// Immutable node contracts.
    pub nodes: BTreeMap<String, WireSchema>,
    /// Named routes, with explicit duplicate rejection.
    pub inputs: Vec<WireRoute>,
}

/// Invocation carried only after negotiated preparation, binding and Run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvokeStep {
    /// Full correlation.
    pub call: StepCallKey,
    /// Canonical owner interval.
    pub tick: WireTick,
    /// Exact input values selected by the owner scheduler.
    pub inputs: BTreeMap<String, WireValue>,
    /// Remaining owner budget, never a restarted full timeout.
    pub remaining_budget: WireDuration,
}

/// Precise callback/transport failure; no generic node-error flattening.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepFailureCode {
    /// Application callback rejected.
    NodeError,
    /// Callback unwound.
    NodePanicked,
    /// Returned values violated the contract.
    InvalidOutputs,
    /// Owner cancelled.
    Cancelled,
    /// Budget exhausted.
    TimedOut,
    /// Host stopped.
    HostStopped,
    /// Worker/connection disappeared; effects unknown.
    WorkerLost,
}

/// Structured retained failure after an attempted invocation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireStepFailure {
    /// Machine-readable cause.
    pub code: StepFailureCode,
    /// Human diagnostic.
    pub message: String,
}

/// One correlated invocation result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepResult {
    /// Exact outstanding call.
    pub call: StepCallKey,
    /// Validated snapshots or a precise failure.
    pub result: Result<BTreeMap<String, WireValue>, WireStepFailure>,
}
