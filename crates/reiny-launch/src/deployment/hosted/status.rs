//! Observations distinguish host process ownership from local task completion.

use std::collections::BTreeMap;

use reiny_core::bindings::ModuleReport;
use reiny_core::host_protocol::{HostFailure, HostOutcome, HostPhase};
use serde::{Deserialize, Serialize};

/// One owned component-host process generation.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ComponentHostStatus {
    /// Informational PID, not process-control authority.
    pub pid: Option<u32>,
    /// Monotonic child generation inside this owner.
    pub generation: u64,
    /// Whole-graph admission generation.
    pub graph_generation: u64,
    /// Verified frozen executable, transport and placement fingerprint.
    pub desired_fingerprint: String,
    /// Last acknowledged lifecycle stage or owner-observed failure.
    pub phase: HostPhase,
    /// First failure, retained through cleanup.
    pub error: Option<String>,
    /// First structured SDK failure; subsequent diagnostics cannot replace it.
    pub failure: Option<HostFailure>,
    /// Local task outcome, never a substitute for exact process reaping.
    pub outcome: Option<HostOutcome>,
    /// Actual process exit or enforced tree-termination result.
    pub exit: Option<String>,
    /// Additional cleanup or protocol failures.
    pub cleanup_errors: Vec<String>,
}

/// One component instance; instances sharing a host are not independently killable.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ComponentStatus {
    /// Exact root placement identity.
    pub host: String,
    /// Frozen component fingerprint.
    pub desired_fingerprint: String,
    /// Last acknowledged stage; unconfirmed cleanup stays unresponsive.
    pub phase: HostPhase,
    /// Exact compiled report from the current generation.
    pub report: Option<ModuleReport>,
    /// First observed host or instance failure.
    pub error: Option<String>,
}

pub(in crate::deployment) fn install(owner: &mut super::super::Owner<'_>) {
    owner.status.component_hosts = owner
        .desired
        .component_hosts
        .iter()
        .map(|(name, host)| {
            (
                name.clone(),
                ComponentHostStatus {
                    pid: None,
                    generation: 0,
                    graph_generation: 0,
                    desired_fingerprint: host.fingerprint.clone(),
                    phase: HostPhase::Authenticating,
                    error: None,
                    failure: None,
                    outcome: None,
                    exit: None,
                    cleanup_errors: Vec::new(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    owner.status.components = owner
        .desired
        .components
        .iter()
        .map(|component| {
            (
                component.namespace.clone(),
                ComponentStatus {
                    host: component.host.clone(),
                    desired_fingerprint: component.fingerprint.clone(),
                    phase: HostPhase::Authenticating,
                    report: None,
                    error: None,
                },
            )
        })
        .collect();
}

pub(super) fn phase(owner: &mut super::super::Owner<'_>, host: &str, phase: HostPhase) {
    if let Some(status) = owner.status.component_hosts.get_mut(host) {
        status.phase = phase;
    }
    for status in owner
        .status
        .components
        .values_mut()
        .filter(|status| status.host == host)
    {
        status.phase = phase;
    }
}
