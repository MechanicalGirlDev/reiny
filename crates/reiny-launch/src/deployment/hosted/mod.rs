//! A staged graph inside the existing owner, never a second process supervisor.

use std::collections::{BTreeMap, BTreeSet};
use std::net::TcpListener;
use std::time::Instant;

use anyhow::{Context, Result, ensure};
use reiny_core::host_protocol::{
    self as protocol, ComponentAssignment, HostMessage, HostPhase, OwnerMessage, SessionIdentity,
    StopReason, TransportSettings,
};

use super::{DeploymentPhase, Owner, START_GRACE, STOP_GRACE};

mod assignment;
mod channel;
mod codec;
mod events;
mod shutdown;
mod spawn;
mod status;
mod tick;

pub use status::{ComponentHostStatus, ComponentStatus};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Authenticating,
    Preparing,
    Prepared,
    Committing,
    Committed,
    Activating,
    Running,
    Stopping,
    Terminal,
}

enum Admission {
    Preparing,
    Committing,
    Activating,
    Active,
    Aborting(DeploymentPhase),
    Finished,
}

struct Session {
    listener: Option<TcpListener>,
    channel: Option<channel::Channel>,
    identity: SessionIdentity,
    nonce: Option<protocol::Nonce>,
    assignments: Vec<ComponentAssignment>,
    transport: TransportSettings,
    phase: Phase,
    reports: BTreeSet<String>,
    startup_deadline: Instant,
    stop_deadline: Option<Instant>,
    stop_reason: Option<StopReason>,
    failure: Option<String>,
    remote: Option<super::remote::RemoteOwnership>,
    gate: Option<super::remote::Gate>,
}

pub(super) struct Hosted {
    sessions: BTreeMap<String, Session>,
    admission: Admission,
    deadline: Instant,
    digest: String,
}

impl Owner<'_> {
    pub(super) fn start_hosted(&mut self) -> Result<()> {
        self.graph_generation = self
            .graph_generation
            .checked_add(1)
            .context("graph generation exhausted")?;
        let hosted = Hosted {
            sessions: BTreeMap::new(),
            admission: Admission::Preparing,
            deadline: Instant::now() + START_GRACE,
            digest: super::digest(&serde_json::to_vec(&self.desired)?),
        };
        self.hosted = Some(hosted);
        self.with_hosted(|hosted, owner| hosted.spawn(owner))
    }

    pub(super) fn observe_hosted(&mut self) -> Result<()> {
        self.with_hosted(|hosted, owner| hosted.tick(owner))
    }

    pub(super) fn stop_hosted(&mut self) -> Result<()> {
        self.with_hosted(|hosted, owner| {
            hosted.stop_all(owner, StopReason::Requested, DeploymentPhase::Stopped)?;
            while !owner.children.is_empty() {
                hosted.tick(owner)?;
                owner.publish()?;
                if !owner.children.is_empty() {
                    std::thread::park_timeout(super::TICK);
                }
            }
            Ok(())
        })
    }

    fn with_hosted(
        &mut self,
        work: impl FnOnce(&mut Hosted, &mut Self) -> Result<()>,
    ) -> Result<()> {
        let mut hosted = self.hosted.take().context("missing hosted owner state")?;
        let result = work(&mut hosted, self);
        self.hosted = Some(hosted);
        result
    }

    pub(super) fn identical_hosted(&self, prepared: &crate::PreparedDeployment) -> Result<bool> {
        let digest = super::digest(&serde_json::to_vec(prepared)?);
        Ok(self.hosted.as_ref().is_some_and(|hosted| {
            matches!(
                hosted.admission,
                Admission::Preparing
                    | Admission::Committing
                    | Admission::Activating
                    | Admission::Active
            ) && self.status.error.is_none()
                && hosted.digest == digest
        }))
    }
}

pub(super) fn validate(prepared: &crate::PreparedDeployment) -> Result<()> {
    prepared.verify()?;
    ensure!(
        prepared.component_hosts.len() <= 64,
        "too many component hosts"
    );
    ensure!(
        prepared.components.len() <= 1024,
        "too many hosted component instances"
    );
    for host in prepared.component_hosts.values() {
        assignment::transport(host, prepared)?;
    }
    Ok(())
}

pub(super) use status::install;
