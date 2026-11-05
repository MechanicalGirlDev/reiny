use protocol::{Compatibility, Milliseconds, Prepare};
use reiny_core::bindings::validate_report;

use super::{
    BTreeSet, Context, HostMessage, HostPhase, Instant, Owner, OwnerMessage, Phase, Result,
    Session, ensure, protocol, status,
};

impl Session {
    pub(super) fn message(&mut self, owner: &mut Owner<'_>, message: HostMessage) -> Result<()> {
        let name = self.identity.host_id.0.clone();
        if self.phase == Phase::Authenticating {
            ensure!(
                matches!(&message, HostMessage::Authenticate(_)),
                "first host message must authenticate"
            );
        }
        match message {
            HostMessage::Steps(_) => anyhow::bail!("step extension was not negotiated"),
            HostMessage::Authenticate(proof) => self.authenticate(&proof)?,
            HostMessage::Report(report) => {
                ensure!(self.phase == Phase::Preparing, "report outside preparation");
                let expected = self
                    .assignments
                    .get(self.reports.len())
                    .context("excess host report")?;
                ensure!(
                    report.namespace == expected.bindings.namespace,
                    "unexpected, duplicate or unordered host report"
                );
                validate_report(&expected.bindings, &report)?;
                self.reports.insert(report.namespace.clone());
                let instance = owner
                    .status
                    .components
                    .get_mut(&report.namespace)
                    .context("unknown report namespace")?;
                instance.report = Some(report);
            }
            HostMessage::PreparedComplete {} => {
                ensure!(
                    self.phase == Phase::Preparing && self.reports.len() == self.assignments.len(),
                    "incomplete host report set"
                );
                self.phase = Phase::Prepared;
            }
            HostMessage::Committed {} => {
                ensure!(
                    self.phase == Phase::Committing,
                    "unexpected commit acknowledgement"
                );
                self.phase = Phase::Committed;
            }
            HostMessage::Running {} => {
                ensure!(
                    self.phase == Phase::Activating,
                    "unexpected running acknowledgement"
                );
                self.phase = Phase::Running;
            }
            HostMessage::Status(snapshot) => {
                ensure!(
                    snapshot.phase == self.observed_phase(),
                    "host status stage mismatch"
                );
                let expected: BTreeSet<_> = self
                    .assignments
                    .iter()
                    .map(|entry| &entry.bindings.namespace)
                    .collect();
                ensure!(
                    snapshot.instances.keys().collect::<BTreeSet<_>>() == expected,
                    "host status namespace mismatch"
                );
                for (namespace, instance) in snapshot.instances {
                    let known = owner
                        .status
                        .components
                        .get(&namespace)
                        .context("unknown status namespace")?;
                    ensure!(
                        instance.report == known.report,
                        "host status altered admitted report"
                    );
                }
            }
            HostMessage::Failure(failure) => self.record_failure(owner, failure)?,
            HostMessage::Outcome(outcome) => self.outcome(owner, outcome)?,
        }
        status::phase(owner, &name, self.observed_phase());
        Ok(())
    }

    fn record_failure(
        &mut self,
        owner: &mut Owner<'_>,
        failure: protocol::HostFailure,
    ) -> Result<()> {
        ensure!(
            failure.instance.as_ref().is_none_or(|namespace| self
                .assignments
                .iter()
                .any(|entry| &entry.bindings.namespace == namespace)),
            "failure outside host assignment"
        );
        let expected_cancellation = self.phase == Phase::Stopping
            && self.stop_reason == Some(super::StopReason::AdmissionAborted)
            && failure.code == protocol::FailureCode::Cancelled
            && matches!(
                failure.phase,
                HostPhase::Preparing | HostPhase::Prepared | HostPhase::Committed
            );
        if !expected_cancellation {
            self.failure.get_or_insert_with(|| {
                format!("host failure {:?} in {:?}", failure.code, failure.phase)
            });
        }
        let status = owner
            .status
            .component_hosts
            .get_mut(&self.identity.host_id.0)
            .context("missing failed host status")?;
        ensure!(status.failure.is_none(), "duplicate host failure event");
        status.failure = Some(failure);
        Ok(())
    }

    fn authenticate(&mut self, proof: &protocol::Authenticate) -> Result<()> {
        ensure!(
            self.phase == Phase::Authenticating,
            "duplicate host authentication"
        );
        let nonce = self.nonce.take().context("consumed host nonce")?;
        let difference = nonce
            .as_bytes()
            .iter()
            .zip(proof.nonce.as_bytes())
            .fold(0_u8, |difference, (left, right)| {
                difference | (left ^ right)
            });
        ensure!(difference == 0, "host authentication rejected");
        ensure!(
            proof.compatibility == Compatibility::current(3),
            "host compatibility mismatch"
        );
        ensure!(
            proof.registrations.len() <= 1024
                && proof
                    .registrations
                    .windows(2)
                    .all(|pair| pair[0].factory < pair[1].factory),
            "host factory registrations must be sorted and unique"
        );
        let required: BTreeSet<_> = self
            .assignments
            .iter()
            .map(|entry| entry.implementation.clone())
            .collect();
        ensure!(
            required
                .iter()
                .all(|entry| proof.registrations.binary_search(entry).is_ok()),
            "required host factory revision missing"
        );
        let channel = self
            .channel
            .as_mut()
            .context("authenticated host has no channel")?;
        channel.queue(OwnerMessage::Authenticated {
            compatibility: Compatibility::current(3),
            required_factories: required.into_iter().collect(),
        })?;
        channel.queue(OwnerMessage::Prepare(Prepare {
            assignments: self.assignments.clone(),
            transport: self.transport.clone(),
            remaining_budget: Milliseconds(u64::try_from(
                self.startup_deadline
                    .saturating_duration_since(Instant::now())
                    .as_millis(),
            )?),
        }))?;
        self.phase = Phase::Preparing;
        Ok(())
    }

    pub(super) fn observed_phase(&self) -> HostPhase {
        match self.phase {
            Phase::Authenticating => HostPhase::Authenticating,
            Phase::Preparing => HostPhase::Preparing,
            Phase::Prepared | Phase::Committing => HostPhase::Prepared,
            Phase::Committed | Phase::Activating => HostPhase::Committed,
            Phase::Running => HostPhase::Running,
            Phase::Stopping | Phase::Terminal => HostPhase::Stopping,
        }
    }
}
