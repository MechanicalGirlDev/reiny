use std::io::ErrorKind;

use reiny_core::bindings::{ReportEntry, validate_graph};

use super::{
    Admission, Context, DeploymentPhase, Hosted, Instant, Owner, OwnerMessage, Phase, Result,
    Session, channel, ensure,
};

impl Hosted {
    pub(super) fn tick(&mut self, owner: &mut Owner<'_>) -> Result<()> {
        let names: Vec<_> = self.sessions.keys().cloned().collect();
        for name in names {
            if !owner.children.contains_key(&name) {
                continue;
            }
            let session = self
                .sessions
                .get_mut(&name)
                .context("missing host session")?;
            let result = session.poll(owner);
            if let Some(error) = self
                .sessions
                .get_mut(&name)
                .context("missing host session")?
                .failure
                .take()
            {
                self.fail(owner, &name, &error)?;
            }
            if let Err(error) = result {
                self.sessions
                    .get_mut(&name)
                    .context("missing host session")?
                    .channel = None;
                self.fail(owner, &name, &format!("{error:#}"))?;
            }
            self.observe_exit(owner, &name)?;
        }
        match self.admission {
            Admission::Preparing | Admission::Committing | Admission::Activating => {
                if Instant::now() >= self.deadline {
                    let name = self
                        .sessions
                        .keys()
                        .next()
                        .cloned()
                        .context("empty hosted admission")?;
                    self.fail(owner, &name, "whole hosted admission timed out")?;
                } else if let Err(error) = self.advance(owner) {
                    let name = self
                        .sessions
                        .keys()
                        .next()
                        .cloned()
                        .context("empty hosted admission")?;
                    self.fail(owner, &name, &format!("{error:#}"))?;
                }
            }
            Admission::Aborting(final_phase) if owner.children.is_empty() => {
                if self
                    .sessions
                    .values()
                    .any(|session| session.remote.is_some())
                {
                    owner.status.phase = DeploymentPhase::Failed;
                    owner.status.error.get_or_insert_with(|| "remote tree cleanup Unknown; exact-generation operator reconciliation required".to_owned());
                } else {
                    owner.status.phase = final_phase;
                }
                self.admission = Admission::Finished;
            }
            Admission::Active | Admission::Aborting(_) | Admission::Finished => {}
        }
        Ok(())
    }

    fn advance(&mut self, owner: &mut Owner<'_>) -> Result<()> {
        ensure!(!self.sessions.is_empty(), "empty hosted deployment");
        match self.admission {
            Admission::Preparing
                if self
                    .sessions
                    .values()
                    .all(|host| host.phase == Phase::Prepared) =>
            {
                ensure!(
                    self.sessions.len() == owner.desired.component_hosts.len(),
                    "incomplete host set"
                );
                let entries = owner
                    .desired
                    .components
                    .iter()
                    .map(|component| {
                        Ok(ReportEntry {
                            bindings: &component.bindings,
                            report: owner
                                .status
                                .components
                                .get(&component.namespace)
                                .and_then(|status| status.report.as_ref())
                                .context("missing assigned report")?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                validate_graph(&entries)?;
                self.send_all(&OwnerMessage::Commit {})?;
                self.admission = Admission::Committing;
                for session in self.sessions.values_mut() {
                    session.phase = Phase::Committing;
                }
            }
            Admission::Committing
                if self
                    .sessions
                    .values()
                    .all(|host| host.phase == Phase::Committed) =>
            {
                // Queue only after every acknowledgement. Delivery is not atomic:
                // a failure can race Run and cannot undo an earlier publication.
                self.send_all(&OwnerMessage::Run {})?;
                self.admission = Admission::Activating;
                for session in self.sessions.values_mut() {
                    session.phase = Phase::Activating;
                }
            }
            Admission::Activating
                if self
                    .sessions
                    .values()
                    .all(|host| host.phase == Phase::Running) =>
            {
                self.admission = Admission::Active;
                owner.status.phase = DeploymentPhase::Ready;
            }
            Admission::Preparing
            | Admission::Committing
            | Admission::Activating
            | Admission::Active
            | Admission::Aborting(_)
            | Admission::Finished => {}
        }
        Ok(())
    }

    fn send_all(&mut self, message: &OwnerMessage) -> Result<()> {
        for session in self.sessions.values_mut() {
            session
                .channel
                .as_mut()
                .context("host channel lost before admission")?
                .queue(message.clone())?;
        }
        Ok(())
    }
}

impl Session {
    fn poll(&mut self, owner: &mut Owner<'_>) -> Result<()> {
        if self.phase == Phase::Authenticating
            && let Some(gate) = &mut self.gate
        {
            gate.poll()?;
        }
        if let Some(listener) = &self.listener {
            match listener.accept() {
                Ok((stream, address)) => {
                    ensure!(address.ip().is_loopback(), "non-loopback host connection");
                    self.channel = Some(channel::Channel::new(stream, self.identity.clone())?);
                    self.listener = None;
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => return Ok(()),
                Err(error) => return Err(error.into()),
            }
        }
        if self.phase == Phase::Terminal {
            return Ok(());
        }
        let Some(channel) = self.channel.as_mut() else {
            return Ok(());
        };
        channel.flush()?;
        for _ in 0..32 {
            let Some(message) = self
                .channel
                .as_mut()
                .context("missing host channel")?
                .next()?
            else {
                break;
            };
            self.message(owner, message)?;
            if self.phase == Phase::Terminal {
                break;
            }
        }
        Ok(())
    }
}
