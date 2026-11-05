//! Actual reports and bind/commit/run barriers.

use super::super::{
    protocol::{
        HostMessage, OwnerMessage, Prepare,
        step::{
            BoundSteps, MAX_VALUE_BYTES, StepAssignment, StepHostMessage, StepOwnerMessage,
            StepPreparedReport,
        },
    },
    step_budget::SnapshotBudget,
    step_wire::{import_schema, import_type, invalid},
};
use super::{Channel, ProcessStepPeer, admission::authenticate_peer};
use std::collections::{BTreeMap, BTreeSet};
use tokio::time::Instant;

#[cfg(test)]
mod tests;

#[derive(Default)]
struct CollectedReports {
    steps: BTreeMap<String, StepPreparedReport>,
    ordinary: BTreeMap<String, crate::bindings::ModuleReport>,
    budget: SnapshotBudget,
}

async fn collect_reports(
    channel: &mut Channel,
    steps: &BTreeMap<String, StepAssignment>,
    bindings: &BTreeMap<String, crate::bindings::ModuleBindings>,
    reports: &mut CollectedReports,
    deadline: Instant,
) -> crate::Result<()> {
    let mut ordinary_actual = BTreeSet::new();
    let mut actual = BTreeSet::new();
    loop {
        match channel.receive(deadline).await? {
            HostMessage::Report(report) => {
                let expected = bindings.get(&report.namespace).ok_or_else(invalid)?;
                crate::bindings::validate_report(expected, &report)?;
                ordinary_actual.insert(report.namespace.clone());
                anyhow::ensure!(
                    reports
                        .ordinary
                        .insert(report.namespace.clone(), report)
                        .is_none(),
                    "duplicate ordinary report"
                );
            }
            HostMessage::Steps(message) => match *message {
                StepHostMessage::Report(report) => {
                    let expected = steps.get(&report.instance).ok_or_else(invalid)?;
                    anyhow::ensure!(
                        report.schema == expected.schema
                            && report.contract == expected.contract
                            && report.outputs.keys().cloned().collect::<BTreeSet<_>>()
                                == expected.seed_outputs,
                        "prepared contract mismatch"
                    );
                    let schema = import_schema(report.schema.clone())?;
                    for (port, value) in &report.outputs {
                        let ty = import_type(value.ty.clone())?;
                        anyhow::ensure!(
                            value.bytes.len() <= MAX_VALUE_BYTES
                                && schema.outputs.get(port) == Some(&ty),
                            "seed schema mismatch"
                        );
                        reports.budget.charge(value.bytes.len())?;
                    }
                    actual.insert(report.instance.clone());
                    anyhow::ensure!(
                        reports
                            .steps
                            .insert(report.instance.clone(), report)
                            .is_none(),
                        "duplicate step report"
                    );
                }
                _ => anyhow::bail!("unexpected preparation event"),
            },
            HostMessage::PreparedComplete {} => break,
            _ => anyhow::bail!("host failed during preparation"),
        }
    }
    anyhow::ensure!(
        actual == steps.keys().cloned().collect(),
        "missing prepared step report"
    );
    anyhow::ensure!(
        ordinary_actual == bindings.keys().cloned().collect(),
        "missing host ordinary report"
    );
    Ok(())
}

pub(super) async fn activate_hosts(
    channels: &mut [Channel],
    bound: &BoundSteps,
    deadline: Instant,
) -> crate::Result<()> {
    for channel in channels.iter_mut() {
        channel
            .send(
                OwnerMessage::Steps(Box::new(StepOwnerMessage::Bind(bound.clone()))),
                deadline,
            )
            .await?;
        match channel.receive(deadline).await? {
            HostMessage::Steps(message) if *message == StepHostMessage::Bound(bound.clone()) => {}
            _ => anyhow::bail!("binding rejected"),
        }
    }
    for channel in channels.iter_mut() {
        channel.send(OwnerMessage::Commit {}, deadline).await?;
        anyhow::ensure!(
            matches!(channel.receive(deadline).await?, HostMessage::Committed {}),
            "commit rejected"
        );
    }
    for channel in channels.iter_mut() {
        channel.send(OwnerMessage::Run {}, deadline).await?;
        anyhow::ensure!(
            matches!(channel.receive(deadline).await?, HostMessage::Running {}),
            "run rejected"
        );
    }
    Ok(())
}

pub(super) struct AuthenticatedPeer {
    pub(super) channel: Channel,
    pub(super) steps: BTreeMap<String, StepAssignment>,
    pub(super) bindings: BTreeMap<String, crate::bindings::ModuleBindings>,
    pub(super) prepare: Prepare,
}
pub(super) struct PreparedHosts {
    pub(super) channels: Vec<Channel>,
    pub(super) placement: BTreeMap<String, usize>,
    pub(super) reports: BTreeMap<String, StepPreparedReport>,
}

pub(super) async fn prepare_hosts(
    peers: Vec<ProcessStepPeer>,
    deadline: Instant,
    budget: SnapshotBudget,
) -> crate::Result<PreparedHosts> {
    let mut authenticated = Vec::new();
    for peer in peers {
        authenticated.push(authenticate_peer(peer, deadline).await?);
    }
    let mut channels = Vec::new();
    let mut placement = BTreeMap::new();
    let mut reports = CollectedReports {
        budget,
        ..CollectedReports::default()
    };
    let mut bindings = BTreeMap::new();
    for (index, mut peer) in authenticated.into_iter().enumerate() {
        peer.channel
            .send(
                OwnerMessage::Steps(Box::new(StepOwnerMessage::Prepare {
                    ordinary: peer.prepare,
                    steps: peer.steps.clone(),
                })),
                deadline,
            )
            .await?;
        collect_reports(
            &mut peer.channel,
            &peer.steps,
            &peer.bindings,
            &mut reports,
            deadline,
        )
        .await?;
        for id in peer.steps.keys() {
            placement.insert(id.clone(), index);
        }
        bindings.extend(peer.bindings);
        channels.push(peer.channel);
    }
    let entries: Vec<_> = bindings
        .iter()
        .map(|(id, bindings)| crate::bindings::ReportEntry {
            bindings,
            report: &reports.ordinary[id],
        })
        .collect();
    crate::bindings::validate_graph(&entries)?;
    Ok(PreparedHosts {
        channels,
        placement,
        reports: reports.steps,
    })
}
