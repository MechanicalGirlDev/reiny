//! Exact assignment and factory authentication before construction.

use super::super::{
    protocol::{
        Compatibility, HostMessage, OwnerMessage,
        step::{StepHostMessage, StepOwnerMessage, StepProtocol, StepSessionKey},
    },
    step_wire::{export_schema, import_schema},
};
use super::{Channel, ProcessStepPeer, preparation::AuthenticatedPeer};
use std::collections::{BTreeMap, BTreeSet};
use tokio::time::Instant;

pub(super) fn validate_assignments(
    peers: &[ProcessStepPeer],
    plan: &super::super::ProcessStepPlan,
    key: &StepSessionKey,
) -> crate::Result<()> {
    plan.validate()?;
    let mut assigned = BTreeMap::new();
    let mut all_instances = BTreeSet::new();
    let mut hosts = BTreeSet::new();
    for peer in peers {
        anyhow::ensure!(
            peer.identity.owner_generation == key.owner_generation
                && peer.identity.graph_generation == key.graph_generation
                && peer.identity.graph_digest == key.graph_digest,
            "session mismatch"
        );
        anyhow::ensure!(hosts.insert(&peer.identity.host_id.0), "duplicate host");
        let mut host_instances = BTreeSet::new();
        for instance in &peer.prepare.assignments {
            anyhow::ensure!(
                all_instances.insert(&instance.bindings.namespace),
                "duplicate instance"
            );
            host_instances.insert(&instance.bindings.namespace);
        }
        for (id, assignment) in &peer.steps {
            anyhow::ensure!(
                host_instances.contains(id) && assigned.insert(id, assignment).is_none(),
                "step assignment mismatch"
            );
            anyhow::ensure!(
                plan.nodes
                    .get(id)
                    .is_some_and(|schema| export_schema(schema) == assignment.schema),
                "assigned schema mismatch"
            );
            import_schema(assignment.schema.clone())?;
        }
    }
    anyhow::ensure!(assigned.len() == plan.nodes.len(), "incomplete placement");
    for source in plan.inputs.values() {
        if let super::super::ProcessStepSource::PreviousTick {
            initial: super::super::ProcessStepSeed::PreparedOutput(output),
            ..
        } = source
        {
            anyhow::ensure!(
                assigned
                    .get(&output.instance)
                    .is_some_and(|a| a.seed_outputs.contains(&output.port)),
                "unauthorized prepared seed"
            );
        }
    }
    Ok(())
}

pub(super) async fn authenticate_peer(
    peer: ProcessStepPeer,
    deadline: Instant,
) -> crate::Result<AuthenticatedPeer> {
    let host_bindings = peer
        .prepare
        .assignments
        .iter()
        .map(|a| (a.bindings.namespace.clone(), a.bindings.clone()))
        .collect();
    let required: Vec<_> = peer
        .prepare
        .assignments
        .iter()
        .map(|a| a.implementation.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut channel = Channel {
        stream: peer.stream,
        identity: peer.identity,
        sent: 0,
        received: None,
        healthy: true,
    };
    let (proof, schemas) = match channel.receive(deadline).await? {
        HostMessage::Steps(message) => match *message {
            StepHostMessage::Authenticate {
                ordinary, schemas, ..
            } => (ordinary, schemas),
            _ => anyhow::bail!("missing step capability"),
        },
        _ => anyhow::bail!("peer lacks negotiated step capability"),
    };
    anyhow::ensure!(
        proof.nonce == peer.nonce && proof.compatibility == Compatibility::current(3),
        "authentication rejected"
    );
    anyhow::ensure!(
        required.iter().all(|r| proof.registrations.contains(r)),
        "factory revision mismatch"
    );
    let expected: BTreeMap<_, _> = peer
        .prepare
        .assignments
        .iter()
        .filter_map(|a| {
            schemas
                .get(&a.implementation.factory)
                .map(|s| (a.bindings.namespace.clone(), s))
        })
        .collect();
    anyhow::ensure!(
        expected.len() == peer.steps.len()
            && peer
                .steps
                .iter()
                .all(|(id, a)| expected.get(id) == Some(&&a.schema)),
        "step kind/schema mismatch"
    );
    channel
        .send(
            OwnerMessage::Steps(Box::new(StepOwnerMessage::Authenticated {
                version: StepProtocol,
                compatibility: Compatibility::current(3),
                required_factories: required,
            })),
            deadline,
        )
        .await?;
    Ok(AuthenticatedPeer {
        channel,
        steps: peer.steps,
        bindings: host_bindings,
        prepare: peer.prepare,
    })
}
