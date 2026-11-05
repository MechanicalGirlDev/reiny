//! Explicit ordinary/step assignments for the loopback host.

use super::*;

pub(super) fn make_peer(
    stream: TcpStream,
    identity: &SessionIdentity,
    nonce: Nonce,
    schema: &StepSchema,
    registration: FactoryRegistration,
) -> ProcessStepPeer {
    ProcessStepPeer {
        stream,
        identity: identity.clone(),
        nonce,
        prepare: Prepare {
            assignments: vec![ComponentAssignment {
                implementation: registration,
                bindings: ModuleBindings {
                    version: MODULE_BINDINGS_VERSION,
                    namespace: "node".into(),
                    endpoint_namespace: "node".into(),
                    inputs: BTreeMap::new(),
                    outputs: BTreeMap::new(),
                    children: BTreeMap::new(),
                    executables: Vec::new(),
                },
                config_origin: None,
            }],
            transport: TransportSettings::Local {
                domain: "step-fixture".into(),
                placement: BTreeMap::from([("node".into(), "fixture".into())]),
            },
            remaining_budget: Milliseconds(5000),
        },
        steps: BTreeMap::from([(
            "node".into(),
            StepAssignment {
                schema: super::super::super::step_wire::export_schema(schema),
                contract: Vec::new(),
                seed_outputs: BTreeSet::from(["out".into()]),
            },
        )]),
    }
}

pub(super) fn identity() -> SessionIdentity {
    SessionIdentity {
        owner_generation: OwnerGeneration(1),
        graph_generation: GraphGeneration(2),
        host_id: HostId("fixture".into()),
        host_generation: HostGeneration(3),
        graph_digest: PreparedDigest("graph".into()),
        bundle_digest: PreparedDigest("bundle".into()),
    }
}
