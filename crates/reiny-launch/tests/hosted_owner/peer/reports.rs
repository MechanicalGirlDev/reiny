use super::{
    Context, EndpointAddress, Envelope, Fault, HostMessage, MODULE_REPORT_VERSION, ModuleReport,
    Peer, PortEndpoint, PortReport, ProtocolVersion, Result, Sequence, Write,
};
use reiny_core::host_protocol::ComponentAssignment;

pub(super) fn send(
    peer: &mut Peer,
    assignments: Vec<ComponentAssignment>,
    fault: Fault,
) -> Result<()> {
    for assignment in assignments {
        let report = ModuleReport {
            version: MODULE_REPORT_VERSION,
            fingerprint_version: reiny_core::SCHEMA_FINGERPRINT_VERSION,
            namespace: match fault {
                Fault::ReportIdentity => "foreign/instance".to_owned(),
                Fault::Nonce
                | Fault::Digest
                | Fault::Generation
                | Fault::Sequence
                | Fault::Compatibility
                | Fault::DuplicateJson
                | Fault::NestedDuplicateJson
                | Fault::Oversize
                | Fault::UnknownField
                | Fault::FirstStatus
                | Fault::GraphSchema
                | Fault::DuplicateReport
                | Fault::PrepareEof
                | Fault::Commit
                | Fault::UnrequestedCancel
                | Fault::BlockPrepare
                | Fault::Healthy => assignment.bindings.namespace,
            },
            endpoint_namespace: assignment.bindings.endpoint_namespace.clone(),
            inputs: assignment
                .bindings
                .inputs
                .iter()
                .map(|(name, binding)| {
                    (
                        name.clone(),
                        PortReport {
                            endpoint: PortEndpoint::Input {
                                sources: binding.sources.clone(),
                            },
                            type_name: binding.contract.type_name.clone(),
                            schema: 43,
                            contract: binding.contract.clone(),
                            response: None,
                        },
                    )
                })
                .collect(),
            outputs: assignment
                .bindings
                .outputs
                .iter()
                .map(|(name, binding)| {
                    (
                        name.clone(),
                        PortReport {
                            endpoint: PortEndpoint::Output {
                                address: EndpointAddress {
                                    namespace: assignment.bindings.endpoint_namespace.clone(),
                                    port: name.clone(),
                                },
                            },
                            type_name: binding.contract.type_name.clone(),
                            schema: 42,
                            contract: binding.contract.clone(),
                            response: None,
                        },
                    )
                })
                .collect(),
        };
        if matches!(fault, Fault::NestedDuplicateJson) {
            let duplicated =
                serde_json::to_string(report.outputs.get("value").context("fixture output")?)?;
            let envelope = Envelope {
                version: ProtocolVersion,
                session: peer.identity.clone(),
                sequence: Sequence(peer.next),
                body: HostMessage::Report(report.clone()),
            };
            let json = serde_json::to_string(&envelope)?.replacen(
                "\"outputs\":{",
                &format!("\"outputs\":{{\"value\":{duplicated},"),
                1,
            );
            peer.socket
                .write_all(&reiny_core::host_protocol::frame_header(json.as_bytes())?)?;
            peer.socket.write_all(json.as_bytes())?;
            peer.next = peer.next.checked_add(1).context("peer sequence overflow")?;
        } else {
            peer.send(HostMessage::Report(report.clone()))?;
        }
        if matches!(fault, Fault::DuplicateReport) {
            peer.send(HostMessage::Report(report))?;
        }
    }
    Ok(())
}
