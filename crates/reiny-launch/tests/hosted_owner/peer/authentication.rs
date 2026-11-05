use super::{
    Authenticate, Compatibility, Context, Envelope, FactoryRegistration, Fault, HostMessage, Nonce,
    Peer, ProtocolVersion, Result, Sequence, Write,
};

pub(super) fn authenticate(peer: &mut Peer, nonce: Nonce, fault: Fault) -> Result<()> {
    let mut proof = Authenticate {
        nonce,
        compatibility: Compatibility::current(3),
        registrations: vec![FactoryRegistration {
            factory: "peer".into(),
            revision: "1".into(),
        }],
    };
    let insertion = match fault {
        Fault::Nonce => {
            let mut corrupt = *proof.nonce.as_bytes();
            corrupt[0] ^= 1;
            proof.nonce = Nonce::new(corrupt);
            None
        }
        Fault::Digest => {
            "wrong".clone_into(&mut peer.identity.graph_digest.0);
            None
        }
        Fault::Generation => {
            peer.identity.graph_generation.0 = peer
                .identity
                .graph_generation
                .0
                .checked_add(1)
                .context("generation overflow")?;
            None
        }
        Fault::Sequence => {
            peer.next = 1;
            None
        }
        Fault::Compatibility => {
            proof.compatibility.fingerprint_version = proof
                .compatibility
                .fingerprint_version
                .checked_add(1)
                .context("version overflow")?;
            None
        }
        Fault::DuplicateJson => Some("\"version\":1"),
        Fault::UnknownField => Some("\"unknown\":true"),
        Fault::FirstStatus => {
            peer.send(HostMessage::Status(reiny_core::host_protocol::HostStatus {
                phase: reiny_core::host_protocol::HostPhase::Authenticating,
                instances: std::collections::BTreeMap::from([(
                    "wire/left".into(),
                    reiny_core::host_protocol::InstanceStatus {
                        phase: reiny_core::host_protocol::HostPhase::Authenticating,
                        report: None,
                        first_failure: None,
                    },
                )]),
            }))?;
            None
        }
        Fault::Oversize => {
            peer.socket.write_all(
                &reiny_core::host_protocol::MAX_FRAME_BYTES
                    .checked_add(1)
                    .context("frame limit overflow")?
                    .to_be_bytes(),
            )?;
            return Ok(());
        }
        Fault::ReportIdentity
        | Fault::GraphSchema
        | Fault::DuplicateReport
        | Fault::PrepareEof
        | Fault::Commit
        | Fault::UnrequestedCancel
        | Fault::BlockPrepare
        | Fault::Healthy
        | Fault::NestedDuplicateJson => None,
    };
    if let Some(insertion) = insertion {
        let envelope = Envelope {
            version: ProtocolVersion,
            session: peer.identity.clone(),
            sequence: Sequence(0),
            body: HostMessage::Authenticate(proof),
        };
        let json = serde_json::to_string(&envelope)?;
        let bytes = format!("{{{insertion},{}", json.get(1..).context("JSON object")?);
        peer.socket
            .write_all(&reiny_core::host_protocol::frame_header(bytes.as_bytes())?)?;
        peer.socket.write_all(bytes.as_bytes())?;
        return Ok(());
    }
    peer.send(HostMessage::Authenticate(proof))
}
