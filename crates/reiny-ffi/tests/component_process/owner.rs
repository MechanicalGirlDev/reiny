//! Authenticated framed owner used by the real C process scenarios.

use super::{BUDGET, protocol};
use reiny::bindings::{ModuleBindings, ReportEntry, validate_graph};
use reiny::component::process::{decode_payload, encode_payload};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub(super) struct Owner {
    pub(super) stream: TcpStream,
    pub(super) identity: protocol::SessionIdentity,
    pub(super) sequence: u64,
    pub(super) peer: Option<protocol::Sequence>,
}

impl Owner {
    pub(super) async fn send(&mut self, body: protocol::OwnerMessage) -> anyhow::Result<()> {
        let payload = encode_payload(&protocol::Envelope {
            version: protocol::ProtocolVersion,
            session: self.identity.clone(),
            sequence: protocol::Sequence(self.sequence),
            body,
        })?;
        self.sequence += 1;
        tokio::time::timeout(BUDGET, async {
            self.stream
                .write_all(&protocol::frame_header(&payload)?)
                .await?;
            self.stream.write_all(&payload).await?;
            anyhow::Ok(())
        })
        .await??;
        Ok(())
    }

    pub(super) async fn receive(&mut self) -> anyhow::Result<protocol::HostMessage> {
        tokio::time::timeout(BUDGET, async {
            let mut header = [0; 4];
            self.stream.read_exact(&mut header).await?;
            let mut payload = vec![0; protocol::payload_length(header)?];
            self.stream.read_exact(&mut payload).await?;
            let envelope: protocol::Envelope<protocol::HostMessage> = decode_payload(&payload)?;
            envelope.check(&self.identity, self.peer)?;
            self.peer = Some(envelope.sequence);
            anyhow::Ok(envelope.body)
        })
        .await?
    }

    pub(super) async fn prepare(&mut self, nonce: &protocol::Nonce) -> anyhow::Result<()> {
        let protocol::HostMessage::Authenticate(proof) = self.receive().await? else {
            anyhow::bail!("missing host proof");
        };
        let registration = protocol::FactoryRegistration {
            factory: "foreign".into(),
            revision: "1".into(),
        };
        assert_eq!(&proof.nonce, nonce);
        assert_eq!(proof.compatibility, protocol::Compatibility::current(3));
        assert_eq!(proof.registrations, vec![registration.clone()]);
        self.send(protocol::OwnerMessage::Authenticated {
            compatibility: protocol::Compatibility::current(3),
            required_factories: vec![registration.clone()],
        })
        .await?;
        let bindings: ModuleBindings = serde_json::from_str(
            r#"{"version":3,"namespace":"foreign","endpoint_namespace":"foreign","inputs":{},"outputs":{}}"#,
        )?;
        self.send(protocol::OwnerMessage::Prepare(protocol::Prepare {
            assignments: vec![protocol::ComponentAssignment {
                implementation: registration,
                bindings: bindings.clone(),
                config_origin: None,
            }],
            transport: protocol::TransportSettings::Zenoh {
                domain: "foreign-process".into(),
                connect: vec![],
                listen: vec![],
                multicast_scouting: false,
            },
            remaining_budget: protocol::Milliseconds(5000),
        }))
        .await?;
        let message = self.receive().await?;
        let protocol::HostMessage::Report(report) = message else {
            anyhow::bail!(
                "missing compiled component report: {message:?}; LD_LIBRARY_PATH={:?}",
                std::env::var_os("LD_LIBRARY_PATH")
            );
        };
        validate_graph(&[ReportEntry {
            bindings: &bindings,
            report: &report,
        }])?;
        assert_eq!(
            self.receive().await?,
            protocol::HostMessage::PreparedComplete {}
        );
        Ok(())
    }
}
