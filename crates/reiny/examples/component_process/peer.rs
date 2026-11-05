use std::collections::BTreeMap;

use reiny::bindings::{MODULE_BINDINGS_VERSION, ModuleBindings, ReportEntry, validate_graph};
use reiny::component::process::protocol::{
    Compatibility, ComponentAssignment, Envelope, FactoryRegistration, HostMessage, HostOutcome,
    Milliseconds, Nonce, OwnerMessage, Prepare, ProtocolVersion, Sequence, SessionIdentity,
    StopReason, TransportSettings, frame_header, payload_length,
};
use reiny::component::process::{decode_payload, encode_payload};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::process::ChildStdout;
use tokio::time::Instant;

pub(super) struct Peer {
    stream: TcpStream,
    identity: SessionIdentity,
    deadline: Instant,
    sent: u64,
    received: Option<Sequence>,
}

impl Peer {
    pub(super) fn new(stream: TcpStream, identity: SessionIdentity, deadline: Instant) -> Self {
        Self {
            stream,
            identity,
            deadline,
            sent: 0,
            received: None,
        }
    }

    pub(super) async fn authenticate(
        &mut self,
        nonce: &Nonce,
    ) -> reiny::Result<FactoryRegistration> {
        let HostMessage::Authenticate(proof) = self.receive().await? else {
            anyhow::bail!("expected host authentication");
        };
        anyhow::ensure!(proof.nonce == *nonce, "authentication rejected");
        anyhow::ensure!(
            proof.compatibility == Compatibility::current(3),
            "compatibility rejected"
        );
        let implementation = FactoryRegistration {
            factory: "service".into(),
            revision: "1".into(),
        };
        anyhow::ensure!(
            proof.registrations == vec![implementation.clone()],
            "factory rejected"
        );
        self.send(OwnerMessage::Authenticated {
            compatibility: Compatibility::current(3),
            required_factories: vec![implementation.clone()],
        })
        .await?;
        Ok(implementation)
    }

    pub(super) async fn admit(
        &mut self,
        implementation: FactoryRegistration,
        stdout: &mut BufReader<ChildStdout>,
    ) -> reiny::Result<()> {
        let bindings = ModuleBindings {
            version: MODULE_BINDINGS_VERSION,
            namespace: "demo/service".into(),
            endpoint_namespace: "demo/service".into(),
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            children: BTreeMap::new(),
            executables: Vec::new(),
        };
        self.send(OwnerMessage::Prepare(Prepare {
            assignments: vec![ComponentAssignment {
                implementation,
                bindings: bindings.clone(),
                config_origin: None,
            }],
            transport: TransportSettings::Zenoh {
                domain: "component-process-example".into(),
                connect: Vec::new(),
                listen: vec!["tcp/127.0.0.1:0".into()],
                multicast_scouting: false,
            },
            remaining_budget: Milliseconds(u64::try_from(
                self.deadline
                    .saturating_duration_since(Instant::now())
                    .as_millis(),
            )?),
        }))
        .await?;
        let HostMessage::Report(report) = self.receive().await? else {
            anyhow::bail!("missing compiled report");
        };
        // This fixture's entire graph is one exact instance. A deployment owner
        // must instead validate all hosts together with its own bindings.
        validate_graph(&[ReportEntry {
            bindings: &bindings,
            report: &report,
        }])?;
        anyhow::ensure!(
            self.receive().await? == HostMessage::PreparedComplete {},
            "prepare barrier"
        );
        self.send(OwnerMessage::Commit {}).await?;
        anyhow::ensure!(
            self.receive().await? == HostMessage::Committed {},
            "commit barrier"
        );
        self.send(OwnerMessage::Run {}).await?;
        anyhow::ensure!(
            self.receive().await? == HostMessage::Running {},
            "run barrier"
        );
        let mut started = String::new();
        stdout.read_line(&mut started).await?;
        anyhow::ensure!(started == "run\n", "missing run hook observation");
        self.send(OwnerMessage::Stop {
            reason: StopReason::Requested,
            remaining_budget: Milliseconds(5000),
        })
        .await?;
        anyhow::ensure!(
            self.receive().await? == HostMessage::Outcome(HostOutcome::Stopped {}),
            "stop outcome"
        );
        Ok(())
    }

    async fn send(&mut self, body: OwnerMessage) -> reiny::Result<()> {
        let bytes = encode_payload(&Envelope {
            version: ProtocolVersion,
            session: self.identity.clone(),
            sequence: Sequence(self.sent),
            body,
        })?;
        self.sent = self
            .sent
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("sequence overflow"))?;
        self.stream.write_all(&frame_header(&bytes)?).await?;
        self.stream.write_all(&bytes).await?;
        Ok(())
    }

    async fn receive(&mut self) -> reiny::Result<HostMessage> {
        let mut header = [0; 4];
        self.stream.read_exact(&mut header).await?;
        let mut payload = vec![0; payload_length(header)?];
        self.stream.read_exact(&mut payload).await?;
        let envelope: Envelope<HostMessage> = decode_payload(&payload)?;
        envelope.check(&self.identity, self.received)?;
        self.received = Some(envelope.sequence);
        Ok(envelope.body)
    }
}
