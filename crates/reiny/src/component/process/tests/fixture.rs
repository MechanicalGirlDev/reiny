use std::collections::BTreeMap;
use std::net::Shutdown;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::timeout;

use super::super::protocol::*;
use super::super::{Bootstrap, ProcessError, ProcessHost, ProcessLimits, ProcessRegistry, codec};
pub(super) use super::component::Event;
use super::component::{Behavior, Factory};
use crate::bindings::{MODULE_BINDINGS_VERSION, ModuleBindings};

pub(super) const BUDGET: Duration = Duration::from_secs(5);

pub(super) fn registration() -> FactoryRegistration {
    FactoryRegistration {
        factory: "service".into(),
        revision: "1".into(),
    }
}

pub(super) fn bindings() -> ModuleBindings {
    ModuleBindings {
        version: MODULE_BINDINGS_VERSION,
        namespace: "test/service".into(),
        endpoint_namespace: "test/service".into(),
        inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        children: BTreeMap::new(),
        executables: Vec::new(),
    }
}

pub(super) struct Fixture {
    pub stream: TcpStream,
    pub identity: SessionIdentity,
    pub nonce: Nonce,
    pub sequence: u64,
    pub events: mpsc::Receiver<Event>,
    pub constructed: Arc<std::sync::atomic::AtomicUsize>,
    peer_sequence: Option<Sequence>,
    close: std::net::TcpStream,
    host: Option<std::thread::JoinHandle<Result<HostOutcome, ProcessError>>>,
}

impl Fixture {
    pub(super) async fn start(behavior: Behavior) -> crate::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let identity = SessionIdentity {
            owner_generation: OwnerGeneration(1),
            graph_generation: GraphGeneration(2),
            host_id: HostId("fixture".into()),
            host_generation: HostGeneration(3),
            graph_digest: PreparedDigest("graph".into()),
            bundle_digest: PreparedDigest("bundle".into()),
        };
        // Deterministic fixture secret; real owners must generate OS entropy.
        let nonce = Nonce::new([42; 32]);
        let bootstrap = Bootstrap {
            address: listener.local_addr()?,
            identity: identity.clone(),
            nonce: nonce.clone(),
        };
        let (events, receiver) = mpsc::channel(16);
        let mut registry = ProcessRegistry::new();
        let constructed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        registry.register(
            registration(),
            Arc::new(Factory {
                events,
                behavior,
                constructed: Arc::clone(&constructed),
            }),
        )?;
        let host = std::thread::spawn(move || {
            ProcessHost::run(
                bootstrap,
                registry,
                ProcessLimits {
                    startup: BUDGET,
                    shutdown: BUDGET,
                    frame: BUDGET,
                    instances: 8,
                },
            )
        });
        let (stream, _) = timeout(BUDGET, listener.accept()).await??;
        let stream = stream.into_std()?;
        let close = stream.try_clone()?;
        Ok(Self {
            stream: TcpStream::from_std(stream)?,
            identity,
            nonce,
            sequence: 0,
            events: receiver,
            constructed,
            peer_sequence: None,
            close,
            host: Some(host),
        })
    }

    pub(super) async fn receive(&mut self) -> crate::Result<HostMessage> {
        timeout(BUDGET, async {
            let mut header = [0; 4];
            self.stream.read_exact(&mut header).await?;
            let mut payload = vec![0; payload_length(header)?];
            self.stream.read_exact(&mut payload).await?;
            let envelope: Envelope<HostMessage> = codec::decode(&payload)?;
            envelope.check(&self.identity, self.peer_sequence)?;
            self.peer_sequence = Some(envelope.sequence);
            Ok(envelope.body)
        })
        .await?
    }

    pub(super) async fn send(&mut self, body: OwnerMessage) -> crate::Result<()> {
        let payload = codec::encode(&Envelope {
            version: ProtocolVersion,
            session: self.identity.clone(),
            sequence: Sequence(self.sequence),
            body,
        })?;
        self.sequence += 1;
        timeout(BUDGET, async {
            self.stream.write_all(&frame_header(&payload)?).await?;
            self.stream.write_all(&payload).await?;
            crate::Result::Ok(())
        })
        .await?
    }

    pub(super) async fn authenticate(&mut self) -> crate::Result<()> {
        let message = self.receive().await?;
        let HostMessage::Authenticate(proof) = message else {
            anyhow::bail!("expected authenticate");
        };
        anyhow::ensure!(proof.nonce == self.nonce, "nonce mismatch");
        anyhow::ensure!(
            proof.compatibility == Compatibility::current(3),
            "compatibility"
        );
        anyhow::ensure!(proof.registrations == vec![registration()], "registrations");
        self.send(OwnerMessage::Authenticated {
            compatibility: Compatibility::current(3),
            required_factories: vec![registration()],
        })
        .await
    }

    pub(super) async fn prepare(
        &mut self,
        origins: Vec<Option<ConfigOrigin>>,
    ) -> crate::Result<()> {
        let assignments: Vec<_> = origins
            .into_iter()
            .enumerate()
            .map(|(index, config_origin)| {
                let mut bindings = bindings();
                if index != 0 {
                    bindings.namespace = format!("test/service{index}");
                    bindings.endpoint_namespace.clone_from(&bindings.namespace);
                }
                ComponentAssignment {
                    implementation: registration(),
                    bindings,
                    config_origin,
                }
            })
            .collect();
        #[cfg(feature = "zenoh")]
        let transport = TransportSettings::Zenoh {
            domain: "process-tests".into(),
            connect: Vec::new(),
            listen: Vec::new(),
            multicast_scouting: false,
        };
        #[cfg(not(feature = "zenoh"))]
        let transport = TransportSettings::Local {
            domain: "process-tests".into(),
            placement: assignments
                .iter()
                .map(|assignment| {
                    (
                        assignment.bindings.namespace.clone(),
                        self.identity.host_id.0.clone(),
                    )
                })
                .collect(),
        };
        self.send(OwnerMessage::Prepare(Prepare {
            assignments,
            transport,
            remaining_budget: Milliseconds(5000),
        }))
        .await
    }

    pub(super) async fn outcome(&mut self) -> crate::Result<HostOutcome> {
        loop {
            match self.receive().await? {
                HostMessage::Failure(_) => {}
                HostMessage::Outcome(outcome) => return Ok(outcome),
                message => anyhow::bail!("expected terminal event, got {message:?}"),
            }
        }
    }

    pub(super) fn join(&mut self) -> crate::Result<HostOutcome> {
        let handle = self
            .host
            .take()
            .ok_or_else(|| anyhow::anyhow!("already joined"))?;
        handle
            .join()
            .map_err(|_| anyhow::anyhow!("host thread panicked"))?
            .map_err(anyhow::Error::new)
    }

    pub(super) fn disconnect(&self) -> crate::Result<()> {
        self.close.shutdown(Shutdown::Both)?;
        Ok(())
    }

    pub(super) async fn event(&mut self) -> crate::Result<Event> {
        timeout(BUDGET, self.events.recv())
            .await?
            .ok_or_else(|| anyhow::anyhow!("missing lifecycle event"))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Test failures also close the exact socket and join the owned host.
        match self.close.shutdown(Shutdown::Both) {
            Ok(()) | Err(_) => {}
        }
        if let Some(host) = self.host.take() {
            assert!(host.join().is_ok(), "host thread panicked during teardown");
        }
    }
}
