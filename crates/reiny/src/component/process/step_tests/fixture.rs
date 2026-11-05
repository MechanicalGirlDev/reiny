//! Owned process fixture and deterministic event gates.

mod component;
mod peer;
use component::Factory;
pub(super) use component::{Mode, Number};
use peer::{identity, make_peer};

use super::ProcessStepClient;
use crate::{
    bindings::{MODULE_BINDINGS_VERSION, ModuleBindings},
    component::process::{
        Bootstrap, ProcessHost, ProcessLimits, ProcessRegistry, ProcessStepOwner, ProcessStepPeer,
        ProcessStepPlan, ProcessStepSeed, ProcessStepSource,
        protocol::{
            ComponentAssignment, FactoryRegistration, GraphGeneration, HostGeneration, HostId,
            Milliseconds, Nonce, OwnerGeneration, Prepare, PreparedDigest, SessionIdentity,
            TransportSettings,
            step::{StepAssignment, StepSessionKey},
        },
    },
    step::{StepInput, StepOutput, StepSchema, StepType},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
    time::timeout,
};

pub(super) const BUDGET: Duration = Duration::from_secs(5);
pub(super) struct Fixture {
    pub client: ProcessStepClient,
    pub entered: mpsc::UnboundedReceiver<()>,
    pub release: watch::Sender<bool>,
    pub constructions: Arc<AtomicUsize>,
    owner: Option<ProcessStepOwner>,
    host: Option<std::thread::JoinHandle<crate::Result<()>>>,
    close: std::net::TcpStream,
}
impl Fixture {
    pub(super) async fn start(mode: Mode) -> crate::Result<Self> {
        Self::start_with_validator(mode, false).await
    }
    pub(super) async fn reject_validator() -> crate::Result<()> {
        match Self::start_with_validator(Mode::Sum, true).await {
            Ok(fixture) => {
                fixture.finish().await?;
                anyhow::bail!("application veto was ignored");
            }
            Err(error) => {
                assert_eq!(error.to_string(), "application admission veto");
                Ok(())
            }
        }
    }
    async fn start_with_validator(mode: Mode, reject: bool) -> crate::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let identity = identity();
        let nonce = Nonce::new([42; 32]);
        let bootstrap = Bootstrap {
            address: listener.local_addr()?,
            identity: identity.clone(),
            nonce: nonce.clone(),
        };
        let (entered, mut events) = mpsc::unbounded_channel();
        let (release, released) = watch::channel(false);
        let constructions = Arc::new(AtomicUsize::new(0));
        let schema = StepSchema {
            inputs: BTreeMap::from([("previous".into(), StepType::of::<Number>())]),
            outputs: BTreeMap::from([("out".into(), StepType::of::<Number>())]),
        };
        let registration = FactoryRegistration {
            factory: "node".into(),
            revision: "1".into(),
        };
        let mut registry = ProcessRegistry::new();
        registry.register_step_prepared(
            registration.clone(),
            schema.clone(),
            BTreeSet::from(["out".into()]),
            Arc::new(Factory {
                mode,
                entered,
                release: released,
                constructions: constructions.clone(),
            }),
        )?;
        let mut host = Some(std::thread::spawn(move || {
            ProcessHost::run(
                bootstrap,
                registry,
                ProcessLimits {
                    startup: BUDGET,
                    shutdown: BUDGET,
                    frame: BUDGET,
                    instances: 8,
                },
            )?;
            Ok(())
        }));
        let (stream, _) = timeout(BUDGET, listener.accept()).await??;
        let stream = stream.into_std()?;
        let close = stream.try_clone()?;
        let peer = make_peer(
            TcpStream::from_std(stream)?,
            &identity,
            nonce,
            &schema,
            registration,
        );
        let source = StepOutput {
            instance: "node".into(),
            port: "out".into(),
        };
        let plan = ProcessStepPlan {
            nodes: BTreeMap::from([("node".into(), schema)]),
            inputs: BTreeMap::from([(
                StepInput {
                    instance: "node".into(),
                    port: "previous".into(),
                },
                ProcessStepSource::PreviousTick {
                    source: source.clone(),
                    initial: ProcessStepSeed::PreparedOutput(source),
                },
            )]),
        };
        let key = StepSessionKey {
            incarnation: [7; 32],
            owner_generation: identity.owner_generation,
            graph_generation: identity.graph_generation,
            graph_digest: identity.graph_digest,
            plan_digest: PreparedDigest(String::new()),
        };
        let admission =
            ProcessStepOwner::prepare(vec![peer], key, plan, Duration::ZERO, BUDGET, |_, _| {
                anyhow::ensure!(!reject, "application admission veto");
                Ok(())
            })
            .await;
        let (owner, client) =
            admit_or_join(admission, &close, &mut host, &mut events, &constructions)?;
        Ok(Self {
            client,
            entered: events,
            release,
            constructions,
            owner: Some(owner),
            host,
            close,
        })
    }
    pub(super) fn disconnect(&self) -> std::io::Result<()> {
        self.close.shutdown(std::net::Shutdown::Both)
    }
    pub(super) async fn finish(mut self) -> crate::Result<()> {
        self.release.send_replace(true);
        if let Some(owner) = self.owner.take() {
            timeout(BUDGET, owner.stop()).await??;
        }
        if let Err(error) = self.close.shutdown(std::net::Shutdown::Both)
            && error.kind() != std::io::ErrorKind::NotConnected
        {
            return Err(error.into());
        }
        if let Some(host) = self.host.take() {
            host.join()
                .map_err(|_| anyhow::anyhow!("host panicked"))??;
        }
        Ok(())
    }
}

fn admit_or_join(
    admission: crate::Result<(ProcessStepOwner, ProcessStepClient)>,
    close: &std::net::TcpStream,
    host: &mut Option<std::thread::JoinHandle<crate::Result<()>>>,
    events: &mut mpsc::UnboundedReceiver<()>,
    constructions: &AtomicUsize,
) -> crate::Result<(ProcessStepOwner, ProcessStepClient)> {
    if admission.is_err() {
        if let Err(error) = close.shutdown(std::net::Shutdown::Both)
            && error.kind() != std::io::ErrorKind::NotConnected
        {
            return Err(error.into());
        }
        if let Some(host) = host.take() {
            host.join()
                .map_err(|_| anyhow::anyhow!("host panicked"))??;
        }
        assert_eq!(constructions.load(Ordering::SeqCst), 1);
        assert!(matches!(
            events.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }
    admission
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.release.send_replace(true);
        let _ = self.close.shutdown(std::net::Shutdown::Both);
        if let Some(host) = self.host.take() {
            assert!(host.join().is_ok(), "host thread panicked");
        }
    }
}
