use std::collections::BTreeMap;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use reiny_launch::{DeploymentClient, DeploymentPhase, DeploymentStatus, ModuleObserver};

use super::{Event, EventKind, Fault, Fixture};

pub(super) const DEADLINE: Duration = Duration::from_secs(30);

struct Observer;

impl ModuleObserver for Observer {
    fn ready(&self, _: &str) -> Result<bool> {
        anyhow::bail!("hosted graph reached legacy observer")
    }
    fn request_stop(&self, _: &str) -> Result<()> {
        anyhow::bail!("hosted graph reached legacy stop")
    }
}

pub(super) struct Harness {
    // Stop and join the owner before deleting the files it still publishes.
    lifetime: Lifetime,
    _root: tempfile::TempDir,
    pub(super) client: DeploymentClient,
    sockets: Arc<Mutex<BTreeMap<String, TcpStream>>>,
    incoming: mpsc::Receiver<Event>,
    received: Vec<Event>,
}

struct Lifetime {
    owner: Option<JoinHandle<Result<()>>>,
    owner_stop: Arc<AtomicBool>,
    listener: Option<JoinHandle<Result<()>>>,
    listener_stop: Arc<AtomicBool>,
    address: std::net::SocketAddr,
}

impl Harness {
    pub(super) fn start(fault: Fault, suspend: bool) -> Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let address = listener.local_addr()?;
        let (root, prepared) = fixture::build(fault, suspend, address)?;
        let sockets = Arc::new(Mutex::new(BTreeMap::new()));
        let shared_sockets = Arc::clone(&sockets);
        let listener_stop = Arc::new(AtomicBool::new(false));
        let signal = Arc::clone(&listener_stop);
        let (sender, incoming) = mpsc::sync_channel(64);
        let listener = std::thread::spawn(move || {
            collector::Collector {
                listener,
                stop: signal,
                sockets: shared_sockets,
                sender,
            }
            .run()
        });
        let owner_stop = Arc::new(AtomicBool::new(false));
        let stop = Arc::clone(&owner_stop);
        let (started, ready) = mpsc::sync_channel(1);
        let owner = std::thread::spawn(move || {
            reiny_launch::serve(prepared, &Observer, &stop, &|| {
                started.send(()).context("owner rendezvous closed")
            })
        });
        let mut lifetime = Lifetime {
            owner: Some(owner),
            owner_stop,
            listener: Some(listener),
            listener_stop,
            address,
        };
        if let Err(error) = ready.recv_timeout(DEADLINE) {
            lifetime.owner_stop.store(true, Ordering::Release);
            let owner_result = lifetime
                .owner
                .take()
                .context("owner thread")?
                .join()
                .map_err(|_| anyhow::anyhow!("owner panicked before listening"))?;
            return owner_result
                .and(Err(error.into()))
                .context("owner bootstrap failed");
        }
        let client = DeploymentClient::find(root.path())?.context("owner not listening")?;
        Ok(Self {
            _root: root,
            client,
            lifetime,
            sockets,
            incoming,
            received: Vec::new(),
        })
    }

    pub(super) fn phase(&self, phase: DeploymentPhase) -> Result<DeploymentStatus> {
        let deadline = Instant::now() + DEADLINE;
        let mut status = self.client.status()?;
        while status.phase != phase {
            status = self.client.wait_for_revision(
                status.revision,
                deadline
                    .checked_duration_since(Instant::now())
                    .context("phase deadline")?,
            )?;
        }
        Ok(status)
    }

    pub(super) fn event(&mut self, host: &str, kind: EventKind) -> Result<()> {
        let deadline = Instant::now() + DEADLINE;
        loop {
            let event = self
                .incoming
                .recv_timeout(
                    deadline
                        .checked_duration_since(Instant::now())
                        .context("event deadline")?,
                )
                .context("peer event stream closed or timed out")?;
            let matched = event.host == host && event.kind == kind;
            self.received.push(event);
            if matched {
                return Ok(());
            }
        }
    }

    pub(super) fn send(&self, host: &str, command: &str) -> Result<()> {
        self.sockets
            .lock()
            .map_err(|_| anyhow::anyhow!("fixture sockets poisoned"))?
            .get_mut(host)
            .context("peer not connected")?
            .write_all(command.as_bytes())?;
        Ok(())
    }

    pub(super) fn stop(&mut self) -> Result<DeploymentStatus> {
        let status = self.client.stop()?;
        if let Some(owner) = self.lifetime.owner.take() {
            owner
                .join()
                .map_err(|_| anyhow::anyhow!("owner panicked"))??;
        }
        self.lifetime.finish_listener()?;
        Ok(status)
    }

    pub(super) fn events(&mut self) -> impl Iterator<Item = &Event> {
        self.received.extend(self.incoming.try_iter());
        self.received.iter()
    }
}

impl Lifetime {
    fn finish_listener(&mut self) -> Result<()> {
        self.listener_stop.store(true, Ordering::Release);
        if let Some(listener) = self.listener.take() {
            // Wake an accept that never got its expected peer after startup failure.
            match TcpStream::connect(self.address) {
                Ok(stream) => drop(stream),
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {}
                Err(error) => return Err(error.into()),
            }
            listener
                .join()
                .map_err(|_| anyhow::anyhow!("event listener panicked"))??;
        }
        Ok(())
    }
}

impl Drop for Lifetime {
    fn drop(&mut self) {
        self.owner_stop.store(true, Ordering::Release);
        if let Some(owner) = self.owner.take() {
            match owner.join() {
                Ok(Ok(())) => {}
                result => eprintln!("owner teardown: {result:?}"),
            }
        }
        if let Err(error) = self.finish_listener() {
            eprintln!("listener teardown: {error:#}");
        }
    }
}

#[path = "support/collector.rs"]
mod collector;
#[path = "support/fixture.rs"]
mod fixture;
