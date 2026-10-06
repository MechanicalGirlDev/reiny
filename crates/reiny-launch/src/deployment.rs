//! A bus-independent deployment owner with authenticated local control.
//!
//! Only the owner holds process handles. State files are observations, never
//! authority to signal a PID. The caller supplies bounded bus observations.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use process_wrap::tokio::{ChildWrapper, CommandWrap};
use reiny_core::bindings::ModuleReport;
use serde::{Deserialize, Serialize};

use crate::modules::{FailurePolicy, RestartPolicy};
use crate::prepared::{PreparedDeployment, PreparedModule, digest};

const TICK: Duration = Duration::from_millis(20);
const STOP_GRACE: Duration = Duration::from_secs(2);
const START_GRACE: Duration = Duration::from_secs(30);
const IO_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_FRAME: usize = 8 * 1024 * 1024;
const MAX_CONNECTIONS: usize = 64;

/// The communication adapter implemented by the embedding CLI.
///
/// Calls must have a bounded duration. A stop acknowledgement only means that
/// the request was delivered; the owner still waits for the exact child.
pub trait ModuleObserver {
    /// Reconfigure the observer before adopting a prepared desired state.
    ///
    /// Implementations should prepare replacement sessions before swapping
    /// their namespace registry, so an error leaves the old registry usable.
    fn configure(&self, _prepared: &PreparedDeployment) -> Result<()> {
        Ok(())
    }

    /// Whether this namespace currently declares application readiness.
    fn ready(&self, namespace: &str) -> Result<bool>;
    /// Request cooperative application shutdown.
    fn request_stop(&self, namespace: &str) -> Result<()>;
}

/// Overall observed deployment state.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentPhase {
    /// Processes have been started but their contracts are not ready.
    Starting,
    /// Every desired process and connected schema is ready.
    Ready,
    /// At least one desired process is unavailable or restarting.
    Degraded,
    /// Failure policy stopped the deployment until explicit apply.
    Suspended,
    /// All owned processes have been stopped and reaped.
    Stopped,
    /// A contract or owner operation failed.
    Failed,
}

/// One desired module and its actual owned process.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ModuleStatus {
    /// Canonical deployment-qualified namespace.
    pub namespace: String,
    /// Desired executable/configuration fingerprint.
    pub desired_fingerprint: String,
    /// Fingerprint of the actual process, absent when stopped.
    pub actual_fingerprint: Option<String>,
    /// Informational PID, never authority to stop a process.
    pub pid: Option<u32>,
    /// Monotonic spawn generation within this owner.
    pub generation: u64,
    /// Observed module lifecycle state.
    pub phase: DeploymentPhase,
    /// Last observed error or termination.
    pub error: Option<String>,
    /// Last actual typed-port report from this generation.
    pub report: Option<ModuleReport>,
}

/// Persisted, credential-free desired/actual deployment status.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DeploymentStatus {
    /// Status format version.
    pub version: u32,
    /// Canonical deployment root.
    pub root: PathBuf,
    /// Deployment identity.
    pub deployment: String,
    /// Communication domain.
    pub domain: String,
    /// Unique owner incarnation; not an authentication credential.
    pub owner_generation: String,
    /// Informational owner PID.
    pub owner_pid: u32,
    /// Whether this status was verified against the active owner.
    pub owner_alive: bool,
    /// Monotonic state revision for bounded event waits.
    pub revision: u64,
    /// Overall lifecycle state.
    pub phase: DeploymentPhase,
    /// Desired modules and their actual owned processes.
    pub modules: BTreeMap<String, ModuleStatus>,
    /// Managed resource addresses and paths.
    pub resources: BTreeMap<String, PathBuf>,
    /// Last deployment-level failure.
    pub error: Option<String>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Control {
    version: u32,
    address: SocketAddr,
    token: String,
    owner_generation: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Request {
    token: String,
    owner_generation: String,
    action: Action,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Action {
    Status,
    Apply { prepared: Box<PreparedDeployment> },
    Stop,
    WaitStopped,
    WaitReady { timeout_ms: u64 },
    WaitRevision { revision: u64, timeout_ms: u64 },
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Response {
    status: Option<DeploymentStatus>,
    error: Option<String>,
}

/// An authenticated connection capability for one owner incarnation.
///
/// This type deliberately does not implement `Debug` or `Serialize`.
#[derive(Clone)]
pub struct DeploymentClient {
    control: Control,
}

impl DeploymentClient {
    /// Find and authenticate the owner of a root. Stale files return `None`.
    pub fn find(root: &Path) -> Result<Option<Self>> {
        let bytes = match std::fs::read(root.join(".reiny/control.json")) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let control: Control = serde_json::from_slice(&bytes)?;
        ensure!(
            control.version == 1 && control.address.ip().is_loopback(),
            "invalid owner control address"
        );
        let client = Self { control };
        // A newly published endpoint can still be reconciling its children.
        // Use the same bound as apply, rather than treating slow startup as stale.
        match client.request(Action::Status, Some(START_GRACE)) {
            Ok(_) => Ok(Some(client)),
            Err(error) => {
                tracing::debug!(%error, "deployment owner is unavailable");
                Ok(None)
            }
        }
    }

    /// Read live state from the authenticated owner.
    pub fn status(&self) -> Result<DeploymentStatus> {
        self.request(Action::Status, Some(IO_TIMEOUT))
    }

    /// Reconcile a prepared desired state, preserving unchanged processes.
    ///
    /// A successful response confirms reconciliation, not application readiness.
    /// Use [`Self::wait_ready`] to verify application contracts.
    pub fn apply(&self, prepared: &PreparedDeployment) -> Result<DeploymentStatus> {
        self.request(
            Action::Apply {
                prepared: Box::new(prepared.clone()),
            },
            Some(START_GRACE),
        )
    }

    /// Stop all owned process trees; returns only after they have been reaped.
    pub fn stop(&self) -> Result<DeploymentStatus> {
        self.request(Action::Stop, Some(START_GRACE))
    }

    /// Subscribe until shutdown has reaped all owned children.
    ///
    /// Owner disappearance or an autonomous owner error wakes this call with an
    /// error. There is no client polling or read timeout for this subscription.
    pub fn wait_stopped(&self) -> Result<DeploymentStatus> {
        self.watch_stopped()?.wait()
    }

    /// Register a termination subscription before triggering another action.
    ///
    /// Registration is acknowledged by the owner; the returned subscription can
    /// be moved to a waiting thread without a subscribe/stop race.
    pub fn watch_stopped(&self) -> Result<StopSubscription> {
        let mut stream = self.open(Action::WaitStopped, Some(IO_TIMEOUT))?;
        decode_response(&mut stream, &self.control.owner_generation)?;
        stream.set_read_timeout(None)?;
        Ok(StopSubscription {
            stream,
            owner_generation: self.control.owner_generation.clone(),
        })
    }

    /// Wait on the owner's state events, rather than polling status files.
    pub fn wait_ready(&self, timeout: Duration) -> Result<DeploymentStatus> {
        self.request(
            Action::WaitReady {
                timeout_ms: u64::try_from(timeout.as_millis())?,
            },
            Some(timeout.saturating_add(IO_TIMEOUT)),
        )
    }

    /// Wait until the owner publishes a revision newer than `revision`.
    pub fn wait_for_revision(&self, revision: u64, timeout: Duration) -> Result<DeploymentStatus> {
        self.request(
            Action::WaitRevision {
                revision,
                timeout_ms: u64::try_from(timeout.as_millis())?,
            },
            Some(timeout.saturating_add(IO_TIMEOUT)),
        )
    }

    fn request(&self, action: Action, timeout: Option<Duration>) -> Result<DeploymentStatus> {
        let mut stream = self.open(action, timeout)?;
        decode_response(&mut stream, &self.control.owner_generation)
    }

    fn open(&self, action: Action, timeout: Option<Duration>) -> Result<TcpStream> {
        let mut stream = TcpStream::connect_timeout(&self.control.address, IO_TIMEOUT)
            .context("deployment owner disappeared")?;
        stream.set_read_timeout(timeout)?;
        stream.set_write_timeout(Some(IO_TIMEOUT))?;
        write_frame(
            &mut stream,
            &Request {
                token: self.control.token.clone(),
                owner_generation: self.control.owner_generation.clone(),
                action,
            },
        )?;
        Ok(stream)
    }
}

/// An already-registered, single-connection termination subscription.
pub struct StopSubscription {
    stream: TcpStream,
    owner_generation: String,
}

impl StopSubscription {
    /// Wait for exact child cleanup, or an owner failure/disappearance.
    pub fn wait(mut self) -> Result<DeploymentStatus> {
        let status = decode_response(&mut self.stream, &self.owner_generation)?;
        ensure!(
            status.phase == DeploymentPhase::Stopped,
            "deployment did not stop cleanly"
        );
        Ok(status)
    }
}

fn decode_response(stream: &mut TcpStream, owner_generation: &str) -> Result<DeploymentStatus> {
    let response: Response =
        read_frame(stream).context("deployment owner disappeared or stopped responding")?;
    if let Some(error) = response.error {
        bail!("{error}");
    }
    let status = response.status.context("owner returned no status")?;
    ensure!(
        status.owner_generation == owner_generation,
        "owner incarnation changed"
    );
    Ok(status)
}

/// Read the last observation without claiming that its owner or PIDs are alive.
pub fn last_status(root: &Path) -> Result<Option<DeploymentStatus>> {
    let bytes = match std::fs::read(root.join(".reiny/state.json")) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut status: DeploymentStatus = serde_json::from_slice(&bytes)?;
    status.owner_alive = false;
    Ok(Some(status))
}

/// Run one deployment owner until stop, cancellation, or an owner error.
///
/// `on_listening` runs after the lock and authenticated endpoint are published,
/// before process startup. It may notify a parent rendezvous but must not make
/// a synchronous request back to this event loop.
pub fn serve(
    prepared: PreparedDeployment,
    observer: &dyn ModuleObserver,
    stop: &AtomicBool,
    on_listening: &dyn Fn() -> Result<()>,
) -> Result<()> {
    let mut owner = Owner::new(prepared, observer)?;
    let result = (|| {
        on_listening()?;
        observer.configure(&owner.desired)?;
        owner.reconcile()?;
        owner.publish()?;
        owner.run(stop)
    })();
    if let Err(error) = &result {
        owner.status.phase = DeploymentPhase::Failed;
        owner.status.error = Some(format!("{error:#}"));
    }
    let cleanup = owner.stop_all();
    owner.status.owner_alive = false;
    if result.is_ok() && cleanup.is_ok() {
        owner.status.phase = DeploymentPhase::Stopped;
    }
    let persist = owner.publish();
    for waiter in owner.waiters.drain(..) {
        let response = if matches!(waiter.action, Action::WaitStopped)
            && result.is_ok()
            && cleanup.is_ok()
            && persist.is_ok()
        {
            Ok(owner.status.clone())
        } else {
            Err(anyhow::anyhow!(
                "deployment owner failed: {}",
                owner
                    .status
                    .error
                    .as_deref()
                    .unwrap_or("cleanup or persistence failed")
            ))
        };
        reply(waiter.stream, response);
    }
    result.and(cleanup).and(persist)
}

struct OwnedProcess {
    child: Box<dyn ChildWrapper>,
    report_path: PathBuf,
    started: Instant,
    reaped: bool,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl OwnedProcess {
    fn terminate(&mut self) -> Result<()> {
        match self.child.start_kill() {
            Ok(()) => {}
            // ESRCH means the owned Unix process group no longer exists.
            #[cfg(unix)]
            Err(error) if error.raw_os_error() == Some(3) => {}
            Err(error) => return Err(error).context("terminating owned process tree"),
        }
        self.runtime
            .block_on(self.child.wait())
            .context("reaping owned process tree")?;
        self.reaped = true;
        Ok(())
    }
}

impl Drop for OwnedProcess {
    fn drop(&mut self) {
        if !self.reaped
            && let Err(error) = self.terminate()
        {
            tracing::error!(%error, "could not reap owned process tree");
        }
    }
}

struct Connection {
    stream: TcpStream,
    bytes: Vec<u8>,
    deadline: Instant,
}

struct Waiter {
    stream: TcpStream,
    action: Action,
    deadline: Option<Instant>,
}

struct Owner<'a> {
    desired: PreparedDeployment,
    observer: &'a dyn ModuleObserver,
    status: DeploymentStatus,
    published: Option<DeploymentStatus>,
    control: Control,
    cache: PathBuf,
    listener: TcpListener,
    // Locks outlive both the process handles and endpoint cleanup.
    locks: Vec<File>,
    children: BTreeMap<String, OwnedProcess>,
    retries: BTreeMap<String, Instant>,
    attempts: BTreeMap<String, u32>,
    next_generation: u64,
    waiters: Vec<Waiter>,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl<'a> Owner<'a> {
    fn new(mut desired: PreparedDeployment, observer: &'a dyn ModuleObserver) -> Result<Self> {
        desired.root = std::fs::canonicalize(&desired.root)?;
        validate_prepared(&desired)?;
        let cache = desired.root.join(".reiny");
        private_directory(&cache)?;
        let root_lock = lock_file(&cache.join("owner.lock"))?;
        let global = std::env::temp_dir().join("reiny-deployment-owners-v1");
        private_directory(&global)?;
        let identity = digest(&serde_json::to_vec(&(
            &desired.domain,
            &desired.deployment,
        ))?);
        let global_lock = lock_file(&global.join(format!("{identity}.lock")))?;
        let generation = nonce()?;
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        listener.set_nonblocking(true)?;
        let control = Control {
            version: 1,
            address: listener.local_addr()?,
            token: nonce()?,
            owner_generation: generation.clone(),
        };
        let status = DeploymentStatus {
            version: 1,
            root: desired.root.clone(),
            deployment: desired.deployment.clone(),
            domain: desired.domain.clone(),
            owner_generation: generation,
            owner_pid: std::process::id(),
            owner_alive: true,
            revision: 0,
            phase: DeploymentPhase::Starting,
            modules: BTreeMap::new(),
            resources: desired.resources.clone(),
            error: None,
        };
        let mut owner = Self {
            desired,
            observer,
            status,
            published: None,
            control,
            cache,
            listener,
            locks: vec![root_lock, global_lock],
            children: BTreeMap::new(),
            retries: BTreeMap::new(),
            attempts: BTreeMap::new(),
            next_generation: 0,
            waiters: Vec::new(),
            runtime: Arc::new(
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?,
            ),
        };
        owner.install_desired_status();
        owner.publish()?;
        atomic_json(&owner.cache.join("prepared.json"), &owner.desired)?;
        atomic_json(&owner.cache.join("control.json"), &owner.control)?;
        Ok(owner)
    }

    fn install_desired_status(&mut self) {
        let names: BTreeSet<_> = self
            .desired
            .nodes
            .iter()
            .map(|node| node.namespace.clone())
            .collect();
        self.status.modules.retain(|name, _| names.contains(name));
        for node in &self.desired.nodes {
            self.status
                .modules
                .entry(node.namespace.clone())
                .and_modify(|status| {
                    status.desired_fingerprint.clone_from(&node.fingerprint);
                })
                .or_insert_with(|| ModuleStatus {
                    namespace: node.namespace.clone(),
                    desired_fingerprint: node.fingerprint.clone(),
                    actual_fingerprint: None,
                    pid: None,
                    generation: 0,
                    phase: DeploymentPhase::Starting,
                    error: None,
                    report: None,
                });
        }
        self.status.resources.clone_from(&self.desired.resources);
    }

    fn publish(&mut self) -> Result<()> {
        if self.published.as_ref() == Some(&self.status) {
            return Ok(());
        }
        self.status.revision = self
            .status
            .revision
            .checked_add(1)
            .context("state revision exhausted")?;
        atomic_json(&self.cache.join("state.json"), &self.status)?;
        self.published = Some(self.status.clone());
        Ok(())
    }

    fn run(&mut self, stop: &AtomicBool) -> Result<()> {
        let mut connections: Vec<Connection> = Vec::new();
        while !stop.load(Ordering::Acquire) {
            self.observe()?;
            self.publish()?;
            for _ in 0..MAX_CONNECTIONS {
                match self.listener.accept() {
                    Ok((stream, _)) => {
                        if connections.len().saturating_add(self.waiters.len()) < MAX_CONNECTIONS {
                            stream.set_nonblocking(true)?;
                            connections.push(Connection {
                                stream,
                                bytes: Vec::new(),
                                deadline: Instant::now() + IO_TIMEOUT,
                            });
                        }
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                    Err(error) => return Err(error.into()),
                }
            }
            let mut index = 0;
            while index < connections.len() {
                match receive(&mut connections[index]) {
                    Ok(Some(request)) => {
                        let connection = connections.swap_remove(index);
                        if !authenticated(&self.control, &request) {
                            reply(
                                connection.stream,
                                Err(anyhow::anyhow!("authentication failed")),
                            );
                            continue;
                        }
                        match request.action {
                            Action::Status => reply(connection.stream, Ok(self.status.clone())),
                            Action::Apply { prepared } => {
                                let result = self.apply(*prepared).map(|()| self.status.clone());
                                reply(connection.stream, result);
                            }
                            Action::Stop => {
                                self.stop_all()?;
                                self.status.phase = DeploymentPhase::Stopped;
                                self.status.owner_alive = false;
                                self.publish()?;
                                reply(connection.stream, Ok(self.status.clone()));
                                return Ok(());
                            }
                            Action::WaitStopped => {
                                reply(connection.stream.try_clone()?, Ok(self.status.clone()));
                                self.waiters.push(Waiter {
                                    stream: connection.stream,
                                    action: Action::WaitStopped,
                                    deadline: None,
                                });
                            }
                            Action::WaitReady { timeout_ms } => {
                                let timeout = Duration::from_millis(timeout_ms)
                                    .min(Duration::from_secs(3600));
                                self.waiters.push(Waiter {
                                    stream: connection.stream,
                                    action: Action::WaitReady { timeout_ms },
                                    deadline: Some(Instant::now() + timeout),
                                });
                            }
                            Action::WaitRevision {
                                revision,
                                timeout_ms,
                            } => {
                                let timeout = Duration::from_millis(timeout_ms)
                                    .min(Duration::from_secs(3600));
                                self.waiters.push(Waiter {
                                    stream: connection.stream,
                                    action: Action::WaitRevision {
                                        revision,
                                        timeout_ms,
                                    },
                                    deadline: Some(Instant::now() + timeout),
                                });
                            }
                        }
                    }
                    Ok(None) => index += 1,
                    Err(error) => {
                        tracing::debug!(%error, "discarding invalid owner request");
                        connections.swap_remove(index);
                    }
                }
            }
            let mut index = 0;
            while index < self.waiters.len() {
                let outcome = self.wait_outcome(&self.waiters[index]);
                if let Some(outcome) = outcome {
                    let waiter = self.waiters.swap_remove(index);
                    reply(waiter.stream, outcome);
                } else {
                    index += 1;
                }
            }
            std::thread::park_timeout(TICK);
        }
        Ok(())
    }

    fn wait_outcome(&self, waiter: &Waiter) -> Option<Result<DeploymentStatus>> {
        match waiter.action {
            Action::WaitReady { .. } => match self.status.phase {
                DeploymentPhase::Ready => return Some(Ok(self.status.clone())),
                DeploymentPhase::Failed | DeploymentPhase::Suspended | DeploymentPhase::Stopped => {
                    return Some(Err(anyhow::anyhow!(
                        "deployment {:?}: {}",
                        self.status.phase,
                        self.status.error.as_deref().unwrap_or("not ready")
                    )));
                }
                DeploymentPhase::Starting | DeploymentPhase::Degraded => {}
            },
            Action::WaitRevision { revision, .. } if self.status.revision > revision => {
                return Some(Ok(self.status.clone()));
            }
            Action::WaitRevision { .. } | Action::WaitStopped => {}
            Action::Status | Action::Apply { .. } | Action::Stop => {
                return Some(Err(anyhow::anyhow!("invalid wait request")));
            }
        }
        waiter
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
            .then(|| Err(anyhow::anyhow!("deployment wait timed out")))
    }
}

impl Drop for Owner<'_> {
    fn drop(&mut self) {
        self.children.clear();
        match std::fs::remove_file(self.cache.join("control.json")) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => tracing::error!(%error, "could not remove owner endpoint"),
        }
        self.locks.clear();
    }
}

impl Owner<'_> {
    fn apply(&mut self, mut prepared: PreparedDeployment) -> Result<()> {
        prepared.root = std::fs::canonicalize(&prepared.root)?;
        validate_prepared(&prepared)?;
        ensure!(
            prepared.root == self.desired.root
                && prepared.deployment == self.desired.deployment
                && prepared.domain == self.desired.domain,
            "apply cannot change owner root, deployment, or domain"
        );
        let removed: Vec<_> = self
            .children
            .keys()
            .filter(|name| {
                !prepared.nodes.iter().any(|node| {
                    &node.namespace == *name
                        && self
                            .status
                            .modules
                            .get(*name)
                            .and_then(|status| status.actual_fingerprint.as_ref())
                            == Some(&node.fingerprint)
                })
            })
            .cloned()
            .collect();
        self.stop_names(&removed)?;
        // Stop with the old observer registry: removed namespaces and replaced
        // providers may no longer be reachable through the new configuration.
        self.observer.configure(&prepared)?;
        atomic_json(&self.cache.join("prepared.json"), &prepared)?;
        self.desired = prepared;
        self.install_desired_status();
        self.retries.clear();
        self.attempts.clear();
        self.status.error = None;
        self.status.phase = DeploymentPhase::Starting;
        self.reconcile()?;
        self.observe()?;
        self.publish()
    }

    fn reconcile(&mut self) -> Result<()> {
        // Clone the small desired descriptions so spawning can mutate ownership.
        let missing: Vec<_> = self
            .desired
            .nodes
            .iter()
            .filter(|node| !self.children.contains_key(&node.namespace))
            .cloned()
            .collect();
        for node in missing {
            if let Err(error) = self.spawn(&node) {
                self.failed(&node.namespace, &format!("{error:#}"), false)?;
                if self.status.phase == DeploymentPhase::Suspended {
                    break;
                }
            }
        }
        Ok(())
    }

    fn spawn(&mut self, node: &PreparedModule) -> Result<()> {
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .context("process generation exhausted")?;
        let generation = self.next_generation;
        let directory = self
            .cache
            .join("generations")
            .join(&self.status.owner_generation)
            .join(generation.to_string());
        std::fs::create_dir_all(&directory)?;
        let bindings_path = directory.join("bindings.json");
        let report_path = directory.join("report.json");
        atomic_json(&bindings_path, &node.bindings)?;
        let stdout = File::create(directory.join("stdout.log"))?;
        let stderr = File::create(directory.join("stderr.log"))?;
        let mut command = Command::new(&node.executable);
        command
            .current_dir(&node.module_dir)
            .args(&node.run.args)
            .arg("--name")
            .arg(&node.namespace)
            .arg("--domain")
            .arg(&self.desired.domain)
            .arg("--module-bindings")
            .arg(&bindings_path)
            .env("REINY_MODULE_REPORT", &report_path)
            .stdin(Stdio::null())
            .stdout(stdout)
            .stderr(stderr);
        if let Some(config) = &node.run.config {
            command.arg("--config").arg(config);
        }
        if let Some(config) = &node.provider.zenoh_config {
            command.arg("--zenoh-config").arg(config);
        }
        for endpoint in &node.provider.connect {
            command.arg("--connect").arg(endpoint);
        }
        let mut command = CommandWrap::from(tokio::process::Command::from(command));
        command.wrap(process_wrap::tokio::KillOnDrop);
        #[cfg(unix)]
        command.wrap(process_wrap::tokio::ProcessGroup::leader());
        #[cfg(windows)]
        command.wrap(process_wrap::tokio::JobObject);
        let _runtime = self.runtime.enter();
        let child = command
            .spawn()
            .with_context(|| format!("spawning module {}", node.namespace))?;
        let pid = child.id().context("spawned child has no process ID")?;
        self.children.insert(
            node.namespace.clone(),
            OwnedProcess {
                child,
                report_path,
                started: Instant::now(),
                reaped: false,
                runtime: Arc::clone(&self.runtime),
            },
        );
        let status = self
            .status
            .modules
            .get_mut(&node.namespace)
            .context("missing desired module status")?;
        status.pid = Some(pid);
        status.generation = generation;
        status.actual_fingerprint = Some(node.fingerprint.clone());
        status.phase = DeploymentPhase::Starting;
        status.error = None;
        status.report = None;
        Ok(())
    }

    fn observe(&mut self) -> Result<()> {
        match self.status.phase {
            DeploymentPhase::Suspended | DeploymentPhase::Stopped | DeploymentPhase::Failed => {
                return Ok(());
            }
            DeploymentPhase::Starting | DeploymentPhase::Ready | DeploymentPhase::Degraded => {}
        }
        let names: Vec<_> = self.children.keys().cloned().collect();
        for name in names {
            self.observe_process(&name)?;
            if self.status.phase == DeploymentPhase::Suspended {
                return Ok(());
            }
        }
        let due: Vec<_> = self
            .retries
            .iter()
            .filter(|(_, deadline)| Instant::now() >= **deadline)
            .map(|(name, _)| name.clone())
            .collect();
        for name in due {
            self.retries.remove(&name);
            let node = self
                .desired
                .nodes
                .iter()
                .find(|node| node.namespace == name)
                .context("missing restart module")?
                .clone();
            if let Err(error) = self.spawn(&node) {
                self.failed(&name, &format!("{error:#}"), true)?;
            }
        }
        let all_ready = self
            .status
            .modules
            .values()
            .all(|status| status.phase == DeploymentPhase::Ready);
        if all_ready {
            if let Err(error) = self.validate_connections() {
                self.status.error = Some(format!("{error:#}"));
                let suspend = self
                    .desired
                    .nodes
                    .iter()
                    .any(|node| node.run.on_failure == FailurePolicy::SuspendDeployment);
                self.stop_all()?;
                self.status.phase = if suspend {
                    DeploymentPhase::Suspended
                } else {
                    DeploymentPhase::Failed
                };
            } else {
                self.status.phase = DeploymentPhase::Ready;
            }
        } else if self
            .status
            .modules
            .values()
            .any(|status| status.phase == DeploymentPhase::Failed)
        {
            if self.retries.is_empty()
                && self
                    .status
                    .modules
                    .values()
                    .all(|status| status.phase == DeploymentPhase::Failed)
            {
                self.status.phase = DeploymentPhase::Failed;
                self.status.error = self
                    .status
                    .modules
                    .values()
                    .find_map(|status| status.error.clone());
            } else {
                self.status.phase = DeploymentPhase::Degraded;
            }
        } else {
            self.status.phase = DeploymentPhase::Starting;
        }
        Ok(())
    }

    fn observe_process(&mut self, name: &str) -> Result<()> {
        let process = self
            .children
            .get_mut(name)
            .context("missing owned process")?;
        if let Some(exit) = process.child.inner_mut().try_wait()? {
            let success = exit.success();
            process.terminate()?;
            self.children.remove(name);
            let node = self
                .desired
                .nodes
                .iter()
                .find(|node| node.namespace == name)
                .context("missing desired module")?;
            let restart = match node.run.restart {
                RestartPolicy::Manual => false,
                RestartPolicy::OnFailure => !success,
                RestartPolicy::Always => true,
            };
            return self.failed(name, &format!("process exited: {exit}"), restart);
        }
        let path = process.report_path.clone();
        let started = process.started;
        let ready = self.observer.ready(name)?;
        let report = match std::fs::read(&path) {
            Ok(bytes) => Some(
                serde_json::from_slice::<ModuleReport>(&bytes)
                    .context("invalid module readiness report"),
            ),
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        if let Some(report) = report {
            let node = self
                .desired
                .nodes
                .iter()
                .find(|node| node.namespace == name)
                .context("missing desired module")?;
            match report.and_then(|report| {
                validate_report(node, &report)?;
                Ok(report)
            }) {
                Ok(report) => {
                    let status = self
                        .status
                        .modules
                        .get_mut(name)
                        .context("missing module status")?;
                    status.report = Some(report);
                    status.phase = if ready {
                        DeploymentPhase::Ready
                    } else {
                        DeploymentPhase::Starting
                    };
                    if started.elapsed() >= Duration::from_secs(60) {
                        self.attempts.remove(name);
                    }
                }
                Err(error) => {
                    self.stop_names(&[name.to_owned()])?;
                    return self.failed(name, &format!("{error:#}"), false);
                }
            }
        } else if started.elapsed() >= START_GRACE {
            self.stop_names(&[name.to_owned()])?;
            return self.failed(name, "application readiness report timed out", false);
        }
        if started.elapsed() >= START_GRACE
            && self
                .status
                .modules
                .get(name)
                .is_some_and(|status| status.phase == DeploymentPhase::Starting)
        {
            self.stop_names(&[name.to_owned()])?;
            self.failed(name, "application readiness timed out", false)?;
        }
        Ok(())
    }

    fn validate_connections(&self) -> Result<()> {
        for node in &self.desired.nodes {
            let report = self
                .status
                .modules
                .get(&node.namespace)
                .and_then(|status| status.report.as_ref())
                .context("missing ready module report")?;
            for (port, input) in &node.bindings.inputs {
                let actual = report.inputs.get(port).context("missing actual input")?;
                let source = self
                    .status
                    .modules
                    .get(&input.source)
                    .and_then(|status| status.report.as_ref())
                    .with_context(|| {
                        format!(
                            "input {}/{port} has no ready source {}",
                            node.namespace, input.source
                        )
                    })?;
                let outputs: Vec<_> = source
                    .outputs
                    .values()
                    .filter(|output| type_matches(&input.type_name, &output.type_name))
                    .collect();
                ensure!(
                    !outputs.is_empty(),
                    "source {} does not implement {}",
                    input.source,
                    input.type_name
                );
                for output in outputs {
                    ensure!(
                        actual.type_name == output.type_name && actual.schema == output.schema,
                        "schema mismatch for {}/{port}: input {}#{:016x}, source {} {}#{:016x}",
                        node.namespace,
                        actual.type_name,
                        actual.schema,
                        input.source,
                        output.type_name,
                        output.schema
                    );
                }
            }
        }
        Ok(())
    }

    fn failed(&mut self, name: &str, error: &str, restart: bool) -> Result<()> {
        let status = self
            .status
            .modules
            .get_mut(name)
            .context("missing failed module status")?;
        status.phase = DeploymentPhase::Failed;
        status.pid = None;
        status.actual_fingerprint = None;
        status.report = None;
        status.error = Some(error.to_owned());
        let policy = self
            .desired
            .nodes
            .iter()
            .find(|node| node.namespace == name)
            .context("missing failed desired module")?
            .run
            .on_failure;
        match policy {
            FailurePolicy::SuspendDeployment => {
                self.status.error = Some(format!("{name}: {error}"));
                self.retries.clear();
                self.stop_all()?;
                self.status.phase = DeploymentPhase::Suspended;
            }
            FailurePolicy::Report => {
                if restart {
                    let attempt = self.attempts.entry(name.to_owned()).or_default();
                    let delay =
                        Duration::from_millis(200_u64.saturating_mul(1_u64 << (*attempt).min(7)));
                    *attempt = attempt.saturating_add(1);
                    self.retries.insert(name.to_owned(), Instant::now() + delay);
                }
            }
        }
        Ok(())
    }

    fn stop_names(&mut self, names: &[String]) -> Result<()> {
        for name in names {
            if self.children.contains_key(name)
                && let Err(error) = self.observer.request_stop(name)
            {
                tracing::warn!(%name, %error, "cooperative stop unavailable; enforcing owned-process deadline");
            }
        }
        let deadline = Instant::now() + STOP_GRACE;
        let mut pending: BTreeSet<_> = names
            .iter()
            .filter(|name| self.children.contains_key(*name))
            .cloned()
            .collect();
        while !pending.is_empty() {
            let mut done = Vec::new();
            for name in &pending {
                let process = self
                    .children
                    .get_mut(name)
                    .context("missing stopping process")?;
                // Poll the leader only: JobObject::try_wait consumes the group's
                // completion-port notification that terminate().wait() needs.
                if process.child.inner_mut().try_wait()?.is_some() || Instant::now() >= deadline {
                    done.push(name.clone());
                }
            }
            for name in done {
                self.children
                    .get_mut(&name)
                    .context("missing stopping process")?
                    .terminate()?;
                self.children.remove(&name);
                pending.remove(&name);
                if let Some(status) = self.status.modules.get_mut(&name) {
                    status.pid = None;
                    status.actual_fingerprint = None;
                    status.report = None;
                    status.phase = DeploymentPhase::Stopped;
                }
            }
            if !pending.is_empty() {
                std::thread::park_timeout(TICK);
            }
        }
        Ok(())
    }

    fn stop_all(&mut self) -> Result<()> {
        self.retries.clear();
        let names: Vec<_> = self.children.keys().cloned().collect();
        self.stop_names(&names)
    }
}

fn type_matches(expected: &str, actual: &str) -> bool {
    expected == actual || (!expected.contains('.') && actual.rsplit('.').next() == Some(expected))
}

fn validate_report(node: &PreparedModule, report: &ModuleReport) -> Result<()> {
    ensure!(
        report.version == 1 && report.namespace == node.namespace,
        "readiness report identity mismatch"
    );
    ensure!(
        report.inputs.len() == node.bindings.inputs.len()
            && report.outputs.len() == node.bindings.outputs.len(),
        "readiness report port count mismatch"
    );
    for (port, binding) in &node.bindings.inputs {
        let actual = report
            .inputs
            .get(port)
            .with_context(|| format!("missing input report {port}"))?;
        ensure!(
            type_matches(&binding.type_name, &actual.type_name) && actual.schema != 0,
            "input {port} has an invalid type or schema"
        );
    }
    for (port, binding) in &node.bindings.outputs {
        let actual = report
            .outputs
            .get(port)
            .with_context(|| format!("missing output report {port}"))?;
        ensure!(
            type_matches(&binding.type_name, &actual.type_name) && actual.schema != 0,
            "output {port} has an invalid type or schema"
        );
    }
    Ok(())
}

fn private_directory(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn lock_file(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    file.try_lock()
        .with_context(|| format!("deployment already has an owner ({})", path.display()))?;
    Ok(file)
}

fn nonce() -> Result<String> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|error| anyhow::anyhow!("generating owner credential: {error}"))?;
    Ok(digest(&bytes))
}

fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("state path has no parent")?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer(temporary.as_file_mut(), value)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}

fn write_frame(stream: &mut TcpStream, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    ensure!(bytes.len() <= MAX_FRAME, "owner frame exceeds size limit");
    stream.write_all(&u32::try_from(bytes.len())?.to_be_bytes())?;
    stream.write_all(&bytes)?;
    Ok(())
}

fn read_frame<T: serde::de::DeserializeOwned>(stream: &mut TcpStream) -> Result<T> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length)?;
    let length = usize::try_from(u32::from_be_bytes(length))?;
    ensure!(length <= MAX_FRAME, "owner frame exceeds size limit");
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn receive(connection: &mut Connection) -> Result<Option<Request>> {
    ensure!(
        Instant::now() < connection.deadline,
        "owner request timed out"
    );
    let mut buffer = [0_u8; 8192];
    loop {
        match connection.stream.read(&mut buffer) {
            Ok(0) => bail!("client disconnected"),
            Ok(count) => {
                connection.bytes.extend_from_slice(&buffer[..count]);
                ensure!(
                    connection.bytes.len() <= MAX_FRAME + 4,
                    "owner frame exceeds size limit"
                );
                if let Some(prefix) = connection.bytes.get(..4) {
                    let length = usize::try_from(u32::from_be_bytes(prefix.try_into()?))?;
                    ensure!(length <= MAX_FRAME, "owner frame exceeds size limit");
                    if connection.bytes.len() >= length + 4 {
                        return Ok(Some(serde_json::from_slice(
                            &connection.bytes[4..length + 4],
                        )?));
                    }
                }
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => return Ok(None),
            Err(error) => return Err(error.into()),
        }
    }
}

fn authenticated(control: &Control, request: &Request) -> bool {
    let difference = control
        .token
        .bytes()
        .zip(request.token.bytes())
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        });
    control.token.len() == request.token.len()
        && difference == 0
        && control.owner_generation == request.owner_generation
}

fn reply(mut stream: TcpStream, result: Result<DeploymentStatus>) {
    let response = match result {
        Ok(status) => Response {
            status: Some(status),
            error: None,
        },
        Err(error) => Response {
            status: None,
            error: Some(format!("{error:#}")),
        },
    };
    let result = (|| {
        stream.set_nonblocking(false)?;
        stream.set_write_timeout(Some(IO_TIMEOUT))?;
        write_frame(&mut stream, &response)
    })();
    if let Err(error) = result {
        tracing::debug!(%error, "owner client disconnected before response");
    }
}

fn validate_prepared(prepared: &PreparedDeployment) -> Result<()> {
    ensure!(
        !prepared.deployment.is_empty() && !prepared.domain.is_empty(),
        "empty deployment identity"
    );
    let mut names = BTreeSet::new();
    for node in &prepared.nodes {
        ensure!(
            names.insert(&node.namespace),
            "duplicate module namespace {}",
            node.namespace
        );
        ensure!(
            node.namespace
                .starts_with(&format!("{}/", prepared.deployment))
                || node.namespace == prepared.deployment,
            "module namespace is outside deployment"
        );
        ensure!(
            node.bindings.version == 1 && node.bindings.namespace == node.namespace,
            "invalid module bindings identity"
        );
        ensure!(!node.fingerprint.is_empty(), "missing prepared fingerprint");
        ensure!(
            node.executable.is_absolute() && node.executable.is_file(),
            "prepared executable is missing: {}",
            node.executable.display()
        );
        ensure!(
            node.module_dir.is_absolute() && node.module_dir.is_dir(),
            "invalid module directory"
        );
    }
    Ok(())
}
