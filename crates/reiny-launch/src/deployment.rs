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
use reiny_core::bindings::{
    MODULE_BINDINGS_VERSION, ModuleReport, PortEndpoint, Replay, Retention,
};
use serde::{Deserialize, Serialize};

use crate::file_lock::FileLock;
use crate::modules::{FailurePolicy, RestartPolicy, RunKind};
use crate::prepared::{PreparedDeployment, PreparedModule, digest};

const TICK: Duration = Duration::from_millis(20);
const STOP_GRACE: Duration = Duration::from_secs(2);
const START_GRACE: Duration = Duration::from_secs(30);
const IO_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_FRAME: usize = 8 * 1024 * 1024;
const MAX_CONNECTIONS: usize = 64;
const STATUS_VERSION: u32 = 2;

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
    WaitTerminal,
    WaitReady { timeout_ms: u64 },
    WaitRevision { revision: u64, timeout_ms: u64 },
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Response {
    status: Option<DeploymentStatus>,
    error: Option<String>,
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
    locks: Vec<FileLock>,
    children: BTreeMap<String, OwnedProcess>,
    retries: BTreeMap<String, Instant>,
    attempts: BTreeMap<String, u32>,
    next_generation: u64,
    graph_generation: u64,
    hosted: Option<hosted::Hosted>,
    remote: BTreeMap<String, remote::SshTarget>,
    waiters: Vec<Waiter>,
    runtime: Arc<tokio::runtime::Runtime>,
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

mod cleanup;
mod client;
mod contracts;
mod desired;
mod events;
mod hosted;
mod io;
mod lifecycle;
mod observe;
mod process;
mod remote;
mod setup;
mod status;
#[cfg(test)]
mod tests;
mod validation;
pub use client::{DeploymentClient, StopSubscription, TerminalSubscription, last_status};
pub use hosted::{ComponentHostStatus, ComponentStatus};
use io::{
    atomic_json, authenticated, lock_file, nonce, private_directory, read_frame, receive, reply,
    validate_report, write_frame,
};
pub use lifecycle::{serve, serve_remote};
use process::{OwnedProcess, ProcessObservation};
pub use remote::{
    RemoteOwnership, RemoteTreeCleanup, SshTarget, reconcile_remote, remote_ownership,
};
pub use status::{DeploymentPhase, DeploymentStatus, ModuleStatus};
pub(crate) use validation::validate_prepared;
