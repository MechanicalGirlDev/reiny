//! One app-owned component-host endpoint, not a deployment supervisor.
//!
//! The owner binds a loopback listener before spawning this executable and writes
//! a single framed [`Bootstrap`] through its private inherited stdin pipe. The
//! address and identity are trusted bootstrap data, not a peer's claimed identity.
//! The host proves possession of the one-use nonce. The owner must compare that
//! proof, compatibility and factory revisions before acknowledging authentication.
//!
//! Only that connected owner may commit retained tickets, after validating every
//! report against its own bindings and validating the complete graph. It must
//! collect every host's commit acknowledgement before sending any run command.
//! A successful send is not delivery, and process death can race run delivery.
//! Neither this endpoint nor its terminal outcome proves child ownership/reaping.
//!
//! Control runs on its own thread's runtime, separate from component workers.
//! On EOF, errors or timeout, shutdown is bounded even if workers block. The
//! executable must exit after [`ProcessHost::run`] returns: unresponsive tasks
//! cannot be killed safely inside a reusable embedding process.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::{ComponentFactory, ComponentRegistry};

use protocol::{FactoryRegistration, Nonce, SessionIdentity};
/// Shared owner/host wire contract.
pub use reiny_core::host_protocol as protocol;

mod codec;
mod connection;
mod error;
mod placement;
mod preparation;
mod session;
mod terminal;

#[cfg(test)]
mod tests;

pub use codec::{decode as decode_payload, encode as encode_payload};
pub use error::ProcessError;

/// Private inherited bootstrap, never stored in argv, logs or prepared files.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bootstrap {
    /// Owner's already-bound loopback control listener.
    pub address: SocketAddr,
    /// Exact generations and prepared digests expected for this connection.
    pub identity: SessionIdentity,
    /// Random one-use owner secret. This type does not generate entropy.
    pub nonce: Nonce,
}

impl Bootstrap {
    /// Read one bounded, strict JSON frame from the private inherited pipe.
    ///
    /// The spawning owner must close the pipe. Pipe acquisition/lifetime belongs
    /// to the executable; this synchronous reader cannot time out arbitrary `Read`.
    pub fn read(reader: &mut impl std::io::Read) -> Result<Self, ProcessError> {
        let mut header = [0; 4];
        reader.read_exact(&mut header).map_err(ProcessError::io)?;
        let length = protocol::payload_length(header).map_err(ProcessError::frame)?;
        let mut payload = vec![0; length];
        reader.read_exact(&mut payload).map_err(ProcessError::io)?;
        codec::decode(&payload)
    }
}

/// Hard local limits, never expanded by an owner's remaining budget.
#[derive(Clone, Copy)]
pub struct ProcessLimits {
    /// Whole connection/authentication/preparation/admission deadline.
    pub startup: Duration,
    /// Whole cooperative shutdown deadline.
    pub shutdown: Duration,
    /// Deadline for a partial frame or one outbound frame, not an idle session.
    pub frame: Duration,
    /// Maximum assigned instances, placement-map entries and registered factories.
    pub instances: usize,
}

impl Default for ProcessLimits {
    fn default() -> Self {
        Self {
            startup: Duration::from_secs(30),
            shutdown: Duration::from_secs(5),
            frame: Duration::from_secs(5),
            instances: 1024,
        }
    }
}

/// Revisioned application allowlist; registration never loads code dynamically.
#[derive(Default)]
pub struct ProcessRegistry {
    local: ComponentRegistry,
    revisions: BTreeMap<String, String>,
}

impl ProcessRegistry {
    /// Construct an empty allowlist.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an implementation once, with its exact application revision.
    pub fn register(
        &mut self,
        implementation: FactoryRegistration,
        factory: Arc<dyn ComponentFactory>,
    ) -> Result<(), ProcessError> {
        if implementation.factory.is_empty() || implementation.revision.is_empty() {
            return Err(ProcessError::new(protocol::FailureCode::Factory));
        }
        self.local
            .register(implementation.factory.clone(), factory)
            .map_err(|_| ProcessError::new(protocol::FailureCode::Factory))?;
        self.revisions
            .insert(implementation.factory, implementation.revision);
        Ok(())
    }

    fn registrations(&self) -> Vec<FactoryRegistration> {
        self.revisions
            .iter()
            .map(|(factory, revision)| FactoryRegistration {
                factory: factory.clone(),
                revision: revision.clone(),
            })
            .collect()
    }
}

/// Synchronous process entry point with independently scheduled control I/O.
pub struct ProcessHost;

impl ProcessHost {
    /// Serve exactly one owner connection. No reconnect or replay is permitted.
    ///
    /// Call outside any Tokio runtime, on the executable's main/control thread.
    /// Return means local cleanup was attempted, not that every task joined.
    /// The caller must exit the executable, especially after `Unresponsive`.
    pub fn run(
        bootstrap: Bootstrap,
        registry: ProcessRegistry,
        limits: ProcessLimits,
    ) -> Result<protocol::HostOutcome, ProcessError> {
        let now = std::time::Instant::now();
        if !bootstrap.address.ip().is_loopback()
            || registry.revisions.len() > limits.instances
            || limits.instances == 0
            || [limits.startup, limits.shutdown, limits.frame]
                .iter()
                .any(|budget| budget.is_zero() || now.checked_add(*budget).is_none())
            || tokio::runtime::Handle::try_current().is_ok()
        {
            return Err(ProcessError::new(protocol::FailureCode::Assignment));
        }
        let workers = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(ProcessError::io)?;
        // Zenoh requires a multithread-capable runtime even for control-side
        // transport construction. Keep its one control worker separate.
        let control = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(ProcessError::io)?;
        let result = control.block_on(session::serve(
            bootstrap,
            registry,
            session::Resources {
                limits,
                workers: workers.handle().clone(),
            },
        ));
        control.shutdown_background();
        // Never wait indefinitely for native code that ignores cancellation.
        workers.shutdown_background();
        result
    }
}
