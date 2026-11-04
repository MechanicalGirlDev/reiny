//! Registered, trusted in-process components sharing an explicit engine.
//!
//! Preparation opens named ports but must not publish or spawn detached work.
//! The host validates the closed graph before committing readiness and starting
//! any run hook. Shutdown is cooperative: admission tickets withdraw readiness,
//! not leaked endpoint handles, and blocking native work cannot be forcibly killed.
//! Readiness loss retained before activation prevents every run. Loss after that
//! final check can race a peer's first run poll; it fails and stops the group,
//! rather than providing a distributed or permanent readiness transaction.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::bindings::ModuleBindings;
use crate::{MessageType, Publisher, RawPresence, RpcClient, RpcServer, Service, Subscriber};

mod coordinator;
mod handle;
mod host;
mod status;
mod worker;

pub use handle::HostHandle;
pub use host::ComponentHost;
pub use status::{
    ComponentPhase, ComponentStatus, HostError, HostErrorCode, HostOutcome, HostPhase, HostStatus,
};

#[cfg(test)]
mod tests;

/// A component lifecycle operation borrowing its component and context.
pub type ComponentFuture<'a> = Pin<Box<dyn Future<Output = crate::Result<()>> + Send + 'a>>;

/// Trusted application code whose named endpoints are admitted as one closed graph.
///
/// Hooks must yield to the runtime and tolerate cancellation. `prepare` must not
/// publish or spawn detached work. `run` is a service and must not return before
/// shutdown. A cancelled prepare/run future is dropped before `stop` is called.
pub trait Component: Send + 'static {
    /// Open every declared endpoint and retain its live handle.
    fn prepare<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a>;
    /// Serve until cancellation; returning early is a host failure.
    fn run<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a>;
    /// Release resources after cancellation, including partially prepared state.
    fn stop<'a>(&'a mut self, ctx: &'a ComponentContext, reason: StopReason)
    -> ComponentFuture<'a>;
}

/// Constructs one independent component from its explicit per-instance config.
pub trait ComponentFactory: Send + Sync + 'static {
    /// Construct a component without detached work or communication.
    fn create(&self, config: Option<&toml::Table>) -> crate::Result<Box<dyn Component>>;
}

/// Why the host is cancelling the group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopReason {
    /// An explicit stop request.
    Requested,
    /// The group could not finish admission.
    StartupFailure,
    /// An admitted service failed.
    ComponentFailure,
}

/// A component's restricted communication and cooperative shutdown capability.
pub struct ComponentContext {
    inner: crate::ModuleContext,
}

impl ComponentContext {
    /// The resolved instance namespace.
    #[must_use]
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    /// The explicit host domain.
    #[must_use]
    pub fn domain(&self) -> &str {
        self.inner.domain()
    }

    /// The same configuration value passed to this instance's factory.
    #[must_use]
    pub fn config_table(&self) -> Option<&toml::Table> {
        self.inner.config_table()
    }

    /// Open a declared input with its exact sources and policies.
    pub fn input<T: prost::Message + Default + MessageType>(
        &self,
        name: &str,
    ) -> crate::Result<Subscriber<T>> {
        self.inner.input(name)
    }

    /// Open a declared output.
    pub fn output<T: prost::Message + MessageType>(
        &self,
        name: &str,
    ) -> crate::Result<Publisher<T>> {
        self.inner.output(name)
    }

    /// Open a declared RPC client.
    pub fn rpc_client<S: Service>(&self, name: &str) -> crate::Result<RpcClient<S>> {
        self.inner.rpc_client(name)
    }

    /// Open a declared RPC server.
    pub fn rpc_server<S: Service>(&self, name: &str) -> crate::Result<RpcServer<S>> {
        self.inner.rpc_server(name)
    }

    /// Observe readiness of this input's declared source modules.
    pub fn watch_input_ready(&self, name: &str) -> crate::Result<RawPresence> {
        self.inner.watch_input_ready(name)
    }

    /// Wait for the host's cooperative shutdown signal.
    pub async fn shutdown(&self) {
        self.inner.shutdown().await;
    }
}

/// An explicit factory allowlist; names never load code dynamically.
#[derive(Default)]
pub struct ComponentRegistry {
    factories: BTreeMap<String, Arc<dyn ComponentFactory>>,
}

impl ComponentRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a factory without replacing an existing registration.
    pub fn register(
        &mut self,
        factory_id: String,
        factory: Arc<dyn ComponentFactory>,
    ) -> Result<(), HostError> {
        match self.factories.entry(factory_id) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(factory);
                Ok(())
            }
            std::collections::btree_map::Entry::Occupied(entry) => Err(HostError::new(
                HostErrorCode::DuplicateFactory,
                None,
                format!("factory '{}' is already registered", entry.key()),
            )),
        }
    }
}

/// One component instance with resolved, authoritative named bindings.
pub struct ComponentInstanceSpec {
    /// Factory selected from the explicit registry.
    pub factory_id: String,
    /// Complete resolved port and source authority.
    pub bindings: ModuleBindings,
    /// Per-instance settings, without process environment defaults.
    pub config: Option<toml::Table>,
}

/// Explicit host resources and lifecycle deadlines.
pub struct HostOptions {
    /// The transport domain shared by this group.
    pub domain: String,
    /// A shared engine, commonly one `Local`.
    pub engine: Arc<dyn crate::engine::Engine>,
    /// Deadline for the whole startup, not per component.
    pub startup_timeout: Duration,
    /// Deadline for all cleanup and joins after shutdown begins.
    pub stop_timeout: Duration,
}
