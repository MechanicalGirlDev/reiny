//! Manifest-authoritative endpoints and the explicit managed readiness boundary.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use reiny_core::bindings::{
    EndpointAddress, EndpointContract, MODULE_REPORT_VERSION, ModuleBindings, ModuleReport,
    PortEndpoint, PortKind, PortReport, QosProfile, ReceiveBuffer, Replay, ResponseReport,
    Retention,
};

use crate::engine::{Guard, Key, READY_CHUNK, STOP_CHUNK};
use crate::{
    Durability, MessageType, ModuleContext, Publisher, Qos, Result, RpcClient, RpcServer, Service,
    Subscriber, validate_segment,
};

mod bridging;
pub(crate) mod config;
mod dynamic;
pub use dynamic::DeclaredPublisher;
mod owned;
#[cfg(any(test, feature = "conformance"))]
mod scope;
pub use owned::OwnedChild;
#[cfg(feature = "zenoh")]
pub(crate) use owned::child_zenoh_config;
#[cfg(test)]
mod tests;

/// Optional absolute JSON report destination supplied by the process owner.
pub const MODULE_REPORT_ENV: &str = "REINY_MODULE_REPORT";
/// Absolute prepared bundle directory supplied by the deployment owner.
pub const BUNDLE_DIR_ENV: &str = "REINY_BUNDLE_DIR";
/// Optional prepared configuration asset directory supplied by the owner.
pub const CONFIG_DIR_ENV: &str = "REINY_CONFIG_DIR";

#[derive(Default)]
pub(crate) struct ModuleRuntime {
    pub(crate) bindings: Option<ModuleBindings>,
    pub(crate) report_path: Option<PathBuf>,
    pub(crate) bundle_dir: Option<PathBuf>,
    pub(crate) config_dir: Option<PathBuf>,
    pub(crate) child_zenoh_config: Option<String>,
    ports: Arc<Mutex<Ports>>,
    stop: Option<Guard>,
}

impl Drop for ModuleRuntime {
    fn drop(&mut self) {
        let mut ports = self.ports.lock().unwrap_or_else(PoisonError::into_inner);
        ports.ready = None;
        for (name, child) in &ports.children {
            if child.kill.send(()).is_err() && !child.exited {
                tracing::warn!(
                    child = name,
                    "owned child supervisor ended before reporting process exit"
                );
            }
        }
    }
}

#[derive(Default)]
struct Ports {
    inputs: BTreeMap<String, Registration>,
    outputs: BTreeMap<String, Registration>,
    children: BTreeMap<String, owned::ChildState>,
    ready: Option<Guard>,
}

struct Registration {
    contract: PortReport,
    transport_type: String,
    live: Weak<()>,
}

pub(crate) struct ManagedPort {
    _live: Arc<()>,
    ports: Weak<Mutex<Ports>>,
    shutdown: crate::shutdown::Shutdown,
}

impl ManagedPort {
    fn new(cloudy: &ModuleContext, live: Arc<()>) -> Self {
        Self {
            _live: live,
            ports: Arc::downgrade(&cloudy.module.ports),
            shutdown: cloudy.shutdown.clone(),
        }
    }
}

impl Drop for ManagedPort {
    fn drop(&mut self) {
        if let Some(ports) = self.ports.upgrade() {
            let mut ports = ports.lock().unwrap_or_else(PoisonError::into_inner);
            if ports.ready.take().is_some() {
                self.shutdown.trigger();
            }
        }
    }
}

fn compiled_type<T: MessageType>(declared: &str) -> Result<ResponseReport> {
    validate_segment("transport type", T::TYPE_NAME)?;
    let actual = T::TYPE_NAME;
    anyhow::ensure!(
        declared == actual,
        "declared port type '{declared}' differs from actual type '{actual}'"
    );
    let schema = T::SCHEMA_FINGERPRINT;
    Ok(ResponseReport {
        type_name: actual.to_string(),
        schema,
    })
}

impl Registration {
    fn of<T: MessageType>(
        contract: &EndpointContract,
        endpoint: PortEndpoint,
        live: &Arc<()>,
    ) -> Result<Self> {
        let compiled = compiled_type::<T>(&contract.type_name)?;
        Ok(Self {
            contract: PortReport {
                type_name: compiled.type_name,
                schema: compiled.schema,
                contract: contract.clone(),
                response: None,
                endpoint,
            },
            transport_type: T::TYPE_NAME.to_string(),
            live: Arc::downgrade(live),
        })
    }

    fn rpc<S: Service>(
        contract: &EndpointContract,
        endpoint: PortEndpoint,
        live: &Arc<()>,
    ) -> Result<Self> {
        let mut registration = Self::of::<S>(contract, endpoint, live)?;
        let response = contract
            .response
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("RPC requires a response type"))?;
        registration.contract.response = Some(compiled_type::<S::Response>(response)?);
        Ok(registration)
    }
}

impl ModuleContext {
    pub(crate) fn configure_module(
        &mut self,
        bindings: Option<ModuleBindings>,
        report_path: Option<PathBuf>,
    ) -> Result<()> {
        if bindings.is_some() {
            let caps = self.engine.caps();
            anyhow::ensure!(
                caps.query && caps.liveliness && caps.attachment,
                "managed modules require query, liveliness and schema attachments"
            );
            let key = Key::module(&self.domain, Some(&self.id), STOP_CHUNK);
            let reply_key = key.clone();
            let shutdown = self.shutdown.clone();
            self.module.stop = Some(self.engine.respond(
                &key,
                Box::new(move |query| {
                    shutdown.trigger();
                    if let Err(error) = query.reply(&reply_key, b"ack".to_vec(), None) {
                        tracing::warn!(key = %reply_key, %error, "stop acknowledgement failed");
                    }
                }),
            )?);
        }
        self.module.bindings = bindings;
        self.module.report_path = report_path;
        self.module.bundle_dir = std::env::var_os(BUNDLE_DIR_ENV).map(PathBuf::from);
        self.module.config_dir = std::env::var_os(CONFIG_DIR_ENV).map(PathBuf::from);
        Ok(())
    }

    pub(crate) fn endpoint_namespace(&self) -> &str {
        self.module
            .bindings
            .as_ref()
            .map_or(&self.id, |bindings| &bindings.endpoint_namespace)
    }

    fn port_endpoint(&self, name: &str, input: bool) -> Result<PortEndpoint> {
        let bindings = self
            .module
            .bindings
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("module bindings are required"))?;
        if input {
            Ok(PortEndpoint::Input {
                sources: bindings
                    .inputs
                    .get(name)
                    .ok_or_else(|| anyhow::anyhow!("undefined input '{name}'"))?
                    .sources
                    .clone(),
            })
        } else {
            Ok(PortEndpoint::Output {
                address: EndpointAddress {
                    namespace: bindings.endpoint_namespace.clone(),
                    port: name.to_owned(),
                },
            })
        }
    }

    fn named_contract(&self, name: &str, input: bool, kind: PortKind) -> Result<&EndpointContract> {
        let bindings =
            self.module.bindings.as_ref().ok_or_else(|| {
                anyhow::anyhow!("named endpoint '{name}' requires module bindings")
            })?;
        anyhow::ensure!(
            !bindings.children.values().any(|child| {
                (if input { &child.inputs } else { &child.outputs })
                    .iter()
                    .any(|port| port == name)
            }),
            "port '{name}' is exclusively delegated to an owned child"
        );
        let contract = if input {
            bindings.inputs.get(name).map(|port| &port.contract)
        } else {
            bindings.outputs.get(name).map(|port| &port.contract)
        }
        .ok_or_else(|| anyhow::anyhow!("undefined endpoint '{name}'"))?;
        anyhow::ensure!(
            contract.kind == kind,
            "wrong endpoint operation for '{name}'"
        );
        Ok(contract)
    }

    fn lock_ports(&self, name: &str, input: bool) -> Result<std::sync::MutexGuard<'_, Ports>> {
        let ports = self
            .module
            .ports
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        anyhow::ensure!(!self.shutdown.is_triggered(), "module is shutting down");
        anyhow::ensure!(ports.ready.is_none(), "cannot create a port after ready()");
        let registrations = if input { &ports.inputs } else { &ports.outputs };
        anyhow::ensure!(
            !registrations.contains_key(name),
            "port '{name}' already created"
        );
        Ok(ports)
    }

    /// Open a stream input from exactly its configured sources, with app-owned replay and buffering.
    ///
    /// Use [`ModuleContext::watch_input_ready`] before [`ModuleContext::ready`] when an initial volatile
    /// publication must wait for a connected application to finish opening its ports.
    pub fn input<T: prost::Message + Default + MessageType>(
        &self,
        name: &str,
    ) -> Result<Subscriber<T>> {
        let contract = self.named_contract(name, true, PortKind::Stream)?;
        let mut ports = self.lock_ports(name, true)?;
        let live = Arc::new(());
        let registration = Registration::of::<T>(contract, self.port_endpoint(name, true)?, &live)?;
        let sources = &self
            .module
            .bindings
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing module bindings"))?
            .inputs[name]
            .sources;
        let mut builder = self.subscriber::<T>().sources(sources);
        if contract.replay == Replay::Last {
            builder = builder.latched();
        }
        if let ReceiveBuffer::Latest(depth) = contract.buffer {
            builder = builder.latest(depth);
        }
        let mut subscriber = builder.build_named(name)?;
        subscriber.managed = Some(ManagedPort::new(self, live));
        ports.inputs.insert(name.to_string(), registration);
        Ok(subscriber)
    }

    /// Watch readiness of exactly the source modules wired to a declared input.
    ///
    /// Existing ready sources are replayed as joined events. Register the watch before
    /// announcing local readiness so cyclic compositions need no startup sleeps.
    pub fn watch_input_ready(&self, name: &str) -> Result<crate::RawPresence> {
        let bindings = self
            .module
            .bindings
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("module bindings are required"))?;
        let input = bindings
            .inputs
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("undefined input '{name}'"))?;
        let keys: Vec<_> = input
            .sources
            .iter()
            .map(|source| source.namespace.as_str())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|source| Key::module(&self.domain, Some(source), READY_CHUNK))
            .collect();
        crate::RawPresence::many(self, &keys)
    }

    /// Open a stream output using the app's transport policy and retained history.
    pub fn output<T: prost::Message + MessageType>(&self, name: &str) -> Result<Publisher<T>> {
        let contract = self.named_contract(name, false, PortKind::Stream)?;
        let mut ports = self.lock_ports(name, false)?;
        let live = Arc::new(());
        let registration =
            Registration::of::<T>(contract, self.port_endpoint(name, false)?, &live)?;
        let mut qos = match contract.qos {
            QosProfile::Default => Qos::DEFAULT,
            QosProfile::Command => Qos::COMMAND,
            QosProfile::Sensor => Qos::SENSOR,
        };
        qos.durability = match contract.retention {
            Retention::Volatile => Durability::Volatile,
            Retention::Last => Durability::TransientLocal,
        };
        let mut publisher = self.publisher::<T>().qos(qos).build_named(name)?;
        publisher.managed = Some(ManagedPort::new(self, live));
        ports.outputs.insert(name.to_string(), registration);
        Ok(publisher)
    }

    /// Open a named RPC caller to its single exact target, verifying both schema fingerprints.
    pub fn rpc_client<S: Service>(&self, name: &str) -> Result<RpcClient<S>> {
        let contract = self.named_contract(name, true, PortKind::Rpc)?;
        let mut ports = self.lock_ports(name, true)?;
        let live = Arc::new(());
        let registration =
            Registration::rpc::<S>(contract, self.port_endpoint(name, true)?, &live)?;
        let sources = &self
            .module
            .bindings
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing module bindings"))?
            .inputs[name]
            .sources;
        anyhow::ensure!(sources.len() == 1, "RPC requires exactly one target");
        let mut caller = self.caller::<S>().target(&sources[0]).build_named();
        caller.managed = Some(ManagedPort::new(self, live));
        ports.inputs.insert(name.to_string(), registration);
        Ok(caller)
    }

    /// Open a named RPC server at the app's endpoint namespace.
    pub fn rpc_server<S: Service>(&self, name: &str) -> Result<RpcServer<S>> {
        let contract = self.named_contract(name, false, PortKind::Rpc)?;
        let mut ports = self.lock_ports(name, false)?;
        let live = Arc::new(());
        let registration =
            Registration::rpc::<S>(contract, self.port_endpoint(name, false)?, &live)?;
        let mut server = RpcServer::declare(self, name)?;
        server.managed = Some(ManagedPort::new(self, live));
        ports.outputs.insert(name.to_string(), registration);
        Ok(server)
    }

    /// Write the compiled report and announce readiness after all ports and owned children are live.
    ///
    /// Every declared child must have completed [`OwnedChild::wait_ready`]. Losing a child's
    /// readiness or process invalidates this module's readiness and requests shutdown.
    pub fn ready(&self) -> Result<()> {
        let mut ports = self
            .module
            .ports
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        anyhow::ensure!(!self.shutdown.is_triggered(), "module is shutting down");
        if ports.ready.is_some() {
            return Ok(());
        }
        anyhow::ensure!(self.engine.caps().liveliness, "ready() requires liveliness");
        let mut report = ModuleReport {
            version: MODULE_REPORT_VERSION,
            namespace: self.id.clone(),
            endpoint_namespace: self.endpoint_namespace().to_owned(),
            fingerprint_version: reiny_core::SCHEMA_FINGERPRINT_VERSION,
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
        };
        for (registered, reported) in [
            (&ports.inputs, &mut report.inputs),
            (&ports.outputs, &mut report.outputs),
        ] {
            for (name, port) in registered {
                anyhow::ensure!(
                    port.live.strong_count() > 0,
                    "declared port '{name}' has no live handle"
                );
                reported.insert(name.clone(), port.contract.clone());
            }
        }
        if let Some(bindings) = &self.module.bindings {
            for name in bindings.children.keys() {
                let child = ports
                    .children
                    .get(name)
                    .ok_or_else(|| anyhow::anyhow!("owned child '{name}' has not been spawned"))?;
                anyhow::ensure!(
                    child.ready && !child.exited,
                    "owned child '{name}' is not live and ready"
                );
                let child_report = child.report.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("owned child '{name}' has no validated report")
                })?;
                report.inputs.extend(child_report.inputs.clone());
                report.outputs.extend(child_report.outputs.clone());
            }
            for (declared, reported) in [
                (bindings.inputs.keys().collect::<Vec<_>>(), &report.inputs),
                (bindings.outputs.keys().collect::<Vec<_>>(), &report.outputs),
            ] {
                for name in declared {
                    anyhow::ensure!(
                        reported.contains_key(name),
                        "declared port '{name}' has no live handle"
                    );
                }
            }
        }
        if let Some(path) = &self.module.report_path {
            config::write_report(path, &report)?;
        }
        ports.ready = Some(self.engine.declare_alive(&Key::module(
            &self.domain,
            Some(&self.id),
            READY_CHUNK,
        ))?);
        Ok(())
    }
}
