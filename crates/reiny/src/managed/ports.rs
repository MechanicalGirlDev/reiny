//! Opening manifest-bound named ports under the shared registry lock.

use std::collections::BTreeSet;
use std::sync::{Arc, PoisonError};

use reiny_core::bindings::{
    EndpointAddress, EndpointContract, PortEndpoint, PortKind, QosProfile, ReceiveBuffer, Replay,
    Retention,
};

use super::{ManagedPort, Ports, Registration};
use crate::engine::{Key, READY_CHUNK};
use crate::{
    Durability, MessageType, ModuleContext, Publisher, Qos, Result, RpcClient, RpcServer, Service,
    Subscriber,
};

impl ModuleContext {
    pub(super) fn port_endpoint(&self, name: &str, input: bool) -> Result<PortEndpoint> {
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

    pub(super) fn named_contract(
        &self,
        name: &str,
        input: bool,
        kind: PortKind,
    ) -> Result<&EndpointContract> {
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

    pub(super) fn lock_ports(
        &self,
        name: &str,
        input: bool,
    ) -> Result<std::sync::MutexGuard<'_, Ports>> {
        let ports = self
            .module
            .ports
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        anyhow::ensure!(!self.shutdown.is_triggered(), "module is shutting down");
        anyhow::ensure!(ports.ready.is_none(), "cannot create a port after ready()");
        anyhow::ensure!(
            ports.admission.is_none(),
            "cannot create a port during admission"
        );
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
}
