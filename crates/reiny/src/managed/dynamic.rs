//! Declared endpoints for languages supplying encoded protobuf messages.

use std::sync::Arc;

use reiny_core::bindings::{
    EndpointContract, PortEndpoint, PortKind, PortReport, QosProfile, ReceiveBuffer, Replay,
    Retention,
};

use super::{ManagedPort, Registration};
use crate::engine::{Key, SUB_CHUNK};
use crate::pubsub::{Core, PublisherCore};
use crate::{Durability, ModuleContext, Qos, RawSubscriber, Result, validate_segment};

/// An owned, declared publisher of encoded protobuf messages.
pub struct DeclaredPublisher {
    core: PublisherCore,
    _managed: ManagedPort,
}

impl DeclaredPublisher {
    /// Publish bytes with the mandatory declared schema fingerprint.
    pub fn send(&self, payload: Vec<u8>) -> Result<()> {
        self.core.send(payload)
    }
}

fn registration(
    contract: &EndpointContract,
    type_name: &str,
    schema: u64,
    endpoint: PortEndpoint,
    live: &Arc<()>,
) -> Result<Registration> {
    anyhow::ensure!(
        type_name == contract.type_name,
        "declared port type '{}' differs from actual type '{type_name}'",
        contract.type_name
    );
    let transport_type = type_name;
    validate_segment("transport type", transport_type)?;
    Ok(Registration {
        contract: PortReport {
            type_name: type_name.to_owned(),
            schema,
            contract: contract.clone(),
            response: None,
            endpoint,
        },
        transport_type: transport_type.to_owned(),
        live: Arc::downgrade(live),
    })
}

impl ModuleContext {
    /// Subscribe to a named input's exact prepared sources and receive encoded protobuf.
    pub fn raw_input(&self, name: &str, type_name: &str, schema: u64) -> Result<RawSubscriber> {
        let contract = self.named_contract(name, true, PortKind::Stream)?;
        let mut ports = self.lock_ports(name, true)?;
        let live = Arc::new(());
        let registration = registration(
            contract,
            type_name,
            schema,
            self.port_endpoint(name, true)?,
            &live,
        )?;
        let sources = self
            .module
            .bindings
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing module bindings"))?
            .inputs[name]
            .sources
            .iter()
            .cloned()
            .map(Some)
            .collect::<Vec<_>>();
        let latest = match contract.buffer {
            ReceiveBuffer::Fifo => None,
            ReceiveBuffer::Latest(depth) => Some(depth),
        };
        let core = Core::declare(
            self,
            &registration.transport_type,
            &sources,
            contract.replay == Replay::Last,
            latest,
        )?;
        let key = Key::input(
            self.domain(),
            self.endpoint_namespace(),
            name,
            &registration.transport_type,
        );
        let presence = self.engine.declare_alive(&key.with_chunk(SUB_CHUNK))?;
        let subscriber = RawSubscriber {
            core,
            expected_schema: Some(schema),
            _managed: Some(ManagedPort::new(self, live)),
            _presence: Some(presence),
        };
        ports.inputs.insert(name.to_owned(), registration);
        Ok(subscriber)
    }

    /// Publish encoded protobuf through a named output's prepared policy.
    pub fn raw_output(
        &self,
        name: &str,
        type_name: &str,
        schema: u64,
    ) -> Result<DeclaredPublisher> {
        let contract = self.named_contract(name, false, PortKind::Stream)?;
        let mut ports = self.lock_ports(name, false)?;
        let live = Arc::new(());
        let registration = registration(
            contract,
            type_name,
            schema,
            self.port_endpoint(name, false)?,
            &live,
        )?;
        let mut qos = match contract.qos {
            QosProfile::Default => Qos::DEFAULT,
            QosProfile::Command => Qos::COMMAND,
            QosProfile::Sensor => Qos::SENSOR,
        };
        let retained = contract.retention == Retention::Last;
        qos.durability = if retained {
            Durability::TransientLocal
        } else {
            Durability::Volatile
        };
        let key = Key::output(
            self.domain(),
            self.endpoint_namespace(),
            name,
            &registration.transport_type,
        );
        let core = PublisherCore::declare(self, &key, &qos, schema)?;
        let publisher = DeclaredPublisher {
            core,
            _managed: ManagedPort::new(self, live),
        };
        ports.outputs.insert(name.to_owned(), registration);
        Ok(publisher)
    }
}
