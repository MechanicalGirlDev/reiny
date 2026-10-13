//! Registration of byte-oriented RPC routes implemented by the named bridge.

use super::{ManagedPort, Registration};
use crate::bindings::{PortKind, PortReport, ResponseReport};
use crate::{Cloudy, Result, validate_segment};
use std::sync::Arc;

impl Cloudy {
    pub(crate) fn bridge_rpc_port(
        &self,
        name: &str,
        input: bool,
        schema: u64,
        response_schema: u64,
    ) -> Result<ManagedPort> {
        let contract = self.named_contract(name, input, PortKind::Rpc)?;
        let mut ports = self.lock_ports(name, input)?;
        let transport_type = contract
            .type_name
            .rsplit('.')
            .next()
            .unwrap_or(&contract.type_name);
        validate_segment("transport type", transport_type)?;
        if !input {
            anyhow::ensure!(
                ports
                    .outputs
                    .values()
                    .all(|port| port.transport_type != transport_type),
                "duplicate output transport type '{transport_type}'"
            );
        }
        let live = Arc::new(());
        let registration = Registration {
            contract: PortReport {
                type_name: contract.type_name.clone(),
                schema,
                contract: contract.clone(),
                response: Some(ResponseReport {
                    type_name: contract
                        .response
                        .clone()
                        .ok_or_else(|| anyhow::anyhow!("RPC requires response type"))?,
                    schema: response_schema,
                }),
            },
            transport_type: transport_type.to_string(),
            live: Arc::downgrade(&live),
        };
        let registrations = if input {
            &mut ports.inputs
        } else {
            &mut ports.outputs
        };
        registrations.insert(name.to_string(), registration);
        Ok(ManagedPort::new(self, live))
    }
}
