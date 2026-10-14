//! Registration of byte-oriented RPC routes implemented by the named bridge.

use super::{ManagedPort, Registration};
use crate::bindings::{PortKind, PortReport, ResponseReport};
use crate::{ModuleContext, Result, validate_segment};
use std::sync::Arc;

impl ModuleContext {
    pub(crate) fn bridge_rpc_port(
        &self,
        name: &str,
        input: bool,
        schema: u64,
        response_schema: u64,
    ) -> Result<ManagedPort> {
        let contract = self.named_contract(name, input, PortKind::Rpc)?;
        let mut ports = self.lock_ports(name, input)?;
        let transport_type = contract.type_name.as_str();
        validate_segment("transport type", transport_type)?;
        let live = Arc::new(());
        let registration = Registration {
            contract: PortReport {
                type_name: contract.type_name.clone(),
                schema,
                endpoint: self.port_endpoint(name, input)?,
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
