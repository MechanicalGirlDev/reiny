//! Manifest-authoritative endpoints and the explicit managed readiness boundary.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use reiny_core::bindings::{
    EndpointContract, ModuleBindings, PortEndpoint, PortReport, ResponseReport,
};

use crate::engine::{Guard, Key, STOP_CHUNK};
use crate::{MessageType, ModuleContext, Result, Service, validate_segment};

mod admission;
pub(crate) use admission::{
    AdmissionError, HostedEvent, HostedOptions, PreparedAdmission, ReadyAdmission,
};
mod bridging;
pub(crate) mod config;
mod dynamic;
pub use dynamic::DeclaredPublisher;
mod owned;
mod ports;
mod report;
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
    admission: Option<Arc<()>>,
}

struct Registration {
    contract: PortReport,
    transport_type: String,
    live: Weak<()>,
}

pub(crate) struct ManagedPort {
    live: Option<Arc<()>>,
    ports: Weak<Mutex<Ports>>,
    shutdown: crate::shutdown::Shutdown,
}

impl ManagedPort {
    fn new(cloudy: &ModuleContext, live: Arc<()>) -> Self {
        Self {
            live: Some(live),
            ports: Arc::downgrade(&cloudy.module.ports),
            shutdown: cloudy.shutdown.clone(),
        }
    }
}

impl Drop for ManagedPort {
    fn drop(&mut self) {
        if let Some(ports) = self.ports.upgrade() {
            let mut ports = ports.lock().unwrap_or_else(PoisonError::into_inner);
            // A concurrent preparation must not see this handle after its drop unlocks.
            drop(self.live.take());
            let admitted = ports.admission.take().is_some();
            let ready = ports.ready.take().is_some();
            if admitted {
                self.shutdown.hosted_event(HostedEvent::ReadinessLost);
            }
            if admitted || ready {
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
            self.install_stop_handler()?;
        }
        self.module.bindings = bindings;
        self.module.report_path = report_path;
        self.module.bundle_dir = std::env::var_os(BUNDLE_DIR_ENV).map(PathBuf::from);
        self.module.config_dir = std::env::var_os(CONFIG_DIR_ENV).map(PathBuf::from);
        Ok(())
    }

    pub(crate) fn install_stop_handler(&mut self) -> Result<()> {
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
        Ok(())
    }

    pub(crate) fn endpoint_namespace(&self) -> &str {
        self.module
            .bindings
            .as_ref()
            .map_or(&self.id, |bindings| &bindings.endpoint_namespace)
    }
}
