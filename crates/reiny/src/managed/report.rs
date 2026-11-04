//! Shared live-handle reports and legacy readiness publication.

use std::collections::BTreeMap;
use std::sync::PoisonError;

use reiny_core::bindings::{MODULE_REPORT_VERSION, ModuleReport};

use super::{AdmissionError, Ports, config};
use crate::engine::{Key, READY_CHUNK};
use crate::{ModuleContext, Result};

impl ModuleContext {
    /// Write the compiled report and announce readiness after all ports and owned children are live.
    ///
    /// Every declared child must have completed [`super::OwnedChild::wait_ready`]. Losing a child's
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
        anyhow::ensure!(ports.admission.is_none(), "module admission is prepared");
        anyhow::ensure!(self.engine.caps().liveliness, "ready() requires liveliness");
        let report = self.build_report(&ports)?;
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

    pub(super) fn build_report(&self, ports: &Ports) -> Result<ModuleReport> {
        let mut report = ModuleReport {
            version: MODULE_REPORT_VERSION,
            namespace: self.id.clone(),
            endpoint_namespace: self.endpoint_namespace().to_owned(),
            fingerprint_version: reiny_core::SCHEMA_FINGERPRINT_VERSION,
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
        };
        for (input, registered, reported) in [
            (true, &ports.inputs, &mut report.inputs),
            (false, &ports.outputs, &mut report.outputs),
        ] {
            for (name, port) in registered {
                if port.live.strong_count() == 0 {
                    return Err(AdmissionError::PortUnavailable {
                        input,
                        name: name.clone(),
                    }
                    .into());
                }
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
            for (input, declared, reported) in [
                (
                    true,
                    bindings.inputs.keys().collect::<Vec<_>>(),
                    &report.inputs,
                ),
                (
                    false,
                    bindings.outputs.keys().collect::<Vec<_>>(),
                    &report.outputs,
                ),
            ] {
                for name in declared {
                    if !reported.contains_key(name) {
                        return Err(AdmissionError::PortUnavailable {
                            input,
                            name: name.clone(),
                        }
                        .into());
                    }
                }
            }
        }
        Ok(report)
    }
}
