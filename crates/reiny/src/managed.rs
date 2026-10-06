//! Named ports and the explicit managed readiness boundary.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use reiny_core::bindings::{ModuleBindings, ModuleReport, PortReport};

use crate::engine::{Guard, Key, READY_CHUNK, STOP_CHUNK};
use crate::{Cloudy, Publisher, Result, Subscriber, Topic, validate_segment};

pub(crate) mod config;
#[cfg(test)]
mod tests;

/// Optional absolute JSON report destination supplied by the process owner.
pub const MODULE_REPORT_ENV: &str = "REINY_MODULE_REPORT";

#[derive(Default)]
pub(crate) struct ModuleRuntime {
    pub(crate) bindings: Option<ModuleBindings>,
    pub(crate) report_path: Option<PathBuf>,
    ports: Mutex<Ports>,
    stop: Option<Guard>,
}

#[derive(Default)]
struct Ports {
    inputs: BTreeMap<String, Registration>,
    outputs: BTreeMap<String, Registration>,
    ready: Option<Guard>,
}

struct Registration {
    contract: PortReport,
    transport_type: &'static str,
    live: Weak<()>,
}

impl Registration {
    fn of<T: Topic>(declared: &str, live: &Arc<()>) -> Result<Self> {
        validate_segment("transport type", T::TYPE)?;
        let actual = T::DESCRIPTOR.map_or(T::TYPE, |descriptor| descriptor.message);
        anyhow::ensure!(
            declared == T::TYPE || declared == actual,
            "declared port type '{declared}' differs from actual type '{actual}'"
        );
        let schema = T::SCHEMA.ok_or_else(|| {
            anyhow::anyhow!("managed port type '{actual}' has no schema fingerprint")
        })?;
        Ok(Self {
            contract: PortReport {
                type_name: actual.to_string(),
                schema,
            },
            transport_type: T::TYPE,
            live: Arc::downgrade(live),
        })
    }
}

impl Cloudy {
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
            let key = Key::topic(&self.domain, Some(&self.id), STOP_CHUNK);
            let reply_key = key.clone();
            let shutdown = self.shutdown.clone();
            self.module.stop = Some(self.engine.respond(
                &key,
                Box::new(move |query| {
                    shutdown.trigger();
                    // This acknowledges receipt, not process exit or resource cleanup.
                    if let Err(error) = query.reply(&reply_key, b"ack".to_vec(), None) {
                        tracing::warn!(key = %reply_key, %error, "stop acknowledgement failed");
                    }
                }),
            )?);
        }
        self.module.bindings = bindings;
        self.module.report_path = report_path;
        Ok(())
    }

    /// Open a declared input from exactly its configured executable namespace.
    ///
    /// Fails without bindings, for an undefined or repeated port, or when the compiled type cannot
    /// verify the declared contract. Unstamped and mismatching samples are never delivered.
    pub fn input<T: prost::Message + Default + Topic>(&self, name: &str) -> Result<Subscriber<T>> {
        let bindings = self
            .module
            .bindings
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("named input '{name}' requires module bindings"))?;
        let input = bindings
            .inputs
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("undefined input port '{name}'"))?;
        let mut ports = self
            .module
            .ports
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        anyhow::ensure!(!self.shutdown.is_triggered(), "module is shutting down");
        anyhow::ensure!(ports.ready.is_none(), "cannot create a port after ready()");
        anyhow::ensure!(
            !ports.inputs.contains_key(name),
            "input port '{name}' already created"
        );
        let live = Arc::new(());
        let registration = Registration::of::<T>(&input.type_name, &live)?;
        let mut subscriber = self.subscriber::<T>().from(&input.source).build()?;
        subscriber.managed = Some(live);
        ports.inputs.insert(name.to_string(), registration);
        Ok(subscriber)
    }

    /// Open a declared output in this module's namespace.
    ///
    /// Fails without bindings, for an undefined or repeated port, unverified type, or when another
    /// output already uses the same transport type within this executable.
    pub fn output<T: prost::Message + Topic>(&self, name: &str) -> Result<Publisher<T>> {
        let bindings = self
            .module
            .bindings
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("named output '{name}' requires module bindings"))?;
        let output = bindings
            .outputs
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("undefined output port '{name}'"))?;
        let mut ports = self
            .module
            .ports
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        anyhow::ensure!(!self.shutdown.is_triggered(), "module is shutting down");
        anyhow::ensure!(ports.ready.is_none(), "cannot create a port after ready()");
        anyhow::ensure!(
            !ports.outputs.contains_key(name),
            "output port '{name}' already created"
        );
        anyhow::ensure!(
            ports
                .outputs
                .values()
                .all(|port| port.transport_type != T::TYPE),
            "duplicate output transport type '{}'",
            T::TYPE
        );
        let live = Arc::new(());
        let registration = Registration::of::<T>(&output.type_name, &live)?;
        let mut publisher = self.publish::<T>()?;
        publisher.managed = Some(live);
        ports.outputs.insert(name.to_string(), registration);
        Ok(publisher)
    }

    /// Publish `@ready` after every declared port has a live typed handle.
    ///
    /// Writes the optional atomic runtime report before raising readiness. Repeated calls are
    /// idempotent. The token lives with this `Cloudy`; a stop acknowledgement does not remove it.
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
        if let Some(bindings) = &self.module.bindings {
            for (name, port) in bindings
                .inputs
                .keys()
                .map(|name| (name, ports.inputs.get(name)))
                .chain(
                    bindings
                        .outputs
                        .keys()
                        .map(|name| (name, ports.outputs.get(name))),
                )
            {
                anyhow::ensure!(
                    port.is_some_and(|port| port.live.strong_count() > 0),
                    "declared port '{name}' has no live handle"
                );
            }
        }
        if let Some(path) = &self.module.report_path {
            let report = ModuleReport {
                version: 1,
                namespace: self.id.clone(),
                inputs: ports
                    .inputs
                    .iter()
                    .map(|(name, port)| (name.clone(), port.contract.clone()))
                    .collect(),
                outputs: ports
                    .outputs
                    .iter()
                    .map(|(name, port)| (name.clone(), port.contract.clone()))
                    .collect(),
            };
            config::write_report(path, &report)?;
        }
        ports.ready = Some(self.engine.declare_alive(&Key::topic(
            &self.domain,
            Some(&self.id),
            READY_CHUNK,
        ))?);
        Ok(())
    }
}
