//! Context-bound admission leases; all transitions share the port registry lock.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use reiny_core::bindings::{ModuleBindings, ModuleReport};
use tokio::sync::watch;

use super::Ports;
use crate::ModuleContext;
use crate::engine::{Key, READY_CHUNK};

pub(crate) struct HostedOptions {
    pub domain: String,
    pub bindings: ModuleBindings,
    pub config: Option<toml::Table>,
    pub engine: Arc<dyn crate::engine::Engine>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum HostedEvent {
    StopRequested,
    ReadinessLost,
}

#[derive(Debug)]
pub(crate) enum AdmissionError {
    InvalidIdentity(String),
    InvalidBindings(String),
    UnsupportedHostedBindings,
    MissingBindings,
    ShuttingDown,
    AlreadyPrepared,
    AlreadyReady,
    PortUnavailable { input: bool, name: String },
    NoLiveliness,
    ForeignTicket,
    StaleTicket,
    Transport(String),
}

impl fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIdentity(detail) => write!(f, "invalid hosted identity: {detail}"),
            Self::InvalidBindings(detail) => write!(f, "invalid hosted bindings: {detail}"),
            Self::UnsupportedHostedBindings => {
                f.write_str("hosted children and executables are unsupported")
            }
            Self::MissingBindings => f.write_str("module bindings are required"),
            Self::ShuttingDown => f.write_str("module is shutting down"),
            Self::AlreadyPrepared => f.write_str("module admission is already prepared"),
            Self::AlreadyReady => f.write_str("module is already ready"),
            Self::PortUnavailable { input, name } => write!(
                f,
                "declared {} port '{name}' has no live handle",
                if *input { "input" } else { "output" }
            ),
            Self::NoLiveliness => f.write_str("admission requires liveliness"),
            Self::ForeignTicket => f.write_str("admission ticket belongs to another context"),
            Self::StaleTicket => f.write_str("admission ticket is no longer current"),
            Self::Transport(detail) => write!(f, "admission transport: {detail}"),
        }
    }
}

impl std::error::Error for AdmissionError {}

pub(crate) struct PreparedAdmission {
    ports: Weak<Mutex<Ports>>,
    generation: Arc<()>,
    report: ModuleReport,
}

impl PreparedAdmission {
    pub(crate) fn report(&self) -> &ModuleReport {
        &self.report
    }
}

impl Drop for PreparedAdmission {
    fn drop(&mut self) {
        if let Some(ports) = self.ports.upgrade() {
            let mut ports = ports.lock().unwrap_or_else(PoisonError::into_inner);
            if ports
                .admission
                .as_ref()
                .is_some_and(|generation| Arc::ptr_eq(generation, &self.generation))
            {
                ports.admission = None;
                ports.ready = None;
            }
        }
    }
}

pub(crate) struct ReadyAdmission {
    _prepared: PreparedAdmission,
}

impl ModuleContext {
    pub(crate) fn attach_step_gate(&self, gate: Arc<crate::component::step::control::Gate>) {
        self.shutdown.attach_step_gate(gate);
    }

    pub(crate) fn hosted_events(&self) -> watch::Receiver<Option<HostedEvent>> {
        self.shutdown.hosted_events()
    }

    pub(crate) fn prepare_admission(
        &self,
    ) -> std::result::Result<super::PreparedAdmission, AdmissionError> {
        let mut ports = self
            .module
            .ports
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let bindings = self
            .module
            .bindings
            .as_ref()
            .ok_or(AdmissionError::MissingBindings)?;
        if !bindings.children.is_empty() || !bindings.executables.is_empty() {
            return Err(AdmissionError::UnsupportedHostedBindings);
        }
        if self.shutdown.is_triggered() {
            return Err(AdmissionError::ShuttingDown);
        }
        if ports.ready.is_some() {
            return Err(AdmissionError::AlreadyReady);
        }
        if ports.admission.is_some() {
            return Err(AdmissionError::AlreadyPrepared);
        }
        if !self.engine.caps().liveliness {
            return Err(AdmissionError::NoLiveliness);
        }
        let report = self.build_report(&ports).map_err(|error| {
            error
                .downcast::<AdmissionError>()
                .unwrap_or_else(|error| AdmissionError::Transport(error.to_string()))
        })?;
        let generation = Arc::new(());
        ports.admission = Some(Arc::clone(&generation));
        Ok(PreparedAdmission {
            ports: Arc::downgrade(&self.module.ports),
            generation,
            report,
        })
    }

    pub(crate) fn commit_admission(
        &self,
        prepared: PreparedAdmission,
    ) -> std::result::Result<super::ReadyAdmission, AdmissionError> {
        if !Weak::ptr_eq(&prepared.ports, &Arc::downgrade(&self.module.ports)) {
            return Err(AdmissionError::ForeignTicket);
        }
        let mut ports = self
            .module
            .ports
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if !ports
            .admission
            .as_ref()
            .is_some_and(|generation| Arc::ptr_eq(generation, &prepared.generation))
        {
            return Err(AdmissionError::StaleTicket);
        }
        if self.shutdown.is_triggered() {
            return Err(AdmissionError::ShuttingDown);
        }
        let report = self.build_report(&ports).map_err(|error| {
            error
                .downcast::<AdmissionError>()
                .unwrap_or_else(|error| AdmissionError::Transport(error.to_string()))
        })?;
        if report != prepared.report {
            return Err(AdmissionError::StaleTicket);
        }
        ports.ready = Some(
            self.engine
                .declare_alive(&Key::module(&self.domain, Some(&self.id), READY_CHUNK))
                .map_err(|error| AdmissionError::Transport(error.to_string()))?,
        );
        drop(ports);
        Ok(ReadyAdmission {
            _prepared: prepared,
        })
    }
}
