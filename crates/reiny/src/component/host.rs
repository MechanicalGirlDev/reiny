use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use crate::managed::HostedOptions;

use super::coordinator::Coordinator;
use super::staged::HostControl;
use super::worker::{Command, Worker};
use super::{
    ComponentInstanceSpec, ComponentPhase, ComponentRegistry, ComponentStatus, HostError,
    HostErrorCode, HostHandle, HostOptions, HostPhase, HostStatus,
};

/// Launches a closed graph from explicitly registered in-process factories.
pub struct ComponentHost;

impl ComponentHost {
    /// Validate static inputs, then immediately return a supervised group handle.
    ///
    /// No component is constructed for a synchronously rejected launch. Startup
    /// and stop deadlines require a Tokio runtime with its time driver enabled.
    pub fn launch(
        registry: Arc<ComponentRegistry>,
        options: HostOptions,
        instances: Vec<ComponentInstanceSpec>,
    ) -> Result<HostHandle, HostError> {
        let bindings = instances
            .iter()
            .map(|instance| instance.bindings.clone())
            .collect();
        let (handle, control) = Self::prepare(registry, options, instances)?;
        tokio::spawn(control.admit_local(bindings));
        Ok(handle)
    }

    /// Start the private staged protocol. The trusted controller, not application
    /// components, owns graph validation and the deployment-wide commit barrier.
    pub(super) fn prepare(
        registry: Arc<ComponentRegistry>,
        options: HostOptions,
        instances: Vec<ComponentInstanceSpec>,
    ) -> Result<(HostHandle, HostControl), HostError> {
        Self::prepare_steps(registry, options, instances, None)
    }

    pub(super) fn prepare_steps(
        registry: Arc<ComponentRegistry>,
        options: HostOptions,
        instances: Vec<ComponentInstanceSpec>,
        driver: Option<super::step::driver::Driver>,
    ) -> Result<(HostHandle, HostControl), HostError> {
        validate(&registry, &options, &instances)?;
        let step_gate = driver.as_ref().map(|driver| driver.gate.clone());
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|error| HostError::new(HostErrorCode::NoRuntime, None, error.to_string()))?;
        let initial = HostStatus {
            phase: HostPhase::Starting,
            components: instances
                .iter()
                .map(|instance| {
                    (
                        instance.bindings.namespace.clone(),
                        ComponentStatus {
                            phase: ComponentPhase::Starting,
                            error: None,
                        },
                    )
                })
                .collect(),
        };
        let (status, status_rx) = watch::channel(initial);
        let (stop, stop_rx) = watch::channel(false);
        let (terminal, terminal_rx) = watch::channel(None);
        let (control, control_rx) = mpsc::channel(1);
        let (reports, reports_rx) = watch::channel(None);
        let (commands, command_rx) = watch::channel(Command::Prepare);
        // At most Opened, Prepared and Failure per component; bounded by graph size.
        let (events, event_rx) = mpsc::channel(instances.len().saturating_mul(3));
        let mut tasks = JoinSet::new();
        let mut pending = BTreeMap::new();
        let mut bindings = Vec::with_capacity(instances.len());
        for instance in instances {
            let id = instance.bindings.namespace.clone();
            let factory = registry
                .factories
                .get(&instance.factory_id)
                .ok_or_else(|| {
                    HostError::new(
                        HostErrorCode::UnknownFactory,
                        Some(&id),
                        &instance.factory_id,
                    )
                })?;
            bindings.push(instance.bindings.clone());
            let task = tasks.spawn_on(
                Worker {
                    factory: factory.clone(),
                    config_origin: instance.config_origin,
                    options: HostedOptions {
                        domain: options.domain.clone(),
                        bindings: instance.bindings,
                        config: instance.config,
                        engine: options.engine.clone(),
                    },
                    commands: command_rx.clone(),
                    events: events.clone(),
                    status: status.clone(),
                    step_gate: step_gate.clone(),
                }
                .run(),
                &runtime,
            );
            pending.insert(task.id(), id);
        }
        drop(registry);
        drop(events);
        let step_driver =
            driver.map(|driver| runtime.spawn(driver.run(stop.clone(), control.clone())));
        let coordinator = Coordinator {
            status,
            stop: stop_rx,
            terminal,
            commands,
            events: event_rx,
            tasks,
            pending,
            bindings,
            contexts: BTreeMap::new(),
            prepared: BTreeMap::new(),
            ready: Vec::new(),
            options,
            control: control_rx,
            reports,
            step_gate: step_gate.clone(),
            step_driver,
        };
        runtime.spawn(coordinator.run());
        Ok((
            HostHandle {
                status: status_rx,
                stop,
                terminal: terminal_rx.clone(),
                step_gate,
            },
            HostControl {
                requests: control,
                reports: reports_rx,
                terminal: terminal_rx,
            },
        ))
    }
}

fn validate(
    registry: &ComponentRegistry,
    options: &HostOptions,
    instances: &[ComponentInstanceSpec],
) -> Result<(), HostError> {
    if instances.is_empty() {
        return Err(HostError::new(
            HostErrorCode::EmptyGraph,
            None,
            "empty component graph",
        ));
    }
    crate::validate_segment("domain", &options.domain)
        .map_err(|error| HostError::new(HostErrorCode::InvalidOptions, None, error.to_string()))?;
    let now = std::time::Instant::now();
    if options.startup_timeout.is_zero()
        || options.stop_timeout.is_zero()
        || now.checked_add(options.startup_timeout).is_none()
        || now.checked_add(options.stop_timeout).is_none()
    {
        return Err(HostError::new(
            HostErrorCode::InvalidOptions,
            None,
            "deadlines must be positive and representable",
        ));
    }
    let mut ids = BTreeSet::new();
    for instance in instances {
        if instance
            .config_origin
            .as_ref()
            .is_some_and(|path| !path.is_absolute())
        {
            return Err(HostError::new(
                HostErrorCode::InvalidOptions,
                Some(&instance.bindings.namespace),
                "config origin must be absolute",
            ));
        }
        let bindings = &instance.bindings;
        let id = &bindings.namespace;
        if !ids.insert(id) {
            return Err(HostError::new(
                HostErrorCode::DuplicateInstance,
                Some(id),
                "duplicate namespace",
            ));
        }
        if !registry.factories.contains_key(&instance.factory_id) {
            return Err(HostError::new(
                HostErrorCode::UnknownFactory,
                Some(id),
                &instance.factory_id,
            ));
        }
        bindings.validate().map_err(|error| {
            HostError::new(HostErrorCode::InvalidBindings, Some(id), error.to_string())
        })?;
        if bindings.namespace != bindings.endpoint_namespace
            || !bindings.children.is_empty()
            || !bindings.executables.is_empty()
        {
            return Err(HostError::new(
                HostErrorCode::InvalidBindings,
                Some(id),
                "hosted instances must be top-level modules without children or executables",
            ));
        }
    }
    Ok(())
}
