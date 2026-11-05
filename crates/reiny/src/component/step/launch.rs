use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;

use super::{
    StepHandle, StepLaunchError,
    control::Gate,
    driver::Driver,
    worker::{Executor, Factory},
};
use crate::component::{LocalHostOptions, options::HostConfiguration};
use crate::{
    component::{ComponentHost, ComponentInstanceSpec, ComponentRegistry, HostHandle, HostOptions},
    step::{StepGraph, StepScheduler},
};

impl ComponentHost {
    /// Launch a local managed graph with explicit, immutable step routes.
    ///
    /// Service components may coexist but cannot be named as step nodes.
    pub fn launch_with_steps(
        registry: Arc<ComponentRegistry>,
        options: HostOptions,
        instances: Vec<ComponentInstanceSpec>,
        graph: StepGraph,
        initial_time: Duration,
    ) -> Result<(HostHandle, StepHandle), StepLaunchError> {
        Self::launch_steps(registry, options.into(), instances, graph, initial_time)
    }

    /// Launch Local managed steps with optional startup and joined-stop budgets.
    ///
    /// Step budgets are chosen separately with `StepHandle::try_step_with_timeout`.
    pub fn launch_local_with_steps(
        registry: Arc<ComponentRegistry>,
        options: LocalHostOptions,
        instances: Vec<ComponentInstanceSpec>,
        graph: StepGraph,
        initial_time: Duration,
    ) -> Result<(HostHandle, StepHandle), StepLaunchError> {
        Self::launch_steps(registry, options.into(), instances, graph, initial_time)
    }

    // These are the existing launch inputs: registry, lifecycle resources,
    // instances, immutable step routing and initial logical time.
    fn launch_steps(
        registry: Arc<ComponentRegistry>,
        options: HostConfiguration,
        mut instances: Vec<ComponentInstanceSpec>,
        graph: StepGraph,
        initial_time: Duration,
    ) -> Result<(HostHandle, StepHandle), StepLaunchError> {
        if !options.engine.as_any().is::<crate::engine::Local>() {
            return Err(StepLaunchError::UnsupportedEngine);
        }
        for (id, schema) in &graph.nodes {
            let instance = instances
                .iter()
                .find(|instance| &instance.bindings.namespace == id)
                .ok_or_else(|| StepLaunchError::NodeSetMismatch {
                    instance: id.clone(),
                })?;
            let (registered, _) = registry
                .step_factories
                .get(&instance.factory_id)
                .ok_or_else(|| StepLaunchError::WrongFactoryKind {
                    instance: id.clone(),
                })?;
            if schema != registered {
                return Err(StepLaunchError::SchemaMismatch {
                    instance: id.clone(),
                });
            }
        }
        for instance in &instances {
            if registry.step_factories.contains_key(&instance.factory_id)
                && !graph.nodes.contains_key(&instance.bindings.namespace)
            {
                return Err(StepLaunchError::NodeSetMismatch {
                    instance: instance.bindings.namespace.clone(),
                });
            }
        }
        let scheduler = StepScheduler::new(graph, initial_time).map_err(StepLaunchError::Plan)?;
        let gate = Gate::new(scheduler.clock());
        let (requests, received) = mpsc::unbounded_channel();
        let step_handle = StepHandle {
            gate: gate.clone(),
            requests,
            plan: scheduler.plan.clone(),
            local: true,
        };
        let mut adapted = ComponentRegistry::new();
        let mut calls = BTreeMap::new();
        // Private registry entries are per instance, including repeated factories.
        // Namespace identifiers cannot collide with each other after host validation.
        for instance in &mut instances {
            let id = instance.bindings.namespace.clone();
            if let Some((_, factory)) = registry.step_factories.get(&instance.factory_id) {
                let (sender, calls_rx) = mpsc::unbounded_channel();
                adapted
                    .register(
                        id.clone(),
                        Arc::new(Factory {
                            implementation: factory.clone(),
                            calls: Mutex::new(Some(calls_rx)),
                            prepared: None,
                        }),
                    )
                    .map_err(StepLaunchError::Host)?;
                calls.insert(id.clone(), sender);
            } else if let Some(factory) = registry.factories.get(&instance.factory_id) {
                adapted
                    .register(id.clone(), factory.clone())
                    .map_err(StepLaunchError::Host)?;
            } else {
                return Err(StepLaunchError::Host(super::super::HostError::new(
                    super::super::HostErrorCode::UnknownFactory,
                    Some(&id),
                    &instance.factory_id,
                )));
            }
            instance.factory_id = id;
        }
        drop(registry);
        let bindings = instances
            .iter()
            .map(|instance| instance.bindings.clone())
            .collect();
        let driver = Driver {
            scheduler,
            executor: Box::new(Executor {
                calls,
                failure: None,
            }),
            requests: received,
            gate: gate.clone(),
        };
        let (handle, control) =
            Self::prepare_steps(Arc::new(adapted), options, instances, Some(driver))
                .map_err(StepLaunchError::Host)?;
        tokio::spawn(control.admit_local(bindings));
        Ok((handle, step_handle))
    }
}
