use super::{
    Arc, Bytes, CLOSED, Error, Factory, Host, INVALID, Lanes, Object, Reservation, Result, STATE,
    UNSUPPORTED, abi, boundary, budget, callbacks, export, failure, insert, lock, object, ordinary,
    runtime, variant, wait, wrong,
};
use abi::output;
use reiny::component::{
    ComponentHost, ComponentInstanceSpec, ComponentRegistry, HostOptions, HostOutcome, HostPhase,
};
use serde::Deserialize;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Graph {
    version: u32,
    pub(super) domain: String,
    pub(super) startup_timeout_ns: u64,
    pub(super) stop_timeout_ns: u64,
    pub(super) instances: Vec<Instance>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Instance {
    pub(super) factory_id: String,
    pub(super) bindings: reiny::bindings::ModuleBindings,
    pub(super) config: Option<toml::Table>,
    pub(super) config_origin: Option<std::path::PathBuf>,
}

fn factories(id: u64) -> Result<std::collections::BTreeMap<String, Arc<Factory>>> {
    let object = object(id)?;
    let registry = lock(variant!(object, Registry))?;
    if !registry.sealed {
        return Err(Error::new(STATE, "registry must be sealed"));
    }
    if registry
        .factories
        .values()
        .any(|factory| factory.step.is_some())
    {
        return Err(Error::new(
            UNSUPPORTED,
            "step factories require Local step launch",
        ));
    }
    Ok(registry.factories.clone())
}

pub(super) fn phase(phase: HostPhase) -> u32 {
    match phase {
        HostPhase::Starting => 1,
        HostPhase::Prepared => 2,
        HostPhase::Committed => 3,
        HostPhase::Running => 4,
        HostPhase::Stopping => 5,
        HostPhase::Stopped => 6,
        HostPhase::Failed => 7,
        HostPhase::Unresponsive => 8,
    }
}

fn snapshot(status: reiny::component::HostStatus) -> Result<Vec<u8>> {
    let components: serde_json::Map<String, serde_json::Value> = status.components.into_iter().map(|(id, value)| {
        (id, serde_json::json!({"phase": format!("{:?}", value.phase), "error": value.error.map(|e| e.to_string())}))
    }).collect();
    serde_json::to_vec(
        &serde_json::json!({"version": 1, "phase": phase(status.phase), "components": components}),
    )
    .map_err(failure)
}

fn outcome(value: HostOutcome) -> Result<Vec<u8>> {
    let value = match value {
        HostOutcome::Stopped => {
            serde_json::json!({"version": 1, "phase": 6, "primary": null, "cleanup": [], "unjoined": []})
        }
        HostOutcome::Failed { primary, cleanup } => {
            serde_json::json!({"version": 1, "phase": 7, "primary": primary.to_string(),
            "cleanup": cleanup.into_iter().map(|e| e.to_string()).collect::<Vec<_>>(), "unjoined": []})
        }
        HostOutcome::Unresponsive {
            primary,
            cleanup,
            instances,
        } => serde_json::json!({"version": 1, "phase": 8,
            "primary": primary.map(|e| e.to_string()), "cleanup": cleanup.into_iter().map(|e| e.to_string()).collect::<Vec<_>>(), "unjoined": instances}),
    };
    serde_json::to_vec(&value).map_err(failure)
}

export!(reiny_exec_graph_import(json: Bytes, out: *mut u64) {
    output(out)?;
    let graph: Graph = reiny::component::process::decode_payload(&json.bytes()?)
        .map_err(|e| Error::new(INVALID, e.to_string()))?;
    if graph.version != 1 { return Err(Error::new(UNSUPPORTED, "unsupported graph version")); }
    budget(graph.startup_timeout_ns)?;
    budget(graph.stop_timeout_ns)?;
    if graph.instances.iter().any(|i| i.config_origin.as_ref().is_some_and(|p| !p.is_absolute() || p.as_os_str().as_encoded_bytes().contains(&0))) {
        return Err(Error::new(INVALID, "config origin must be absolute and contain no NUL"));
    }
    out.write(insert(Object::Graph(graph), false)?);
    Ok(())
});

export!(reiny_exec_host_launch_local(registry: u64, graph: u64, out: *mut u64) {
    output(out)?;
    ordinary()?;
    let factories = factories(registry)?;
    let graph_object = object(graph)?;
    let graph = variant!(graph_object, Graph);
    let reservation = Reservation::new()?;
    let lanes = Arc::new(Lanes::default());
    let mut registry = ComponentRegistry::new();
    for (id, factory) in factories {
        registry.register(id, Arc::new(callbacks::Registered::new(factory, lanes.clone())?)).map_err(failure)?;
    }
    let options = HostOptions {
        domain: graph.domain.clone(),
        engine: Arc::new(reiny::engine::Local::new()),
        startup_timeout: budget(graph.startup_timeout_ns)?,
        stop_timeout: budget(graph.stop_timeout_ns)?,
    };
    let instances = graph.instances.iter().map(|i| ComponentInstanceSpec {
        factory_id: i.factory_id.clone(), bindings: i.bindings.clone(), config: i.config.clone(), config_origin: i.config_origin.clone()
    }).collect();
    let handle = {
        let _entered = runtime()?.enter();
        ComponentHost::launch(Arc::new(registry), options, instances).map_err(failure)?
    };
    out.write(reservation.publish(Object::Host(Host { handle: Arc::new(handle), lanes }))?);
    Ok(())
});

#[unsafe(no_mangle)]
pub(crate) extern "C" fn reiny_exec_host_request_stop(handle: u64) -> i32 {
    boundary(|| {
        let object = object(handle)?;
        let host = variant!(object, Host).handle.clone();
        runtime()?.spawn(async move {
            host.stop().await;
        });
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub(crate) extern "C" fn reiny_exec_host_wait_running(handle: u64, timeout_ns: u64) -> i32 {
    boundary(|| {
        let object = object(handle)?;
        let host = variant!(object, Host);
        wait(timeout_ns, async {
            host.handle.wait_running().await.map_err(failure)
        })
    })
}

#[unsafe(no_mangle)]
pub(crate) extern "C" fn reiny_exec_host_wait_joined(handle: u64, timeout_ns: u64) -> i32 {
    boundary(|| {
        let object = object(handle)?;
        let host = variant!(object, Host);
        wait(timeout_ns, async {
            host.handle.wait_terminal().await;
            host.lanes.joined().await
        })
    })
}

export!(reiny_exec_host_phase(handle: u64, out: *mut u32) {
    output(out)?;
    let object = object(handle)?;
    out.write(phase(variant!(object, Host).handle.status().phase));
    Ok(())
});

export!(reiny_exec_host_snapshot(handle: u64, out: *mut u64) {
    output(out)?;
    let object = object(handle)?;
    out.write(insert(Object::Buffer(snapshot(variant!(object, Host).handle.status())?), false)?);
    Ok(())
});

export!(reiny_exec_host_subscribe(handle: u64, out: *mut u64) {
    output(out)?;
    let object = object(handle)?;
    out.write(insert(Object::Observer(tokio::sync::Mutex::new(variant!(object, Host).handle.subscribe())), false)?);
    Ok(())
});

export!(reiny_exec_host_wait_terminal(handle: u64, timeout_ns: u64, out: *mut u64) {
    output(out)?;
    let object = object(handle)?;
    let host = variant!(object, Host);
    let reservation = Reservation::new()?;
    let result = wait(timeout_ns, async { Ok(host.handle.wait_terminal().await) })?;
    out.write(reservation.publish(Object::Buffer(outcome(result)?))?);
    Ok(())
});

export!(reiny_exec_observer_next(handle: u64, timeout_ns: u64, out: *mut u64) {
    output(out)?;
    let object = object(handle)?;
    let observer = variant!(object, Observer);
    let reservation = Reservation::new()?;
    let status = wait(timeout_ns, async {
        let mut observer = observer.lock().await;
        observer.changed().await.map_err(|_| Error::new(CLOSED, "status observer closed"))?;
        let status = observer.borrow_and_update().clone();
        Ok(status)
    })?;
    out.write(reservation.publish(Object::Buffer(snapshot(status)?))?);
    Ok(())
});

export!(reiny_exec_process_host_run(registry: u64, startup_ns: u64, stop_ns: u64, frame_ns: u64, max_instances: usize, out: *mut u64) {
    output(out)?;
    ordinary()?;
    let factories = factories(registry)?;
    let reservation = Reservation::new()?;
    let lanes = Arc::new(Lanes::default());
    let mut registry = reiny::component::process::ProcessRegistry::new();
    for (id, factory) in factories {
        registry.register(reiny::component::process::protocol::FactoryRegistration {
            factory: id, revision: factory.revision.clone(),
        }, Arc::new(callbacks::Registered::new(factory, lanes.clone())?)).map_err(failure)?;
    }
    let limits = reiny::component::process::ProcessLimits {
        startup: budget(startup_ns)?, shutdown: budget(stop_ns)?, frame: budget(frame_ns)?, instances: max_instances,
    };
    let bootstrap = reiny::component::process::Bootstrap::read(&mut std::io::stdin().lock()).map_err(failure)?;
    let outcome = reiny::component::process::ProcessHost::run(bootstrap, registry, limits).map_err(failure)?;
    out.write(reservation.publish(Object::Buffer(serde_json::to_vec(&outcome).map_err(failure)?))?);
    Ok(())
});
