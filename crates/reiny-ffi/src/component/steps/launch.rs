use super::{
    Arc, Bytes, Error, Factory, FactoryV1, Host, INVALID, Lanes, Object, Registration, Reservation,
    STATE, StepCallback, Time, abi, boundary, budget, callbacks, export, failure, lock, object,
    ordinary, output, runtime, variant, wrong,
};
use reiny::component::{ComponentHost, ComponentInstanceSpec, ComponentRegistry, HostOptions};
use std::ffi::c_void;

#[cfg(test)]
mod tests;

#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct StepFactoryV1 {
    struct_size: u32,
    abi_major: u32,
    flags: u32,
    reserved: u32,
    factory_data: *mut c_void,
    create: Option<abi::Create>,
    prepare: Option<abi::Hook>,
    step: Option<StepCallback>,
    stop: Option<abi::Stop>,
    destroy_instance: Option<abi::Destroy>,
    release_factory: Option<abi::Destroy>,
}

unsafe fn register(
    registry: u64,
    name: Bytes,
    revision: Bytes,
    schema: u64,
    table: *const StepFactoryV1,
    token: Option<u64>,
) -> super::Result<()> {
    if table.is_null() || !table.is_aligned() {
        return Err(Error::new(INVALID, "invalid step table"));
    }
    // SAFETY: [FFI/bounds] caller supplies a readable size prefix; null and
    // alignment were checked before accessing it.
    if usize::try_from(unsafe { table.cast::<u32>().read() }).ok()
        != Some(std::mem::size_of::<StepFactoryV1>())
    {
        return Err(Error::new(INVALID, "invalid step table size"));
    }
    // SAFETY: [FFI/bounds] exact size was checked; caller promises that many
    // initialized bytes in this live table allocation.
    let table = unsafe { table.read() };
    if table.abi_major != 1
        || table.flags != 0
        || table.reserved != 0
        || table.create.is_none()
        || table.prepare.is_none()
        || table.stop.is_none()
        || table.destroy_instance.is_none()
    {
        return Err(Error::new(
            INVALID,
            "invalid step table version, flags or hooks",
        ));
    }
    let callback = table
        .step
        .ok_or_else(|| Error::new(INVALID, "missing step callback"))?;
    // SAFETY: [bounds/lifetime] caller provides the header's valid input slices.
    let (name, revision) = unsafe { (name.text()?, revision.text()?) };
    if name.is_empty() || revision.is_empty() {
        return Err(Error::new(INVALID, "empty factory identity"));
    }
    let schema = object(schema)?;
    let schema = variant!(schema, StepSchema).clone();
    let released = if let Some(token) = token {
        if table.release_factory.is_some() {
            return Err(Error::new(
                INVALID,
                "notified registration forbids a foreign finalizer",
            ));
        }
        let value = object(token)?;
        Some(variant!(value, ReleaseToken).clone())
    } else {
        None
    };
    let registry = object(registry)?;
    let mut registry = lock(variant!(registry, Registry))?;
    if registry.sealed || registry.factories.contains_key(&name) {
        return Err(Error::new(STATE, "registry sealed or duplicate factory"));
    }
    if let Some(released) = &released {
        released
            .compare_exchange(
                0,
                1,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
            )
            .map_err(|_| Error::new(STATE, "factory release token already bound"))?;
    }
    let mut factory = Factory::new(
        FactoryV1 {
            struct_size: table.struct_size,
            abi_major: 1,
            flags: 0,
            mode: 2,
            factory_data: table.factory_data,
            create: table.create,
            prepare: table.prepare,
            run: None,
            stop: table.stop,
            destroy_instance: table.destroy_instance,
            release_factory: table.release_factory,
        },
        revision,
        released,
    );
    factory.step = Some(Registration { callback, schema });
    registry.factories.insert(name, Arc::new(factory));
    Ok(())
}

export!(reiny_step_register(registry: u64, name: Bytes, revision: Bytes, schema: u64, table: *const StepFactoryV1) {
    register(registry, name, revision, schema, table, None)
});

export!(reiny_step_register_notified(registry: u64, name: Bytes, revision: Bytes, schema: u64, table: *const StepFactoryV1, token: u64) {
    register(registry, name, revision, schema, table, Some(token))
});

export!(reiny_step_host_launch_local(registry: u64, graph: u64, steps: u64, initial: Time, host_out: *mut u64, step_out: *mut u64) {
    output(host_out)?;
    output(step_out)?;
    ordinary()?;
    let initial = initial.duration()?;
    let registry_object = object(registry)?;
    let factories = {
        let registry = lock(variant!(registry_object, Registry))?;
        if !registry.sealed { return Err(Error::new(STATE, "registry must be sealed")); }
        registry.factories.clone()
    };
    let graph_object = object(graph)?;
    let graph = variant!(graph_object, Graph);
    let steps = object(steps)?;
    let steps = variant!(steps, StepGraph).clone();
    let host_slot = Reservation::new()?;
    let step_slot = Reservation::new()?;
    let lanes = Arc::new(Lanes::default());
    let mut registry = ComponentRegistry::new();
    for (id, factory) in factories {
        let schema = factory.step.as_ref().map(|step| step.schema.clone());
        let registered = Arc::new(callbacks::Registered::new(factory, lanes.clone())?);
        match schema {
            Some(schema) => registry.register_step(id, schema, registered).map_err(failure)?,
            None => registry.register(id, registered).map_err(failure)?,
        }
    }
    let options = HostOptions {
        domain: graph.domain.clone(), engine: Arc::new(reiny::engine::Local::new()),
        startup_timeout: budget(graph.startup_timeout_ns)?, stop_timeout: budget(graph.stop_timeout_ns)?,
    };
    let instances = graph.instances.iter().map(|i| ComponentInstanceSpec {
        factory_id: i.factory_id.clone(), bindings: i.bindings.clone(),
        config: i.config.clone(), config_origin: i.config_origin.clone(),
    }).collect();
    let (handle, steps) = {
        let _entered = runtime()?.enter();
        ComponentHost::launch_with_steps(Arc::new(registry), options, instances, steps, initial).map_err(failure)?
    };
    let host = host_slot.publish(Object::Host(Host { handle: Arc::new(handle), lanes }))?;
    let steps = step_slot.publish(Object::StepHandle(steps))?;
    host_out.write(host);
    step_out.write(steps);
    Ok(())
});
