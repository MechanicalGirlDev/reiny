use super::{
    BTreeMap, BTreeSet, MODULE_MANIFEST_VERSION, ModuleCall, ModuleError, ModuleManifest,
    PortConnection, ProviderKind, ProviderSpec, ReceiveBuffer, Replay, invalid, validate_contract,
    validate_name, validate_port_names,
};

pub(super) fn validate_manifest(
    namespace: &str,
    manifest: &ModuleManifest,
    providers: &BTreeMap<String, ProviderSpec>,
    root: bool,
) -> Result<(), ModuleError> {
    if !root
        && (manifest.deployment.is_some()
            || manifest.domain.is_some()
            || !manifest.providers.is_empty()
            || manifest.component_hosts.is_some()
            || manifest.placement.is_some())
    {
        return Err(invalid(
            namespace,
            "deployment, domain, providers, component_hosts and placement belong to the root",
        ));
    }
    validate_definition(namespace, manifest)?;
    if let Some(run) = &manifest.run {
        run.validate(namespace)?;
        providers
            .get(&run.provider)
            .filter(|provider| provider.kind == ProviderKind::Process)
            .ok_or_else(|| {
                invalid(
                    namespace,
                    format!("unknown process provider '{}'", run.provider),
                )
            })?;
    }
    Ok(())
}

fn validate_calls(
    namespace: &str,
    calls: &BTreeMap<String, ModuleCall>,
) -> Result<(), ModuleError> {
    for (name, call) in calls {
        validate_name(name, "module")?;
        let child = format!("{namespace}/{name}");
        for port in call.inputs.keys().chain(call.outputs.keys()) {
            validate_name(port, "port")?;
        }
        for input in call.inputs.values() {
            if input.from.is_none() {
                return Err(invalid(&child, "caller inputs require from"));
            }
        }
        for output in call.outputs.values() {
            if output.from.is_some() {
                return Err(invalid(
                    &child,
                    "caller output contracts cannot contain from",
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn validate_definition(
    namespace: &str,
    manifest: &ModuleManifest,
) -> Result<(), ModuleError> {
    if manifest.version != MODULE_MANIFEST_VERSION && manifest.version != 4 {
        return Err(invalid(
            namespace,
            format!("unsupported module version {}", manifest.version),
        ));
    }
    if manifest.version == MODULE_MANIFEST_VERSION
        && (manifest.component.is_some()
            || manifest.component_hosts.is_some()
            || manifest.placement.is_some())
    {
        return Err(invalid(
            namespace,
            "component, component_hosts and placement require version 4",
        ));
    }
    if [
        manifest.run.is_some(),
        manifest.component.is_some(),
        !manifest.modules.is_empty(),
    ]
    .into_iter()
    .filter(|present| *present)
    .count()
        != 1
    {
        return Err(invalid(
            namespace,
            "module requires exactly one of run, component or nonempty modules",
        ));
    }
    if manifest.build.is_some() && manifest.run.is_none() {
        return Err(invalid(namespace, "build requires run"));
    }
    validate_calls(namespace, &manifest.modules)?;
    validate_port_names(&manifest.inputs)?;
    validate_port_names(&manifest.outputs)?;
    for port in manifest.inputs.values() {
        validate_contract(namespace, &port.contract(), true)?;
        if port.from.is_some() {
            return Err(invalid(
                namespace,
                "module inputs are supplied by their caller, not by from",
            ));
        }
    }
    if let Some(run) = &manifest.run {
        run.validate_names(namespace)?;
    }
    if let Some(component) = &manifest.component {
        component.validate(namespace)?;
    }
    if manifest.run.is_some() || manifest.component.is_some() {
        validate_executable_outputs(namespace, manifest)?;
    } else {
        for port in manifest.inputs.values() {
            if port.replay != Replay::Live || port.buffer != ReceiveBuffer::Fifo {
                return Err(invalid(
                    namespace,
                    "composite inputs forward contracts; receive policy belongs to the executable app",
                ));
            }
        }
        for port in manifest.outputs.values() {
            if port.from.is_none() {
                return Err(invalid(namespace, "composite outputs require from"));
            }
            PortConnection::try_from(port.clone())?;
        }
    }
    let (mut delegated_inputs, mut delegated_outputs) = (BTreeSet::new(), BTreeSet::new());
    if !manifest.owned_children.is_empty() && manifest.run.is_none() {
        return Err(invalid(namespace, "owned_children requires run"));
    }
    for (name, child) in &manifest.owned_children {
        validate_name(name, "owned child")?;
        for (names, ports, seen) in [
            (&child.inputs, &manifest.inputs, &mut delegated_inputs),
            (&child.outputs, &manifest.outputs, &mut delegated_outputs),
        ] {
            for port in names {
                if !ports.contains_key(port) || !seen.insert(port) {
                    return Err(invalid(
                        namespace,
                        format!(
                            "owned child '{name}' has undefined or multiply delegated port '{port}'"
                        ),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_executable_outputs(
    namespace: &str,
    manifest: &ModuleManifest,
) -> Result<(), ModuleError> {
    for port in manifest.outputs.values() {
        validate_contract(namespace, &port.contract(), false)?;
        if port.from.is_some() {
            return Err(invalid(
                namespace,
                "executable outputs cannot forward another module",
            ));
        }
    }
    Ok(())
}
