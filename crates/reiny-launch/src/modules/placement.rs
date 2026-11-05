//! Root placement validation does not synthesize ports or executable grants.

use super::{
    BTreeMap, BTreeSet, ComponentHost, Loader, ModuleError, ProviderKind, RestartPolicy, invalid,
    validate_name,
};

impl Loader {
    pub(super) fn component_hosts(
        &self,
        deployment: &str,
    ) -> Result<BTreeMap<String, ComponentHost>, ModuleError> {
        let root = &self.instances[deployment];
        let mut hosts = BTreeMap::new();
        if let Some(specs) = &root.manifest.component_hosts {
            for (name, spec) in specs {
                validate_name(name, "component host")?;
                validate_name(&spec.bin, "host binary")?;
                validate_name(&spec.provider, "host provider")?;
                match spec.restart {
                    RestartPolicy::Manual => {}
                    RestartPolicy::OnFailure | RestartPolicy::Always => {
                        return Err(invalid(
                            name,
                            "hosted restart and rolling replacement are unsupported; use manual whole-graph recovery",
                        ));
                    }
                }
                let provider = root
                    .providers
                    .get(&spec.provider)
                    .filter(|provider| provider.kind == ProviderKind::Process)
                    .ok_or_else(|| {
                        invalid(
                            name,
                            format!("unknown process provider '{}'", spec.provider),
                        )
                    })?;
                if spec.transport == super::ComponentTransport::Local
                    && (!provider.connect.is_empty() || provider.zenoh_config.is_some())
                {
                    return Err(invalid(name, "local host forbids Zenoh provider settings"));
                }
                let mut spec = spec.clone();
                if let Some(build) = &mut spec.build {
                    build.resolve_paths(&root.dir);
                }
                hosts.insert(
                    name.clone(),
                    ComponentHost {
                        module_dir: root.dir.clone(),
                        spec,
                        provider: provider.clone(),
                    },
                );
            }
        }
        Ok(hosts)
    }

    pub(super) fn components(
        &self,
        deployment: &str,
        hosts: &BTreeMap<String, ComponentHost>,
    ) -> Result<BTreeMap<String, String>, ModuleError> {
        let root = &self.instances[deployment];
        let expected: BTreeSet<_> = self
            .instances
            .iter()
            .filter(|(_, instance)| instance.manifest.component.is_some())
            .map(|(namespace, _)| namespace.clone())
            .collect();
        let placement = root.manifest.placement.clone().unwrap_or_default();
        if placement.keys().cloned().collect::<BTreeSet<_>>() != expected {
            return Err(invalid(
                deployment,
                "placement must assign exactly every component's full canonical instance namespace",
            ));
        }
        let mut used = BTreeSet::new();
        for (namespace, host) in &placement {
            reiny_core::bindings::validate_namespace(namespace)
                .map_err(|error| invalid(namespace, error.to_string()))?;
            if !hosts.contains_key(host) {
                return Err(invalid(
                    namespace,
                    format!("unknown or empty component host '{host}'"),
                ));
            }
            used.insert(host);
        }
        if used.len() != hosts.len() {
            return Err(invalid(
                deployment,
                "every component host must have at least one instance",
            ));
        }
        if !expected.is_empty()
            && self
                .instances
                .values()
                .any(|instance| instance.manifest.run.is_some())
        {
            return Err(invalid(
                deployment,
                "mixed legacy run and hosted components are unsupported",
            ));
        }
        Ok(placement)
    }
}
