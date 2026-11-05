use super::{
    ArtifactResource, BTreeMap, EndpointAddress, ExecutableModule, InputBinding, Instance,
    MODULE_BINDINGS_VERSION, ModuleBindings, ModuleError, OutputBinding, PathBuf, PortAddress,
    PortKind, ProviderKind, Replay, ResolvedConnection, ResolvedRoute, Retention, absolute,
    invalid, validate_name,
};

impl Instance {
    pub(super) fn bindings(
        &self,
        namespace: &str,
        resolved: &BTreeMap<PortAddress, Vec<ResolvedRoute>>,
        connections: &mut Vec<ResolvedConnection>,
    ) -> Result<ModuleBindings, ModuleError> {
        let mut inputs = BTreeMap::new();
        for (name, port) in &self.manifest.inputs {
            let address = PortAddress {
                namespace: namespace.to_string(),
                input: true,
                name: name.clone(),
            };
            let endpoints = &resolved[&address];
            let contract = port.contract();
            if contract.kind == PortKind::Rpc && endpoints.len() != 1 {
                return Err(invalid(
                    namespace,
                    "RPC inputs require exactly one endpoint",
                ));
            }
            let mut sources = Vec::new();
            for route in endpoints {
                if contract.replay == Replay::Last && route.contract.retention != Retention::Last {
                    return Err(invalid(
                        address.to_string(),
                        "replay last requires retained output",
                    ));
                }
                let source = EndpointAddress {
                    namespace: route.source.namespace.clone(),
                    port: route.source.name.clone(),
                };
                if sources.contains(&source) {
                    return Err(invalid(
                        address.to_string(),
                        "multiple aliases resolve to the same publisher",
                    ));
                }
                sources.push(source);
                connections.push(ResolvedConnection {
                    source: route.source.clone(),
                    destination: address.clone(),
                    via: route.via.clone(),
                    contract: route.contract.clone(),
                });
            }
            inputs.insert(name.clone(), InputBinding { sources, contract });
        }
        let outputs = self
            .manifest
            .outputs
            .iter()
            .map(|(name, port)| {
                (
                    name.clone(),
                    OutputBinding {
                        contract: port.contract(),
                    },
                )
            })
            .collect();
        Ok(ModuleBindings {
            version: MODULE_BINDINGS_VERSION,
            namespace: namespace.to_string(),
            endpoint_namespace: namespace.to_string(),
            inputs,
            outputs,
            children: self.manifest.owned_children.clone(),
            executables: self
                .manifest
                .run
                .iter()
                .flat_map(|run| {
                    std::iter::once(run.bin.clone()).chain(run.companions.iter().cloned())
                })
                .collect(),
        })
    }

    pub(super) fn executable(
        &self,
        namespace: &str,
        resolved: &BTreeMap<PortAddress, Vec<ResolvedRoute>>,
        connections: &mut Vec<ResolvedConnection>,
    ) -> Result<Option<ExecutableModule>, ModuleError> {
        let Some(run) = &self.manifest.run else {
            return Ok(None);
        };
        let mut run = run.clone();
        self.resolve_config(&mut run.config, namespace)?;
        let provider = self
            .providers
            .get(&run.provider)
            .ok_or_else(|| invalid(namespace, "unknown executable provider"))?
            .clone();
        Ok(Some(ExecutableModule {
            namespace: namespace.to_string(),
            module_dir: self.dir.clone(),
            run,
            build: self.manifest.build.clone(),
            provider,
            bindings: self.bindings(namespace, resolved, connections)?,
        }))
    }

    pub(super) fn resolve_config(
        &self,
        config: &mut Option<PathBuf>,
        namespace: &str,
    ) -> Result<(), ModuleError> {
        if let Some(config) = config {
            *config = absolute(&self.dir, config);
            if !config.is_file() {
                return Err(invalid(
                    namespace,
                    format!("configuration file not found: {}", config.display()),
                ));
            }
        }
        Ok(())
    }

    pub(super) fn resources(&self, namespace: &str) -> Result<Vec<ArtifactResource>, ModuleError> {
        let mut resources = Vec::new();
        for (name, resource) in &self.manifest.resources {
            validate_name(name, "resource")?;
            let provider = self.providers.get(&resource.provider).ok_or_else(|| {
                invalid(
                    namespace,
                    format!("unknown artifact provider '{}'", resource.provider),
                )
            })?;
            if resource.kind != "artifact.file" || provider.kind != ProviderKind::Artifact {
                return Err(invalid(
                    namespace,
                    "file resources require type artifact.file and an artifact provider",
                ));
            }
            resources.push(ArtifactResource {
                address: format!("{namespace}/{name}"),
                source: absolute(&self.dir, &resource.source),
                sha256: resource.sha256.clone(),
            });
        }
        Ok(resources)
    }
}
