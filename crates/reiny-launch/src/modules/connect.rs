use super::{
    ArtifactResource, BTreeMap, BTreeSet, ComponentModule, ExecutableModule, Loader, ModuleError,
    PortAddress, PortSources, ResolvedConnection, check_contract, invalid, resolve_endpoint,
};

pub(super) struct ConnectedModules {
    pub(super) nodes: Vec<ExecutableModule>,
    pub(super) components: Vec<ComponentModule>,
    pub(super) resources: Vec<ArtifactResource>,
}

impl Loader {
    pub(super) fn connect(
        &self,
        placement: &BTreeMap<String, String>,
        connections: &mut Vec<ResolvedConnection>,
    ) -> Result<ConnectedModules, ModuleError> {
        let mut edges = BTreeMap::new();
        let mut port_types = BTreeMap::new();
        let mut terminals = BTreeMap::new();
        for (namespace, instance) in &self.instances {
            for (name, port) in &instance.manifest.inputs {
                port_types.insert(
                    PortAddress {
                        namespace: namespace.clone(),
                        input: true,
                        name: name.clone(),
                    },
                    port.contract(),
                );
            }
            for (name, port) in &instance.manifest.outputs {
                let address = PortAddress {
                    namespace: namespace.clone(),
                    input: false,
                    name: name.clone(),
                };
                port_types.insert(address.clone(), port.contract());
                if instance.manifest.run.is_some() || instance.manifest.component.is_some() {
                    terminals.insert(address, port.contract());
                } else if let Some(from) = &port.from {
                    edges.insert(address, self.references(namespace, from)?);
                }
            }
            for (child, call) in &instance.manifest.modules {
                for (name, port) in &call.inputs {
                    let from = port
                        .from
                        .as_ref()
                        .ok_or_else(|| invalid(namespace, "missing input reference"))?;
                    edges.insert(
                        PortAddress {
                            namespace: format!("{namespace}/{child}"),
                            input: true,
                            name: name.clone(),
                        },
                        self.references(namespace, from)?,
                    );
                }
            }
        }
        let mut resolved = BTreeMap::new();
        for address in port_types.keys() {
            resolve_endpoint(
                address,
                &edges,
                &terminals,
                &mut BTreeSet::new(),
                &mut resolved,
            )?;
        }
        for destination in edges.keys() {
            let expected = port_types
                .get(destination)
                .ok_or_else(|| invalid(destination.to_string(), "unknown destination"))?;
            for route in &resolved[destination] {
                check_contract(&destination.to_string(), expected, &route.contract)?;
                if destination.input && expected.semantics != route.contract.semantics {
                    return Err(invalid(
                        destination.to_string(),
                        "source semantic assertions differ",
                    ));
                }
            }
        }
        let mut nodes = Vec::new();
        let mut components = Vec::new();
        let mut resources = Vec::new();
        for (namespace, instance) in &self.instances {
            resources.extend(instance.resources(namespace)?);
            if let Some(node) = instance.executable(namespace, &resolved, connections)? {
                nodes.push(node);
            }
            if let Some(spec) = &instance.manifest.component {
                let mut component = spec.clone();
                instance.resolve_config(&mut component.config, namespace)?;
                components.push(ComponentModule {
                    namespace: namespace.clone(),
                    module_dir: instance.dir.clone(),
                    component,
                    host: placement[namespace].clone(),
                    bindings: instance.bindings(namespace, &resolved, connections)?,
                });
            }
        }
        if nodes.is_empty() && components.is_empty() {
            return Err(invalid("deployment", "no executable modules are declared"));
        }
        Ok(ConnectedModules {
            nodes,
            components,
            resources,
        })
    }

    fn references(
        &self,
        namespace: &str,
        sources: &PortSources,
    ) -> Result<Vec<PortAddress>, ModuleError> {
        if sources.references().is_empty() {
            return Err(invalid(namespace, "fan-in requires at least one source"));
        }
        let mut unique = BTreeSet::new();
        sources
            .references()
            .iter()
            .map(|text| {
                let address = self.reference(namespace, text)?;
                if !unique.insert(address.clone()) {
                    return Err(invalid(namespace, "duplicate endpoint in fan-in"));
                }
                Ok(address)
            })
            .collect()
    }
}
