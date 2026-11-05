use super::{
    BTreeMap, DeploymentPlan, Loader, ModuleError, ModuleManifest, Path, PathBuf, SourceResolver,
    absolute, invalid, manifest_dir, read_manifest, validate_definition, validate_name,
};

impl DeploymentPlan {
    /// Load a root directory or its `main.yaml`, resolve sources and check all ports.
    ///
    /// No build or executable module is run. `update` explicitly refreshes Git refs.
    pub fn load(path: &Path, update: bool) -> Result<Self, ModuleError> {
        Self::load_with(path, update, &crate::CancelToken::default()).map_err(|error| match error {
            crate::PrepareError::Failed(error) => error
                .downcast::<ModuleError>()
                .unwrap_or_else(ModuleError::Acquisition),
            error => ModuleError::Acquisition(error.into()),
        })
    }

    /// Resolve declarations and pinned Git sources with cancellable acquisition.
    /// This never invokes any builder.
    pub fn load_with(
        path: &Path,
        update: bool,
        cancel: &crate::CancelToken,
    ) -> Result<Self, crate::PrepareError> {
        cancel.check()?;
        let result = (|| {
            let root = manifest_dir(path)?;
            let manifest = read_manifest(&root)?;
            Self::resolve(root, manifest, update, Some(cancel))
        })();
        if cancel.is_cancelled() {
            Err(crate::PrepareError::Cancelled)
        } else {
            result.map_err(|error| crate::PrepareError::Failed(error.into()))
        }
    }

    fn resolve(
        root: PathBuf,
        manifest: ModuleManifest,
        update: bool,
        cancel: Option<&crate::CancelToken>,
    ) -> Result<Self, ModuleError> {
        let deployment = manifest.deployment.clone().ok_or_else(|| {
            invalid(
                root.display().to_string(),
                "root main.yaml requires deployment",
            )
        })?;
        validate_name(&deployment, "deployment")?;
        let domain = manifest
            .domain
            .clone()
            .unwrap_or_else(|| deployment.clone());
        validate_name(&domain, "domain")?;
        if !manifest.inputs.is_empty() {
            return Err(invalid(&deployment, "root inputs have no supplying parent"));
        }
        let mut providers = manifest.providers.clone();
        for (name, provider) in &mut providers {
            validate_name(name, "provider")?;
            provider.bin_dir = absolute(&root, &provider.bin_dir);
            if let Some(config) = &mut provider.zenoh_config {
                *config = absolute(&root, config);
            }
        }
        let mut loader = Loader {
            resolver: match cancel {
                Some(cancel) => SourceResolver::open_with(&root, update, cancel)?,
                None => SourceResolver::open(&root, update)?,
            },
            instances: BTreeMap::new(),
            source_stack: Vec::new(),
        };
        loader.visit(&deployment, &root, manifest, providers, true)?;
        let component_hosts = loader.component_hosts(&deployment)?;
        let placement = loader.components(&deployment, &component_hosts)?;
        let mut connections = Vec::new();
        let connected = loader.connect(&placement, &mut connections)?;
        for component in &connected.components {
            for source in component
                .bindings
                .inputs
                .values()
                .flat_map(|input| &input.sources)
            {
                let source_host = placement.get(&source.namespace).ok_or_else(|| {
                    invalid(&component.namespace, "hosted source outside placement")
                })?;
                if source_host != &component.host
                    && [source_host, &component.host].iter().any(|host| {
                        component_hosts[*host].spec.transport == super::ComponentTransport::Local
                    })
                {
                    return Err(invalid(
                        &component.namespace,
                        "edge crosses a local-only host boundary",
                    ));
                }
            }
        }
        let module_sources = loader
            .instances
            .iter()
            .map(|(name, instance)| (name.clone(), instance.dir.clone()))
            .collect();
        loader.resolver.finish()?;
        Ok(Self {
            root,
            deployment,
            domain,
            module_sources,
            nodes: connected.nodes,
            components: connected.components,
            component_hosts,
            connections,
            resources: connected.resources,
        })
    }
}

/// Check a runtime definition, resolving it when it declares a deployment root.
///
/// Reusable app definitions are checked without requiring a caller's providers or
/// sources. Deployment roots use the same resolver as [`DeploymentPlan::load`],
/// without building artifacts or starting processes. Schema catalogs are not read.
pub fn check_manifest(path: &Path) -> Result<Option<DeploymentPlan>, ModuleError> {
    let root = manifest_dir(path)?;
    let manifest = read_manifest(&root)?;
    if manifest.deployment.is_some() {
        DeploymentPlan::resolve(root, manifest, false, None).map(Some)
    } else {
        let name = root.display().to_string();
        if manifest.domain.is_some()
            || !manifest.providers.is_empty()
            || manifest.component_hosts.is_some()
            || manifest.placement.is_some()
        {
            return Err(invalid(&name, "root settings require deployment"));
        }
        validate_definition(&name, &manifest)?;
        Ok(None)
    }
}
