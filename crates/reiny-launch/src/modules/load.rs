use super::{
    BTreeMap, Instance, Loader, ModuleError, ModuleManifest, ModuleSource, Path, PortAddress,
    ProviderSpec, absolute, check_type, invalid, read_manifest, validate_manifest, validate_name,
};

impl Loader {
    pub(super) fn visit(
        &mut self,
        namespace: &str,
        dir: &Path,
        mut manifest: ModuleManifest,
        providers: BTreeMap<String, ProviderSpec>,
        root: bool,
    ) -> Result<(), ModuleError> {
        let dir = std::fs::canonicalize(dir).map_err(|source| ModuleError::Io {
            path: dir.to_path_buf(),
            source,
        })?;
        if self.source_stack.contains(&dir) {
            return Err(ModuleError::Cycle(format!("source {}", dir.display())));
        }
        validate_manifest(namespace, &manifest, &providers, root)?;
        self.resolver.check_cancelled()?;
        if let Some(build) = &mut manifest.build {
            build.resolve_paths(&dir);
        }
        self.source_stack.push(dir.clone());
        for (name, call) in &manifest.modules {
            let child_namespace = format!("{namespace}/{name}");
            let child_dir = match &call.source {
                ModuleSource::Local(path) => absolute(&dir, path),
                ModuleSource::Git(source) => {
                    self.resolver.resolve(&child_namespace, &dir, source)?
                }
            };
            let mut child = read_manifest(&child_dir)?;
            if call.inputs.keys().ne(child.inputs.keys()) {
                return Err(invalid(
                    &child_namespace,
                    "caller must connect exactly the child's declared inputs",
                ));
            }
            for (name, port) in &call.inputs {
                check_type(
                    &format!("{child_namespace}.in.{name}"),
                    &child.inputs[name],
                    port,
                )?;
            }
            for (name, port) in &call.outputs {
                let declared = child.outputs.get(name).ok_or_else(|| {
                    invalid(&child_namespace, format!("child has no output '{name}'"))
                })?;
                check_type(&format!("{child_namespace}.out.{name}"), declared, port)?;
            }
            if call.config.is_some() || call.config_assets.is_some() || call.args.is_some() {
                let (config, assets) = match (&mut child.run, &mut child.component) {
                    (Some(run), None) => {
                        if let Some(args) = &call.args {
                            run.args.clone_from(args);
                        }
                        (&mut run.config, &mut run.config_assets)
                    }
                    (None, Some(component)) => {
                        if call.args.is_some() {
                            return Err(invalid(
                                &child_namespace,
                                "component instances do not accept args",
                            ));
                        }
                        (&mut component.config, &mut component.config_assets)
                    }
                    (None, None) | (Some(_), Some(_)) => {
                        return Err(invalid(
                            &child_namespace,
                            "instance overrides require exactly one executable or component leaf",
                        ));
                    }
                };
                if let Some(selected) = &call.config {
                    *config = Some(absolute(&dir, selected));
                }
                if let Some(config_assets) = &call.config_assets {
                    assets.clone_from(config_assets);
                }
            }
            let mut child_providers = providers.clone();
            for (slot, name) in &call.providers {
                validate_name(slot, "provider slot")?;
                let provider = providers.get(name).ok_or_else(|| {
                    invalid(namespace, format!("unknown provider mapping '{name}'"))
                })?;
                child_providers.insert(slot.clone(), provider.clone());
            }
            self.visit(&child_namespace, &child_dir, child, child_providers, false)?;
        }
        self.source_stack.pop();
        self.instances.insert(
            namespace.to_string(),
            Instance {
                dir,
                manifest,
                providers,
            },
        );
        Ok(())
    }

    pub(super) fn reference(
        &self,
        namespace: &str,
        text: &str,
    ) -> Result<PortAddress, ModuleError> {
        let error = |message: &str| ModuleError::Reference {
            module: namespace.to_string(),
            reference: text.to_string(),
            message: message.to_string(),
        };
        let (owner, name) = text
            .split_once('.')
            .ok_or_else(|| error("use sibling.port or in.port"))?;
        if name.contains('.') {
            return Err(error(
                "module shared_types and absolute paths are not public references",
            ));
        }
        let instance = self
            .instances
            .get(namespace)
            .ok_or_else(|| error("unknown module"))?;
        if owner == "in" {
            if !instance.manifest.inputs.contains_key(name) {
                return Err(error("input is not declared"));
            }
            Ok(PortAddress {
                namespace: namespace.to_string(),
                input: true,
                name: name.to_string(),
            })
        } else {
            instance
                .manifest
                .modules
                .get(owner)
                .ok_or_else(|| error("sibling module is not declared"))?;
            let child = self
                .instances
                .get(&format!("{namespace}/{owner}"))
                .ok_or_else(|| error("sibling module is not resolved"))?;
            if !child.manifest.outputs.contains_key(name) {
                return Err(error("sibling output is not declared by its app"));
            }
            Ok(PortAddress {
                namespace: format!("{namespace}/{owner}"),
                input: false,
                name: name.to_string(),
            })
        }
    }
}
