use super::{
    BTreeMap, Context, DeploymentPlan, ModuleManifest, ModuleSource, Path, PathBuf, ProviderSpec,
    Result, collect_libs, copy_config_tree, copy_executable, copy_file, ensure, instance_path,
    is_runtime_library, relative_path, relocate,
};

pub(super) fn write_manifests(
    plan: &DeploymentPlan,
    out: &Path,
    origins: &[PathBuf],
    manifests: &mut BTreeMap<String, ModuleManifest>,
    providers: &BTreeMap<String, ProviderSpec>,
) -> Result<()> {
    for (namespace, manifest) in manifests {
        let instance = instance_path(namespace, &plan.deployment)?;
        manifest.build = None;
        for (name, call) in &mut manifest.modules {
            let child = instance_path(&format!("{namespace}/{name}"), &plan.deployment)?;
            call.source = ModuleSource::Local(relative_path(&instance, &child));
            // Each bundled leaf already contains its resolved instance settings.
            call.config = None;
            call.args = None;
        }
        let source = plan
            .module_sources
            .get(namespace)
            .context("instance source missing")?;
        for resource in manifest.resources.values_mut() {
            let path = if resource.source.is_absolute() {
                resource.source.clone()
            } else {
                source.join(&resource.source)
            };
            resource.source = relative_path(&instance, &relocate(&path, origins)?);
        }
        if namespace == &plan.deployment {
            manifest.providers = providers.clone();
        }
        let path = out.join(&instance).join("main.yaml");
        std::fs::create_dir_all(path.parent().context("manifest has no parent")?)?;
        // Omit build-only keys entirely, rather than serializing null declarations.
        let mut value = serde_yaml::to_value(&manifest)?;
        let map = value
            .as_mapping_mut()
            .context("manifest is not a mapping")?;
        map.remove(serde_yaml::Value::String("build".into()));
        map.remove(serde_yaml::Value::String("schema".into()));
        std::fs::write(path, serde_yaml::to_string(&value)?)?;
    }
    Ok(())
}

pub(super) fn bundle_config(config: &Path, root: &Path, out: &Path) -> Result<PathBuf> {
    let cache = root.join(".reiny/config");
    let relative = config
        .strip_prefix(&cache)
        .context("prepared config is outside the immutable config cache")?;
    let bundle = relative
        .components()
        .next()
        .context("config bundle has no address")?;
    let source = cache.join(bundle.as_os_str());
    let destination = out.join("configs").join(bundle.as_os_str());
    copy_config_tree(&source, &destination)?;
    Ok(PathBuf::from("configs").join(relative))
}

pub(super) fn finalize_bundle(
    plan: &DeploymentPlan,
    out: &Path,
    launcher_name: &str,
    include_system: bool,
) -> Result<()> {
    let bundled = DeploymentPlan::load(out, false).context("checking bundled main.yaml")?;
    ensure!(
        plan.nodes
            .iter()
            .map(|node| (&node.namespace, &node.bindings))
            .eq(bundled
                .nodes
                .iter()
                .map(|node| (&node.namespace, &node.bindings))),
        "bundle changed module namespaces or runtime bindings"
    );
    for node in &bundled.nodes {
        ensure!(
            node.provider
                .bin_dir
                .join(format!("{}{}", node.run.bin, std::env::consts::EXE_SUFFIX))
                .is_file(),
            "bundled executable missing for {}",
            node.namespace
        );
    }
    bundled
        .prepare()
        .context("verifying bundled runtime files")?;
    // Loading a local deployment can create resolver state; it is not a bundle input.
    if out.join(".reiny").exists() {
        std::fs::remove_dir_all(out.join(".reiny"))?;
    }
    let exe = std::env::current_exe().context("resolving launcher executable")?;
    let destination = out.join(format!("{launcher_name}{}", std::env::consts::EXE_SUFFIX));
    copy_executable(&exe, &destination)?;
    let mut libs = Vec::new();
    collect_libs(&exe, include_system, &mut libs)?;
    for lib in libs {
        copy_file(
            &lib,
            &out.join(lib.file_name().context("launcher library has no name")?),
        )?;
    }
    if let Some(parent) = exe.parent() {
        for entry in std::fs::read_dir(parent)? {
            let entry = entry?;
            if entry.path().is_file() && is_runtime_library(&entry.path()) {
                copy_file(&entry.path(), &out.join(entry.file_name()))?;
            }
        }
    }
    Ok(())
}
