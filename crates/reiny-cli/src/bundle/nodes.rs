use super::{
    BTreeMap, BTreeSet, Context, DeploymentPlan, ModuleManifest, Path, PathBuf, PreparedDeployment,
    ProviderKind, ProviderSpec, Result, bundle_config, collect_libs, copy_config_tree,
    copy_executable, copy_file, ensure, instance_path, relative_path, relocate,
};

pub(super) fn bundle_nodes(
    plan: &DeploymentPlan,
    prepared: &PreparedDeployment,
    out: &Path,
    origins: &[PathBuf],
    manifests: &mut BTreeMap<String, ModuleManifest>,
    include_system: bool,
) -> Result<BTreeMap<String, ProviderSpec>> {
    let mut slots = BTreeSet::new();
    for manifest in manifests.values() {
        slots.extend(manifest.providers.keys().cloned());
        for call in manifest.modules.values() {
            slots.extend(call.providers.keys().cloned());
            slots.extend(call.providers.values().cloned());
        }
    }
    let mut providers = manifests
        .get(&plan.deployment)
        .context("root instance missing")?
        .providers
        .clone();
    for provider in providers.values_mut() {
        provider.bin_dir = PathBuf::from(".");
        if let Some(config) = &provider.zenoh_config {
            let source = if config.is_absolute() {
                config.clone()
            } else {
                plan.root.join(config)
            };
            provider.zenoh_config = Some(relocate(&source, origins)?);
        }
    }

    for (index, node) in prepared.nodes.iter().enumerate() {
        let instance = instance_path(&node.namespace, &plan.deployment)?;
        let suffix = node
            .namespace
            .strip_prefix(&plan.deployment)
            .context("node namespace outside deployment")?
            .trim_start_matches('/');
        let bin_dir = PathBuf::from("bin").join(if suffix.is_empty() { "_root" } else { suffix });
        let destination = out.join(&bin_dir);
        std::fs::create_dir_all(&destination)?;
        let executable_dir = node
            .executable
            .parent()
            .context("executable has no directory")?;
        for entry in std::fs::read_dir(executable_dir)? {
            let entry = entry?;
            if entry.path().is_dir() {
                copy_config_tree(&entry.path(), &destination.join(entry.file_name()))?;
            } else if entry.path().is_file() {
                if entry.path() == node.executable {
                    copy_executable(&entry.path(), &destination.join(entry.file_name()))?;
                } else {
                    copy_file(&entry.path(), &destination.join(entry.file_name()))?;
                }
            }
        }
        let mut libs = Vec::new();
        collect_libs(&node.executable, include_system, &mut libs)?;
        for lib in libs {
            copy_file(
                &lib,
                &destination.join(lib.file_name().context("library has no name")?),
            )?;
        }
        write_bundle_files(&destination)?;
        let mut slot = format!("__bundle_{index}");
        while slots.contains(&slot) {
            slot.push('_');
        }
        slots.insert(slot.clone());
        let mut provider = node.provider.clone();
        provider.bin_dir = bin_dir;
        provider.kind = ProviderKind::Process;
        if let Some(config) = &provider.zenoh_config {
            provider.zenoh_config = Some(bundle_config(config, &plan.root, out)?);
        }
        providers.insert(slot.clone(), provider);
        let manifest = manifests
            .get_mut(&node.namespace)
            .context("leaf instance missing")?;
        let run = manifest.run.as_mut().context("leaf run missing")?;
        *run = node.run.clone();
        run.provider = slot;
        if let Some(config) = &node.run.config {
            let source = node
                .config_bundle_dir
                .as_ref()
                .context("prepared app config has no bundle directory")?;
            let entry = config
                .strip_prefix(source)
                .context("prepared app config is outside its bundle directory")?;
            let destination = PathBuf::from("configs")
                .join(source.file_name().context("config bundle has no address")?);
            copy_config_tree(source, &out.join(&destination))?;
            run.config = Some(relative_path(&instance, &destination.join(entry)));
        }
    }
    Ok(providers)
}

/// Retain the exact runtime inventory when this distribution is prepared again.
pub(super) fn write_bundle_files(directory: &Path) -> Result<()> {
    fn collect(root: &Path, directory: &Path, files: &mut BTreeSet<PathBuf>) -> Result<()> {
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_dir() {
                collect(root, &entry.path(), files)?;
            } else {
                ensure!(kind.is_file(), "bundle runtime entry is not a regular file");
                files.insert(entry.path().strip_prefix(root)?.to_owned());
            }
        }
        Ok(())
    }
    let metadata = directory.join(".reiny-bundle.json");
    ensure!(!metadata.try_exists()?, "reserved bundle metadata path");
    let mut files = BTreeSet::new();
    collect(directory, directory, &mut files)?;
    std::fs::write(
        metadata,
        serde_json::to_vec(&serde_json::json!({"v": 1, "files": files}))?,
    )?;
    Ok(())
}
