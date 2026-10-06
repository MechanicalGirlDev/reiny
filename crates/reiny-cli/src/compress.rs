//! Materialize an executable, source-independent `main.yaml` deployment.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, ensure};
use reiny_launch::{
    DeploymentPlan, ModuleManifest, ModuleSource, PreparedDeployment, ProviderKind, ProviderSpec,
};

/// Bundle a deployment and its launcher without flattening its invocation tree.
pub(crate) fn compress(
    config: &Path,
    out: &Path,
    launcher: Option<&str>,
    include_system: bool,
) -> Result<()> {
    let plan = DeploymentPlan::load(config, false)
        .with_context(|| format!("loading deployment {}", config.display()))?;
    let launcher_name = launcher.unwrap_or("reiny");
    ensure!(
        !launcher_name.is_empty()
            && launcher_name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_-.".contains(&byte))
            && Path::new(launcher_name).components().count() == 1
            && matches!(
                Path::new(launcher_name).components().next(),
                Some(Component::Normal(_))
            )
            && !["main.yaml", "sources", "modules", "bin"].contains(&launcher_name),
        "launcher must be a plain executable name"
    );
    // Never merge with another bundle or replace an existing executable.
    ensure!(
        !out.exists() || (out.is_dir() && std::fs::read_dir(out)?.next().is_none()),
        "bundle output must be an empty directory: {}",
        out.display()
    );
    std::fs::create_dir_all(out)?;
    let out = out.canonicalize()?;
    ensure!(
        out != plan.root,
        "bundle output cannot be the deployment root"
    );
    let prepared = plan.prepare().context("preparing deployment artifacts")?;

    let mut manifests = BTreeMap::new();
    for (namespace, source) in &plan.module_sources {
        let path = source.join("main.yaml");
        let manifest: ModuleManifest = serde_yaml::from_slice(&std::fs::read(&path)?)
            .with_context(|| format!("reading {}", path.display()))?;
        manifests.insert(namespace.clone(), manifest);
    }
    let origins = source_origins(&plan)?;
    for (index, origin) in origins.iter().enumerate() {
        copy_tree(
            origin,
            &out.join("sources").join(index.to_string()),
            &out,
            false,
        )?;
    }

    // Instance directories retain module-local working-directory assets. Config files
    // instead point into complete origin trees, preserving their relative includes.
    for (namespace, source) in &plan.module_sources {
        if manifests
            .get(namespace)
            .context("instance manifest missing")?
            .run
            .is_none()
        {
            continue;
        }
        let destination = out.join(instance_path(namespace, &plan.deployment)?);
        if namespace == &plan.deployment {
            copy_tree(source, &out, &out, true)?;
        } else {
            copy_tree(source, &destination, &out, true)?;
        }
    }
    let providers = bundle_nodes(
        &plan,
        &prepared,
        &out,
        &origins,
        &mut manifests,
        include_system,
    )?;
    write_manifests(&plan, &out, &origins, &mut manifests, &providers)?;
    finalize_bundle(&plan, &out, launcher_name, include_system)?;
    println!("bundled {} into {}", plan.deployment, out.display());
    Ok(())
}

fn bundle_nodes(
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
            if entry.path().is_file() {
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
        let mut slot = format!("__bundle_{index}");
        while slots.contains(&slot) {
            slot.push('_');
        }
        slots.insert(slot.clone());
        let mut provider = node.provider.clone();
        provider.bin_dir = bin_dir;
        provider.kind = ProviderKind::Process;
        if let Some(config) = &provider.zenoh_config {
            provider.zenoh_config = Some(relocate(config, origins)?);
        }
        providers.insert(slot.clone(), provider);
        let manifest = manifests
            .get_mut(&node.namespace)
            .context("leaf instance missing")?;
        let run = manifest.run.as_mut().context("leaf run missing")?;
        run.provider = slot;
        if let Some(config) = &node.run.config {
            run.config = Some(relative_path(&instance, &relocate(config, origins)?));
        }
    }
    Ok(providers)
}

fn write_manifests(
    plan: &DeploymentPlan,
    out: &Path,
    origins: &[PathBuf],
    manifests: &mut BTreeMap<String, ModuleManifest>,
    providers: &BTreeMap<String, ProviderSpec>,
) -> Result<()> {
    for (namespace, manifest) in manifests {
        let instance = instance_path(namespace, &plan.deployment)?;
        manifest.build = None;
        manifest.schema = None;
        for (name, call) in &mut manifest.modules {
            let child = instance_path(&format!("{namespace}/{name}"), &plan.deployment)?;
            call.source = ModuleSource::Local(relative_path(&instance, &child));
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

fn finalize_bundle(
    plan: &DeploymentPlan,
    out: &Path,
    launcher_name: &str,
    include_system: bool,
) -> Result<()> {
    let bundled = DeploymentPlan::load(out, false).context("checking bundled main.yaml")?;
    ensure!(
        serde_json::to_value(
            plan.nodes
                .iter()
                .map(|node| (&node.namespace, &node.bindings))
                .collect::<Vec<_>>()
        )? == serde_json::to_value(
            bundled
                .nodes
                .iter()
                .map(|node| (&node.namespace, &node.bindings))
                .collect::<Vec<_>>()
        )?,
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

fn instance_path(namespace: &str, deployment: &str) -> Result<PathBuf> {
    if namespace == deployment {
        return Ok(PathBuf::new());
    }
    let suffix = namespace
        .strip_prefix(&format!("{deployment}/"))
        .context("instance namespace outside deployment")?;
    Ok(PathBuf::from("modules").join(suffix))
}

/// Stable origin IDs follow sorted canonical directories, not filesystem iteration.
fn source_origins(plan: &DeploymentPlan) -> Result<Vec<PathBuf>> {
    let checkouts = plan.root.join(".reiny/cache/git/checkouts");
    let mut roots = BTreeSet::from([plan.root.clone()]);
    for dir in plan.module_sources.values() {
        if let Ok(relative) = dir.strip_prefix(&checkouts) {
            let hash = relative
                .components()
                .next()
                .context("Git checkout has no origin")?;
            roots.insert(checkouts.join(hash.as_os_str()).canonicalize()?);
        } else if !dir.starts_with(&plan.root) {
            roots.insert(dir.clone());
        }
    }
    let roots: Vec<_> = roots.into_iter().collect();
    Ok(roots
        .iter()
        .filter(|root| {
            root.starts_with(&checkouts)
                || !roots.iter().any(|other| {
                    other != *root && root.starts_with(other) && !root.starts_with(&checkouts)
                })
        })
        .cloned()
        .collect())
}

fn relocate(path: &Path, origins: &[PathBuf]) -> Result<PathBuf> {
    let path = path
        .canonicalize()
        .with_context(|| format!("resolving runtime file {}", path.display()))?;
    ensure!(
        path.is_file(),
        "runtime path is not a file: {}",
        path.display()
    );
    // Git checkout roots are more specific than the enclosing deployment root.
    let (index, root) = origins.iter().enumerate()
        .filter(|(_, root)| path.starts_with(root))
        .max_by_key(|(_, root)| root.components().count())
        .with_context(|| format!("runtime file {} is outside owned module origins; move it into a module source tree", path.display()))?;
    let relative = path.strip_prefix(root)?;
    ensure!(!relative.components().any(|part| {
        matches!(part, Component::Normal(name) if name == ".git" || name == ".reiny" || name == "target")
    }), "runtime file {} is in an excluded build/cache directory", path.display());
    Ok(PathBuf::from("sources")
        .join(index.to_string())
        .join(relative))
}

fn relative_path(from: &Path, to: &Path) -> PathBuf {
    let common = from
        .components()
        .zip(to.components())
        .take_while(|(a, b)| a == b)
        .count();
    let mut result = PathBuf::new();
    for _ in from.components().skip(common) {
        result.push("..");
    }
    for component in to.components().skip(common) {
        result.push(component.as_os_str());
    }
    if result.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        result
    }
}

fn copy_tree(source: &Path, destination: &Path, output: &Path, omit_manifest: bool) -> Result<()> {
    std::fs::create_dir_all(destination)?;
    let mut entries = std::fs::read_dir(source)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let name = entry.file_name();
        if name == ".git"
            || name == ".reiny"
            || name == "target"
            || (omit_manifest && name == "main.yaml")
        {
            continue;
        }
        let path = entry.path();
        if path.canonicalize()? == output {
            continue;
        }
        ensure!(
            !entry.file_type()?.is_symlink(),
            "runtime source tree contains a symlink; materialize it before bundling: {}",
            path.display()
        );
        let target = destination.join(name);
        if path.is_dir() {
            copy_tree(&path, &target, output, false)?;
        } else {
            copy_file(&path, &target)?;
        }
    }
    Ok(())
}

fn copy_file(src: &Path, dst: &Path) -> Result<()> {
    if dst.exists() {
        ensure!(
            std::fs::read(src)? == std::fs::read(dst)?,
            "bundle file collision: {} -> {}",
            src.display(),
            dst.display()
        );
        return Ok(());
    }
    std::fs::create_dir_all(dst.parent().context("copied file has no parent")?)?;
    std::fs::copy(src, dst)
        .with_context(|| format!("copying {} -> {}", src.display(), dst.display()))?;
    Ok(())
}

/// Unix loaders do not search an executable's directory by default. A tiny
/// exec wrapper supplies that directory even after immutable artifact staging.
/// The native executable uses a runtime-library suffix so staging retains it.
fn copy_executable(src: &Path, dst: &Path) -> Result<()> {
    if !cfg!(unix) {
        return copy_file(src, dst);
    }
    let name = dst
        .file_name()
        .context("executable has no filename")?
        .to_str()
        .context("executable filename is not UTF-8")?;
    let native_name = format!("{name}.runtime.so");
    let native = dst.with_file_name(&native_name);
    copy_file(src, &native)?;
    ensure!(
        !dst.exists(),
        "executable wrapper collision: {}",
        dst.display()
    );
    let wrapper = format!(
        "#!/bin/sh\nHERE=$(CDPATH= cd -- \"$(dirname -- \"$0\")\" && pwd) || exit 1\nLD_LIBRARY_PATH=\"$HERE${{LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}}\" DYLD_LIBRARY_PATH=\"$HERE${{DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}}\" exec \"$HERE/{native_name}\" \"$@\"\n"
    );
    std::fs::write(dst, wrapper)?;
    std::fs::set_permissions(dst, std::fs::metadata(src)?.permissions())?;
    Ok(())
}

fn is_runtime_library(path: &Path) -> bool {
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_default();
    name.ends_with(".dll")
        || name.ends_with(".dylib")
        || name.ends_with(".so")
        || name.contains(".so.")
}

/// Linux linker-resolved dependencies supplement the immutable staged libraries.
fn collect_libs(bin: &Path, include_system: bool, out: &mut Vec<PathBuf>) -> Result<()> {
    if !cfg!(target_os = "linux") {
        return Ok(());
    }
    let output = std::process::Command::new("ldd")
        .arg(bin)
        .output()
        .context("ldd is required to discover Linux runtime libraries")?;
    if !output.status.success() {
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        ensure!(
            text.contains("not a dynamic executable") || text.contains("statically linked"),
            "ldd failed for {}: {text}",
            bin.display()
        );
        return Ok(());
    }
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        if let Some((_, rest)) = line.split_once("=>") {
            let path = rest.split_whitespace().next().unwrap_or("");
            ensure!(
                path != "not",
                "unresolved runtime library for {}: {line}",
                bin.display()
            );
            if !path.is_empty() && (include_system || !is_system_lib(path)) {
                ensure!(
                    Path::new(path).is_file(),
                    "resolved runtime library missing: {path}"
                );
                out.push(PathBuf::from(path));
            }
        }
    }
    Ok(())
}

fn is_system_lib(path: &str) -> bool {
    path.contains("ld-linux")
        || path.contains("linux-vdso")
        || path.starts_with("/lib/")
        || path.starts_with("/lib64/")
        || path.starts_with("/usr/lib/")
        || path.starts_with("/usr/lib64/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_libraries_stay_out_of_the_bundle() {
        for system in [
            "/lib/x86_64-linux-gnu/libc.so.6",
            "/lib64/ld-linux-x86-64.so.2",
            "/usr/lib/x86_64-linux-gnu/libstdc++.so.6",
            "/usr/lib64/libm.so.6",
            "linux-vdso.so.1",
        ] {
            assert!(is_system_lib(system), "{system} should be a system library");
        }
        for ours in [
            "/home/nop/dev/robot/target/release/libmylib.so",
            "/opt/robot/lib/libdriver.so",
            "./libplugin.so",
        ] {
            assert!(!is_system_lib(ours), "{ours} has to be bundled");
        }
        assert!(!is_system_lib("/home/nop/usr/lib/libmine.so"));
    }
}
