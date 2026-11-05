use super::{
    BTreeMap, Component, Context, ModuleManifest, Path, Result, bundle_nodes, copy_tree, ensure,
    finalize_bundle, instance_path, source_origins, write_manifests,
};

/// Bundle a deployment and its launcher without flattening its invocation tree.
pub(crate) fn bundle_with_control(
    config: &Path,
    out: &Path,
    launcher: Option<&str>,
    include_system: bool,
    control: &crate::control::Control,
) -> Result<()> {
    let options = crate::preparecmd::BuildOptions::default();
    let plan = crate::preparecmd::load(config, &options, control)
        .with_context(|| format!("loading deployment {}", config.display()))?;
    ensure!(
        plan.components.is_empty() && plan.component_hosts.is_empty(),
        "hosted component bundle export is not supported; use the verified prepared deployment"
    );
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
    let prepared = crate::preparecmd::materialize(
        &plan,
        &options,
        control,
        &crate::preparecmd::Protocol::new(None),
    )
    .context("preparing deployment artifacts")?;

    let mut manifests = BTreeMap::new();
    for (namespace, source) in &plan.module_sources {
        control.check()?;
        let path = source.join("main.yaml");
        let manifest: ModuleManifest = serde_yaml::from_slice(&std::fs::read(&path)?)
            .with_context(|| format!("reading {}", path.display()))?;
        manifests.insert(namespace.clone(), manifest);
    }
    let origins = source_origins(&plan)?;
    for (index, origin) in origins.iter().enumerate() {
        control.check()?;
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
        control.check()?;
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
    control.check()?;
    let providers = bundle_nodes(
        &plan,
        &prepared,
        &out,
        &origins,
        &mut manifests,
        include_system,
    )?;
    write_manifests(&plan, &out, &origins, &mut manifests, &providers)?;
    control.check()?;
    finalize_bundle(&plan, &out, launcher_name, include_system)?;
    control.check()?;
    println!("bundled {} into {}", plan.deployment, out.display());
    Ok(())
}
