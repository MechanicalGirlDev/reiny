use super::{BuildOptions, BuildSpec, Context, Control, DeploymentPlan, Path, Result};

pub(crate) fn load(
    path: &Path,
    options: &BuildOptions,
    control: &Control,
) -> Result<DeploymentPlan> {
    control.check()?;
    let mut plan = DeploymentPlan::load_with(path, false, &control.cancel)?;
    let bin_dir = options
        .bin_dir
        .as_ref()
        .map(std::fs::canonicalize)
        .transpose()
        .context("resolving binary directory")?;
    for node in &mut plan.nodes {
        if node.build.is_none()
            && let Some(dir) = &bin_dir
        {
            node.provider.bin_dir.clone_from(dir);
        }
        if options.release
            && let Some(BuildSpec::Cargo(build)) = &mut node.build
        {
            "release".clone_into(&mut build.profile);
        }
    }
    for host in plan.component_hosts.values_mut() {
        if host.spec.build.is_none()
            && let Some(dir) = &bin_dir
        {
            host.provider.bin_dir.clone_from(dir);
        }
        if options.release
            && let Some(BuildSpec::Cargo(build)) = &mut host.spec.build
        {
            "release".clone_into(&mut build.profile);
        }
    }
    Ok(plan)
}
