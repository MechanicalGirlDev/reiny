use super::{
    BTreeMap, BTreeSet, BuildRequest, Context, DeploymentPlan, Environment, Group, GroupKind,
    Member, Path, Result, Work, artifacts, ensure, prepared,
};

pub(super) fn plan_groups(
    plan: &DeploymentPlan,
    base: &Path,
    environment: &Environment,
) -> Result<Vec<Group>> {
    let mut builds: BTreeMap<String, Group> = BTreeMap::new();
    let mut frozen = Vec::new();
    for (index, node) in plan.nodes.iter().enumerate() {
        node.run.validate(&node.namespace)?;
        let mut bins = BTreeSet::new();
        for bin in std::iter::once(&node.run.bin).chain(&node.run.companions) {
            artifacts::validate_executable_name(bin)?;
            ensure!(bins.insert(bin.clone()), "duplicate executable name {bin}");
        }
        let (id, kind, work) = if let Some(spec) = &node.build {
            let build = BuildRequest::new(spec, &node.module_dir, base, environment)?;
            if let artifacts::BuildSpec::Command(command) = &build.spec {
                for bin in &bins {
                    ensure!(
                        command.outputs.contains_key(bin),
                        "{}: command outputs does not declare {bin}",
                        node.namespace
                    );
                }
            }
            (
                format!("build-{}", build.key),
                build.kind(),
                Work::Build(Box::new(build)),
            )
        } else {
            let directory = node.provider.bin_dir.canonicalize().with_context(|| {
                format!("prebuilt directory {}", node.provider.bin_dir.display())
            })?;
            for bin in &bins {
                ensure!(
                    directory.join(artifacts::executable_name(bin)).is_file(),
                    "prepared binary not found: {}",
                    directory.join(artifacts::executable_name(bin)).display()
                );
            }
            (
                format!(
                    "prebuilt-{}",
                    prepared::digest(&serde_json::to_vec(&directory)?)
                ),
                GroupKind::Prebuilt,
                Work::Prebuilt(directory),
            )
        };
        let group = builds.entry(id.clone()).or_insert_with(|| Group {
            id,
            kind,
            members: Vec::new(),
            bins: BTreeSet::new(),
            work,
        });
        group.members.push(Member::Module(index));
        group.bins.extend(bins);
        frozen.extend(plan_configs(node, index)?);
    }
    let (host_builds, host_configs) = super::hosted::groups(plan, base, environment)?;
    builds.extend(host_builds);
    frozen.extend(host_configs);
    for (index, resource) in plan.resources.iter().enumerate() {
        ensure!(
            resource.source.is_file(),
            "artifact {} source not found: {}",
            resource.address,
            resource.source.display()
        );
        frozen.push(Group {
            id: format!("resource-{}", resource.address),
            kind: GroupKind::Resource,
            members: Vec::new(),
            bins: BTreeSet::new(),
            work: Work::Resource(index),
        });
    }
    Ok(builds.into_values().chain(frozen).collect())
}

fn plan_configs(node: &crate::modules::ExecutableModule, index: usize) -> Result<Vec<Group>> {
    let mut frozen = Vec::new();
    ensure!(
        node.run.config.is_some() || node.run.config_assets.is_empty(),
        "{}: config_assets requires run.config",
        node.namespace
    );
    for (config, assets, zenoh) in [
        (
            node.run.config.as_ref(),
            node.run.config_assets.as_slice(),
            false,
        ),
        (node.provider.zenoh_config.as_ref(), [].as_slice(), true),
    ] {
        if let Some(config) = config {
            let (files, entry) = prepared::config_files(config, assets).with_context(|| {
                format!(
                    "{}: module {}; selected config {}; declared assets {:?}",
                    node.namespace,
                    node.module_dir.display(),
                    config.display(),
                    assets
                )
            })?;
            frozen.push(Group {
                id: format!(
                    "config-{}-{}",
                    node.namespace,
                    if zenoh { "zenoh" } else { "app" }
                ),
                kind: GroupKind::Config,
                members: vec![Member::Module(index)],
                bins: BTreeSet::new(),
                work: Work::Config {
                    files,
                    entry,
                    zenoh,
                },
            });
        }
    }
    Ok(frozen)
}
