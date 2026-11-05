use super::{
    BTreeMap, BTreeSet, BuildRequest, Context, DeploymentPlan, Environment, Group, GroupKind,
    Member, Path, PathBuf, Result, Work, artifacts, ensure, prepared,
};

pub(super) fn validate_plan(plan: &DeploymentPlan) -> Result<()> {
    ensure!(
        plan.nodes.is_empty() || (plan.components.is_empty() && plan.component_hosts.is_empty()),
        "mixed hosted and legacy deployment"
    );
    ensure!(
        plan.components.is_empty() == plan.component_hosts.is_empty(),
        "incomplete hosted deployment"
    );
    let mut names = BTreeSet::new();
    let mut hosts = BTreeSet::new();
    for component in &plan.components {
        ensure!(
            names.insert(&component.namespace),
            "duplicate component placement"
        );
        ensure!(
            plan.component_hosts.contains_key(&component.host),
            "unknown component host"
        );
        hosts.insert(&component.host);
    }
    ensure!(
        hosts.len() == plan.component_hosts.len(),
        "unused component host"
    );
    for host in plan.component_hosts.values() {
        ensure!(
            host.spec.restart == crate::RestartPolicy::Manual
                && host.provider.kind == crate::ProviderKind::Process,
            "hosted components require a process provider and manual recovery"
        );
    }
    Ok(())
}

pub(super) fn groups(
    plan: &DeploymentPlan,
    base: &Path,
    environment: &Environment,
) -> Result<(BTreeMap<String, Group>, Vec<Group>)> {
    let mut builds: BTreeMap<String, Group> = BTreeMap::new();
    let mut configs = Vec::new();
    for (name, host) in &plan.component_hosts {
        artifacts::validate_executable_name(&host.spec.bin)?;
        let (id, kind, work) = if let Some(spec) = &host.spec.build {
            let request = BuildRequest::new(spec, &host.module_dir, base, environment)?;
            match &request.spec {
                artifacts::BuildSpec::Command(command) => ensure!(
                    command.outputs.contains_key(&host.spec.bin),
                    "host {name}: command output does not declare {}",
                    host.spec.bin
                ),
                artifacts::BuildSpec::Cargo(_) => {}
            }
            (
                format!("host-build-{}", request.key),
                request.kind(),
                Work::Build(Box::new(request)),
            )
        } else {
            let directory = host
                .provider
                .bin_dir
                .canonicalize()
                .context("host prebuilt directory")?;
            ensure!(
                directory
                    .join(artifacts::executable_name(&host.spec.bin))
                    .is_file(),
                "host {name}: prebuilt executable is missing"
            );
            (
                format!(
                    "host-prebuilt-{}",
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
        group.members.push(Member::Host(name.clone()));
        group.bins.insert(host.spec.bin.clone());
        if let Some(config) = &host.provider.zenoh_config {
            configs.push(config_group(
                config,
                &[],
                (format!("host-config-{name}"), Member::Host(name.clone())),
            )?);
        }
    }
    for (index, component) in plan.components.iter().enumerate() {
        ensure!(
            component.component.config.is_some() || component.component.config_assets.is_empty(),
            "component config assets require a selected config"
        );
        if let Some(config) = &component.component.config {
            configs.push(config_group(
                config,
                &component.component.config_assets,
                (
                    format!("component-config-{}", component.namespace),
                    Member::Component(index),
                ),
            )?);
        }
    }
    Ok((builds, configs))
}

fn config_group(config: &Path, assets: &[PathBuf], identity: (String, Member)) -> Result<Group> {
    let (files, entry) = prepared::config_files(config, assets)?;
    Ok(Group {
        id: identity.0,
        kind: GroupKind::Config,
        members: vec![identity.1],
        bins: BTreeSet::new(),
        work: Work::Config {
            files,
            entry,
            zenoh: false,
        },
    })
}
