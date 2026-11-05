use super::{
    BTreeMap, Context, DeploymentPlan, Group, Member, Output, PathBuf, PreparedDeployment,
    PreparedModule, Result, Work, prepared,
};

pub(super) fn assemble<'a>(
    plan: &DeploymentPlan,
    root: PathBuf,
    outputs: impl Iterator<Item = (&'a Group, Output)>,
) -> Result<PreparedDeployment> {
    if !plan.components.is_empty() {
        return super::hosted_assembly::assemble(plan, root, outputs);
    }
    let mut nodes = Vec::with_capacity(plan.nodes.len());
    for node in &plan.nodes {
        nodes.push(PreparedModule {
            namespace: node.namespace.clone(),
            module_dir: node.module_dir.clone(),
            executable: PathBuf::new(),
            bundle_dir: PathBuf::new(),
            config_dir: None,
            config_bundle_dir: None,
            run: node.run.clone(),
            provider: node.provider.clone(),
            bindings: node.bindings.clone(),
            fingerprint: String::new(),
        });
    }
    let mut resources = BTreeMap::new();
    for (group, output) in outputs {
        match output {
            Output::Build(built) => {
                for member in &group.members {
                    let Member::Module(index) = member else {
                        anyhow::bail!("hosted artifact in legacy assembly");
                    };
                    let node = &mut nodes[*index];
                    node.executable = built
                        .executables
                        .get(&node.run.bin)
                        .context("missing prepared executable")?
                        .clone();
                    node.bundle_dir = node
                        .executable
                        .parent()
                        .context("bundle directory")?
                        .to_owned();
                }
            }
            Output::Config(path, bundle) => {
                for member in &group.members {
                    let Member::Module(index) = member else {
                        anyhow::bail!("hosted config in legacy assembly");
                    };
                    let node = &mut nodes[*index];
                    if matches!(group.work, Work::Config { zenoh: true, .. }) {
                        node.provider.zenoh_config = Some(path.clone());
                    } else {
                        node.run.config = Some(path.clone());
                        node.config_dir = Some(path.parent().context("config parent")?.to_owned());
                        node.config_bundle_dir = Some(bundle.clone());
                    }
                }
            }
            Output::Resource(path) => {
                if let Work::Resource(index) = group.work {
                    resources.insert(plan.resources[index].address.clone(), path);
                }
            }
        }
    }
    for node in &mut nodes {
        node.fingerprint = prepared::fingerprint(node, &plan.domain)?;
    }
    let prepared = PreparedDeployment {
        root,
        deployment: plan.deployment.clone(),
        domain: plan.domain.clone(),
        nodes,
        components: Vec::new(),
        component_hosts: BTreeMap::new(),
        resources,
    };
    Ok(prepared)
}
