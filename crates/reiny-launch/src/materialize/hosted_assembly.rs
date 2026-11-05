use crate::prepared::{PreparedComponent, PreparedComponentHost};

use super::{
    BTreeMap, Context, DeploymentPlan, Group, Member, Output, PathBuf, PreparedDeployment, Result,
    Work, prepared,
};

pub(super) fn assemble<'a>(
    plan: &DeploymentPlan,
    root: PathBuf,
    outputs: impl Iterator<Item = (&'a Group, Output)>,
) -> Result<PreparedDeployment> {
    let mut hosts: BTreeMap<_, _> = plan
        .component_hosts
        .iter()
        .map(|(name, host)| {
            let mut instances: Vec<_> = plan
                .components
                .iter()
                .filter(|component| &component.host == name)
                .map(|component| component.namespace.clone())
                .collect();
            instances.sort();
            (
                name.clone(),
                PreparedComponentHost {
                    module_dir: host.module_dir.clone(),
                    executable: PathBuf::new(),
                    bundle_dir: PathBuf::new(),
                    spec: host.spec.clone(),
                    provider: host.provider.clone(),
                    instances,
                    fingerprint: String::new(),
                },
            )
        })
        .collect();
    let mut components: Vec<_> = plan
        .components
        .iter()
        .map(|component| PreparedComponent {
            namespace: component.namespace.clone(),
            module_dir: component.module_dir.clone(),
            component: component.component.clone(),
            host: component.host.clone(),
            config_dir: None,
            config_bundle_dir: None,
            bindings: component.bindings.clone(),
            fingerprint: String::new(),
        })
        .collect();
    let mut resources = BTreeMap::new();
    for (group, output) in outputs {
        match output {
            Output::Build(built) => {
                for member in &group.members {
                    let Member::Host(name) = member else {
                        anyhow::bail!("non-host build in hosted assembly");
                    };
                    let host = hosts.get_mut(name).context("unknown build host")?;
                    host.executable = built
                        .executables
                        .get(&host.spec.bin)
                        .context("missing exact host executable")?
                        .clone();
                    host.bundle_dir = host.executable.parent().context("host bundle")?.to_owned();
                    host.provider.bin_dir = host.bundle_dir.clone();
                }
            }
            Output::Config(path, bundle) => {
                for member in &group.members {
                    match member {
                        Member::Host(name) => {
                            let host = hosts.get_mut(name).context("unknown config host")?;
                            host.provider.zenoh_config = Some(path.clone());
                        }
                        Member::Component(index) => {
                            let component = &mut components[*index];
                            component.component.config = Some(path.clone());
                            component.config_dir =
                                Some(path.parent().context("config origin")?.to_owned());
                            component.config_bundle_dir = Some(bundle.clone());
                        }
                        Member::Module(_) => anyhow::bail!("legacy config in hosted assembly"),
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
    for (name, host) in &mut hosts {
        host.fingerprint = prepared::host_fingerprint(name, host, &plan.domain)?;
    }
    for component in &mut components {
        let host = hosts.get(&component.host).context("unassigned component")?;
        component.fingerprint =
            prepared::component_fingerprint(component, &host.fingerprint, &plan.domain)?;
    }
    Ok(PreparedDeployment {
        root,
        deployment: plan.deployment.clone(),
        domain: plan.domain.clone(),
        nodes: Vec::new(),
        components,
        component_hosts: hosts,
        resources,
    })
}
