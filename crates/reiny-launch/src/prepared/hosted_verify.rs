use std::collections::{BTreeMap, BTreeSet};

use super::verify::contained_file;
use super::{
    Context, PathBuf, PreparedComponent, PreparedComponentHost, PreparedDeployment, Result,
    artifacts, component_fingerprint, ensure, host_fingerprint, lexical_absolute,
};

impl PreparedDeployment {
    pub(super) fn verify_hosted(&self, cancel: &crate::CancelToken) -> Result<()> {
        ensure!(self.nodes.is_empty(), "mixed hosted and legacy deployment");
        ensure!(
            !self.components.is_empty() && !self.component_hosts.is_empty(),
            "incomplete hosted deployment"
        );
        let mut instances = BTreeMap::new();
        let mut assignments: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        let mut directories = BTreeSet::new();
        for component in &self.components {
            cancel.check()?;
            ensure!(
                instances
                    .insert(component.namespace.as_str(), component)
                    .is_none(),
                "duplicate component namespace {}",
                component.namespace
            );
            ensure!(
                component.namespace == self.deployment
                    || component
                        .namespace
                        .starts_with(&format!("{}/", self.deployment)),
                "component namespace is outside deployment"
            );
            component
                .bindings
                .validate()
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            ensure!(
                component.bindings.namespace == component.namespace
                    && component.bindings.endpoint_namespace == component.namespace
                    && component.bindings.children.is_empty()
                    && component.bindings.executables.is_empty(),
                "invalid hosted component bindings identity or authority"
            );
            declaration_name(&component.component.factory)?;
            ensure!(
                !component.component.factory_revision.trim().is_empty(),
                "empty component factory revision"
            );
            ensure!(
                component.module_dir.is_absolute() && component.module_dir.is_dir(),
                "invalid component module directory"
            );
            let host = self
                .component_hosts
                .get(&component.host)
                .context("unknown component host")?;
            assignments
                .entry(&component.host)
                .or_default()
                .push(&component.namespace);
            verify_config(component, &mut directories)?;
            ensure!(
                component_fingerprint(component, &host.fingerprint, &self.domain)?
                    == component.fingerprint,
                "prepared component {} fingerprint mismatch",
                component.namespace
            );
        }
        for (name, host) in &self.component_hosts {
            cancel.check()?;
            let mut assigned = assignments
                .remove(name.as_str())
                .context("unused component host")?;
            assigned.sort_unstable();
            let declared: Vec<_> = host.instances.iter().map(String::as_str).collect();
            ensure!(
                assigned == declared,
                "host instance set does not match exact placement"
            );
            directories.extend(verify_host(name, host, &self.domain)?);
        }
        self.verify_hosted_edges(&instances)?;
        for directory in directories {
            artifacts::verify_directory(&directory, cancel)?;
        }
        Ok(())
    }

    fn verify_hosted_edges(&self, instances: &BTreeMap<&str, &PreparedComponent>) -> Result<()> {
        for component in &self.components {
            for input in component.bindings.inputs.values() {
                for source in &input.sources {
                    let source_component = instances
                        .get(source.namespace.as_str())
                        .context("hosted input source outside deployment")?;
                    ensure!(
                        source_component.host == component.host
                            || [&source_component.host, &component.host]
                                .iter()
                                .all(|host| {
                                    self.component_hosts[*host].spec.transport
                                        != crate::ComponentTransport::Local
                                }),
                        "edge crosses a local-only host boundary"
                    );
                    let output = source_component
                        .bindings
                        .outputs
                        .get(&source.port)
                        .context("unknown hosted source port")?;
                    ensure!(
                        output.contract.type_name == input.contract.type_name
                            && output.contract.kind == input.contract.kind
                            && output.contract.response == input.contract.response
                            && (input.contract.replay != reiny_core::bindings::Replay::Last
                                || output.contract.retention
                                    == reiny_core::bindings::Retention::Last),
                        "hosted input source contract mismatch"
                    );
                }
            }
        }
        Ok(())
    }
}

fn verify_host(
    name: &str,
    host: &PreparedComponentHost,
    domain: &str,
) -> Result<BTreeSet<PathBuf>> {
    ensure!(
        host.spec.transport != crate::ComponentTransport::Local
            || (host.provider.connect.is_empty() && host.provider.zenoh_config.is_none()),
        "local host forbids Zenoh provider settings"
    );
    for name in [name, &host.spec.bin, &host.spec.provider] {
        declaration_name(name)?;
    }
    ensure!(
        host.spec.restart == crate::RestartPolicy::Manual,
        "hosted components require manual whole-graph recovery"
    );
    ensure!(
        host.module_dir.is_absolute() && host.module_dir.is_dir(),
        "invalid host module directory"
    );
    ensure!(
        host.provider.kind == crate::ProviderKind::Process
            && host.provider.bin_dir == host.bundle_dir,
        "host provider does not name its frozen process bundle"
    );
    ensure!(
        host.executable
            == host
                .bundle_dir
                .join(artifacts::executable_name(&host.spec.bin)),
        "prepared host executable does not match host bin"
    );
    contained_file(&host.bundle_dir, &host.executable)?;
    let mut directories = BTreeSet::from([host.bundle_dir.clone()]);
    if let Some(config) = &host.provider.zenoh_config {
        let directory = config.parent().context("host transport config parent")?;
        contained_file(directory, config)?;
        directories.insert(directory.to_owned());
    }
    ensure!(
        host_fingerprint(name, host, domain)? == host.fingerprint,
        "prepared host {name} fingerprint mismatch"
    );
    Ok(directories)
}

fn declaration_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty()
            && name != "in"
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte)),
        "invalid hosted declaration name {name}"
    );
    Ok(())
}

fn verify_config(component: &PreparedComponent, directories: &mut BTreeSet<PathBuf>) -> Result<()> {
    match (
        &component.component.config,
        &component.config_dir,
        &component.config_bundle_dir,
    ) {
        (Some(config), Some(origin), Some(bundle)) => {
            ensure!(
                config.parent() == Some(origin.as_path())
                    && origin.is_absolute()
                    && origin.canonicalize()?
                        == config.canonicalize()?.parent().context("config parent")?,
                "component config origin does not match its frozen config"
            );
            contained_file(bundle, config)?;
            let mut paths = BTreeSet::from([config.canonicalize()?]);
            for asset in &component.component.config_assets {
                ensure!(
                    !asset.as_os_str().is_empty() && !asset.is_absolute() && !asset.has_root(),
                    "component config asset is not relative"
                );
                let path = lexical_absolute(&origin.join(asset))?;
                contained_file(bundle, &path)?;
                ensure!(
                    paths.insert(path.canonicalize()?),
                    "duplicate component config asset"
                );
            }
            directories.insert(bundle.clone());
        }
        (None, None, None) => ensure!(
            component.component.config_assets.is_empty(),
            "component assets require a config"
        ),
        _ => anyhow::bail!("inconsistent component config metadata"),
    }
    Ok(())
}
