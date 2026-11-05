use super::{BTreeSet, MODULE_BINDINGS_VERSION, PreparedDeployment, Result, bail, ensure, hosted};

pub(crate) fn validate_prepared(prepared: &PreparedDeployment) -> Result<()> {
    if !prepared.components.is_empty() || !prepared.component_hosts.is_empty() {
        return hosted::validate(prepared);
    }
    ensure!(
        !prepared.deployment.is_empty() && !prepared.domain.is_empty(),
        "empty deployment identity"
    );
    let mut names = BTreeSet::new();
    for node in &prepared.nodes {
        node.run.validate(&node.namespace)?;
        ensure!(
            names.insert(&node.namespace),
            "duplicate module namespace {}",
            node.namespace
        );
        ensure!(
            node.namespace
                .starts_with(&format!("{}/", prepared.deployment))
                || node.namespace == prepared.deployment,
            "module namespace is outside deployment"
        );
        ensure!(
            node.bindings.version == MODULE_BINDINGS_VERSION
                && node.bindings.namespace == node.namespace
                && node.bindings.endpoint_namespace == node.namespace,
            "invalid module bindings identity"
        );
        ensure!(!node.fingerprint.is_empty(), "missing prepared fingerprint");
        ensure!(
            node.executable.is_absolute() && node.executable.is_file(),
            "prepared executable is missing: {}",
            node.executable.display()
        );
        ensure!(
            node.bundle_dir.is_absolute()
                && node.bundle_dir.is_dir()
                && node.executable.parent() == Some(node.bundle_dir.as_path())
                && std::fs::canonicalize(&node.executable)?.parent()
                    == Some(std::fs::canonicalize(&node.bundle_dir)?.as_path()),
            "prepared executable is outside its exact bundle directory"
        );
        match (&node.run.config, &node.config_dir) {
            (Some(config), Some(directory)) => {
                ensure!(
                    directory.is_absolute()
                        && directory.is_dir()
                        && config.is_absolute()
                        && config.is_file()
                        && config.parent() == Some(directory.as_path()),
                    "prepared config is outside its exact config directory"
                );
            }
            (None, None) => {}
            (Some(_), None) | (None, Some(_)) => {
                bail!("prepared config directory does not match application config")
            }
        }
        ensure!(
            node.module_dir.is_absolute() && node.module_dir.is_dir(),
            "invalid module directory"
        );
    }
    Ok(())
}
