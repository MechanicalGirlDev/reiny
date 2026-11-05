use super::{
    Context, Path, PreparedDeployment, Result, artifacts, ensure, fingerprint, lexical_absolute,
};

impl PreparedDeployment {
    /// Validate metadata, runtime contracts, paths, fingerprints and frozen bytes.
    ///
    /// Original build outputs/config sources are not consulted. Application
    /// working directories must still exist. Integrity hashes are not signatures.
    pub fn verify(&self) -> Result<()> {
        self.verify_with(&crate::CancelToken::default())
    }

    pub(crate) fn verify_with(&self, cancel: &crate::CancelToken) -> Result<()> {
        cancel.check()?;
        if self.components.is_empty() && self.component_hosts.is_empty() {
            crate::deployment::validate_prepared(self)?;
        } else {
            self.verify_hosted(cancel)?;
        }
        ensure!(
            self.root.is_absolute() && self.root.is_dir(),
            "invalid prepared root"
        );
        reiny_core::bindings::validate_namespace(&self.deployment)
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        reiny_core::bindings::validate_namespace(&self.domain)
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        let mut directories = std::collections::BTreeSet::new();
        for node in &self.nodes {
            cancel.check()?;
            node.bindings
                .validate()
                .map_err(|error| anyhow::anyhow!("{}: {error}", node.namespace))?;
            ensure!(
                node.provider.kind == crate::ProviderKind::Process,
                "executable requires process provider"
            );
            ensure!(
                node.provider.bin_dir.is_absolute(),
                "provider bin directory is not absolute"
            );
            let requested: std::collections::BTreeSet<_> = std::iter::once(&node.run.bin)
                .chain(&node.run.companions)
                .collect();
            let bound: std::collections::BTreeSet<_> = node.bindings.executables.iter().collect();
            ensure!(
                requested == bound,
                "prepared executable declarations do not match bindings"
            );
            for bin in requested {
                artifacts::validate_executable_name(bin)?;
                contained_file(
                    &node.bundle_dir,
                    &node.bundle_dir.join(artifacts::executable_name(bin)),
                )?;
            }
            ensure!(
                node.executable
                    == node
                        .bundle_dir
                        .join(artifacts::executable_name(&node.run.bin)),
                "prepared executable does not match run.bin"
            );
            directories.insert(node.bundle_dir.clone());
            match (&node.run.config, &node.config_bundle_dir, &node.config_dir) {
                (Some(config), Some(bundle), Some(origin)) => {
                    contained_file(bundle, config)?;
                    for asset in &node.run.config_assets {
                        ensure!(
                            !asset.is_absolute() && !asset.has_root(),
                            "config asset is not relative"
                        );
                        contained_file(bundle, &lexical_absolute(&origin.join(asset))?)?;
                    }
                    directories.insert(bundle.clone());
                }
                (None, None, None) => ensure!(
                    node.run.config_assets.is_empty(),
                    "config assets require a config"
                ),
                _ => anyhow::bail!("prepared config bundle metadata is inconsistent"),
            }
            if let Some(config) = &node.provider.zenoh_config {
                let bundle = config
                    .parent()
                    .context("Zenoh config has no bundle directory")?;
                contained_file(bundle, config)?;
                directories.insert(bundle.to_owned());
            }
            ensure!(
                fingerprint(node, &self.domain)? == node.fingerprint,
                "prepared module {} fingerprint mismatch",
                node.namespace
            );
        }
        for (address, resource) in &self.resources {
            reiny_core::bindings::validate_namespace(address)
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            ensure!(
                address.starts_with(&format!("{}/", self.deployment)),
                "resource outside deployment"
            );
            let bundle = resource
                .parent()
                .context("resource has no bundle directory")?;
            contained_file(bundle, resource)?;
            directories.insert(bundle.to_owned());
        }
        for directory in directories {
            artifacts::verify_directory(&directory, cancel)?;
        }
        Ok(())
    }
}

pub(super) fn contained_file(directory: &Path, path: &Path) -> Result<()> {
    ensure!(
        directory.is_absolute() && path.is_absolute() && path.is_file(),
        "missing absolute prepared file: {}",
        path.display()
    );
    let canonical_directory = directory.canonicalize()?;
    let canonical_path = path.canonicalize()?;
    ensure!(
        path.starts_with(directory) && canonical_path.starts_with(&canonical_directory),
        "prepared file escapes bundle: {}",
        path.display()
    );
    Ok(())
}
