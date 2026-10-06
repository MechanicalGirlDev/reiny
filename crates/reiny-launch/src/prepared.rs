//! Materialize a validated deployment without starting any processes.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use reiny_core::bindings::ModuleBindings;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::artifacts;
use crate::modules::{DeploymentPlan, ProviderSpec, RunSpec};

/// One immutable, prepared executable module.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedModule {
    /// Canonical executable namespace.
    pub namespace: String,
    /// Application working directory.
    pub module_dir: PathBuf,
    /// Exact compiler artifact or installed executable.
    pub executable: PathBuf,
    /// Resolved startup and lifecycle settings.
    pub run: RunSpec,
    /// Resolved process provider.
    pub provider: ProviderSpec,
    /// Exact input/output contract.
    pub bindings: ModuleBindings,
    /// Content and startup fingerprint used for delta application.
    pub fingerprint: String,
}

/// A deployment whose artifacts and configurations have been checked.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedDeployment {
    /// Root module directory.
    pub root: PathBuf,
    /// Stable deployment identity.
    pub deployment: String,
    /// Shared communication domain.
    pub domain: String,
    /// All prepared executable modules.
    pub nodes: Vec<PreparedModule>,
    /// Managed file artifacts keyed by stable address.
    pub resources: std::collections::BTreeMap<String, PathBuf>,
}

impl DeploymentPlan {
    /// Prepare files and Cargo artifacts. Never spawn application processes.
    pub fn prepare(&self) -> Result<PreparedDeployment> {
        let cache = self.root.join(".reiny");
        std::fs::create_dir_all(&cache)?;
        let mut resources = std::collections::BTreeMap::new();
        for resource in &self.resources {
            let bytes = std::fs::read(&resource.source)
                .with_context(|| format!("reading artifact {}", resource.source.display()))?;
            let digest = digest(&bytes);
            if let Some(expected) = &resource.sha256
                && &digest != expected
            {
                anyhow::bail!(
                    "artifact {} SHA-256 mismatch: expected {expected}, found {digest}",
                    resource.address
                );
            }
            let filename = resource
                .source
                .file_name()
                .context("artifact source has no filename")?;
            let dir = cache.join("artifacts").join(&digest);
            std::fs::create_dir_all(&dir)?;
            let destination = dir.join(filename);
            if !destination.exists() {
                std::fs::copy(&resource.source, &destination)?;
            } else if digest_file(&destination)? != digest {
                anyhow::bail!("cached artifact {} has changed", destination.display());
            }
            resources.insert(resource.address.clone(), destination);
        }
        let mut nodes = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            let executable = if let Some(build) = &node.build {
                artifacts::prepare(build, &node.module_dir, &node.run.bin, &cache.join("build"))?
            } else {
                let path = node.provider.bin_dir.join(format!(
                    "{}{}",
                    node.run.bin,
                    std::env::consts::EXE_SUFFIX
                ));
                let path = std::fs::canonicalize(&path)
                    .with_context(|| format!("prepared binary not found: {}", path.display()))?;
                artifacts::stage_prebuilt(&path, &cache.join("build"))?
            };
            let mut hash = Sha256::new();
            hash.update(std::fs::read(&executable)?);
            hash.update(executable.to_string_lossy().as_bytes());
            hash.update(serde_json::to_vec(&(
                &node.module_dir,
                &node.run,
                &node.provider,
                &node.bindings,
                &self.domain,
            ))?);
            if let Some(config) = &node.run.config {
                hash.update(
                    std::fs::read(config)
                        .with_context(|| format!("reading managed config {}", config.display()))?,
                );
            }
            nodes.push(PreparedModule {
                namespace: node.namespace.clone(),
                module_dir: node.module_dir.clone(),
                executable,
                run: node.run.clone(),
                provider: node.provider.clone(),
                bindings: node.bindings.clone(),
                fingerprint: format!("{:x}", hash.finalize()),
            });
        }
        Ok(PreparedDeployment {
            root: self.root.clone(),
            deployment: self.deployment.clone(),
            domain: self.domain.clone(),
            nodes,
            resources,
        })
    }
}

pub(crate) fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(crate) fn digest_file(path: &Path) -> Result<String> {
    Ok(digest(&std::fs::read(path)?))
}
