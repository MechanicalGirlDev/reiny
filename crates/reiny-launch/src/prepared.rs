//! Materialize a validated deployment without starting any processes.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, ensure};
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
    /// Immutable executable directory, including explicitly declared helpers.
    pub bundle_dir: PathBuf,
    /// Immutable selected configuration origin directory.
    pub config_dir: Option<PathBuf>,
    /// Immutable common-ancestor tree containing config and all declared assets.
    pub config_bundle_dir: Option<PathBuf>,
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
    /// Immutable hosted instances; never executed by the legacy process owner.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub components: Vec<PreparedComponent>,
    /// Immutable process hosts keyed by their placement identity.
    #[serde(
        default,
        deserialize_with = "hosted::deserialize_hosts",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub component_hosts: BTreeMap<String, PreparedComponentHost>,
    /// Managed file artifacts keyed by stable address.
    pub resources: std::collections::BTreeMap<String, PathBuf>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version: u32,
    reiny_version: String,
    prepare_protocol: u32,
    payload_sha256: String,
    deployment: PreparedDeployment,
}

impl PreparedDeployment {
    /// Verify and atomically publish a new public prepared file.
    /// An existing destination is never replaced.
    pub fn write_file(&self, path: &Path) -> Result<()> {
        use std::io::Write;
        ensure!(
            !path.exists(),
            "prepared output already exists: {}",
            path.display()
        );
        self.verify()?;
        let envelope = Envelope {
            version: if self.components.is_empty() {
                1
            } else {
                crate::PREPARED_FILE_VERSION
            },
            reiny_version: crate::VERSION.to_owned(),
            prepare_protocol: crate::PREPARE_PROTOCOL_VERSION,
            payload_sha256: digest(&serde_json::to_vec(self)?),
            deployment: self.clone(),
        };
        let parent = path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let mut staging = tempfile::Builder::new()
            .prefix(".prepared-")
            .tempfile_in(parent)?;
        serde_json::to_writer(&mut staging, &envelope)?;
        staging.write_all(b"\n")?;
        staging.as_file().sync_all()?;
        staging
            .persist_noclobber(path)
            .with_context(|| format!("publish new prepared output {}", path.display()))?;
        Ok(())
    }

    /// Load a supported envelope and verify its payload and all frozen bytes.
    /// This does not resolve a plan, invoke a builder, or repair artifacts.
    pub fn load_file(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path)?;
        let envelope: Envelope = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse prepared file {}", path.display()))?;
        ensure!(
            matches!(envelope.version, 1 | crate::PREPARED_FILE_VERSION),
            "unsupported prepared file version {}",
            envelope.version
        );
        ensure!(
            envelope.version != 1
                || (envelope.deployment.components.is_empty()
                    && envelope.deployment.component_hosts.is_empty()),
            "version 1 prepared files cannot contain hosted components"
        );
        ensure!(
            envelope.version != 2 || !envelope.deployment.components.is_empty(),
            "version 2 prepared files require hosted components"
        );
        if envelope.version == 1 {
            // New optional fields must not widen the original strict v1 shape.
            let raw: serde_json::Value = serde_json::from_slice(&bytes)?;
            ensure!(
                raw["deployment"].get("components").is_none()
                    && raw["deployment"].get("component_hosts").is_none(),
                "version 1 prepared files cannot declare hosted fields"
            );
        }
        ensure!(
            envelope.prepare_protocol == crate::PREPARE_PROTOCOL_VERSION,
            "unsupported prepare protocol {}",
            envelope.prepare_protocol
        );
        ensure!(
            envelope.reiny_version == crate::VERSION,
            "prepared file requires reiny {}, running {}",
            envelope.reiny_version,
            crate::VERSION
        );
        ensure!(
            digest(&serde_json::to_vec(&envelope.deployment)?) == envelope.payload_sha256,
            "prepared deployment payload SHA-256 mismatch"
        );
        envelope.deployment.verify()?;
        Ok(envelope.deployment)
    }
}

mod config;
mod hosted;
mod hosted_verify;
mod verify;
pub(crate) use config::{config_files, freeze_config, lexical_absolute};
pub use hosted::{PreparedComponent, PreparedComponentHost};
pub(crate) use hosted::{component_fingerprint, host_fingerprint};

pub(crate) fn fingerprint(node: &PreparedModule, domain: &str) -> Result<String> {
    let mut hash = Sha256::new();
    hash.update(std::fs::read(&node.executable)?);
    hash.update(node.executable.to_string_lossy().as_bytes());
    hash.update(serde_json::to_vec(&(
        &node.module_dir,
        &node.run,
        &node.provider,
        &node.bindings,
        domain,
    ))?);
    if let Some(config) = &node.run.config {
        hash.update(std::fs::read(config)?);
    }
    if let Some(config) = &node.provider.zenoh_config {
        hash.update(std::fs::read(config)?);
    }
    Ok(format!("{:x}", hash.finalize()))
}

impl DeploymentPlan {
    /// Prepare files and Cargo artifacts. Never spawn application processes.
    pub fn prepare(&self) -> Result<PreparedDeployment> {
        self.prepare_with(&crate::PrepareOptions::default())
            .map_err(Into::into)
    }

    /// Materialize exact compatible groups with progress and cancellation.
    /// All subprocess trees are reaped before this method returns.
    pub fn prepare_with(
        &self,
        options: &crate::PrepareOptions<'_>,
    ) -> Result<PreparedDeployment, crate::PrepareError> {
        crate::materialize::prepare(self, options)
    }
}

pub(crate) fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(crate) fn digest_file(path: &Path) -> Result<String> {
    Ok(digest(&std::fs::read(path)?))
}
