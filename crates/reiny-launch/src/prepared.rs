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
            version: crate::PREPARED_FILE_VERSION,
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
        let envelope: Envelope = serde_json::from_slice(&std::fs::read(path)?)
            .with_context(|| format!("parse prepared file {}", path.display()))?;
        ensure!(
            envelope.version == crate::PREPARED_FILE_VERSION,
            "unsupported prepared file version {}",
            envelope.version
        );
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

    /// Validate metadata, runtime contracts, paths, fingerprints and frozen bytes.
    ///
    /// Original build outputs/config sources are not consulted. Application
    /// working directories must still exist. Integrity hashes are not signatures.
    pub fn verify(&self) -> Result<()> {
        self.verify_with(&crate::CancelToken::default())
    }

    pub(crate) fn verify_with(&self, cancel: &crate::CancelToken) -> Result<()> {
        cancel.check()?;
        crate::deployment::validate_prepared(self)?;
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

fn contained_file(directory: &Path, path: &Path) -> Result<()> {
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

pub(crate) fn config_files(
    config: &Path,
    assets: &[PathBuf],
) -> Result<(BTreeMap<PathBuf, PathBuf>, PathBuf)> {
    let config = lexical_absolute(config)?;
    let parent = config.parent().context("config has no parent directory")?;
    let mut sources = vec![config.clone()];
    let mut root = parent.to_owned();
    for asset in assets {
        ensure!(
            !asset.as_os_str().is_empty()
                && !asset.has_root()
                && asset
                    .components()
                    .all(|part| !matches!(part, Component::Prefix(_))),
            "config asset must be relative to the selected config: {}",
            asset.display()
        );
        let source = lexical_absolute(&parent.join(asset))?;
        ensure!(
            !sources.contains(&source),
            "conflicting config bundle path {}",
            asset.display()
        );
        while !source.starts_with(&root) {
            ensure!(root.pop(), "config files have no common filesystem root");
        }
        sources.push(source);
    }
    let entry = config.strip_prefix(&root)?.to_owned();
    let mut files = BTreeMap::new();
    for source in sources {
        ensure!(
            source.is_file(),
            "config file not found: {}",
            source.display()
        );
        files.insert(source.strip_prefix(&root)?.to_owned(), source);
    }
    Ok((files, entry))
}

pub(crate) fn freeze_config(
    files: &BTreeMap<PathBuf, PathBuf>,
    entry: &Path,
    cache: &Path,
    cancel: &crate::CancelToken,
) -> Result<(PathBuf, PathBuf)> {
    let staged = artifacts::stage_files_with(files, cache, entry, cancel)?;
    let bundle = staged
        .ancestors()
        .nth(entry.components().count())
        .context("staged config has no bundle directory")?
        .to_owned();
    Ok((staged, bundle))
}

pub(crate) fn lexical_absolute(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => ensure!(
                normalized.pop(),
                "config path traverses beyond filesystem root: {}",
                path.display()
            ),
        }
    }
    ensure!(normalized.is_absolute(), "config source is not absolute");
    Ok(normalized)
}

pub(crate) fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(crate) fn digest_file(path: &Path) -> Result<String> {
    Ok(digest(&std::fs::read(path)?))
}
