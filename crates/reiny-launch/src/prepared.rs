//! Materialize a validated deployment without starting any processes.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, ensure};
use reiny_core::bindings::ModuleBindings;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::artifacts;
use crate::modules::{DeploymentPlan, ExecutableModule, ProviderSpec, RunSpec};

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
                std::fs::write(&destination, &bytes)?;
            } else if digest_file(&destination)? != digest {
                anyhow::bail!("cached artifact {} has changed", destination.display());
            }
            resources.insert(resource.address.clone(), destination);
        }
        let mut nodes = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            let executable = prepare_executable(node, &cache.join("build"))?;
            let bundle_dir = executable
                .parent()
                .context("prepared executable has no bundle directory")?
                .to_owned();
            let mut run = node.run.clone();
            ensure!(
                run.config.is_some() || run.config_assets.is_empty(),
                "config_assets requires run.config"
            );
            let (config_dir, config_bundle_dir) = if let Some(config) = &mut run.config {
                let (staged, bundle) =
                    freeze_config(config, &run.config_assets, &cache.join("config"))?;
                *config = staged;
                (
                    Some(config.parent().context("config has no parent")?.to_owned()),
                    Some(bundle),
                )
            } else {
                (None, None)
            };
            let mut provider = node.provider.clone();
            if let Some(config) = &mut provider.zenoh_config {
                *config = freeze_config(config, &[], &cache.join("config"))?.0;
            }
            let mut hash = Sha256::new();
            hash.update(std::fs::read(&executable)?);
            hash.update(executable.to_string_lossy().as_bytes());
            hash.update(serde_json::to_vec(&(
                &node.module_dir,
                &run,
                &provider,
                &node.bindings,
                &self.domain,
            ))?);
            if let Some(config) = &run.config {
                hash.update(
                    std::fs::read(config)
                        .with_context(|| format!("reading managed config {}", config.display()))?,
                );
            }
            if let Some(config) = &provider.zenoh_config {
                hash.update(std::fs::read(config)?);
            }
            nodes.push(PreparedModule {
                namespace: node.namespace.clone(),
                module_dir: node.module_dir.clone(),
                executable,
                bundle_dir,
                config_dir,
                config_bundle_dir,
                run,
                provider,
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

fn prepare_executable(node: &ExecutableModule, cache: &Path) -> Result<PathBuf> {
    if let Some(build) = &node.build {
        if node.run.companions.is_empty() {
            artifacts::prepare(build, &node.module_dir, &node.run.bin, cache)
        } else {
            artifacts::prepare_bundle(
                build,
                &node.module_dir,
                &node.run.bin,
                &node.run.companions,
                cache,
            )
        }
    } else {
        let path =
            node.provider
                .bin_dir
                .join(format!("{}{}", node.run.bin, std::env::consts::EXE_SUFFIX));
        let path = std::fs::canonicalize(&path)
            .with_context(|| format!("prepared binary not found: {}", path.display()))?;
        if node.run.companions.is_empty() {
            artifacts::stage_prebuilt(&path, cache)
        } else {
            let companions: Vec<_> = node
                .run
                .companions
                .iter()
                .map(|bin| {
                    node.provider
                        .bin_dir
                        .join(format!("{bin}{}", std::env::consts::EXE_SUFFIX))
                })
                .collect();
            artifacts::stage_prebuilt_bundle(&path, &companions, cache)
        }
    }
}

fn freeze_config(config: &Path, assets: &[PathBuf], cache: &Path) -> Result<(PathBuf, PathBuf)> {
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
    std::fs::create_dir_all(cache)?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(cache.join("artifacts.lock"))?;
    let _lock = crate::file_lock::FileLock::new(lock)
        .context("another config preparer holds the cache lock")?;
    let staged = artifacts::stage_files(&files, cache, &entry)?;
    let bundle = staged
        .ancestors()
        .nth(entry.components().count())
        .context("staged config has no bundle directory")?
        .to_owned();
    Ok((staged, bundle))
}

fn lexical_absolute(path: &Path) -> Result<PathBuf> {
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
