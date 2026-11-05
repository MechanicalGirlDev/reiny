//! Materialize an executable, source-independent `main.yaml` deployment.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, ensure};
use reiny_launch::{
    DeploymentPlan, ModuleManifest, ModuleSource, PreparedDeployment, ProviderKind, ProviderSpec,
};

mod entry;
mod files;
mod manifest;
mod nodes;
mod paths;
#[cfg(test)]
mod tests;
pub(crate) use entry::bundle_with_control;
#[cfg(test)]
use files::is_system_lib;
use files::{
    collect_libs, copy_config_tree, copy_executable, copy_file, copy_tree, is_runtime_library,
};
use manifest::{bundle_config, finalize_bundle, write_manifests};
use nodes::bundle_nodes;
use paths::{instance_path, relative_path, relocate, source_origins};
