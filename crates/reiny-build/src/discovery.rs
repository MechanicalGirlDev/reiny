//! Build-time discovery deliberately ignores runtime providers and composition.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use super::Manifest;

#[derive(Deserialize)]
struct MainManifest {
    version: u32,
    schema: Option<Manifest>,
}

/// Find the nearest ancestor catalog, skipping runtime-only module manifests.
pub(super) fn find_manifest(start: &Path) -> Result<(PathBuf, Manifest)> {
    for dir in start.ancestors() {
        let candidate = dir.join("main.yaml");
        if candidate.is_file() {
            super::rerun_if_changed(&candidate);
            if let Some(schema) = read_main(&candidate)? {
                return Ok((candidate, schema));
            }
        }
    }
    bail!(
        "main.yaml containing schema not found in {} or any parent directory",
        start.display()
    )
}

/// A dependency must provide its own public catalog, not inherit another one.
pub(super) fn parse_manifest(path: &Path) -> Result<Manifest> {
    read_main(path)?.with_context(|| format!("{} has no schema block", path.display()))
}

fn read_main(path: &Path) -> Result<Option<Manifest>> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let manifest: MainManifest =
        serde_yaml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    if manifest.version != 1 {
        bail!(
            "{}: unsupported main.yaml version {} (expected 1)",
            path.display(),
            manifest.version
        );
    }
    Ok(manifest.schema)
}
