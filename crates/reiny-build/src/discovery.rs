//! Build-time discovery deliberately ignores runtime providers and composition.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use super::Manifest;

#[derive(Deserialize)]
struct CatalogManifest {
    version: u32,
    schema: Manifest,
}

/// Find the nearest ancestor schema.yaml, without reading runtime manifests.
pub(super) fn find_manifest(start: &Path) -> Result<(PathBuf, Manifest)> {
    for dir in start.ancestors() {
        let candidate = dir.join("schema.yaml");
        if candidate.is_file() {
            super::rerun_if_changed(&candidate);
            return Ok((candidate.clone(), parse_manifest(&candidate)?));
        }
    }
    bail!(
        "schema.yaml not found in {} or any parent directory",
        start.display()
    )
}

/// A dependency must provide its own public catalog, not inherit another one.
pub(super) fn parse_manifest(path: &Path) -> Result<Manifest> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let manifest: CatalogManifest =
        serde_yaml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    if manifest.version != 2 {
        bail!(
            "{}: unsupported schema.yaml version {} (expected 2)",
            path.display(),
            manifest.version
        );
    }
    Ok(manifest.schema)
}
