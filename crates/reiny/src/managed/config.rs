//! Parsing the managed startup boundary and publishing its atomic runtime report.

use std::io::Write;
use std::path::Path;

use anyhow::Context;
use reiny_core::bindings::{ModuleBindings, ModuleReport};

use crate::Result;

/// Validate a canonical path without flattening its module segments.
pub(crate) fn validate_namespace(value: &str) -> Result<()> {
    reiny_core::bindings::validate_namespace(value)?;
    Ok(())
}

pub(crate) fn validate_bindings(bindings: &ModuleBindings, id: &str) -> Result<()> {
    bindings.validate()?;
    anyhow::ensure!(
        bindings.namespace == id,
        "module bindings namespace '{}' differs from --name '{id}'",
        bindings.namespace
    );
    Ok(())
}

pub(crate) fn read_bindings(path: &Path) -> Result<ModuleBindings> {
    anyhow::ensure!(
        path.is_absolute(),
        "--module-bindings requires an absolute path"
    );
    let bytes = std::fs::read(path)
        .with_context(|| format!("reading module bindings {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing module bindings {}", path.display()))
}

pub(crate) fn write_report(path: &Path, report: &ModuleReport) -> Result<()> {
    let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
    let bytes = serde_json::to_vec(report)?;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .with_context(|| format!("creating module report {}", temporary.display()))?;
    let result = (|| -> Result<()> {
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err()
        && let Err(error) = std::fs::remove_file(&temporary)
    {
        tracing::warn!(path = %temporary.display(), %error, "module report cleanup failed");
    }
    result.with_context(|| format!("publishing module report {}", path.display()))
}
