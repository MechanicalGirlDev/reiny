//! Parse startup contracts before opening the engine.

use std::path::Path;

use crate::managed::{MODULE_REPORT_ENV, config as managed_config};
use crate::shutdown::Shutdown;
use crate::{Cloudy, Result, RuntimeOptions, validate_segment};

impl Cloudy {
    /// Open the engine from the options (or take `opts.engine`) and build a `Cloudy`.
    ///
    /// Call it inside a tokio runtime. It watches no signals — it is the entry point for slotting
    /// [`crate::engine::Local`] into a `#[tokio::test]` and running a launch, and the one a bridge uses
    /// to open its second `Cloudy`. The process's entry point is [`run_with`].
    pub async fn open(mut opts: RuntimeOptions) -> Result<Self> {
        managed_config::validate_namespace(&opts.id)?;
        validate_segment("--domain", &opts.domain)?;
        let managed = opts.bindings_requested
            || opts.module_bindings.is_some()
            || opts.module_bindings_path.is_some();
        if managed {
            anyhow::ensure!(
                opts.argument_errors.is_empty(),
                "{}",
                opts.argument_errors.join("; ")
            );
        }
        anyhow::ensure!(
            !(opts.module_bindings.is_some() && opts.module_bindings_path.is_some()),
            "supply module bindings either directly or by path, not both"
        );
        let bindings = match opts.module_bindings.take() {
            Some(bindings) => Some(bindings),
            None => opts
                .module_bindings_path
                .as_deref()
                .map(managed_config::read_bindings)
                .transpose()?,
        };
        if let Some(bindings) = &bindings {
            managed_config::validate_bindings(bindings, &opts.id)?;
        }
        if let Some(path) = &opts.module_report_path {
            anyhow::ensure!(
                path.is_absolute(),
                "{MODULE_REPORT_ENV} requires an absolute path"
            );
            anyhow::ensure!(
                bindings.is_some(),
                "{MODULE_REPORT_ENV} requires module bindings"
            );
        }
        let config = if managed {
            opts.config_path
                .as_deref()
                .map(|path| {
                    let text = std::fs::read_to_string(path).map_err(|error| {
                        anyhow::anyhow!("reading --config {}: {error}", path.display())
                    })?;
                    parse_config(path, &text)
                })
                .transpose()?
        } else {
            load_config(opts.config_path.as_deref())
        };
        let engine = opts.take_engine().await?;
        tracing::info!(id = %opts.id, domain = %opts.domain, "reiny launch up");
        let mut cloudy = Self::new(
            engine,
            opts.id,
            opts.domain,
            Shutdown::new(),
            config,
            opts.extra_args,
        )
        .await?;
        cloudy.configure_module(bindings, opts.module_report_path)?;
        Ok(cloudy)
    }
}

/// Read `--config <path>` and parse it into a TOML table. Unreadable or broken: warn and return `None`
/// (= use only `[config]`'s defaults).
fn load_config(path: Option<&Path>) -> Option<toml::Table> {
    let path = path?;
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "ignoring unreadable --config");
            return None;
        }
    };
    match parse_config(path, &text) {
        Ok(table) => Some(table),
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "ignoring unparsable --config");
            None
        }
    }
}

/// YAML is the format; `.toml` and `.json` are read by extension. All three deserialize into the
/// same `toml::Table`, so the generated `config()` never sees the difference. A YAML `null` has no
/// TOML counterpart and rejects the whole file.
pub(super) fn parse_config(path: &Path, text: &str) -> Result<toml::Table> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase);
    Ok(match ext.as_deref() {
        Some("toml") => text.parse::<toml::Table>()?,
        Some("json") => serde_json::from_str(text)?,
        _ => serde_yaml::from_str(text)?,
    })
}
