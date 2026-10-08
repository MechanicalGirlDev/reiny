//! `reiny check` resolves module schema and validates executable composition roots without building.
//!
//! No proto is compiled (`reiny-build` is used with `default-features = false`). A layout mistake (a
//! hyphenated dependency key, a topic collision, a missing proto) is caught here by `reiny-build`'s
//! validation, early. The output is for people; for CI, branch on the exit code.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

/// `reiny check [path]`. Search upward for the nearest `main.yaml`.
pub(crate) fn check(path: Option<&Path>) -> Result<()> {
    let dir = match path {
        Some(p) => p.to_path_buf(),
        None => std::env::current_dir()?,
    };

    let manifest = nearest_manifest(&dir)?;
    let text = std::fs::read_to_string(&manifest)
        .with_context(|| format!("reading {}", manifest.display()))?;
    let module: reiny_launch::ModuleManifest =
        serde_yaml::from_str(&text).with_context(|| format!("parsing {}", manifest.display()))?;
    if !matches!(module.version, 1 | 2) {
        bail!(
            "{}: unsupported module version {}",
            manifest.display(),
            module.version
        );
    }
    if module.deployment.is_some() {
        let plan = reiny_launch::DeploymentPlan::load(&manifest, false)?;
        println!(
            "deployment: {} ({} executable modules, {} resources)",
            plan.deployment,
            plan.nodes.len(),
            plan.resources.len(),
        );
    }
    if module.schema.is_none() && module.deployment.is_some() {
        return Ok(());
    }
    let schema_dir = manifest.parent().context("main.yaml has no directory")?;
    let resolution = reiny_build::describe(schema_dir)?;

    println!("reiny check — {}", resolution.manifest_path().display());
    println!("mode: {}", resolution.mode().label());
    let schema_crates = resolution.schema_crates();
    if !schema_crates.is_empty() {
        println!("schema crates ({}):", schema_crates.len());
        for c in &schema_crates {
            let part = c
                .part
                .as_deref()
                .map_or_else(|| " (single)".to_string(), |p| format!(" [schema.{p}]"));
            let deps = if c.depends.is_empty() {
                String::new()
            } else {
                format!(" — depends on {}", c.depends.join(", "))
            };
            println!("  {}{part}{deps}", c.crate_name);
        }
    }
    if resolution.has_config() {
        println!("config: schema.config present (typed cloudy.config())");
    }

    let services = resolution.services();
    if !services.is_empty() {
        println!();
        println!("services ({}):", services.len());
        let w_name = services.iter().map(|s| s.name.len()).max().unwrap_or(0);
        let w_req = services.iter().map(|s| s.request.len()).max().unwrap_or(0);
        for s in &services {
            println!(
                "  {:<w_name$}  {:<w_req$} ({})  ->  {} ({})",
                s.name, s.request, s.request_message, s.response, s.response_message
            );
        }
    }

    let types = resolution.types();
    println!();
    println!("types ({}):", types.len());
    if types.is_empty() {
        println!("  (none)");
        return Ok(());
    }

    // Line the columns up.
    let w_alias = types.iter().map(|t| t.alias.len()).max().unwrap_or(0);
    let w_msg = types.iter().map(|t| t.message.len()).max().unwrap_or(0);
    let w_topic = types
        .iter()
        .map(|t| t.topic_segment.len())
        .max()
        .unwrap_or(0);
    let w_mod = types.iter().map(|t| t.module.len()).max().unwrap_or(0);

    for t in &types {
        let proto = rel_to(resolution.manifest_path(), &t.proto);
        let owner = t
            .owner
            .as_deref()
            .map_or_else(String::new, |o| format!("  ({o})"));
        println!(
            "  {:<w_alias$}  {:<w_msg$}  ->  reiny/<domain>/<id>/{:<w_topic$}  [{:<w_mod$}]  {}{owner}",
            t.alias,
            t.message,
            t.topic_segment,
            t.module,
            proto.display(),
        );
    }

    Ok(())
}

fn nearest_manifest(path: &Path) -> Result<PathBuf> {
    let path =
        std::fs::canonicalize(path).with_context(|| format!("resolving {}", path.display()))?;
    let start = if path.is_file() {
        if path.file_name().is_some_and(|name| name == "main.yaml") {
            return Ok(path);
        }
        bail!("expected main.yaml or a module directory");
    } else {
        path.as_path()
    };
    for dir in start.ancestors() {
        let manifest = dir.join("main.yaml");
        if manifest.is_file() {
            return Ok(manifest);
        }
    }
    bail!("no main.yaml found above {}", path.display())
}

/// Make a proto path relative to the module directory, for readability.
fn rel_to(manifest_path: &Path, proto: &Path) -> PathBuf {
    let base = manifest_path.parent().unwrap_or(Path::new("."));
    proto
        .strip_prefix(base)
        .map_or_else(|_| proto.to_path_buf(), Path::to_path_buf)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)] // tests may fail by panicking
mod tests {
    use super::*;

    /// Proto paths are printed relative to the manifest so the table stays readable, but a path from
    /// outside that tree (a dependency's proto, or a schema crate's) has to stay absolute rather than
    /// come out as something that does not exist.
    #[test]
    fn proto_paths_print_relative_to_the_manifest() {
        let manifest = Path::new("/proj/main.yaml");
        assert_eq!(
            rel_to(manifest, Path::new("/proj/proto/ping.proto")),
            Path::new("proto/ping.proto")
        );
        assert_eq!(
            rel_to(manifest, Path::new("/proj/a/b/deep.proto")),
            Path::new("a/b/deep.proto")
        );
        // Outside the manifest's directory: left as it is.
        assert_eq!(
            rel_to(manifest, Path::new("/other/proto/shared.proto")),
            Path::new("/other/proto/shared.proto")
        );
        // A manifest path with no parent falls back to ".", which strips nothing.
        assert_eq!(
            rel_to(Path::new("main.yaml"), Path::new("proto/ping.proto")),
            Path::new("proto/ping.proto")
        );
    }
}
