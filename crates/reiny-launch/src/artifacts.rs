//! Build Cargo binaries and stage immutable executable/runtime-library bundles.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::file_lock::FileLock;

/// Supported module build engines.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BuildKind {
    /// Build a Rust executable through Cargo.
    Cargo,
}

/// Declarative Cargo build configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BuildSpec {
    /// Build engine, serialized as `type`.
    #[serde(rename = "type")]
    pub kind: BuildKind,
    /// Manifest path relative to the module; defaults to `Cargo.toml`.
    #[serde(default = "default_manifest")]
    pub manifest: PathBuf,
    /// Optional workspace package selector.
    #[serde(default)]
    pub package: Option<String>,
    /// Cargo profile; defaults to `release`.
    #[serde(default = "default_profile")]
    pub profile: String,
    /// Cargo features enabled for this build.
    #[serde(default)]
    pub features: Vec<String>,
    /// Whether Cargo default features are enabled; defaults to true.
    #[serde(default = "default_true")]
    pub default_features: bool,
    /// Require Cargo.lock to remain unchanged; defaults to true.
    #[serde(default = "default_true")]
    pub locked: bool,
}

fn default_manifest() -> PathBuf {
    PathBuf::from("Cargo.toml")
}
fn default_profile() -> String {
    "release".to_owned()
}
fn default_true() -> bool {
    true
}

/// Run Cargo's own incremental build and stage the selected executable.
///
/// The returned file lives outside Cargo's target directory and is never replaced.
///
/// # Errors
/// Returns an error for a failed build, missing/ambiguous executable, or staging failure.
pub(crate) fn prepare(
    spec: &BuildSpec,
    module_dir: &Path,
    bin: &str,
    cache_dir: &Path,
) -> Result<PathBuf> {
    let module_dir = module_dir
        .canonicalize()
        .context("canonicalize build module")?;
    let manifest = module_dir
        .join(&spec.manifest)
        .canonicalize()
        .context("resolve Cargo manifest")?;
    fs::create_dir_all(cache_dir)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(cache_dir.join("artifacts.lock"))?;
    let _lock = FileLock::new(lock).context("another artifact preparer holds the cache lock")?;
    let mut command = Command::new("cargo");
    command
        .current_dir(&module_dir)
        .args([
            "build",
            "--message-format=json-render-diagnostics",
            "--manifest-path",
        ])
        .arg(&manifest)
        .args(["--profile", &spec.profile, "--bin", bin])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    // Keep compiler outputs out of deep, revision-addressed Git checkouts.
    // Native Windows linkers do not reliably support long object/output paths.
    if std::env::var_os("CARGO_TARGET_DIR").is_none() {
        command.arg("--target-dir").arg(cache_dir.join("cargo"));
    }
    if let Some(package) = &spec.package {
        command.args(["--package", package]);
    }
    if !spec.features.is_empty() {
        command.args(["--features", &spec.features.join(",")]);
    }
    if !spec.default_features {
        command.arg("--no-default-features");
    }
    if spec.locked {
        command.arg("--locked");
    }
    let mut child = command.spawn().context("start Cargo build")?;
    let mut executable = None;
    let mut ambiguous = false;
    let parse_result = (|| {
        let stdout = child.stdout.take().context("Cargo stdout missing")?;
        for line in BufReader::new(stdout).lines() {
            let line = line?;
            let Ok(message) = serde_json::from_str::<Value>(&line) else {
                eprintln!("{line}");
                continue;
            };
            if message["reason"] == "compiler-message" {
                if let Some(rendered) = message["message"]["rendered"].as_str() {
                    eprint!("{rendered}");
                }
            } else if message["reason"] == "compiler-artifact"
                && message["target"]["name"] == bin
                && message["target"]["kind"]
                    .as_array()
                    .is_some_and(|kinds| kinds.iter().any(|kind| kind == "bin"))
                && let Some(path) = message["executable"].as_str()
            {
                let path = PathBuf::from(path);
                if executable
                    .as_ref()
                    .is_some_and(|previous| previous != &path)
                {
                    ambiguous = true;
                }
                executable = Some(path);
            }
        }
        Ok::<(), anyhow::Error>(())
    })();
    // Reap the build even when stdout parsing fails.
    let status = child.wait().context("wait for Cargo build")?;
    ensure!(status.success(), "Cargo build failed with {status}");
    parse_result?;
    ensure!(
        !ambiguous,
        "Cargo returned multiple executables named {bin}"
    );
    let executable = executable.context("Cargo did not report the requested executable")?;
    stage(&executable, cache_dir)
}

pub(crate) fn stage_prebuilt(executable: &Path, cache: &Path) -> Result<PathBuf> {
    fs::create_dir_all(cache)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(cache.join("artifacts.lock"))?;
    let _lock = FileLock::new(lock).context("another artifact preparer holds the cache lock")?;
    stage(executable, cache)
}

fn stage(executable: &Path, cache: &Path) -> Result<PathBuf> {
    let parent = executable
        .parent()
        .context("executable has no parent directory")?;
    let name = executable
        .file_name()
        .context("executable has no file name")?;
    let mut files = BTreeMap::new();
    files.insert(name.to_owned(), executable.to_owned());
    // Cargo puts native runtime libraries beside the binary or under deps.
    // Bundle their exact bytes; never touch an already staged active executable.
    for dir in [parent.to_owned(), parent.join("deps")] {
        if !dir.is_dir() {
            continue;
        }
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let name = entry.file_name();
            let text = name.to_string_lossy();
            if path.is_file()
                && (text.ends_with(".dll")
                    || text.ends_with(".dylib")
                    || text.ends_with(".so")
                    || text.contains(".so."))
                && let Some(previous) = files.insert(name.clone(), path.clone())
            {
                ensure!(
                    fs::read(&previous)? == fs::read(&path)?,
                    "conflicting runtime libraries named {}",
                    name.to_string_lossy()
                );
            }
        }
    }
    // Snapshot before hashing: another Cargo invocation may change target files
    // after our build exits. The content address describes the staged bytes.
    let staging = cache.join("bundle.partial");
    if staging.exists() {
        fs::remove_dir_all(&staging)?;
    }
    fs::create_dir(&staging)?;
    for (name, source) in &files {
        if let Err(error) = fs::copy(source, staging.join(name)) {
            fs::remove_dir_all(&staging)?;
            return Err(error.into());
        }
    }
    let mut digest = Sha256::new();
    for name in files.keys() {
        let path = staging.join(name);
        let name = name.to_str().context("non-UTF-8 runtime filename")?;
        digest.update((name.len() as u64).to_le_bytes());
        digest.update(name.as_bytes());
        digest.update(fs::metadata(&path)?.len().to_le_bytes());
        let mut file = fs::File::open(&path)?;
        let mut bytes = [0; 16 * 1024];
        loop {
            let count = file.read(&mut bytes)?;
            if count == 0 {
                break;
            }
            digest.update(&bytes[..count]);
        }
    }
    let destination = cache.join(format!("{:x}", digest.finalize()));
    if destination.exists() {
        let staged = destination.join(name);
        ensure!(staged.is_file(), "incomplete immutable artifact bundle");
        fs::remove_dir_all(&staging)?;
        return Ok(staged);
    }
    let result = fs::rename(&staging, &destination);
    if result.is_err() {
        fs::remove_dir_all(&staging)?;
    }
    result?;
    let staged = destination.join(name);
    if !staged.is_file() {
        bail!("staged executable is missing");
    }
    Ok(staged)
}

#[cfg(test)]
mod tests {
    use super::{stage, stage_prebuilt};
    use std::fs;

    #[test]
    fn runtime_libraries_participate_in_immutable_bundle_address() -> anyhow::Result<()> {
        let fixture = tempfile::tempdir()?;
        let target = fixture.path().join("target");
        let cache = fixture.path().join("cache");
        fs::create_dir_all(target.join("deps"))?;
        fs::create_dir(&cache)?;
        let bin = target.join("fixture.exe");
        fs::write(&bin, b"executable")?;
        fs::write(target.join("companion.dll"), b"first library")?;
        fs::write(target.join("deps/libfixture.so.1"), b"dependency library")?;
        let first = stage_prebuilt(&bin, &cache)?;
        let first_dir = first
            .parent()
            .ok_or_else(|| anyhow::anyhow!("missing bundle directory"))?;
        assert_eq!(fs::read(first_dir.join("companion.dll"))?, b"first library");
        assert_eq!(
            fs::read(first_dir.join("libfixture.so.1"))?,
            b"dependency library"
        );
        assert_eq!(stage_prebuilt(&bin, &cache)?, first);
        fs::write(target.join("companion.dll"), b"second library")?;
        let second = stage(&bin, &cache)?;
        assert_ne!(first, second);
        assert_eq!(fs::read(first_dir.join("companion.dll"))?, b"first library");
        Ok(())
    }
}
