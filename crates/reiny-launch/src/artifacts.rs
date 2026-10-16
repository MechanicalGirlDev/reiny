//! Declarative builders and immutable executable/runtime-library bundles.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::file_lock::FileLock;
use crate::progress::{CancelToken, Events, GroupKind, PrepareEvent};
use crate::runner::{self, Stream};

#[path = "runtime_libraries.rs"]
mod runtime_libraries;

/// Supported module build engines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildKind {
    /// Build a Rust executable through Cargo.
    Cargo,
    /// Run an explicit argument vector.
    Command,
}

/// Declarative build engine and its exact configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BuildSpec {
    /// Cargo's own incremental compiler.
    Cargo(CargoBuild),
    /// An explicit command with declared outputs.
    Command(CommandBuild),
}

impl BuildSpec {
    /// Return the selected engine without duplicating its configuration.
    #[must_use]
    pub const fn kind(&self) -> BuildKind {
        match self {
            Self::Cargo(_) => BuildKind::Cargo,
            Self::Command(_) => BuildKind::Command,
        }
    }

    pub(crate) fn resolve_paths(&mut self, module_dir: &Path) {
        match self {
            Self::Cargo(build) => {
                build.manifest = module_dir.join(&build.manifest);
                build.cwd = Some(module_dir.join(build.cwd.as_deref().unwrap_or(Path::new("."))));
            }
            Self::Command(build) => build.cwd = module_dir.join(&build.cwd),
        }
    }
}

/// Exact Cargo build configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CargoBuild {
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
    /// Working directory relative to the declaring module.
    #[serde(default)]
    pub cwd: Option<PathBuf>,
    /// Optional Cargo target triple.
    #[serde(default)]
    pub target: Option<String>,
    /// Environment overrides for this invocation.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

impl Default for CargoBuild {
    fn default() -> Self {
        Self {
            manifest: default_manifest(),
            package: None,
            profile: default_profile(),
            features: Vec::new(),
            default_features: true,
            locked: true,
            cwd: None,
            target: None,
            env: BTreeMap::new(),
        }
    }
}

/// An argv builder; shells are used only when explicitly named in `argv`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CommandBuild {
    /// Nonempty executable and argument vector.
    pub argv: Vec<String>,
    /// Working directory relative to the declaring module.
    #[serde(default = "default_cwd")]
    pub cwd: PathBuf,
    /// Environment overrides for this invocation.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Logical executable names (without suffix) to sources relative to cwd.
    #[serde(deserialize_with = "unique_map")]
    pub outputs: BTreeMap<String, PathBuf>,
    /// Safe relative bundle destinations to sources relative to cwd.
    #[serde(default, deserialize_with = "unique_map")]
    pub files: BTreeMap<PathBuf, PathBuf>,
}

fn unique_map<'de, D, K, V>(deserializer: D) -> std::result::Result<BTreeMap<K, V>, D::Error>
where
    D: serde::Deserializer<'de>,
    K: Deserialize<'de> + Ord,
    V: Deserialize<'de>,
{
    struct Unique<K, V>(std::marker::PhantomData<(K, V)>);
    impl<'de, K: Deserialize<'de> + Ord, V: Deserialize<'de>> serde::de::Visitor<'de> for Unique<K, V> {
        type Value = BTreeMap<K, V>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a map with unique keys")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut access: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut values = BTreeMap::new();
            while let Some((key, value)) = access.next_entry()? {
                if values.insert(key, value).is_some() {
                    return Err(serde::de::Error::custom(
                        "duplicate build output or file destination",
                    ));
                }
            }
            Ok(values)
        }
    }
    deserializer.deserialize_map(Unique(std::marker::PhantomData))
}

fn default_cwd() -> PathBuf {
    PathBuf::from(".")
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

pub(crate) type Environment = BTreeMap<std::ffi::OsString, std::ffi::OsString>;

pub(crate) struct BuildRequest {
    pub(crate) key: String,
    pub(crate) spec: BuildSpec,
    environment: Environment,
    target: PathBuf,
}

pub(crate) struct Built {
    pub(crate) executables: BTreeMap<String, PathBuf>,
    pub(crate) fresh: bool,
}

impl BuildRequest {
    pub(crate) fn new(
        spec: &BuildSpec,
        module_dir: &Path,
        base: &Path,
        environment: &Environment,
    ) -> Result<Self> {
        let mut spec = spec.clone();
        spec.resolve_paths(module_dir);
        let mut environment = environment.clone();
        match &mut spec {
            BuildSpec::Cargo(build) => {
                build.manifest = build
                    .manifest
                    .canonicalize()
                    .context("resolve Cargo manifest")?;
                build.cwd = Some(
                    build
                        .cwd
                        .as_ref()
                        .context("missing Cargo cwd")?
                        .canonicalize()?,
                );
                build.features.sort();
                build.features.dedup();
                for (key, value) in &build.env {
                    environment.insert(key.into(), value.into());
                }
            }
            BuildSpec::Command(build) => {
                ensure!(
                    !build.argv.is_empty() && !build.argv[0].is_empty(),
                    "command argv must be nonempty"
                );
                build.cwd = build.cwd.canonicalize().context("resolve command cwd")?;
                let mut destinations = build.files.clone();
                for (name, source) in &build.outputs {
                    validate_executable_name(name)?;
                    ensure!(
                        destinations
                            .insert(executable_name(name), source.clone())
                            .is_none(),
                        "duplicate command output {name}"
                    );
                }
                validate_paths(&destinations)?;
                for (key, value) in &build.env {
                    environment.insert(key.into(), value.into());
                }
            }
        }
        // Compiler-affecting inherited environment is frozen for the whole
        // request. Temporary transaction paths are not compiler identities.
        let relevant: BTreeMap<_, _> = environment
            .iter()
            .filter(|(key, _)| {
                let key = key.to_string_lossy();
                key.starts_with("RUST")
                    || key.starts_with("CARGO")
                    || key.starts_with("CC")
                    || key.starts_with("CXX")
                    || key.starts_with("AR")
                    || key.starts_with("LD")
                    || matches!(
                        key.as_ref(),
                        "PATH"
                            | "Path"
                            | "INCLUDE"
                            | "LIB"
                            | "LIBPATH"
                            | "SDKROOT"
                            | "MACOSX_DEPLOYMENT_TARGET"
                            | "PKG_CONFIG_PATH"
                    )
            })
            .filter(|(key, _)| *key != "CARGO_TARGET_DIR")
            .collect();
        let key = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&(&spec, base, format!("{relevant:?}")))?)
        );
        // Keep native linker object paths short, especially for build scripts.
        // Group identity retains the full digest. Any directory-prefix collision
        // shares the target lock, never merges groups, and Cargo still validates
        // each exact request before its outputs are snapshotted.
        let target = base.join("cargo").join(&key[..32]);
        Ok(Self {
            key,
            spec,
            environment,
            target,
        })
    }

    pub(crate) const fn kind(&self) -> GroupKind {
        match self.spec {
            BuildSpec::Cargo(_) => GroupKind::Cargo,
            BuildSpec::Command(_) => GroupKind::Command,
        }
    }

    pub(crate) fn execute(
        &self,
        bins: &BTreeSet<String>,
        cache: &Path,
        cancel: &CancelToken,
        events: &Events<'_>,
        id: &str,
    ) -> Result<Built> {
        cancel.check()?;
        match &self.spec {
            BuildSpec::Cargo(build) => self.cargo(build, bins, cache, cancel, events, id),
            BuildSpec::Command(build) => {
                for bin in bins {
                    ensure!(
                        build.outputs.contains_key(bin),
                        "command outputs does not declare executable {bin}"
                    );
                }
                let _lock = build_lock(&build.cwd.join(".reiny/command.lock"), cancel, events, id)?;
                let mut command = Command::new(&build.argv[0]);
                command
                    .args(&build.argv[1..])
                    .current_dir(&build.cwd)
                    .env_clear()
                    .envs(&self.environment);
                let status = runner::run(command, cancel, |stream, text| {
                    diagnostic(events, id, stream, text);
                    Ok(())
                })?;
                ensure!(
                    status.success(),
                    "command {:?} failed with {status}",
                    build.argv
                );
                cancel.check()?;
                let mut files = BTreeMap::new();
                for (name, source) in &build.files {
                    files.insert(name.clone(), command_source(&build.cwd, source)?);
                }
                for (name, source) in &build.outputs {
                    ensure!(
                        files
                            .insert(executable_name(name), command_source(&build.cwd, source)?)
                            .is_none(),
                        "duplicate command output {name}"
                    );
                }
                let entry = executable_name(bins.first().context("empty build group")?);
                let first = stage_files_with(&files, cache, &entry, cancel)?;
                let directory = first.parent().context("command snapshot has no parent")?;
                Ok(Built {
                    executables: bins
                        .iter()
                        .map(|bin| (bin.clone(), directory.join(executable_name(bin))))
                        .collect(),
                    fresh: false,
                })
            }
        }
    }

    fn cargo_command(&self, build: &CargoBuild, bins: &BTreeSet<String>) -> Result<Command> {
        let mut command = Command::new(
            self.environment
                .get(std::ffi::OsStr::new("CARGO"))
                .cloned()
                .unwrap_or_else(|| "cargo".into()),
        );
        command
            .current_dir(cargo_path(build.cwd.as_ref().context("missing Cargo cwd")?))
            .env_clear()
            .envs(&self.environment)
            .args([
                "build",
                "--message-format=json-render-diagnostics",
                "--manifest-path",
            ])
            .arg(cargo_path(&build.manifest).as_os_str())
            .args(["--profile", &build.profile])
            .arg("--target-dir")
            .arg(cargo_path(&self.target).as_os_str());
        for bin in bins {
            command.args(["--bin", bin]);
        }
        if let Some(package) = &build.package {
            command.args(["--package", package]);
        }
        if let Some(target) = &build.target {
            command.args(["--target", target]);
        }
        if !build.features.is_empty() {
            command.args(["--features", &build.features.join(",")]);
        }
        if !build.default_features {
            command.arg("--no-default-features");
        }
        if build.locked {
            command.arg("--locked");
        }
        Ok(command)
    }

    fn cargo(
        &self,
        build: &CargoBuild,
        bins: &BTreeSet<String>,
        cache: &Path,
        cancel: &CancelToken,
        events: &Events<'_>,
        id: &str,
    ) -> Result<Built> {
        let _lock = build_lock(&self.target.with_extension("lock"), cancel, events, id)?;
        let command = self.cargo_command(build, bins)?;
        let mut executables = BTreeMap::new();
        let mut linked_paths = BTreeSet::new();
        let mut fresh = true;
        let mut done = 0_u32;
        let status = runner::run(command, cancel, |stream, text| {
            let message = if stream == Stream::Stdout {
                serde_json::from_str::<serde_json::Value>(text).ok()
            } else {
                None
            };
            if let Some(message) = message {
                if message["reason"] == "compiler-artifact" {
                    done = done
                        .checked_add(1)
                        .context("compiler artifact counter exhausted")?;
                    let artifact_fresh = message["fresh"].as_bool().unwrap_or(false);
                    fresh &= artifact_fresh;
                    let name = message["target"]["name"]
                        .as_str()
                        .context("Cargo artifact has no target name")?;
                    events.emit(PrepareEvent::Compiled {
                        group: id.to_owned(),
                        crate_name: name.to_owned(),
                        fresh: artifact_fresh,
                        done,
                    });
                    if bins.contains(name)
                        && message["target"]["kind"]
                            .as_array()
                            .is_some_and(|kinds| kinds.iter().any(|kind| kind == "bin"))
                        && let Some(path) = message["executable"].as_str()
                    {
                        let manifest = message["manifest_path"]
                            .as_str()
                            .context("Cargo artifact has no manifest path")?;
                        let package = Path::new(manifest)
                            .parent()
                            .context("Cargo manifest has no parent")?;
                        let artifact = (PathBuf::from(path), package.to_owned());
                        ensure!(
                            executables.insert(name.to_owned(), artifact).is_none(),
                            "Cargo returned multiple executables named {name}"
                        );
                    }
                } else if message["reason"] == "build-script-executed" {
                    linked_paths.extend(cargo_link_paths(&message)?);
                } else if message["reason"] == "compiler-message" {
                    events.emit(PrepareEvent::Diagnostic {
                        group: Some(id.to_owned()),
                        level: message["message"]["level"]
                            .as_str()
                            .unwrap_or("error")
                            .to_owned(),
                        text: message["message"]["rendered"]
                            .as_str()
                            .unwrap_or(text)
                            .to_owned(),
                    });
                }
            } else {
                diagnostic(events, id, stream, text);
            }
            Ok(())
        })?;
        ensure!(status.success(), "Cargo build failed with {status}");
        for bin in bins {
            ensure!(
                executables.contains_key(bin),
                "Cargo did not report requested executable {bin}"
            );
        }
        cancel.check()?;
        let mut files = BTreeMap::new();
        for (bin, (path, package)) in &executables {
            files.insert(executable_name(bin), path.clone());
            let linked: Vec<_> = linked_paths.iter().map(|dir| package.join(dir)).collect();
            collect_libraries(path, &linked, &mut files)?;
        }
        let entry = executable_name(bins.first().context("empty Cargo group")?);
        let first = stage_files_with(&files, cache, &entry, cancel)?;
        let directory = first.parent().context("Cargo snapshot has no parent")?;
        Ok(Built {
            executables: bins
                .iter()
                .map(|bin| (bin.clone(), directory.join(executable_name(bin))))
                .collect(),
            fresh: fresh && done > 0,
        })
    }
}

fn cargo_path(path: &Path) -> std::borrow::Cow<'_, Path> {
    #[cfg(windows)]
    {
        use std::path::{Component, Prefix};
        let mut components = path.components();
        if let Some(Component::Prefix(prefix)) = components.next()
            && let Prefix::VerbatimDisk(drive) = prefix.kind()
        {
            // Cargo can alternate path-package source spellings when entered
            // through a verbatim drive path, invalidating unchanged fingerprints.
            // Normalize only the process boundary, not canonical group identity.
            let mut normal = PathBuf::from(format!("{}:", char::from(drive)));
            for component in components {
                normal.push(component.as_os_str());
            }
            return std::borrow::Cow::Owned(normal);
        }
    }
    std::borrow::Cow::Borrowed(path)
}

fn diagnostic(events: &Events<'_>, id: &str, stream: Stream, text: &str) {
    events.emit(PrepareEvent::Diagnostic {
        group: Some(id.to_owned()),
        level: if stream == Stream::Stderr {
            "stderr"
        } else {
            "stdout"
        }
        .to_owned(),
        text: text.to_owned(),
    });
}

fn build_lock(
    path: &Path,
    cancel: &CancelToken,
    events: &Events<'_>,
    id: &str,
) -> Result<FileLock> {
    FileLock::wait_with(path, cancel, || {
        events.emit(PrepareEvent::Diagnostic {
            group: Some(id.to_owned()),
            level: "info".to_owned(),
            text: format!("Waiting for build lock {}", path.display()),
        });
    })
}

fn command_source(cwd: &Path, source: &Path) -> Result<PathBuf> {
    ensure!(
        !source.is_absolute() && !source.has_root(),
        "command output must be relative to cwd: {}",
        source.display()
    );
    let source = cwd
        .join(source)
        .canonicalize()
        .with_context(|| format!("command output not found: {}", cwd.join(source).display()))?;
    ensure!(
        source.starts_with(cwd) && source.is_file(),
        "command output escapes cwd or is not a file: {}",
        source.display()
    );
    Ok(source)
}

pub(crate) fn executable_name(bin: &str) -> PathBuf {
    PathBuf::from(format!("{bin}{}", std::env::consts::EXE_SUFFIX))
}

pub(crate) fn validate_executable_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty()
            && name != "."
            && name != ".."
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_-.".contains(&byte)),
        "invalid executable name {name}"
    );
    ensure!(
        std::env::consts::EXE_SUFFIX.is_empty()
            || !name.to_lowercase().ends_with(std::env::consts::EXE_SUFFIX),
        "logical executable name must not include the platform suffix: {name}"
    );
    Ok(())
}

#[cfg(test)]
pub(crate) fn stage_prebuilt(executable: &Path, cache: &Path) -> Result<PathBuf> {
    stage_prebuilt_bundle(executable, &[], cache, &CancelToken::default())
}

pub(crate) fn stage_prebuilt_bundle(
    executable: &Path,
    companions: &[PathBuf],
    cache: &Path,
    cancel: &CancelToken,
) -> Result<PathBuf> {
    // Written by `reiny bundle` from its frozen files, not inferred from the
    // installation directory. Never sweep unrelated files into a snapshot.
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct BundleFiles {
        v: u32,
        files: Vec<PathBuf>,
    }
    let directory = executable.parent().context("executable has no directory")?;
    let metadata = directory.join(".reiny-bundle.json");
    if !metadata.try_exists()? {
        return stage_bundle(executable, companions, cache, cancel);
    }
    let manifest: BundleFiles =
        serde_json::from_slice(&fs::read(&metadata)?).context("invalid bundle file list")?;
    ensure!(manifest.v == 1, "unsupported bundle file list version");
    let mut files = BTreeMap::new();
    for name in manifest.files {
        ensure!(
            name != Path::new(".reiny-bundle.json"),
            "bundle file list cannot include itself"
        );
        let source = directory.join(&name);
        ensure!(
            files.insert(name, source).is_none(),
            "duplicate bundle file list entry"
        );
    }
    validate_paths(&files)?;
    let directory = directory.canonicalize()?;
    for source in files.values_mut() {
        *source = source.canonicalize()?;
        ensure!(
            source.starts_with(&directory),
            "bundle file list entry escapes its directory"
        );
    }
    for required in std::iter::once(executable).chain(companions.iter().map(PathBuf::as_path)) {
        let name = required.file_name().context("executable has no filename")?;
        ensure!(
            files.contains_key(Path::new(name)),
            "bundle file list omits executable {}",
            required.display()
        );
    }
    let entry = executable
        .file_name()
        .context("executable has no filename")?;
    stage_files_with(&files, cache, Path::new(entry), cancel)
}

#[cfg(test)]
fn stage(executable: &Path, cache: &Path) -> Result<PathBuf> {
    stage_bundle(executable, &[], cache, &CancelToken::default())
}

fn stage_bundle(
    executable: &Path,
    companions: &[PathBuf],
    cache: &Path,
    cancel: &CancelToken,
) -> Result<PathBuf> {
    let name = executable
        .file_name()
        .context("executable has no file name")?;
    let mut files = BTreeMap::new();
    files.insert(PathBuf::from(name), executable.to_owned());
    for companion in companions {
        ensure!(
            companion.is_file(),
            "explicit companion executable not found: {}",
            companion.display()
        );
        let name = companion
            .file_name()
            .context("companion executable has no filename")?;
        ensure!(
            files
                .insert(PathBuf::from(name), companion.clone())
                .is_none(),
            "conflicting bundle path {}",
            name.to_string_lossy()
        );
    }
    collect_libraries(executable, &[], &mut files)?;
    for companion in companions {
        collect_libraries(companion, &[], &mut files)?;
    }
    stage_files_with(&files, cache, &PathBuf::from(name), cancel)
}

fn collect_libraries(
    executable: &Path,
    linked: &[PathBuf],
    files: &mut BTreeMap<PathBuf, PathBuf>,
) -> Result<()> {
    let parent = executable
        .parent()
        .context("executable has no parent directory")?;
    // Siblings and explicit native search directories can contain libraries
    // loaded dynamically, without an entry in an executable's import table.
    collect_library_directory(parent, files)?;
    for directory in linked {
        collect_linked_libraries(directory, files)?;
    }
    if let Some(imports) = runtime_libraries::pe_imports(executable)? {
        // Cargo deps also contains compiler proc macros. Only runtime imports
        // may promote those candidates into the bundle.
        runtime_libraries::collect_pe_dependencies(executable, imports, parent, files)?;
    } else {
        // Scripts, opaque command fixtures, ELF and Mach-O retain their existing
        // discovery contract; PE-specific pruning is not safe for those formats.
        collect_library_directory(&parent.join("deps"), files)?;
    }
    Ok(())
}

fn cargo_link_paths(message: &serde_json::Value) -> Result<Vec<PathBuf>> {
    message["linked_paths"]
        .as_array()
        .context("Cargo build script has no linked_paths")?
        .iter()
        .map(|value| {
            let value = value
                .as_str()
                .context("Cargo link search path is not a string")?;
            // Cargo preserves rustc's optional search-kind prefix in its JSON.
            let path = ["native=", "dependency=", "crate=", "framework=", "all="]
                .iter()
                .find_map(|prefix| value.strip_prefix(prefix))
                .unwrap_or(value);
            Ok(PathBuf::from(path))
        })
        .collect()
}

fn collect_linked_libraries(
    directory: &Path,
    files: &mut BTreeMap<PathBuf, PathBuf>,
) -> Result<()> {
    collect_library_directory(directory, files)?;
    // Native distributions commonly separate import libraries from runtime DLLs.
    if directory.is_dir()
        && directory
            .file_name()
            .is_some_and(|name| name == "lib" || name == "lib64")
    {
        collect_library_directory(&directory.with_file_name("bin"), files)?;
    }
    Ok(())
}

fn collect_library_directory(
    directory: &Path,
    files: &mut BTreeMap<PathBuf, PathBuf>,
) -> Result<()> {
    if !directory.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name();
        let text = name.to_string_lossy();
        if path.is_file()
            && (text.to_ascii_lowercase().ends_with(".dll")
                || text.ends_with(".dylib")
                || text.ends_with(".so")
                || text.contains(".so."))
            && let Some(previous) = files.insert(PathBuf::from(&name), path.clone())
        {
            ensure!(
                fs::read(&previous)? == fs::read(&path)?,
                "conflicting runtime libraries named {}",
                name.to_string_lossy()
            );
        }
    }
    Ok(())
}

fn validate_paths(files: &BTreeMap<PathBuf, PathBuf>) -> Result<()> {
    let mut paths = BTreeSet::new();
    for name in files.keys() {
        ensure!(
            !name.as_os_str().is_empty()
                && name
                    .components()
                    .all(|part| matches!(part, std::path::Component::Normal(_))),
            "bundle path must be a normal relative path: {}",
            name.display()
        );
        let text = name.to_str().context("non-UTF-8 bundle path")?;
        let normalized = text.replace('\\', "/").to_lowercase();
        ensure!(
            paths.insert(normalized),
            "conflicting bundle path {}",
            name.display()
        );
    }
    for name in &paths {
        ensure!(
            !paths
                .iter()
                .any(|other| other.starts_with(&format!("{name}/"))),
            "conflicting bundle path {name}"
        );
    }
    Ok(())
}

pub(crate) fn stage_files_with(
    files: &BTreeMap<PathBuf, PathBuf>,
    cache: &Path,
    entry: &Path,
    cancel: &CancelToken,
) -> Result<PathBuf> {
    validate_paths(files)?;
    ensure!(files.contains_key(entry), "bundle entry is not declared");
    cancel.check()?;
    fs::create_dir_all(cache)?;
    // Snapshot before hashing: another Cargo invocation may change target files
    // after our build exits. The content address describes the staged bytes.
    let staging = tempfile::Builder::new()
        .prefix(".bundle-")
        .tempdir_in(cache)?;
    for (name, source) in files {
        cancel.check()?;
        ensure!(
            source.is_file(),
            "bundle source is not a file: {}",
            source.display()
        );
        let destination = staging.path().join(name);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut input = fs::File::open(source)?;
        let mut output = fs::File::create(&destination)?;
        let mut bytes = [0; 16 * 1024];
        loop {
            cancel.check()?;
            let count = input.read(&mut bytes)?;
            if count == 0 {
                break;
            }
            output.write_all(&bytes[..count])?;
        }
        output.set_permissions(input.metadata()?.permissions())?;
    }
    cancel.check()?;
    let digest = directory_digest(staging.path(), cancel)?;
    let destination = cache.join(&digest);
    if !destination.exists() {
        match fs::rename(staging.path(), &destination) {
            Ok(()) => {}
            Err(error) if destination.exists() => {
                tracing::trace!(%error, "another transaction published identical bytes");
            }
            Err(error) => return Err(error.into()),
        }
    }
    verify_directory(&destination, cancel)?;
    let staged = destination.join(entry);
    ensure!(staged.is_file(), "incomplete immutable artifact bundle");
    Ok(staged)
}

/// Hash exactly the sorted relative paths, lengths and bytes used by staging.
fn directory_digest(directory: &Path, cancel: &CancelToken) -> Result<String> {
    let mut files = BTreeSet::new();
    collect_files(directory, directory, &mut files)?;
    let mut digest = Sha256::new();
    for name in files {
        cancel.check()?;
        let path = directory.join(&name);
        let name = name.to_str().context("non-UTF-8 runtime filename")?;
        digest.update((name.len() as u64).to_le_bytes());
        digest.update(name.as_bytes());
        digest.update(fs::metadata(&path)?.len().to_le_bytes());
        let mut file = fs::File::open(&path)?;
        let mut bytes = [0; 16 * 1024];
        loop {
            cancel.check()?;
            let count = file.read(&mut bytes)?;
            if count == 0 {
                break;
            }
            digest.update(&bytes[..count]);
        }
    }
    Ok(format!("{:x}", digest.finalize()))
}

pub(crate) fn verify_directory(directory: &Path, cancel: &CancelToken) -> Result<()> {
    let expected = directory
        .file_name()
        .and_then(|name| name.to_str())
        .context("invalid immutable directory name")?;
    ensure!(
        directory_digest(directory, cancel)? == expected,
        "immutable bundle integrity mismatch: {}",
        directory.display()
    );
    Ok(())
}

fn collect_files(root: &Path, directory: &Path, files: &mut BTreeSet<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        ensure!(
            !kind.is_symlink(),
            "immutable bundle contains a symbolic link"
        );
        if kind.is_dir() {
            collect_files(root, &entry.path(), files)?;
        } else if kind.is_file() {
            files.insert(entry.path().strip_prefix(root)?.to_owned());
        } else {
            bail!("immutable bundle contains a non-file");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{stage, stage_prebuilt, stage_prebuilt_bundle};
    use crate::progress::CancelToken;
    use anyhow::Context;
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn cargo_link_paths_preserve_unqualified_paths_and_strip_search_kinds() -> anyhow::Result<()> {
        let message = serde_json::json!({"linked_paths": [
            "native=/sdk/lib", "dependency=/deps", "crate=/crate",
            "framework=/frameworks", "all=/all", "/sdk=version/lib", "relative/lib"
        ]});
        assert_eq!(
            super::cargo_link_paths(&message)?,
            [
                "/sdk/lib",
                "/deps",
                "/crate",
                "/frameworks",
                "/all",
                "/sdk=version/lib",
                "relative/lib"
            ]
            .map(PathBuf::from)
        );
        assert!(super::cargo_link_paths(&serde_json::json!({})).is_err());
        assert!(super::cargo_link_paths(&serde_json::json!({"linked_paths": [1]})).is_err());
        Ok(())
    }

    #[test]
    fn linked_native_directories_keep_collision_checks_and_immutable_bytes() -> anyhow::Result<()> {
        let fixture = tempfile::tempdir()?;
        let lib = fixture.path().join("native/lib");
        let bin = fixture.path().join("native/bin");
        let custom = fixture.path().join("native/custom");
        for directory in [&lib, &bin, &custom] {
            fs::create_dir_all(directory)?;
        }
        for (name, bytes) in [
            ("fixture.dll", b"first".as_slice()),
            ("libfixture.so.1", b"elf".as_slice()),
            ("libfixture.dylib", b"mach".as_slice()),
            ("ignored.lib", b"import".as_slice()),
            ("ignored.exe", b"unrelated".as_slice()),
        ] {
            fs::write(lib.join(name), bytes)?;
        }
        fs::write(bin.join("fixture.dll"), b"first")?;
        fs::create_dir(lib.join("not-a-file.dll"))?;
        let app = fixture.path().join("app.exe");
        fs::write(&app, b"app")?;
        let mut files = BTreeMap::from([(PathBuf::from("app.exe"), app.clone())]);
        super::collect_linked_libraries(&custom, &mut files)?;
        super::collect_linked_libraries(&fixture.path().join("native/lib64"), &mut files)?;
        assert_eq!(files.len(), 1);
        super::collect_linked_libraries(&lib, &mut files)?;
        assert_eq!(files.len(), 4);
        assert_eq!(
            files[&PathBuf::from("fixture.dll")],
            bin.join("fixture.dll")
        );
        let cache = fixture.path().join("snapshots");
        let first = super::stage_files_with(
            &files,
            &cache,
            std::path::Path::new("app.exe"),
            &CancelToken::default(),
        )?;
        fs::write(bin.join("fixture.dll"), b"second")?;
        let mut conflict = BTreeMap::from([(PathBuf::from("app.exe"), app)]);
        assert!(super::collect_linked_libraries(&lib, &mut conflict).is_err());
        fs::write(lib.join("fixture.dll"), b"second")?;
        super::collect_linked_libraries(&lib, &mut files)?;
        let second = super::stage_files_with(
            &files,
            &cache,
            std::path::Path::new("app.exe"),
            &CancelToken::default(),
        )?;
        assert_ne!(first, second);
        assert_eq!(
            fs::read(
                first
                    .parent()
                    .context("first snapshot")?
                    .join("fixture.dll")
            )?,
            b"first"
        );
        assert_eq!(
            fs::read(
                second
                    .parent()
                    .context("second snapshot")?
                    .join("fixture.dll")
            )?,
            b"second"
        );
        let mut lib64 = BTreeMap::new();
        fs::create_dir(fixture.path().join("native/lib64"))?;
        super::collect_linked_libraries(&fixture.path().join("native/lib64"), &mut lib64)?;
        assert_eq!(lib64.len(), 1);
        assert_eq!(
            lib64[&PathBuf::from("fixture.dll")],
            bin.join("fixture.dll")
        );
        Ok(())
    }

    #[test]
    fn prebuilt_rejects_invalid_bundle_file_lists() -> anyhow::Result<()> {
        // Given an installed executable, companion, and a file outside their directory.
        let fixture = tempfile::tempdir()?;
        let installed = fixture.path().join("installed");
        let cache = fixture.path().join("cache");
        fs::create_dir(&installed)?;
        let app = installed.join("app.exe");
        let helper = installed.join("helper.exe");
        fs::write(&app, b"app")?;
        fs::write(&helper, b"helper")?;
        fs::write(fixture.path().join("outside.bin"), b"outside")?;
        let base = vec!["app.exe", "helper.exe"];
        let cases = [
            serde_json::json!({"v": 2, "files": base}),
            serde_json::json!({"v": 1, "files": ["app.exe"]}),
            serde_json::json!({"v": 1, "files": ["helper.exe"]}),
            serde_json::json!({"v": 1, "files": ["app.exe", "helper.exe", "app.exe"]}),
            serde_json::json!({"v": 1, "files": ["app.exe", "helper.exe", "APP.exe"]}),
            serde_json::json!({"v": 1, "files": ["app.exe", "helper.exe", "../outside.bin"]}),
            serde_json::json!({"v": 1, "files": [app, helper]}),
            serde_json::json!({"v": 1, "files": ["app.exe", "helper.exe", ".reiny-bundle.json"]}),
        ];
        for manifest in cases {
            fs::write(
                installed.join(".reiny-bundle.json"),
                serde_json::to_vec(&manifest)?,
            )?;
            // When prebuilt preparation consumes an invalid declared inventory.
            let result = stage_prebuilt_bundle(
                &app,
                std::slice::from_ref(&helper),
                &cache,
                &CancelToken::default(),
            );
            // Then it fails instead of publishing a partial or escaping snapshot.
            assert!(result.is_err(), "{manifest}");
            assert!(!cache.exists());
        }
        Ok(())
    }

    #[test]
    #[cfg(windows)]
    fn cargo_path_preserves_unc_and_device_namespaces() {
        for path in [
            r"\\?\UNC\server\share\Cargo.toml",
            r"\\?\Volume{00000000-0000-0000-0000-000000000000}\Cargo.toml",
            r"\\server\share\Cargo.toml",
            r"C:\project\Cargo.toml",
        ] {
            assert_eq!(
                super::cargo_path(std::path::Path::new(path)).as_ref(),
                std::path::Path::new(path)
            );
        }
    }

    #[test]
    #[cfg(windows)]
    fn cargo_invocation_uses_stable_drive_paths_without_changing_group_identity()
    -> anyhow::Result<()> {
        use super::{BuildRequest, BuildSpec, CargoBuild, Environment};
        use anyhow::{Context, bail};
        use std::collections::BTreeSet;

        let fixture = tempfile::tempdir()?;
        fs::write(fixture.path().join("Cargo.toml"), "[workspace]\n")?;
        let directory = std::path::absolute(fixture.path())?;
        let base = fixture.path().canonicalize()?.join("cache");
        let spec = BuildSpec::Cargo(CargoBuild {
            manifest: directory.join("Cargo.toml"),
            cwd: Some(directory.clone()),
            ..CargoBuild::default()
        });
        let request = BuildRequest::new(&spec, &directory, &base, &Environment::new())?;
        let BuildSpec::Cargo(build) = &request.spec else {
            bail!("expected Cargo request");
        };
        let stored = build.manifest.clone();
        let key = request.key.clone();
        let command = request.cargo_command(build, &BTreeSet::from(["app".into()]))?;
        assert_eq!(command.get_current_dir(), Some(directory.as_path()));
        let args: Vec<_> = command.get_args().collect();
        let manifest = args
            .iter()
            .position(|arg| *arg == "--manifest-path")
            .context("manifest argument")?;
        assert_eq!(args[manifest + 1], directory.join("Cargo.toml").as_os_str());
        let target = args
            .iter()
            .position(|arg| *arg == "--target-dir")
            .context("target argument")?;
        assert_eq!(
            args[target + 1],
            directory
                .join("cache")
                .join("cargo")
                .join(&key[..32])
                .as_os_str()
        );
        assert_eq!(build.manifest, stored);
        assert_eq!(request.key, key);
        Ok(())
    }

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
