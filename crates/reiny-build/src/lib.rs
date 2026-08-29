//! reiny's build helper. Each launch's `build.rs` calls [`compile`].
//!
//! It reads `Reiny.toml`, compiles the protos it needs with prost, and writes `$OUT_DIR/reiny_generated.rs`:
//!
//! - the `publications` / `dependencies::<project>` / `internals` modules (re-exports of the generated types)
//! - one `impl ::reiny::Topic` per message type (the type → topic mapping, embedded)
//!
//! `#[reiny::main]` includes that file into the crate root, which is why user code names types as
//! `use crate::publications::Ping;`.
//!
//! Two layouts are handled:
//! - **per-project** (a Reiny.toml with `[project]`): resolves its own `[publications]` plus the public
//!   types of every `[dependencies]` project. The type → topic owner is "the project that publishes it".
//! - **workspace shared** (a Reiny.toml with `[internals]` / `[projects.*]`): compiles the whole shared
//!   catalog `[internals]` and exposes it as `internals::*`.

// This is a helper crate called from build scripts, so the right response to a misconfiguration is to
// stop the build at once with a context-carrying panic. The unwrap/expect/panic lints are lifted here.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeMap;
use std::env;
// writeln! is used only by the codegen and by the fingerprint (the descriptors feature).
#[cfg(feature = "descriptors")]
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

// ---------------------------------------------------------------------------
// The Reiny.toml schema
// ---------------------------------------------------------------------------

/// The whole Reiny.toml (it accepts the per-project shape and the workspace shape alike).
#[derive(Debug, Deserialize)]
struct Manifest {
    /// The per-project identity. Its presence is what selects per-project mode.
    project: Option<Project>,
    /// The per-project public types.
    #[serde(default)]
    publications: BTreeMap<String, TypeDef>,
    /// The per-project dependency projects.
    #[serde(default)]
    dependencies: BTreeMap<String, Dependency>,
    /// The workspace's shared catalog.
    #[serde(default)]
    internals: BTreeMap<String, TypeDef>,
    /// Each workspace project's publish / subscribe declarations.
    #[serde(default)]
    projects: BTreeMap<String, ProjectDecl>,
    /// The workspace's shared schema crates (with them, a type is generated once and shared).
    schema: Option<SchemaDecl>,
    /// The per-project typed config schema + defaults (read through `cloudy.config()`).
    config: Option<toml::Table>,
    /// `[services]`: request type → response type (generates `impl reiny::Service`).
    #[serde(default)]
    services: BTreeMap<String, ServiceDef>,
}

/// `Name = { request = "Req", response = "Resp" }`. The values are catalog aliases
/// (`[publications]` / `[internals]` keys; a per-project dependency type is `<dep>::<Alias>`).
#[derive(Debug, Deserialize)]
struct ServiceDef {
    request: String,
    response: String,
}

/// The two shapes of `[schema]`. `crate` is a keyword, so it is taken through a rename.
///
/// - **single** (0.2) `[schema] crate = "myapp-schema"` — one crate prost-compiles all of
///   `[internals]` and writes the `impl Topic`s; launches re-export it as a Cargo dependency.
/// - **multi-crate** (0.3) `[schema.<name>] crate/protos/depends` — the schema is split across
///   several independently publishable crates. Ownership follows the proto path, and a leaf type is
///   generated exactly once.
///
/// Untagged, so the single shape is tried first (a string `crate` key = single, otherwise a part table).
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum SchemaDecl {
    Single(SchemaSingle),
    Multi(BTreeMap<String, SchemaPartDef>),
}

#[derive(Debug, Deserialize)]
struct SchemaSingle {
    #[serde(rename = "crate")]
    crate_name: String,
}

/// One part of `[schema.<name>]`.
#[derive(Debug, Deserialize)]
struct SchemaPartDef {
    #[serde(rename = "crate")]
    crate_name: String,
    /// The protos this part owns (relative to the directory holding Reiny.toml).
    protos: Vec<String>,
    /// The part names it depends on. Must be **transitively closed**, and each edge needs a Cargo dep too.
    #[serde(default)]
    depends: Vec<String>,
}

/// One normalized schema crate. The single shape folds in as `name: None` / `protos: None`
/// (= it owns all of `[internals]`).
#[derive(Debug, Clone)]
struct SchemaPart {
    /// The part name from `[schema.<name>]`. `None` for the single shape.
    name: Option<String>,
    crate_name: String,
    /// The absolute paths of the owned protos. `None` for the single shape.
    protos: Option<Vec<PathBuf>>,
    depends: Vec<String>,
}

impl SchemaPart {
    /// The extern crate name (`myapp-proto-geometry` → `myapp_proto_geometry`).
    fn crate_ident(&self) -> String {
        self.crate_name.replace('-', "_")
    }
}

/// An introspection view of one schema crate (for `reiny check`).
#[derive(Debug, Clone)]
pub struct SchemaCrateInfo {
    /// The part name from `[schema.<name>]`. `None` for a single `[schema]`.
    pub part: Option<String>,
    /// The Cargo package name.
    pub crate_name: String,
    /// The part names it depends on.
    pub depends: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Project {
    /// The project name. The runtime instance id is decided by the launcher / `--id` and topics follow
    /// from types, so this is kept only for [project] mode detection and self-description.
    name: String,
}

/// `Type = { proto = "...", message = "pkg.Type" }`.
#[derive(Debug, Deserialize)]
struct TypeDef {
    proto: String,
    message: String,
}

/// `dep = { version = "0.1", path = "../dep" }`. `version` is not used for validation yet.
#[derive(Debug, Deserialize)]
struct Dependency {
    path: PathBuf,
}

/// The publications / dependencies of `[projects.<name>]` (naming catalog keys).
/// Besides the existence check, they are exposed as an introspection view through
/// [`Resolution::projects`] (`reiny run` builds its topic flow diagram from it).
#[derive(Debug, Default, Deserialize)]
struct ProjectDecl {
    #[serde(default)]
    publications: Vec<String>,
    #[serde(default)]
    dependencies: Vec<String>,
}

// ---------------------------------------------------------------------------
// The intermediate representation
// ---------------------------------------------------------------------------

/// Which generated module a type is emitted into.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Exposure {
    Publications,
    Internals,
    Dependencies(String),
}

/// One resolved service. `request` / `response` index into `Resolution::entries`.
#[derive(Debug, Clone)]
struct ServiceEntry {
    name: String,
    request: usize,
    response: usize,
}

/// One resolved message type.
#[derive(Debug, Clone)]
struct Entry {
    /// The public name in the generated module (the Reiny.toml key, e.g. `Ping`).
    alias: String,
    /// The proto package's segments (e.g. `["ping"]`).
    package: Vec<String>,
    /// The Rust type name (e.g. `Ping`). This becomes the topic's type segment (`reiny/<id>/Ping`).
    ident: String,
    /// Which module it is emitted into.
    exposure: Exposure,
    /// The absolute path of the proto to compile.
    proto: PathBuf,
    /// The schema part owning this type (multi-crate shape only). Follows the proto path.
    owner: Option<String>,
}

impl Entry {
    /// The proto's fully qualified message name (e.g. `hs.Vector3`). The key to the descriptor fingerprint.
    fn fq_name(&self) -> String {
        let mut segs = self.package.clone();
        segs.push(self.ident.clone());
        segs.join(".")
    }

    /// The type path as seen from `__reiny_generated` (e.g. `__pb::ping::Ping`).
    fn type_path(&self) -> String {
        let mut segs = vec!["__pb".to_string()];
        segs.extend(self.package.iter().cloned());
        segs.push(self.ident.clone());
        segs.join("::")
    }
}

// ---------------------------------------------------------------------------
// Resolution modes and results (public API; also used for introspection from the CLI)
// ---------------------------------------------------------------------------

/// Which layout the Reiny.toml was resolved as.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mode {
    /// per-project (`[project]`). Generates its own publications plus the dependencies' public types.
    PerProject,
    /// Workspace shared (`[internals]`/`[projects]`, no `[schema]`). Every launch prost-compiles
    /// `[internals]` for itself (as it always did).
    Workspace,
    /// Workspace + `[schema]`, and we are the **schema crate itself**: prost-compile our share and
    /// write the `impl Topic`s. Launches share it as a Cargo dependency.
    Schema {
        /// The part we are responsible for in the multi-crate shape (`[schema.<name>]`). `None` for a
        /// single `[schema]` (= it owns all of `[internals]`).
        part: Option<String>,
    },
    /// Workspace + `[schema]`, and we are a launch **consuming** the schema. No proto is recompiled;
    /// the schema crates' types are merely re-exported as `internals`.
    SchemaConsumer {
        /// The extern idents of the schema crates depended on (`myapp-schema` → `myapp_schema`).
        /// In the multi-crate shape every part is listed (`internals` is their union).
        crate_idents: Vec<String>,
    },
}

impl Mode {
    /// A human-readable label (for `reiny check`).
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Mode::PerProject => "per-project",
            Mode::Workspace => "workspace",
            Mode::Schema { part: None } => "workspace+schema",
            Mode::Schema { part: Some(_) } => "workspace+schema (part)",
            Mode::SchemaConsumer { .. } => "workspace+schema (consumer)",
        }
    }
}

/// An introspection view of one resolved message type ([`Entry`] made public for `reiny check`).
#[derive(Debug, Clone)]
pub struct TypeInfo {
    /// The public name in the generated module (the Reiny.toml key).
    pub alias: String,
    /// The proto's fully qualified message name (e.g. `ping.Ping`).
    pub message: String,
    /// The type → topic type segment (`reiny/<id>/<segment>`).
    pub topic_segment: String,
    /// Which generated module it appears in (`publications` / `internals` / `dependencies::<dep>`).
    pub module: String,
    /// The absolute path of the proto to compile.
    pub proto: PathBuf,
    /// The schema part that owns it (multi-crate shape only).
    pub owner: Option<String>,
}

/// The result of resolving a Reiny.toml. Pure information from before any proto is compiled, so it is
/// available without the `compile` feature. `reiny check` prints it; [`compile`] writes its output from it.
pub struct Resolution {
    mode: Mode,
    entries: Vec<Entry>,
    config: Option<toml::Table>,
    manifest_path: PathBuf,
    /// The normalized `[schema]` parts (empty when there are none).
    schema_parts: Vec<SchemaPart>,
    /// `[services]` (empty when there are none).
    services: Vec<ServiceEntry>,
    /// The `[projects.*]` declarations (workspace layout only; empty for per-project).
    projects: Vec<ProjectInfo>,
}

/// An introspection view of one `[projects.<name>]`. publications / dependencies hold the
/// `[internals]` keys (aliases) verbatim (look them up through [`Resolution::types`]'s `alias`).
#[derive(Debug, Clone)]
pub struct ProjectInfo {
    /// The `[projects.<name>]` key (= the package / default bin name).
    pub name: String,
    /// The aliases of the types it publishes.
    pub publications: Vec<String>,
    /// The aliases of the types it subscribes to.
    pub dependencies: Vec<String>,
}

/// An introspection view of one `[services]` entry (for `reiny check`).
#[derive(Debug, Clone)]
pub struct ServiceInfo {
    /// The `[services]` key (a display name; it does not appear in the generated code).
    pub name: String,
    /// The request type's alias (exactly as written in Reiny.toml).
    pub request: String,
    /// The request type's fully qualified message name (e.g. `calc.Add`).
    pub request_message: String,
    /// The response type's alias.
    pub response: String,
    /// The response type's fully qualified message name.
    pub response_message: String,
}

impl Resolution {
    /// Which layout it was resolved as.
    #[must_use]
    pub fn mode(&self) -> &Mode {
        &self.mode
    }

    /// The absolute path of the Reiny.toml that was used.
    #[must_use]
    pub fn manifest_path(&self) -> &Path {
        &self.manifest_path
    }

    /// Whether it has a `[config]` (the per-project typed configuration).
    #[must_use]
    pub fn has_config(&self) -> bool {
        self.config.is_some()
    }

    /// The schema crates `[schema]` declares (empty when none). The single shape is one entry with `part` = `None`.
    #[must_use]
    pub fn schema_crates(&self) -> Vec<SchemaCrateInfo> {
        self.schema_parts
            .iter()
            .map(|p| SchemaCrateInfo {
                part: p.name.clone(),
                crate_name: p.crate_name.clone(),
                depends: p.depends.clone(),
            })
            .collect()
    }

    /// The list of `[services]` (request type → response type).
    #[must_use]
    pub fn services(&self) -> Vec<ServiceInfo> {
        self.services
            .iter()
            .map(|s| {
                let req = &self.entries[s.request];
                let resp = &self.entries[s.response];
                ServiceInfo {
                    name: s.name.clone(),
                    request: req.alias.clone(),
                    request_message: req.fq_name(),
                    response: resp.alias.clone(),
                    response_message: resp.fq_name(),
                }
            })
            .collect()
    }

    /// The list of `[projects.*]` declarations (workspace layout only; empty for per-project).
    #[must_use]
    pub fn projects(&self) -> &[ProjectInfo] {
        &self.projects
    }

    /// The resolved types, with their topics and modules.
    #[must_use]
    pub fn types(&self) -> Vec<TypeInfo> {
        self.entries
            .iter()
            .map(|e| TypeInfo {
                alias: e.alias.clone(),
                message: {
                    let mut m = e.package.clone();
                    m.push(e.ident.clone());
                    m.join(".")
                },
                topic_segment: e.ident.clone(),
                module: match &e.exposure {
                    Exposure::Publications => "publications".to_string(),
                    Exposure::Internals => "internals".to_string(),
                    Exposure::Dependencies(d) => format!("dependencies::{d}"),
                },
                proto: e.proto.clone(),
                owner: e.owner.clone(),
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

/// prost-build itself. Re-exported so a downstream `build.rs` can name the `Config` type that
/// [`compile_with`]'s closure receives (which keeps the two versions from drifting apart).
#[cfg(feature = "compile")]
pub use prost_build;

/// Called from `build.rs`. Reads Reiny.toml, compiles the protos and puts the output in `$OUT_DIR`.
///
/// Needs the `compile` feature (on by default). For introspection that does not want prost pulled in —
/// as in `reiny check` — use [`describe`] directly.
#[cfg(feature = "compile")]
pub fn compile() -> Result<()> {
    compile_with(|_| {})
}

/// The same as [`compile`], but with access to the `prost_build::Config` just before prost gets it.
///
/// An escape hatch so reiny does not block off prost's knobs: `type_attribute` to derive serde on wire
/// types, `file_descriptor_set_path` for dynamic decoding through `prost-reflect`, `bytes()` /
/// `btree_map` / `boxed` … none of which needs a dedicated API on reiny's side.
///
/// ```ignore
/// // build.rs
/// reiny_build::compile_with(|c| {
///     c.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
/// })
/// .expect("reiny codegen");
/// ```
///
/// `out_dir` / `include_file` are what reiny assembles its output with, so overriding them from the
/// closure only breaks the fit with that output (leave them alone).
#[cfg(feature = "compile")]
pub fn compile_with(customize: impl FnOnce(&mut prost_build::Config)) -> Result<()> {
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").context("CARGO_MANIFEST_DIR not set")?);
    let out_dir = PathBuf::from(env::var("OUT_DIR").context("OUT_DIR not set")?);
    let pkg_name = env::var("CARGO_PKG_NAME").context("CARGO_PKG_NAME not set")?;

    let resolution = resolve_for(&manifest_dir, &pkg_name)?;
    report_verbose(&resolution);

    // A schema-consuming launch recompiles no protos and merely re-exports the schema crates.
    // Everything else compiles its share of the protos and emits the full output.
    let generated = if let Mode::SchemaConsumer { crate_idents } = &resolution.mode {
        render_consumer(crate_idents)
    } else {
        let plan = compile_plan(&resolution)?;
        let fds = compile_protos(&plan, &out_dir, customize)?;
        let scan = scan_descriptors(&fds, &plan.own_names);
        if let Some(meta) = &plan.emit_meta {
            emit_schema_metadata(meta, &scan.fqns)?;
        }
        render_generated(
            &plan.entries,
            resolution.config.as_ref(),
            &scan.fingerprints,
            &service_impls(&resolution, &plan.entries),
        )?
    };

    let generated_path = out_dir.join("reiny_generated.rs");
    std::fs::write(&generated_path, generated)
        .with_context(|| format!("writing {}", generated_path.display()))?;

    Ok(())
}

/// Introspection for `reiny check`. It resolves the **whole catalog** the Reiny.toml describes rather
/// than one package's view of it (in a workspace, all of `[internals]`; per-project, own publications
/// plus dependencies). No proto is compiled, so it works without the `compile` feature. Validation
/// (identifiers, topic collisions) still runs, so a misconfigured layout surfaces here.
pub fn describe(dir: &Path) -> Result<Resolution> {
    let (manifest_path, manifest) = find_manifest(dir)
        .with_context(|| format!("locating Reiny.toml from {}", dir.display()))?;
    let manifest_root = manifest_path
        .parent()
        .expect("Reiny.toml has a parent")
        .to_path_buf();

    let schema_parts = normalize_schema(&manifest, &manifest_root)?;

    let (mode, entries) = if manifest.project.is_some() {
        (
            Mode::PerProject,
            resolve_per_project(&manifest, &manifest_root)?,
        )
    } else if !manifest.internals.is_empty() || !manifest.projects.is_empty() {
        // The catalog view: bound to no single package. Say so when there is a [schema].
        let mut entries = internals_entries(&manifest, &manifest_root)?;
        assign_owners(&mut entries, &schema_parts, &manifest_path)?;
        let mode = if schema_parts.is_empty() {
            Mode::Workspace
        } else {
            Mode::Schema { part: None }
        };
        (mode, entries)
    } else {
        bail!(
            "{} has neither [project] (per-project) nor [internals]/[projects] (workspace)",
            manifest_path.display()
        );
    };

    validate_no_topic_collision(&entries, &manifest_path)?;
    let services = resolve_services(&manifest, &entries, &schema_parts, &manifest_path)?;

    Ok(Resolution {
        mode,
        entries,
        projects: project_infos(&manifest),
        config: manifest.config,
        manifest_path,
        schema_parts,
        services,
    })
}

/// Find the Reiny.toml → decide the mode → validate → build a [`Resolution`] (no proto touched yet).
/// Called from build.rs (`compile`), from one package's point of view.
#[cfg(feature = "compile")]
fn resolve_for(manifest_dir: &Path, pkg_name: &str) -> Result<Resolution> {
    let (manifest_path, manifest) = find_manifest(manifest_dir)
        .with_context(|| format!("locating Reiny.toml from {}", manifest_dir.display()))?;
    let manifest_root = manifest_path
        .parent()
        .expect("Reiny.toml has a parent")
        .to_path_buf();
    rerun_if_changed(&manifest_path);

    let schema_parts = normalize_schema(&manifest, &manifest_root)?;

    let (mode, entries) = if manifest.project.is_some() {
        (
            Mode::PerProject,
            resolve_per_project(&manifest, &manifest_root)?,
        )
    } else if !manifest.internals.is_empty() || !manifest.projects.is_empty() {
        resolve_workspace(
            &manifest,
            &manifest_root,
            pkg_name,
            &manifest_path,
            &schema_parts,
        )?
    } else {
        bail!(
            "{} has neither [project] (per-project) nor [internals]/[projects] (workspace)",
            manifest_path.display()
        );
    };

    validate_no_topic_collision(&entries, &manifest_path)?;
    let services = resolve_services(&manifest, &entries, &schema_parts, &manifest_path)?;

    Ok(Resolution {
        mode,
        entries,
        projects: project_infos(&manifest),
        config: manifest.config,
        manifest_path,
        schema_parts,
        services,
    })
}

/// Copy `[projects.*]` into the introspection view.
fn project_infos(manifest: &Manifest) -> Vec<ProjectInfo> {
    manifest
        .projects
        .iter()
        .map(|(name, d)| ProjectInfo {
            name: name.clone(),
            publications: d.publications.clone(),
            dependencies: d.dependencies.clone(),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Finding and resolving the Reiny.toml
// ---------------------------------------------------------------------------

/// Search upward from `start` for a `Reiny.toml` (the nearest one wins).
fn find_manifest(start: &Path) -> Result<(PathBuf, Manifest)> {
    let mut dir = Some(start.to_path_buf());
    while let Some(d) = dir {
        let candidate = d.join("Reiny.toml");
        if candidate.is_file() {
            let manifest = parse_manifest(&candidate)?;
            return Ok((candidate, manifest));
        }
        dir = d.parent().map(Path::to_path_buf);
    }
    bail!(
        "Reiny.toml not found in {} or any parent directory",
        start.display()
    )
}

fn parse_manifest(path: &Path) -> Result<Manifest> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

/// per-project: own publications plus each dependency project's public types.
fn resolve_per_project(manifest: &Manifest, root: &Path) -> Result<Vec<Entry>> {
    let _project = manifest.project.as_ref().expect("project present");
    let mut entries = Vec::new();

    // Our own public types.
    for (alias, td) in &manifest.publications {
        ensure_rust_ident(alias, "publication alias", "[publications]")?;
        entries.push(make_entry(alias, td, root, Exposure::Publications)?);
    }

    // The dependency projects' public types (re-exported as dependencies::<dep>::*).
    for (dep_name, dep) in &manifest.dependencies {
        // A dep name becomes `pub mod <dep>` in the generated code, so it must be a Rust identifier.
        ensure_rust_ident(dep_name, "dependency key", "[dependencies]")?;
        let dep_dir = resolve_relative(root, &dep.path);
        let dep_manifest_path = dep_dir.join("Reiny.toml");
        let dep_manifest = parse_manifest(&dep_manifest_path).with_context(|| {
            format!(
                "dependency '{}' Reiny.toml at {}",
                dep_name,
                dep_manifest_path.display()
            )
        })?;
        rerun_if_changed(&dep_manifest_path);
        dep_manifest.project.as_ref().with_context(|| {
            format!(
                "dependency '{}' ({}) is not a per-project Reiny.toml (no [project])",
                dep_name,
                dep_manifest_path.display()
            )
        })?;
        for (alias, td) in &dep_manifest.publications {
            ensure_rust_ident(alias, "publication alias", "dependency [publications]")?;
            entries.push(make_entry(
                alias,
                td,
                &dep_dir,
                Exposure::Dependencies(dep_name.clone()),
            )?);
        }
    }

    Ok(entries)
}

/// Workspace shared: all of [internals] into `internals::*`. A topic follows the type name, so it does
/// not depend on which project publishes it. With a `[schema]`, the mode splits on whether we are the
/// schema crate itself or a consuming launch. Called from build.rs (`compile`) per package.
#[cfg(feature = "compile")]
fn resolve_workspace(
    manifest: &Manifest,
    root: &Path,
    pkg_name: &str,
    manifest_path: &Path,
    parts: &[SchemaPart],
) -> Result<(Mode, Vec<Entry>)> {
    // The mode follows from whether there is a [schema] and whether we are the schema crate itself.
    let in_projects = manifest.projects.contains_key(pkg_name);
    let mode = if parts.is_empty() {
        if in_projects {
            Mode::Workspace
        } else {
            bail!(
                "package '{pkg_name}' has no [projects.{pkg_name}] entry in the workspace \
                 Reiny.toml ({})",
                manifest_path.display()
            );
        }
    } else if let Some(mine) = parts.iter().find(|p| p.crate_name == pkg_name) {
        Mode::Schema {
            part: mine.name.clone(),
        }
    } else if in_projects {
        // A consuming launch has to appear in [projects.<pkg>]. internals is the union of all parts.
        Mode::SchemaConsumer {
            crate_idents: parts.iter().map(SchemaPart::crate_ident).collect(),
        }
    } else {
        let names: Vec<&str> = parts.iter().map(|p| p.crate_name.as_str()).collect();
        bail!(
            "package '{pkg_name}' は {} の [projects.{pkg_name}] にも、スキーマクレート \
             ({}) のいずれにも該当しません",
            manifest_path.display(),
            names.join(" / ")
        );
    };

    // entries resolves [internals] in every mode (in consumer mode it only feeds introspection and
    // diagnostics; no proto is compiled). The owning part follows from the proto path.
    let mut entries = internals_entries(manifest, root)?;
    assign_owners(&mut entries, parts, manifest_path)?;

    Ok((mode, entries))
}

/// Turn `[internals]` into `Exposure::Internals` entries (the owning part is still empty).
fn internals_entries(manifest: &Manifest, root: &Path) -> Result<Vec<Entry>> {
    let mut entries = Vec::new();
    for (alias, td) in &manifest.internals {
        ensure_rust_ident(alias, "internals alias", "[internals]")?;
        entries.push(make_entry(alias, td, root, Exposure::Internals)?);
    }
    Ok(entries)
}

// ---------------------------------------------------------------------------
// Normalizing [schema] and assigning ownership
// ---------------------------------------------------------------------------

/// The canonical form for comparing paths, so that `[internals].proto` and `[schema.*].protos`
/// naming the same file in different spellings (`./x.proto` vs `x.proto`) still compare equal.
fn canonical(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

/// Normalize `[schema]` into a list of parts and validate the identifiers and the dependencies.
/// Empty when there is no `[schema]`.
fn normalize_schema(manifest: &Manifest, root: &Path) -> Result<Vec<SchemaPart>> {
    let Some(decl) = &manifest.schema else {
        return Ok(Vec::new());
    };
    let parts: Vec<SchemaPart> = match decl {
        SchemaDecl::Single(single) => vec![SchemaPart {
            name: None,
            crate_name: single.crate_name.clone(),
            protos: None,
            depends: Vec::new(),
        }],
        SchemaDecl::Multi(map) => {
            if map.is_empty() {
                bail!(
                    "[schema] に区画がありません([schema] crate = \"...\" か [schema.<name>] を書いてください)"
                );
            }
            let mut out = Vec::new();
            for (name, def) in map {
                if def.protos.is_empty() {
                    bail!(
                        "[schema.{name}] の protos が空です(この区画が所有する proto を列挙してください)"
                    );
                }
                let mut protos = Vec::new();
                for p in &def.protos {
                    let abs = resolve_relative(root, Path::new(p));
                    if !abs.is_file() {
                        bail!(
                            "[schema.{name}] の proto が見つかりません: {}",
                            abs.display()
                        );
                    }
                    protos.push(abs);
                }
                out.push(SchemaPart {
                    name: Some(name.clone()),
                    crate_name: def.crate_name.clone(),
                    protos: Some(protos),
                    depends: def.depends.clone(),
                });
            }
            out
        }
    };

    for p in &parts {
        ensure_rust_ident(&p.crate_ident(), "[schema].crate", "[schema]")?;
    }
    validate_depends(&parts)?;
    Ok(parts)
}

/// Check that `depends` names real parts, has no cycles, and is **transitively closed**.
///
/// The closure is required because of protoc: if `c`'s proto imports `b`'s and that `b` imports `a`'s,
/// then `a`'s types land in `c`'s descriptor set as well. Unless `c` externs `a`, `a`'s types are
/// generated into `c` too and the same type exists twice.
/// cargo hands `DEP_*` only to **direct** dependents, so reiny has to demand the closure itself.
fn validate_depends(parts: &[SchemaPart]) -> Result<()> {
    let by_name: BTreeMap<&str, &SchemaPart> = parts
        .iter()
        .filter_map(|p| p.name.as_deref().map(|n| (n, p)))
        .collect();

    for p in parts {
        let Some(me) = p.name.as_deref() else {
            continue;
        };
        for dep in &p.depends {
            if dep == me {
                bail!("[schema.{me}] の depends が自分自身を指しています");
            }
            if !by_name.contains_key(dep.as_str()) {
                bail!("[schema.{me}] の depends にある `{dep}` という区画がありません");
            }
        }
        // Walk the transitive closure and name what was left undeclared (this catches cycles too).
        let mut stack: Vec<&str> = p.depends.iter().map(String::as_str).collect();
        let mut seen: Vec<&str> = Vec::new();
        while let Some(cur) = stack.pop() {
            if cur == me {
                bail!("[schema] の depends に循環があります(`{me}` に戻ってきました)");
            }
            if seen.contains(&cur) {
                continue;
            }
            seen.push(cur);
            let Some(part) = by_name.get(cur) else {
                continue;
            };
            for next in &part.depends {
                if !p.depends.iter().any(|d| d == next) {
                    bail!(
                        "[schema.{me}] の depends は推移的に閉じている必要があります: \
                         `{cur}` が `{next}` に依存しているので、[schema.{me}] の depends にも \
                         `{next}` を、Cargo の依存にもそのクレートを足してください"
                    );
                }
                stack.push(next);
            }
        }
    }
    Ok(())
}

/// Decide each `[internals]` type's owning part from its proto path. Only meaningful in the multi-crate shape.
/// A proto belonging to no part, or one two parts fight over, is rejected here.
fn assign_owners(entries: &mut [Entry], parts: &[SchemaPart], manifest_path: &Path) -> Result<()> {
    // The single shape (protos = None) owns everything, so nothing needs assigning.
    if parts.iter().all(|p| p.protos.is_none()) {
        return Ok(());
    }

    let mut owner_of: BTreeMap<PathBuf, &str> = BTreeMap::new();
    for part in parts {
        let (Some(name), Some(protos)) = (part.name.as_deref(), part.protos.as_ref()) else {
            continue;
        };
        for proto in protos {
            let key = canonical(proto);
            if let Some(prev) = owner_of.get(&key) {
                bail!(
                    "proto {} を [schema.{prev}] と [schema.{name}] が両方 protos に挙げています",
                    proto.display()
                );
            }
            owner_of.insert(key, name);
        }
    }

    for e in entries {
        let key = canonical(&e.proto);
        let Some(owner) = owner_of.get(&key) else {
            bail!(
                "{} の [internals] `{}` が使う proto {} は、どの [schema.<name>] の protos にも \
                 挙がっていません(所有区画は proto パスで決まります)",
                manifest_path.display(),
                e.alias,
                e.proto.display()
            );
        };
        e.owner = Some((*owner).to_string());
    }
    Ok(())
}

/// Build an `Entry` from a `TypeDef`. The proto path is made absolute against `base`.
fn make_entry(alias: &str, td: &TypeDef, base: &Path, exposure: Exposure) -> Result<Entry> {
    let (package, ident) = split_message(&td.message)
        .with_context(|| format!("invalid message path '{}'", td.message))?;
    let proto = resolve_relative(base, Path::new(&td.proto));
    if !proto.is_file() {
        bail!("proto file not found: {}", proto.display());
    }
    rerun_if_changed(&proto);
    Ok(Entry {
        alias: alias.to_string(),
        package,
        ident,
        exposure,
        proto,
        owner: None,
    })
}

/// `"ping.Ping"` → (`["ping"]`, `"Ping"`). `"Ping"` → (`[]`, `"Ping"`).
fn split_message(message: &str) -> Result<(Vec<String>, String)> {
    let parts: Vec<&str> = message.split('.').filter(|s| !s.is_empty()).collect();
    let (ident, package) = parts.split_last().context("empty message path")?;
    Ok((
        package.iter().map(ToString::to_string).collect(),
        ident.to_string(),
    ))
}

// ---------------------------------------------------------------------------
// The compile plan (built only with the compile feature; CLI introspection does not use it)
// ---------------------------------------------------------------------------

/// "What this package compiles and what it merely references", gathered into one place.
/// For a single `[schema]` / workspace / per-project it simply holds everything; only while building
/// one `[schema.<name>]` part is it narrowed to our share, with `externs` filled in.
#[cfg(feature = "compile")]
struct CompilePlan<'a> {
    /// The types to emit (when building a part, only the ones we own).
    entries: Vec<&'a Entry>,
    /// The protos handed to prost.
    protos: Vec<PathBuf>,
    /// protoc's include directories.
    includes: Vec<PathBuf>,
    /// `(".hs.Vector3", "::myapp_proto_geometry::__pb::hs::Vector3")`.
    /// With these, prost **does not generate** the type and only rewrites the reference.
    externs: Vec<(String, String)>,
    /// Our own file names as they appear in the descriptor. Empty means "all of it is ours".
    own_names: Vec<String>,
    /// `Some` when `links` metadata is emitted (= other schema crates may reference us).
    emit_meta: Option<PartMeta>,
}

/// What a schema part hands downstream.
#[cfg(feature = "compile")]
struct PartMeta {
    crate_name: String,
    include: PathBuf,
}

#[cfg(feature = "compile")]
fn compile_plan(resolution: &Resolution) -> Result<CompilePlan<'_>> {
    let part_name = match &resolution.mode {
        Mode::Schema { part } => part.clone(),
        _ => None,
    };

    let Some(part_name) = part_name else {
        // Single [schema] / workspace / per-project: compile all of it ourselves, as ever.
        let mut protos: Vec<PathBuf> = Vec::new();
        let mut includes: Vec<PathBuf> = Vec::new();
        for e in &resolution.entries {
            if !protos.contains(&e.proto) {
                protos.push(e.proto.clone());
            }
            if let Some(parent) = e.proto.parent()
                && !includes.contains(&parent.to_path_buf())
            {
                includes.push(parent.to_path_buf());
            }
        }
        return Ok(CompilePlan {
            entries: resolution.entries.iter().collect(),
            protos,
            includes,
            externs: Vec::new(),
            own_names: Vec::new(),
            emit_meta: None,
        });
    };

    let part = resolution
        .schema_parts
        .iter()
        .find(|p| p.name.as_deref() == Some(part_name.as_str()))
        .with_context(|| format!("[schema.{part_name}] が見つかりません"))?;
    let protos = part
        .protos
        .clone()
        .with_context(|| format!("[schema.{part_name}] に protos がありません"))?;

    // Make the common ancestor of our protos the single include. The file names in the descriptor
    // follow from it too, so it must be the very include handed downstream.
    let root = common_ancestor(&protos)
        .with_context(|| format!("[schema.{part_name}] の protos に共通の親がありません"))?;
    let own_names = protos
        .iter()
        .map(|p| {
            p.strip_prefix(&root).map_or_else(
                |_| p.to_string_lossy().into_owned(),
                |r| r.to_string_lossy().replace('\\', "/"),
            )
        })
        .collect();

    let mut includes = vec![root.clone()];
    let mut externs = Vec::new();
    for dep_name in &part.depends {
        let dep = resolution
            .schema_parts
            .iter()
            .find(|p| p.name.as_deref() == Some(dep_name.as_str()))
            .with_context(|| format!("[schema.{dep_name}] が見つかりません"))?;
        let (include, types) = read_dep_metadata(&dep.crate_name)?;
        if !includes.contains(&include) {
            includes.push(include);
        }
        let ident = dep.crate_ident();
        for fqn in types {
            let rust = format!("::{ident}::__pb::{}", fqn.replace('.', "::"));
            externs.push((format!(".{fqn}"), rust));
        }
    }

    let entries = resolution
        .entries
        .iter()
        .filter(|e| e.owner.as_deref() == Some(part_name.as_str()))
        .collect();

    Ok(CompilePlan {
        entries,
        protos,
        includes,
        externs,
        own_names,
        emit_meta: Some(PartMeta {
            crate_name: part.crate_name.clone(),
            include: root,
        }),
    })
}

/// The common parent directory of the given files.
#[cfg(feature = "compile")]
fn common_ancestor(paths: &[PathBuf]) -> Option<PathBuf> {
    let mut iter = paths.iter().map(|p| p.parent().map(Path::to_path_buf));
    let mut acc = iter.next()??;
    for next in iter {
        let next = next?;
        while !next.starts_with(&acc) {
            acc = acc.parent()?.to_path_buf();
        }
    }
    Some(acc)
}

/// The name mangling cargo uses for `DEP_<LINKS>_<KEY>` (upcase, non-alphanumerics to `_`).
#[cfg(feature = "compile")]
fn envify(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

/// Read the include directory and FQN list a dependency schema crate handed over through `links`.
///
/// cargo only passes on metadata emitted by a **direct** dependency's build script, so not getting it
/// here usually means either "it is not a Cargo dependency" or "it does not declare `links`".
#[cfg(feature = "compile")]
fn read_dep_metadata(dep_crate: &str) -> Result<(PathBuf, Vec<String>)> {
    let key = envify(dep_crate);
    let include = env::var(format!("DEP_{key}_PROTO_INCLUDE"));
    let types = env::var(format!("DEP_{key}_PROTO_TYPES"));
    let (Ok(include), Ok(types)) = (include, types) else {
        bail!(
            "スキーマクレート `{dep_crate}` のメタ(DEP_{key}_PROTO_INCLUDE / _PROTO_TYPES)が \
             見つかりません。(1) `{dep_crate}` をこのクレートの [dependencies] に直接足す、\
             (2) `{dep_crate}` の Cargo.toml に `links = \"{dep_crate}\"` を書く —— の 2 つを \
             確認してください(cargo は直接依存にしかメタを渡しません)"
        );
    };
    let types = types
        .split(',')
        .filter(|t| !t.is_empty())
        .map(ToString::to_string)
        .collect();
    Ok((PathBuf::from(include), types))
}

/// Hand our own include directory and the FQNs we define downstream.
/// Without `links` cargo distributes none of it, so make that noticeable on the spot.
#[cfg(feature = "compile")]
fn emit_schema_metadata(meta: &PartMeta, fqns: &[String]) -> Result<()> {
    // Re-run the check below the moment links is added or removed. reiny-build states its own
    // rerun-if-changed, so without this a fixed Cargo.toml would not re-run the build script.
    println!("cargo:rerun-if-env-changed=CARGO_MANIFEST_LINKS");
    let want = envify(&meta.crate_name);
    match env::var("CARGO_MANIFEST_LINKS") {
        Ok(links) if envify(&links) == want => {}
        Ok(links) => bail!(
            "`{}` の Cargo.toml の links が `{links}` になっています。reiny は依存側から \
             DEP_{want}_* を引くので、`links = \"{}\"` にしてください",
            meta.crate_name,
            meta.crate_name
        ),
        Err(_) => bail!(
            "スキーマクレート `{}` の Cargo.toml に `links = \"{}\"` が要ります。\
             これが無いと cargo が proto の include パスと型一覧を依存側へ渡せません \
             (reiny は Cargo.toml を書き換えられないので、この 1 行だけ手で足してください)",
            meta.crate_name,
            meta.crate_name
        ),
    }
    println!("cargo:proto_include={}", meta.include.display());
    println!("cargo:proto_types={}", fqns.join(","));
    Ok(())
}

// ---------------------------------------------------------------------------
// Proto compilation and walking the descriptor
// ---------------------------------------------------------------------------

#[cfg(feature = "compile")]
fn compile_protos(
    plan: &CompilePlan<'_>,
    out_dir: &Path,
    customize: impl FnOnce(&mut prost_build::Config),
) -> Result<prost_types::FileDescriptorSet> {
    let (protos, includes) = (&plan.protos, &plan.includes);

    let descriptor_path = out_dir.join("reiny_descriptors.bin");
    let mut config = prost_build::Config::new();
    config
        .out_dir(out_dir)
        // Bundle every package into one file so it can be included as nested pub mods.
        .include_file("reiny_protos.rs")
        // Both the type → fingerprint (Topic::SCHEMA) and the FQN list handed downstream come from here.
        .file_descriptor_set_path(&descriptor_path);
    // Types owned by another schema crate get "the reference rewritten, nothing generated".
    // That is the mechanism keeping a leaf type from being generated twice across the split.
    for (proto_path, rust_path) in &plan.externs {
        config.extern_path(proto_path.clone(), rust_path.clone());
    }
    // Generated types come from prost-derive and refer to `::prost`, so the downstream crate needs a
    // `prost` dependency (the same assumption prost / tonic make). prost_path only changes the derive
    // call; the `::prost` inside the expansion stays, so keep the default and let downstream carry prost.

    // Use the bundled binary unless protoc was supplied from outside (so no external install is needed).
    // Put it on the config rather than the process-global `env::set_var("PROTOC")` — a build script is
    // single-threaded, but not rewriting someone else's process environment is better where possible.
    println!("cargo:rerun-if-env-changed=PROTOC");
    if env::var_os("PROTOC").is_none()
        && let Ok(protoc) = protoc_bin_vendored::protoc_bin_path()
    {
        config.protoc_executable(protoc);
    }

    // The caller's customization is applied **after** reiny's defaults (so it can override them).
    customize(&mut config);

    config
        .compile_protos(protos, includes)
        .context("prost: compiling protos")?;

    let bytes = std::fs::read(&descriptor_path)
        .with_context(|| format!("reading {}", descriptor_path.display()))?;
    <prost_types::FileDescriptorSet as prost::Message>::decode(bytes.as_slice())
        .context("decoding the descriptor set prost just wrote")
}

/// Pull "the FQNs we define" and "type → schema fingerprint" out of a descriptor set.
#[cfg(feature = "compile")]
struct DescriptorScan {
    /// The FQNs of the messages / enums our own files define (in declaration order).
    fqns: Vec<String>,
    /// Message FQN → fingerprint.
    fingerprints: BTreeMap<String, u64>,
}

/// Collect the FQNs of the files listed in `own_names` as "ours" (empty means all of them are ours).
/// Fingerprints are computed for every message, imported ones included — only our own types are ever
/// looked up, so it does no harm and it removes a branch.
#[cfg(feature = "compile")]
fn scan_descriptors(fds: &prost_types::FileDescriptorSet, own_names: &[String]) -> DescriptorScan {
    let mut scan = DescriptorScan {
        fqns: Vec::new(),
        fingerprints: BTreeMap::new(),
    };
    for file in &fds.file {
        let mine = own_names.is_empty() || own_names.iter().any(|n| n == file.name());
        let prefix = if file.package().is_empty() {
            String::new()
        } else {
            format!("{}.", file.package())
        };
        for msg in &file.message_type {
            walk_message(msg, &prefix, mine, &mut scan);
        }
        for en in &file.enum_type {
            let fq = format!("{prefix}{}", en.name());
            if mine {
                scan.fqns.push(fq);
            }
        }
    }
    scan
}

#[cfg(feature = "compile")]
fn walk_message(
    msg: &prost_types::DescriptorProto,
    prefix: &str,
    mine: bool,
    scan: &mut DescriptorScan,
) {
    let fq = format!("{prefix}{}", msg.name());
    if mine {
        scan.fqns.push(fq.clone());
    }
    scan.fingerprints.insert(fq.clone(), fingerprint(&fq, msg));

    let nested_prefix = format!("{fq}.");
    for nested in &msg.nested_type {
        // A map field's synthesized type (`FooEntry`) is invisible downstream, so do not count it.
        if nested
            .options
            .as_ref()
            .is_some_and(prost_types::MessageOptions::map_entry)
        {
            continue;
        }
        walk_message(nested, &nested_prefix, mine, scan);
    }
    for en in &msg.enum_type {
        if mine {
            scan.fqns.push(format!("{nested_prefix}{}", en.name()));
        }
    }
}

/// One message's schema fingerprint.
///
/// The ingredients are **only the fields the message itself declares** (number / name / type / label /
/// referenced type name / oneof membership). It does not follow a referenced message any deeper — what a
/// fingerprint guards against is "a same-named but different type landing on the same topic", and the
/// top-level shape settles that. If a leaf's changes must matter, that leaf should be the topic type.
///
/// The hash is FNV-1a 64. `DefaultHasher` is unusable because its values can change between Rust
/// releases (a fingerprint is worthless unless it is stable across builds).
#[cfg(feature = "descriptors")]
fn fingerprint(fq_name: &str, msg: &prost_types::DescriptorProto) -> u64 {
    let mut fields: Vec<&prost_types::FieldDescriptorProto> = msg.field.iter().collect();
    fields.sort_by_key(|f| f.number());

    let mut canonical = format!("m {fq_name}\n");
    for f in fields {
        writeln!(
            canonical,
            "f {} {} {} {} {} {}",
            f.number(),
            f.name(),
            f.r#type() as i32,
            f.label() as i32,
            f.type_name(),
            f.oneof_index.unwrap_or(-1)
        )
        .ok();
    }
    fnv1a64(canonical.as_bytes())
}

#[cfg(feature = "descriptors")]
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

// ---------------------------------------------------------------------------
// Reading an encoded descriptor set (what `reiny bag` uses on an `@schema` response)
// ---------------------------------------------------------------------------

/// Look up the fingerprint of the fully qualified message name `message` in an encoded
/// `FileDescriptorSet`. `Ok(None)` when the set does not have it. It is the same computation as a
/// generated type's `Topic::SCHEMA`, so it can be matched against what rides on the bus.
#[cfg(feature = "descriptors")]
pub fn message_fingerprint(file_set: &[u8], message: &str) -> Result<Option<u64>> {
    let fds = decode_file_set(file_set)?;
    Ok(find_message(&fds, message).map(|(_, msg)| fingerprint(message, msg)))
}

/// The `FileDescriptorSet` (encoded) pruned to the file defining `message` plus its transitive imports.
/// `Ok(None)` when the set does not have it.
///
/// An MCAP schema is one record per type, so putting a whole crate's set in each would put (number of
/// types × set size) into every bag. Pruning to the files actually needed avoids that.
#[cfg(feature = "descriptors")]
pub fn descriptor_subset(file_set: &[u8], message: &str) -> Result<Option<Vec<u8>>> {
    let fds = decode_file_set(file_set)?;
    let Some((root, _)) = find_message(&fds, message) else {
        return Ok(None);
    };
    // Collect the transitive imports. The order is the original set's (deterministically).
    let mut wanted: Vec<&str> = vec![root.name()];
    let mut cursor = 0;
    while cursor < wanted.len() {
        let name = wanted[cursor];
        cursor += 1;
        if let Some(file) = fds.file.iter().find(|f| f.name() == name) {
            for dep in &file.dependency {
                if !wanted.contains(&dep.as_str()) {
                    wanted.push(dep);
                }
            }
        }
    }
    let subset = prost_types::FileDescriptorSet {
        file: fds
            .file
            .iter()
            .filter(|f| wanted.contains(&f.name()))
            .cloned()
            .collect(),
    };
    Ok(Some(prost::Message::encode_to_vec(&subset)))
}

#[cfg(feature = "descriptors")]
fn decode_file_set(bytes: &[u8]) -> Result<prost_types::FileDescriptorSet> {
    <prost_types::FileDescriptorSet as prost::Message>::decode(bytes)
        .context("decoding FileDescriptorSet")
}

/// Find a descriptor by fully qualified message name (`pkg.Outer.Inner`), descending into nested types.
#[cfg(feature = "descriptors")]
fn find_message<'a>(
    fds: &'a prost_types::FileDescriptorSet,
    message: &str,
) -> Option<(
    &'a prost_types::FileDescriptorProto,
    &'a prost_types::DescriptorProto,
)> {
    for file in &fds.file {
        let rest = if file.package().is_empty() {
            message
        } else {
            match message.strip_prefix(file.package()) {
                Some(r) => r.strip_prefix('.')?,
                None => continue,
            }
        };
        let mut segs = rest.split('.');
        let first = segs.next()?;
        let mut cur = file.message_type.iter().find(|m| m.name() == first)?;
        for seg in segs {
            cur = cur.nested_type.iter().find(|m| m.name() == seg)?;
        }
        return Some((file, cur));
    }
    None
}

// ---------------------------------------------------------------------------
// Generating reiny_generated.rs
// ---------------------------------------------------------------------------

/// Assemble the contents of `$OUT_DIR/reiny_generated.rs`. The paths are written on the assumption that
/// it is included inside `#[reiny::main]`'s `mod __reiny_generated`.
#[cfg(feature = "compile")]
fn render_generated(
    entries: &[&Entry],
    config: Option<&toml::Table>,
    fingerprints: &BTreeMap<String, u64>,
    services: &[(String, String)],
) -> Result<String> {
    let mut out = String::new();
    out.push_str("// @generated by reiny-build — do not edit.\n");

    // Every package prost bundled.
    out.push_str("#[allow(clippy::all, unused_imports, dead_code)]\n");
    out.push_str("pub mod __pb {\n");
    out.push_str("    include!(concat!(env!(\"OUT_DIR\"), \"/reiny_protos.rs\"));\n");
    out.push_str("}\n\n");

    // The per-module re-exports.
    let publications: Vec<&Entry> = entries
        .iter()
        .copied()
        .filter(|e| e.exposure == Exposure::Publications)
        .collect();
    if !publications.is_empty() {
        render_reexport_module(&mut out, "publications", "super", &publications);
    }

    let internals: Vec<&Entry> = entries
        .iter()
        .copied()
        .filter(|e| e.exposure == Exposure::Internals)
        .collect();
    if !internals.is_empty() {
        render_reexport_module(&mut out, "internals", "super", &internals);
    }

    // dependencies nests by project name.
    let mut dep_names: Vec<&String> = entries
        .iter()
        .filter_map(|e| match &e.exposure {
            Exposure::Dependencies(name) => Some(name),
            _ => None,
        })
        .collect();
    dep_names.sort();
    dep_names.dedup();
    if !dep_names.is_empty() {
        out.push_str("pub mod dependencies {\n");
        for dep in dep_names {
            let group: Vec<&Entry> = entries
                .iter()
                .copied()
                .filter(|e| e.exposure == Exposure::Dependencies(dep.clone()))
                .collect();
            // From dependencies::<dep>, __pb is super::super::__pb.
            writeln!(out, "    pub mod {dep} {{").ok();
            for e in &group {
                writeln!(
                    out,
                    "        pub use super::super::{} as {};",
                    e.type_path(),
                    e.alias
                )
                .ok();
            }
            out.push_str("    }\n");
        }
        out.push_str("}\n\n");
    }

    // The descriptor set prost wrote. `Topic::DESCRIPTOR` points at it and a publisher announces it at
    // `@schema` (`reiny bag record` picks it up and embeds it in the MCAP).
    out.push_str(
        "/// The `FileDescriptorSet` of this crate's protos (for `reiny::Topic::DESCRIPTOR`).\n",
    );
    out.push_str("#[doc(hidden)]\n#[allow(dead_code, unreachable_pub)]\n");
    out.push_str(
        "pub const REINY_DESCRIPTORS: &[u8] = \
         include_bytes!(concat!(env!(\"OUT_DIR\"), \"/reiny_descriptors.bin\"));\n\n",
    );

    // The type → topic type segment. One impl per type (aliases may repeat, but the type is one, so dedup).
    // SCHEMA / DESCRIPTOR come from the descriptor (they default to None, so silently omit what is missing).
    let mut seen = Vec::new();
    out.push_str(
        "// type → topic (publish: reiny/<domain>/<id>/<TYPE>, \
         subscribe: reiny/<domain>/*/<TYPE>)\n",
    );
    for e in entries {
        let path = e.type_path();
        if seen.contains(&path) {
            continue;
        }
        seen.push(path.clone());
        let fq_name = e.fq_name();
        let schema = match fingerprints.get(&fq_name) {
            Some(fp) => format!(
                " const SCHEMA: Option<u64> = Some({fp:#018x}); \
                 const DESCRIPTOR: Option<::reiny::Descriptor> = \
                 Some(::reiny::Descriptor {{ message: {fq_name:?}, file_set: REINY_DESCRIPTORS }});"
            ),
            None => String::new(),
        };
        writeln!(
            out,
            "impl ::reiny::Topic for {path} {{ const TYPE: &'static str = {:?};{schema} }}",
            e.ident
        )
        .ok();
    }

    // [services]: request type → response type. The impl lands in the crate defining the request type.
    if !services.is_empty() {
        out.push_str("\n// [services] (the request type is the service's address: reiny/<domain>/<id>/<TYPE>)\n");
        for (request, response) in services {
            writeln!(
                out,
                "impl ::reiny::Service for {request} {{ type Response = {response}; }}"
            )
            .ok();
        }
    }

    // With a [config], generate the typed `config::Config` and the `cloudy.config()` extension.
    if let Some(table) = config {
        out.push('\n');
        out.push_str(&render_config(table)?);
    }

    Ok(out)
}

/// The `(request type path, response type path)` pairs to emit an `impl Service` for in this output.
///
/// A service whose request type is not in `plan_entries` (the types this crate generates) belongs to
/// another part and is skipped. A response owned elsewhere is referenced as `::<crate>::__pb::…` (the `extern_path` form).
#[cfg(feature = "compile")]
fn service_impls(resolution: &Resolution, plan_entries: &[&Entry]) -> Vec<(String, String)> {
    resolution
        .services
        .iter()
        .filter_map(|s| {
            let req = &resolution.entries[s.request];
            let resp = &resolution.entries[s.response];
            let mine = plan_entries
                .iter()
                .any(|e| e.type_path() == req.type_path());
            if !mine {
                return None;
            }
            let response = if resp.owner == req.owner {
                resp.type_path()
            } else {
                let ident = resolution
                    .schema_parts
                    .iter()
                    .find(|p| p.name == resp.owner)
                    .map(SchemaPart::crate_ident)?;
                format!("::{ident}::{}", resp.type_path())
            };
            Some((req.type_path(), response))
        })
        .collect()
}

/// The thin output for a schema-consuming launch. No proto is recompiled; the schema crates'
/// `internals` is simply shown as `crate::internals` (the `Topic`/`Message` impls exist exactly once,
/// in the schema crate, and coherence makes them global).
#[cfg(feature = "compile")]
fn render_consumer(crate_idents: &[String]) -> String {
    let mut out = String::new();
    out.push_str("// @generated by reiny-build — schema consumer (no proto recompiled).\n");
    // Re-export the schema crates' public types as internals. The `impl Topic` / `impl Message` tied to
    // each type are already defined in the schema crate, so showing the types is all that is needed.
    // Across a multi-crate split it is the union of every part (aliases are [internals] keys: no duplicates).
    out.push_str("pub mod internals {\n");
    for ident in crate_idents {
        writeln!(out, "    pub use ::{ident}::internals::*;").ok();
    }
    out.push_str("}\n");
    out
}

/// From the `[config]` TOML table, generate the typed `config::Config` (with defaults) and an extension
/// trait that grows `config()` on `::reiny::Cloudy`. `#[reiny::main]`'s glob re-export brings the trait
/// into scope, so user code can write `cloudy.config()`.
#[cfg(feature = "compile")]
fn render_config(table: &toml::Table) -> Result<String> {
    // TOML value → (Rust type, default literal, getter, the expression assigning `v` to the field).
    struct Field {
        name: String,
        rust_ty: &'static str,
        default_lit: String,
        getter: &'static str,
        assign: String,
    }

    let mut fields = Vec::new();
    for (key, val) in table {
        let f = match val {
            toml::Value::String(s) => Field {
                name: key.clone(),
                rust_ty: "String",
                default_lit: format!("{s:?}.to_string()"),
                getter: "as_str",
                assign: "v.to_string()".to_string(),
            },
            toml::Value::Integer(i) => Field {
                name: key.clone(),
                rust_ty: "u64",
                default_lit: format!("{i}u64"),
                getter: "as_integer",
                assign: "v as u64".to_string(),
            },
            toml::Value::Float(fl) => Field {
                name: key.clone(),
                rust_ty: "f64",
                default_lit: format!("{fl}f64"),
                getter: "as_float",
                assign: "v".to_string(),
            },
            toml::Value::Boolean(b) => Field {
                name: key.clone(),
                rust_ty: "bool",
                default_lit: format!("{b}"),
                getter: "as_bool",
                assign: "v".to_string(),
            },
            other => bail!(
                "[config].{key}: unsupported value type {} (only string/int/float/bool)",
                other.type_str()
            ),
        };
        fields.push(f);
    }

    let mut out = String::new();
    out.push_str("// The typed configuration generated from [config].\n");
    out.push_str("pub mod config {\n");
    out.push_str("    #[derive(Clone, Debug)]\n    pub struct Config {\n");
    for f in &fields {
        writeln!(out, "        pub {}: {},", f.name, f.rust_ty).ok();
    }
    out.push_str("    }\n");
    out.push_str(
        "    impl Default for Config {\n        fn default() -> Self {\n            Self {\n",
    );
    for f in &fields {
        writeln!(out, "                {}: {},", f.name, f.default_lit).ok();
    }
    out.push_str("            }\n        }\n    }\n");
    out.push_str("    impl Config {\n        #[doc(hidden)]\n        pub fn __from_table(table: &::reiny::__toml::Table) -> Self {\n            let mut cfg = Self::default();\n");
    for f in &fields {
        writeln!(
            out,
            "            if let Some(v) = table.get({:?}).and_then(|x| x.{}()) {{ cfg.{} = {}; }}",
            f.name, f.getter, f.name, f.assign
        )
        .ok();
    }
    out.push_str("            cfg\n        }\n    }\n}\n\n");

    // The `cloudy.config()` extension. The glob re-export brings it into scope.
    out.push_str(
        "#[doc(hidden)]\npub trait __CloudyConfigExt { fn config(&self) -> config::Config; }\n",
    );
    out.push_str("impl __CloudyConfigExt for ::reiny::Cloudy {\n");
    out.push_str("    fn config(&self) -> config::Config {\n");
    out.push_str("        match self.config_table() {\n");
    out.push_str("            Some(t) => config::Config::__from_table(t),\n");
    out.push_str("            None => config::Config::default(),\n");
    out.push_str("        }\n    }\n}\n");

    Ok(out)
}

/// Write one `pub mod <name> { pub use <prefix>::__pb::...::T as Alias; ... }`.
#[cfg(feature = "compile")]
fn render_reexport_module(out: &mut String, name: &str, prefix: &str, entries: &[&Entry]) {
    writeln!(out, "pub mod {name} {{").ok();
    for e in entries {
        writeln!(
            out,
            "    pub use {prefix}::{} as {};",
            e.type_path(),
            e.alias
        )
        .ok();
    }
    out.push_str("}\n\n");
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Rust's reserved words (unusable as a module or re-export name in the generated code). Raw identifiers
/// are deliberately not used, so a clash is an error.
const RUST_KEYWORDS: &[&str] = &[
    "as", "break", "const", "continue", "crate", "dyn", "else", "enum", "extern", "false", "fn",
    "for", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref",
    "return", "self", "Self", "static", "struct", "super", "trait", "true", "type", "unsafe",
    "use", "where", "while", "async", "await", "abstract", "become", "box", "do", "final", "macro",
    "override", "priv", "typeof", "unsized", "virtual", "yield", "try", "union",
];

/// Whether `name` is a Rust identifier (ASCII, a letter or `_` first, then alphanumerics or `_`, not a keyword).
fn is_valid_rust_ident(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    if name == "_" {
        return false;
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return false;
    }
    !RUST_KEYWORDS.contains(&name)
}

/// Validate the names that become identifiers verbatim in the generated code (dep keys, aliases, schema
/// crates) and stop the build with an error that says **where to fix it**. Without this, a bad name turns
/// into an rustc syntax error far downstream (`expected ; or {, found -`) with no visible cause.
fn ensure_rust_ident(name: &str, what: &str, section: &str) -> Result<()> {
    if is_valid_rust_ident(name) {
        return Ok(());
    }
    let suggestion = name.replace('-', "_");
    let hint = if suggestion != name && is_valid_rust_ident(&suggestion) {
        format!(" — ハイフン等は使えません。`{suggestion}` のような識別子にしてください")
    } else {
        " — 先頭は英字か `_`、以降は英数字か `_` のみ、予約語は不可".to_string()
    };
    bail!("{section} の {what} `{name}` は Rust 識別子として無効です{hint}");
}

/// Whether two different types were assigned the same topic segment (type name). Since type = topic, two
/// different types sharing a segment collide on the wire. Reject that early.
fn validate_no_topic_collision(entries: &[Entry], manifest_path: &Path) -> Result<()> {
    // ident (= topic segment) → the first type path seen.
    let mut by_segment: BTreeMap<String, String> = BTreeMap::new();
    for e in entries {
        let path = e.type_path();
        if let Some(prev) = by_segment.get(&e.ident) {
            // Another alias for the same type is not a collision. A different type would cross the wiring.
            if *prev != path {
                bail!(
                    "トピックセグメント `{}` が異なる型に重複しています(`{}` と `{}`)。\
                     型 = トピックなので衝突します({})",
                    e.ident,
                    prev,
                    path,
                    manifest_path.display()
                );
            }
        } else {
            by_segment.insert(e.ident.clone(), path);
        }
    }
    Ok(())
}

/// Look the `[services]` aliases up in the catalog and validate them.
///
/// - An alias is a `[publications]` / `[internals]` key, or a per-project dependency type `<dep>::<Alias>`.
/// - The same request type cannot serve two services (Rust has one associated type; this names the
///   section and stops before rustc's coherence error does).
/// - In a multi-crate `[schema]`, the response's owning part must be the request's own part or in its
///   `depends` (the impl can only be written in the crate that defines the request type).
fn resolve_services(
    manifest: &Manifest,
    entries: &[Entry],
    parts: &[SchemaPart],
    manifest_path: &Path,
) -> Result<Vec<ServiceEntry>> {
    let mut services = Vec::new();
    // request type path → service name (for duplicate detection).
    let mut by_request: BTreeMap<String, String> = BTreeMap::new();
    for (name, def) in &manifest.services {
        let lookup = |what: &str, alias: &str| -> Result<usize> {
            find_alias(entries, alias).with_context(|| {
                format!(
                    "[services] `{name}` の {what} `{alias}` はカタログにありません\
                     ([publications] / [internals] のキー、または依存型なら `<dep>::<Alias>`)({})",
                    manifest_path.display()
                )
            })
        };
        let request = lookup("request", &def.request)?;
        let response = lookup("response", &def.response)?;
        let req = &entries[request];
        if let Some(prev) = by_request.insert(req.type_path(), name.clone()) {
            bail!(
                "[services] `{prev}` と `{name}` が同じ request 型 `{}` を使っています。\
                 request 型 1 つにつき response 型は 1 つです({})",
                def.request,
                manifest_path.display()
            );
        }
        let resp = &entries[response];
        if req.owner != resp.owner
            && let Some(req_owner) = &req.owner
        {
            let part = parts.iter().find(|p| p.name.as_deref() == Some(req_owner));
            let reachable = part.is_some_and(|p| {
                resp.owner
                    .as_deref()
                    .is_some_and(|o| p.depends.iter().any(|d| d == o))
            });
            if !reachable {
                bail!(
                    "[services] `{name}` の response `{}` は [schema.{}] が所有していますが、\
                     request `{}` を所有する [schema.{req_owner}] の depends に入っていません\
                     (impl は request 型を定義するクレートに出ます)({})",
                    def.response,
                    resp.owner.as_deref().unwrap_or("?"),
                    def.request,
                    manifest_path.display()
                );
            }
        }
        services.push(ServiceEntry {
            name: name.clone(),
            request,
            response,
        });
    }
    Ok(services)
}

/// `[services]` alias → index into `entries`. `<dep>::<Alias>` is a per-project dependency type.
fn find_alias(entries: &[Entry], alias: &str) -> Option<usize> {
    if let Some((dep, a)) = alias.split_once("::") {
        entries.iter().position(|e| {
            matches!(&e.exposure, Exposure::Dependencies(d) if d == dep) && e.alias == a
        })
    } else {
        entries.iter().position(|e| {
            matches!(e.exposure, Exposure::Publications | Exposure::Internals) && e.alias == alias
        })
    }
}

// ---------------------------------------------------------------------------
// Diagnostics (REINY_VERBOSE=1 prints the resolution to cargo:warning)
// ---------------------------------------------------------------------------

/// When `REINY_VERBOSE` is set, print the resolved mode, the type → topic table and the protos to be
/// compiled through `cargo:warning=`. For checking ownership and transitive imports (silent by default).
#[cfg(feature = "compile")]
fn report_verbose(res: &Resolution) {
    println!("cargo:rerun-if-env-changed=REINY_VERBOSE");
    if env::var_os("REINY_VERBOSE").is_none() {
        return;
    }
    let warn = |s: &str| println!("cargo:warning=reiny: {s}");
    warn(&format!(
        "mode = {} ({})",
        res.mode.label(),
        res.manifest_path.display()
    ));
    if let Mode::SchemaConsumer { crate_idents } = &res.mode {
        warn(&format!(
            "consumes schema crate(s) `{}` (no proto recompiled; {} type(s) re-exported)",
            crate_idents.join("`, `"),
            res.entries.len()
        ));
    }
    for part in res.schema_crates() {
        let label = part.part.as_deref().unwrap_or("(single)");
        let deps = if part.depends.is_empty() {
            String::new()
        } else {
            format!(" depends on {}", part.depends.join(", "))
        };
        warn(&format!("schema [{label}] = {}{deps}", part.crate_name));
    }
    for t in res.types() {
        warn(&format!(
            "type {} = {} -> topic reiny/<domain>/<id>/{} [{}]{}",
            t.alias,
            t.message,
            t.topic_segment,
            t.module,
            t.owner
                .as_deref()
                .map_or_else(String::new, |o| format!(" owned by [schema.{o}]"))
        ));
    }
    for s in res.services() {
        warn(&format!(
            "service {} = {} ({}) -> {} ({})",
            s.name, s.request, s.request_message, s.response, s.response_message
        ));
    }
    // The deduplicated protos to compile (prost pulls the transitive imports separately).
    let mut protos: Vec<&Path> = res.entries.iter().map(|e| e.proto.as_path()).collect();
    protos.sort_unstable();
    protos.dedup();
    if matches!(res.mode, Mode::SchemaConsumer { .. }) {
        warn("compiles 0 proto (shared via schema crate)");
    } else {
        for p in protos {
            warn(&format!("compiles {}", p.display()));
        }
    }
}

// ---------------------------------------------------------------------------
// Odds and ends
// ---------------------------------------------------------------------------

fn resolve_relative(base: &Path, p: &Path) -> PathBuf {
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        base.join(p)
    }
}

fn rerun_if_changed(path: &Path) {
    // Only emit cargo directives from a build.rs (where OUT_DIR exists). For CLI introspection such as
    // `reiny check`, do nothing so stdout stays clean.
    if env::var_os("OUT_DIR").is_some() {
        println!("cargo:rerun-if-changed={}", path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_message_handles_package_and_bare() {
        let (pkg, ty) = split_message("ping.Ping").unwrap();
        assert_eq!(pkg, vec!["ping".to_string()]);
        assert_eq!(ty, "Ping");

        let (pkg, ty) = split_message("a.b.Msg").unwrap();
        assert_eq!(pkg, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(ty, "Msg");

        let (pkg, ty) = split_message("Bare").unwrap();
        assert!(pkg.is_empty());
        assert_eq!(ty, "Bare");
    }

    #[test]
    fn type_path_joins_pb_package_ident() {
        let e = Entry {
            alias: "Ping".into(),
            package: vec!["ping".into()],
            ident: "Ping".into(),
            exposure: Exposure::Publications,
            proto: PathBuf::from("/x/ping.proto"),
            owner: None,
        };
        assert_eq!(e.type_path(), "__pb::ping::Ping");
    }

    #[test]
    fn per_project_manifest_parses() {
        let m: Manifest = toml::from_str(
            r#"
            [project]
            name = "ping"
            version = "0.1.0"
            [publications]
            Ping = { proto = "proto/ping.proto", message = "ping.Ping" }
            [dependencies]
            pong = { version = "0.1", path = "../pong" }
        "#,
        )
        .unwrap();
        assert_eq!(m.project.unwrap().name, "ping");
        assert!(m.publications.contains_key("Ping"));
        assert_eq!(m.dependencies["pong"].path, PathBuf::from("../pong"));
    }

    #[test]
    fn workspace_manifest_parses() {
        let m: Manifest = toml::from_str(
            r#"
            [workspace]
            version = "0.1.0"
            [internals]
            Ping = { proto = "proto/ping.proto", message = "ping.Ping" }
            Pong = { proto = "proto/pong.proto", message = "pong.Pong" }
            [projects.ping]
            publications = ["Ping"]
            dependencies = ["Pong"]
        "#,
        )
        .unwrap();
        assert!(m.project.is_none());
        assert_eq!(m.internals.len(), 2);
        assert_eq!(m.projects["ping"].publications, vec!["Ping".to_string()]);

        // [projects.*] shows up verbatim as the introspection view (reiny run's flow diagram).
        let infos = project_infos(&m);
        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].name, "ping");
        assert_eq!(infos[0].publications, vec!["Ping".to_string()]);
        assert_eq!(infos[0].dependencies, vec!["Pong".to_string()]);
    }

    #[test]
    fn schema_manifest_parses() {
        let m: Manifest = toml::from_str(
            r#"
            [internals]
            Ping = { proto = "proto/ping.proto", message = "ping.Ping" }
            [schema]
            crate = "myapp-schema"
            [projects.ping]
            publications = ["Ping"]
        "#,
        )
        .unwrap();
        match m.schema.unwrap() {
            SchemaDecl::Single(s) => assert_eq!(s.crate_name, "myapp-schema"),
            SchemaDecl::Multi(_) => panic!("did not parse as the single shape"),
        }
    }

    #[test]
    fn multi_schema_manifest_parses() {
        let m: Manifest = toml::from_str(
            r#"
            [internals]
            Pose = { proto = "proto/geometry/geometry.proto", message = "hs.Pose" }
            [schema.geometry]
            crate = "myapp-proto-geometry"
            protos = ["proto/geometry/geometry.proto"]
            [schema.state]
            crate = "myapp-proto-state"
            protos = ["proto/state/state.proto"]
            depends = ["geometry"]
        "#,
        )
        .unwrap();
        match m.schema.unwrap() {
            SchemaDecl::Multi(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts["state"].crate_name, "myapp-proto-state");
                assert_eq!(parts["state"].depends, vec!["geometry".to_string()]);
                assert_eq!(parts["geometry"].protos.len(), 1);
            }
            SchemaDecl::Single(_) => panic!("did not parse as the multi-crate shape"),
        }
    }

    /// A part for tests (protos never touch a real file here — only the depends validation is exercised).
    fn part(name: &str, depends: &[&str]) -> SchemaPart {
        SchemaPart {
            name: Some(name.to_string()),
            crate_name: format!("p-{name}"),
            protos: Some(Vec::new()),
            depends: depends.iter().map(|d| (*d).to_string()).collect(),
        }
    }

    #[test]
    fn depends_must_be_transitively_closed() {
        // a ← b ← c with c not declaring a: cargo hands DEP_* only to direct dependents, so this cannot
        // be allowed through. It has to be pointed out by name.
        let parts = vec![part("a", &[]), part("b", &["a"]), part("c", &["b"])];
        let err = validate_depends(&parts).unwrap_err().to_string();
        assert!(err.contains("`a`"), "got: {err}");

        // Closed, so it passes.
        let parts = vec![part("a", &[]), part("b", &["a"]), part("c", &["b", "a"])];
        assert!(validate_depends(&parts).is_ok());
    }

    #[test]
    fn depends_rejects_unknown_and_self_and_cycles() {
        let err = validate_depends(&[part("a", &["nope"])])
            .unwrap_err()
            .to_string();
        assert!(err.contains("nope"), "got: {err}");

        let err = validate_depends(&[part("a", &["a"])])
            .unwrap_err()
            .to_string();
        assert!(err.contains("自分自身"), "got: {err}");

        let parts = vec![part("a", &["b"]), part("b", &["a"])];
        assert!(validate_depends(&parts).is_err());
    }

    #[test]
    fn owners_come_from_proto_paths() {
        let parts = vec![
            SchemaPart {
                name: Some("geometry".into()),
                crate_name: "p-geometry".into(),
                protos: Some(vec![PathBuf::from("/x/geometry.proto")]),
                depends: Vec::new(),
            },
            SchemaPart {
                name: Some("state".into()),
                crate_name: "p-state".into(),
                protos: Some(vec![PathBuf::from("/x/state.proto")]),
                depends: vec!["geometry".into()],
            },
        ];
        let mut entries = vec![
            Entry {
                alias: "Pose".into(),
                package: vec!["hs".into()],
                ident: "Pose".into(),
                exposure: Exposure::Internals,
                proto: PathBuf::from("/x/geometry.proto"),
                owner: None,
            },
            Entry {
                alias: "RobotState".into(),
                package: vec!["hs".into()],
                ident: "RobotState".into(),
                exposure: Exposure::Internals,
                proto: PathBuf::from("/x/state.proto"),
                owner: None,
            },
        ];
        assign_owners(&mut entries, &parts, Path::new("/x/Reiny.toml")).unwrap();
        assert_eq!(entries[0].owner.as_deref(), Some("geometry"));
        assert_eq!(entries[1].owner.as_deref(), Some("state"));

        // A proto belonging to no part is rejected (silently dropping it from codegen would be worse).
        entries[1].proto = PathBuf::from("/x/stray.proto");
        let err = assign_owners(&mut entries, &parts, Path::new("/x/Reiny.toml"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("stray.proto"), "got: {err}");
    }

    #[test]
    #[cfg(feature = "compile")]
    fn envify_matches_cargo_dep_env_naming() {
        assert_eq!(envify("myapp-proto-geometry"), "MYAPP_PROTO_GEOMETRY");
        // The hyphen and underscore spellings mangle to the same env name, so links may use either.
        assert_eq!(envify("myapp_proto_geometry"), "MYAPP_PROTO_GEOMETRY");
    }

    #[test]
    #[cfg(feature = "compile")]
    fn fingerprint_tracks_field_shape_not_order() {
        use prost_types::{DescriptorProto, FieldDescriptorProto};

        let field = |number: i32, name: &str, ty: i32| FieldDescriptorProto {
            name: Some(name.to_string()),
            number: Some(number),
            r#type: Some(ty),
            ..Default::default()
        };
        let msg = |fields: Vec<FieldDescriptorProto>| DescriptorProto {
            name: Some("Probe".to_string()),
            field: fields,
            ..Default::default()
        };

        let a = fingerprint("hs.Probe", &msg(vec![field(1, "x", 1), field(2, "y", 1)]));
        // Only the declaration order differs, so the fingerprint is the same (sorted by number first).
        let b = fingerprint("hs.Probe", &msg(vec![field(2, "y", 1), field(1, "x", 1)]));
        assert_eq!(a, b);

        // Changing a type changes it.
        let c = fingerprint("hs.Probe", &msg(vec![field(1, "x", 5), field(2, "y", 1)]));
        assert_ne!(a, c);
        // The same shape under a different type name is a different thing (the whole point: same-name clashes).
        let d = fingerprint(
            "other.Probe",
            &msg(vec![field(1, "x", 1), field(2, "y", 1)]),
        );
        assert_ne!(a, d);
    }

    #[test]
    #[cfg(feature = "compile")]
    fn common_ancestor_of_sibling_protos() {
        let paths = vec![
            PathBuf::from("/x/proto/geometry/a.proto"),
            PathBuf::from("/x/proto/geometry/b.proto"),
        ];
        assert_eq!(
            common_ancestor(&paths),
            Some(PathBuf::from("/x/proto/geometry"))
        );
        let paths = vec![
            PathBuf::from("/x/proto/a/one.proto"),
            PathBuf::from("/x/proto/b/two.proto"),
        ];
        assert_eq!(common_ancestor(&paths), Some(PathBuf::from("/x/proto")));
    }

    #[test]
    fn valid_rust_idents() {
        for ok in ["Ping", "control_app", "_x", "a1", "Schema9"] {
            assert!(is_valid_rust_ident(ok), "{ok} should be valid");
        }
        for bad in [
            "humanoid-system-control-app",
            "1ping",
            "",
            "_",
            "crate",
            "self",
            "a.b",
            "ä",
        ] {
            assert!(!is_valid_rust_ident(bad), "{bad} should be invalid");
        }
    }

    #[test]
    fn ensure_ident_suggests_underscore_for_hyphen() {
        let err = ensure_rust_ident("control-app", "dependency key", "[dependencies]")
            .unwrap_err()
            .to_string();
        assert!(err.contains("control_app"), "got: {err}");
    }

    #[test]
    fn topic_collision_is_rejected() {
        // The same ident in a different package yields the same topic segment → a collision.
        let entries = vec![
            Entry {
                alias: "A".into(),
                package: vec!["a".into()],
                ident: "Ping".into(),
                exposure: Exposure::Internals,
                proto: PathBuf::from("/x/a.proto"),
                owner: None,
            },
            Entry {
                alias: "B".into(),
                package: vec!["b".into()],
                ident: "Ping".into(),
                exposure: Exposure::Internals,
                proto: PathBuf::from("/x/b.proto"),
                owner: None,
            },
        ];
        let err = validate_no_topic_collision(&entries, Path::new("/x/Reiny.toml")).unwrap_err();
        assert!(err.to_string().contains("Ping"));
    }

    fn svc_entries() -> Vec<Entry> {
        let mk = |alias: &str, ident: &str, exposure: Exposure| Entry {
            alias: alias.into(),
            package: vec!["calc".into()],
            ident: ident.into(),
            exposure,
            proto: PathBuf::from("/x/calc.proto"),
            owner: None,
        };
        vec![
            mk("Add", "Add", Exposure::Internals),
            mk("Sum", "Sum", Exposure::Internals),
            mk("Echo", "Echo", Exposure::Dependencies("dep".into())),
        ]
    }

    fn svc_manifest(services: &str) -> Manifest {
        toml::from_str(&format!(
            "[internals]\nAdd = {{ proto = \"calc.proto\", message = \"calc.Add\" }}\n[services]\n{services}"
        ))
        .unwrap()
    }

    #[test]
    fn services_resolve_aliases_including_dependency_form() {
        let m = svc_manifest(
            "Adder = { request = \"Add\", response = \"Sum\" }\nEcho = { request = \"dep::Echo\", response = \"dep::Echo\" }",
        );
        let svcs = resolve_services(&m, &svc_entries(), &[], Path::new("/x/Reiny.toml")).unwrap();
        assert_eq!(svcs.len(), 2);
        assert_eq!(
            (svcs[0].name.as_str(), svcs[0].request, svcs[0].response),
            ("Adder", 0, 1)
        );
        assert_eq!(
            (svcs[1].name.as_str(), svcs[1].request, svcs[1].response),
            ("Echo", 2, 2)
        );
    }

    #[test]
    fn services_reject_unknown_alias_and_duplicate_request() {
        let m = svc_manifest("Adder = { request = \"Add\", response = \"Nope\" }");
        let err =
            resolve_services(&m, &svc_entries(), &[], Path::new("/x/Reiny.toml")).unwrap_err();
        assert!(err.to_string().contains("Nope"), "{err}");

        let m = svc_manifest(
            "A = { request = \"Add\", response = \"Sum\" }\nB = { request = \"Add\", response = \"Add\" }",
        );
        let err =
            resolve_services(&m, &svc_entries(), &[], Path::new("/x/Reiny.toml")).unwrap_err();
        assert!(err.to_string().contains("同じ request 型"), "{err}");
    }

    #[test]
    fn topic_collision_allows_same_type_aliased_twice() {
        // Listing the same type (same package/ident) under two aliases is not a collision.
        let entries = vec![
            Entry {
                alias: "A".into(),
                package: vec!["a".into()],
                ident: "Ping".into(),
                exposure: Exposure::Publications,
                proto: PathBuf::from("/x/a.proto"),
                owner: None,
            },
            Entry {
                alias: "AliasOfA".into(),
                package: vec!["a".into()],
                ident: "Ping".into(),
                exposure: Exposure::Dependencies("dep".into()),
                proto: PathBuf::from("/x/a.proto"),
                owner: None,
            },
        ];
        assert!(validate_no_topic_collision(&entries, Path::new("/x/Reiny.toml")).is_ok());
    }

    #[test]
    #[cfg(feature = "compile")]
    fn consumer_render_reexports_schema_crate() {
        let out = render_consumer(&["myapp_schema".to_string()]);
        assert!(out.contains("pub use ::myapp_schema::internals::*"));
    }

    /// A two-file `FileDescriptorSet` (msg.proto imports geometry.proto), assembled by hand.
    /// It pokes at `descriptor_subset` / `message_fingerprint` without running protoc.
    #[cfg(feature = "descriptors")]
    fn two_file_set() -> Vec<u8> {
        use prost_types::field_descriptor_proto::{Label, Type};
        use prost_types::{
            DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        };

        let scalar = |name: &str, number: i32, ty: Type| FieldDescriptorProto {
            name: Some(name.to_string()),
            number: Some(number),
            label: Some(Label::Optional as i32),
            r#type: Some(ty as i32),
            ..Default::default()
        };
        let geometry = FileDescriptorProto {
            name: Some("geometry.proto".to_string()),
            package: Some("geo".to_string()),
            message_type: vec![DescriptorProto {
                name: Some("Point".to_string()),
                field: vec![scalar("x", 1, Type::Double), scalar("y", 2, Type::Double)],
                ..Default::default()
            }],
            ..Default::default()
        };
        let msg = FileDescriptorProto {
            name: Some("msg.proto".to_string()),
            package: Some("msg".to_string()),
            dependency: vec!["geometry.proto".to_string()],
            message_type: vec![DescriptorProto {
                name: Some("Ping".to_string()),
                field: vec![
                    scalar("seq", 1, Type::Uint32),
                    FieldDescriptorProto {
                        name: Some("at".to_string()),
                        number: Some(4),
                        label: Some(Label::Optional as i32),
                        r#type: Some(Type::Message as i32),
                        type_name: Some(".geo.Point".to_string()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };
        prost::Message::encode_to_vec(&FileDescriptorSet {
            file: vec![geometry, msg],
        })
    }

    #[test]
    #[cfg(feature = "descriptors")]
    fn descriptor_subset_prunes_to_the_file_closure() {
        let set = two_file_set();

        // The file containing Ping, plus its transitive import (geometry).
        let for_ping = descriptor_subset(&set, "msg.Ping").unwrap().unwrap();
        let decoded: prost_types::FileDescriptorSet = prost::Message::decode(&*for_ping).unwrap();
        let mut names: Vec<&str> = decoded
            .file
            .iter()
            .map(prost_types::FileDescriptorProto::name)
            .collect();
        names.sort_unstable();
        assert_eq!(names, ["geometry.proto", "msg.proto"]);

        // Point is on the non-importing side, so it prunes down to its own file alone.
        let for_point = descriptor_subset(&set, "geo.Point").unwrap().unwrap();
        let decoded: prost_types::FileDescriptorSet = prost::Message::decode(&*for_point).unwrap();
        let names: Vec<&str> = decoded
            .file
            .iter()
            .map(prost_types::FileDescriptorProto::name)
            .collect();
        assert_eq!(names, ["geometry.proto"]);

        // A type that is not there is None.
        assert!(descriptor_subset(&set, "msg.Nope").unwrap().is_none());
    }

    #[test]
    #[cfg(feature = "descriptors")]
    fn message_fingerprint_matches_direct_fingerprint() {
        let set = two_file_set();
        let decoded: prost_types::FileDescriptorSet = prost::Message::decode(&*set).unwrap();
        let (_, point) = find_message(&decoded, "geo.Point").unwrap();
        assert_eq!(
            message_fingerprint(&set, "geo.Point").unwrap(),
            Some(fingerprint("geo.Point", point)),
        );
        assert_eq!(message_fingerprint(&set, "geo.Missing").unwrap(), None);
    }

    // -----------------------------------------------------------------------
    // Resolving a real Reiny.toml on disk (`describe`, the `reiny check` path)
    // -----------------------------------------------------------------------

    /// A throwaway directory tree for manifest fixtures. `describe` reads real files — it rejects a
    /// `proto` that is not there — so these tests need a filesystem. The directory removes itself.
    struct Fixture(PathBuf);

    impl Fixture {
        fn new(name: &str) -> Self {
            use std::sync::atomic::{AtomicU32, Ordering};
            static NEXT: AtomicU32 = AtomicU32::new(0);
            let dir = std::env::temp_dir().join(format!(
                "reiny-build-{name}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn root(&self) -> &Path {
            &self.0
        }

        fn path(&self, rel: &str) -> PathBuf {
            self.0.join(rel)
        }

        /// Write `contents` at `rel`, creating the parent directories.
        fn write(&self, rel: &str, contents: &str) {
            let path = self.path(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, contents).unwrap();
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// `Resolution` is not `Debug`, so `unwrap_err` is unavailable; this says the same thing, and
    /// renders the whole anyhow chain so a context line can be asserted on.
    fn describe_err(dir: &Path) -> String {
        match describe(dir) {
            Err(e) => format!("{e:#}"),
            Ok(_) => panic!("expected {} to fail resolution", dir.display()),
        }
    }

    /// A per-project layout end to end: the mode, and the type → topic table with the module each type
    /// lands in. A dependency's public types come along under `dependencies::<dep>` — resolved by
    /// reading *that project's* Reiny.toml, which is the only reason a dependency needs one.
    #[test]
    fn describe_resolves_a_per_project_layout() {
        let f = Fixture::new("per-project");
        f.write("pong/proto/pong.proto", "");
        f.write(
            "pong/Reiny.toml",
            r#"
            [project]
            name = "pong"
            version = "0.1.0"
            [publications]
            Pong = { proto = "proto/pong.proto", message = "pong.Pong" }
            "#,
        );
        f.write("ping/proto/ping.proto", "");
        f.write(
            "ping/Reiny.toml",
            r#"
            [project]
            name = "ping"
            version = "0.1.0"
            [publications]
            Ping = { proto = "proto/ping.proto", message = "ping.Ping" }
            [dependencies]
            pong = { version = "0.1", path = "../pong" }
            "#,
        );

        let res = describe(&f.path("ping")).unwrap();
        assert!(matches!(res.mode(), Mode::PerProject), "{:?}", res.mode());
        assert!(res.manifest_path().parent().unwrap().ends_with("ping"));
        assert!(!res.has_config());
        assert!(res.services().is_empty());
        assert!(res.projects().is_empty(), "per-project has no [projects.*]");

        let mut types: Vec<(String, String, String)> = res
            .types()
            .into_iter()
            .map(|t| (t.alias, t.topic_segment, t.module))
            .collect();
        types.sort();
        assert_eq!(
            types,
            vec![
                (
                    "Ping".to_string(),
                    "Ping".to_string(),
                    "publications".to_string()
                ),
                (
                    "Pong".to_string(),
                    "Pong".to_string(),
                    "dependencies::pong".to_string()
                ),
            ]
        );
        // The topic segment is the bare type name — the proto package is stripped.
        let ping = res.types().into_iter().find(|t| t.alias == "Ping").unwrap();
        assert_eq!(ping.message, "ping.Ping");
        assert_eq!(ping.topic_segment, "Ping");
        assert!(ping.proto.is_absolute());
    }

    /// A workspace layout end to end: everything in `[internals]` is resolved into `internals`,
    /// regardless of which project publishes it, and `[projects.*]` survives as the introspection view
    /// `reiny run` draws its flow diagram from.
    #[test]
    fn describe_resolves_a_workspace_layout() {
        let f = Fixture::new("workspace");
        f.write("proto/ping.proto", "");
        f.write("proto/pong.proto", "");
        f.write(
            "Reiny.toml",
            r#"
            [internals]
            Ping = { proto = "proto/ping.proto", message = "ping.Ping" }
            Pong = { proto = "proto/pong.proto", message = "pong.Pong" }
            [projects.talker]
            publications = ["Ping"]
            dependencies = ["Pong"]
            [projects.listener]
            publications = ["Pong"]
            dependencies = ["Ping"]
            "#,
        );

        let res = describe(f.root()).unwrap();
        assert!(matches!(res.mode(), Mode::Workspace), "{:?}", res.mode());
        assert!(res.schema_crates().is_empty());
        assert!(
            res.types().iter().all(|t| t.module == "internals"),
            "workspace types all go to internals"
        );

        let mut projects: Vec<&str> = res.projects().iter().map(|p| p.name.as_str()).collect();
        projects.sort_unstable();
        assert_eq!(projects, ["listener", "talker"]);
        let talker = res
            .projects()
            .iter()
            .find(|p| p.name == "talker")
            .expect("talker is declared");
        assert_eq!(talker.publications, ["Ping"]);
        assert_eq!(talker.dependencies, ["Pong"]);
    }

    /// The manifest search runs *upward* and the nearest one wins. That is what lets a launch inside a
    /// workspace carry its own Reiny.toml, and what lets a subdirectory (`src/`, where a build script
    /// runs) inherit the one above it.
    #[test]
    fn manifest_search_takes_the_nearest_one_upward() {
        let f = Fixture::new("upward");
        f.write("proto/shared.proto", "");
        f.write(
            "Reiny.toml",
            r#"
            [internals]
            Shared = { proto = "proto/shared.proto", message = "ws.Shared" }
            [projects.ping]
            publications = ["Shared"]
            "#,
        );
        f.write("ping/proto/ping.proto", "");
        f.write(
            "ping/Reiny.toml",
            r#"
            [project]
            name = "ping"
            version = "0.1.0"
            [publications]
            Ping = { proto = "proto/ping.proto", message = "ping.Ping" }
            "#,
        );
        std::fs::create_dir_all(f.path("ping/src")).unwrap();

        // From the launch directory: its own manifest, not the workspace's.
        let inner = describe(&f.path("ping")).unwrap();
        assert!(matches!(inner.mode(), Mode::PerProject));
        // From a subdirectory with no manifest: the nearest ancestor's, which is still the launch's.
        let nested = describe(&f.path("ping/src")).unwrap();
        assert_eq!(nested.manifest_path(), inner.manifest_path());
        // From the root: the workspace manifest.
        let outer = describe(f.root()).unwrap();
        assert!(matches!(outer.mode(), Mode::Workspace));
        assert_ne!(outer.manifest_path(), inner.manifest_path());
    }

    /// `describe` also runs validation, which is the whole point of `reiny check`: a layout mistake has
    /// to be named here rather than becoming a rustc error inside generated code much later.
    #[test]
    fn describe_reports_layout_mistakes() {
        // Neither [project] nor [internals]/[projects]: the message names both ways out.
        let f = Fixture::new("neither");
        f.write("Reiny.toml", "[workspace]\nversion = \"0.1.0\"\n");
        let err = describe_err(f.root());
        assert!(err.contains("[project]"), "{err}");
        assert!(err.contains("[internals]"), "{err}");

        // A publication naming a proto that is not on disk.
        let f = Fixture::new("missing-proto");
        f.write(
            "Reiny.toml",
            r#"
            [project]
            name = "ping"
            version = "0.1.0"
            [publications]
            Ping = { proto = "proto/ping.proto", message = "ping.Ping" }
            "#,
        );
        let err = describe_err(f.root());
        assert!(err.contains("proto file not found"), "{err}");

        // A [dependencies] key becomes `pub mod <key>` in the generated code, so a hyphen has to be
        // caught here, not as a syntax error inside reiny_generated.rs.
        let f = Fixture::new("bad-dep-key");
        f.write("dep/proto/d.proto", "");
        f.write(
            "dep/Reiny.toml",
            r#"
            [project]
            name = "dep"
            version = "0.1.0"
            [publications]
            D = { proto = "proto/d.proto", message = "d.D" }
            "#,
        );
        f.write("app/proto/a.proto", "");
        f.write(
            "app/Reiny.toml",
            r#"
            [project]
            name = "app"
            version = "0.1.0"
            [publications]
            A = { proto = "proto/a.proto", message = "a.A" }
            [dependencies]
            my-dep = { version = "0.1", path = "../dep" }
            "#,
        );
        let err = describe_err(&f.path("app"));
        assert!(err.contains("[dependencies]"), "{err}");
        assert!(err.contains("my-dep"), "{err}");
    }

    /// No Reiny.toml in the directory or any parent: the error names where the search started, because
    /// "which directory did you mean" is the only useful thing to say about it.
    #[test]
    fn missing_manifest_names_the_starting_directory() {
        let f = Fixture::new("no-manifest"); // deliberately empty
        let err = describe_err(f.root());
        assert!(err.contains("Reiny.toml"), "{err}");
        assert!(err.contains("no-manifest"), "{err}");
    }

    /// `[config]` and `[services]` reach the introspection view: `reiny check` prints both, and a
    /// service is reported by the aliases *and* the fully qualified message names it resolved to.
    #[test]
    fn describe_reports_config_and_services() {
        let f = Fixture::new("services");
        f.write("proto/calc.proto", "");
        f.write(
            "Reiny.toml",
            r#"
            [project]
            name = "calc"
            version = "0.1.0"
            [publications]
            Add = { proto = "proto/calc.proto", message = "calc.Add" }
            Sum = { proto = "proto/calc.proto", message = "calc.Sum" }
            [services]
            Adder = { request = "Add", response = "Sum" }
            [config]
            rate_hz = 10
            name = "calc"
            "#,
        );

        let res = describe(f.root()).unwrap();
        assert!(res.has_config());
        let services = res.services();
        assert_eq!(services.len(), 1);
        let s = &services[0];
        assert_eq!(s.name, "Adder");
        assert_eq!(
            (s.request.as_str(), s.request_message.as_str()),
            ("Add", "calc.Add")
        );
        assert_eq!(
            (s.response.as_str(), s.response_message.as_str()),
            ("Sum", "calc.Sum")
        );
    }
}
