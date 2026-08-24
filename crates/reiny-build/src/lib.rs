//! reiny の build 補助。各 grain の `build.rs` から [`compile`] を呼ぶ。
//!
//! `Reiny.toml` を読み、必要な proto を prost でコンパイルし、`$OUT_DIR/reiny_generated.rs` に
//!
//! - `publications` / `dependencies::<project>` / `internals` の各モジュール(生成型の再エクスポート)
//! - 各メッセージ型への `impl ::reiny::Topic`(型 → トピックの埋め込み)
//!
//! を書き出す。このファイルは `#[reiny::main]` が crate ルートへ取り込むので、利用側は
//! `use crate::publications::Ping;` のように型を参照できる。
//!
//! 2 つの配置を扱う:
//! - **per-project**(`[project]` を持つ Reiny.toml): 自分の `[publications]` と、`[dependencies]`
//!   先プロジェクトの公開型を解決する。型 → トピックは「その型を公開するプロジェクト」。
//! - **workspace 共有**(`[internals]` / `[projects.*]` を持つ Reiny.toml): 共有カタログ
//!   `[internals]` を全部コンパイルし `internals::*` として公開する。

// build スクリプトから呼ばれる補助 crate なので、設定不備は context 付き panic で
// 即座に build を止めるのが正しい。unwrap/expect/panic 系の制限は本 crate では外す。
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeMap;
use std::env;
// writeln! を使う生成コードは compile 機能側だけ。
#[cfg(feature = "compile")]
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

// ---------------------------------------------------------------------------
// Reiny.toml のスキーマ
// ---------------------------------------------------------------------------

/// Reiny.toml 全体(per-project / workspace どちらの形も受ける)。
#[derive(Debug, Deserialize)]
struct Manifest {
    /// per-project の身元。これがあれば per-project モード。
    project: Option<Project>,
    /// per-project の公開型。
    #[serde(default)]
    publications: BTreeMap<String, TypeDef>,
    /// per-project の依存プロジェクト。
    #[serde(default)]
    dependencies: BTreeMap<String, Dependency>,
    /// workspace 共有カタログ。
    #[serde(default)]
    internals: BTreeMap<String, TypeDef>,
    /// workspace 各プロジェクトの公開/購読宣言。
    #[serde(default)]
    projects: BTreeMap<String, ProjectDecl>,
    /// workspace 共有スキーマクレート(あれば、型を 1 度だけ生成して共有する)。
    schema: Option<SchemaDecl>,
    /// per-project の型付き設定スキーマ + 既定値(`cloudy.config()` で読む)。
    config: Option<toml::Table>,
}

/// `[schema]` の 2 形。`crate` はキーワードなので rename で受ける。
///
/// - **単一形**(0.2)`[schema] crate = "myapp-schema"` —— `[internals]` 全部を 1 クレートが
///   prost コンパイル + `impl Topic` し、grain は Cargo 依存として再エクスポートする。
/// - **多クレート形**(0.3)`[schema.<name>] crate/protos/depends` —— スキーマを独立に公開可能な
///   複数クレートへ割る。所属は proto パスから決まり、リーフ型は `extern_path` で 1 度しか
///   生成されない。
///
/// untagged なので単一形を先に試す(`crate` キーが文字列なら単一形、そうでなければ区画表)。
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

/// `[schema.<name>]` の 1 区画。
#[derive(Debug, Deserialize)]
struct SchemaPartDef {
    #[serde(rename = "crate")]
    crate_name: String,
    /// この区画が所有する proto(Reiny.toml のあるディレクトリ基準)。
    protos: Vec<String>,
    /// 依存する区画名。**推移的に閉じている**必要があり、同じ辺が Cargo の依存にも要る。
    #[serde(default)]
    depends: Vec<String>,
}

/// 正規化した 1 スキーマクレート。単一形は `name: None` / `protos: None`
/// (= `[internals]` 全部を持つ)として畳む。
#[derive(Debug, Clone)]
struct SchemaPart {
    /// `[schema.<name>]` の区画名。単一形は `None`。
    name: Option<String>,
    crate_name: String,
    /// 所有 proto の絶対パス。単一形は `None`。
    protos: Option<Vec<PathBuf>>,
    depends: Vec<String>,
}

impl SchemaPart {
    /// extern crate 名(`myapp-proto-geometry` → `myapp_proto_geometry`)。
    fn crate_ident(&self) -> String {
        self.crate_name.replace('-', "_")
    }
}

/// スキーマクレート 1 件の内省ビュー(`reiny check` 用)。
#[derive(Debug, Clone)]
pub struct SchemaCrateInfo {
    /// `[schema.<name>]` の区画名。単一 `[schema]` なら `None`。
    pub part: Option<String>,
    /// Cargo パッケージ名。
    pub crate_name: String,
    /// 依存する区画名。
    pub depends: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Project {
    /// プロジェクト名。実行時インスタンス id はランチャ/`--id` が決め、トピックは型から
    /// 決まるので、ここでは [project] モード判定と宣言の自己記述のためだけに保持する。
    name: String,
}

/// `Type = { proto = "...", message = "pkg.Type" }`。
#[derive(Debug, Deserialize)]
struct TypeDef {
    proto: String,
    message: String,
}

/// `dep = { version = "0.1", path = "../dep" }`。version は今は検証に使わない。
#[derive(Debug, Deserialize)]
struct Dependency {
    path: PathBuf,
}

/// `[projects.<name>]` の publications / dependencies(カタログのキー名を参照)。
/// 現状は宣言の存在確認のみ。将来 publish/subscribe の許可制に使う。
#[derive(Debug, Default, Deserialize)]
#[allow(dead_code)]
struct ProjectDecl {
    #[serde(default)]
    publications: Vec<String>,
    #[serde(default)]
    dependencies: Vec<String>,
}

// ---------------------------------------------------------------------------
// 中間表現
// ---------------------------------------------------------------------------

/// どの生成モジュールへ型を出すか。
#[derive(Debug, Clone, PartialEq, Eq)]
enum Exposure {
    Publications,
    Internals,
    Dependencies(String),
}

/// 解決済みの 1 メッセージ型。
#[derive(Debug, Clone)]
struct Entry {
    /// 生成モジュールでの公開名(Reiny.toml のキー、例 `Ping`)。
    alias: String,
    /// proto package のセグメント列(例 `["ping"]`)。
    package: Vec<String>,
    /// Rust 型名(例 `Ping`)。これがトピックの型セグメント(`reiny/<id>/Ping`)になる。
    ident: String,
    /// どのモジュールへ出すか。
    exposure: Exposure,
    /// コンパイルすべき proto の絶対パス。
    proto: PathBuf,
    /// この型を所有するスキーマ区画名(多クレート形のみ)。proto パスから決まる。
    owner: Option<String>,
}

impl Entry {
    /// proto の完全メッセージ名(例 `hs.Vector3`)。descriptor 由来の指紋を引く鍵。
    #[cfg(feature = "compile")]
    fn fq_name(&self) -> String {
        let mut segs = self.package.clone();
        segs.push(self.ident.clone());
        segs.join(".")
    }

    /// `__reiny_generated` から見た型パス(例 `__pb::ping::Ping`)。
    fn type_path(&self) -> String {
        let mut segs = vec!["__pb".to_string()];
        segs.extend(self.package.iter().cloned());
        segs.push(self.ident.clone());
        segs.join("::")
    }
}

// ---------------------------------------------------------------------------
// 解決モードと結果(CLI からの内省にも使う公開 API)
// ---------------------------------------------------------------------------

/// Reiny.toml をどの配置として解決したか。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mode {
    /// per-project(`[project]`)。自分の publications + 依存先の公開型を生成する。
    PerProject,
    /// workspace 共有(`[internals]`/`[projects]`、`[schema]` 無し)。各 grain が
    /// `[internals]` を自前で prost コンパイルする(従来どおり)。
    Workspace,
    /// workspace + `[schema]`。自分が **スキーマクレート本体**で、担当分を prost コンパイル +
    /// `impl Topic` する。grain はこれを Cargo 依存として共有する。
    Schema {
        /// 多クレート形(`[schema.<name>]`)での担当区画名。単一 `[schema]` なら `None`
        /// (= `[internals]` 全部を持つ)。
        part: Option<String>,
    },
    /// workspace + `[schema]`。自分はスキーマを **消費する grain**。proto は再コンパイルせず、
    /// スキーマクレート群の型を `internals` として再エクスポートするだけ。
    SchemaConsumer {
        /// 依存するスキーマクレートの extern ident(`myapp-schema` → `myapp_schema`)。
        /// 多クレート形では全区画が並ぶ(`internals` はその和集合)。
        crate_idents: Vec<String>,
    },
}

impl Mode {
    /// 人が読むラベル(`reiny check` 用)。
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

/// 解決済みのメッセージ型 1 件の内省ビュー(`reiny check` 用に [`Entry`] を公開化したもの)。
#[derive(Debug, Clone)]
pub struct TypeInfo {
    /// 生成モジュールでの公開名(Reiny.toml のキー)。
    pub alias: String,
    /// proto の完全メッセージ名(例 `ping.Ping`)。
    pub message: String,
    /// 型 → トピックの型セグメント(`reiny/<id>/<segment>`)。
    pub topic_segment: String,
    /// どの生成モジュールへ出るか(`publications` / `internals` / `dependencies::<dep>`)。
    pub module: String,
    /// コンパイル対象 proto の絶対パス。
    pub proto: PathBuf,
    /// 所有するスキーマ区画名(多クレート形のみ)。
    pub owner: Option<String>,
}

/// Reiny.toml を解決した結果。proto コンパイル前の純粋な情報なので、`compile` 機能(prost)無しでも
/// 得られる。`reiny check` はこれを表示し、[`compile`] はこれを使って生成物を書き出す。
pub struct Resolution {
    mode: Mode,
    entries: Vec<Entry>,
    config: Option<toml::Table>,
    manifest_path: PathBuf,
    /// 正規化した `[schema]` 区画群(無ければ空)。
    schema_parts: Vec<SchemaPart>,
}

impl Resolution {
    /// どの配置で解決したか。
    #[must_use]
    pub fn mode(&self) -> &Mode {
        &self.mode
    }

    /// 採用した Reiny.toml の絶対パス。
    #[must_use]
    pub fn manifest_path(&self) -> &Path {
        &self.manifest_path
    }

    /// `[config]` を持つか(per-project の型付き設定)。
    #[must_use]
    pub fn has_config(&self) -> bool {
        self.config.is_some()
    }

    /// `[schema]` が宣言するスキーマクレート群(無ければ空)。単一形は 1 件で `part` が `None`。
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

    /// 解決済みの型一覧(トピック・モジュール付き)。
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
// エントリポイント
// ---------------------------------------------------------------------------

/// prost-build そのもの。[`compile_with`] のクロージャが受け取る `Config` の型を
/// 利用側 `build.rs` が名指しできるよう再エクスポートする(版ズレ防止)。
#[cfg(feature = "compile")]
pub use prost_build;

/// `build.rs` から呼ぶ。Reiny.toml を読み、proto をコンパイルして生成物を `$OUT_DIR` に出す。
///
/// `compile` 機能(既定 on)が要る。`reiny check` のように prost を引きたくない内省用途では
/// [`resolve`] を直接使う。
#[cfg(feature = "compile")]
pub fn compile() -> Result<()> {
    compile_with(|_| {})
}

/// [`compile`] と同じだが、prost へ渡す直前の `prost_build::Config` を触れる。
///
/// reiny が prost のノブを塞がないための逃げ道。`type_attribute` で wire 型に serde を
/// derive する、`file_descriptor_set_path` を出して `prost-reflect` で動的デコードする、
/// `bytes()` / `btree_map` / `boxed` …… いずれも reiny 側に専用 API を足さずに済む。
///
/// ```ignore
/// // build.rs
/// reiny_build::compile_with(|c| {
///     c.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
/// })
/// .expect("reiny codegen");
/// ```
///
/// `out_dir` / `include_file` は reiny が生成物を組み立てるのに使うので、
/// クロージャで上書きしても reiny の生成物とは噛み合わなくなる(触らないこと)。
#[cfg(feature = "compile")]
pub fn compile_with(customize: impl FnOnce(&mut prost_build::Config)) -> Result<()> {
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").context("CARGO_MANIFEST_DIR not set")?);
    let out_dir = PathBuf::from(env::var("OUT_DIR").context("OUT_DIR not set")?);
    let pkg_name = env::var("CARGO_PKG_NAME").context("CARGO_PKG_NAME not set")?;

    let resolution = resolve_for(&manifest_dir, &pkg_name)?;
    report_verbose(&resolution);

    // スキーマ消費 grain は proto を再コンパイルせず、スキーマクレート群を再エクスポートするだけ。
    // それ以外は担当分の proto をコンパイルして完全な生成物を出す。
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
        )?
    };

    let generated_path = out_dir.join("reiny_generated.rs");
    std::fs::write(&generated_path, generated)
        .with_context(|| format!("writing {}", generated_path.display()))?;

    Ok(())
}

/// `reiny check` 向けの内省。特定パッケージの視点ではなく、Reiny.toml が表す **カタログ全体**を
/// 解決する(workspace では `[internals]` を全部、per-project では自分の publications + 依存)。
/// proto はコンパイルしないので `compile` 機能無しでも使える。検証(識別子・トピック衝突)は
/// 通すので、配置ミスはここで分かる。
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
        // カタログ視点: どの 1 パッケージにも束縛しない。[schema] があればその旨を示す。
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

    Ok(Resolution {
        mode,
        entries,
        config: manifest.config,
        manifest_path,
        schema_parts,
    })
}

/// Reiny.toml を探索 → モード判定 → 検証して [`Resolution`] を組む(proto はまだ触らない)。
/// build.rs(`compile`)から、パッケージ視点で呼ぶ。
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

    Ok(Resolution {
        mode,
        entries,
        config: manifest.config,
        manifest_path,
        schema_parts,
    })
}

// ---------------------------------------------------------------------------
// Reiny.toml の探索と解決
// ---------------------------------------------------------------------------

/// `start` から上方向へ `Reiny.toml` を探す(最も近いものを採用)。
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

/// per-project: 自分の publications + 各依存先プロジェクトの公開型。
fn resolve_per_project(manifest: &Manifest, root: &Path) -> Result<Vec<Entry>> {
    let _project = manifest.project.as_ref().expect("project present");
    let mut entries = Vec::new();

    // 自分の公開型。
    for (alias, td) in &manifest.publications {
        ensure_rust_ident(alias, "publication alias", "[publications]")?;
        entries.push(make_entry(alias, td, root, Exposure::Publications)?);
    }

    // 依存先プロジェクトの公開型(dependencies::<dep>::* として再エクスポート)。
    for (dep_name, dep) in &manifest.dependencies {
        // dep 名は生成コードで `pub mod <dep>` になるので Rust 識別子必須(ハイフン不可)。
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

/// workspace 共有: [internals] を全部 `internals::*` へ。トピックは型名から決まるので、
/// どのプロジェクトが公開するかには依らない。`[schema]` があれば、自分がスキーマクレート本体か
/// 消費 grain かでモードが分かれる。build.rs(`compile`)からパッケージ視点で呼ぶ。
#[cfg(feature = "compile")]
fn resolve_workspace(
    manifest: &Manifest,
    root: &Path,
    pkg_name: &str,
    manifest_path: &Path,
    parts: &[SchemaPart],
) -> Result<(Mode, Vec<Entry>)> {
    // [schema] の有無と、自分がスキーマクレート本体かでモードを決める。
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
        // 消費 grain は [projects.<pkg>] に居る必要がある。internals は全区画の和。
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

    // entries は全モードで [internals] を解決しておく(消費モードでは内省・診断にのみ使い、
    // proto はコンパイルしない)。所有区画は proto パスから決まる。
    let mut entries = internals_entries(manifest, root)?;
    assign_owners(&mut entries, parts, manifest_path)?;

    Ok((mode, entries))
}

/// `[internals]` を `Exposure::Internals` の Entry 群にする(所有区画はまだ空)。
fn internals_entries(manifest: &Manifest, root: &Path) -> Result<Vec<Entry>> {
    let mut entries = Vec::new();
    for (alias, td) in &manifest.internals {
        ensure_rust_ident(alias, "internals alias", "[internals]")?;
        entries.push(make_entry(alias, td, root, Exposure::Internals)?);
    }
    Ok(entries)
}

// ---------------------------------------------------------------------------
// [schema] の正規化と所有割り当て
// ---------------------------------------------------------------------------

/// パス比較用の正準形。`[internals].proto` と `[schema.*].protos` が同じファイルを別表記
/// (`./x.proto` と `x.proto` など)で指しても同一と判定できるようにする。
fn canonical(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

/// `[schema]` を区画の並びへ正規化し、識別子・依存の健全性を検証する。
/// `[schema]` が無ければ空。
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

/// `depends` が実在の区画を指し、循環が無く、**推移的に閉じている**ことを確かめる。
///
/// 閉じている必要があるのは protoc の都合である: `c` の proto が `b` の proto を import し、
/// その `b` が `a` を import していると、`a` の型も `c` の descriptor set に入ってくる。
/// `c` が `a` を extern しないと `a` の型が `c` にも生成され、同じ型が 2 つできてしまう。
/// cargo は **直接依存** にしか `DEP_*` を渡さないので、reiny 側で閉包を要求するしかない。
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
        // 推移閉包を辿り、宣言漏れがあれば名指しで指摘する(循環もここで検出)。
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

/// 各 `[internals]` 型の所有区画を proto パスから決める。多クレート形でのみ意味を持つ。
/// どの区画にも属さない proto、2 区画が取り合う proto はここで弾く。
fn assign_owners(entries: &mut [Entry], parts: &[SchemaPart], manifest_path: &Path) -> Result<()> {
    // 単一形(protos = None)は全部を持つので割り当て不要。
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

/// `TypeDef` から `Entry` を組む。proto パスは `base` 基準で絶対化する。
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

/// `"ping.Ping"` → (`["ping"]`, `"Ping"`)。`"Ping"` → (`[]`, `"Ping"`)。
fn split_message(message: &str) -> Result<(Vec<String>, String)> {
    let parts: Vec<&str> = message.split('.').filter(|s| !s.is_empty()).collect();
    let (ident, package) = parts.split_last().context("empty message path")?;
    Ok((
        package.iter().map(ToString::to_string).collect(),
        ident.to_string(),
    ))
}

// ---------------------------------------------------------------------------
// コンパイル計画(compile 機能でのみビルド。CLI 内省では使わない)
// ---------------------------------------------------------------------------

/// 「このパッケージが何をコンパイルし、何を外部参照にするか」を 1 つにまとめたもの。
/// 単一 `[schema]` / workspace / per-project では素直に全部を持ち、`[schema.<name>]` の
/// 1 区画をビルドしているときだけ、自分の担当分に絞られて `externs` が埋まる。
#[cfg(feature = "compile")]
struct CompilePlan<'a> {
    /// 生成物へ出す型(区画ビルドでは自分が所有するものだけ)。
    entries: Vec<&'a Entry>,
    /// prost に渡す proto。
    protos: Vec<PathBuf>,
    /// protoc の include ディレクトリ。
    includes: Vec<PathBuf>,
    /// `(".hs.Vector3", "::myapp_proto_geometry::__pb::hs::Vector3")`。
    /// これがあると prost は当該型を **生成せず** 参照だけを差し替える。
    externs: Vec<(String, String)>,
    /// descriptor 上での自分のファイル名。空なら「全部自分のもの」とみなす。
    own_names: Vec<String>,
    /// `links` メタを出す(= 他のスキーマクレートから参照されうる)なら Some。
    emit_meta: Option<PartMeta>,
}

/// スキーマ区画が下流へ渡すもの。
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
        // 単一 [schema] / workspace / per-project: 従来どおり全部を自分でコンパイルする。
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

    // 自分の proto 群の共通祖先を 1 本の include にする。descriptor 上のファイル名も
    // ここからの相対で決まるので、下流に渡す include と必ず同じものを使う。
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

/// 与えられたファイル群の共通の親ディレクトリ。
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

/// cargo が `DEP_<LINKS>_<KEY>` を作るときの名前変換(大文字化 + 非英数を `_` に)。
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

/// 依存スキーマクレートが `links` 経由で渡した include ディレクトリと FQN 一覧を読む。
///
/// cargo は **直接依存** の build script が出したメタしか渡さないので、ここが取れないのは
/// たいてい「Cargo の依存に入っていない」か「相手が `links` を宣言していない」のどちらか。
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

/// 自分の include ディレクトリと、自分が定義した FQN 一覧を下流へ渡す。
/// `links` が無いと cargo はこれを誰にも配らないので、その場で気付けるようにする。
#[cfg(feature = "compile")]
fn emit_schema_metadata(meta: &PartMeta, fqns: &[String]) -> Result<()> {
    // links を足した/消した瞬間に下の検査をやり直させる。reiny-build は rerun-if-changed を
    // 明示している都合上、これが無いと Cargo.toml を直しても build script が再実行されない。
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
// proto コンパイルと descriptor 走査
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
        // 全パッケージを 1 ファイルに束ね、ネストした pub mod として include できるようにする。
        .include_file("reiny_protos.rs")
        // 型 → 指紋(Topic::SCHEMA)と、区画が下流へ渡す FQN 一覧の両方をここから作る。
        .file_descriptor_set_path(&descriptor_path);
    // 他のスキーマクレートが持つ型は「参照だけ差し替え、生成はしない」。
    // これが多クレート分割でリーフ型が二重生成されない仕組み。
    for (proto_path, rust_path) in &plan.externs {
        config.extern_path(proto_path.clone(), rust_path.clone());
    }
    // 生成型は prost-derive 由来で `::prost` を参照するため、利用側 crate は `prost` 依存が要る
    // (prost / tonic と同じ前提)。prost_path はderive 呼び出しだけ変えても展開内の `::prost`
    // は残るので、既定の `::prost` のまま利用側に prost を持たせる。

    // protoc が外から与えられていなければ同梱バイナリを使う(外部インストール不要)。
    // プロセスグローバルな `env::set_var("PROTOC")` ではなく config に載せる —— build script は
    // 単一スレッドとはいえ、他人のプロセス環境を書き換えずに済むならその方がよい。
    println!("cargo:rerun-if-env-changed=PROTOC");
    if env::var_os("PROTOC").is_none()
        && let Ok(protoc) = protoc_bin_vendored::protoc_bin_path()
    {
        config.protoc_executable(protoc);
    }

    // 利用側のカスタマイズは reiny の既定の**後**に当てる(上書きできる側にする)。
    customize(&mut config);

    config
        .compile_protos(protos, includes)
        .context("prost: compiling protos")?;

    let bytes = std::fs::read(&descriptor_path)
        .with_context(|| format!("reading {}", descriptor_path.display()))?;
    <prost_types::FileDescriptorSet as prost::Message>::decode(bytes.as_slice())
        .context("decoding the descriptor set prost just wrote")
}

/// descriptor set から「自分が定義した FQN 一覧」と「型 → スキーマ指紋」を取り出す。
#[cfg(feature = "compile")]
struct DescriptorScan {
    /// 自分のファイルが定義するメッセージ / enum の FQN(宣言順)。
    fqns: Vec<String>,
    /// メッセージ FQN → 指紋。
    fingerprints: BTreeMap<String, u64>,
}

/// `own_names` に載ったファイルを「自分のもの」として FQN を集める(空なら全部が自分のもの)。
/// 指紋は import 由来も含め全メッセージについて計算する —— 引くのは自分の型だけなので害は無く、
/// 分岐が 1 つ減る。
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
        // map フィールドの合成型(`FooEntry`)は利用側から見えないので数えない。
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

/// メッセージ 1 件のスキーマ指紋。
///
/// 材料は **そのメッセージ自身が宣言するフィールド** だけ(番号 / 名前 / 型 / ラベル /
/// 参照先の型名 / oneof 所属)。参照先メッセージの中身までは追わない —— 指紋が守りたいのは
/// 「同名だが別物の型が同じトピックに乗る」ケースで、それはトップレベルの形だけで判別できる。
/// 逆にリーフ型の変更まで見たければ、そのリーフ自身がトピック型であるべきである。
///
/// ハッシュは FNV-1a 64。`DefaultHasher` は Rust の版で値が変わりうるので使えない
/// (指紋はビルドを跨いで安定していなければ意味が無い)。
#[cfg(feature = "compile")]
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

#[cfg(feature = "compile")]
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

// ---------------------------------------------------------------------------
// reiny_generated.rs の生成
// ---------------------------------------------------------------------------

/// `$OUT_DIR/reiny_generated.rs` の中身を組む。`#[reiny::main]` の `mod __reiny_generated` 内に
/// include される前提でパスを書く。
#[cfg(feature = "compile")]
fn render_generated(
    entries: &[&Entry],
    config: Option<&toml::Table>,
    fingerprints: &BTreeMap<String, u64>,
) -> Result<String> {
    let mut out = String::new();
    out.push_str("// @generated by reiny-build — do not edit.\n");

    // prost が束ねた全パッケージ。
    out.push_str("#[allow(clippy::all, unused_imports, dead_code)]\n");
    out.push_str("pub mod __pb {\n");
    out.push_str("    include!(concat!(env!(\"OUT_DIR\"), \"/reiny_protos.rs\"));\n");
    out.push_str("}\n\n");

    // モジュール別の再エクスポート。
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

    // dependencies はプロジェクト名でネストする。
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
            // dependencies::<dep> から __pb は super::super::__pb。
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

    // 型 → トピックの型セグメント。型ごとに 1 回だけ impl(別名で重複しても型は同一なので dedup)。
    // SCHEMA は descriptor 由来の指紋(既定 None なので、引けなければ黙って省く)。
    let mut seen = Vec::new();
    out.push_str(
        "// 型 → トピック(publish: reiny/<domain>/<id>/<TYPE>、\
         subscribe: reiny/<domain>/*/<TYPE>)。\n",
    );
    for e in entries {
        let path = e.type_path();
        if seen.contains(&path) {
            continue;
        }
        seen.push(path.clone());
        let schema = match fingerprints.get(&e.fq_name()) {
            Some(fp) => format!(" const SCHEMA: Option<u64> = Some({fp:#018x});"),
            None => String::new(),
        };
        writeln!(
            out,
            "impl ::reiny::Topic for {path} {{ const TYPE: &'static str = {:?};{schema} }}",
            e.ident
        )
        .ok();
    }

    // [config] があれば型付き設定 `config::Config` と `cloudy.config()` 拡張を生成する。
    if let Some(table) = config {
        out.push('\n');
        out.push_str(&render_config(table)?);
    }

    Ok(out)
}

/// スキーマ消費 grain 向けの薄い生成物。proto は再コンパイルせず、スキーマクレートの
/// `internals` をそのまま `crate::internals` として見せるだけ(`Topic`/`Message` impl は
/// スキーマクレート側に 1 つだけあり、coherence でグローバルに効く)。
#[cfg(feature = "compile")]
fn render_consumer(crate_idents: &[String]) -> String {
    let mut out = String::new();
    out.push_str("// @generated by reiny-build — schema consumer (no proto recompiled).\n");
    // スキーマクレート群の公開型を internals として再エクスポート。型に紐づく impl Topic /
    // impl Message はスキーマクレート側で定義済みなので、ここでは型を見せるだけでよい。
    // 多クレート分割では全区画の和になる(alias は [internals] のキーなので重複しない)。
    out.push_str("pub mod internals {\n");
    for ident in crate_idents {
        writeln!(out, "    pub use ::{ident}::internals::*;").ok();
    }
    out.push_str("}\n");
    out
}

/// `[config]` の TOML table から、型付き `config::Config`(既定値つき)と、`::reiny::Cloudy` に
/// `config()` を生やす拡張トレイトを生成する。`#[reiny::main]` の glob re-export で
/// トレイトがスコープに入るので、利用側は `cloudy.config()` と書ける。
#[cfg(feature = "compile")]
fn render_config(table: &toml::Table) -> Result<String> {
    // TOML 値 → (Rust 型, 既定値リテラル, getter, `v` を field へ代入する式)。
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
    out.push_str("// [config] から生成した型付き設定。\n");
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

    // `cloudy.config()` 拡張。glob re-export でスコープに入る。
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

/// `pub mod <name> { pub use <prefix>::__pb::...::T as Alias; ... }` を 1 つ書く。
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
// 検証
// ---------------------------------------------------------------------------

/// Rust の予約語(生成コードのモジュール名/再エクスポート名に使えない)。raw identifier 化は
/// しない方針なので、ぶつかったらエラーにする。
const RUST_KEYWORDS: &[&str] = &[
    "as", "break", "const", "continue", "crate", "dyn", "else", "enum", "extern", "false", "fn",
    "for", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref",
    "return", "self", "Self", "static", "struct", "super", "trait", "true", "type", "unsafe",
    "use", "where", "while", "async", "await", "abstract", "become", "box", "do", "final", "macro",
    "override", "priv", "typeof", "unsized", "virtual", "yield", "try", "union",
];

/// `name` が Rust 識別子(ASCII、先頭は英字/`_`、以降は英数/`_`、予約語でない)か。
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

/// 生成コードでそのまま識別子になる名前(dep キー・alias・schema crate)を検証し、NG なら
/// 「どこを直すか」を含むエラーで build を止める。これが無いと不正名はずっと下流の rustc
/// 構文エラー(例 `expected ; or {, found -`)になって原因が分からない。
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

/// 同じトピックセグメント(型名)に異なる型が割り当たっていないか。型 = トピックなので、
/// 別々の型が同じセグメントを持つと配線が衝突する。早期に弾く。
fn validate_no_topic_collision(entries: &[Entry], manifest_path: &Path) -> Result<()> {
    // ident(= トピックセグメント) → 最初に見た型パス。
    let mut by_segment: BTreeMap<String, String> = BTreeMap::new();
    for e in entries {
        let path = e.type_path();
        if let Some(prev) = by_segment.get(&e.ident) {
            // 同じ型の別 alias は衝突ではない。別の型なら配線がぶつかる。
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

// ---------------------------------------------------------------------------
// 診断(REINY_VERBOSE=1 で解決結果を cargo:warning に出す)
// ---------------------------------------------------------------------------

/// `REINY_VERBOSE` がセットされていれば、解決したモード・型→トピック・コンパイル対象 proto を
/// `cargo:warning=` で表示する。所有割り当てや推移 import の確認に使う(既定では何も出さない)。
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
    // dedup したコンパイル対象 proto(推移 import は prost が別途引く)。
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
// 小道具
// ---------------------------------------------------------------------------

fn resolve_relative(base: &Path, p: &Path) -> PathBuf {
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        base.join(p)
    }
}

fn rerun_if_changed(path: &Path) {
    // build.rs(OUT_DIR がある)でのみ cargo へ指示を出す。`reiny check` のような CLI 内省では
    // 標準出力を汚さないよう何もしない。
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
            SchemaDecl::Multi(_) => panic!("単一形として読めていない"),
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
            SchemaDecl::Single(_) => panic!("多クレート形として読めていない"),
        }
    }

    /// テスト用の区画(protos は実ファイルを見ないので空でよい — depends の検証だけを見る)。
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
        // a ← b ← c で c が a を宣言していない: cargo は直接依存にしか DEP_* を渡さないので
        // これは通せない。名指しで指摘されること。
        let parts = vec![part("a", &[]), part("b", &["a"]), part("c", &["b"])];
        let err = validate_depends(&parts).unwrap_err().to_string();
        assert!(err.contains("`a`"), "got: {err}");

        // 閉じていれば通る。
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

        // どの区画にも属さない proto は弾く(黙って生成から漏れる方が怖い)。
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
        // ハイフン形とアンダースコア形は同じ env 名になるので、どちらで links を書いてもよい。
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
        // 宣言順が違うだけなら同じ指紋(番号で並べ替えてから畳む)。
        let b = fingerprint("hs.Probe", &msg(vec![field(2, "y", 1), field(1, "x", 1)]));
        assert_eq!(a, b);

        // 型が変われば変わる。
        let c = fingerprint("hs.Probe", &msg(vec![field(1, "x", 5), field(2, "y", 1)]));
        assert_ne!(a, c);
        // 同じ形でも別の型名なら別物(= 同名衝突を弾くための本命)。
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
        // 別パッケージの同名 ident は同じトピックセグメントになる → 衝突。
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

    #[test]
    fn topic_collision_allows_same_type_aliased_twice() {
        // 同じ型(同じパッケージ/ident)を別 alias で 2 度挙げても衝突ではない。
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
}
