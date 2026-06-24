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
    schema: Option<Schema>,
    /// per-project の型付き設定スキーマ + 既定値(`cloudy.config()` で読む)。
    config: Option<toml::Table>,
}

/// `[schema] crate = "myapp-schema"`。workspace モードで、`[internals]` の型を **この
/// クレートだけ**が prost コンパイル + `impl Topic` し、他の grain はそれを Cargo 依存として
/// 再エクスポートする(grain ごとの重複 prost コンパイルを無くす)。`crate` はキーワードなので
/// rename で受ける。
#[derive(Debug, Deserialize)]
struct Schema {
    #[serde(rename = "crate")]
    crate_name: String,
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
}

impl Entry {
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
    /// workspace + `[schema]`。自分が **スキーマクレート本体**で、`[internals]` を 1 度だけ
    /// prost コンパイル + `impl Topic` する。grain はこれを Cargo 依存として共有する。
    Schema,
    /// workspace + `[schema]`。自分はスキーマを **消費する grain**。proto は再コンパイルせず、
    /// スキーマクレート(`crate_ident`)の型を `internals` として再エクスポートするだけ。
    SchemaConsumer {
        /// 依存するスキーマクレートの extern ident(`myapp-schema` → `myapp_schema`)。
        crate_ident: String,
    },
}

impl Mode {
    /// 人が読むラベル(`reiny check` 用)。
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Mode::PerProject => "per-project",
            Mode::Workspace => "workspace",
            Mode::Schema => "workspace+schema (schema crate)",
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
}

/// Reiny.toml を解決した結果。proto コンパイル前の純粋な情報なので、`compile` 機能(prost)無しでも
/// 得られる。`reiny check` はこれを表示し、[`compile`] はこれを使って生成物を書き出す。
pub struct Resolution {
    mode: Mode,
    entries: Vec<Entry>,
    config: Option<toml::Table>,
    manifest_path: PathBuf,
    /// `[schema].crate`(あれば)。表示用。
    schema_crate: Option<String>,
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

    /// `[schema].crate`(workspace 共有スキーマクレート名)。無ければ `None`。
    #[must_use]
    pub fn schema_crate(&self) -> Option<&str> {
        self.schema_crate.as_deref()
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
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// エントリポイント
// ---------------------------------------------------------------------------

/// `build.rs` から呼ぶ。Reiny.toml を読み、proto をコンパイルして生成物を `$OUT_DIR` に出す。
///
/// `compile` 機能(既定 on)が要る。`reiny check` のように prost を引きたくない内省用途では
/// [`resolve`] を直接使う。
#[cfg(feature = "compile")]
pub fn compile() -> Result<()> {
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").context("CARGO_MANIFEST_DIR not set")?);
    let out_dir = PathBuf::from(env::var("OUT_DIR").context("OUT_DIR not set")?);
    let pkg_name = env::var("CARGO_PKG_NAME").context("CARGO_PKG_NAME not set")?;

    let resolution = resolve_for(&manifest_dir, &pkg_name)?;
    report_verbose(&resolution);

    // スキーマ消費 grain は proto を再コンパイルせず、スキーマクレートを再エクスポートするだけ。
    // それ以外は proto をコンパイルして完全な生成物を出す。
    let generated = if let Mode::SchemaConsumer { crate_ident } = &resolution.mode {
        render_consumer(crate_ident)
    } else {
        compile_protos(&resolution.entries, &out_dir)?;
        render_generated(&resolution.entries, resolution.config.as_ref())?
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

    let schema_crate = manifest.schema.as_ref().map(|s| s.crate_name.clone());

    let (mode, entries) = if manifest.project.is_some() {
        (
            Mode::PerProject,
            resolve_per_project(&manifest, &manifest_root)?,
        )
    } else if !manifest.internals.is_empty() || !manifest.projects.is_empty() {
        // カタログ視点: どの 1 パッケージにも束縛しない。[schema] があればその旨を示す。
        if let Some(s) = &manifest.schema {
            let crate_ident = s.crate_name.replace('-', "_");
            ensure_rust_ident(&crate_ident, "[schema].crate", "[schema]")?;
        }
        let mut entries = Vec::new();
        for (alias, td) in &manifest.internals {
            ensure_rust_ident(alias, "internals alias", "[internals]")?;
            entries.push(make_entry(alias, td, &manifest_root, Exposure::Internals)?);
        }
        let mode = if schema_crate.is_some() {
            Mode::Schema
        } else {
            Mode::Workspace
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
        schema_crate,
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

    let schema_crate = manifest.schema.as_ref().map(|s| s.crate_name.clone());

    let (mode, entries) = if manifest.project.is_some() {
        (
            Mode::PerProject,
            resolve_per_project(&manifest, &manifest_root)?,
        )
    } else if !manifest.internals.is_empty() || !manifest.projects.is_empty() {
        resolve_workspace(&manifest, &manifest_root, pkg_name, &manifest_path)?
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
        schema_crate,
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
) -> Result<(Mode, Vec<Entry>)> {
    // [schema] の有無と、自分がスキーマクレート本体かでモードを決める。
    let in_projects = manifest.projects.contains_key(pkg_name);
    let mode = if let Some(schema) = &manifest.schema {
        let crate_ident = schema.crate_name.replace('-', "_");
        ensure_rust_ident(&crate_ident, "[schema].crate", "[schema]")?;
        if pkg_name == schema.crate_name {
            Mode::Schema
        } else if in_projects {
            // 消費 grain は [projects.<pkg>] に居る必要がある。
            Mode::SchemaConsumer { crate_ident }
        } else {
            bail!(
                "package '{pkg_name}' は {} の [projects.{pkg_name}] にも [schema].crate \
                 (= '{}') にも該当しません",
                manifest_path.display(),
                schema.crate_name
            );
        }
    } else if in_projects {
        Mode::Workspace
    } else {
        bail!(
            "package '{pkg_name}' has no [projects.{pkg_name}] entry in the workspace \
             Reiny.toml ({})",
            manifest_path.display()
        );
    };

    // entries は全モードで [internals] を解決しておく(消費モードでは内省・診断にのみ使い、
    // proto はコンパイルしない)。
    let mut entries = Vec::new();
    for (alias, td) in &manifest.internals {
        ensure_rust_ident(alias, "internals alias", "[internals]")?;
        entries.push(make_entry(alias, td, root, Exposure::Internals)?);
    }

    Ok((mode, entries))
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
// proto コンパイル(compile 機能でのみビルド。CLI 内省では使わない)
// ---------------------------------------------------------------------------

#[cfg(feature = "compile")]
fn compile_protos(entries: &[Entry], out_dir: &Path) -> Result<()> {
    // 重複 proto を除いた一覧と、include に使う親ディレクトリ集合。
    let mut protos: Vec<PathBuf> = Vec::new();
    let mut includes: Vec<PathBuf> = Vec::new();
    for e in entries {
        if !protos.contains(&e.proto) {
            protos.push(e.proto.clone());
        }
        if let Some(parent) = e.proto.parent() {
            let parent = parent.to_path_buf();
            if !includes.contains(&parent) {
                includes.push(parent);
            }
        }
    }

    // protoc が外から与えられていなければ同梱バイナリを使う(外部インストール不要)。
    println!("cargo:rerun-if-env-changed=PROTOC");
    if env::var_os("PROTOC").is_none()
        && let Ok(protoc) = protoc_bin_vendored::protoc_bin_path()
    {
        // SAFETY: build script は単一スレッド。
        unsafe {
            env::set_var("PROTOC", protoc);
        }
    }

    let mut config = prost_build::Config::new();
    config
        .out_dir(out_dir)
        // 全パッケージを 1 ファイルに束ね、ネストした pub mod として include できるようにする。
        .include_file("reiny_protos.rs");
    // 生成型は prost-derive 由来で `::prost` を参照するため、利用側 crate は `prost` 依存が要る
    // (prost / tonic と同じ前提)。prost_path はderive 呼び出しだけ変えても展開内の `::prost`
    // は残るので、既定の `::prost` のまま利用側に prost を持たせる。

    config
        .compile_protos(&protos, &includes)
        .context("prost: compiling protos")?;

    Ok(())
}

// ---------------------------------------------------------------------------
// reiny_generated.rs の生成
// ---------------------------------------------------------------------------

/// `$OUT_DIR/reiny_generated.rs` の中身を組む。`#[reiny::main]` の `mod __reiny_generated` 内に
/// include される前提でパスを書く。
#[cfg(feature = "compile")]
fn render_generated(entries: &[Entry], config: Option<&toml::Table>) -> Result<String> {
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
        .filter(|e| e.exposure == Exposure::Publications)
        .collect();
    if !publications.is_empty() {
        render_reexport_module(&mut out, "publications", "super", &publications);
    }

    let internals: Vec<&Entry> = entries
        .iter()
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
    let mut seen = Vec::new();
    out.push_str("// 型 → トピック(publish: reiny/<id>/<TYPE>、subscribe: reiny/*/<TYPE>)。\n");
    for e in entries {
        let path = e.type_path();
        if seen.contains(&path) {
            continue;
        }
        seen.push(path.clone());
        writeln!(
            out,
            "impl ::reiny::Topic for {path} {{ const TYPE: &'static str = {:?}; }}",
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
fn render_consumer(crate_ident: &str) -> String {
    let mut out = String::new();
    out.push_str("// @generated by reiny-build — schema consumer (no proto recompiled).\n");
    // スキーマクレートの公開型を internals として再エクスポート。型に紐づく impl Topic /
    // impl Message はスキーマクレートで定義済みなので、ここでは型を見せるだけでよい。
    writeln!(
        out,
        "pub mod internals {{ pub use ::{crate_ident}::internals::*; }}"
    )
    .ok();
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
    if let Mode::SchemaConsumer { crate_ident } = &res.mode {
        warn(&format!(
            "consumes schema crate `{crate_ident}` (no proto recompiled; \
             {} type(s) re-exported)",
            res.entries.len()
        ));
    }
    for t in res.types() {
        warn(&format!(
            "type {} = {} -> topic reiny/<id>/{} [{}]",
            t.alias, t.message, t.topic_segment, t.module
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
        assert_eq!(m.schema.unwrap().crate_name, "myapp-schema");
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
            },
            Entry {
                alias: "B".into(),
                package: vec!["b".into()],
                ident: "Ping".into(),
                exposure: Exposure::Internals,
                proto: PathBuf::from("/x/b.proto"),
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
            },
            Entry {
                alias: "AliasOfA".into(),
                package: vec!["a".into()],
                ident: "Ping".into(),
                exposure: Exposure::Dependencies("dep".into()),
                proto: PathBuf::from("/x/a.proto"),
            },
        ];
        assert!(validate_no_topic_collision(&entries, Path::new("/x/Reiny.toml")).is_ok());
    }

    #[test]
    #[cfg(feature = "compile")]
    fn consumer_render_reexports_schema_crate() {
        let out = render_consumer("myapp_schema");
        assert!(out.contains("pub use ::myapp_schema::internals::*"));
    }
}
