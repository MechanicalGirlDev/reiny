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
    /// per-project の型付き設定スキーマ + 既定値(`cloudy.config()` で読む)。
    config: Option<toml::Table>,
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
// エントリポイント
// ---------------------------------------------------------------------------

/// `build.rs` から呼ぶ。Reiny.toml を読み、proto をコンパイルして生成物を `$OUT_DIR` に出す。
pub fn compile() -> Result<()> {
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").context("CARGO_MANIFEST_DIR not set")?);
    let out_dir = PathBuf::from(env::var("OUT_DIR").context("OUT_DIR not set")?);
    let pkg_name = env::var("CARGO_PKG_NAME").context("CARGO_PKG_NAME not set")?;

    let (manifest_path, manifest) = find_manifest(&manifest_dir)
        .with_context(|| format!("locating Reiny.toml from {}", manifest_dir.display()))?;
    let manifest_root = manifest_path
        .parent()
        .expect("Reiny.toml has a parent")
        .to_path_buf();
    rerun_if_changed(&manifest_path);

    let entries = if manifest.project.is_some() {
        resolve_per_project(&manifest, &manifest_root)?
    } else if !manifest.internals.is_empty() || !manifest.projects.is_empty() {
        resolve_workspace(&manifest, &manifest_root, &pkg_name)?
    } else {
        bail!(
            "{} has neither [project] (per-project) nor [internals]/[projects] (workspace)",
            manifest_path.display()
        );
    };

    compile_protos(&entries, &out_dir)?;
    let generated = render_generated(&entries, manifest.config.as_ref())?;
    let generated_path = out_dir.join("reiny_generated.rs");
    std::fs::write(&generated_path, generated)
        .with_context(|| format!("writing {}", generated_path.display()))?;

    Ok(())
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
        entries.push(make_entry(alias, td, root, Exposure::Publications)?);
    }

    // 依存先プロジェクトの公開型(dependencies::<dep>::* として再エクスポート)。
    for (dep_name, dep) in &manifest.dependencies {
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
/// どのプロジェクトが公開するかには依らない。
fn resolve_workspace(manifest: &Manifest, root: &Path, pkg_name: &str) -> Result<Vec<Entry>> {
    if !manifest.projects.contains_key(pkg_name) {
        bail!(
            "package '{pkg_name}' has no [projects.{pkg_name}] entry in the workspace Reiny.toml"
        );
    }

    let mut entries = Vec::new();
    for (alias, td) in &manifest.internals {
        entries.push(make_entry(alias, td, root, Exposure::Internals)?);
    }

    Ok(entries)
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
// proto コンパイル
// ---------------------------------------------------------------------------

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

/// `[config]` の TOML table から、型付き `config::Config`(既定値つき)と、`::reiny::Cloudy` に
/// `config()` を生やす拡張トレイトを生成する。`#[reiny::main]` の glob re-export で
/// トレイトがスコープに入るので、利用側は `cloudy.config()` と書ける。
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
    println!("cargo:rerun-if-changed={}", path.display());
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
}
