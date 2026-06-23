//! `reiny new` / `reiny init` / `reiny add` — grain 雛形の生成と依存配線。
//!
//! `new` と `init` の違いはディレクトリを新規に作るかどうかだけで、生成する中身
//! (`Cargo.toml` / `Reiny.toml` / `build.rs` / `proto/` / `src/main.rs`)は同じ。
//! `add` は自分の `Reiny.toml` の `[dependencies]` に相手を追記する(src は変えない)。

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

/// `reiny new <path> --publish <T>`。`<path>` を新規作成して雛形を書き出す。
pub(crate) fn new(path: &Path, publish: Option<&str>, name: Option<&str>) -> Result<()> {
    if path.exists() {
        bail!(
            "{} already exists — use `reiny init {}` to scaffold in place",
            path.display(),
            path.display()
        );
    }
    std::fs::create_dir_all(path).with_context(|| format!("creating {}", path.display()))?;
    scaffold(path, publish, name)?;
    println!(
        "created grain '{}' at {}",
        project_name(path, name)?,
        path.display()
    );
    Ok(())
}

/// `reiny init [path] --publish <T>`。ディレクトリは作らず、その場に雛形を足す。
pub(crate) fn init(path: Option<&Path>, publish: Option<&str>, name: Option<&str>) -> Result<()> {
    let dir = match path {
        Some(p) => p.to_path_buf(),
        None => std::env::current_dir().context("resolving current dir")?,
    };
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    scaffold(&dir, publish, name)?;
    println!(
        "initialized grain '{}' in {}",
        project_name(&dir, name)?,
        dir.display()
    );
    Ok(())
}

/// 雛形一式を `dir` に書き出す。既存ファイルは壊さない(`Cargo.toml` は追記マージ)。
fn scaffold(dir: &Path, publish: Option<&str>, name: Option<&str>) -> Result<()> {
    let proj = project_name(dir, name)?;

    write_cargo_toml(dir, &proj)?;
    write_if_absent(&dir.join("Reiny.toml"), &reiny_toml(&proj, publish))?;
    write_if_absent(&dir.join("build.rs"), BUILD_RS)?;

    if let Some(ty) = publish {
        let lower = ty.to_lowercase();
        std::fs::create_dir_all(dir.join("proto")).context("creating proto/")?;
        write_if_absent(
            &dir.join("proto").join(format!("{lower}.proto")),
            &proto(ty),
        )?;
    }

    std::fs::create_dir_all(dir.join("src")).context("creating src/")?;
    write_if_absent(&dir.join("src").join("main.rs"), &main_rs(publish))?;
    Ok(())
}

/// `reiny add <path>`。カレント grain の `Reiny.toml` の `[dependencies]` に相手を追記する。
pub(crate) fn add(dep_path: &Path) -> Result<()> {
    let cwd = std::env::current_dir().context("resolving current dir")?;
    let my_manifest = cwd.join("Reiny.toml");
    if !my_manifest.is_file() {
        bail!(
            "no Reiny.toml in {} — run this inside a grain project",
            cwd.display()
        );
    }

    let dep_manifest = dep_path.join("Reiny.toml");
    let dep_text = std::fs::read_to_string(&dep_manifest)
        .with_context(|| format!("reading dependency manifest {}", dep_manifest.display()))?;
    let dep: DepManifest =
        toml::from_str(&dep_text).with_context(|| format!("parsing {}", dep_manifest.display()))?;
    let dep_name = dep
        .project
        .as_ref()
        .map(|p| p.name.clone())
        .with_context(|| format!("{} has no [project].name", dep_manifest.display()))?;
    let version = dep
        .project
        .and_then(|p| p.version)
        .map_or_else(|| "0.1".to_string(), |v| major_minor(&v));

    // 相対パスはそのまま使う(Reiny.toml のパスは manifest dir 基準で解決される)。
    let dep_path_str = dep_path.to_string_lossy().replace('\\', "/");
    let entry = format!("{dep_name} = {{ version = \"{version}\", path = \"{dep_path_str}\" }}");

    let text = std::fs::read_to_string(&my_manifest)
        .with_context(|| format!("reading {}", my_manifest.display()))?;
    match insert_dependency(&text, &dep_name, &entry) {
        InsertResult::Added(updated) => {
            std::fs::write(&my_manifest, updated)
                .with_context(|| format!("writing {}", my_manifest.display()))?;
            println!("added dependency '{dep_name}' (path = {dep_path_str})");
        }
        InsertResult::AlreadyPresent => {
            println!("dependency '{dep_name}' already declared — nothing to do");
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Cargo.toml(新規生成 / 既存への追記マージ)
// ---------------------------------------------------------------------------

/// reiny の各クレートへの path 依存を埋め込んで `Cargo.toml` を用意する。既存があれば
/// `[package].name` と reiny 依存を補うだけで、他は壊さない。
fn write_cargo_toml(dir: &Path, proj: &str) -> Result<()> {
    let (reiny_dep, build_dep) = if let Some((reiny, build)) = locate_reiny_crates(dir) {
        (
            format!("{{ path = {:?} }}", reiny.to_string_lossy()),
            format!("{{ path = {:?} }}", build.to_string_lossy()),
        )
    } else {
        eprintln!(
            "warning: reiny crates not found above {} — using version deps (won't build offline)",
            dir.display()
        );
        ("\"0.1\"".to_string(), "\"0.1\"".to_string())
    };

    let path = dir.join("Cargo.toml");
    if path.is_file() {
        merge_cargo_toml(&path, proj, &reiny_dep, &build_dep)
    } else {
        let body = format!(
            "[package]\n\
             name = \"{proj}\"\n\
             version = \"0.1.0\"\n\
             edition = \"2021\"\n\
             \n\
             [dependencies]\n\
             reiny = {reiny_dep}\n\
             prost = \"0.13\"\n\
             tokio = {{ version = \"1\", features = [\"full\"] }}\n\
             tracing = \"0.1\"\n\
             tracing-subscriber = {{ version = \"0.3\", features = [\"env-filter\"] }}\n\
             \n\
             [build-dependencies]\n\
             reiny-build = {build_dep}\n"
        );
        std::fs::write(&path, body).with_context(|| format!("writing {}", path.display()))?;
        Ok(())
    }
}

/// 既存 `Cargo.toml` に reiny 依存・build-deps・`[package].name` を補う(他は保持)。
fn merge_cargo_toml(path: &Path, proj: &str, reiny_dep: &str, build_dep: &str) -> Result<()> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let mut doc: toml::Table =
        toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;

    let pkg = doc
        .entry("package".to_string())
        .or_insert_with(|| toml::Value::Table(toml::Table::new()));
    if let Some(t) = pkg.as_table_mut() {
        t.entry("name".to_string())
            .or_insert_with(|| toml::Value::String(proj.to_string()));
        t.entry("version".to_string())
            .or_insert_with(|| toml::Value::String("0.1.0".to_string()));
        t.entry("edition".to_string())
            .or_insert_with(|| toml::Value::String("2021".to_string()));
    }

    let reiny_val: toml::Value = toml::from_str(&format!("x = {reiny_dep}"))
        .ok()
        .and_then(|t: toml::Table| t.get("x").cloned())
        .unwrap_or_else(|| toml::Value::String("0.1".to_string()));
    let build_val: toml::Value = toml::from_str(&format!("x = {build_dep}"))
        .ok()
        .and_then(|t: toml::Table| t.get("x").cloned())
        .unwrap_or_else(|| toml::Value::String("0.1".to_string()));

    insert_dep(&mut doc, "dependencies", "reiny", reiny_val);
    insert_simple_deps(&mut doc);
    insert_dep(&mut doc, "build-dependencies", "reiny-build", build_val);

    let rendered = toml::to_string_pretty(&doc).context("re-serializing Cargo.toml")?;
    std::fs::write(path, rendered).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

/// `prost` / `tokio` / `tracing` / `tracing-subscriber` を `[dependencies]` に補う。
fn insert_simple_deps(doc: &mut toml::Table) {
    let str_dep = |s: &str| toml::Value::String(s.to_string());
    insert_dep(doc, "dependencies", "prost", str_dep("0.13"));
    insert_dep(doc, "dependencies", "tracing", str_dep("0.1"));
    if let Ok(v) = toml::from_str::<toml::Table>("x = { version = \"1\", features = [\"full\"] }")
        && let Some(tokio) = v.get("x").cloned()
    {
        insert_dep(doc, "dependencies", "tokio", tokio);
    }
    if let Ok(v) =
        toml::from_str::<toml::Table>("x = { version = \"0.3\", features = [\"env-filter\"] }")
        && let Some(sub) = v.get("x").cloned()
    {
        insert_dep(doc, "dependencies", "tracing-subscriber", sub);
    }
}

/// `doc[table][key]` を未設定なら value で埋める。
fn insert_dep(doc: &mut toml::Table, table: &str, key: &str, value: toml::Value) {
    let t = doc
        .entry(table.to_string())
        .or_insert_with(|| toml::Value::Table(toml::Table::new()));
    if let Some(tbl) = t.as_table_mut() {
        tbl.entry(key.to_string()).or_insert(value);
    }
}

// ---------------------------------------------------------------------------
// Reiny.toml の [dependencies] への追記(テキストを保ったまま)
// ---------------------------------------------------------------------------

enum InsertResult {
    Added(String),
    AlreadyPresent,
}

/// `[dependencies]` セクションの末尾に `entry` 行を挿入する。セクションが無ければ末尾に作る。
/// コメントや整形を壊さないよう、TOML を再シリアライズせずテキストを編集する。
fn insert_dependency(text: &str, dep_name: &str, entry: &str) -> InsertResult {
    let lines: Vec<&str> = text.lines().collect();
    let mut dep_header: Option<usize> = None;
    for (i, line) in lines.iter().enumerate() {
        if line.trim() == "[dependencies]" {
            dep_header = Some(i);
            break;
        }
    }

    let Some(start) = dep_header else {
        // セクションが無い → 末尾に新設。
        let mut out = text.trim_end().to_string();
        out.push_str("\n\n[dependencies]\n");
        out.push_str(entry);
        out.push('\n');
        return InsertResult::Added(out);
    };

    // セクションの範囲(次のテーブルヘッダ or EOF まで)。
    let mut end = lines.len();
    for (i, line) in lines.iter().enumerate().skip(start + 1) {
        let t = line.trim_start();
        if t.starts_with('[') {
            end = i;
            break;
        }
    }

    // 既に宣言済みか。
    let already = lines[start + 1..end].iter().any(|l| {
        let t = l.trim_start();
        t.strip_prefix(dep_name)
            .is_some_and(|rest| rest.trim_start().starts_with('='))
    });
    if already {
        return InsertResult::AlreadyPresent;
    }

    // セクション内の最後の非空行の直後に挿入する。
    let mut insert_at = start + 1;
    for (i, line) in lines.iter().enumerate().take(end).skip(start + 1) {
        if !line.trim().is_empty() {
            insert_at = i + 1;
        }
    }

    let mut out: Vec<String> = lines.iter().map(|s| (*s).to_string()).collect();
    out.insert(insert_at, entry.to_string());
    let mut joined = out.join("\n");
    if text.ends_with('\n') {
        joined.push('\n');
    }
    InsertResult::Added(joined)
}

// ---------------------------------------------------------------------------
// テンプレート
// ---------------------------------------------------------------------------

const BUILD_RS: &str = "//! Reiny.toml 駆動のコード生成。\n\
//! [publications] の proto から `reiny::publications::*` を、[dependencies] の公開型から\n\
//! `reiny::dependencies::<project>::*` を生成し、「型 → トピック」を埋め込む。\n\
fn main() {\n\
    reiny_build::compile().expect(\"reiny codegen from Reiny.toml\");\n\
}\n";

fn reiny_toml(proj: &str, publish: Option<&str>) -> String {
    let publications = match publish {
        Some(ty) => {
            let lower = ty.to_lowercase();
            format!("{ty} = {{ proto = \"proto/{lower}.proto\", message = \"{lower}.{ty}\" }}\n")
        }
        None => String::new(),
    };
    format!(
        "[project]\n\
         # プロジェクト名 = プロセス/インスタンス名(トピックではない)。\n\
         name = \"{proj}\"\n\
         version = \"0.1.0\"\n\
         \n\
         # このプロジェクトが公開する型(型 = トピック)。\n\
         [publications]\n\
         {publications}\
         \n\
         # 依存する他プロジェクト。`reiny add <path>` で追記される。\n\
         [dependencies]\n"
    )
}

fn proto(ty: &str) -> String {
    let lower = ty.to_lowercase();
    format!(
        "syntax = \"proto3\";\n\
         \n\
         package {lower};\n\
         \n\
         // {ty}: 一定間隔で送られるメッセージ。\n\
         message {ty} {{\n\
         \x20 uint64 seq = 1;\n\
         \x20 int64 sent_unix = 2;\n\
         }}\n"
    )
}

fn main_rs(publish: Option<&str>) -> String {
    match publish {
        Some(ty) => {
            let lower = ty.to_lowercase();
            format!(
                "//! {lower} — 公開型 `{ty}` を一定間隔で流す grain。\n\
                 //!\n\
                 //! 他プロジェクトの型を購読するには `reiny add <path>` で依存を足し、\n\
                 //! `cloudy.subscribe::<T>()` をこの main に書き足す。\n\
                 \n\
                 use std::time::Duration;\n\
                 \n\
                 use reiny::prelude::*;\n\
                 \n\
                 use crate::publications::{ty};\n\
                 \n\
                 #[reiny::main]\n\
                 async fn main(cloudy: Cloudy) -> reiny::Result<()> {{\n\
                 \x20   let out = cloudy.publish::<{ty}>()?;\n\
                 \x20   let mut tick = tokio::time::interval(Duration::from_secs(1));\n\
                 \x20   let mut seq = 0;\n\
                 \x20   loop {{\n\
                 \x20       tokio::select! {{\n\
                 \x20           _ = tick.tick() => {{\n\
                 \x20               out.send({ty} {{ seq, sent_unix: cloudy.now_unix() }}).await?;\n\
                 \x20               tracing::info!(seq, \"{lower} →\");\n\
                 \x20               seq += 1;\n\
                 \x20           }}\n\
                 \x20           _ = cloudy.shutdown() => break,\n\
                 \x20       }}\n\
                 \x20   }}\n\
                 \x20   Ok(())\n\
                 }}\n"
            )
        }
        None => "//! 純粋な購読者(sink)の雛形。`reiny add <path>` で依存を足し、\n\
             //! `cloudy.subscribe::<T>()` を書いて受信ループを組む。\n\
             \n\
             use reiny::prelude::*;\n\
             \n\
             #[reiny::main]\n\
             async fn main(cloudy: Cloudy) -> reiny::Result<()> {\n\
             \x20   tracing::info!(\"up; waiting. add a dependency and subscribe::<T>()\");\n\
             \x20   cloudy.shutdown().await;\n\
             \x20   Ok(())\n\
             }\n"
        .to_string(),
    }
}

// ---------------------------------------------------------------------------
// 小道具
// ---------------------------------------------------------------------------

/// `--name` 指定、無ければディレクトリ名をプロジェクト名にする。
fn project_name(dir: &Path, name: Option<&str>) -> Result<String> {
    if let Some(n) = name {
        return Ok(n.to_string());
    }
    let abs = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
    abs.file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .with_context(|| format!("cannot derive a name from {}", dir.display()))
}

/// `dir` から上方向に reiny リポジトリ(`crates/reiny` と `crates/reiny-build` を持つ)を探す。
/// 見つかればその 2 クレートへの絶対パスを返す。生成プロジェクトの path 依存に使う。
fn locate_reiny_crates(dir: &Path) -> Option<(PathBuf, PathBuf)> {
    let start = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
    let mut cur = Some(start.as_path());
    while let Some(d) = cur {
        let reiny = d.join("crates").join("reiny");
        let build = d.join("crates").join("reiny-build");
        if reiny.join("Cargo.toml").is_file() && build.join("Cargo.toml").is_file() {
            return Some((reiny, build));
        }
        cur = d.parent();
    }
    None
}

/// `"0.1.0"` → `"0.1"`。解釈できなければ元のまま。
fn major_minor(v: &str) -> String {
    let parts: Vec<&str> = v.split('.').collect();
    if parts.len() >= 2 {
        format!("{}.{}", parts[0], parts[1])
    } else {
        v.to_string()
    }
}

/// 既に在るファイルは上書きしない(雛形は壊さない)。
fn write_if_absent(path: &Path, contents: &str) -> Result<()> {
    if path.exists() {
        return Ok(());
    }
    std::fs::write(path, contents).with_context(|| format!("writing {}", path.display()))
}

// ---------------------------------------------------------------------------
// 依存先 Reiny.toml の最小スキーマ([project] だけ読む)
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct DepManifest {
    project: Option<DepProject>,
}

#[derive(serde::Deserialize)]
struct DepProject {
    name: String,
    version: Option<String>,
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)] // テストは panic で失敗を表現してよい
mod tests {
    use super::*;

    #[test]
    fn inserts_into_existing_dependencies_section() {
        let src = "[project]\nname = \"pong\"\n\n[publications]\n\n[dependencies]\n";
        let entry = "ping = { version = \"0.1\", path = \"../ping\" }";
        match insert_dependency(src, "ping", entry) {
            InsertResult::Added(out) => {
                assert!(out.contains(entry));
                assert!(out.trim_end().ends_with(entry));
            }
            InsertResult::AlreadyPresent => panic!("should add"),
        }
    }

    #[test]
    fn skips_when_already_present() {
        let src = "[dependencies]\nping = { version = \"0.1\", path = \"../ping\" }\n";
        let entry = "ping = { version = \"0.1\", path = \"../x\" }";
        assert!(matches!(
            insert_dependency(src, "ping", entry),
            InsertResult::AlreadyPresent
        ));
    }

    #[test]
    fn creates_section_when_absent() {
        let src = "[project]\nname = \"ping\"\n";
        let entry = "pong = { version = \"0.1\", path = \"../pong\" }";
        match insert_dependency(src, "pong", entry) {
            InsertResult::Added(out) => {
                assert!(out.contains("[dependencies]"));
                assert!(out.contains(entry));
            }
            InsertResult::AlreadyPresent => panic!("should add"),
        }
    }

    #[test]
    fn major_minor_trims() {
        assert_eq!(major_minor("0.1.0"), "0.1");
        assert_eq!(major_minor("1.2.3-rc1"), "1.2");
        assert_eq!(major_minor("7"), "7");
    }
}
