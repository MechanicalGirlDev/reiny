//! `reiny check` — Reiny.toml を解決し、型 → トピックの対応と所有/依存モードを表示する。
//!
//! proto はコンパイルしない(`reiny-build` を `default-features = false` で使う)。配置ミス
//! (ハイフン入りの依存キー、トピック衝突、proto 不在など)はここで `reiny-build` の検証に
//! かかって早期に分かる。出力は人向け。CI 用途なら終了コードで判定できる。

use std::path::{Path, PathBuf};

use anyhow::Result;

/// `reiny check [path]`。`path` 省略時はカレントディレクトリから上方へ Reiny.toml を探す。
pub(crate) fn check(path: Option<&Path>) -> Result<()> {
    let dir = match path {
        Some(p) => p.to_path_buf(),
        None => std::env::current_dir()?,
    };

    let resolution = reiny_build::describe(&dir)?;

    println!("reiny check — {}", resolution.manifest_path().display());
    println!("mode: {}", resolution.mode().label());
    if let Some(schema) = resolution.schema_crate() {
        println!("schema crate: {schema} (compiles [internals] once; grains share it)");
    }
    if resolution.has_config() {
        println!("config: [config] present (typed cloudy.config())");
    }

    let types = resolution.types();
    println!();
    println!("types ({}):", types.len());
    if types.is_empty() {
        println!("  (none)");
        return Ok(());
    }

    // 列幅を揃える。
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
        println!(
            "  {:<w_alias$}  {:<w_msg$}  ->  reiny/<id>/{:<w_topic$}  [{:<w_mod$}]  {}",
            t.alias,
            t.message,
            t.topic_segment,
            t.module,
            proto.display(),
        );
    }

    Ok(())
}

/// proto パスを Reiny.toml のあるディレクトリ基準の相対にして読みやすくする(無理なら絶対のまま)。
fn rel_to(manifest_path: &Path, proto: &Path) -> PathBuf {
    let base = manifest_path.parent().unwrap_or(Path::new("."));
    proto
        .strip_prefix(base)
        .map_or_else(|_| proto.to_path_buf(), Path::to_path_buf)
}
