//! `reiny bag` / `reiny topic` / `reiny node` / `reiny service` が共有する、バスの語彙。
//!
//! キーの形(`reiny/<domain>/<id>/<TYPE>` と脇道 `@schema` / `@service` / `@grain`)、grain と
//! 同じ経路でのセッション構築、走っている publisher / server が名乗る descriptor の収集。
//! ここに置くのは「reiny / zenoh の約束事を知らないと書けない」部分だけで、統計や表示は
//! 各サブコマンド側にある。
//!
//! 同期。zenoh の `.wait()` で足り、tokio は要らない(CLI 全体も tokio 無し)。

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;
use reiny::zenoh::{self, Wait};
use reiny::{RuntimeOptions, ZenohSource};

/// キーのプレフィクスと、脇道のチャンク名。reiny 本体と揃える(verbatim なので `*` に見えない)。
pub(crate) const KEY_ROOT: &str = "reiny";
pub(crate) const SCHEMA_CHUNK: &str = "@schema";
pub(crate) const SERVICE_CHUNK: &str = "@service";
pub(crate) const GRAIN_CHUNK: &str = "@grain";

/// grain と同じ綴りの fabric 引数。全サブコマンドで共通。
#[derive(Args, Clone)]
pub(crate) struct BusArgs {
    /// 論理名前空間(既定: `--domain` > `REINY_DOMAIN` > "default")。
    #[arg(long)]
    pub(crate) domain: Option<String>,
    /// zenoh 設定ファイル(JSON5 / JSON / YAML)。
    #[arg(long)]
    pub(crate) zenoh_config: Option<PathBuf>,
    /// 接続先エンドポイント(繰り返し可、例 `tcp/127.0.0.1:7447`)。
    #[arg(long)]
    pub(crate) connect: Vec<String>,
    /// zenoh の動作モード(`peer` / `client` / `router`)。
    #[arg(long)]
    pub(crate) zenoh_mode: Option<String>,
}

impl BusArgs {
    /// fabric 引数から (zenoh セッション, 解決済み domain) を組む。`RuntimeOptions` を経由するので
    /// 既定値・`REINY_DOMAIN`・`--connect` の json5 化は grain と同じ経路になる。
    pub(crate) fn open(&self) -> Result<(zenoh::Session, String)> {
        let mut opts = RuntimeOptions::new("reiny-cli");
        if let Some(d) = &self.domain {
            opts.domain.clone_from(d);
        }
        if let Some(f) = &self.zenoh_config {
            opts.zenoh = ZenohSource::File(f.clone());
        }
        if !self.connect.is_empty() {
            let list = self
                .connect
                .iter()
                .map(|e| format!("\"{e}\""))
                .collect::<Vec<_>>()
                .join(",");
            opts.zenoh_overrides
                .push(("connect/endpoints".to_string(), format!("[{list}]")));
        }
        if let Some(m) = &self.zenoh_mode {
            opts.zenoh_overrides
                .push(("mode".to_string(), format!("\"{m}\"")));
        }
        let config = opts.zenoh_config()?;
        let session = zenoh::open(config)
            .wait()
            .map_err(anyhow::Error::msg)
            .context("opening zenoh session")?;
        Ok((session, opts.domain))
    }
}

/// キー `reiny/<domain>/<source>/<TYPE>` の 3 セグメント。脇道(`/@…`)付きは弾く(None)。
pub(crate) struct KeyParts<'a> {
    pub(crate) domain: &'a str,
    pub(crate) source: &'a str,
    pub(crate) ty: &'a str,
}

impl<'a> KeyParts<'a> {
    pub(crate) fn parse(key: &'a str) -> Option<Self> {
        let mut segs = key.split('/');
        if segs.next()? != KEY_ROOT {
            return None;
        }
        let domain = segs.next()?;
        let source = segs.next()?;
        let ty = segs.next()?;
        // 4 段ちょうど(脇道 `/@schema/...` が付いていたら型のトピックではない)。
        if segs.next().is_some() {
            return None;
        }
        Some(Self { domain, source, ty })
    }

    /// `reiny/<domain>/<source>/<TYPE>/<chunk>` の形(脇道付き)。`chunk` は `@service` など。
    pub(crate) fn parse_with_chunk(key: &'a str, chunk: &str) -> Option<Self> {
        let base = key.strip_suffix(chunk)?.strip_suffix('/')?;
        Self::parse(base)
    }
}

/// `reiny/<domain>/<id>/…` から `<id>` を取る(presence 判定用)。
pub(crate) fn key_source(key: &str) -> &str {
    key.split('/').nth(2).unwrap_or_default()
}

/// zenoh の attachment を reiny の指紋(8 バイト LE)として読む。形が違えば `None`。
pub(crate) fn attachment_u64(sample: &zenoh::sample::Sample) -> Option<u64> {
    let bytes = sample.attachment()?.to_bytes();
    <[u8; 8]>::try_from(bytes.as_ref())
        .ok()
        .map(u64::from_le_bytes)
}

/// `key` に生きている liveliness トークンのキー一覧(昇順、重複なし)。
pub(crate) fn alive_keys(session: &zenoh::Session, key: &str) -> Result<Vec<String>> {
    let replies = session
        .liveliness()
        .get(key)
        .wait()
        .map_err(anyhow::Error::msg)
        .with_context(|| format!("liveliness get {key}"))?;
    let mut keys: Vec<String> = replies
        .iter()
        .filter_map(|r| r.result().ok().map(|s| s.key_expr().as_str().to_string()))
        .collect();
    keys.sort();
    keys.dedup();
    Ok(keys)
}

/// `@schema` を 1 発撃って、走っている publisher / server が名乗る descriptor set を集める。
/// 返りは `<base key>` → `(fqn, その message に刈った FileDescriptorSet)`。
///
/// `pattern` は `reiny/<domain>/*/*`(全型)や `reiny/<domain>/*/<TYPE>`(1 型)。
pub(crate) fn collect_schemas(
    session: &zenoh::Session,
    pattern: &str,
) -> BTreeMap<String, (String, Vec<u8>)> {
    let mut schemas = BTreeMap::new();
    let key = format!("{pattern}/{SCHEMA_CHUNK}/*");
    let replies = match session.get(&key).wait() {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "schema get failed");
            return schemas;
        }
    };
    for reply in &replies {
        let Ok(sample) = reply.result() else { continue };
        let full = sample.key_expr().as_str();
        // reiny/<domain>/<id>/<TYPE>/@schema/<fqn>
        let Some((base, fqn)) = full.split_once(&format!("/{SCHEMA_CHUNK}/")) else {
            continue;
        };
        let file_set = sample.payload().to_bytes();
        match reiny_build::descriptor_subset(&file_set, fqn) {
            Ok(Some(subset)) => {
                schemas.insert(base.to_string(), (fqn.to_string(), subset));
            }
            Ok(None) => tracing::warn!(fqn, "@schema payload lacks the named message"),
            Err(e) => tracing::warn!(fqn, error = %e, "undecodable @schema payload"),
        }
    }
    tracing::debug!(schemas = schemas.len(), "collected schemas");
    schemas
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn key_parts_parse_four_segments_only() {
        let p = KeyParts::parse("reiny/lab/ctrl/RobotState").unwrap();
        assert_eq!((p.domain, p.source, p.ty), ("lab", "ctrl", "RobotState"));
        assert!(KeyParts::parse("reiny/lab/ctrl").is_none());
        assert!(KeyParts::parse("reiny/lab/ctrl/RobotState/@schema/hs.RobotState").is_none());
        assert!(KeyParts::parse("other/lab/ctrl/RobotState").is_none());
    }

    #[test]
    fn key_parts_with_chunk() {
        let p = KeyParts::parse_with_chunk("reiny/lab/ctrl/Calib/@service", SERVICE_CHUNK).unwrap();
        assert_eq!((p.source, p.ty), ("ctrl", "Calib"));
        assert!(KeyParts::parse_with_chunk("reiny/lab/ctrl/Calib", SERVICE_CHUNK).is_none());
    }

    #[test]
    fn key_source_is_third_segment() {
        assert_eq!(key_source("reiny/lab/ctrl/RobotState"), "ctrl");
        assert_eq!(key_source("reiny/lab/ctrl/@grain"), "ctrl");
        assert_eq!(key_source("reiny"), "");
    }
}
