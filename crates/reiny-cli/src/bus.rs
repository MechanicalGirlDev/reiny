//! The bus vocabulary `reiny bag` / `reiny topic` / `reiny node` / `reiny service` share.
//!
//! The key shape (`reiny/<domain>/<id>/<TYPE>` plus the side chunks `@schema` / `@service` /
//! `@launch`), building a session by the same path a launch does, and collecting the descriptors a
//! running publisher / server announces. Only what cannot be written without knowing reiny's and
//! zenoh's conventions lives here; the statistics and the printing belong to each subcommand.
//!
//! Synchronous. zenoh's `.wait()` is enough and tokio is not needed (nor is it anywhere else in the CLI).

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;
use reiny::zenoh::{self, Wait};
use reiny::{RuntimeOptions, ZenohSource};

/// The key prefix and the side chunk names. Kept in step with reiny proper (verbatim, so `*` never sees them).
pub(crate) const KEY_ROOT: &str = "reiny";
pub(crate) const SCHEMA_CHUNK: &str = "@schema";
pub(crate) const SERVICE_CHUNK: &str = "@service";
pub(crate) const SUB_CHUNK: &str = "@sub";
pub(crate) const LAUNCH_CHUNK: &str = "@launch";

/// The fabric arguments, spelled as a launch spells them. Shared by every subcommand.
#[derive(Args, Clone)]
pub(crate) struct BusArgs {
    /// The logical namespace (default: `--domain` > `REINY_DOMAIN` > "default").
    #[arg(long)]
    pub(crate) domain: Option<String>,
    /// A zenoh configuration file (JSON5 / JSON / YAML).
    #[arg(long)]
    pub(crate) zenoh_config: Option<PathBuf>,
    /// An endpoint to connect to (repeatable, e.g. `tcp/127.0.0.1:7447`).
    #[arg(long)]
    pub(crate) connect: Vec<String>,
    /// zenoh's mode (`peer` / `client` / `router`).
    #[arg(long)]
    pub(crate) zenoh_mode: Option<String>,
}

impl BusArgs {
    /// Map the fabric arguments onto the same `RuntimeOptions` a launch uses (so the defaults,
    /// `REINY_DOMAIN` and the json5-ification of `--connect` all take a launch's path). tracing is off:
    pub(crate) fn runtime_options(&self, id: &str) -> RuntimeOptions {
        let mut opts = RuntimeOptions::new(id);
        opts.install_tracing = false;
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
        opts
    }

    /// Build a (zenoh session, resolved domain) pair from the fabric arguments.
    pub(crate) fn open(&self) -> Result<(zenoh::Session, String)> {
        let opts = self.runtime_options("reiny-cli");
        let config = opts.zenoh_config()?;
        let session = zenoh::open(config)
            .wait()
            .map_err(anyhow::Error::msg)
            .context("opening zenoh session")?;
        Ok((session, opts.domain))
    }
}

/// The three segments of a key `reiny/<domain>/<source>/<TYPE>`. A side chunk (`/@…`) is rejected (None).
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
        // Exactly four segments (with a side chunk `/@schema/...` it is not a type's topic).
        if segs.next().is_some() {
            return None;
        }
        Some(Self { domain, source, ty })
    }

    /// The form `reiny/<domain>/<source>/<TYPE>/<chunk>` (with a side chunk). `chunk` is `@service` and the like.
    pub(crate) fn parse_with_chunk(key: &'a str, chunk: &str) -> Option<Self> {
        let base = key.strip_suffix(chunk)?.strip_suffix('/')?;
        Self::parse(base)
    }
}

/// Take `<id>` out of `reiny/<domain>/<id>/…` (for reading presence).
pub(crate) fn key_source(key: &str) -> &str {
    key.split('/').nth(2).unwrap_or_default()
}

/// Read a zenoh attachment as reiny's fingerprint (8 bytes LE). `None` when the shape differs.
pub(crate) fn attachment_u64(sample: &zenoh::sample::Sample) -> Option<u64> {
    let bytes = sample.attachment()?.to_bytes();
    <[u8; 8]>::try_from(bytes.as_ref())
        .ok()
        .map(u64::from_le_bytes)
}

/// The keys of the liveliness tokens alive on `key` (sorted, deduplicated).
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

/// The subset of [`collect_schemas_all`] holding each key's **own type**'s descriptor (the one whose
/// short fqn matches the key's type segment, else the first). Enough for a pub/sub type.
pub(crate) fn collect_schemas(
    session: &zenoh::Session,
    pattern: &str,
) -> BTreeMap<String, (String, Vec<u8>)> {
    collect_schemas_all(session, pattern)
        .into_iter()
        .filter_map(|(base, named)| {
            let ty = KeyParts::parse(&base).map(|p| p.ty.to_string());
            let own = named
                .iter()
                .position(|(fqn, _)| fqn.rsplit('.').next() == ty.as_deref())
                .unwrap_or(0);
            named.into_iter().nth(own).map(|n| (base, n))
        })
        .collect()
}

/// Fire one `@schema` query and collect the descriptor sets running publishers / servers announce.
/// The result is `<base key>` → `[(fqn, the FileDescriptorSet pruned to that message)]` (a service's
/// key announces two: the request and the response).
///
/// `pattern` is `reiny/<domain>/*/*` (every type) or `reiny/<domain>/*/<TYPE>` (one).
pub(crate) fn collect_schemas_all(
    session: &zenoh::Session,
    pattern: &str,
) -> BTreeMap<String, Vec<(String, Vec<u8>)>> {
    let mut schemas: BTreeMap<String, Vec<(String, Vec<u8>)>> = BTreeMap::new();
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
                schemas
                    .entry(base.to_string())
                    .or_default()
                    .push((fqn.to_string(), subset));
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
        assert_eq!(key_source("reiny/lab/ctrl/@launch"), "ctrl");
        assert_eq!(key_source("reiny"), "");
    }
}
