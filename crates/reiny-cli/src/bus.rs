//! The bus vocabulary `reiny bag` / `reiny topic` / `reiny node` / `reiny service` share.
//!
//! The key shape (`reiny/<domain>/<id>/<TYPE>` plus the side chunks `@schema` / `@service` /
//! `@launch`), building a session by the same path a launch does, and collecting the descriptors a
//! running publisher / server announces. Only what cannot be written without knowing reiny's and
//! zenoh's conventions lives here; the statistics and the printing belong to each subcommand.
//!
//! Bus calls are synchronous via zenoh's `.wait()`; deployment and bridge adapters use their own runtimes.

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Context, Result, ensure};
use clap::Args;
use reiny::bindings::{
    CONTRACT_VERSION, EndpointContract, ModuleBindings, ModuleReport, PortKind, PortReport,
    ReceiveBuffer, Replay,
};
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
    /// Runtime identity supplied by the deployment owner.
    #[arg(long = "id", alias = "name", global = true)]
    pub(crate) runtime_id: Option<String>,
    /// Application configuration supplied by the deployment owner.
    #[arg(long, global = true)]
    pub(crate) config: Option<PathBuf>,
    /// Prepared named endpoint contract resolved from main.yaml.
    #[arg(long, global = true)]
    pub(crate) module_bindings: Option<PathBuf>,
    /// The logical namespace (default: `--domain` > `REINY_DOMAIN` > "default").
    #[arg(long, global = true)]
    pub(crate) domain: Option<String>,
    /// A zenoh configuration file (JSON5 / JSON / YAML).
    #[arg(long, global = true)]
    pub(crate) zenoh_config: Option<PathBuf>,
    /// An endpoint to connect to (repeatable, e.g. `tcp/127.0.0.1:7447`).
    #[arg(long, global = true)]
    pub(crate) connect: Vec<String>,
    /// zenoh's mode (`peer` / `client` / `router`).
    #[arg(long, global = true)]
    pub(crate) zenoh_mode: Option<String>,
}

impl BusArgs {
    pub(crate) fn context(
        &self,
        session: zenoh::Session,
        bindings: ModuleBindings,
    ) -> Result<(tokio::runtime::Runtime, std::sync::Arc<reiny::Cloudy>)> {
        let runtime = tokio::runtime::Runtime::new()?;
        let mut options = self.runtime_options(&bindings.namespace);
        options.module_bindings = Some(bindings);
        options.engine = Some(std::sync::Arc::new(reiny::engine::Zenoh::from_session(
            session,
        )));
        let cloudy = runtime.block_on(reiny::Cloudy::open(options))?;
        Ok((runtime, std::sync::Arc::new(cloudy)))
    }

    /// Data commands never open a session before validating their complete prepared contract.
    pub(crate) fn bindings(&self) -> Result<ModuleBindings> {
        let path = self.module_bindings.as_ref().context(
            "data operations require --module-bindings (a prepared main.yaml endpoint contract)",
        )?;
        let bindings: ModuleBindings = serde_json::from_slice(
            &std::fs::read(path).with_context(|| format!("reading {}", path.display()))?,
        )
        .with_context(|| format!("parsing {}", path.display()))?;
        validate_bindings(&bindings, &self.runtime_options("reiny-cli").domain)?;
        ensure!(
            self.runtime_id
                .as_deref()
                .is_none_or(|id| id == bindings.namespace),
            "--id must match the bindings namespace"
        );
        Ok(bindings)
    }

    /// Map the fabric arguments onto the same `RuntimeOptions` a launch uses (so the defaults,
    /// `REINY_DOMAIN` and the json5-ification of `--connect` all take a launch's path). tracing is off:
    pub(crate) fn runtime_options(&self, id: &str) -> RuntimeOptions {
        let mut opts = RuntimeOptions::new(id);
        opts.install_tracing = false;
        opts.config_path.clone_from(&self.config);
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

/// Direct dynamic inputs/callers report only ports actually validated and opened by this command.
pub(crate) fn ready_inputs(
    session: &zenoh::Session,
    domain: &str,
    bindings: &ModuleBindings,
    inputs: BTreeMap<String, PortReport>,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> Result<(
    zenoh::liveliness::LivelinessToken,
    zenoh::query::Queryable<()>,
)> {
    ensure!(
        bindings.outputs.is_empty() && inputs.len() == bindings.inputs.len(),
        "CLI command must implement every endpoint in its prepared contract"
    );
    for (name, report) in &inputs {
        ensure!(
            bindings
                .inputs
                .get(name)
                .is_some_and(|input| input.contract == report.contract),
            "runtime report differs from endpoint contract"
        );
    }
    let report = ModuleReport {
        version: CONTRACT_VERSION,
        namespace: bindings.namespace.clone(),
        inputs,
        outputs: BTreeMap::new(),
    };
    if let Some(path) = std::env::var_os("REINY_MODULE_REPORT") {
        let path = PathBuf::from(path);
        ensure!(
            path.is_absolute(),
            "REINY_MODULE_REPORT requires an absolute path"
        );
        let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
        std::fs::write(&temporary, serde_json::to_vec(&report)?)?;
        std::fs::rename(temporary, path)?;
    }
    let stop_key = format!(
        "{KEY_ROOT}/{domain}/{}/{}",
        bindings.namespace,
        reiny::engine::STOP_CHUNK
    );
    let reply_key = stop_key.clone();
    let control = session
        .declare_queryable(stop_key)
        .callback(move |query| {
            stop.store(true, std::sync::atomic::Ordering::SeqCst);
            let _ = query.reply(reply_key.clone(), b"ack".to_vec()).wait();
        })
        .wait()
        .map_err(anyhow::Error::msg)?;
    let token = session
        .liveliness()
        .declare_token(format!(
            "{KEY_ROOT}/{domain}/{}/{}",
            bindings.namespace,
            reiny::engine::READY_CHUNK,
        ))
        .wait()
        .map_err(anyhow::Error::msg)?;
    Ok((token, control))
}

pub(crate) fn stream_report(contract: &EndpointContract, schema: u64) -> PortReport {
    PortReport {
        type_name: contract.type_name.clone(),
        schema,
        contract: contract.clone(),
        response: None,
    }
}

fn segment(value: &str) -> bool {
    !value.is_empty()
        && !value.contains(['/', '*', '?', '#', '$', '@'])
        && !value.contains(char::is_whitespace)
}

pub(crate) fn validate_bindings(bindings: &ModuleBindings, domain: &str) -> Result<()> {
    ensure!(segment(domain), "data domain must be one exact key segment");
    bindings.validate()?;
    ensure!(
        bindings.children.is_empty(),
        "CLI bindings cannot delegate endpoints to owned children"
    );
    Ok(())
}

pub(crate) fn topic_type(contract: &EndpointContract) -> &str {
    contract
        .type_name
        .rsplit('.')
        .next()
        .unwrap_or(&contract.type_name)
}

pub(crate) fn contract_schema(
    session: &zenoh::Session,
    key: &str,
    contract: &EndpointContract,
) -> Result<(String, Vec<u8>)> {
    let (fqn, set) = collect_schemas(session, key)
        .remove(key)
        .with_context(|| format!("no descriptor for declared endpoint {key}"))?;
    ensure!(
        fqn == contract.type_name,
        "descriptor type differs from endpoint contract for {key}"
    );
    Ok((fqn, set))
}

pub(crate) fn schema_fingerprint(set: &[u8], fqn: &str) -> Result<u64> {
    reiny_build::message_fingerprint(set, fqn)?
        .with_context(|| format!("descriptor set lacks the schema fingerprint for {fqn}"))
}

pub(crate) fn verify_fingerprint(expected: u64, received: Option<u64>) -> Result<()> {
    ensure!(
        received == Some(expected),
        "missing or mismatched schema fingerprint"
    );
    Ok(())
}

pub(crate) fn input_policy(contract: &EndpointContract, snapshot: bool) -> Result<()> {
    ensure!(
        contract.buffer == ReceiveBuffer::Fifo,
        "CLI input requires fifo buffering; latest buffering is unsupported"
    );
    ensure!(
        snapshot || contract.replay == Replay::Live,
        "CLI input replay:last is unsupported for this operation"
    );
    Ok(())
}

pub(crate) fn input_keys(
    bindings: &ModuleBindings,
    domain: &str,
    name: &str,
    kind: PortKind,
    source: Option<&str>,
) -> Result<(EndpointContract, Vec<String>)> {
    let port = bindings
        .inputs
        .get(name)
        .with_context(|| format!("undeclared input/uses endpoint {name:?}"))?;
    ensure!(
        port.contract.kind == kind,
        "wrong operation kind for input endpoint {name}"
    );
    ensure!(
        source.is_none_or(|s| port.sources.iter().any(|declared| declared == s)),
        "source is not declared for endpoint {name}"
    );
    let keys = port
        .sources
        .iter()
        .filter(|s| source.is_none_or(|wanted| wanted == s.as_str()))
        .map(|source| {
            let key = reiny::engine::Key::topic(domain, Some(source), topic_type(&port.contract));
            key.validate()?;
            Ok(key.to_string())
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((port.contract.clone(), keys))
}

pub(crate) fn output_key(
    bindings: &ModuleBindings,
    domain: &str,
    name: &str,
    kind: PortKind,
    source: Option<&str>,
) -> Result<(EndpointContract, String)> {
    let port = bindings
        .outputs
        .get(name)
        .with_context(|| format!("undeclared output/provides endpoint {name:?}"))?;
    ensure!(
        port.contract.kind == kind,
        "wrong operation kind for output endpoint {name}"
    );
    ensure!(
        source.is_none_or(|s| s == bindings.endpoint_namespace),
        "--as must match the contract endpoint namespace"
    );
    let key = reiny::engine::Key::topic(
        domain,
        Some(&bindings.endpoint_namespace),
        topic_type(&port.contract),
    );
    key.validate()?;
    Ok((port.contract.clone(), key.to_string()))
}

/// The domain, hierarchical source and type of a topic. Side chunks are rejected.
pub(crate) struct KeyParts<'a> {
    pub(crate) domain: &'a str,
    pub(crate) source: &'a str,
    pub(crate) ty: &'a str,
}

impl<'a> KeyParts<'a> {
    pub(crate) fn parse(key: &'a str) -> Option<Self> {
        let rest = key.strip_prefix("reiny/")?;
        let (domain, rest) = rest.split_once('/')?;
        let (source, ty) = rest.rsplit_once('/')?;
        if domain.is_empty()
            || source
                .split('/')
                .any(|part| part.is_empty() || part.starts_with('@'))
            || ty.is_empty()
            || (ty.starts_with('@')
                && !matches!(
                    ty,
                    LAUNCH_CHUNK | reiny::engine::READY_CHUNK | reiny::engine::STOP_CHUNK
                ))
        {
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

/// Take the complete source namespace from a topic or presence key.
pub(crate) fn key_source(key: &str) -> &str {
    KeyParts::parse(key)
        .or_else(|| {
            key.split_once("/@")
                .and_then(|(base, _)| KeyParts::parse(base))
        })
        .map_or("", |parts| parts.source)
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
/// `pattern` is `reiny/<domain>/*/**` (every type) or `reiny/<domain>/**/<TYPE>` (one).
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
    use reiny::bindings::Retention;

    #[test]
    fn dynamic_samples_require_the_exact_descriptor_fingerprint() {
        assert!(verify_fingerprint(42, None).is_err());
        assert!(verify_fingerprint(42, Some(41)).is_err());
        verify_fingerprint(42, Some(42)).unwrap();
        assert!(schema_fingerprint(b"not a descriptor", "test.Probe").is_err());
    }

    #[test]
    fn unsupported_input_policies_are_refused_explicitly() {
        let mut contract = EndpointContract {
            type_name: "test.Probe".into(),
            ..Default::default()
        };
        input_policy(&contract, false).unwrap();
        contract.buffer = ReceiveBuffer::Latest(1);
        assert!(input_policy(&contract, true).is_err());
        contract.buffer = ReceiveBuffer::Fifo;
        contract.replay = Replay::Last;
        assert!(input_policy(&contract, false).is_err());
        input_policy(&contract, true).unwrap();
    }

    fn bindings() -> ModuleBindings {
        serde_yaml::from_str(
            "version: 2\nnamespace: tools/cli\nendpoint_namespace: tools/cli\ninputs:\n  state:\n    contract: {type: robot.State}\n    sources: [robot/control, robot/simulation]\n  calibrate:\n    contract: {type: robot.Calibrate, kind: rpc, response: robot.Result}\n    sources: [robot/control]\noutputs:\n  command:\n    contract: {type: robot.Command}\n",
        ).unwrap()
    }

    #[test]
    fn named_endpoints_resolve_only_exact_declared_addresses() {
        let bindings = bindings();
        validate_bindings(&bindings, "lab").unwrap();
        let (contract, keys) =
            input_keys(&bindings, "lab", "state", PortKind::Stream, None).unwrap();
        assert_eq!(contract.type_name, "robot.State");
        assert_eq!(
            keys,
            [
                "reiny/lab/robot/control/State",
                "reiny/lab/robot/simulation/State"
            ]
        );
        let (_, key) = output_key(&bindings, "lab", "command", PortKind::Stream, None).unwrap();
        assert_eq!(key, "reiny/lab/tools/cli/Command");
        assert!(input_keys(&bindings, "lab", "State", PortKind::Stream, None).is_err());
        assert!(input_keys(&bindings, "lab", "state", PortKind::Stream, Some("**")).is_err());
        assert!(input_keys(&bindings, "lab", "calibrate", PortKind::Stream, None).is_err());
        assert!(output_key(&bindings, "lab", "state", PortKind::Stream, None).is_err());
        assert!(output_key(&bindings, "lab", "command", PortKind::Stream, Some("other")).is_err());
    }

    #[test]
    fn owned_cli_children_keep_the_declared_host_endpoint_address() {
        let mut bindings = bindings();
        bindings.namespace = "tools/cli/owned/inspector".into();
        validate_bindings(&bindings, "lab").unwrap();
        let (_, key) = output_key(&bindings, "lab", "command", PortKind::Stream, None).unwrap();
        assert_eq!(key, "reiny/lab/tools/cli/Command");
    }

    #[test]
    fn invalid_contracts_are_refused_before_transport_operations() {
        for change in 0..10 {
            let mut bindings = bindings();
            match change {
                0 => bindings.version = 1,
                1 => bindings.endpoint_namespace = "other".into(),
                2 => bindings.inputs.get_mut("state").unwrap().sources = vec!["**".into()],
                3 => bindings.inputs.get_mut("state").unwrap().sources.clear(),
                4 => bindings
                    .inputs
                    .get_mut("calibrate")
                    .unwrap()
                    .sources
                    .push("other".into()),
                5 => {
                    bindings
                        .outputs
                        .get_mut("command")
                        .unwrap()
                        .contract
                        .type_name = "*".into();
                }
                6 => bindings.inputs.get_mut("state").unwrap().contract.retention = Retention::Last,
                7 => {
                    bindings
                        .inputs
                        .get_mut("calibrate")
                        .unwrap()
                        .contract
                        .response = None;
                }
                8 => bindings.namespace = "tools/../cli".into(),
                _ => {
                    let duplicate = bindings.outputs["command"].clone();
                    bindings.outputs.insert("duplicate".into(), duplicate);
                }
            }
            assert!(
                validate_bindings(&bindings, "lab").is_err(),
                "mutation {change}"
            );
        }
        assert!(validate_bindings(&bindings(), "*").is_err());
    }

    #[test]
    fn key_parts_parse_hierarchical_sources_without_side_chunks() {
        let p = KeyParts::parse("reiny/lab/ctrl/RobotState").unwrap();
        assert_eq!((p.domain, p.source, p.ty), ("lab", "ctrl", "RobotState"));
        assert!(KeyParts::parse("reiny/lab/ctrl").is_none());
        assert!(KeyParts::parse("reiny/lab/ctrl/RobotState/@schema/hs.RobotState").is_none());
        assert!(KeyParts::parse("other/lab/ctrl/RobotState").is_none());
        let nested = KeyParts::parse("reiny/lab/robot/control/RobotState").unwrap();
        assert_eq!(
            (nested.domain, nested.source, nested.ty),
            ("lab", "robot/control", "RobotState")
        );
        assert!(
            KeyParts::parse("reiny/lab/robot/control/RobotState/@schema/hs.RobotState").is_none()
        );
    }

    #[test]
    fn key_parts_with_chunk() {
        let p = KeyParts::parse_with_chunk("reiny/lab/ctrl/Calib/@service", SERVICE_CHUNK).unwrap();
        assert_eq!((p.source, p.ty), ("ctrl", "Calib"));
        assert!(KeyParts::parse_with_chunk("reiny/lab/ctrl/Calib", SERVICE_CHUNK).is_none());
    }

    #[test]
    fn key_source_preserves_module_path() {
        assert_eq!(key_source("reiny/lab/ctrl/RobotState"), "ctrl");
        assert_eq!(key_source("reiny/lab/ctrl/@launch"), "ctrl");
        assert_eq!(key_source("reiny"), "");
        assert_eq!(
            key_source("reiny/lab/robot/control/@launch"),
            "robot/control"
        );
        assert_eq!(
            key_source("reiny/lab/robot/control/Calib/@service"),
            "robot/control"
        );
    }
}
