//! `reiny service list / call` — the equivalent of `ros2 service`.
//!
//! `list` fires `@service` presence and takes the response type from the second descriptor the server
//! announces at `@schema`. `call` takes the request / response descriptors from `@schema` and goes
//! JSON → proto → `get` (with a payload) → proto → JSON. That is what lets a service such as a
//! calibration or a physical reset be fired from a shell (a way to touch hardware without a GUI).

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};
use reiny::bindings::PortKind;
use reiny::zenoh::Wait;
use reiny::zenoh::query::ConsolidationMode;

use crate::bus::{
    BusArgs, KEY_ROOT, KeyParts, SERVICE_CHUNK, alive_keys, attachment_u64, collect_schemas_all,
    input_keys, ready_inputs, stream_report, topic_type, verify_fingerprint,
};
use crate::codec::Codec;

#[derive(Args)]
pub(crate) struct ServiceArgs {
    #[command(subcommand)]
    command: ServiceCommand,
}

#[derive(Subcommand)]
enum ServiceCommand {
    /// List the live services (request type → response type) and their servers' launch ids.
    List {
        #[command(flatten)]
        bus: BusArgs,
    },
    /// Call a service with JSON and print the reply as JSON.
    Call(CallArgs),
}

#[derive(Args)]
struct CallArgs {
    /// The declared uses endpoint name.
    #[arg(value_name = "USES")]
    ty: String,
    /// The request as JSON (the proto3 JSON mapping; `{}` when omitted).
    json: Option<String>,
    /// Require this server identity (it must match the uses endpoint's wired source).
    #[arg(long)]
    to: Option<String>,
    /// How many seconds to wait for the reply.
    #[arg(long, default_value_t = 10.0)]
    timeout: f64,
    #[command(flatten)]
    bus: BusArgs,
}

pub(crate) fn run(args: ServiceArgs) -> Result<()> {
    match args.command {
        ServiceCommand::List { bus } => list(&bus),
        ServiceCommand::Call(a) => call(&a),
    }
}

/// The (request fqn, response fqn) pair from the descriptors on one named server's key.
/// The request's full protobuf name matches the type segment; the response is the other.
type Named = (String, Vec<u8>);

fn split_schemas<'a>(ty: &str, named: &'a [Named]) -> (Option<&'a Named>, Option<&'a Named>) {
    let request = named.iter().find(|(fqn, _)| fqn == ty);
    let response = named
        .iter()
        .find(|(fqn, _)| request.is_none_or(|r| r.0 != *fqn));
    (request, response)
}

fn list(bus: &BusArgs) -> Result<()> {
    let (session, domain) = bus.open()?;
    let srvs = alive_keys(
        &session,
        &reiny::engine::Key::all(&domain)
            .with_chunk(SERVICE_CHUNK)
            .to_string(),
    )?;
    if srvs.is_empty() {
        println!("(no live servers in domain {domain})");
        return Ok(());
    }
    let schemas = collect_schemas_all(&session, &reiny::engine::Key::all(&domain).to_string());

    // type → (response fqn, servers)
    let mut rows: BTreeMap<String, (String, Vec<String>)> = BTreeMap::new();
    for k in &srvs {
        let Some(p) = KeyParts::parse_with_chunk(k, SERVICE_CHUNK) else {
            continue;
        };
        let base = format!(
            "{KEY_ROOT}/{}/{}/@out/{}/{}",
            p.domain, p.source, p.port, p.ty
        );
        let response = schemas
            .get(&base)
            .and_then(|named| split_schemas(p.ty, named).1)
            .map_or_else(|| "?".to_string(), |(fqn, _)| fqn.clone());
        let row = rows
            .entry(p.ty.to_string())
            .or_insert_with(|| (response.clone(), Vec::new()));
        if row.0 == "?" && response != "?" {
            row.0 = response;
        }
        row.1.push(format!("{}:{}", p.source, p.port));
    }
    let w_ty = rows.keys().map(String::len).max().unwrap_or(0).max(4);
    let w_resp = rows
        .values()
        .map(|(r, _)| r.len())
        .max()
        .unwrap_or(0)
        .max(8);
    println!("{:<w_ty$}  ->  {:<w_resp$}  SRV", "TYPE_NAME", "RESPONSE");
    for (ty, (resp, servers)) in &rows {
        println!("{ty:<w_ty$}  ->  {resp:<w_resp$}  {}", servers.join(" "));
    }
    Ok(())
}

fn call(args: &CallArgs) -> Result<()> {
    let bindings = args.bus.bindings()?;
    anyhow::ensure!(
        bindings.inputs.len() == 1 && bindings.outputs.is_empty(),
        "service call requires a contract containing only its named rpc_client endpoint"
    );
    let domain = args.bus.runtime_options("reiny-cli").domain;
    let (contract, keys) = input_keys(
        &bindings,
        &domain,
        &args.ty,
        PortKind::Rpc,
        args.to.as_deref(),
    )?;
    let key = &keys[0];
    let (session, _) = args.bus.open()?;

    // The request / response descriptors come from the `@schema` the server announces.
    let schemas = collect_schemas_all(&session, key);
    let named = schemas.values().next().with_context(|| {
        format!(
            "no server of `{}` describes its schema on the bus (is one running in domain {domain}, \
             and does its type carry a DESCRIPTOR?)",
            args.ty
        )
    })?;
    let (request, response) = split_schemas(topic_type(&contract), named);
    let (req_fqn, req_set) =
        request.with_context(|| format!("@schema of `{}` lacks the request message", args.ty))?;
    let (resp_fqn, resp_set) =
        response.with_context(|| format!("@schema of `{}` lacks the response message", args.ty))?;
    anyhow::ensure!(
        *req_fqn == contract.type_name && Some(resp_fqn.as_str()) == contract.response.as_deref(),
        "service descriptor types do not match the declared rpc_client contract"
    );
    let req_codec = Codec::from_file_set(req_set, req_fqn)?;
    let resp_codec = Codec::from_file_set(resp_set, resp_fqn)?;
    let mut report = stream_report(
        &contract,
        req_codec.fingerprint,
        &bindings.inputs[&args.ty].sources,
    );
    report.response = Some(reiny::bindings::ResponseReport {
        type_name: resp_fqn.clone(),
        schema: resp_codec.fingerprint,
    });
    let _ready = ready_inputs(
        &session,
        &domain,
        &bindings,
        BTreeMap::from([(args.ty.clone(), report)]),
        std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )?;

    let payload = req_codec.encode_json(args.json.as_deref().unwrap_or("{}"))?;
    let get = session
        .get(key)
        .payload(payload)
        .attachment(req_codec.fingerprint.to_le_bytes().to_vec())
        .timeout(Duration::from_secs_f64(args.timeout))
        .consolidation(ConsolidationMode::None);
    let replies = get.wait().map_err(anyhow::Error::msg).context("get")?;
    let Ok(reply) = replies.recv() else {
        bail!(
            "no reply from `{}` (no server, or it dropped the request)",
            args.ty
        );
    };
    match reply.result() {
        Ok(sample) => {
            anyhow::ensure!(
                sample.key_expr().as_str() == key,
                "service reply came from a different endpoint"
            );
            println!(
                "{}",
                decode_reply(
                    &resp_codec,
                    &sample.payload().to_bytes(),
                    attachment_u64(sample)
                )?
            );
            Ok(())
        }
        Err(e) => bail!(
            "server replied with error: {}",
            String::from_utf8_lossy(&e.payload().to_bytes())
        ),
    }
}

fn decode_reply(codec: &Codec, payload: &[u8], fingerprint: Option<u64>) -> Result<String> {
    verify_fingerprint(codec.fingerprint, fingerprint)?;
    codec.decode_json(payload)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)] // tests may fail by panicking
mod tests {
    use super::*;

    #[test]
    fn dynamic_reply_requires_the_response_schema_stamp() {
        let codec = Codec::from_file_set(&crate::codec::tests::probe_set(), "e2e.Probe").unwrap();
        assert!(decode_reply(&codec, &[], None).is_err());
        assert!(decode_reply(&codec, &[], Some(codec.fingerprint ^ 1)).is_err());
        assert!(decode_reply(&codec, &[], Some(codec.fingerprint)).is_ok());
    }

    fn named(fqns: &[&str]) -> Vec<Named> {
        fqns.iter().map(|f| ((*f).to_string(), vec![])).collect()
    }

    /// A server announces two descriptors on one key and they arrive in no particular order, so which
    /// is the request is decided by the key's type segment — never by position. Getting it backwards
    /// would encode the reply as the request.
    #[test]
    fn the_request_is_the_one_matching_the_type_segment() {
        for order in [["calc.Add", "calc.Sum"], ["calc.Sum", "calc.Add"]] {
            let schemas = named(&order);
            let (request, response) = split_schemas("calc.Add", &schemas);
            assert_eq!(request.map(|r| r.0.as_str()), Some("calc.Add"), "{order:?}");
            assert_eq!(
                response.map(|r| r.0.as_str()),
                Some("calc.Sum"),
                "{order:?}"
            );
        }
    }

    /// The complete package name participates in the request selection.
    #[test]
    fn the_complete_package_name_selects_the_request() {
        let schemas = named(&["some.deep.package.Add", "other.Sum"]);
        let (request, response) = split_schemas("some.deep.package.Add", &schemas);
        assert_eq!(request.map(|r| r.0.as_str()), Some("some.deep.package.Add"));
        assert_eq!(response.map(|r| r.0.as_str()), Some("other.Sum"));
    }

    /// A server announcing only its request type leaves no response to decode with — that has to be
    /// `None`, not the request itself.
    #[test]
    fn a_lone_request_has_no_response() {
        let schemas = named(&["calc.Add"]);
        let (request, response) = split_schemas("calc.Add", &schemas);
        assert_eq!(request.map(|r| r.0.as_str()), Some("calc.Add"));
        assert!(response.is_none());
    }

    /// With nothing matching the type segment there is no request, and the first entry is offered as
    /// the response — the fallback that keeps a `list` readable when a server announces an unexpected name.
    #[test]
    fn without_a_match_the_first_entry_is_the_response() {
        let schemas = named(&["calc.Other", "calc.Sum"]);
        let (request, response) = split_schemas("calc.Add", &schemas);
        assert!(request.is_none());
        assert_eq!(response.map(|r| r.0.as_str()), Some("calc.Other"));

        let (request, response) = split_schemas("calc.Add", &[]);
        assert!(request.is_none());
        assert!(response.is_none());
    }
}
