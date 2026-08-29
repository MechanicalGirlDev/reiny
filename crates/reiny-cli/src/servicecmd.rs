//! `reiny service list / call` —— `ros2 service` 相当。
//!
//! `list` は `@service` の presence を撃ち、response 型は server が `@schema` で名乗る 2 本目の
//! descriptor から取る。`call` は `@schema` で request / response の descriptor を取り、
//! JSON → proto → `get`(payload 付き)→ proto → JSON と往復する。これで校正・物理リセットの
//! ような service がシェルから撃てる(GUI を上げずに実機を触る道)。

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};
use reiny::zenoh::Wait;
use reiny::zenoh::query::ConsolidationMode;

use crate::bus::{BusArgs, KEY_ROOT, KeyParts, SERVICE_CHUNK, alive_keys, collect_schemas_all};
use crate::codec::Codec;

#[derive(Args)]
pub(crate) struct ServiceArgs {
    #[command(subcommand)]
    command: ServiceCommand,
}

#[derive(Subcommand)]
enum ServiceCommand {
    /// 生きている service(request 型 → response 型)と server の launch id を並べる。
    List {
        #[command(flatten)]
        bus: BusArgs,
    },
    /// service を JSON で呼び、応答を JSON で出す。
    Call(CallArgs),
}

#[derive(Args)]
struct CallArgs {
    /// request 型名(キーの型セグメント。例 `CalibrationCommand`)。
    ty: String,
    /// request の JSON(proto3 JSON mapping。省略時は `{}`)。
    json: Option<String>,
    /// この launch id の server に撃つ(既定は同 domain の任意の server、最初の応答)。
    #[arg(long)]
    to: Option<String>,
    /// 応答を待つ秒数。
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

/// server のキー `reiny/<d>/<id>/<Req>` が名乗る descriptor 群から (request fqn, response fqn)。
/// request は短い名前が型セグメントに一致するもの、response はもう片方。
type Named = (String, Vec<u8>);

fn split_schemas<'a>(ty: &str, named: &'a [Named]) -> (Option<&'a Named>, Option<&'a Named>) {
    let request = named
        .iter()
        .find(|(fqn, _)| fqn.rsplit('.').next() == Some(ty));
    let response = named
        .iter()
        .find(|(fqn, _)| request.is_none_or(|r| r.0 != *fqn));
    (request, response)
}

fn list(bus: &BusArgs) -> Result<()> {
    let (session, domain) = bus.open()?;
    let srvs = alive_keys(
        &session,
        &format!("{KEY_ROOT}/{domain}/*/*/{SERVICE_CHUNK}"),
    )?;
    if srvs.is_empty() {
        println!("(no live servers in domain {domain})");
        return Ok(());
    }
    let schemas = collect_schemas_all(&session, &format!("{KEY_ROOT}/{domain}/*/*"));

    // 型 → (response fqn, servers)
    let mut rows: BTreeMap<String, (String, Vec<String>)> = BTreeMap::new();
    for k in &srvs {
        let Some(p) = KeyParts::parse_with_chunk(k, SERVICE_CHUNK) else {
            continue;
        };
        let base = format!("{KEY_ROOT}/{}/{}/{}", p.domain, p.source, p.ty);
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
        row.1.push(p.source.to_string());
    }
    let w_ty = rows.keys().map(String::len).max().unwrap_or(0).max(4);
    let w_resp = rows
        .values()
        .map(|(r, _)| r.len())
        .max()
        .unwrap_or(0)
        .max(8);
    println!("{:<w_ty$}  ->  {:<w_resp$}  SRV", "TYPE", "RESPONSE");
    for (ty, (resp, servers)) in &rows {
        println!("{ty:<w_ty$}  ->  {resp:<w_resp$}  {}", servers.join(" "));
    }
    Ok(())
}

fn call(args: &CallArgs) -> Result<()> {
    let (session, domain) = args.bus.open()?;
    let to = args.to.as_deref().unwrap_or("*");
    let key = format!("{KEY_ROOT}/{domain}/{to}/{}", args.ty);

    // request / response の descriptor は server が名乗る `@schema` から取る。
    let schemas = collect_schemas_all(&session, &key);
    let named = schemas.values().next().with_context(|| {
        format!(
            "no server of `{}` describes its schema on the bus (is one running in domain {domain}, \
             and does its type carry a DESCRIPTOR?)",
            args.ty
        )
    })?;
    let (request, response) = split_schemas(&args.ty, named);
    let (req_fqn, req_set) =
        request.with_context(|| format!("@schema of `{}` lacks the request message", args.ty))?;
    let (resp_fqn, resp_set) =
        response.with_context(|| format!("@schema of `{}` lacks the response message", args.ty))?;
    let req_codec = Codec::from_file_set(req_set, req_fqn)?;
    let resp_codec = Codec::from_file_set(resp_set, resp_fqn)?;

    let payload = req_codec.encode_json(args.json.as_deref().unwrap_or("{}"))?;
    let mut get = session
        .get(&key)
        .payload(payload)
        .timeout(Duration::from_secs_f64(args.timeout))
        .consolidation(ConsolidationMode::None);
    if let Some(fp) = req_codec.fingerprint {
        get = get.attachment(fp.to_le_bytes().to_vec());
    }
    let replies = get.wait().map_err(anyhow::Error::msg).context("get")?;
    let Ok(reply) = replies.recv() else {
        bail!(
            "no reply from `{}` (no server, or it dropped the request)",
            args.ty
        );
    };
    match reply.result() {
        Ok(sample) => {
            println!("{}", resp_codec.decode_json(&sample.payload().to_bytes())?);
            Ok(())
        }
        Err(e) => bail!(
            "server replied with error: {}",
            String::from_utf8_lossy(&e.payload().to_bytes())
        ),
    }
}
