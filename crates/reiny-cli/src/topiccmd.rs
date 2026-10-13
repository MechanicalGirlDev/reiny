//! `reiny topic list / hz / bw / echo / pub` and `reiny node list / info` — the equivalents of
//! `ros2 topic` / `ros2 node`.
//!
//! The way to look at the bus without bringing up a GUI. `list` / `node` only fire liveliness
//! (presence); `hz` / `bw` subscribe only to the declared input's exact sources and aggregate them
//! per source. The clock is the receiver's `Instant` (zenoh's timestamping is off by default).
//!
//! `list` counts three roles, one liveliness query each: publishers sit on the type's own key,
//! subscribers on `…/@sub` and servers on `…/@service`. `echo` and `pub` are the two directions of
//! the same trick — decode / encode JSON through the descriptor a running launch serves at `@schema`.

#![allow(clippy::cast_precision_loss)] // a count → f64 is for the statistics display only.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};
use reiny::bindings::PortKind;
use reiny::zenoh;

use crate::bus::{
    BoundSubscription, BusArgs, KEY_ROOT, KeyParts, LAUNCH_CHUNK, SERVICE_CHUNK, SUB_CHUNK,
    alive_keys, attachment_u64, collect_schemas, contract_schema, input_keys, input_policy,
    key_source, output_key, ready_inputs, schema_fingerprint, stream_report, topic_type,
    verify_fingerprint,
};
use crate::codec::{Codec, hex};

// ===========================================================================
// reiny topic
// ===========================================================================

#[derive(Args)]
pub(crate) struct TopicArgs {
    #[command(subcommand)]
    command: TopicCommand,
}

#[derive(Subcommand)]
enum TopicCommand {
    /// For each live type, list the launch ids of its publishers / subscribers / servers.
    List(ListArgs),
    /// One type's receive rate (per source). Until Ctrl+C or `--duration`.
    Hz(RateArgs),
    /// One type's receive bandwidth (per source). Until Ctrl+C or `--duration`.
    Bw(RateArgs),
    /// Stream one type's contents as JSON (decoded with the descriptor its publisher announces at `@schema`).
    Echo(EchoArgs),
    /// Publish one type from JSON (encoded with the `@schema` on the bus).
    Pub(PubArgs),
}

#[derive(Args)]
struct PubArgs {
    /// The declared output endpoint name.
    #[arg(value_name = "OUTPUT")]
    ty: String,
    /// The message as JSON (the proto3 JSON mapping; `{}` when omitted).
    json: Option<String>,
    /// The source id to announce (the key's `<id>` segment).
    #[arg(long = "as")]
    as_id: Option<String>,
    /// Send this many times per second (by default it sends once and stops; without `--count`, until Ctrl+C).
    #[arg(long)]
    rate: Option<f64>,
    /// Stop after this many messages (with 2 or more and no `--rate`, it ticks at 1 Hz).
    #[arg(long)]
    count: Option<usize>,
    #[command(flatten)]
    bus: BusArgs,
}

#[derive(Args)]
struct EchoArgs {
    /// The declared input endpoint name.
    #[arg(value_name = "INPUT")]
    ty: String,
    /// Only what comes from this launch id.
    #[arg(long)]
    from: Option<String>,
    /// Stop after this many messages.
    #[arg(long)]
    count: Option<usize>,
    /// Stop after this many seconds.
    #[arg(long)]
    duration: Option<f64>,
    /// Print the payload as hex instead of decoding it.
    #[arg(long)]
    raw: bool,
    #[command(flatten)]
    bus: BusArgs,
}

#[derive(Args)]
struct ListArgs {
    #[command(flatten)]
    bus: BusArgs,
}

#[derive(Args)]
struct RateArgs {
    /// The declared input endpoint name.
    #[arg(value_name = "INPUT")]
    ty: String,
    /// Count only this declared source (default: all wired sources, aggregated separately).
    #[arg(long)]
    from: Option<String>,
    /// The statistics window (in seconds).
    #[arg(long, default_value_t = 10.0)]
    window: f64,
    /// Stop after this many seconds (default: at Ctrl+C).
    #[arg(long)]
    duration: Option<f64>,
    #[command(flatten)]
    bus: BusArgs,
}

pub(crate) fn run_topic(args: TopicArgs) -> Result<()> {
    match args.command {
        TopicCommand::List(a) => list(&a),
        TopicCommand::Hz(a) => rate(&a, Mode::Hz),
        TopicCommand::Bw(a) => rate(&a, Mode::Bw),
        TopicCommand::Echo(a) => echo(&a),
        TopicCommand::Pub(a) => publish(&a),
    }
}

/// `reiny topic pub <OUTPUT> [JSON]` — publish through a declared output.
///
/// The descriptor comes off the bus (`@schema`), and **a subscriber announces it too**, so a launch
/// that only listens — the usual target during bring-up, when its real publisher is exactly the thing
/// not running yet — can still be poked.
fn publish(args: &PubArgs) -> Result<()> {
    if args.rate.is_some_and(|r| r <= 0.0) {
        bail!("--rate must be positive");
    }
    let bindings = args.bus.bindings()?;
    anyhow::ensure!(
        bindings.inputs.is_empty() && bindings.outputs.len() == 1,
        "topic pub requires a contract containing only its named output"
    );
    let domain = args.bus.runtime_options("reiny-cli").domain;
    let (contract, key) = output_key(
        &bindings,
        &domain,
        &args.ty,
        PortKind::Stream,
        args.as_id.as_deref(),
    )?;
    let (session, _) = args.bus.open()?;

    let pattern = format!("{KEY_ROOT}/{domain}/**/{}", topic_type(&contract));
    let (fqn, file_set) = collect_schemas(&session, &pattern)
        .into_values()
        .find(|(fqn, _)| *fqn == contract.type_name)
        .with_context(|| {
            format!(
                "nothing on the bus describes `{}` (is a launch that publishes or subscribes to it \
                 running in domain {domain}, and does its type carry a DESCRIPTOR?)",
                args.ty
            )
        })?;
    let codec = Codec::from_file_set(&file_set, &fqn)?;
    let payload = codec.encode_json(args.json.as_deref().unwrap_or("{}"))?;

    let (runtime, cloudy) = args.bus.context(session, bindings)?;
    let publisher = cloudy.raw_output(&args.ty, &fqn, codec.fingerprint)?;
    cloudy.ready()?;

    let stop = Arc::new(AtomicBool::new(false));
    {
        let cloudy = Arc::clone(&cloudy);
        let stop = Arc::clone(&stop);
        runtime.spawn(async move {
            cloudy.shutdown().await;
            stop.store(true, Ordering::SeqCst);
        });
    }
    {
        let stop = Arc::clone(&stop);
        ctrlc::set_handler(move || stop.store(true, Ordering::SeqCst))
            .context("installing Ctrl+C handler")?;
    }

    // Defaults: one message and out. `--rate` on its own runs until Ctrl+C; a `--count` above one
    // paces itself at 1 Hz, because sending n messages back to back is never what anyone means.
    let limit = args.count.unwrap_or(usize::from(args.rate.is_none()));
    let period = Duration::from_secs_f64(1.0 / args.rate.unwrap_or(1.0));

    // No settle before the first put: the `@schema` query above already completed a round trip with
    // the launch we are about to talk to, so the link and its subscriptions are established.
    let mut sent = 0usize;
    let mut next = Instant::now();
    while !stop.load(Ordering::SeqCst) && (limit == 0 || sent < limit) {
        publisher
            .send(payload.clone())
            .with_context(|| format!("publishing to {key}"))?;
        sent += 1;
        if limit != 0 && sent >= limit {
            break;
        }
        // Absolute deadlines so the rate does not drift, in slices so Ctrl+C is not held up.
        next += period;
        loop {
            let remaining = next.saturating_duration_since(Instant::now());
            if remaining.is_zero() || stop.load(Ordering::SeqCst) {
                break;
            }
            std::thread::sleep(remaining.min(Duration::from_millis(50)));
        }
    }
    println!("{key}: sent {sent} ({})", codec.full_name());
    Ok(())
}

fn echo(args: &EchoArgs) -> Result<()> {
    let bindings = args.bus.bindings()?;
    anyhow::ensure!(
        bindings.inputs.len() == 1 && bindings.outputs.is_empty(),
        "topic echo requires a contract containing only its named input"
    );
    let domain = args.bus.runtime_options("reiny-cli").domain;
    let (contract, keys) = input_keys(
        &bindings,
        &domain,
        &args.ty,
        PortKind::Stream,
        args.from.as_deref(),
    )?;
    input_policy(&contract, false)?;
    let (_, all_keys) = input_keys(&bindings, &domain, &args.ty, PortKind::Stream, None)?;
    let (session, _) = args.bus.open()?;
    let mut codecs = BTreeMap::new();
    for key in &all_keys {
        let (fqn, set) = contract_schema(&session, key, &contract)?;
        codecs.insert(key.clone(), Codec::from_file_set(&set, &fqn)?);
    }
    let subscriber = BoundSubscription::open(&session, &all_keys)?;

    let stop = Arc::new(AtomicBool::new(false));
    let expected = codecs
        .values()
        .next()
        .context("input requires a source")?
        .fingerprint;
    anyhow::ensure!(
        codecs.values().all(|codec| codec.fingerprint == expected),
        "declared input sources disagree on schema fingerprint"
    );
    let _ready = ready_inputs(
        &session,
        &domain,
        &bindings,
        BTreeMap::from([(args.ty.clone(), stream_report(&contract, expected))]),
        Arc::clone(&stop),
    )?;
    {
        let stop = Arc::clone(&stop);
        ctrlc::set_handler(move || stop.store(true, Ordering::SeqCst))
            .context("installing Ctrl+C handler")?;
    }
    let deadline = args
        .duration
        .map(|s| Instant::now() + Duration::from_secs_f64(s));

    let mut shown = 0usize;
    while !stop.load(Ordering::SeqCst) {
        if deadline.is_some_and(|d| Instant::now() >= d) {
            break;
        }
        if args.count.is_some_and(|n| shown >= n) {
            break;
        }
        match subscriber.recv_timeout(Duration::from_millis(100)) {
            Ok(Some(sample)) => {
                if sample.kind() != zenoh::sample::SampleKind::Put {
                    continue;
                }
                let key = sample.key_expr().as_str().to_string();
                if !keys.contains(&key) {
                    continue;
                }
                let source = key_source(&key).to_string();
                let payload = sample.payload().to_bytes();
                let codec = codecs
                    .get(&key)
                    .context("sample came from an undeclared source")?;
                let text = echo_text(codec, &payload, attachment_u64(&sample), args.raw)?;
                println!("{source}  {text}");
                shown += 1;
            }
            Ok(None) => {}
            Err(_) => break,
        }
    }
    Ok(())
}

fn echo_text(codec: &Codec, payload: &[u8], fingerprint: Option<u64>, raw: bool) -> Result<String> {
    verify_fingerprint(codec.fingerprint, fingerprint)?;
    if raw {
        Ok(hex(payload))
    } else {
        codec.decode_json(payload)
    }
}

/// One `topic list` row: who publishes, who listens, who serves this type.
#[derive(Default)]
struct Roles {
    pubs: Vec<String>,
    subs: Vec<String>,
    srvs: Vec<String>,
}

impl Roles {
    /// A column's text; `-` when nobody fills that role.
    fn cell(ids: &[String]) -> String {
        if ids.is_empty() {
            "-".to_string()
        } else {
            ids.join(" ")
        }
    }
}

fn list(args: &ListArgs) -> Result<()> {
    let (session, domain) = args.bus.open()?;

    // (domain, type) → roles. Publishers sit on the type's own key; subscribers and servers hang off
    // a verbatim chunk, which is exactly why the `*/*` query above never mixes them in.
    let mut rows: BTreeMap<(String, String), Roles> = BTreeMap::new();
    let mut collect =
        |keys: Vec<String>, chunk: Option<&str>, pick: fn(&mut Roles) -> &mut Vec<String>| {
            for k in &keys {
                let parsed = match chunk {
                    Some(c) => KeyParts::parse_with_chunk(k, c),
                    None => KeyParts::parse(k),
                };
                if let Some(p) = parsed {
                    let row = rows
                        .entry((p.domain.to_string(), p.ty.to_string()))
                        .or_default();
                    pick(row).push(p.source.to_string());
                }
            }
        };
    collect(
        alive_keys(&session, &reiny::engine::Key::all(&domain).to_string())?,
        None,
        |r| &mut r.pubs,
    );
    collect(
        alive_keys(
            &session,
            &reiny::engine::Key::all(&domain)
                .with_chunk(SUB_CHUNK)
                .to_string(),
        )?,
        Some(SUB_CHUNK),
        |r| &mut r.subs,
    );
    collect(
        alive_keys(
            &session,
            &reiny::engine::Key::all(&domain)
                .with_chunk(SERVICE_CHUNK)
                .to_string(),
        )?,
        Some(SERVICE_CHUNK),
        |r| &mut r.srvs,
    );
    if rows.is_empty() {
        println!("(no live publishers, subscribers or servers in domain {domain})");
        return Ok(());
    }

    // The DOMAIN column only appears when the domain has a wildcard in it.
    let show_domain = domain.contains('*');
    let w_dom = rows.keys().map(|(d, _)| d.len()).max().unwrap_or(0).max(6);
    let w_ty = rows.keys().map(|(_, t)| t.len()).max().unwrap_or(0).max(4);
    let width = |f: fn(&Roles) -> &Vec<String>, header: usize| {
        rows.values()
            .map(|r| Roles::cell(f(r)).len())
            .max()
            .unwrap_or(0)
            .max(header)
    };
    let w_pub = width(|r| &r.pubs, 3);
    let w_listen = width(|r| &r.subs, 3);
    if show_domain {
        println!(
            "{:<w_dom$}  {:<w_ty$}  {:<w_pub$}  {:<w_listen$}  SRV",
            "DOMAIN", "TYPE", "PUB", "SUB"
        );
    } else {
        println!(
            "{:<w_ty$}  {:<w_pub$}  {:<w_listen$}  SRV",
            "TYPE", "PUB", "SUB"
        );
    }
    for ((d, t), roles) in &rows {
        let (p, s, v) = (
            Roles::cell(&roles.pubs),
            Roles::cell(&roles.subs),
            Roles::cell(&roles.srvs),
        );
        if show_domain {
            println!("{d:<w_dom$}  {t:<w_ty$}  {p:<w_pub$}  {s:<w_listen$}  {v}");
        } else {
            println!("{t:<w_ty$}  {p:<w_pub$}  {s:<w_listen$}  {v}");
        }
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Hz,
    Bw,
}

fn rate(args: &RateArgs, mode: Mode) -> Result<()> {
    let bindings = args.bus.bindings()?;
    anyhow::ensure!(
        bindings.inputs.len() == 1 && bindings.outputs.is_empty(),
        "topic rate requires a contract containing only its named input"
    );
    let domain = args.bus.runtime_options("reiny-cli").domain;
    let (contract, keys) = input_keys(
        &bindings,
        &domain,
        &args.ty,
        PortKind::Stream,
        args.from.as_deref(),
    )?;
    input_policy(&contract, false)?;
    let (_, all_keys) = input_keys(&bindings, &domain, &args.ty, PortKind::Stream, None)?;
    let (session, _) = args.bus.open()?;
    let mut fingerprints = BTreeMap::new();
    for key in &all_keys {
        let (fqn, set) = contract_schema(&session, key, &contract)?;
        fingerprints.insert(key.clone(), schema_fingerprint(&set, &fqn)?);
    }
    let subscriber = BoundSubscription::open(&session, &all_keys)?;

    let stop = Arc::new(AtomicBool::new(false));
    let expected = *fingerprints
        .values()
        .next()
        .context("input requires a source")?;
    anyhow::ensure!(
        fingerprints
            .values()
            .all(|fingerprint| *fingerprint == expected),
        "declared input sources disagree on schema fingerprint"
    );
    let _ready = ready_inputs(
        &session,
        &domain,
        &bindings,
        BTreeMap::from([(args.ty.clone(), stream_report(&contract, expected))]),
        Arc::clone(&stop),
    )?;
    {
        let stop = Arc::clone(&stop);
        ctrlc::set_handler(move || stop.store(true, Ordering::SeqCst))
            .context("installing Ctrl+C handler")?;
    }
    let deadline = args
        .duration
        .map(|s| Instant::now() + Duration::from_secs_f64(s));
    let window = Duration::from_secs_f64(args.window);

    let mut series: BTreeMap<String, Series> = BTreeMap::new();
    let mut last_print = Instant::now();
    tracing::info!(keys = ?keys, "listening; Ctrl+C to stop");
    while !stop.load(Ordering::SeqCst) {
        if deadline.is_some_and(|d| Instant::now() >= d) {
            break;
        }
        match subscriber.recv_timeout(Duration::from_millis(100)) {
            Ok(Some(sample)) => {
                if sample.kind() != zenoh::sample::SampleKind::Put {
                    continue;
                }
                if !keys.iter().any(|key| key == sample.key_expr().as_str()) {
                    continue;
                }
                let expected = fingerprints
                    .get(sample.key_expr().as_str())
                    .context("sample came from an undeclared source")?;
                verify_fingerprint(*expected, attachment_u64(&sample))?;
                let source = key_source(sample.key_expr().as_str()).to_string();
                series.entry(source).or_default().push(
                    Instant::now(),
                    sample.payload().len(),
                    window,
                );
            }
            Ok(None) => {}
            Err(_) => break,
        }
        if last_print.elapsed() >= Duration::from_secs(1) {
            print_rates(&series, mode, &args.ty);
            last_print = Instant::now();
        }
    }
    print_rates(&series, mode, &args.ty);
    Ok(())
}

fn print_rates(series: &BTreeMap<String, Series>, mode: Mode, ty: &str) {
    if series.is_empty() {
        println!("{ty}: no messages yet");
        return;
    }
    let w_src = series.keys().map(String::len).max().unwrap_or(0).max(6);
    match mode {
        Mode::Hz => {
            println!(
                "{:<w_src$}  {:>10}  {:>10}  {:>10}  {:>10}  {:>6}",
                "source", "rate", "min", "max", "std dev", "window"
            );
            for (source, s) in series {
                match s.hz() {
                    Some(h) => println!(
                        "{source:<w_src$}  {:>10}  {:>10}  {:>10}  {:>10}  {:>6}",
                        format!("{:.1} Hz", h.rate),
                        fmt_dur(h.min),
                        fmt_dur(h.max),
                        fmt_dur(h.std_dev),
                        s.len()
                    ),
                    None => println!("{source:<w_src$}  (need 2+ messages)"),
                }
            }
        }
        Mode::Bw => {
            println!(
                "{:<w_src$}  {:>12}  {:>10}  {:>6}",
                "source", "rate", "mean msg", "window"
            );
            for (source, s) in series {
                match s.bw() {
                    Some(b) => println!(
                        "{source:<w_src$}  {:>12}  {:>10}  {:>6}",
                        format!("{}/s", fmt_bytes(b.bytes_per_sec)),
                        fmt_bytes(b.mean_size),
                        s.len()
                    ),
                    None => println!("{source:<w_src$}  (need 2+ messages)"),
                }
            }
        }
    }
    println!();
}

/// One source's receive history (only what is inside the window).
#[derive(Default)]
struct Series {
    stamps: VecDeque<Instant>,
    bytes: VecDeque<usize>,
}

struct HzStats {
    rate: f64,
    min: Duration,
    max: Duration,
    std_dev: Duration,
}

struct BwStats {
    bytes_per_sec: f64,
    mean_size: f64,
}

impl Series {
    fn push(&mut self, now: Instant, len: usize, window: Duration) {
        self.stamps.push_back(now);
        self.bytes.push_back(len);
        while self
            .stamps
            .front()
            .is_some_and(|t| now.duration_since(*t) > window)
        {
            self.stamps.pop_front();
            self.bytes.pop_front();
        }
    }

    fn len(&self) -> usize {
        self.stamps.len()
    }

    /// The same as `ros2 topic hz`: the reciprocal of the mean inter-arrival gap, plus min, max and standard deviation.
    fn hz(&self) -> Option<HzStats> {
        if self.stamps.len() < 2 {
            return None;
        }
        let deltas: Vec<f64> = self
            .stamps
            .iter()
            .zip(self.stamps.iter().skip(1))
            .map(|(a, b)| b.duration_since(*a).as_secs_f64())
            .collect();
        let n = deltas.len() as f64;
        let mean = deltas.iter().sum::<f64>() / n;
        let var = deltas.iter().map(|d| (d - mean).powi(2)).sum::<f64>() / n;
        let min = deltas.iter().copied().fold(f64::INFINITY, f64::min);
        let max = deltas.iter().copied().fold(0.0, f64::max);
        Some(HzStats {
            rate: if mean > 0.0 { 1.0 / mean } else { 0.0 },
            min: Duration::from_secs_f64(min),
            max: Duration::from_secs_f64(max),
            std_dev: Duration::from_secs_f64(var.sqrt()),
        })
    }

    /// The bytes received between the window's first and last sample, over that span of time.
    fn bw(&self) -> Option<BwStats> {
        let (first, last) = (self.stamps.front()?, self.stamps.back()?);
        let span = last.duration_since(*first).as_secs_f64();
        if self.stamps.len() < 2 || span <= 0.0 {
            return None;
        }
        let total: usize = self.bytes.iter().sum();
        Some(BwStats {
            bytes_per_sec: total as f64 / span,
            mean_size: total as f64 / self.bytes.len() as f64,
        })
    }
}

fn fmt_dur(d: Duration) -> String {
    let s = d.as_secs_f64();
    if s >= 1.0 {
        format!("{s:.2} s")
    } else if s >= 1e-3 {
        format!("{:.2} ms", s * 1e3)
    } else {
        format!("{:.1} us", s * 1e6)
    }
}

fn fmt_bytes(b: f64) -> String {
    if b >= 1e6 {
        format!("{:.2} MB", b / 1e6)
    } else if b >= 1e3 {
        format!("{:.2} KB", b / 1e3)
    } else {
        format!("{b:.0} B")
    }
}

// ===========================================================================
// reiny node
// ===========================================================================

#[derive(Args)]
pub(crate) struct NodeArgs {
    #[command(subcommand)]
    command: NodeCommand,
}

#[derive(Subcommand)]
enum NodeCommand {
    /// List the live launch ids.
    List(ListArgs),
    /// The types one launch publishes and serves.
    Info {
        /// The launch id.
        id: String,
        #[command(flatten)]
        bus: BusArgs,
    },
}

pub(crate) fn run_node(args: NodeArgs) -> Result<()> {
    match args.command {
        NodeCommand::List(a) => node_list(&a),
        NodeCommand::Info { id, bus } => node_info(&id, &bus),
    }
}

fn node_list(args: &ListArgs) -> Result<()> {
    let (session, domain) = args.bus.open()?;
    let mut ids: BTreeSet<String> = BTreeSet::new();
    // A 0.5 launch announces `@launch`. Older ones (`@grain`, or no token at all) are picked up
    // from their publisher / server tokens instead.
    for pattern in [
        format!("{KEY_ROOT}/{domain}/**/{LAUNCH_CHUNK}"),
        reiny::engine::Key::all(&domain).to_string(),
        reiny::engine::Key::all(&domain)
            .with_chunk(SERVICE_CHUNK)
            .to_string(),
    ] {
        for k in alive_keys(&session, &pattern)? {
            let id = key_source(&k);
            if !id.is_empty() {
                ids.insert(id.to_string());
            }
        }
    }
    if ids.is_empty() {
        println!("(no live launches in domain {domain})");
    }
    for id in ids {
        println!("{id}");
    }
    Ok(())
}

fn node_info(id: &str, bus: &BusArgs) -> Result<()> {
    let (session, domain) = bus.open()?;
    let alive = !alive_keys(
        &session,
        &format!("{KEY_ROOT}/{domain}/{id}/{LAUNCH_CHUNK}"),
    )?
    .is_empty();
    let pubs: Vec<String> = alive_keys(&session, &format!("{KEY_ROOT}/{domain}/{id}/*"))?
        .iter()
        .filter_map(|k| KeyParts::parse(k).map(|p| p.ty.to_string()))
        .collect();
    let chunked = |chunk: &'static str| -> Result<Vec<String>> {
        Ok(
            alive_keys(&session, &format!("{KEY_ROOT}/{domain}/{id}/*/{chunk}"))?
                .iter()
                .filter_map(|k| KeyParts::parse_with_chunk(k, chunk).map(|p| p.ty.to_string()))
                .collect(),
        )
    };
    let subs = chunked(SUB_CHUNK)?;
    let srvs = chunked(SERVICE_CHUNK)?;
    if !alive && pubs.is_empty() && subs.is_empty() && srvs.is_empty() {
        println!("{id}: not found in domain {domain}");
        return Ok(());
    }
    println!(
        "{id}{}",
        if alive {
            ""
        } else {
            "  (no @launch token: pre-0.5)"
        }
    );
    for (label, types) in [("pub", &pubs), ("sub", &subs), ("srv", &srvs)] {
        println!("{label} : {}", Roles::cell(types));
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::float_cmp)]
mod tests {
    use super::*;

    #[test]
    fn raw_echo_does_not_bypass_schema_validation() {
        let codec = Codec::from_file_set(&crate::codec::tests::probe_set(), "e2e.Probe").unwrap();
        for raw in [false, true] {
            assert!(echo_text(&codec, &[], None, raw).is_err());
            assert!(echo_text(&codec, &[], Some(codec.fingerprint ^ 1), raw).is_err());
            assert!(echo_text(&codec, &[], Some(codec.fingerprint), raw).is_ok());
        }
    }

    #[test]
    fn hz_stats_from_regular_intervals() {
        let t0 = Instant::now();
        let mut s = Series::default();
        for i in 0..11 {
            s.push(
                t0 + Duration::from_millis(10 * i),
                100,
                Duration::from_secs(10),
            );
        }
        let h = s.hz().unwrap();
        assert!((h.rate - 100.0).abs() < 1e-6, "{}", h.rate);
        assert_eq!(h.min, Duration::from_millis(10));
        assert_eq!(h.max, Duration::from_millis(10));
        assert_eq!(h.std_dev, Duration::ZERO);
        let b = s.bw().unwrap();
        assert!(
            (b.bytes_per_sec - 11_000.0).abs() < 1e-6,
            "{}",
            b.bytes_per_sec
        );
        assert_eq!(b.mean_size, 100.0);
    }

    #[test]
    fn window_drops_old_samples_and_needs_two() {
        let t0 = Instant::now();
        let mut s = Series::default();
        s.push(t0, 1, Duration::from_secs(1));
        assert!(s.hz().is_none());
        s.push(t0 + Duration::from_secs(5), 1, Duration::from_secs(1));
        assert_eq!(s.len(), 1, "the one outside the window is dropped");
    }

    #[test]
    fn formats() {
        assert_eq!(fmt_dur(Duration::from_millis(10)), "10.00 ms");
        assert_eq!(fmt_dur(Duration::from_micros(80)), "80.0 us");
        assert_eq!(fmt_dur(Duration::from_secs(2)), "2.00 s");
        assert_eq!(fmt_bytes(12_300.0), "12.30 KB");
        assert_eq!(fmt_bytes(12.0), "12 B");
    }
}
