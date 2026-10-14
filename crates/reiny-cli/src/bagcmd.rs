//! `reiny bag record / play / info` — recording, replaying and summarizing the bus (the equivalent of rosbag2; the format is MCAP).
//!
//! reiny's share is only what cannot be written without knowing zenoh's key shape and reiny's
//! conventions (domain / source / presence / latched / fingerprints / `@schema`). Operations that
//! need none of that vocabulary — cutting by time or topic, producing statistics — are left to the `mcap` CLI (`docs/design/bag.md`).
//!
//! Recording and playback use named SDK endpoints on a tokio runtime. File inspection remains synchronous.

// Every numeric cast in this file is a bounded conversion of a time (ns), a rate or a date; making
// each one a try_from would only line up `.unwrap()`s behind it. The one-letter bindings in civil() are the calendar's own notation.
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::many_single_char_names
)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};
use reiny::bindings::{PortKind, Replay};
use reiny::zenoh::{self, Wait};

use crate::bus::{
    BusArgs, KEY_ROOT, KeyParts, contract_schema, input_keys, input_policy, key_source, output_key,
    schema_fingerprint, verify_fingerprint,
};

#[derive(Args)]
pub(crate) struct BagArgs {
    #[command(subcommand)]
    command: BagCommand,
}

#[derive(Subcommand)]
enum BagCommand {
    /// Subscribe to the bus and record into MCAP (until Ctrl+C or `--duration`).
    Record(RecordArgs),
    /// Replay an MCAP onto the bus.
    Play(PlayArgs),
    /// Summarize what is in an MCAP.
    Info(InfoArgs),
}

pub(crate) fn run(args: BagArgs) -> Result<()> {
    match args.command {
        BagCommand::Record(a) => record(&a),
        BagCommand::Play(a) => play(&a),
        BagCommand::Info(a) => info(&a),
    }
}

// ===========================================================================
// record
// ===========================================================================

#[derive(Args)]
struct RecordArgs {
    /// The output file (default: `<yyyymmdd-HHMMSS>.mcap`, UTC).
    #[arg(long)]
    out: Option<PathBuf>,
    /// Filter the declared inputs by type segment (repeatable).
    #[arg(long = "type")]
    types: Vec<String>,
    /// Filter the declared inputs by source id (repeatable).
    #[arg(long)]
    from: Vec<String>,
    /// Do not record this type (repeatable).
    #[arg(long = "exclude-type")]
    exclude_types: Vec<String>,
    /// Stop after this many seconds (default: at Ctrl+C).
    #[arg(long)]
    duration: Option<f64>,
    /// Do not record the latched values (the snapshot) from before recording started.
    #[arg(long)]
    no_snapshot: bool,
    #[command(flatten)]
    bus: BusArgs,
}

/// A channel's state while recording: the MCAP `channel_id` and whether its schema is registered.
struct RecordChannels<W: std::io::Write + std::io::Seek> {
    writer: mcap::Writer<W>,
    /// key → (`channel_id`, `sequence`).
    channels: BTreeMap<String, (u16, u32)>,
    /// The descriptor sets already fetched (FQN → subset bytes), handed to the schema when a channel is created.
    schemas: BTreeMap<String, (String, Vec<u8>)>,
    fingerprints: BTreeMap<String, u64>,
}

fn record(args: &RecordArgs) -> Result<()> {
    let bindings = args.bus.bindings()?;
    let domain = args.bus.runtime_options("reiny-cli").domain;
    let contracts = record_contracts(args, &bindings, &domain)?;
    let (session, _) = args.bus.open()?;
    let mut schemas = BTreeMap::new();
    let mut fingerprints = BTreeMap::new();
    for (key, contract) in &contracts {
        let (fqn, set) = contract_schema(&session, key, contract)?;
        fingerprints.insert(key.clone(), schema_fingerprint(&set, &fqn)?);
        schemas.insert(key.clone(), (fqn, set));
    }
    let (runtime, cloudy) = args.bus.context(session, bindings.clone())?;
    let subscriber = record_subscriber(&runtime, &cloudy, &bindings, &domain, &fingerprints)?;
    let out = args
        .out
        .clone()
        .unwrap_or_else(|| PathBuf::from(format!("{}.mcap", utc_stamp(now_unix_nanos()))));

    let file = std::io::BufWriter::new(
        std::fs::File::create(&out).with_context(|| format!("creating {}", out.display()))?,
    );
    let writer = mcap::Writer::with_options(
        file,
        mcap::WriteOptions::new().compression(Some(mcap::Compression::Zstd)),
    )
    .map_err(anyhow::Error::msg)?;
    let mut state = RecordChannels {
        writer,
        channels: BTreeMap::new(),
        schemas,
        fingerprints,
    };

    let filter = Filter::new(&args.types, &args.from, &args.exclude_types);
    let mut count: u64 = 0;

    // Leave on Ctrl+C or --duration.
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
    let deadline = args
        .duration
        .map(|s| Instant::now() + Duration::from_secs_f64(s));

    tracing::info!(out = %out.display(), domain = %domain, "recording; Ctrl+C to stop");
    while !stop.load(Ordering::SeqCst) {
        if let Some(d) = deadline
            && Instant::now() >= d
        {
            break;
        }
        match subscriber.recv_timeout(Duration::from_millis(100)) {
            Ok((key, sample)) => {
                let log = now_unix_nanos();
                let pubt = sample.timestamp.unwrap_or(log);
                let latched = contracts[&key].replay == Replay::Last;
                if write_sample(
                    &mut state,
                    &key,
                    &sample.payload,
                    sample.schema,
                    log,
                    pubt,
                    latched,
                    &filter,
                )? {
                    count += 1;
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(_) => break, // the channel is disconnected.
        }
    }

    state.writer.finish().map_err(anyhow::Error::msg)?;
    tracing::info!(messages = count, out = %out.display(), "recording finished");
    println!("{} — {count} messages", out.display());
    Ok(())
}

fn record_contracts(
    args: &RecordArgs,
    bindings: &reiny::bindings::ModuleBindings,
    domain: &str,
) -> Result<BTreeMap<String, reiny::bindings::EndpointContract>> {
    anyhow::ensure!(
        bindings.outputs.is_empty(),
        "bag record contract cannot contain outputs"
    );
    let mut contracts = BTreeMap::new();
    for (name, port) in &bindings.inputs {
        anyhow::ensure!(
            port.contract.kind == PortKind::Stream,
            "bag record supports only stream inputs"
        );
        let (contract, keys) = input_keys(bindings, domain, name, PortKind::Stream, None)?;
        input_policy(&contract, true)?;
        anyhow::ensure!(
            !args.no_snapshot || contract.replay == Replay::Live,
            "--no-snapshot conflicts with the declared replay:last input policy"
        );
        for key in keys {
            contracts.insert(key, contract.clone());
        }
    }
    anyhow::ensure!(
        !contracts.is_empty(),
        "bag record requires declared stream inputs"
    );
    Ok(contracts)
}

fn record_subscriber(
    runtime: &tokio::runtime::Runtime,
    cloudy: &reiny::ModuleContext,
    bindings: &reiny::bindings::ModuleBindings,
    domain: &str,
    fingerprints: &BTreeMap<String, u64>,
) -> Result<std::sync::mpsc::Receiver<(String, reiny::RawEnvelope)>> {
    let (sender, subscriber) = std::sync::mpsc::sync_channel(256);
    let mut receivers = Vec::new();
    for (name, port) in &bindings.inputs {
        let (_, keys) = input_keys(bindings, domain, name, PortKind::Stream, None)?;
        let expected = fingerprints[&keys[0]];
        anyhow::ensure!(
            keys.iter().all(|key| fingerprints[key] == expected),
            "declared input sources disagree on schema fingerprint"
        );
        let input = cloudy.raw_input(name, &port.contract.type_name, expected)?;
        receivers.push((port.contract.clone(), input));
    }
    cloudy.ready()?;
    for (contract, mut input) in receivers {
        let sender = sender.clone();
        let domain = domain.to_owned();
        runtime.spawn(async move {
            while let Some(sample) = input.recv().await {
                let key = format!(
                    "{KEY_ROOT}/{domain}/{}/@out/{}/{}",
                    sample.source,
                    sample.source_port,
                    crate::bus::topic_type(&contract)
                );
                if sender.send((key, sample)).is_err() {
                    break;
                }
            }
        });
    }
    Ok(subscriber)
}

/// Write one sample into the MCAP, creating the channel (and its schema, if any) the first time a key
#[allow(clippy::too_many_arguments)]
fn write_sample<W: std::io::Write + std::io::Seek>(
    state: &mut RecordChannels<W>,
    key: &str,
    payload: &[u8],
    fingerprint: Option<u64>,
    log_time: u64,
    publish_time: u64,
    latched: bool,
    filter: &Filter,
) -> Result<bool> {
    let Some(parts) = KeyParts::parse(key) else {
        return Ok(false);
    };
    if !filter.accepts(&parts) {
        return Ok(false);
    }
    let expected = state
        .fingerprints
        .get(key)
        .context("sample came from an undeclared input")?;
    verify_fingerprint(*expected, fingerprint)?;

    let channel_id = if let Some(&(id, _)) = state.channels.get(key) {
        id
    } else {
        let schema_id = match state.schemas.get(key) {
            Some((fqn, subset)) => state
                .writer
                .add_schema(fqn, "protobuf", subset)
                .map_err(anyhow::Error::msg)?,
            None => 0,
        };
        let mut meta: BTreeMap<String, String> = BTreeMap::new();
        meta.insert("reiny.domain".to_string(), parts.domain.to_string());
        meta.insert("reiny.source".to_string(), parts.source.to_string());
        meta.insert("reiny.port".to_string(), parts.port.to_string());
        meta.insert("reiny.key".to_string(), key.to_string());
        meta.insert(
            "reiny.fingerprint_version".to_string(),
            reiny::SCHEMA_FINGERPRINT_VERSION.to_string(),
        );
        meta.insert("reiny.type".to_string(), parts.ty.to_string());
        if let Some(fp) = fingerprint {
            meta.insert("reiny.schema".to_string(), format!("{fp:016x}"));
        }
        if latched {
            meta.insert("reiny.latched".to_string(), "true".to_string());
        }
        let id = state
            .writer
            .add_channel(schema_id, key, "protobuf", &meta)
            .map_err(anyhow::Error::msg)?;
        state.channels.insert(key.to_string(), (id, 0));
        id
    };

    let seq = match state.channels.get_mut(key) {
        Some(entry) => {
            let s = entry.1;
            entry.1 += 1;
            s
        }
        None => return Ok(false), // inserted just above, so it should be there; do not panic if not.
    };
    state
        .writer
        .write_to_known_channel(
            &mcap::records::MessageHeader {
                channel_id,
                sequence: seq,
                log_time,
                publish_time,
            },
            payload,
        )
        .map_err(anyhow::Error::msg)?;
    Ok(true)
}

// ===========================================================================
// play
// ===========================================================================

#[derive(Args)]
struct PlayArgs {
    /// The MCAP to replay.
    file: PathBuf,
    /// Map an exact recorded endpoint key to a declared output (repeatable).
    #[arg(long = "map", value_name = "RECORDED-KEY=OUTPUT")]
    mappings: Vec<String>,
    /// Require this source identity (it must equal the contract's endpoint namespace).
    #[arg(long = "as")]
    as_id: Option<String>,
    /// The replay speed (1.0 = real time, 2.0 = twice as fast).
    #[arg(long, default_value_t = 1.0)]
    rate: f64,
    /// Go back to the start on reaching the end.
    #[arg(long = "loop")]
    looping: bool,
    /// Skip this many seconds from the start.
    #[arg(long)]
    start: Option<f64>,
    /// How long to replay (in seconds, counted from `--start`).
    #[arg(long)]
    duration: Option<f64>,
    /// Replay only this type (repeatable).
    #[arg(long = "type")]
    types: Vec<String>,
    /// Replay only this source (repeatable; matched against the id before any rewrite).
    #[arg(long)]
    from: Vec<String>,
    /// Replay into a domain that has live publishers too (this removes the safety catch).
    #[arg(long)]
    force: bool,
    #[command(flatten)]
    bus: BusArgs,
}

/// What a channel needs at replay time: its output key and reiny's conventions.
struct PlayChannel {
    /// The mapped named output key, with the target domain and declared namespace.
    key: String,
    ty: String,
    publisher: Arc<reiny::DeclaredPublisher>,
}

fn play(args: &PlayArgs) -> Result<()> {
    let bindings = args.bus.bindings()?;
    anyhow::ensure!(
        bindings.inputs.is_empty(),
        "bag play contract cannot contain inputs"
    );
    anyhow::ensure!(
        !args.mappings.is_empty(),
        "bag play requires --map recorded-key=output"
    );
    let domain = args.bus.runtime_options("reiny-cli").domain;
    if args.rate <= 0.0 {
        bail!("--rate must be positive");
    }
    let bytes =
        std::fs::read(&args.file).with_context(|| format!("reading {}", args.file.display()))?;

    // Build the channel catalogue first (the type, latched and fingerprint are wanted before any message body is read).
    let summary = mcap::Summary::read(&bytes)
        .map_err(anyhow::Error::msg)?
        .context("bag has no summary section; recover it with `mcap recover`")?;

    let plan = playback_plan(args, &bindings, &domain, &summary)?;
    let (session, _) = args.bus.open()?;

    // The safety catch: refuse by default when the replay domain has a live publisher of the same type.
    // Looked at **before** declaring our own publishers (so as not to count them).
    if !args.force {
        guard_live_publishers(&session, &domain, &plan)?;
    }

    // Stand up each channel's publisher + liveliness token (+ latched queryable).
    let (runtime, cloudy) = args.bus.context(session, bindings)?;
    let channels = declare_play_channels(&cloudy, plan)?;
    let stop = Arc::new(AtomicBool::new(false));
    {
        let stop = Arc::clone(&stop);
        ctrlc::set_handler(move || stop.store(true, Ordering::SeqCst))
            .context("installing Ctrl+C handler")?;
    }
    {
        let cloudy = Arc::clone(&cloudy);
        let stop = Arc::clone(&stop);
        runtime.spawn(async move {
            cloudy.shutdown().await;
            stop.store(true, Ordering::SeqCst);
        });
    }
    cloudy.ready()?;

    let msgs = replay_order(&bytes, args.start, args.duration)?;
    if msgs.is_empty() {
        tracing::warn!("no messages in the selected range");
        return Ok(());
    }
    let first = msgs[0].0;

    loop {
        let t0 = Instant::now();
        for (log_time, channel_id, data) in &msgs {
            if stop.load(Ordering::SeqCst) {
                return Ok(());
            }
            let Some(ch) = channels.get(channel_id) else {
                continue;
            };
            // Sleep to an absolute deadline (so it does not drift the way accumulated relative sleeps do).
            let target = t0 + Duration::from_nanos(((log_time - first) as f64 / args.rate) as u64);
            while !stop.load(Ordering::SeqCst) && Instant::now() < target {
                std::thread::sleep(
                    target
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_millis(50)),
                );
            }
            if stop.load(Ordering::SeqCst) {
                return Ok(());
            }
            ch.publisher
                .send(data.clone())
                .with_context(|| format!("publishing {} to {}", ch.ty, ch.key))?;
        }
        if !args.looping {
            break;
        }
    }
    tracing::info!(messages = msgs.len(), "playback finished");
    Ok(())
}

/// A channel's information after filtering and before declaring.
struct PlannedChannel {
    key: String,
    ty: String,
    name: String,
    fqn: String,
    fingerprint: u64,
}

fn playback_mappings(arguments: &[String]) -> Result<BTreeMap<&str, &str>> {
    let mut mappings = BTreeMap::new();
    let mut outputs = std::collections::BTreeSet::new();
    for mapping in arguments {
        let (recorded, output) = mapping
            .split_once('=')
            .context("--map requires recorded-key=output")?;
        anyhow::ensure!(
            KeyParts::parse(recorded).is_some()
                && recorded.contains("/@out/")
                && !output.is_empty(),
            "--map requires an exact recorded endpoint key and an output name"
        );
        anyhow::ensure!(
            mappings.insert(recorded, output).is_none(),
            "duplicate mapping for recorded endpoint {recorded}"
        );
        anyhow::ensure!(
            outputs.insert(output),
            "multiple recorded endpoints map to output {output}"
        );
    }
    Ok(mappings)
}

fn playback_plan(
    args: &PlayArgs,
    bindings: &reiny::bindings::ModuleBindings,
    domain: &str,
    summary: &mcap::Summary,
) -> Result<BTreeMap<u16, PlannedChannel>> {
    let filter = Filter::new(&args.types, &args.from, &[]);
    let mut plan = BTreeMap::new();
    let mappings = playback_mappings(&args.mappings)?;
    let mut used = std::collections::BTreeSet::new();
    for (id, ch) in &summary.channels {
        let Some(parts) = KeyParts::parse(&ch.topic) else {
            continue;
        };
        if !filter.accepts(&parts) {
            continue;
        }
        let schema = ch
            .schema
            .as_ref()
            .context("bag playback requires a protobuf schema for every selected channel")?;
        anyhow::ensure!(
            schema.encoding == "protobuf" && ch.message_encoding == "protobuf",
            "bag playback requires protobuf channel and schema encodings"
        );
        let name = mappings
            .get(ch.topic.as_str())
            .with_context(|| format!("missing --map for recorded endpoint {}", ch.topic))?;
        anyhow::ensure!(
            used.insert(ch.topic.as_str()),
            "ambiguous recorded endpoint {} has multiple channels",
            ch.topic
        );
        let output = bindings
            .outputs
            .get(*name)
            .with_context(|| format!("undeclared mapped output {name}"))?;
        anyhow::ensure!(
            output.contract.kind == PortKind::Stream && output.contract.type_name == schema.name,
            "bag channel {} is incompatible with mapped output {name}",
            ch.topic
        );
        anyhow::ensure!(
            parts.ty == crate::bus::topic_type(&output.contract),
            "bag channel type differs from its descriptor"
        );
        let (_, key) = output_key(
            bindings,
            domain,
            name,
            PortKind::Stream,
            args.as_id.as_deref(),
        )?;
        let fingerprint = schema_fingerprint(&schema.data, &schema.name)?;
        anyhow::ensure!(
            ch.metadata.get("reiny.key").map(String::as_str) == Some(ch.topic.as_str())
                && ch.metadata.get("reiny.port").map(String::as_str) == Some(parts.port)
                && ch.metadata.get("reiny.source").map(String::as_str) == Some(parts.source)
                && ch.metadata.get("reiny.domain").map(String::as_str) == Some(parts.domain)
                && ch.metadata.get("reiny.type").map(String::as_str) == Some(parts.ty)
                && ch.metadata.get("reiny.fingerprint_version")
                    == Some(&reiny::SCHEMA_FINGERPRINT_VERSION.to_string()),
            "bag channel {} lacks consistent named endpoint metadata",
            ch.topic
        );
        let recorded = ch
            .metadata
            .get("reiny.schema")
            .and_then(|value| u64::from_str_radix(value, 16).ok());
        verify_fingerprint(fingerprint, recorded)?;
        plan.insert(
            *id,
            PlannedChannel {
                key,
                ty: parts.ty.to_string(),
                name: (*name).to_string(),
                fqn: schema.name.clone(),
                fingerprint,
            },
        );
    }
    if plan.is_empty() {
        bail!("nothing to play (no channels matched the filters)");
    }
    anyhow::ensure!(
        used.len() == mappings.len(),
        "--map names an absent or filtered recorded endpoint"
    );
    anyhow::ensure!(
        mappings.len() == bindings.outputs.len(),
        "bag play must map every declared output"
    );
    Ok(plan)
}

/// For each planned channel, look for a live publisher **other than ours** in the replay domain.
fn guard_live_publishers(
    session: &zenoh::Session,
    domain: &str,
    plan: &BTreeMap<u16, PlannedChannel>,
) -> Result<()> {
    let mut types: Vec<&str> = plan.values().map(|p| p.ty.as_str()).collect();
    types.sort_unstable();
    types.dedup();
    for ty in types {
        let key = format!("{KEY_ROOT}/{domain}/**/@out/*/{ty}");
        let Ok(replies) = session.liveliness().get(&key).wait() else {
            continue;
        };
        let mut live: Vec<String> = Vec::new();
        for reply in &replies {
            if let Ok(sample) = reply.result() {
                let id = key_source(sample.key_expr().as_str());
                if !id.is_empty() && !live.iter().any(|s| s == id) {
                    live.push(id.to_string());
                }
            }
        }
        if !live.is_empty() {
            bail!(
                "live publisher(s) for {ty} in domain {domain:?}: {}\n       \
                 replay elsewhere (--domain) or override with --force",
                live.join(", ")
            );
        }
    }
    Ok(())
}

fn declare_play_channels(
    cloudy: &reiny::ModuleContext,
    plan: BTreeMap<u16, PlannedChannel>,
) -> Result<BTreeMap<u16, PlayChannel>> {
    let mut channels = BTreeMap::new();
    let mut publishers = BTreeMap::new();
    for (id, p) in plan {
        if !publishers.contains_key(&p.name) {
            publishers.insert(
                p.name.clone(),
                Arc::new(cloudy.raw_output(&p.name, &p.fqn, p.fingerprint)?),
            );
        }
        let publisher = Arc::clone(&publishers[&p.name]);
        channels.insert(
            id,
            PlayChannel {
                key: p.key,
                ty: p.ty,
                publisher,
            },
        );
    }
    Ok(channels)
}

/// Read an MCAP linearly, cut to `--start` / `--duration`, yielding (`log_time`, `channel_id`, `data`).
fn replay_order(
    bytes: &[u8],
    start: Option<f64>,
    duration: Option<f64>,
) -> Result<Vec<(u64, u16, Vec<u8>)>> {
    let mut first: Option<u64> = None;
    let mut out = Vec::new();
    for msg in mcap::MessageStream::new(bytes).map_err(anyhow::Error::msg)? {
        let msg = msg.map_err(anyhow::Error::msg)?;
        let base = *first.get_or_insert(msg.log_time);
        let offset_ns = msg.log_time.saturating_sub(base);
        if let Some(s) = start
            && offset_ns < (s * 1e9) as u64
        {
            continue;
        }
        if let Some(d) = duration {
            let from = start.unwrap_or(0.0);
            if offset_ns > ((from + d) * 1e9) as u64 {
                break;
            }
        }
        out.push((msg.log_time, msg.channel.id, msg.data.into_owned()));
    }
    Ok(out)
}

// ===========================================================================
// info
// ===========================================================================

#[derive(Args)]
struct InfoArgs {
    /// The MCAP to summarize.
    file: PathBuf,
}

fn info(args: &InfoArgs) -> Result<()> {
    let bytes =
        std::fs::read(&args.file).with_context(|| format!("reading {}", args.file.display()))?;
    let summary = mcap::Summary::read(&bytes)
        .map_err(anyhow::Error::msg)?
        .context("bag has no summary section; recover it with `mcap recover`")?;
    let stats = summary
        .stats
        .as_ref()
        .context("bag has no statistics record")?;

    let start = stats.message_start_time;
    let end = stats.message_end_time;
    let span_s = (end.saturating_sub(start)) as f64 / 1e9;
    println!(
        "{}  {:.1} s  ({} → {})  {} messages",
        args.file.display(),
        span_s,
        utc_full(start),
        utc_full(end),
        stats.message_count,
    );

    // The domain comes from the channels' metadata (they should agree; if not, list them).
    let mut domains: Vec<&str> = summary
        .channels
        .values()
        .filter_map(|c| c.metadata.get("reiny.domain").map(String::as_str))
        .collect();
    domains.sort_unstable();
    domains.dedup();
    if !domains.is_empty() {
        println!("domain: {}", domains.join(", "));
    }

    // One row per channel: source / type / count / mean Hz / latched / schema + fingerprint.
    let mut rows: Vec<Row> = summary
        .channels
        .iter()
        .map(|(id, ch)| {
            let count = stats.channel_message_counts.get(id).copied().unwrap_or(0);
            let m = &ch.metadata;
            Row {
                source: format!(
                    "{}:{}",
                    m.get("reiny.source").map_or("", String::as_str),
                    m.get("reiny.port").map_or("", String::as_str)
                ),
                ty: m
                    .get("reiny.type")
                    .cloned()
                    .unwrap_or_else(|| ch.topic.clone()),
                count,
                hz: if span_s > 0.0 {
                    count as f64 / span_s
                } else {
                    0.0
                },
                latched: m.get("reiny.latched").map(String::as_str) == Some("true"),
                schema: ch.schema.as_ref().map(|s| s.name.clone()),
                fingerprint: m.get("reiny.schema").cloned(),
            }
        })
        .collect();
    rows.sort_by(|a, b| (&a.source, &a.ty).cmp(&(&b.source, &b.ty)));

    let w_src = rows.iter().map(|r| r.source.len()).max().unwrap_or(0);
    let w_ty = rows.iter().map(|r| r.ty.len()).max().unwrap_or(0);
    for r in &rows {
        let latched = if r.latched { "latched" } else { "       " };
        // The descriptor (its type name, if any) and the fingerprint are printed independently — a stage-1 bag carries only the fingerprint.
        let schema = r.schema.as_deref().map_or("—".to_string(), str::to_string);
        let fp = r
            .fingerprint
            .as_deref()
            .map_or(String::new(), |f| format!("  {f}"));
        println!(
            "  {:<w_src$}  {:<w_ty$}  {:>7}  {:>6.1} Hz  {latched}  schema {schema}{fp}",
            r.source, r.ty, r.count, r.hz,
        );
    }
    Ok(())
}

struct Row {
    source: String,
    ty: String,
    count: u64,
    hz: f64,
    latched: bool,
    schema: Option<String>,
    fingerprint: Option<String>,
}

// ===========================================================================
// Shared odds and ends
// ===========================================================================

/// The type / source / excluded-type filter. An empty allow list means "everything".
struct Filter {
    types: Vec<String>,
    from: Vec<String>,
    exclude: Vec<String>,
}

impl Filter {
    fn new(types: &[String], from: &[String], exclude: &[String]) -> Self {
        Self {
            types: types.to_vec(),
            from: from.to_vec(),
            exclude: exclude.to_vec(),
        }
    }

    fn accepts(&self, parts: &KeyParts<'_>) -> bool {
        if self.exclude.iter().any(|t| t == parts.ty) {
            return false;
        }
        if !self.types.is_empty() && !self.types.iter().any(|t| t == parts.ty) {
            return false;
        }
        if !self.from.is_empty() && !self.from.iter().any(|s| s == parts.source) {
            return false;
        }
        true
    }
}

fn now_unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

/// Unix nanoseconds → `yyyymmdd-HHMMSS` (UTC). For the default file name.
fn utc_stamp(ns: u64) -> String {
    let (y, mo, d, h, mi, s) = civil(ns / 1_000_000_000);
    format!("{y:04}{mo:02}{d:02}-{h:02}{mi:02}{s:02}")
}

/// Unix nanoseconds → `yyyy-mm-dd HH:MM:SS` (UTC). For info's output.
fn utc_full(ns: u64) -> String {
    let (y, mo, d, h, mi, s) = civil(ns / 1_000_000_000);
    format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}")
}

/// Unix seconds → (year, month, day, hour, minute, second) UTC. Howard Hinnant's `civil_from_days`.
/// Fifteen lines to avoid pulling in the `time` crate (only a bag's file name and display need it).
fn civil(secs: u64) -> (i64, u32, u32, u32, u32, u32) {
    let days = (secs / 86_400) as i64;
    let rem = (secs % 86_400) as u32;
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = if mo <= 2 { y + 1 } else { y };
    (y, mo, d, h, mi, s)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    fn playback_fixture() -> (PlayArgs, reiny::bindings::ModuleBindings, mcap::Summary) {
        use clap::Parser;
        #[derive(Parser)]
        struct Command {
            #[command(flatten)]
            args: PlayArgs,
        }
        let args = Command::parse_from(["play", "recording.mcap"]).args;
        let bindings = serde_yaml::from_str(
            "version: 3\nnamespace: replay\nendpoint_namespace: replay\ninputs: {}\noutputs:\n  left:\n    contract: {type: e2e.Probe}\n  right:\n    contract: {type: e2e.Probe}\n",
        ).unwrap();
        let set = crate::codec::tests::probe_set();
        let fingerprint = schema_fingerprint(&set, "e2e.Probe").unwrap();
        let keys = [
            "reiny/v3/lab/ctrl/@out/left/e2e.Probe",
            "reiny/v3/lab/ctrl/@out/right/e2e.Probe",
        ];
        let mut bytes = std::io::Cursor::new(Vec::new());
        {
            let mut state = RecordChannels {
                writer: mcap::Writer::new(&mut bytes).unwrap(),
                channels: BTreeMap::new(),
                schemas: keys
                    .iter()
                    .map(|key| ((*key).into(), ("e2e.Probe".into(), set.clone())))
                    .collect(),
                fingerprints: keys
                    .iter()
                    .map(|key| ((*key).into(), fingerprint))
                    .collect(),
            };
            for key in keys {
                write_sample(
                    &mut state,
                    key,
                    &[],
                    Some(fingerprint),
                    1,
                    1,
                    false,
                    &Filter::new(&[], &[], &[]),
                )
                .unwrap();
            }
            state.writer.finish().unwrap();
        }
        let summary = mcap::Summary::read(bytes.get_ref()).unwrap().unwrap();
        (args, bindings, summary)
    }

    #[test]
    fn replay_maps_exact_recorded_endpoints_without_type_inference() {
        let (mut args, bindings, summary) = playback_fixture();
        assert!(playback_plan(&args, &bindings, "replay", &summary).is_err());
        args.mappings = vec![
            "reiny/v3/lab/ctrl/@out/left/e2e.Probe=right".into(),
            "reiny/v3/lab/ctrl/@out/right/e2e.Probe=left".into(),
        ];
        let plan = playback_plan(&args, &bindings, "replay", &summary).unwrap();
        for (id, channel) in &summary.channels {
            let expected = if channel.topic.contains("/left/") {
                "right"
            } else {
                "left"
            };
            assert_eq!(plan[id].name, expected);
            assert_eq!(
                plan[id].key,
                format!("reiny/v3/replay/replay/@out/{expected}/e2e.Probe")
            );
            assert_eq!(channel.metadata["reiny.key"], channel.topic);
            assert_eq!(
                channel.metadata["reiny.port"],
                if expected == "right" { "left" } else { "right" }
            );
        }
    }

    #[test]
    fn replay_rejects_duplicate_ambiguous_and_incompatible_mappings() {
        let (mut args, mut bindings, mut summary) = playback_fixture();
        let first = "reiny/v3/lab/ctrl/@out/left/e2e.Probe=left";
        let second = "reiny/v3/lab/ctrl/@out/right/e2e.Probe=right";
        for mappings in [
            vec![first],
            vec![first, first, second],
            vec![first, "reiny/v3/lab/ctrl/@out/right/e2e.Probe=left"],
            vec![first, "reiny/v3/lab/ctrl/@out/unknown/e2e.Probe=right"],
            vec![first, "reiny/v3/lab/ctrl/@out/right/e2e.Probe=unknown"],
        ] {
            args.mappings = mappings.into_iter().map(str::to_string).collect();
            assert!(playback_plan(&args, &bindings, "replay", &summary).is_err());
        }
        args.mappings = vec![first.into(), second.into()];
        bindings.outputs.get_mut("left").unwrap().contract.type_name = "other.Probe".into();
        assert!(playback_plan(&args, &bindings, "replay", &summary).is_err());
        bindings.outputs.get_mut("left").unwrap().contract.type_name = "e2e.Probe".into();
        let duplicate = Arc::clone(summary.channels.values().next().unwrap());
        summary.channels.insert(99, duplicate);
        assert!(playback_plan(&args, &bindings, "replay", &summary).is_err());
        summary.channels.remove(&99);
        let channel = Arc::make_mut(summary.channels.values_mut().next().unwrap());
        channel
            .metadata
            .insert("reiny.schema".into(), "0000000000000000".into());
        assert!(playback_plan(&args, &bindings, "replay", &summary).is_err());
    }

    #[test]
    fn live_samples_and_snapshots_are_not_written_without_matching_schema() {
        let key = "reiny/v3/lab/ctrl/@out/probe/e2e.Probe";
        let set = crate::codec::tests::probe_set();
        let expected = schema_fingerprint(&set, "e2e.Probe").unwrap();
        let mut state = RecordChannels {
            writer: mcap::Writer::new(std::io::Cursor::new(Vec::new())).unwrap(),
            channels: BTreeMap::new(),
            schemas: BTreeMap::from([(key.into(), ("e2e.Probe".into(), set))]),
            fingerprints: BTreeMap::from([(key.into(), expected)]),
        };
        let filter = Filter::new(&[], &[], &[]);
        for latched in [false, true] {
            for fingerprint in [None, Some(expected ^ 1)] {
                assert!(
                    write_sample(&mut state, key, &[], fingerprint, 1, 1, latched, &filter)
                        .is_err()
                );
                assert!(state.channels.is_empty());
            }
        }
        assert!(write_sample(&mut state, key, &[], Some(expected), 1, 1, true, &filter).unwrap());
        state.writer.finish().unwrap();
    }

    #[test]
    fn filter_types_from_exclude() {
        let f = Filter::new(&["A".into()], &["ctrl".into()], &["B".into()]);
        let mk = |src, ty| KeyParts {
            domain: "d",
            source: src,
            port: "out",
            ty,
        };
        assert!(f.accepts(&mk("ctrl", "A")));
        assert!(!f.accepts(&mk("gui", "A")), "the wrong source");
        assert!(!f.accepts(&mk("ctrl", "B")), "an excluded type");
        assert!(!f.accepts(&mk("ctrl", "C")), "not on the allow list");
        // An empty filter passes everything.
        let all = Filter::new(&[], &[], &[]);
        assert!(all.accepts(&mk("any", "Any")));
    }

    #[test]
    fn civil_matches_known_epochs() {
        assert_eq!(civil(0), (1970, 1, 1, 0, 0, 0));
        // 2025-08-27 10:14:02 UTC = 1756289642
        assert_eq!(civil(1_756_289_642), (2025, 8, 27, 10, 14, 2));
        assert_eq!(utc_stamp(1_756_289_642_000_000_000), "20250827-101402");
        // A leap day: 2024-02-29 00:00:00 UTC = 1709164800
        assert_eq!(civil(1_709_164_800), (2024, 2, 29, 0, 0, 0));
    }
}
