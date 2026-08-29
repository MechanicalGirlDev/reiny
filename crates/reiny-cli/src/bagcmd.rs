//! `reiny bag record / play / info` — recording, replaying and summarizing the bus (the equivalent of rosbag2; the format is MCAP).
//!
//! reiny's share is only what cannot be written without knowing zenoh's key shape and reiny's
//! conventions (domain / source / presence / latched / fingerprints / `@schema`). Operations that
//! need none of that vocabulary — cutting by time or topic, producing statistics — are left to the `mcap` CLI (`docs/design/bag.md`).
//!
//! Synchronous. zenoh's `.wait()` and `std::thread` are enough, and tokio is not needed (nor is it anywhere else in the CLI).

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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};
use reiny::zenoh::{self, Wait};

use crate::bus::{BusArgs, KEY_ROOT, KeyParts, attachment_u64, collect_schemas, key_source};

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
    /// Record only this type (the type segment's name, repeatable; default: every type).
    #[arg(long = "type")]
    types: Vec<String>,
    /// Record only this source id (repeatable; default: every source).
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
}

fn record(args: &RecordArgs) -> Result<()> {
    let (session, domain) = args.bus.open()?;
    let out = args
        .out
        .clone()
        .unwrap_or_else(|| PathBuf::from(format!("{}.mcap", utc_stamp(now_unix_nanos()))));

    let key = format!("{KEY_ROOT}/{domain}/*/*");
    let subscriber = session
        .declare_subscriber(&key)
        .wait()
        .map_err(anyhow::Error::msg)
        .context("declaring recording subscriber")?;

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
        schemas: BTreeMap::new(),
    };

    // Collect the schemas (descriptors) first: whatever the running publishers announce at `@schema`.
    state.schemas = collect_schemas(&session, &key);

    let filter = Filter::new(&args.types, &args.from, &args.exclude_types);
    let mut count: u64 = 0;

    // The snapshot: the latched values published before recording started go in at the front.
    if !args.no_snapshot {
        let start = now_unix_nanos();
        for (key, payload, fp) in snapshot(&session, &key) {
            if write_sample(&mut state, &key, &payload, fp, start, start, true, &filter)? {
                count += 1;
            }
        }
    }

    // Leave on Ctrl+C or --duration.
    let stop = Arc::new(AtomicBool::new(false));
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
            Ok(Some(sample)) => {
                if sample.kind() != zenoh::sample::SampleKind::Put {
                    continue;
                }
                let key = sample.key_expr().as_str().to_string();
                let payload = sample.payload().to_bytes().to_vec();
                let fp = attachment_u64(&sample);
                let log = now_unix_nanos();
                let pubt = sample.timestamp().map_or(log, timestamp_nanos);
                if write_sample(&mut state, &key, &payload, fp, log, pubt, false, &filter)? {
                    count += 1;
                }
            }
            Ok(None) => {}   // a timeout; go back and look at stop.
            Err(_) => break, // the channel is disconnected.
        }
    }

    state.writer.finish().map_err(anyhow::Error::msg)?;
    tracing::info!(messages = count, out = %out.display(), "recording finished");
    println!("{} — {count} messages", out.display());
    Ok(())
}

/// The snapshot: fire one `get` and collect the latched publishers' most recent values, as (key, payload, fingerprint).
fn snapshot(session: &zenoh::Session, key: &str) -> Vec<(String, Vec<u8>, Option<u64>)> {
    let replies = match session.get(key).wait() {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "snapshot get failed; recording without it");
            return Vec::new();
        }
    };
    let mut out = Vec::new();
    for reply in &replies {
        if let Ok(sample) = reply.result() {
            out.push((
                sample.key_expr().as_str().to_string(),
                sample.payload().to_bytes().to_vec(),
                attachment_u64(sample),
            ));
        }
    }
    out
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
    /// Rewrite every channel's source id to this (default: as recorded).
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
    /// The output key `reiny/<domain>/<source>/<TYPE>` (with --domain / --as already applied).
    key: String,
    ty: String,
    publisher: zenoh::pubsub::Publisher<'static>,
    _token: zenoh::liveliness::LivelinessToken,
    /// This channel's fingerprint (put back into the attachment).
    fingerprint: Option<u64>,
    /// For a latched channel, the data behind the queryable that answers with the most recent value sent.
    latch: Option<Arc<Mutex<Option<Vec<u8>>>>>,
    _latch_queryable: Option<zenoh::query::Queryable<()>>,
}

fn play(args: &PlayArgs) -> Result<()> {
    if args.rate <= 0.0 {
        bail!("--rate must be positive");
    }
    let bytes =
        std::fs::read(&args.file).with_context(|| format!("reading {}", args.file.display()))?;

    // Build the channel catalogue first (the type, latched and fingerprint are wanted before any message body is read).
    let summary = mcap::Summary::read(&bytes)
        .map_err(anyhow::Error::msg)?
        .context("bag has no summary section; recover it with `mcap recover`")?;
    let (session, domain) = args.bus.open()?;

    let filter = Filter::new(&args.types, &args.from, &[]);

    // Decide which channels are replayed (after filtering) and what their output keys are.
    let mut plan: BTreeMap<u16, PlannedChannel> = BTreeMap::new();
    for (id, ch) in &summary.channels {
        let Some(parts) = KeyParts::parse(&ch.topic) else {
            continue;
        };
        if !filter.accepts(&parts) {
            continue;
        }
        let source = args.as_id.as_deref().unwrap_or(parts.source);
        let key = format!("{KEY_ROOT}/{domain}/{source}/{}", parts.ty);
        plan.insert(
            *id,
            PlannedChannel {
                key,
                ty: parts.ty.to_string(),
                fingerprint: ch
                    .metadata
                    .get("reiny.schema")
                    .and_then(|s| u64::from_str_radix(s, 16).ok()),
                latched: ch.metadata.get("reiny.latched").map(String::as_str) == Some("true"),
            },
        );
    }
    if plan.is_empty() {
        bail!("nothing to play (no channels matched the filters)");
    }

    // The safety catch: refuse by default when the replay domain has a live publisher of the same type.
    // Looked at **before** declaring our own publishers (so as not to count them).
    if !args.force {
        guard_live_publishers(&session, &domain, &plan)?;
    }

    // Stand up each channel's publisher + liveliness token (+ latched queryable).
    let channels = declare_play_channels(&session, plan)?;

    let msgs = replay_order(&bytes, args.start, args.duration)?;
    if msgs.is_empty() {
        tracing::warn!("no messages in the selected range");
        return Ok(());
    }
    let first = msgs[0].0;

    loop {
        let t0 = Instant::now();
        for (log_time, channel_id, data) in &msgs {
            let Some(ch) = channels.get(channel_id) else {
                continue;
            };
            // Sleep to an absolute deadline (so it does not drift the way accumulated relative sleeps do).
            let target = t0 + Duration::from_nanos(((log_time - first) as f64 / args.rate) as u64);
            let now = Instant::now();
            if target > now {
                std::thread::sleep(target - now);
            }
            if let Some(slot) = &ch.latch
                && let Ok(mut g) = slot.lock()
            {
                *g = Some(data.clone());
            }
            let mut put = ch.publisher.put(data.clone());
            if let Some(fp) = ch.fingerprint {
                put = put.attachment(fp.to_le_bytes().to_vec());
            }
            if let Err(e) = put.wait() {
                tracing::warn!(key = %ch.key, ty = %ch.ty, error = %e, "put failed");
            }
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
    fingerprint: Option<u64>,
    latched: bool,
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
        let key = format!("{KEY_ROOT}/{domain}/*/{ty}");
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
    session: &zenoh::Session,
    plan: BTreeMap<u16, PlannedChannel>,
) -> Result<BTreeMap<u16, PlayChannel>> {
    let mut channels = BTreeMap::new();
    for (id, p) in plan {
        let publisher = session
            .declare_publisher(p.key.clone())
            .wait()
            .map_err(anyhow::Error::msg)?;
        let token = session
            .liveliness()
            .declare_token(p.key.clone())
            .wait()
            .map_err(anyhow::Error::msg)?;
        let (latch, latch_q) = if p.latched {
            let slot: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
            let q = declare_latch_queryable(session, &p.key, p.fingerprint, Arc::clone(&slot))?;
            (Some(slot), Some(q))
        } else {
            (None, None)
        };
        channels.insert(
            id,
            PlayChannel {
                key: p.key,
                ty: p.ty,
                publisher,
                _token: token,
                fingerprint: p.fingerprint,
                latch,
                _latch_queryable: latch_q,
            },
        );
    }
    Ok(channels)
}

/// The queryable for a latched channel during replay. A copy of reiny's own `declare_latch` (it just answers with the last value).
fn declare_latch_queryable(
    session: &zenoh::Session,
    key: &str,
    fingerprint: Option<u64>,
    slot: Arc<Mutex<Option<Vec<u8>>>>,
) -> Result<zenoh::query::Queryable<()>> {
    let reply_key = key.to_string();
    session
        .declare_queryable(key.to_string())
        .callback(move |query| {
            let payload = slot.lock().ok().and_then(|g| g.clone());
            let Some(bytes) = payload else { return };
            let mut reply = query.reply(reply_key.clone(), bytes);
            if let Some(fp) = fingerprint {
                reply = reply.attachment(fp.to_le_bytes().to_vec());
            }
            if let Err(e) = reply.wait() {
                tracing::warn!(key = %reply_key, error = %e, "latched reply failed");
            }
        })
        .wait()
        .map_err(anyhow::Error::msg)
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
                source: m.get("reiny.source").cloned().unwrap_or_default(),
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

fn timestamp_nanos(ts: &zenoh::time::Timestamp) -> u64 {
    ts.get_time()
        .to_system_time()
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

    #[test]
    fn filter_types_from_exclude() {
        let f = Filter::new(&["A".into()], &["ctrl".into()], &["B".into()]);
        let mk = |src, ty| KeyParts {
            domain: "d",
            source: src,
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
