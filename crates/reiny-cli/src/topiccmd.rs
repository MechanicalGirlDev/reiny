//! `reiny topic list / hz / bw` と `reiny node list / info` —— `ros2 topic` / `ros2 node` 相当。
//!
//! GUI を上げずにバスを覗く口。`list` / `node` は liveliness(presence)を撃つだけ、
//! `hz` / `bw` は raw 購読 1 本(`reiny bag record` と同じ)を **送信元ごと**に集計する ——
//! `*` で複数 publisher が混ざるので、合算ではなく行を分ける。時刻は受信側の `Instant`
//! (zenoh の timestamping は既定 off なので依存しない)。

#![allow(clippy::cast_precision_loss)] // 件数 → f64 は統計表示のみ。

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use reiny::zenoh::{self, Wait};

use crate::bus::{
    BusArgs, KEY_ROOT, KeyParts, LAUNCH_CHUNK, SERVICE_CHUNK, alive_keys, attachment_u64,
    collect_schemas, key_source,
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
    /// 生きている型ごとに、publisher と server の launch id を並べる。
    List(ListArgs),
    /// 1 型の受信レート(送信元ごと)。Ctrl+C か `--duration` で終了。
    Hz(RateArgs),
    /// 1 型の受信帯域(送信元ごと)。Ctrl+C か `--duration` で終了。
    Bw(RateArgs),
    /// 1 型の中身を JSON で流す(publisher が `@schema` で名乗る descriptor で decode)。
    Echo(EchoArgs),
}

#[derive(Args)]
struct EchoArgs {
    /// 型名(キーの型セグメント。例 `RobotState`)。
    ty: String,
    /// この launch id からのものだけ。
    #[arg(long)]
    from: Option<String>,
    /// この件数で終了する。
    #[arg(long)]
    count: Option<usize>,
    /// この秒数で終了する。
    #[arg(long)]
    duration: Option<f64>,
    /// decode せず payload を hex で出す。
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
    /// 型名(キーの型セグメント。例 `RobotState`)。
    ty: String,
    /// この launch id からのものだけを数える(既定は全 publisher、送信元ごとに集計)。
    #[arg(long)]
    from: Option<String>,
    /// 統計の窓(秒)。
    #[arg(long, default_value_t = 10.0)]
    window: f64,
    /// この秒数で終了する(既定は Ctrl+C まで)。
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
    }
}

fn echo(args: &EchoArgs) -> Result<()> {
    let (session, domain) = args.bus.open()?;
    let pattern = format!(
        "{KEY_ROOT}/{domain}/{}/{}",
        args.from.as_deref().unwrap_or("*"),
        args.ty
    );
    let subscriber = session
        .declare_subscriber(&pattern)
        .wait()
        .map_err(anyhow::Error::msg)
        .with_context(|| format!("declaring subscriber {pattern}"))?;

    let stop = Arc::new(AtomicBool::new(false));
    {
        let stop = Arc::clone(&stop);
        ctrlc::set_handler(move || stop.store(true, Ordering::SeqCst))
            .context("installing Ctrl+C handler")?;
    }
    let deadline = args
        .duration
        .map(|s| Instant::now() + Duration::from_secs_f64(s));

    // キー → decoder。descriptor を名乗らない publisher は None(hex で出し、1 度だけ案内する)。
    // 途中参加の publisher は初見のキーで @schema を撃ち直す。
    let mut codecs: BTreeMap<String, Option<Codec>> = BTreeMap::new();
    let fetch = |key: &str, codecs: &mut BTreeMap<String, Option<Codec>>| {
        if !args.raw && !codecs.contains_key(key) {
            let found = collect_schemas(&session, key)
                .remove(key)
                .and_then(|(fqn, set)| match Codec::from_file_set(&set, &fqn) {
                    Ok(c) => Some(c),
                    Err(e) => {
                        tracing::warn!(key, error = %e, "unusable @schema; showing hex");
                        None
                    }
                });
            if found.is_none() {
                eprintln!(
                    "{key}: publisher does not describe its type on the bus (no DESCRIPTOR); showing hex"
                );
            }
            codecs.insert(key.to_string(), found);
        }
    };
    let mut warned_fp: BTreeSet<String> = BTreeSet::new();
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
                let source = key_source(&key).to_string();
                let payload = sample.payload().to_bytes();
                fetch(&key, &mut codecs);
                let text = match codecs.get(&key).and_then(Option::as_ref) {
                    Some(codec) => {
                        if let (Some(mine), Some(theirs)) =
                            (codec.fingerprint, attachment_u64(&sample))
                            && mine != theirs
                            && warned_fp.insert(source.clone())
                        {
                            tracing::warn!(
                                source,
                                expected = format!("{mine:016x}"),
                                received = format!("{theirs:016x}"),
                                "schema fingerprint differs from the @schema descriptor; a typed subscriber would drop this"
                            );
                        }
                        codec.decode_json(&payload).unwrap_or_else(|e| {
                            tracing::warn!(source, error = %e, "undecodable payload; showing hex");
                            hex(&payload)
                        })
                    }
                    None => hex(&payload),
                };
                println!("{source}  {text}");
                shown += 1;
            }
            Ok(None) => {}
            Err(_) => break,
        }
    }
    Ok(())
}

fn list(args: &ListArgs) -> Result<()> {
    let (session, domain) = args.bus.open()?;
    let pubs = alive_keys(&session, &format!("{KEY_ROOT}/{domain}/*/*"))?;
    let srvs = alive_keys(
        &session,
        &format!("{KEY_ROOT}/{domain}/*/*/{SERVICE_CHUNK}"),
    )?;

    // (domain, type) → (publishers, servers)
    let mut rows: BTreeMap<(String, String), (Vec<String>, Vec<String>)> = BTreeMap::new();
    for k in &pubs {
        if let Some(p) = KeyParts::parse(k) {
            rows.entry((p.domain.to_string(), p.ty.to_string()))
                .or_default()
                .0
                .push(p.source.to_string());
        }
    }
    for k in &srvs {
        if let Some(p) = KeyParts::parse_with_chunk(k, SERVICE_CHUNK) {
            rows.entry((p.domain.to_string(), p.ty.to_string()))
                .or_default()
                .1
                .push(p.source.to_string());
        }
    }
    if rows.is_empty() {
        println!("(no live publishers or servers in domain {domain})");
        return Ok(());
    }

    // domain にワイルドカードがあるときだけ DOMAIN 列を出す。
    let show_domain = domain.contains('*');
    let w_dom = rows.keys().map(|(d, _)| d.len()).max().unwrap_or(0).max(6);
    let w_ty = rows.keys().map(|(_, t)| t.len()).max().unwrap_or(0).max(4);
    let w_pub = rows
        .values()
        .map(|(p, _)| p.join(" ").len())
        .max()
        .unwrap_or(0)
        .max(3);
    if show_domain {
        println!(
            "{:<w_dom$}  {:<w_ty$}  {:<w_pub$}  SRV",
            "DOMAIN", "TYPE", "PUB"
        );
    } else {
        println!("{:<w_ty$}  {:<w_pub$}  SRV", "TYPE", "PUB");
    }
    for ((d, t), (p, s)) in &rows {
        let p = if p.is_empty() {
            "-".to_string()
        } else {
            p.join(" ")
        };
        let s = if s.is_empty() {
            "-".to_string()
        } else {
            s.join(" ")
        };
        if show_domain {
            println!("{d:<w_dom$}  {t:<w_ty$}  {p:<w_pub$}  {s}");
        } else {
            println!("{t:<w_ty$}  {p:<w_pub$}  {s}");
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
    let (session, domain) = args.bus.open()?;
    let key = format!(
        "{KEY_ROOT}/{domain}/{}/{}",
        args.from.as_deref().unwrap_or("*"),
        args.ty
    );
    let subscriber = session
        .declare_subscriber(&key)
        .wait()
        .map_err(anyhow::Error::msg)
        .with_context(|| format!("declaring subscriber {key}"))?;

    let stop = Arc::new(AtomicBool::new(false));
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
    tracing::info!(key = %key, "listening; Ctrl+C to stop");
    while !stop.load(Ordering::SeqCst) {
        if deadline.is_some_and(|d| Instant::now() >= d) {
            break;
        }
        match subscriber.recv_timeout(Duration::from_millis(100)) {
            Ok(Some(sample)) => {
                if sample.kind() != zenoh::sample::SampleKind::Put {
                    continue;
                }
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

/// 1 送信元の受信履歴(窓の中だけ)。
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

    /// `ros2 topic hz` と同じ: 到着間隔の平均の逆数、最小、最大、標準偏差。
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

    /// 窓の先頭〜末尾の間に受けたバイト数 / その時間。
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
    /// 生きている launch id を並べる。
    List(ListArgs),
    /// 1 launch が publish / serve している型。
    Info {
        /// launch id。
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
    // 0.5 の launch は @launch を名乗る。それ以前(@grain / トークン無し)も
    // publisher / server のトークンから拾う。
    for pattern in [
        format!("{KEY_ROOT}/{domain}/*/{LAUNCH_CHUNK}"),
        format!("{KEY_ROOT}/{domain}/*/*"),
        format!("{KEY_ROOT}/{domain}/*/*/{SERVICE_CHUNK}"),
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
    let srvs: Vec<String> = alive_keys(
        &session,
        &format!("{KEY_ROOT}/{domain}/{id}/*/{SERVICE_CHUNK}"),
    )?
    .iter()
    .filter_map(|k| KeyParts::parse_with_chunk(k, SERVICE_CHUNK).map(|p| p.ty.to_string()))
    .collect();
    if !alive && pubs.is_empty() && srvs.is_empty() {
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
    println!(
        "pub : {}",
        if pubs.is_empty() {
            "-".to_string()
        } else {
            pubs.join(" ")
        }
    );
    println!(
        "srv : {}",
        if srvs.is_empty() {
            "-".to_string()
        } else {
            srvs.join(" ")
        }
    );
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::float_cmp)]
mod tests {
    use super::*;

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
        assert_eq!(s.len(), 1, "窓の外の 1 件目は落ちる");
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
