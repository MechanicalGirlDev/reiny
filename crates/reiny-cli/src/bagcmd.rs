//! `reiny bag record / play / info` —— バスの記録・再生・要約(rosbag2 相当、形式は MCAP)。
//!
//! reiny が担うのは、zenoh のキーの形と reiny の約束事(domain / 送信元 / presence / latched /
//! 指紋 / `@schema`)を知らないと書けない部分だけ。時間やトピックで切る・統計を出すといった
//! 「バスの語彙を要さない」操作は `mcap` CLI に委ねる(`docs/design/bag.md`)。
//!
//! 同期。zenoh の `.wait()` と `std::thread` で足り、tokio は要らない(CLI 全体も tokio 無し)。

// この file の数値キャストはすべて時刻(ns)・速度(rate)・日付の境界済み変換で、個々を
// try_from にしても後ろに .unwrap() が並ぶだけ。civil() の 1 文字束縛も暦の慣用表記。
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
use reiny::{RuntimeOptions, ZenohSource};

/// キーのプレフィクスと、脇道(`@schema`)のチャンク名。reiny 本体と揃える。
const KEY_ROOT: &str = "reiny";
const SCHEMA_CHUNK: &str = "@schema";

#[derive(Args)]
pub(crate) struct BagArgs {
    #[command(subcommand)]
    command: BagCommand,
}

#[derive(Subcommand)]
enum BagCommand {
    /// バスを購読して MCAP へ録る(Ctrl+C か `--duration` で終了)。
    Record(RecordArgs),
    /// MCAP をバスへ再生する。
    Play(PlayArgs),
    /// MCAP の中身を要約する。
    Info(InfoArgs),
}

/// grain と同じ綴りの fabric 引数。3 サブコマンドで共通。
#[derive(Args, Clone)]
struct BusArgs {
    /// 論理名前空間(既定: `--domain` > `REINY_DOMAIN` > "default")。
    #[arg(long)]
    domain: Option<String>,
    /// zenoh 設定ファイル(JSON5 / JSON / YAML)。
    #[arg(long)]
    zenoh_config: Option<PathBuf>,
    /// 接続先エンドポイント(繰り返し可、例 `tcp/127.0.0.1:7447`)。
    #[arg(long)]
    connect: Vec<String>,
    /// zenoh の動作モード(`peer` / `client` / `router`)。
    #[arg(long)]
    zenoh_mode: Option<String>,
}

impl BusArgs {
    /// fabric 引数から (zenoh 設定, 解決済み domain) を組む。`RuntimeOptions` を経由するので
    /// 既定値・`REINY_DOMAIN`・`--connect` の json5 化は grain と同じ経路になる。
    fn open(&self) -> Result<(zenoh::Session, String)> {
        let mut opts = RuntimeOptions::new("reiny-bag");
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
    /// 出力ファイル(既定: `<yyyymmdd-HHMMSS>.mcap`、UTC)。
    #[arg(long)]
    out: Option<PathBuf>,
    /// この型だけ録る(型セグメント名、繰り返し可。既定: 全型)。
    #[arg(long = "type")]
    types: Vec<String>,
    /// この送信元 id だけ録る(繰り返し可。既定: 全送信元)。
    #[arg(long)]
    from: Vec<String>,
    /// この型は録らない(繰り返し可)。
    #[arg(long = "exclude-type")]
    exclude_types: Vec<String>,
    /// この秒数で自動終了(既定: Ctrl+C まで)。
    #[arg(long)]
    duration: Option<f64>,
    /// 記録開始前の latched 値(snapshot)を録らない。
    #[arg(long)]
    no_snapshot: bool,
    #[command(flatten)]
    bus: BusArgs,
}

/// 記録中のチャネル状態。MCAP の `channel_id` と、スキーマ登録済みかを覚える。
struct RecordChannels<W: std::io::Write + std::io::Seek> {
    writer: mcap::Writer<W>,
    /// キー → (`channel_id`, `sequence`)。
    channels: BTreeMap<String, (u16, u32)>,
    /// 取得済みの descriptor set(FQN → subset bytes)。チャネル作成時にスキーマへ回す。
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

    // スキーマ(descriptor)を先に集める。走っている publisher が `@schema` で名乗るものを拾う。
    collect_schemas(&session, &domain, &mut state.schemas);

    let filter = Filter::new(&args.types, &args.from, &args.exclude_types);
    let mut count: u64 = 0;

    // snapshot: 記録開始前に publish された latched 値を先頭に入れる。
    if !args.no_snapshot {
        let start = now_unix_nanos();
        for (key, payload, fp) in snapshot(&session, &key) {
            if write_sample(&mut state, &key, &payload, fp, start, start, true, &filter)? {
                count += 1;
            }
        }
    }

    // Ctrl+C か --duration で抜ける。
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
            Ok(None) => {}   // タイムアウト。stop を見に戻る。
            Err(_) => break, // チャネル切断。
        }
    }

    state.writer.finish().map_err(anyhow::Error::msg)?;
    tracing::info!(messages = count, out = %out.display(), "recording finished");
    println!("{} — {count} messages", out.display());
    Ok(())
}

/// snapshot: `get` を 1 発撃って latched publisher の直近値を集める。返りは (key, payload, 指紋)。
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

/// `@schema` を 1 発撃って、走っている publisher が名乗る descriptor set を集める。
/// キー `<...>/@schema/<fqn>` から fqn を取り、必要ファイルだけに刈って覚える。
fn collect_schemas(
    session: &zenoh::Session,
    domain: &str,
    schemas: &mut BTreeMap<String, (String, Vec<u8>)>,
) {
    let key = format!("{KEY_ROOT}/{domain}/*/*/{SCHEMA_CHUNK}/*");
    let replies = match session.get(&key).wait() {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "schema get failed; recording without schemas");
            return;
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
}

/// 1 サンプルを MCAP へ書く。初見キーはチャネル(＋あればスキーマ)を作る。フィルタで落ちれば false。
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
        None => return Ok(false), // 直前に insert したので届かないはずだが、panic はしない。
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
    /// 再生する MCAP。
    file: PathBuf,
    /// 全チャネルの送信元 id をこれに書き換える(既定: 録画時のまま)。
    #[arg(long = "as")]
    as_id: Option<String>,
    /// 再生速度(1.0 = 実時間、2.0 = 倍速)。
    #[arg(long, default_value_t = 1.0)]
    rate: f64,
    /// 末尾まで行ったら先頭へ戻る。
    #[arg(long = "loop")]
    looping: bool,
    /// 先頭からこの秒数を読み飛ばす。
    #[arg(long)]
    start: Option<f64>,
    /// 再生する長さ(秒。`--start` から数える)。
    #[arg(long)]
    duration: Option<f64>,
    /// この型だけ再生(繰り返し可)。
    #[arg(long = "type")]
    types: Vec<String>,
    /// この送信元だけ再生(繰り返し可。書き換え前の id で判定)。
    #[arg(long)]
    from: Vec<String>,
    /// 生きた publisher が居る domain へも流す(安全弁を外す)。
    #[arg(long)]
    force: bool,
    #[command(flatten)]
    bus: BusArgs,
}

/// 再生用に各チャネルの出力キーと reiny の約束事を保持する。
struct PlayChannel {
    /// 出力キー `reiny/<domain>/<source>/<TYPE>`(--domain / --as を反映済み)。
    key: String,
    ty: String,
    publisher: zenoh::pubsub::Publisher<'static>,
    _token: zenoh::liveliness::LivelinessToken,
    /// このチャネルの指紋(attachment に戻す)。
    fingerprint: Option<u64>,
    /// latched チャネルなら、直近に流した値を返す queryable の元データ。
    latch: Option<Arc<Mutex<Option<Vec<u8>>>>>,
    _latch_queryable: Option<zenoh::query::Queryable<()>>,
}

fn play(args: &PlayArgs) -> Result<()> {
    if args.rate <= 0.0 {
        bail!("--rate must be positive");
    }
    let bytes =
        std::fs::read(&args.file).with_context(|| format!("reading {}", args.file.display()))?;

    // チャネルの目録を先に作る(メッセージ本体を読む前に、型・latched・指紋を知りたい)。
    let summary = mcap::Summary::read(&bytes)
        .map_err(anyhow::Error::msg)?
        .context("bag has no summary section; recover it with `mcap recover`")?;
    let (session, domain) = args.bus.open()?;

    let filter = Filter::new(&args.types, &args.from, &[]);

    // どのチャネルを再生するか(フィルタ後)と、その出力キーを決める。
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

    // 安全弁: 同じ型の生きた publisher が再生先 domain に居るなら、既定で拒否する。
    // publisher を宣言する **前** に見る(自分のを数えないため)。
    if !args.force {
        guard_live_publishers(&session, &domain, &plan)?;
    }

    // 各チャネルの publisher + liveliness トークン(+ latched queryable)を建てる。
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
            // 絶対期限へスリープ(相対 sleep の累積ではないのでドリフトしない)。
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

/// フィルタ通過後・宣言前のチャネル情報。
struct PlannedChannel {
    key: String,
    ty: String,
    fingerprint: Option<u64>,
    latched: bool,
}

/// 各 planned チャネルについて、再生先 domain に**自分以外の**生きた publisher が居ないか見る。
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

/// 再生中の latched チャネル用 queryable。reiny 本体の `declare_latch` の写し(直近値を返すだけ)。
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

/// MCAP を線形に読み、`--start` / `--duration` で範囲を切って (`log_time`, `channel_id`, `data`) を返す。
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
    /// 要約する MCAP。
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

    // domain はチャネル metadata から(全チャネル同じはず。違えば列挙)。
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

    // 行はチャネル。送信元 / 型 / 件数 / 平均 Hz / latched / スキーマ + 指紋。
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
        // 記述子(あれば型名)と指紋(あれば)は独立に出す —— 段 1 の bag は指紋だけ載る。
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
// 共有の小道具
// ===========================================================================

/// キー `reiny/<domain>/<source>/<TYPE>` の 3 セグメント。`@schema` 付きは弾く(None)。
struct KeyParts<'a> {
    domain: &'a str,
    source: &'a str,
    ty: &'a str,
}

impl<'a> KeyParts<'a> {
    fn parse(key: &'a str) -> Option<Self> {
        let mut segs = key.split('/');
        if segs.next()? != KEY_ROOT {
            return None;
        }
        let domain = segs.next()?;
        let source = segs.next()?;
        let ty = segs.next()?;
        // 4 段ちょうど(脇道 `/@schema/...` が付いていたら記録対象ではない)。
        if segs.next().is_some() {
            return None;
        }
        Some(Self { domain, source, ty })
    }
}

/// `reiny/<domain>/<id>/<TYPE>` から `<id>` を取る(presence 判定用)。
fn key_source(key: &str) -> &str {
    key.split('/').nth(2).unwrap_or_default()
}

/// 型 / 送信元 / 除外型のフィルタ。空の許可リストは「全部」。
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

/// zenoh の attachment を reiny の指紋(8 バイト LE)として読む。形が違えば `None`。
fn attachment_u64(sample: &zenoh::sample::Sample) -> Option<u64> {
    let bytes = sample.attachment()?.to_bytes();
    <[u8; 8]>::try_from(bytes.as_ref())
        .ok()
        .map(u64::from_le_bytes)
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

/// Unix ナノ秒 → `yyyymmdd-HHMMSS`(UTC)。既定ファイル名用。
fn utc_stamp(ns: u64) -> String {
    let (y, mo, d, h, mi, s) = civil(ns / 1_000_000_000);
    format!("{y:04}{mo:02}{d:02}-{h:02}{mi:02}{s:02}")
}

/// Unix ナノ秒 → `yyyy-mm-dd HH:MM:SS`(UTC)。info の表示用。
fn utc_full(ns: u64) -> String {
    let (y, mo, d, h, mi, s) = civil(ns / 1_000_000_000);
    format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}")
}

/// Unix 秒 → (年, 月, 日, 時, 分, 秒) UTC。Howard Hinnant の `civil_from_days`。
/// `time` クレートを引かないための 15 行(bag のファイル名と表示にしか要らない)。
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
    fn key_parts_parse_and_reject_schema_chunk() {
        let p = KeyParts::parse("reiny/lab/ctrl/RobotState").unwrap();
        assert_eq!((p.domain, p.source, p.ty), ("lab", "ctrl", "RobotState"));
        // 脇道は記録対象ではない。
        assert!(KeyParts::parse("reiny/lab/ctrl/RobotState/@schema/hs.RobotState").is_none());
        assert!(KeyParts::parse("reiny/lab/ctrl").is_none());
        assert!(KeyParts::parse("other/lab/ctrl/T").is_none());
    }

    #[test]
    fn filter_types_from_exclude() {
        let f = Filter::new(&["A".into()], &["ctrl".into()], &["B".into()]);
        let mk = |src, ty| KeyParts {
            domain: "d",
            source: src,
            ty,
        };
        assert!(f.accepts(&mk("ctrl", "A")));
        assert!(!f.accepts(&mk("gui", "A")), "送信元が違う");
        assert!(!f.accepts(&mk("ctrl", "B")), "除外型");
        assert!(!f.accepts(&mk("ctrl", "C")), "許可リスト外");
        // 空フィルタは全通し。
        let all = Filter::new(&[], &[], &[]);
        assert!(all.accepts(&mk("any", "Any")));
    }

    #[test]
    fn civil_matches_known_epochs() {
        assert_eq!(civil(0), (1970, 1, 1, 0, 0, 0));
        // 2025-08-27 10:14:02 UTC = 1756289642
        assert_eq!(civil(1_756_289_642), (2025, 8, 27, 10, 14, 2));
        assert_eq!(utc_stamp(1_756_289_642_000_000_000), "20250827-101402");
        // 閏日: 2024-02-29 00:00:00 UTC = 1709164800
        assert_eq!(civil(1_709_164_800), (2024, 2, 29, 0, 0, 0));
    }
}
