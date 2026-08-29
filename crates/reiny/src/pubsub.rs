//! 型付き publisher / subscriber と、その builder・presence。
//!
//! キーの形は `reiny/<domain>/<id>/<TYPE>`(publish)/ `reiny/<domain>/*/<TYPE>`(subscribe)。
//! 組み立ては [`Cloudy::key_for`](crate::Cloudy) が一手に引き受ける。バスへの出入りは
//! [`Engine`](crate::engine::Engine) 越し —— ここにあるのは encode / decode、指紋の照合、
//! latched の「presence を見てから get」、latest-wins、受信バッファ(Fifo / Ring)で、
//! どのエンジンでも同じ実装が動く。

use std::collections::{HashSet, VecDeque};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use prost::Message;
use tokio::sync::{Notify, mpsc};

use crate::engine::{
    Callback, Engine, Guard, Key, Presence as RawPresence, QueryParams, RawPublisher, RawQuery,
    RawReplies, ReplyResult, SCHEMA_CHUNK, Sample,
};
use crate::shutdown::Shutdown;
use crate::{Cloudy, Descriptor, Durability, History, Priority, Qos, Reliability, Result, Topic};

/// 既定の受信バッファ(Fifo)の深さ。zenoh の `API_DATA_RECEPTION_CHANNEL_SIZE` と同じ。
const FIFO_CAPACITY: usize = 256;
/// latched の問い合わせを諦めるまで。zenoh の `get` の既定と同じ。
const LATCHED_TIMEOUT: Duration = Duration::from_secs(10);

/// 受信メッセージと、reiny が知っている来歴。
///
/// `source` / `timestamp` は 0.2 では [`Subscriber::recv`] の中で捨てられていた情報なので、
/// 取り出すのに追加の実行コストは無い。
pub struct Envelope<T> {
    /// decode 済みのメッセージ本体。
    pub value: T,
    /// 送信元 launch の id(キーの `<id>` セグメント)。
    pub source: String,
    /// 送信時刻(unix ns)。エンジンが持つときだけ載る(zenoh は timestamping が有効なとき)。
    pub timestamp: Option<u64>,
}

/// `T::SCHEMA` を attachment の形(8 バイト LE)に。
pub(crate) fn fingerprint(schema: Option<u64>) -> Option<Vec<u8>> {
    schema.map(|f| f.to_le_bytes().to_vec())
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
// publisher
// ---------------------------------------------------------------------------

/// [`Cloudy::publisher`] が返す builder。何も指定しなければ [`Cloudy::publish`] と同じ。
#[must_use = "builder は .build() するまで何もしない"]
pub struct PublisherBuilder<'a, T> {
    cloudy: &'a Cloudy,
    qos: Qos,
    _marker: PhantomData<T>,
}

impl<'a, T> PublisherBuilder<'a, T> {
    pub(crate) fn new(cloudy: &'a Cloudy) -> Self {
        Self {
            cloudy,
            qos: Qos::DEFAULT,
            _marker: PhantomData,
        }
    }

    /// `QoS` をまとめて指定する —— [`Qos::SENSOR`] / [`Qos::COMMAND`] / [`Qos::STATE`] の
    /// プロファイルか、自前の [`Qos`]。後から呼ぶ糖衣(`.latched()` 等)はこの上に重なる。
    pub fn qos(mut self, qos: Qos) -> Self {
        self.qos = qos;
        self
    }

    /// 直近 1 件を保持し、遅れて来た購読者の問い合わせに答える(latched)。
    /// 「起動時に 1 回配れば済む設定」を定期再送し続けるタスクの代わり。履歴は 1 件だけ。
    /// = `durability: TransientLocal`。
    pub fn latched(mut self) -> Self {
        self.qos.durability = Durability::TransientLocal;
        self
    }

    /// 輻輳時に捨てるか待つか。= `Qos.reliability`。
    pub fn reliability(mut self, reliability: Reliability) -> Self {
        self.qos.reliability = reliability;
        self
    }

    /// 送信優先度。= `Qos.priority`。
    pub fn priority(mut self, priority: Priority) -> Self {
        self.qos.priority = priority;
        self
    }

    /// バッチングを飛ばして即時送信する(低レイテンシ・低スループット)。= `Qos.express`。
    pub fn express(mut self, express: bool) -> Self {
        self.qos.express = express;
        self
    }

    /// publisher を宣言する。同じキーの liveliness トークンも必ず同伴する
    /// ([`Cloudy::publishers`] の土台。opt-out は無い)。
    ///
    /// `history: KeepLast(n > 1)` はエラー —— publisher が持つのは latched の 1 件までで、
    /// n 件のリングは購読側 `.latest(n)` の仕事(黙って 1 に丸めない)。エンジンに無い機能
    /// (latched に要る query、presence に要る liveliness)もここでエラーにする。
    pub fn build(self) -> Result<Publisher<T>>
    where
        T: Message + Topic,
    {
        if let History::KeepLast(n) = self.qos.history
            && n != 1
        {
            anyhow::bail!(
                "publisher of {}: history KeepLast({n}) is not supported — a publisher keeps at \
                 most 1 (latched); use the subscriber's `.latest({n})` for a ring",
                T::TYPE
            );
        }
        let latched = self.qos.durability == Durability::TransientLocal;
        let engine = self.cloudy.engine();
        let caps = engine.caps();
        if !caps.liveliness {
            anyhow::bail!(
                "publisher of {}: this engine has no liveliness, which presence needs",
                T::TYPE
            );
        }
        if latched && !caps.query {
            anyhow::bail!(
                "publisher of {}: this engine has no query support, which latched needs",
                T::TYPE
            );
        }
        let key = self.cloudy.key_for(Some(self.cloudy.id()), T::TYPE);
        let publisher = engine.publisher(&key, &self.qos)?;

        let last: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
        let queryable = if latched {
            Some(declare_latch(
                self.cloudy,
                &key,
                Arc::clone(&last),
                T::SCHEMA,
            )?)
        } else {
            None
        };

        // トークンは latched queryable の**後**に宣言する。購読側は presence(このトークン)を
        // 合図に直近値を問い合わせるので、逆順だと「居るのに queryable はまだ届いていない」
        // 一瞬に問い合わせが飛んで空振りする。同じリンク上の宣言は順序が保たれる。
        let token = engine.declare_alive(&key)?;
        // `@schema` は診断用なので、query の無いエンジンでは黙って立てない(エラーにしない ——
        // `reiny-build` 生成型は全部 DESCRIPTOR を持つ)。
        let schema = match T::DESCRIPTOR {
            Some(descriptor) if caps.query => Some(declare_schema(self.cloudy, &key, descriptor)?),
            _ => None,
        };

        tracing::debug!(key = %key, latched, "publisher declared");
        Ok(Publisher {
            raw: publisher,
            _token: token,
            queryable,
            _schema: schema,
            last,
            _marker: PhantomData,
        })
    }
}

/// [`Topic::DESCRIPTOR`] を持つ型の publisher が、自分のキーの脇
/// `<key>/@schema/<message>` で descriptor set を名乗るための queryable。
///
/// `@schema` は verbatim チャンク —— `*` / `**` のどちらにもマッチしないので、型のトピックを
/// 購読・記録している誰にも見えない。拾うのは `reiny bag record` のように
/// `reiny/<domain>/*/*/@schema/*` と明示して問い合わせる側だけ。
pub(crate) fn declare_schema(cloudy: &Cloudy, key: &Key, descriptor: Descriptor) -> Result<Guard> {
    let reply_key = key.with_chunk(format!("{SCHEMA_CHUNK}/{}", descriptor.message));
    let callback_key = reply_key.clone();
    cloudy.engine().respond(
        &reply_key,
        Box::new(move |query: Box<dyn RawQuery>| {
            if let Err(e) = query.reply(&callback_key, descriptor.file_set.to_vec(), None) {
                tracing::warn!(key = %callback_key, error = %e, "schema reply failed");
            }
        }),
    )
}

/// latched publisher の裏側 —— 自分の publish キーに queryable を 1 本立て、
/// 最後に送った値をそのまま返すだけ。
fn declare_latch(
    cloudy: &Cloudy,
    key: &Key,
    last: Arc<Mutex<Option<Vec<u8>>>>,
    schema: Option<u64>,
) -> Result<Guard> {
    let reply_key = key.clone();
    cloudy.engine().respond(
        key,
        Box::new(move |query: Box<dyn RawQuery>| {
            // payload 付きの query は service の呼び出し(`service.rs`)。同じ型を latched publish
            // しつつ serve する launch で、呼び出しに直近値を返してしまわないよう無視する。
            if query.payload().is_some() {
                return;
            }
            let Some(bytes) = lock(&last).clone() else {
                return;
            };
            // ライブ経路と同じ指紋を載せる。載せないと latched 応答だけ照合を素通りする。
            if let Err(e) = query.reply(&reply_key, bytes, fingerprint(schema)) {
                tracing::warn!(key = %reply_key, error = %e, "latched reply failed");
            }
        }),
    )
}

/// 型付き publisher。[`Cloudy::publish`] / [`PublisherBuilder::build`] で得る。
///
/// drop すると liveliness トークンも落ちるので、購読側の [`Cloudy::watch_publishers`] に
/// [`PresenceEvent::Left`] が届く。
pub struct Publisher<T> {
    raw: Box<dyn RawPublisher>,
    /// publisher と生死を共にする presence トークン(保持するだけ)。
    _token: Guard,
    /// latched のときだけ立つ、直近値を返す queryable(保持するだけ)。
    queryable: Option<Guard>,
    /// `T::DESCRIPTOR` があるときだけ立つ、`@schema` で descriptor を名乗る queryable(保持するだけ)。
    _schema: Option<Guard>,
    last: Arc<Mutex<Option<Vec<u8>>>>,
    _marker: PhantomData<T>,
}

impl<T: Message + Topic> Publisher<T> {
    /// メッセージを encode して発行する。`T::SCHEMA` があれば指紋を attachment に載せる。
    ///
    /// `Reliable`(既定)なら送信路が詰まっている間ブロックする。`async` なのは API の形を
    /// 保つため —— 今のエンジンに await 地点は無い。
    #[allow(clippy::unused_async)] // 0.4 からの API。エンジンが本当に待つ日のために残す。
    pub async fn send(&self, message: T) -> Result<()> {
        let buf = message.encode_to_vec();
        if self.queryable.is_some() {
            *lock(&self.last) = Some(buf.clone());
        }
        self.raw.put(buf, fingerprint(T::SCHEMA))
    }
}

// ---------------------------------------------------------------------------
// subscriber
// ---------------------------------------------------------------------------

/// [`Cloudy::subscriber`] が返す builder。何も指定しなければ [`Cloudy::subscribe`] と同じ。
#[must_use = "builder は .build() するまで何もしない"]
pub struct SubscriberBuilder<'a, T> {
    cloudy: &'a Cloudy,
    from: Option<String>,
    latched: bool,
    latest: Option<usize>,
    _marker: PhantomData<T>,
}

impl<'a, T> SubscriberBuilder<'a, T> {
    pub(crate) fn new(cloudy: &'a Cloudy) -> Self {
        Self {
            cloudy,
            from: None,
            latched: false,
            latest: None,
            _marker: PhantomData,
        }
    }

    /// 特定の launch id だけを購読する。同じ型を複数の launch が publish する構成で、
    /// 購読側が出し手を選ぶための指定。
    pub fn from(mut self, id: impl Into<String>) -> Self {
        self.from = Some(id.into());
        self
    }

    /// latched publisher の直近値を受け取ってからライブ購読に入る。
    ///
    /// 問い合わせ(`get`)は**宣言直後ではなく、その型の publisher の presence を見てから**撃つ。
    /// `get` はその瞬間のルーティング表しか見ないので、宣言直後に撃つと「セッションは開いたが
    /// publisher が居る peer とのリンクがまだ張れていない」一瞬に空振りし、**publisher が
    /// 再送しない限り恒久的に黙る**(latched の存在意義そのものが消える)。presence は
    /// liveliness の**購読**なので、後から張れたリンクの宣言もちゃんと届く —— この非対称性が
    /// 穴を塞ぐ。publisher が増えるたびに、その id へ 1 回だけ問い合わせ直す。
    pub fn latched(mut self) -> Self {
        self.latched = true;
        self
    }

    /// 直近 `n` 件だけを保持し、溢れたら**最古を捨てる**(ROS 2 の `KEEP_LAST(n)`)。
    ///
    /// 既定(未指定)は Fifo(256 件)で、**満杯になるとエンジンの受信スレッドがブロックし、
    /// その launch の全購読が詰まる**。高レートの状態量を自分の周期でしか読まない
    /// 購読(GUI が 100Hz の `RobotState` を描画周期で読む等)は `latest(1)` にする。
    /// コマンド系は既定のまま —— 黙って落ちる方が制御では危ない。
    pub fn latest(mut self, n: usize) -> Self {
        self.latest = Some(n.max(1));
        self
    }

    /// subscriber を宣言する。エンジンに無い機能(`*` 購読、latched)はここでエラー。
    pub fn build(self) -> Result<Subscriber<T>>
    where
        T: Message + Default + Topic,
    {
        let engine = self.cloudy.engine();
        let caps = engine.caps();
        if self.from.is_none() && !caps.wildcard_source {
            anyhow::bail!(
                "subscriber of {}: this engine is point-to-point; name the source with `.from(id)`",
                T::TYPE
            );
        }
        if self.latched && !(caps.query && caps.liveliness) {
            anyhow::bail!(
                "subscriber of {}: this engine has no query / liveliness, which latched needs",
                T::TYPE
            );
        }
        let key = self.cloudy.key_for(self.from.as_deref(), T::TYPE);

        // 受信バッファは reiny が持つ。engine の callback はここに積むだけ。
        let (chan, on_sample): (Chan, Callback<Sample>) = if let Some(n) = self.latest {
            let ring = Arc::new(Ring::new(n));
            let sink = Arc::clone(&ring);
            (Chan::Ring(ring), Box::new(move |s| sink.push(s)))
        } else {
            let (tx, rx) = flume::bounded(FIFO_CAPACITY);
            (
                Chan::Fifo(rx),
                Box::new(move |s| {
                    // 満杯なら engine のスレッドをブロックする(zenoh の FifoChannel と同じ)。
                    let _ = tx.send(s);
                }),
            )
        };
        let guard = engine.subscribe(&key, on_sample)?;

        // latched の問い合わせは presence を待ってから撃つ(`latched()` のコメント参照)。
        // 購読の**後**に見張り始めるので、応答を待つ間に流れたライブ sample も落とさない。
        let presence = if self.latched {
            Some(self.cloudy.watch_key::<T>(&key)?)
        } else {
            None
        };

        tracing::debug!(key = %key, latched = self.latched, latest = ?self.latest, "subscriber declared");
        Ok(Subscriber {
            chan,
            _guard: guard,
            engine: Arc::clone(engine),
            key,
            latched: None,
            latched_done: true,
            presence,
            presence_done: !self.latched,
            queried: HashSet::new(),
            seen: HashSet::new(),
            warned: HashSet::new(),
            shutdown: self.cloudy.shutdown_handle(),
            _marker: PhantomData,
        })
    }
}

/// 受信チャネル。既定は Fifo(満杯でブロック)、[`SubscriberBuilder::latest`] で Ring
/// (満杯で最古を捨てる)。enum にして `Subscriber<T>` をバッファでジェネリックにしない ——
/// 公開型の形はダウンストリームの構造体フィールドに現れている。
enum Chan {
    Fifo(flume::Receiver<Sample>),
    Ring(Arc<Ring>),
}

impl Chan {
    /// 次の sample。どちらも cancel-safe(await 地点に取り出し済みの値を抱えない)。
    async fn recv(&self) -> Option<Sample> {
        match self {
            Self::Fifo(rx) => rx.recv_async().await.ok(),
            Self::Ring(ring) => Some(ring.pop().await),
        }
    }
}

/// 直近 n 件のリング。満杯なら最古を捨てる。
struct Ring {
    queue: Mutex<VecDeque<Sample>>,
    capacity: usize,
    notify: Notify,
}

impl Ring {
    fn new(capacity: usize) -> Self {
        Self {
            queue: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            notify: Notify::new(),
        }
    }

    fn push(&self, sample: Sample) {
        let mut queue = lock(&self.queue);
        if queue.len() == self.capacity {
            queue.pop_front();
        }
        queue.push_back(sample);
        drop(queue);
        self.notify.notify_one();
    }

    async fn pop(&self) -> Sample {
        loop {
            // 待ち手を先に登録してからキューを見る —— 間に push が来ても取りこぼさない。
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(sample) = lock(&self.queue).pop_front() {
                return sample;
            }
            notified.await;
        }
    }
}

/// 型付き subscriber。[`Cloudy::subscribe`] / [`SubscriberBuilder::build`] で得る。
///
/// `recv` はシャットダウン(Ctrl+C / SIGTERM / [`Cloudy::shutdown_now`])で `None` を返すので、
/// `while let Some(m) = sub.recv().await` のループが自然に抜ける。
///
/// # cancel-safety
///
/// [`Subscriber::recv`] / [`Subscriber::recv_envelope`] は **cancel-safe**: `tokio::select!` や
/// `tokio::time::timeout` で途中で捨てても、届いていた sample は失われず次の `recv` が返す。
/// 「一定時間来なければ切断扱い」はこれで書く(reiny に deadline API は無い):
///
/// ```ignore
/// match tokio::time::timeout(Duration::from_secs(1), sub.recv()).await {
///     Ok(Some(m)) => on_message(m),
///     Ok(None) => break,       // シャットダウン
///     Err(_) => on_stale(),    // 居るのに黙っている(居なくなったのは watch_publishers で取る)
/// }
/// ```
pub struct Subscriber<T> {
    chan: Chan,
    /// 購読の取っ手(保持するだけ。drop で undeclare)。
    _guard: Guard,
    /// latched の問い合わせを撃ち直すための材料(エンジンと購読キー)。
    engine: Arc<dyn Engine>,
    key: Key,
    /// いま飛んでいる latched 問い合わせの応答列(presence を見て撃つ)。
    latched: Option<Box<dyn RawReplies>>,
    /// 応答列を読み切ったか。撃っていなければ true。
    latched_done: bool,
    /// latched のとき、publisher の参加 / 離脱を見張るストリーム。
    presence: Option<Presence<T>>,
    /// presence ストリームが終端したか。latched でなければ最初から true。
    presence_done: bool,
    /// 直近値を既に問い合わせた publisher id(離脱したら忘れ、復帰時に撃ち直す)。
    queried: HashSet<String>,
    /// ライブ sample を配り終えた送信元。latched 応答がこれより後に届いたら捨てる。
    seen: HashSet<String>,
    /// スキーマ指紋の不一致を既に警告した送信元(送信元ごとに 1 度だけ鳴らす)。
    warned: HashSet<String>,
    shutdown: Shutdown,
    _marker: PhantomData<T>,
}

impl<T: Message + Default + Topic> Subscriber<T> {
    /// 次のメッセージを受け取る。チャネルが閉じたか、シャットダウンが要求されたら `None`。
    ///
    /// decode に失敗したサンプル(壊れた/別スキーマのペイロード)は警告して読み飛ばし、
    /// 受信を続ける。`None` は「もう来ない」を意味する終端シグナルとしてのみ返す。
    pub async fn recv(&mut self) -> Option<T> {
        self.recv_envelope().await.map(|e| e.value)
    }

    /// [`Subscriber::recv`] と同じだが、送信元 id と timestamp も返す。
    pub async fn recv_envelope(&mut self) -> Option<Envelope<T>> {
        let Self {
            chan,
            engine,
            key,
            latched,
            latched_done,
            presence,
            presence_done,
            queried,
            seen,
            warned,
            shutdown,
            ..
        } = self;
        loop {
            tokio::select! {
                biased;
                () = shutdown.wait() => return None,
                // publisher が見えた合図。その id へ 1 回だけ直近値を問い合わせる。
                event = recv_presence(presence.as_mut()), if !*presence_done => {
                    match event {
                        Some(PresenceEvent::Joined(id)) => {
                            if queried.insert(id) {
                                let params = QueryParams { payload: None, attachment: None, timeout: LATCHED_TIMEOUT };
                                match engine.query(key, params) {
                                    Ok(replies) => {
                                        *latched = Some(replies);
                                        *latched_done = false;
                                    }
                                    Err(e) => tracing::warn!(key = %key, error = %e, "latched get failed"),
                                }
                            }
                        }
                        Some(PresenceEvent::Left(id)) => { queried.remove(&id); }
                        None => *presence_done = true,
                    }
                }
                // latched 応答を先に流す(短命な列なので飢餓は起きない)。
                reply = recv_reply(latched.as_mut()), if !*latched_done => {
                    let Some(reply) = reply else {
                        *latched_done = true;
                        continue;
                    };
                    let Ok(sample) = reply else { continue };
                    // latest-wins: そのソースのライブ sample を既に配っていたら遅れた応答は捨てる。
                    let source = sample.key.source.clone().unwrap_or_default();
                    if seen.contains(&source) {
                        continue;
                    }
                    if let Some(envelope) = unwrap_sample::<T>(&sample, source, warned) {
                        return Some(envelope);
                    }
                }
                sample = chan.recv() => {
                    let sample = sample?; // channel closed
                    let source = sample.key.source.clone().unwrap_or_default();
                    // latched を見張っている間だけ覚える(遅れて届いた直近値を捨てるため)。
                    if !*presence_done {
                        seen.insert(source.clone());
                    }
                    if let Some(envelope) = unwrap_sample::<T>(&sample, source, warned) {
                        return Some(envelope);
                    }
                }
            }
        }
    }
}

/// 指紋を照合して decode する。落とすべき sample なら `None`。
fn unwrap_sample<T: Message + Default + Topic>(
    sample: &Sample,
    source: String,
    warned: &mut HashSet<String>,
) -> Option<Envelope<T>> {
    if !schema_matches::<T>(sample, &source, warned) {
        return None;
    }
    let value = decode::<T>(sample)?;
    Some(Envelope {
        value,
        source,
        timestamp: sample.timestamp,
    })
}

/// `Option<&mut replies>` を future 化する。precondition 付き select 分岐で使うので、
/// `None` は「永遠に来ない」= `pending` として扱う(unwrap を避けるため)。
async fn recv_reply(replies: Option<&mut Box<dyn RawReplies>>) -> Option<ReplyResult> {
    match replies {
        Some(r) => r.next().await,
        None => std::future::pending().await,
    }
}

/// [`recv_reply`] の presence 版。latched でない購読では `None` = `pending`。
async fn recv_presence<T>(presence: Option<&mut Presence<T>>) -> Option<PresenceEvent> {
    match presence {
        Some(p) => p.recv().await,
        None => std::future::pending().await,
    }
}

/// attachment に載った送信側の指紋を自分の `T::SCHEMA` と突き合わせる。
///
/// 素通しにするのは「照合できないとき」だけ —— どちらかが `None`(手書き `impl Topic` や
/// 指紋を載せない送信側、attachment を持たないエンジン)、または attachment が既知の形
/// (8 バイト LE)でないとき。不一致だけを落とし、その送信元については 1 度しか警告しない
/// (毎サンプル鳴らすとログが埋まる)。
fn schema_matches<T: Topic>(sample: &Sample, source: &str, warned: &mut HashSet<String>) -> bool {
    let (Some(mine), Some(theirs)) = (
        T::SCHEMA,
        attachment_fingerprint(sample.attachment.as_deref()),
    ) else {
        return true;
    };
    if theirs == mine {
        return true;
    }
    if warned.insert(source.to_string()) {
        tracing::warn!(
            ty = T::TYPE,
            source,
            expected = format!("{mine:016x}"),
            received = format!("{theirs:016x}"),
            "schema fingerprint mismatch; dropping samples from this source"
        );
    }
    false
}

/// attachment に載った reiny の指紋(8 バイト LE)。無い / 形が違う(他人の attachment)なら `None`。
pub(crate) fn attachment_fingerprint(attachment: Option<&[u8]>) -> Option<u64> {
    let raw = <[u8; 8]>::try_from(attachment?).ok()?;
    Some(u64::from_le_bytes(raw))
}

fn decode<T: Message + Default + Topic>(sample: &Sample) -> Option<T> {
    match T::decode(sample.payload.as_slice()) {
        Ok(msg) => Some(msg),
        Err(e) => {
            tracing::warn!(ty = T::TYPE, error = %e, "skipping undecodable sample");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// presence
// ---------------------------------------------------------------------------

/// publisher の参加 / 離脱。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PresenceEvent {
    /// この launch id がその型の publisher を宣言した(宣言済みのものも初回に流れる)。
    Joined(String),
    /// publisher が drop された、またはプロセスごと落ちた。
    Left(String),
}

/// [`Cloudy::watch_publishers`] が返すイベントストリーム。
pub struct Presence<T> {
    rx: mpsc::UnboundedReceiver<PresenceEvent>,
    /// 見張りの取っ手(保持するだけ)。
    _guard: Guard,
    shutdown: Shutdown,
    _marker: PhantomData<T>,
}

impl<T> Presence<T> {
    pub(crate) fn new(cloudy: &Cloudy, key: &Key) -> Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        let guard = cloudy.engine().watch_alive(
            key,
            Box::new(move |event| {
                let event = match event {
                    RawPresence::Joined(k) => PresenceEvent::Joined(k.source.unwrap_or_default()),
                    RawPresence::Left(k) => PresenceEvent::Left(k.source.unwrap_or_default()),
                };
                let _ = tx.send(event);
            }),
        )?;
        Ok(Self {
            rx,
            _guard: guard,
            shutdown: cloudy.shutdown_handle(),
            _marker: PhantomData,
        })
    }

    /// 次のイベントを待つ。シャットダウンかチャネル終端で `None`。
    pub async fn recv(&mut self) -> Option<PresenceEvent> {
        tokio::select! {
            biased;
            () = self.shutdown.wait() => None,
            event = self.rx.recv() => event,
        }
    }
}
