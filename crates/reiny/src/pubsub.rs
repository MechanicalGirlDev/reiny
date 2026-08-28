//! 型付き publisher / subscriber と、その builder・presence。
//!
//! キーの形は `reiny/<domain>/<id>/<TYPE>`(publish)/ `reiny/<domain>/*/<TYPE>`(subscribe)。
//! 組み立ては [`Cloudy::key_for`](crate::Cloudy) が一手に引き受ける。

use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use prost::Message;
use zenoh::Wait;
use zenoh::bytes::ZBytes;
use zenoh::handlers::{FifoChannelHandler, RingChannel, RingChannelHandler};
use zenoh::liveliness::LivelinessToken;
use zenoh::pubsub::{Publisher as ZPublisher, Subscriber as ZSubscriber};
use zenoh::qos::{CongestionControl, Priority};
use zenoh::query::{Query, Queryable, Reply};
use zenoh::sample::{Sample, SampleKind};
use zenoh::session::Session;
use zenoh::time::Timestamp;

use crate::shutdown::Shutdown;
use crate::{Cloudy, Descriptor, Result, Topic, source_of};

/// 受信メッセージと、reiny が知っている来歴。
///
/// `source` / `timestamp` は 0.2 では [`Subscriber::recv`] の中で捨てられていた情報なので、
/// 取り出すのに追加の実行コストは無い。
pub struct Envelope<T> {
    /// decode 済みのメッセージ本体。
    pub value: T,
    /// 送信元 grain の id(キーの `<id>` セグメント)。
    pub source: String,
    /// zenoh の timestamping が有効なときのみ載る送信時刻。
    pub timestamp: Option<Timestamp>,
}

// ---------------------------------------------------------------------------
// publisher
// ---------------------------------------------------------------------------

/// [`Cloudy::publisher`] が返す builder。何も指定しなければ [`Cloudy::publish`] と同じ。
#[must_use = "builder は .build() するまで何もしない"]
pub struct PublisherBuilder<'a, T> {
    cloudy: &'a Cloudy,
    latched: bool,
    priority: Option<Priority>,
    congestion: Option<CongestionControl>,
    express: Option<bool>,
    _marker: PhantomData<T>,
}

impl<'a, T> PublisherBuilder<'a, T> {
    pub(crate) fn new(cloudy: &'a Cloudy) -> Self {
        Self {
            cloudy,
            latched: false,
            priority: None,
            congestion: None,
            express: None,
            _marker: PhantomData,
        }
    }

    /// 直近 1 件を保持し、遅れて来た購読者の問い合わせに答える(latched)。
    /// 「起動時に 1 回配れば済む設定」を定期再送し続けるタスクの代わり。履歴は 1 件だけ。
    pub fn latched(mut self) -> Self {
        self.latched = true;
        self
    }

    /// zenoh の送信優先度。
    pub fn priority(mut self, priority: Priority) -> Self {
        self.priority = Some(priority);
        self
    }

    /// 輻輳時に捨てるか待つか。
    pub fn congestion(mut self, congestion: CongestionControl) -> Self {
        self.congestion = Some(congestion);
        self
    }

    /// バッチングを飛ばして即時送信する(低レイテンシ・低スループット)。
    pub fn express(mut self, express: bool) -> Self {
        self.express = Some(express);
        self
    }

    /// publisher を宣言する。同じキーの liveliness トークンも必ず同伴する
    /// ([`Cloudy::publishers`] の土台。opt-out は無い)。
    pub fn build(self) -> Result<Publisher<T>>
    where
        T: Message + Topic,
    {
        let key = self.cloudy.key_for(self.cloudy.id(), T::TYPE);
        let session = self.cloudy.session();

        // QoS setter は zenoh 側で `#[internal_trait]` により固有メソッドとしても生えているので、
        // `QoSBuilderTrait` を import せず(= `internal` feature を開けず)に呼べる。
        let mut builder = session.declare_publisher(key.clone());
        if let Some(p) = self.priority {
            builder = builder.priority(p);
        }
        if let Some(c) = self.congestion {
            builder = builder.congestion_control(c);
        }
        if let Some(e) = self.express {
            builder = builder.express(e);
        }
        let publisher = builder.wait().map_err(anyhow::Error::msg)?;

        let last: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
        let queryable = if self.latched {
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
        let token = session
            .liveliness()
            .declare_token(key.clone())
            .wait()
            .map_err(anyhow::Error::msg)?;
        let schema = match T::DESCRIPTOR {
            Some(descriptor) => Some(declare_schema(self.cloudy, &key, descriptor)?),
            None => None,
        };

        tracing::debug!(key = %key, latched = self.latched, "publisher declared");
        Ok(Publisher {
            publisher,
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
pub(crate) fn declare_schema(
    cloudy: &Cloudy,
    key: &str,
    descriptor: Descriptor,
) -> Result<Queryable<()>> {
    let reply_key = format!("{key}/@schema/{}", descriptor.message);
    let callback_key = reply_key.clone();
    cloudy
        .session()
        .declare_queryable(reply_key)
        .callback(move |query: Query| {
            if let Err(e) = query
                .reply(callback_key.clone(), descriptor.file_set.to_vec())
                .wait()
            {
                tracing::warn!(key = %callback_key, error = %e, "schema reply failed");
            }
        })
        .wait()
        .map_err(anyhow::Error::msg)
}

/// latched publisher の裏側 —— 自分の publish キーに queryable を 1 本立て、
/// 最後に送った値をそのまま返すだけ(`zenoh-ext` は使わない)。
fn declare_latch(
    cloudy: &Cloudy,
    key: &str,
    last: Arc<Mutex<Option<Vec<u8>>>>,
    fingerprint: Option<u64>,
) -> Result<Queryable<()>> {
    let reply_key = key.to_string();
    cloudy
        .session()
        .declare_queryable(key.to_string())
        .callback(move |query: Query| {
            // payload 付きの query は service の呼び出し(`service.rs`)。同じ型を latched publish
            // しつつ serve する grain で、呼び出しに直近値を返してしまわないよう無視する。
            if query.payload().is_some() {
                return;
            }
            // poison しても latched は「最後の値を返すだけ」なので、取れなければ黙って何も返さない。
            let payload = last.lock().ok().and_then(|g| g.clone());
            let Some(bytes) = payload else { return };
            // ライブ経路と同じ指紋を載せる。載せないと latched 応答だけ照合を素通りする。
            let mut reply = query.reply(reply_key.clone(), bytes);
            if let Some(fingerprint) = fingerprint {
                reply = reply.attachment(fingerprint.to_le_bytes().to_vec());
            }
            if let Err(e) = reply.wait() {
                tracing::warn!(key = %reply_key, error = %e, "latched reply failed");
            }
        })
        .wait()
        .map_err(anyhow::Error::msg)
}

/// 型付き publisher。[`Cloudy::publish`] / [`PublisherBuilder::build`] で得る。
///
/// drop すると liveliness トークンも落ちるので、購読側の [`Cloudy::watch_publishers`] に
/// [`PresenceEvent::Left`] が届く。
#[allow(clippy::struct_field_names)] // 内側の zenoh publisher を素直に指す名前。
pub struct Publisher<T> {
    publisher: ZPublisher<'static>,
    /// publisher と生死を共にする presence トークン(保持するだけ)。
    _token: LivelinessToken,
    /// latched のときだけ立つ、直近値を返す queryable(保持するだけ)。
    queryable: Option<Queryable<()>>,
    /// `T::DESCRIPTOR` があるときだけ立つ、`@schema` で descriptor を名乗る queryable(保持するだけ)。
    _schema: Option<Queryable<()>>,
    last: Arc<Mutex<Option<Vec<u8>>>>,
    _marker: PhantomData<T>,
}

impl<T: Message + Topic> Publisher<T> {
    /// メッセージを encode して発行する。`T::SCHEMA` があれば指紋を attachment に載せる。
    pub async fn send(&self, message: T) -> Result<()> {
        let buf = message.encode_to_vec();
        if self.queryable.is_some()
            && let Ok(mut slot) = self.last.lock()
        {
            *slot = Some(buf.clone());
        }
        // attachment setter も `#[internal_trait]` の固有メソッド側を使う(trait import 不要)。
        let mut put = self.publisher.put(buf);
        if let Some(fingerprint) = T::SCHEMA {
            put = put.attachment(fingerprint.to_le_bytes().to_vec());
        }
        put.await.map_err(anyhow::Error::msg)?;
        Ok(())
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

    /// 特定の grain id だけを購読する。同じ型を複数の grain が publish する構成で、
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
    /// 既定(未指定)は zenoh の `FifoChannel`(256 件)で、**満杯になると zenoh の受信スレッドが
    /// ブロックし、その grain の全購読が詰まる**。高レートの状態量を自分の周期でしか読まない
    /// 購読(GUI が 100Hz の `RobotState` を描画周期で読む等)は `latest(1)` にする。
    /// コマンド系は既定のまま —— 黙って落ちる方が制御では危ない。
    pub fn latest(mut self, n: usize) -> Self {
        self.latest = Some(n.max(1));
        self
    }

    /// subscriber を宣言する。
    pub fn build(self) -> Result<Subscriber<T>>
    where
        T: Message + Default + Topic,
    {
        let key = self
            .cloudy
            .key_for(self.from.as_deref().unwrap_or("*"), T::TYPE);
        let session = self.cloudy.session();
        let subscriber = match self.latest {
            Some(n) => Chan::Ring(
                session
                    .declare_subscriber(key.clone())
                    .with(RingChannel::new(n))
                    .wait()
                    .map_err(anyhow::Error::msg)?,
            ),
            None => Chan::Fifo(
                session
                    .declare_subscriber(key.clone())
                    .wait()
                    .map_err(anyhow::Error::msg)?,
            ),
        };

        // latched の問い合わせは presence を待ってから撃つ(`latched()` のコメント参照)。
        // 購読の**後**に見張り始めるので、応答を待つ間に流れたライブ sample も落とさない。
        let presence = if self.latched {
            Some(self.cloudy.watch_key::<T>(key.clone())?)
        } else {
            None
        };

        tracing::debug!(key = %key, latched = self.latched, latest = ?self.latest, "subscriber declared");
        Ok(Subscriber {
            subscriber,
            session: session.clone(),
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
/// (満杯で最古を捨てる)。enum にして `Subscriber<T>` を handler でジェネリックにしない ——
/// 公開型の形はダウンストリームの構造体フィールドに現れている。
enum Chan {
    Fifo(ZSubscriber<FifoChannelHandler<Sample>>),
    Ring(ZSubscriber<RingChannelHandler<Sample>>),
}

impl Chan {
    /// 次の sample。チャネル終端で `None`。どちらの handler も cancel-safe
    /// (flume の `recv_async` / Ring の `pull` → `not_empty` 待ちのループは、await 地点に
    /// 取り出し済みの値を抱えない)。
    async fn recv_async(&self) -> Option<Sample> {
        match self {
            Self::Fifo(s) => s.recv_async().await.ok(),
            Self::Ring(s) => s.recv_async().await.ok(),
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
    // フィールド名が型名と重なるが、内側の zenoh subscriber を素直に指す名前。
    #[allow(clippy::struct_field_names)]
    subscriber: Chan,
    /// latched の問い合わせを撃ち直すための材料(セッションと購読キー)。
    session: Session,
    key: String,
    /// いま飛んでいる latched 問い合わせの応答チャネル(presence を見て撃つ)。
    latched: Option<FifoChannelHandler<Reply>>,
    /// 応答チャネルを読み切ったか。撃っていなければ true。
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
            subscriber,
            session,
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
                                match session.get(&*key).wait() {
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
                // latched 応答を先に流す(短命なチャネルなので飢餓は起きない)。
                reply = recv_reply(latched.as_ref()), if !*latched_done => {
                    let Some(reply) = reply else {
                        *latched_done = true;
                        continue;
                    };
                    let Ok(sample) = reply.result() else { continue };
                    // latest-wins: そのソースのライブ sample を既に配っていたら遅れた応答は捨てる。
                    let source = source_of(sample.key_expr().as_str()).to_string();
                    if seen.contains(&source) {
                        continue;
                    }
                    if !schema_matches::<T>(sample, &source, warned) {
                        continue;
                    }
                    if let Some(value) = decode::<T>(sample) {
                        return Some(Envelope { value, source, timestamp: sample.timestamp().copied() });
                    }
                }
                sample = subscriber.recv_async() => {
                    let sample = sample?; // channel closed
                    let source = source_of(sample.key_expr().as_str()).to_string();
                    // latched を見張っている間だけ覚える(遅れて届いた直近値を捨てるため)。
                    if !*presence_done {
                        seen.insert(source.clone());
                    }
                    if !schema_matches::<T>(&sample, &source, warned) {
                        continue;
                    }
                    if let Some(value) = decode::<T>(&sample) {
                        return Some(Envelope { value, source, timestamp: sample.timestamp().copied() });
                    }
                }
            }
        }
    }
}

/// `Option<&handler>` を future 化する。precondition 付き select 分岐で使うので、
/// `None` は「永遠に来ない」= `pending` として扱う(unwrap を避けるため)。
async fn recv_reply(handler: Option<&FifoChannelHandler<Reply>>) -> Option<Reply> {
    match handler {
        Some(h) => h.recv_async().await.ok(),
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
/// 指紋を載せない送信側)、または attachment が既知の形(8 バイト LE)でないとき。不一致
/// だけを落とし、その送信元については 1 度しか警告しない(毎サンプル鳴らすとログが埋まる)。
fn schema_matches<T: Topic>(sample: &Sample, source: &str, warned: &mut HashSet<String>) -> bool {
    let (Some(mine), Some(theirs)) = (T::SCHEMA, attachment_fingerprint(sample.attachment()))
    else {
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
pub(crate) fn attachment_fingerprint(attachment: Option<&ZBytes>) -> Option<u64> {
    let bytes = attachment?.to_bytes();
    let raw = <[u8; 8]>::try_from(bytes.as_ref()).ok()?;
    Some(u64::from_le_bytes(raw))
}

fn decode<T: Message + Default + Topic>(sample: &Sample) -> Option<T> {
    let bytes = sample.payload().to_bytes();
    match T::decode(bytes.as_ref()) {
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
    /// この grain id がその型の publisher を宣言した(宣言済みのものも初回に流れる)。
    Joined(String),
    /// publisher が drop された、またはプロセスごと落ちた。
    Left(String),
}

/// [`Cloudy::watch_publishers`] が返すイベントストリーム。
pub struct Presence<T> {
    subscriber: ZSubscriber<FifoChannelHandler<Sample>>,
    shutdown: Shutdown,
    _marker: PhantomData<T>,
}

impl<T> Presence<T> {
    pub(crate) fn new(
        subscriber: ZSubscriber<FifoChannelHandler<Sample>>,
        shutdown: Shutdown,
    ) -> Self {
        Self {
            subscriber,
            shutdown,
            _marker: PhantomData,
        }
    }

    /// 次のイベントを待つ。シャットダウンかチャネル終端で `None`。
    pub async fn recv(&mut self) -> Option<PresenceEvent> {
        tokio::select! {
            biased;
            () = self.shutdown.wait() => None,
            sample = self.subscriber.recv_async() => {
                let sample = sample.ok()?;
                let id = source_of(sample.key_expr().as_str()).to_string();
                Some(match sample.kind() {
                    SampleKind::Put => PresenceEvent::Joined(id),
                    SampleKind::Delete => PresenceEvent::Left(id),
                })
            }
        }
    }
}
