//! 型付き publisher / subscriber と、その builder・presence。
//!
//! キーの形は `reiny/<domain>/<id>/<TYPE>`(publish)/ `reiny/<domain>/*/<TYPE>`(subscribe)。
//! 組み立ては [`Cloudy::key_for`](crate::Cloudy) が一手に引き受ける。

use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use prost::Message;
use zenoh::Wait;
use zenoh::handlers::FifoChannelHandler;
use zenoh::liveliness::LivelinessToken;
use zenoh::pubsub::{Publisher as ZPublisher, Subscriber as ZSubscriber};
use zenoh::qos::{CongestionControl, Priority};
use zenoh::query::{Query, Queryable, Reply};
use zenoh::sample::{Sample, SampleKind};
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

        let token = session
            .liveliness()
            .declare_token(key.clone())
            .wait()
            .map_err(anyhow::Error::msg)?;

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
fn declare_schema(cloudy: &Cloudy, key: &str, descriptor: Descriptor) -> Result<Queryable<()>> {
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
    _marker: PhantomData<T>,
}

impl<'a, T> SubscriberBuilder<'a, T> {
    pub(crate) fn new(cloudy: &'a Cloudy) -> Self {
        Self {
            cloudy,
            from: None,
            latched: false,
            _marker: PhantomData,
        }
    }

    /// 特定の grain id だけを購読する。同じ型を複数の grain が publish する構成で、
    /// 購読側が出し手を選ぶための指定。
    pub fn from(mut self, id: impl Into<String>) -> Self {
        self.from = Some(id.into());
        self
    }

    /// 宣言した直後に latched publisher へ 1 回問い合わせ、直近値を受け取ってから
    /// ライブ購読に入る。
    pub fn latched(mut self) -> Self {
        self.latched = true;
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
        let subscriber = session
            .declare_subscriber(key.clone())
            .wait()
            .map_err(anyhow::Error::msg)?;

        // 宣言の**後**に撃つ。先に撃つと、応答を待つ間に流れたライブ sample を落とす。
        let latched = if self.latched {
            Some(session.get(&key).wait().map_err(anyhow::Error::msg)?)
        } else {
            None
        };

        tracing::debug!(key = %key, latched = self.latched, "subscriber declared");
        Ok(Subscriber {
            subscriber,
            latched,
            latched_done: !self.latched,
            seen: HashSet::new(),
            warned: HashSet::new(),
            shutdown: self.cloudy.shutdown_handle(),
            _marker: PhantomData,
        })
    }
}

/// 型付き subscriber。[`Cloudy::subscribe`] / [`SubscriberBuilder::build`] で得る。
///
/// `recv` はシャットダウン(Ctrl+C / SIGTERM / [`Cloudy::shutdown_now`])で `None` を返すので、
/// `while let Some(m) = sub.recv().await` のループが自然に抜ける。
pub struct Subscriber<T> {
    // フィールド名が型名と重なるが、内側の zenoh subscriber を素直に指す名前。
    #[allow(clippy::struct_field_names)]
    subscriber: ZSubscriber<FifoChannelHandler<Sample>>,
    /// latched 問い合わせの応答チャネル(撃った場合のみ)。
    latched: Option<FifoChannelHandler<Reply>>,
    /// 応答チャネルを読み切ったか。latched でなければ最初から true。
    latched_done: bool,
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
            latched,
            latched_done,
            seen,
            warned,
            shutdown,
            ..
        } = self;
        loop {
            tokio::select! {
                biased;
                () = shutdown.wait() => return None,
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
                    let Ok(sample) = sample else { return None }; // channel closed
                    let source = source_of(sample.key_expr().as_str()).to_string();
                    if !*latched_done {
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

/// attachment に載った送信側の指紋を自分の `T::SCHEMA` と突き合わせる。
///
/// 素通しにするのは「照合できないとき」だけ —— どちらかが `None`(手書き `impl Topic` や
/// 指紋を載せない送信側)、または attachment が既知の形(8 バイト LE)でないとき。不一致
/// だけを落とし、その送信元については 1 度しか警告しない(毎サンプル鳴らすとログが埋まる)。
fn schema_matches<T: Topic>(sample: &Sample, source: &str, warned: &mut HashSet<String>) -> bool {
    let (Some(mine), Some(attachment)) = (T::SCHEMA, sample.attachment()) else {
        return true;
    };
    let bytes = attachment.to_bytes();
    let Ok(raw) = <[u8; 8]>::try_from(bytes.as_ref()) else {
        return true; // reiny の指紋ではない attachment。他人のものなので触らない。
    };
    let theirs = u64::from_le_bytes(raw);
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
