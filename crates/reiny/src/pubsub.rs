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
use zenoh::query::{Query, Queryable, Reply};
use zenoh::sample::{Sample, SampleKind};
use zenoh::time::Timestamp;

use crate::shutdown::Shutdown;
use crate::{Cloudy, Result, Topic, source_of};

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
///
/// QoS(priority / congestion control / express)はここには**無い**。zenoh は該当 setter を
/// `internal` + `unstable` feature の裏に置いており、3 つの setter のためにその 2 枚を開けるのは
/// 割に合わない。必要なら [`Cloudy::session`] から `declare_publisher` を直接呼ぶ。
#[must_use = "builder は .build() するまで何もしない"]
pub struct PublisherBuilder<'a, T> {
    cloudy: &'a Cloudy,
    latched: bool,
    _marker: PhantomData<T>,
}

impl<'a, T> PublisherBuilder<'a, T> {
    pub(crate) fn new(cloudy: &'a Cloudy) -> Self {
        Self {
            cloudy,
            latched: false,
            _marker: PhantomData,
        }
    }

    /// 直近 1 件を保持し、遅れて来た購読者の問い合わせに答える(latched)。
    /// 「起動時に 1 回配れば済む設定」を定期再送し続けるタスクの代わり。履歴は 1 件だけ。
    pub fn latched(mut self) -> Self {
        self.latched = true;
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

        let publisher = session
            .declare_publisher(key.clone())
            .wait()
            .map_err(anyhow::Error::msg)?;

        let token = session
            .liveliness()
            .declare_token(key.clone())
            .wait()
            .map_err(anyhow::Error::msg)?;

        let last: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
        let queryable = if self.latched {
            Some(declare_latch(self.cloudy, &key, Arc::clone(&last))?)
        } else {
            None
        };

        tracing::debug!(key = %key, latched = self.latched, "publisher declared");
        Ok(Publisher {
            publisher,
            _token: token,
            queryable,
            last,
            _marker: PhantomData,
        })
    }
}

/// latched publisher の裏側 —— 自分の publish キーに queryable を 1 本立て、
/// 最後に送った値をそのまま返すだけ(`zenoh-ext` は使わない)。
fn declare_latch(
    cloudy: &Cloudy,
    key: &str,
    last: Arc<Mutex<Option<Vec<u8>>>>,
) -> Result<Queryable<()>> {
    let reply_key = key.to_string();
    cloudy
        .session()
        .declare_queryable(key.to_string())
        .callback(move |query: Query| {
            // poison しても latched は「最後の値を返すだけ」なので、取れなければ黙って何も返さない。
            let payload = last.lock().ok().and_then(|g| g.clone());
            if let Some(bytes) = payload
                && let Err(e) = query.reply(reply_key.clone(), bytes).wait()
            {
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
    last: Arc<Mutex<Option<Vec<u8>>>>,
    _marker: PhantomData<T>,
}

impl<T: Message + Topic> Publisher<T> {
    /// メッセージを encode して発行する。
    pub async fn send(&self, message: T) -> Result<()> {
        let buf = message.encode_to_vec();
        if self.queryable.is_some()
            && let Ok(mut slot) = self.last.lock()
        {
            *slot = Some(buf.clone());
        }
        self.publisher.put(buf).await.map_err(anyhow::Error::msg)?;
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
