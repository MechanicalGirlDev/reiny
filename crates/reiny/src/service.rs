//! 型付き request/response —— **request 型がサービスの住所**。
//!
//! ```text
//! server : queryable  reiny/<domain>/<id>/<Req>      (+ liveliness  reiny/<domain>/<id>/<Req>/@service)
//! client : get        reiny/<domain>/<id|*>/<Req>    payload = Req、応答 payload = Req::Response
//! ```
//!
//! publish と同じキー形なので、宛先指定([`CallerBuilder::to`])も presence
//! ([`Cloudy::servers`] / [`Cloudy::watch_servers`])も pub/sub と同じ仕組みで効く。
//! latched publisher(`pubsub.rs`)と同じキーを共有できる —— 区別は **payload の有無**で、
//! latched 購読者の `get` は payload 無し、service の呼び出しは必ず payload 有り。
//!
//! 相関・タイムアウト・「返さずに drop したら応答ゼロ」は zenoh の query が持っているので、
//! reiny が足すのは encode / decode と指紋の照合だけ。

use std::fmt;
use std::marker::PhantomData;
use std::time::Duration;

use prost::Message;
use zenoh::Session;
use zenoh::Wait;
use zenoh::handlers::FifoChannelHandler;
use zenoh::liveliness::LivelinessToken;
use zenoh::query::{ConsolidationMode, Query, Queryable};

use crate::pubsub::{attachment_fingerprint, declare_schema};
use crate::shutdown::Shutdown;
use crate::{Cloudy, Result, SERVICE_CHUNK, Topic};

/// zenoh の `get` の既定と同じ。
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
/// zenoh 側の query 期限を自分の期限よりこれだけ後ろに置く(先に自分の timer が切れるように)。
const ZENOH_TIMEOUT_MARGIN: Duration = Duration::from_secs(1);

/// request 型 → response 型。request 型の [`Topic::TYPE`] がそのままキーの型セグメントになる。
///
/// `reiny-build` は `Reiny.toml` の `[services]` から impl を生成する。手書きも 1 行:
///
/// ```ignore
/// impl reiny::Service for Add { type Response = Sum; }
/// ```
///
/// `Response: Topic` を要求するのは、応答にも `SCHEMA` 指紋を載せるため。
pub trait Service: Topic + Message + Default {
    /// 応答の型。
    type Response: Topic + Message + Default;
}

// ---------------------------------------------------------------------------
// server
// ---------------------------------------------------------------------------

/// 型付き server。[`Cloudy::serve`] で得る。
///
/// drop すると liveliness トークンも落ち、[`Cloudy::watch_servers`] に `Left` が届く。
pub struct Server<S> {
    queryable: Queryable<FifoChannelHandler<Query>>,
    /// server と生死を共にする presence トークン(保持するだけ)。
    _token: LivelinessToken,
    /// request / response の `DESCRIPTOR` があるときだけ立つ `@schema` queryable(保持するだけ)。
    _schema: Vec<Queryable<()>>,
    /// 自分のキー。応答はこのキーで返す(問い合わせキー `reiny/<domain>/*/<Req>` と交差する)。
    key: String,
    shutdown: Shutdown,
    _marker: PhantomData<S>,
}

impl<S: Service> Server<S> {
    pub(crate) fn declare(cloudy: &Cloudy) -> Result<Self> {
        let key = cloudy.key_for(cloudy.id(), S::TYPE);
        let session = cloudy.session();
        let queryable = session
            .declare_queryable(key.clone())
            .wait()
            .map_err(anyhow::Error::msg)?;
        let token = session
            .liveliness()
            .declare_token(format!("{key}/{SERVICE_CHUNK}"))
            .wait()
            .map_err(anyhow::Error::msg)?;
        // `reiny service call` が JSON ↔ proto を組むのに request / response 両方の descriptor が要る。
        let mut schema = Vec::new();
        for descriptor in [S::DESCRIPTOR, S::Response::DESCRIPTOR]
            .into_iter()
            .flatten()
        {
            schema.push(declare_schema(cloudy, &key, descriptor)?);
        }
        tracing::debug!(key = %key, "service declared");
        Ok(Self {
            queryable,
            _token: token,
            _schema: schema,
            key,
            shutdown: cloudy.shutdown_handle(),
            _marker: PhantomData,
        })
    }

    /// 次の request を待つ。シャットダウンかチャネル終端で `None`。
    ///
    /// decode できない request は警告して読み飛ばす(呼び出し側には `reply_err` が返る)。
    /// **指紋不一致も `reply_err`** —— 購読のように黙って捨てると、呼び出し側は応答ゼロを
    /// 「server が居ない」と誤診する。payload の無い query(latched 購読者の `get`)は無視する。
    pub async fn recv(&mut self) -> Option<Request<S>> {
        loop {
            let query = tokio::select! {
                biased;
                () = self.shutdown.wait() => return None,
                query = self.queryable.recv_async() => query.ok()?,
            };
            let Some(payload) = query.payload() else {
                continue;
            };
            if let (Some(mine), Some(theirs)) =
                (S::SCHEMA, attachment_fingerprint(query.attachment()))
                && theirs != mine
            {
                tracing::warn!(
                    ty = S::TYPE,
                    expected = format!("{mine:016x}"),
                    received = format!("{theirs:016x}"),
                    "schema fingerprint mismatch on request; replying with error"
                );
                reply_err(
                    &query,
                    format!(
                        "schema fingerprint mismatch: expected {mine:016x}, received {theirs:016x}"
                    ),
                )
                .await;
                continue;
            }
            let decoded = S::decode(payload.to_bytes().as_ref());
            match decoded {
                Ok(value) => {
                    return Some(Request {
                        value,
                        query,
                        key: self.key.clone(),
                    });
                }
                Err(e) => {
                    tracing::warn!(ty = S::TYPE, error = %e, "skipping undecodable request");
                    reply_err(&query, format!("undecodable request: {e}")).await;
                }
            }
        }
    }
}

async fn reply_err(query: &Query, message: String) {
    if let Err(e) = query.reply_err(message.into_bytes()).await {
        tracing::warn!(key = %query.key_expr(), error = %e, "reply_err failed");
    }
}

/// 受け取った request。[`Request::reply`] / [`Request::reply_err`] のどちらかで消費する。
///
/// どちらも呼ばずに drop すると zenoh が query を finalize し、呼び出し側は
/// [`CallError::NoReply`] を受け取る —— 「返し忘れ」はハングにならない。
pub struct Request<S: Service> {
    /// decode 済みの request 本体。
    pub value: S,
    query: Query,
    key: String,
}

impl<S: Service> Request<S> {
    /// 応答を返す。`S::Response::SCHEMA` があれば指紋を attachment に載せる。
    pub async fn reply(self, response: S::Response) -> Result<()> {
        // attachment setter は `#[internal_trait]` の固有メソッド側(trait import 不要)。
        let mut reply = self.query.reply(self.key.clone(), response.encode_to_vec());
        if let Some(fingerprint) = S::Response::SCHEMA {
            reply = reply.attachment(fingerprint.to_le_bytes().to_vec());
        }
        reply.await.map_err(anyhow::Error::msg)
    }

    /// エラーで応える。呼び出し側には [`CallError::Remote`] としてこの文字列が届く。
    pub async fn reply_err(self, message: impl Into<String>) -> Result<()> {
        self.query
            .reply_err(message.into().into_bytes())
            .await
            .map_err(anyhow::Error::msg)
    }
}

// ---------------------------------------------------------------------------
// client
// ---------------------------------------------------------------------------

/// [`Cloudy::caller`] が返す builder。何も指定しなければ [`Cloudy::call`] と同じ。
#[must_use = "builder は .build() するまで何もしない"]
pub struct CallerBuilder<'a, S> {
    cloudy: &'a Cloudy,
    to: Option<String>,
    timeout: Duration,
    _marker: PhantomData<S>,
}

impl<'a, S> CallerBuilder<'a, S> {
    pub(crate) fn new(cloudy: &'a Cloudy) -> Self {
        Self {
            cloudy,
            to: None,
            timeout: DEFAULT_TIMEOUT,
            _marker: PhantomData,
        }
    }

    /// 特定の grain id の server だけに撃つ。未指定は同 domain の全 server に撃ち、
    /// **最初の応答を採る** —— server が 2 つ以上居る構成ではこちらで選ぶこと。
    pub fn to(mut self, id: impl Into<String>) -> Self {
        self.to = Some(id.into());
        self
    }

    /// 応答を待つ上限(既定 10 s = zenoh の既定)。超えると [`CallError::Timeout`]。
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// caller を組む。宣言を伴わない(`get` は都度撃つ)ので失敗しない。
    #[must_use]
    pub fn build(self) -> Caller<S>
    where
        S: Service,
    {
        Caller {
            session: self.cloudy.session().clone(),
            key: self
                .cloudy
                .key_for(self.to.as_deref().unwrap_or("*"), S::TYPE),
            timeout: self.timeout,
            _marker: PhantomData,
        }
    }
}

/// 型付き client。[`Cloudy::caller`] で組む。`Cloudy` を借りないので構造体に持てる。
pub struct Caller<S> {
    session: Session,
    key: String,
    timeout: Duration,
    _marker: PhantomData<S>,
}

impl<S: Service> Caller<S> {
    /// request を送って応答を待つ。
    pub async fn call(&self, request: S) -> std::result::Result<S::Response, CallError> {
        // consolidation を明示で切る: service はキャッシュではなく、同じキーの 2 応答は
        // 「重複」ではなく「別の答え」。期限は自分の timer で切り(→ `Timeout`)、zenoh 側の
        // 期限はその外に置く —— zenoh は期限切れも「応答ゼロ」としか見せず、`NoReply` と
        // 区別できないため。
        let mut get = self
            .session
            .get(&self.key)
            .payload(request.encode_to_vec())
            .timeout(self.timeout + ZENOH_TIMEOUT_MARGIN)
            .consolidation(ConsolidationMode::None);
        if let Some(fingerprint) = S::SCHEMA {
            get = get.attachment(fingerprint.to_le_bytes().to_vec());
        }
        let replies = get
            .await
            .map_err(|e| CallError::Zenoh(anyhow::Error::msg(e)))?;
        let Ok(reply) = tokio::time::timeout(self.timeout, replies.recv_async())
            .await
            .map_err(|_| CallError::Timeout)?
        else {
            // チャネルが閉じた = 該当 queryable が全部 finalize した(server 不在 / 返さず drop)。
            return Err(CallError::NoReply);
        };
        match reply.result() {
            Ok(sample) => {
                if let (Some(expected), Some(received)) = (
                    S::Response::SCHEMA,
                    attachment_fingerprint(sample.attachment()),
                ) && expected != received
                {
                    return Err(CallError::Schema { expected, received });
                }
                S::Response::decode(sample.payload().to_bytes().as_ref()).map_err(CallError::Decode)
            }
            Err(e) => Err(CallError::Remote(
                String::from_utf8_lossy(&e.payload().to_bytes()).into_owned(),
            )),
        }
    }
}

/// [`Caller::call`] の失敗。`Remote` / `NoReply` を潰さないのは、呼び出し側が
/// 「server が居ない」と「server が断った」を分岐したい場面(GUI のボタン活性など)があるから。
#[derive(Debug)]
pub enum CallError {
    /// 応答が 1 つも来なかった: server 不在、または server が返さずに drop した。
    NoReply,
    /// タイムアウトまでに応答が無かった。
    Timeout,
    /// server が [`Request::reply_err`] した。中身は server が渡した文字列。
    Remote(String),
    /// 応答の指紋が `Response::SCHEMA` と違う。
    Schema {
        /// 自分の `Response::SCHEMA`。
        expected: u64,
        /// 応答に載っていた指紋。
        received: u64,
    },
    /// 応答が `Response` として decode できない。
    Decode(prost::DecodeError),
    /// zenoh 層のエラー。
    Zenoh(anyhow::Error),
}

impl fmt::Display for CallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoReply => write!(f, "no reply (no server, or the server dropped the request)"),
            Self::Timeout => write!(f, "timed out waiting for a reply"),
            Self::Remote(msg) => write!(f, "server replied with error: {msg}"),
            Self::Schema { expected, received } => write!(
                f,
                "response schema fingerprint mismatch: expected {expected:016x}, received {received:016x}"
            ),
            Self::Decode(e) => write!(f, "undecodable response: {e}"),
            Self::Zenoh(e) => write!(f, "zenoh: {e}"),
        }
    }
}

impl std::error::Error for CallError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Decode(e) => Some(e),
            Self::Zenoh(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}
