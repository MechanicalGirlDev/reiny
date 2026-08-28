//! エンジン抽象 —— reiny が下のバスに求める 5 つの原始操作。
//!
//! reiny の上のロジック(encode / decode、指紋の照合、latched の「presence を見てから get」、
//! latest-wins、`Shutdown` 連動、service の `NoReply` / `Timeout` 判定)は全部 reiny のコードで、
//! バスに頼っているのは publish / subscribe / liveliness / queryable + get だけ
//! (`docs/design/0.5.0.md` §1.1)。それを [`Engine`] に切り出し、[`Cloudy`](crate::Cloudy) は
//! `Arc<dyn Engine>` を持つ。既定は zenoh([`Zenoh`]、feature `zenoh`)、テスト用に
//! プロセス内バス([`Local`])。
//!
//! # 契約
//!
//! - **受信バッファは reiny が持つ。** engine は sample / event / query ごとに callback を呼ぶ
//!   だけ。callback は**非 async のスレッド**から呼ぶこと(zenoh がそう。`Local` は専用スレッド)。
//!   reiny 側の callback はチャネルに積むだけで、Fifo が満杯なら engine のスレッドをブロックする
//!   (zenoh の `FifoChannel` と同じ)。
//! - **[`Guard`] の drop = undeclare。** subscriber / token / responder は取っ手を落とすと消える。
//! - **[`Engine::alive`] 以外は同期。** zenoh 1.x の builder の `await` は `ready(wait())` と
//!   等価で、非同期にする意味が無い。本当に待つのは [`RawReplies::next`] だけ —— これは
//!   **cancel-safe** であること(`Subscriber::recv` の select 分岐に置かれる)。
//! - [`Caps`] で無い機能を名乗る。reiny は無い機能を build 時にエラーにする(黙って劣化させ
//!   ない)。例外は attachment: 無ければ指紋は `None` 扱いで素通し(0.3.0 §2.7 の意味論)。

use std::any::Any;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{Qos, Result};

#[cfg(any(test, feature = "conformance"))]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
pub mod conformance;
mod local;
#[cfg(feature = "zenoh")]
mod zenoh;

pub use local::Local;
#[cfg(feature = "zenoh")]
pub use zenoh::Zenoh;

/// `Send` な boxed future。
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
/// engine が呼ぶ callback。engine のスレッドから呼ばれるので `Send + Sync`。
pub type Callback<T> = Box<dyn Fn(T) + Send + Sync>;
/// 宣言の取っ手。drop で undeclare。
pub type Guard = Box<dyn Any + Send + Sync>;
/// query への応答 1 件。`Err` は相手が `reply_err` した中身。
pub type ReplyResult = std::result::Result<Sample, Vec<u8>>;
/// [`Engine::respond`] に渡す callback。
pub type QueryCallback = Callback<Box<dyn RawQuery>>;

/// キー先頭の固定プレフィクス。
pub const KEY_ROOT: &str = "reiny";
/// service の presence トークンが付く verbatim チャンク(`…/<Req>/@service`)。publisher の
/// トークン(型のキーそのもの)と分けるのは、`publishers::<Req>()` に server が混ざらないため。
pub const SERVICE_CHUNK: &str = "@service";
/// grain そのものの presence トークン(`reiny/<domain>/<id>/@grain`)。publisher を 1 つも
/// 持たない grain も `reiny node list` に出すため。
pub const GRAIN_CHUNK: &str = "@grain";
/// descriptor を名乗る queryable のチャンク(`…/<T>/@schema/<message>`)。
pub const SCHEMA_CHUNK: &str = "@schema";

/// 住所。zenoh は `reiny/<domain>/<source>/<ty>[/<chunk>]` と描く(0.4 と同じ wire)。
///
/// 他のエンジンは別の描き方をしてよい —— 共通なのは「`ty` が住所」という 1 点だけ。
/// `source` も住所の一部とは限らず、engine は `subscribe` の `source: Some(id)` を **filter**
/// として実装してよい(iceoryx2 は型 1 つ = service 1 つで、source は header に載る)。
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Key {
    /// 論理名前空間。
    pub domain: String,
    /// 送信元 grain id。`None` = 全部(`*`)。
    pub source: Option<String>,
    /// 型セグメント([`Topic::TYPE`](crate::Topic::TYPE))。`None` = 全部(`*`)。grain の
    /// トークンは型の位置に verbatim の `@grain` を置く(`*` にはマッチしない)。
    pub ty: Option<String>,
    /// 型の後ろの verbatim チャンク(`@service` / `@schema/<message>`)。`*` にも `**` にも
    /// マッチしない —— 型のトピックを汚さないための隔離。パターンでも完全一致。
    pub chunk: Option<String>,
}

impl Key {
    /// 型のキー。`source: None` は `*`。
    #[must_use]
    pub fn topic(domain: &str, source: Option<&str>, ty: &str) -> Self {
        Self {
            domain: domain.to_string(),
            source: source.map(str::to_string),
            ty: Some(ty.to_string()),
            chunk: None,
        }
    }

    /// grain の presence トークン `reiny/<domain>/<id>/@grain`(`id: None` は全 grain)。
    #[must_use]
    pub fn grain(domain: &str, id: Option<&str>) -> Self {
        Self {
            domain: domain.to_string(),
            source: id.map(str::to_string),
            ty: Some(GRAIN_CHUNK.to_string()),
            chunk: None,
        }
    }

    /// 全 source・全型のパターン `reiny/<domain>/*/*`(verbatim は含まない)。
    #[must_use]
    pub fn all(domain: &str) -> Self {
        Self {
            domain: domain.to_string(),
            source: None,
            ty: None,
            chunk: None,
        }
    }

    /// 型の位置が verbatim(`@grain`)か。
    #[must_use]
    pub fn is_verbatim_type(&self) -> bool {
        self.ty.as_deref().is_some_and(|t| t.starts_with('@'))
    }

    /// チャンクを付けた複製。
    #[must_use]
    pub fn with_chunk(&self, chunk: impl Into<String>) -> Self {
        Self {
            chunk: Some(chunk.into()),
            ..self.clone()
        }
    }

    /// zenoh 形 `reiny/<domain>/<source>/<ty>[/<chunk>]` から組む。形が違えば `None`。
    #[must_use]
    pub fn parse(key: &str) -> Option<Self> {
        let mut parts = key.split('/');
        if parts.next()? != KEY_ROOT {
            return None;
        }
        let domain = parts.next()?.to_string();
        let source = wildcard_to_none(parts.next()?);
        let ty = wildcard_to_none(parts.next()?);
        let tail: Vec<&str> = parts.collect();
        Some(Self {
            domain,
            source,
            ty,
            chunk: (!tail.is_empty()).then(|| tail.join("/")),
        })
    }

    /// `self` をパターンとして `key` がマッチするか。`None` のセグメントは `*` で、型の `*` は
    /// verbatim(`@grain`)にマッチしない。chunk は完全一致。
    #[must_use]
    pub fn matches(&self, key: &Key) -> bool {
        let ty_ok = match (&self.ty, &key.ty) {
            (None, Some(t)) => !t.starts_with('@'),
            (None, None) => true,
            (Some(p), other) => other.as_ref() == Some(p),
        };
        self.domain == key.domain
            && self
                .source
                .as_ref()
                .is_none_or(|s| key.source.as_ref() == Some(s))
            && ty_ok
            && self.chunk == key.chunk
    }
}

fn wildcard_to_none(segment: &str) -> Option<String> {
    (segment != "*").then(|| segment.to_string())
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{KEY_ROOT}/{}/{}/{}",
            self.domain,
            self.source.as_deref().unwrap_or("*"),
            self.ty.as_deref().unwrap_or("*")
        )?;
        if let Some(chunk) = &self.chunk {
            write!(f, "/{chunk}")?;
        }
        Ok(())
    }
}

/// engine が届ける 1 件。publish された sample も query への応答も同じ形。
#[derive(Clone, Debug)]
pub struct Sample {
    /// 届いたキー(publisher / responder の具体キー。`source` は `Some`)。
    pub key: Key,
    /// encode 済みのメッセージ本体。
    pub payload: Vec<u8>,
    /// reiny の指紋(8 バイト LE)か、他人の attachment。
    pub attachment: Option<Vec<u8>>,
    /// 送信時刻(unix ns)。engine が持たなければ `None`。
    pub timestamp: Option<u64>,
}

/// liveliness の参加 / 離脱。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Presence {
    /// このキーのトークンが立った(宣言済みのものも `watch_alive` 直後に流れる)。
    Joined(Key),
    /// トークンが落ちた(drop、またはプロセスごと)。
    Left(Key),
}

/// engine が持つ機能。無いものは reiny が build 時にエラーにする。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)] // フラグの集合そのもの。状態機械ではない。
pub struct Caps {
    /// `source: None` の subscribe / alive / watch(全 publisher)。点対点リンクには無い。
    pub wildcard_source: bool,
    /// `declare_alive` / `alive` / `watch_alive`(presence)。
    pub liveliness: bool,
    /// `respond` / `query`(latched / service / `@schema`)。
    pub query: bool,
    /// sample に attachment を載せられる(指紋の照合)。無ければ照合は素通し。
    pub attachment: bool,
}

impl Caps {
    /// 全部ある。
    pub const ALL: Caps = Caps {
        wildcard_source: true,
        liveliness: true,
        query: true,
        attachment: true,
    };
}

/// [`Engine::query`] の引数。
#[derive(Clone, Debug)]
pub struct QueryParams {
    /// request 本体。`None` は latched の問い合わせ(payload の有無で service と区別する)。
    pub payload: Option<Vec<u8>>,
    /// request 型の指紋。
    pub attachment: Option<Vec<u8>>,
    /// これを過ぎたら engine は応答の列を閉じる。
    pub timeout: Duration,
}

/// reiny が下のバスに求めるもの。5 操作: publish / subscribe / liveliness(3 つ)/ queryable + get。
pub trait Engine: Send + Sync + 'static {
    /// 持っている機能。
    fn caps(&self) -> Caps;

    /// `key`(具体キー)への publisher を宣言する。
    fn publisher(&self, key: &Key, qos: &Qos) -> Result<Box<dyn RawPublisher>>;

    /// `key`(パターン)にマッチする sample ごとに `on_sample` を呼ぶ。
    fn subscribe(&self, key: &Key, on_sample: Callback<Sample>) -> Result<Guard>;

    /// `key`(具体キー)に presence トークンを立てる。取っ手の drop、またはプロセスの死で落ちる。
    fn declare_alive(&self, key: &Key) -> Result<Guard>;

    /// `key`(パターン)に立っているトークンの一覧。`timeout` を過ぎたら手持ちで返す。
    fn alive(&self, key: &Key, timeout: Duration) -> BoxFuture<'_, Result<Vec<Key>>>;

    /// `key`(パターン)のトークンの参加 / 離脱を `on_event` に流す。宣言済みは `Joined` で先に流す。
    fn watch_alive(&self, key: &Key, on_event: Callback<Presence>) -> Result<Guard>;

    /// `key`(具体キー)への query に `on_query` で応える。応えずに drop すれば finalize。
    /// `on_query` の型は [`QueryCallback`]。
    fn respond(&self, key: &Key, on_query: QueryCallback) -> Result<Guard>;

    /// `key`(パターン)にマッチする全 responder に query を撃つ。
    fn query(&self, key: &Key, params: QueryParams) -> Result<Box<dyn RawReplies>>;

    /// downcast の口。`cloudy.engine().as_any().downcast_ref::<Zenoh>()`。
    fn as_any(&self) -> &dyn Any;
}

/// [`Engine::publisher`] が返す送信口。
pub trait RawPublisher: Send + Sync {
    /// 1 件送る。`Reliable` なら送信路が空くまでブロックしてよい。
    fn put(&self, payload: Vec<u8>, attachment: Option<Vec<u8>>) -> Result<()>;
}

/// 受け取った query。[`RawQuery::reply`] / [`RawQuery::reply_err`] で消費するか、drop で finalize。
pub trait RawQuery: Send {
    /// 問い合わせのキー(パターンのこともある)。
    fn key(&self) -> &Key;
    /// request 本体。`None` は latched の問い合わせ。
    fn payload(&self) -> Option<&[u8]>;
    /// request の attachment。
    fn attachment(&self) -> Option<&[u8]>;
    /// `key`(応える側の具体キー)で応答を返す。
    fn reply(
        self: Box<Self>,
        key: &Key,
        payload: Vec<u8>,
        attachment: Option<Vec<u8>>,
    ) -> Result<()>;
    /// エラーで応える。呼び出し側には `Err(message)` が届く。
    fn reply_err(self: Box<Self>, message: Vec<u8>) -> Result<()>;
}

/// [`Engine::query`] の応答の列。
pub trait RawReplies: Send {
    /// 次の応答。全 responder が finalize したか、`timeout` を過ぎたら `None`。**cancel-safe**。
    fn next(&mut self) -> BoxFuture<'_, Option<ReplyResult>>;
}

/// 現在時刻(unix ns)。engine が sample に時刻を持たないとき用。
#[must_use]
pub fn now_unix_ns() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_nanos()).ok())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn key_round_trips_through_the_zenoh_form() {
        for text in [
            "reiny/lab/ctrl/RobotState",
            "reiny/lab/*/RobotState",
            "reiny/lab/*/*",
            "reiny/lab/ctrl/@grain",
            "reiny/lab/*/@grain",
            "reiny/lab/*/Add/@service",
            "reiny/lab/ctrl/RobotState/@schema/hs.RobotState",
        ] {
            let key = Key::parse(text).expect(text);
            assert_eq!(key.to_string(), text);
        }
        assert_eq!(
            Key::parse("reiny/lab/ctrl/RobotState")
                .unwrap()
                .source
                .as_deref(),
            Some("ctrl")
        );
        assert_eq!(Key::parse("reiny/lab/*/RobotState").unwrap().source, None);
        assert_eq!(
            Key::parse("reiny/lab/ctrl/@grain").unwrap(),
            Key::grain("lab", Some("ctrl"))
        );
        assert_eq!(
            Key::parse("reiny/lab/*/*/@service").unwrap(),
            Key::all("lab").with_chunk(SERVICE_CHUNK)
        );
        for bad in [
            "",
            "reiny",
            "reiny/lab",
            "reiny/lab/ctrl",
            "other/lab/ctrl/T",
        ] {
            assert!(Key::parse(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn pattern_matching_treats_none_as_star_and_chunks_verbatim() {
        let any = Key::topic("lab", None, "T");
        assert!(any.matches(&Key::topic("lab", Some("a"), "T")));
        assert!(!any.matches(&Key::topic("lab", Some("a"), "U")));
        assert!(!any.matches(&Key::topic("other", Some("a"), "T")));
        assert!(!any.matches(&Key::topic("lab", Some("a"), "T").with_chunk(SERVICE_CHUNK)));
        assert!(Key::grain("lab", None).matches(&Key::grain("lab", Some("a"))));
        let all = Key::all("lab");
        assert_eq!(all.to_string(), "reiny/lab/*/*");
        assert!(all.matches(&Key::topic("lab", Some("a"), "T")));
        assert!(!all.matches(&Key::grain("lab", Some("a"))), "verbatim");
        assert!(!all.matches(&Key::topic("lab", Some("a"), "T").with_chunk(SERVICE_CHUNK)));
        let services = all.with_chunk(SERVICE_CHUNK);
        assert!(services.matches(&Key::topic("lab", Some("a"), "T").with_chunk(SERVICE_CHUNK)));
        assert!(!services.matches(&Key::topic("lab", Some("a"), "T")));
    }
}
