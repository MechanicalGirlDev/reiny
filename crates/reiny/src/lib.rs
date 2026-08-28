//! reiny SDK。
//!
//! grain は「型を渡すだけ」で publish / subscribe する。型 → トピックの対応は
//! `reiny-build`(各 grain の `build.rs`)が `Reiny.toml` から生成し、各型に [`Topic`] を
//! impl することで埋め込む。利用側はトピック名(文字列)に触れない。
//!
//! ```ignore
//! use reiny::prelude::*;
//! use crate::publications::Ping;        // reiny-build が生成
//! use crate::dependencies::pong::Pong;  // 〃
//!
//! #[reiny::main]
//! async fn main(cloudy: Cloudy) -> reiny::Result<()> {
//!     let pings = cloudy.publish::<Ping>()?;
//!     let mut pongs = cloudy.subscribe::<Pong>()?;
//!     // ...
//!     Ok(())
//! }
//! ```
//!
//! # エンジン
//!
//! [`Cloudy`] はバスを [`engine::Engine`] 越しに使う。既定は zenoh([`engine::Zenoh`]、
//! feature `zenoh`)。[`engine::Local`] はプロセス内バスで、ネットワーク無しで grain の
//! テストが書ける —— [`Cloudy::open`] に `RuntimeOptions::engine` で挿す。
//!
//! # 逃げ道
//!
//! reiny が包んでいない zenoh 機能は [`Cloudy::session`] から直接触れる。その代わり
//! **zenoh は reiny の公開依存**であり(`pub use zenoh`)、zenoh のメジャー更新は
//! reiny の破壊的変更になる —— ここから先は zenoh の semver があなたのものになる。

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// テストは panic で失敗を表現してよい。
pub mod bridge;
#[cfg(all(test, feature = "zenoh"))]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod e2e;
pub mod engine;
mod pubsub;
#[cfg(all(test, feature = "zenoh"))]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod rpc_e2e;
mod runtime;
mod service;
mod shutdown;

use engine::{Engine, Guard, Key, SERVICE_CHUNK};
use shutdown::Shutdown;

pub use pubsub::{
    Envelope, Presence, PresenceEvent, Publisher, PublisherBuilder, Subscriber, SubscriberBuilder,
};
#[cfg(feature = "zenoh")]
pub use runtime::ZenohSource;
pub use runtime::{DOMAIN_ENV, RuntimeOptions, run_with};
pub use service::{CallError, Caller, CallerBuilder, Request, Server};

/// 型語彙は `reiny-core`(`no_std`)に住む。zenoh が走らない場所(`reiny-link` の MCU 側)と
/// 同じ trait を共有するためで、利用側から見えるパスは 0.4 と同じ `reiny::Topic` のまま。
pub use reiny_core::{Descriptor, Durability, History, Priority, Qos, Reliability, Service, Topic};

/// zenoh そのもの。[`Cloudy::session`] を使うコードが reiny と版ズレを起こさないよう、
/// reiny がリンクしている zenoh を再エクスポートする。
#[cfg(feature = "zenoh")]
pub use zenoh;

/// `#[reiny::main]` — grain のエントリポイント属性。詳細は [`reiny_macros::main`]。
pub use reiny_macros::main;

/// 共有スキーマクレート(`[schema] crate = ...`)の `lib.rs` に置く 1 行。
///
/// workspace モードで `[internals]` を **1 度だけ** prost コンパイル + `impl Topic` する
/// クレートが、`reiny-build` 生成物(`$OUT_DIR/reiny_generated.rs`)をライブラリとして取り込み、
/// `internals` / `__pb` を公開する。消費側 grain はこのクレートを Cargo 依存にするだけで、
/// 自前の proto 再コンパイルが要らなくなる(`#[reiny::main]` の取り込みと同じ仕組み)。
///
/// ```ignore
/// // myapp-schema/src/lib.rs
/// reiny::schema!();
/// // → myapp_schema::internals::* が他の grain から使える
/// ```
#[macro_export]
macro_rules! schema {
    () => {
        #[doc(hidden)]
        mod __reiny_generated {
            include!(concat!(env!("OUT_DIR"), "/reiny_generated.rs"));
        }
        #[allow(unused_imports)]
        pub use __reiny_generated::*;
    };
}

/// `reiny-build` 生成の `config()` 拡張が設定 table を読むための再エクスポート。
/// 利用側 crate に `toml` 依存を持たせずに済ませる(prost と違い hidden)。
#[doc(hidden)]
pub use toml as __toml;

/// reiny の結果型。失敗は [`anyhow::Error`] にまとめる。
pub type Result<T> = anyhow::Result<T>;

/// 名前空間(ドメイン)を指定しなかったときの既定値。
pub const DEFAULT_DOMAIN: &str = "default";

/// presence の問い合わせ(`publishers()` 等)を諦めるまで。zenoh の `get` の既定と同じ。
const ALIVE_TIMEOUT: Duration = Duration::from_secs(10);
/// 起動時の「同じ id が居るか」の問い合わせ。起動を遅らせない程度に短く。
const DUPLICATE_CHECK: Duration = Duration::from_millis(300);

/// grain のランタイムハンドル。`#[reiny::main]` が構築して渡す。
///
/// エンジン([`engine::Engine`])と、自分のインスタンス id・名前空間・協調シャットダウンを束ねる。
/// `publish` / `subscribe` は **型を型引数で渡すだけ**。トピックは [`Topic`] から解決される。
pub struct Cloudy {
    engine: Arc<dyn Engine>,
    id: String,
    domain: String,
    shutdown: Shutdown,
    /// grain の presence トークン(保持するだけ。プロセスが落ちれば消える)。
    _grain: Guard,
    /// `--config <path>` で渡された設定ファイルを parse したもの(無ければ `None`)。
    /// `reiny-build` 生成の `config()` 拡張(per-project の `[config]`)が読む。
    config: Option<toml::Table>,
    /// reiny が解釈しなかった起動引数(grain 固有の `--port` など)。
    extra_args: Vec<String>,
}

impl Cloudy {
    /// エンジンの上に構築する。`@grain` トークンを立て、同じ id の grain が既に居れば警告する
    /// (エラーにはしない —— `bag play --as` のような意図的な成り代わりがある)。
    async fn new(
        engine: Arc<dyn Engine>,
        id: String,
        domain: String,
        shutdown: Shutdown,
        config: Option<toml::Table>,
        extra_args: Vec<String>,
    ) -> Result<Self> {
        let grain_key = Key::grain(&domain, Some(&id));
        if engine.caps().liveliness {
            if let Ok(alive) = engine.alive(&grain_key, DUPLICATE_CHECK).await
                && !alive.is_empty()
            {
                tracing::warn!(
                    id,
                    domain,
                    "another grain with the same id is already on the bus; \
                     both will publish under the same keys"
                );
            }
        } else {
            tracing::debug!(
                id,
                domain,
                "engine has no liveliness; grain presence is off"
            );
        }
        let grain = engine.declare_alive(&grain_key)?;
        Ok(Self {
            engine,
            id,
            domain,
            shutdown,
            _grain: grain,
            config,
            extra_args,
        })
    }

    /// 型 `T` の publisher を作る。自分の `reiny/<domain>/<id>/<T::TYPE>` へ発行する。
    /// `QoS` や latched が要るときは [`Cloudy::publisher`] の builder を使う。
    pub fn publish<T>(&self) -> Result<Publisher<T>>
    where
        T: prost::Message + Topic,
    {
        self.publisher::<T>().build()
    }

    /// 型 `T` の subscriber を作る。`reiny/<domain>/*/<T::TYPE>`(同 domain の全 publisher の
    /// 同型)を購読する。送信元を選ぶ / latched を使うときは [`Cloudy::subscriber`] の builder。
    pub fn subscribe<T>(&self) -> Result<Subscriber<T>>
    where
        T: prost::Message + Default + Topic,
    {
        self.subscriber::<T>().build()
    }

    /// 型 `T` の publisher builder。`.latched()` を重ねて `.build()`。
    pub fn publisher<T>(&self) -> PublisherBuilder<'_, T> {
        PublisherBuilder::new(self)
    }

    /// 型 `T` の subscriber builder。`.from(id)` / `.latched()` を重ねて `.build()`。
    pub fn subscriber<T>(&self) -> SubscriberBuilder<'_, T> {
        SubscriberBuilder::new(self)
    }

    /// いま型 `T` を publish している grain id の一覧(自分を含む。id 昇順)。
    ///
    /// [`Cloudy::publish`] は publisher と同じキーに liveliness トークンを同伴させる
    /// (opt-out は無い —— 持たない publisher を許すとこの戻り値が信用できなくなる)ので、
    /// プロセスが落ちれば即座にここから消える。アプリ層のハートビートは要らない。
    pub async fn publishers<T: Topic>(&self) -> Result<Vec<String>> {
        self.alive_ids(self.key_for(None, T::TYPE)).await
    }

    /// liveliness キー `key` に生きているトークンの `<id>` 一覧(昇順、重複なし)。
    async fn alive_ids(&self, key: Key) -> Result<Vec<String>> {
        let mut ids: Vec<String> = self
            .engine
            .alive(&key, ALIVE_TIMEOUT)
            .await?
            .into_iter()
            .filter_map(|k| k.source)
            .collect();
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    /// 型 `T` の publisher の参加 / 離脱イベント。宣言時点で生きている publisher は
    /// [`PresenceEvent::Joined`] として最初に流れてくる(history 有効)。
    pub fn watch_publishers<T: Topic>(&self) -> Result<Presence<T>> {
        self.watch_key(&self.key_for(None, T::TYPE))
    }

    /// liveliness キー `key` の参加 / 離脱ストリーム(宣言済みは `Joined` として最初に流れる)。
    pub(crate) fn watch_key<T>(&self, key: &Key) -> Result<Presence<T>> {
        Presence::new(self, key)
    }

    /// request 型 `S` の server を立てる。`reiny/<domain>/<id>/<S::TYPE>` に queryable を置き、
    /// [`Server::recv`] で request を受けて [`Request::reply`] で返す。
    pub fn serve<S: Service>(&self) -> Result<Server<S>> {
        Server::declare(self)
    }

    /// request 型 `S` の caller builder。`.to(id)` / `.timeout(d)` を重ねて `.build()`。
    pub fn caller<S: Service>(&self) -> CallerBuilder<'_, S> {
        CallerBuilder::new(self)
    }

    /// `S` を 1 発呼ぶ(同 domain の任意の server、既定タイムアウト)。
    /// [`Cloudy::caller`] の糖衣。
    pub async fn call<S: Service>(
        &self,
        request: S,
    ) -> std::result::Result<S::Response, CallError> {
        self.caller::<S>().build().call(request).await
    }

    /// いま request 型 `S` を serve している grain id の一覧(自分を含む。id 昇順)。
    pub async fn servers<S: Service>(&self) -> Result<Vec<String>> {
        self.alive_ids(self.key_for(None, S::TYPE).with_chunk(SERVICE_CHUNK))
            .await
    }

    /// request 型 `S` の server の参加 / 離脱イベント([`Cloudy::watch_publishers`] の server 版)。
    pub fn watch_servers<S: Service>(&self) -> Result<Presence<S>> {
        self.watch_key(&self.key_for(None, S::TYPE).with_chunk(SERVICE_CHUNK))
    }

    /// 下のエンジン。他エンジン固有の機能へは `engine().as_any().downcast_ref::<E>()`。
    #[must_use]
    pub fn engine(&self) -> &Arc<dyn Engine> {
        &self.engine
    }

    /// 同じ id / domain / シャットダウン / 設定で、**別のエンジン**の上に 2 本目を組む。
    /// bridge(`reiny::bridge::forward`)が「zenoh の `Cloudy`」と「リンクの `Cloudy`」を
    /// 1 プロセスに持つための口。`@grain` トークンは新しいエンジンにも立つ。
    pub async fn with_engine(&self, engine: Arc<dyn Engine>) -> Result<Self> {
        Self::new(
            engine,
            self.id.clone(),
            self.domain.clone(),
            self.shutdown.clone(),
            self.config.clone(),
            self.extra_args.clone(),
        )
        .await
    }

    /// 内部の zenoh セッション。reiny が包んでいない機能(queryable / スカウティング /
    /// attachment / 任意キーの pub/sub)へ直接届くための逃げ道。エンジンが zenoh でなければ `None`。
    ///
    /// これを使うと zenoh の semver が自分のものになる —— reiny が zenoh のメジャーを
    /// 上げたとき、ここを通るコードは道連れになる。
    #[cfg(feature = "zenoh")]
    #[must_use]
    pub fn session(&self) -> Option<&zenoh::Session> {
        self.engine
            .as_any()
            .downcast_ref::<engine::Zenoh>()
            .map(engine::Zenoh::session)
    }

    /// 自分のインスタンス id(`--id` / `--name`、なければ `CARGO_PKG_NAME`)。
    /// ランチャは同じ bin を複数起動すると連番(例 `pong-1`, `pong-2`)を振る。
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// 自分の論理名前空間(`--domain` / `REINY_DOMAIN`、既定 [`DEFAULT_DOMAIN`])。
    /// 同じバス上でも domain が違う grain とは通信しない。
    #[must_use]
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// reiny が解釈しなかった起動引数。grain 固有の引数(`--port` など)はここから拾う。
    /// reiny は未知の引数をエラーにしない —— 知りようがないし、厳格化すると全 grain が落ちる。
    #[must_use]
    pub fn extra_args(&self) -> &[String] {
        &self.extra_args
    }

    /// 現在時刻(Unix 秒)。メッセージのタイムスタンプ用の小道具。
    #[must_use]
    pub fn now_unix(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
    }

    /// `--config` で渡された設定 table(無ければ `None`)。`reiny-build` 生成の
    /// 型付き `config()` 拡張が `[config]` の既定値に重ねて読むための原データ。
    #[doc(hidden)]
    #[must_use]
    pub fn config_table(&self) -> Option<&toml::Table> {
        self.config.as_ref()
    }

    /// シャットダウンが要求されるまで待つ(自前のループを持つ grain 用)。
    pub async fn shutdown(&self) {
        self.shutdown.wait().await;
    }

    /// シャットダウンを自分から要求する(冪等)。購読ループの `recv` が `None` を返し、
    /// [`Cloudy::shutdown`] の待ちが解ける。
    pub fn shutdown_now(&self) {
        self.shutdown.trigger();
    }

    /// `reiny/<domain>/<source>/<ty>` を組む。`source` は自 id か `None`(= `*`)。
    pub(crate) fn key_for(&self, source: Option<&str>, ty: &str) -> Key {
        Key::topic(&self.domain, source, ty)
    }

    pub(crate) fn shutdown_handle(&self) -> Shutdown {
        self.shutdown.clone()
    }
}

/// キーの 1 セグメントとして使える名前か検証する。ワイルドカードや区切りが混じると
/// キーの意味が壊れる(黙って別トピックへ行く)ので、起動時に弾く。
fn validate_segment(what: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        anyhow::bail!("{what} must not be empty");
    }
    if let Some(bad) = value
        .chars()
        .find(|c| matches!(c, '/' | '*' | '?' | '#' | '$' | '@') || c.is_whitespace())
    {
        anyhow::bail!(
            "{what} '{value}' contains '{bad}': it must be a single key segment \
             (no '/', '*', '?', '#', '$', '@' or whitespace)"
        );
    }
    Ok(())
}

/// よく使うものをまとめた prelude。`use reiny::prelude::*;`
pub mod prelude {
    pub use crate::{
        CallError, Caller, Cloudy, Descriptor, Envelope, PresenceEvent, Priority, Publisher, Qos,
        Reliability, Request, Server, Service, Subscriber, Topic,
    };
}

/// `#[reiny::main]` 展開が呼ぶランタイム。利用側が直接触ることは想定しない。
#[doc(hidden)]
pub mod __rt {
    use std::future::Future;

    use crate::{Cloudy, Result, RuntimeOptions, run_with};

    /// 0.2 互換のエントリ。引数から [`RuntimeOptions`] を組んで [`run_with`] へ渡す。
    pub fn run<F, Fut>(default_name: &str, user: F) -> Result<()>
    where
        F: FnOnce(Cloudy) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        run_with(RuntimeOptions::from_args(default_name), user)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segments_reject_wildcards_and_separators() {
        assert!(validate_segment("domain", "lab").is_ok());
        for bad in ["", "a/b", "*", "a*", "a b", "x?", "#", "$y", "a@b"] {
            assert!(
                validate_segment("domain", bad).is_err(),
                "should reject {bad:?}"
            );
        }
    }
}
