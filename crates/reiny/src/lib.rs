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
//! # 逃げ道
//!
//! reiny が包んでいない zenoh 機能は [`Cloudy::session`] から直接触れる。その代わり
//! **zenoh は reiny の公開依存**であり(`pub use zenoh`)、zenoh のメジャー更新は
//! reiny の破壊的変更になる —— ここから先は zenoh の semver があなたのものになる。

use std::time::{SystemTime, UNIX_EPOCH};

use zenoh::Session;
use zenoh::Wait;

// テストは panic で失敗を表現してよい。
#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod e2e;
mod pubsub;
#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod rpc_e2e;
mod runtime;
mod service;
mod shutdown;

use shutdown::Shutdown;

pub use pubsub::{
    Envelope, Presence, PresenceEvent, Publisher, PublisherBuilder, Subscriber, SubscriberBuilder,
};
pub use runtime::{DOMAIN_ENV, RuntimeOptions, ZenohSource, run_with};
pub use service::{CallError, Caller, CallerBuilder, Request, Server};

/// 型語彙は `reiny-core`(`no_std`)に住む。zenoh が走らない場所(`reiny-link` の MCU 側)と
/// 同じ trait を共有するためで、利用側から見えるパスは 0.4 と同じ `reiny::Topic` のまま。
pub use reiny_core::{Descriptor, Durability, History, Priority, Qos, Reliability, Service, Topic};

/// zenoh そのもの。[`Cloudy::session`] を使うコードが reiny と版ズレを起こさないよう、
/// reiny がリンクしている zenoh を再エクスポートする。
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

/// キー先頭の固定プレフィクス。
const KEY_ROOT: &str = "reiny";

/// 名前空間(ドメイン)を指定しなかったときの既定値。
pub const DEFAULT_DOMAIN: &str = "default";

/// service の presence トークンが付く verbatim チャンク(`reiny/<domain>/<id>/<Req>/@service`)。
/// publisher のトークン(型のキーそのもの)と分けるのは、`publishers::<Req>()` に server が
/// 混ざらないようにするため。`@` 始まりは `*` / `**` のどちらにもマッチしない。
pub(crate) const SERVICE_CHUNK: &str = "@service";

/// grain そのものの presence トークン(`reiny/<domain>/<id>/@grain`)。publisher を 1 つも
/// 持たない grain も `reiny node list` に出すため。同じく verbatim。
pub(crate) const GRAIN_CHUNK: &str = "@grain";

/// grain のランタイムハンドル。`#[reiny::main]` が構築して渡す。
///
/// Zenoh セッションと、自分のインスタンス id・名前空間・協調シャットダウンを束ねる。
/// `publish` / `subscribe` は **型を型引数で渡すだけ**。トピックは [`Topic`] から解決される。
pub struct Cloudy {
    session: Session,
    id: String,
    domain: String,
    shutdown: Shutdown,
    /// grain の presence トークン(保持するだけ。プロセスが落ちれば消える)。
    _grain: zenoh::liveliness::LivelinessToken,
    /// `--config <path>` で渡された設定ファイルを parse したもの(無ければ `None`)。
    /// `reiny-build` 生成の `config()` 拡張(per-project の `[config]`)が読む。
    config: Option<toml::Table>,
    /// reiny が解釈しなかった起動引数(grain 固有の `--port` など)。
    extra_args: Vec<String>,
}

impl Cloudy {
    /// ランタイム内部から構築する([`run_with`] 用)。`@grain` トークンを立て、同じ id の grain が
    /// 既に居れば警告する(エラーにはしない —— `bag play --as` のような意図的な成り代わりがある)。
    fn new(
        session: Session,
        id: String,
        domain: String,
        shutdown: Shutdown,
        config: Option<toml::Table>,
        extra_args: Vec<String>,
    ) -> Result<Self> {
        let grain_key = format!("{KEY_ROOT}/{domain}/{id}/{GRAIN_CHUNK}");
        if let Ok(replies) = session
            .liveliness()
            .get(&grain_key)
            .timeout(std::time::Duration::from_millis(300))
            .wait()
            && replies.iter().any(|r| r.result().is_ok())
        {
            tracing::warn!(
                id,
                domain,
                "another grain with the same id is already on the bus; \
                 both will publish under the same keys"
            );
        }
        let grain = session
            .liveliness()
            .declare_token(grain_key)
            .wait()
            .map_err(anyhow::Error::msg)?;
        Ok(Self {
            session,
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
        self.alive_ids(self.key_for("*", T::TYPE)).await
    }

    /// liveliness キー `key` に生きているトークンの `<id>` 一覧(昇順、重複なし)。
    async fn alive_ids(&self, key: String) -> Result<Vec<String>> {
        let replies = self
            .session
            .liveliness()
            .get(key)
            .await
            .map_err(anyhow::Error::msg)?;
        let mut ids: Vec<String> = Vec::new();
        while let Ok(reply) = replies.recv_async().await {
            if let Ok(sample) = reply.result() {
                let id = source_of(sample.key_expr().as_str());
                if !id.is_empty() && !ids.iter().any(|s| s == id) {
                    ids.push(id.to_string());
                }
            }
        }
        ids.sort();
        Ok(ids)
    }

    /// 型 `T` の publisher の参加 / 離脱イベント。宣言時点で生きている publisher は
    /// [`PresenceEvent::Joined`] として最初に流れてくる(history 有効)。
    pub fn watch_publishers<T: Topic>(&self) -> Result<Presence<T>> {
        self.watch_key(self.key_for("*", T::TYPE))
    }

    /// liveliness キー `key` の参加 / 離脱ストリーム(宣言済みは `Joined` として最初に流れる)。
    pub(crate) fn watch_key<T>(&self, key: String) -> Result<Presence<T>> {
        let sub = self
            .session
            .liveliness()
            .declare_subscriber(key)
            .history(true)
            .wait()
            .map_err(anyhow::Error::msg)?;
        Ok(Presence::new(sub, self.shutdown.clone()))
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
        self.alive_ids(format!("{}/{SERVICE_CHUNK}", self.key_for("*", S::TYPE)))
            .await
    }

    /// request 型 `S` の server の参加 / 離脱イベント([`Cloudy::watch_publishers`] の server 版)。
    pub fn watch_servers<S: Service>(&self) -> Result<Presence<S>> {
        self.watch_key(format!("{}/{SERVICE_CHUNK}", self.key_for("*", S::TYPE)))
    }

    /// 内部の zenoh セッション。reiny が包んでいない機能(queryable / スカウティング /
    /// attachment / 任意キーの pub/sub)へ直接届くための逃げ道。
    ///
    /// これを使うと zenoh の semver が自分のものになる —— reiny が zenoh のメジャーを
    /// 上げたとき、ここを通るコードは道連れになる。
    #[must_use]
    pub fn session(&self) -> &Session {
        &self.session
    }

    /// 自分のインスタンス id(`--id` / `--name`、なければ `CARGO_PKG_NAME`)。
    /// ランチャは同じ bin を複数起動すると連番(例 `pong-1`, `pong-2`)を振る。
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// 自分の論理名前空間(`--domain` / `REINY_DOMAIN`、既定 [`DEFAULT_DOMAIN`])。
    /// 同じ zenoh fabric 上でも domain が違う grain とは通信しない。
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

    /// `reiny/<domain>/<source>/<ty>` を組む。`source` は自 id か `*`。
    pub(crate) fn key_for(&self, source: &str, ty: &str) -> String {
        format!("{KEY_ROOT}/{}/{source}/{ty}", self.domain)
    }

    pub(crate) fn shutdown_handle(&self) -> Shutdown {
        self.shutdown.clone()
    }
}

/// キー `reiny/<domain>/<id>/<TYPE>` から `<id>` を取り出す。形が違えば空文字。
fn source_of(key: &str) -> &str {
    key.split('/').nth(2).unwrap_or_default()
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
    fn source_is_the_third_segment() {
        assert_eq!(source_of("reiny/default/pong-2/Ping"), "pong-2");
        assert_eq!(source_of("reiny/lab/ctrl/RobotState"), "ctrl");
        // 想定外の形は空文字(呼び出し側で弾く)。
        assert_eq!(source_of("reiny/default"), "");
        assert_eq!(source_of(""), "");
    }

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
