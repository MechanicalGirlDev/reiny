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

use std::marker::PhantomData;
use std::time::{SystemTime, UNIX_EPOCH};

use prost::Message;
use zenoh::Session;
use zenoh::Wait;
use zenoh::handlers::FifoChannelHandler;
use zenoh::pubsub::{Publisher as ZPublisher, Subscriber as ZSubscriber};
use zenoh::sample::Sample;

mod shutdown;
use shutdown::Shutdown;

/// `#[reiny::main]` — grain のエントリポイント属性。詳細は [`reiny_macros::main`]。
pub use reiny_macros::main;

/// `reiny-build` 生成の `config()` 拡張が設定 table を読むための再エクスポート。
/// 利用側 crate に `toml` 依存を持たせずに済ませる(prost と違い hidden)。
#[doc(hidden)]
pub use toml as __toml;

/// reiny の結果型。失敗は [`anyhow::Error`] にまとめる。
pub type Result<T> = anyhow::Result<T>;

/// 型 → トピックの対応。`reiny-build` が `Reiny.toml` を読んで各メッセージ型に impl する。
///
/// トピックは **型でアドレスする**。型 `Ping` は `reiny/<id>/Ping` へ publish され、
/// `reiny/*/Ping`(全 publisher の同じ型)で subscribe される。`<id>` は実行時の
/// インスタンス id([`Cloudy::id`])で、`TYPE` がキーの型セグメント(例 `Ping`)。
/// 発行側・購読側のどちらの crate でも同じ型は同じ `TYPE` になる。
pub trait Topic {
    /// トピックキーの型セグメント(例 `Ping`)。publish は `reiny/<id>/<TYPE>`、
    /// subscribe は `reiny/*/<TYPE>`。
    const TYPE: &'static str;
}

/// grain のランタイムハンドル。`#[reiny::main]` が構築して渡す。
///
/// Zenoh セッションと、自分のインスタンス id・協調シャットダウンを束ねる。`publish` /
/// `subscribe` は **型を型引数で渡すだけ**。トピックは [`Topic`] から解決される。
pub struct Cloudy {
    session: Session,
    id: String,
    shutdown: Shutdown,
    /// `--config <path>` で渡された設定ファイルを parse したもの(無ければ `None`)。
    /// `reiny-build` 生成の `config()` 拡張(per-project の `[config]`)が読む。
    config: Option<toml::Table>,
}

impl Cloudy {
    /// ランタイム内部から構築する(`__rt::run` 用)。
    fn new(session: Session, id: String, shutdown: Shutdown, config: Option<toml::Table>) -> Self {
        Self {
            session,
            id,
            shutdown,
            config,
        }
    }

    /// 型 `T` の publisher を作る。自分の `reiny/<id>/<T::TYPE>` へ発行する。
    pub fn publish<T>(&self) -> Result<Publisher<T>>
    where
        T: Message + Topic,
    {
        let key = format!("reiny/{}/{}", self.id, T::TYPE);
        let publisher = self
            .session
            .declare_publisher(key.clone())
            .wait()
            .map_err(anyhow::Error::msg)?;
        tracing::debug!(key = %key, "publisher declared");
        Ok(Publisher {
            publisher,
            _marker: PhantomData,
        })
    }

    /// 型 `T` の subscriber を作る。`reiny/*/<T::TYPE>`(全 publisher の同型)を購読する。
    pub fn subscribe<T>(&self) -> Result<Subscriber<T>>
    where
        T: Message + Default + Topic,
    {
        let key = format!("reiny/*/{}", T::TYPE);
        let subscriber = self
            .session
            .declare_subscriber(key.clone())
            .wait()
            .map_err(anyhow::Error::msg)?;
        tracing::debug!(key = %key, "subscriber declared");
        Ok(Subscriber {
            subscriber,
            shutdown: self.shutdown.clone(),
            _marker: PhantomData,
        })
    }

    /// 自分のインスタンス id(`--id` / `--name`、なければ `CARGO_PKG_NAME`)。
    /// ランチャは同じ bin を複数起動すると連番(例 `pong-1`, `pong-2`)を振る。
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
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
}

/// 型付き publisher。[`Cloudy::publish`] で得る。
pub struct Publisher<T: Message> {
    publisher: ZPublisher<'static>,
    _marker: PhantomData<T>,
}

impl<T: Message + Topic> Publisher<T> {
    /// メッセージを encode して発行する。
    pub async fn send(&self, message: T) -> Result<()> {
        let buf = message.encode_to_vec();
        self.publisher.put(buf).await.map_err(anyhow::Error::msg)?;
        Ok(())
    }
}

/// 型付き subscriber。[`Cloudy::subscribe`] で得る。
///
/// `recv` はシャットダウン(Ctrl+C)で `None` を返すので、`while let Some(m) = sub.recv().await`
/// のループが Ctrl+C で自然に抜ける。
pub struct Subscriber<T: Message + Default> {
    // フィールド名が型名と重なるが、内側の zenoh subscriber を素直に指す名前。
    #[allow(clippy::struct_field_names)]
    subscriber: ZSubscriber<FifoChannelHandler<Sample>>,
    shutdown: Shutdown,
    _marker: PhantomData<T>,
}

impl<T: Message + Default + Topic> Subscriber<T> {
    /// 次のメッセージを受け取る。チャネルが閉じたか、シャットダウンが要求されたら `None`。
    ///
    /// decode に失敗したサンプル(壊れた/別スキーマのペイロード)は警告して読み飛ばし、
    /// 受信を続ける。`None` は「もう来ない」を意味する終端シグナルとしてのみ返す。
    pub async fn recv(&mut self) -> Option<T> {
        loop {
            tokio::select! {
                biased;
                () = self.shutdown.wait() => return None,
                sample = self.subscriber.recv_async() => {
                    match sample {
                        Ok(sample) => {
                            let bytes = sample.payload().to_bytes();
                            match T::decode(bytes.as_ref()) {
                                Ok(msg) => return Some(msg),
                                Err(e) => {
                                    tracing::warn!(ty = T::TYPE, error = %e, "skipping undecodable sample");
                                }
                            }
                        }
                        Err(_) => return None, // channel closed
                    }
                }
            }
        }
    }
}

/// よく使うものをまとめた prelude。`use reiny::prelude::*;`
pub mod prelude {
    pub use crate::{Cloudy, Publisher, Subscriber, Topic};
}

/// `#[reiny::main]` 展開が呼ぶランタイム。利用側が直接触ることは想定しない。
#[doc(hidden)]
pub mod __rt {
    use std::future::Future;
    use std::str::FromStr;

    use tracing::Level;

    use crate::{Cloudy, Result};

    struct Options {
        id: String,
        log_level: Level,
        config_path: Option<String>,
    }

    /// `--id` / `--name` / `--log-level` / `--config` を緩く解釈する。reiny ランチャは
    /// これらを、`cargo run` は何も渡さない。未知の引数は無視する。
    fn parse_args(default_id: &str) -> Options {
        let mut id = default_id.to_string();
        let mut log_level = Level::INFO;
        let mut config_path = None;

        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                // ランチャはインスタンス id を渡す(連番、例 pong-2)。`--name` も同義で受ける。
                "--id" | "--name" => {
                    if let Some(v) = args.next() {
                        id = v;
                    }
                }
                "--log-level" => {
                    if let Some(v) = args.next()
                        && let Ok(l) = Level::from_str(&v)
                    {
                        log_level = l;
                    }
                }
                "--config" => {
                    config_path = args.next();
                }
                _ => {} // 未知の引数は無視(値付きでも次ループで読み飛ばされる)。
            }
        }
        Options {
            id,
            log_level,
            config_path,
        }
    }

    /// `--config <path>` を読み、TOML table として parse する。読めない/壊れている場合は
    /// 警告して `None`(= `[config]` の既定値だけを使う)。
    fn load_config(path: Option<&str>) -> Option<toml::Table> {
        let path = path?;
        match std::fs::read_to_string(path) {
            Ok(text) => match text.parse::<toml::Table>() {
                Ok(table) => Some(table),
                Err(e) => {
                    tracing::warn!(path, error = %e, "ignoring unparsable --config");
                    None
                }
            },
            Err(e) => {
                tracing::warn!(path, error = %e, "ignoring unreadable --config");
                None
            }
        }
    }

    fn init_tracing(level: Level) {
        let _ = tracing_subscriber::fmt().with_max_level(level).try_init();
    }

    /// tokio ランタイムを建て、Zenoh セッション・Ctrl+C シャットダウンを用意して
    /// 利用側の `async fn main(cloudy)` を実行する。
    pub fn run<F, Fut>(default_name: &str, user: F) -> Result<()>
    where
        F: FnOnce(Cloudy) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let opts = parse_args(default_name);
        init_tracing(opts.log_level);
        let config = load_config(opts.config_path.as_deref());

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        rt.block_on(async move {
            let shutdown = crate::Shutdown::new();
            {
                let shutdown = shutdown.clone();
                tokio::spawn(async move {
                    if tokio::signal::ctrl_c().await.is_ok() {
                        tracing::info!("Ctrl+C received; shutting down");
                        shutdown.trigger();
                    }
                });
            }

            let session = zenoh::open(zenoh::Config::default())
                .await
                .map_err(anyhow::Error::msg)?;
            tracing::info!(id = %opts.id, "reiny grain up");

            let cloudy = Cloudy::new(session, opts.id, shutdown, config);
            user(cloudy).await
        })
    }
}
