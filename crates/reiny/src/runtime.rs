//! launch ランタイムの起動オプションと入口。
//!
//! `#[reiny::main]` は [`RuntimeOptions::from_args`] → [`run_with`] を呼ぶだけなので、
//! 自前でオプションを組めば同じ入口をライブラリとして使える。tokio runtime を自分で持つ
//! (テスト、bridge)なら [`Cloudy::open`] が async の入口。

use std::future::Future;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use tracing::Level;

use crate::engine::Engine;
use crate::shutdown::Shutdown;
use crate::{Cloudy, DEFAULT_DOMAIN, Result, validate_segment};

/// domain を環境変数で与えるときのキー。CI の並列ジョブのように、config を書き換えずに
/// 系統を分けたいときの入口。
pub const DOMAIN_ENV: &str = "REINY_DOMAIN";

/// zenoh セッション設定の出どころ。
///
/// reiny は zenoh の設定スキーマを **ラップしない**。ラップした瞬間に zenoh の設定項目へ
/// 追随する義務が生まれ、「薄さが価値」という前提を裏切るため。
#[cfg(feature = "zenoh")]
pub enum ZenohSource {
    /// `zenoh::Config::default()`(0.2 と同じ)。
    Default,
    /// `zenoh::Config::from_env()`。
    Env,
    /// `zenoh::Config::from_file()`(JSON5 / JSON / YAML)。
    File(PathBuf),
    /// 呼び出し側が組み立てたもの。
    Config(Box<zenoh::Config>),
}

#[cfg(feature = "zenoh")]
impl ZenohSource {
    fn to_config(&self) -> Result<zenoh::Config> {
        match self {
            Self::Default => Ok(zenoh::Config::default()),
            Self::Env => zenoh::Config::from_env().map_err(anyhow::Error::msg),
            Self::File(path) => zenoh::Config::from_file(path)
                .map_err(anyhow::Error::msg)
                .map_err(|e| e.context(format!("loading zenoh config {}", path.display()))),
            Self::Config(config) => Ok((**config).clone()),
        }
    }
}

/// launch ランタイムの起動オプション。
pub struct RuntimeOptions {
    /// インスタンス id。キーの `<id>` セグメントになる。
    pub id: String,
    /// 論理名前空間。キーの `<domain>` セグメントになる。
    pub domain: String,
    /// 使うエンジン。`None` なら zenoh(feature `zenoh`)を `zenoh` / `zenoh_overrides` から開く。
    /// テストは [`crate::engine::Local`]、bridge は自前のエンジンをここに挿す。
    pub engine: Option<Arc<dyn Engine>>,
    /// zenoh セッション設定の出どころ。
    #[cfg(feature = "zenoh")]
    pub zenoh: ZenohSource,
    /// `zenoh` を組み立てた後に重ねる `Config::insert_json5` の (key, json5) 列。
    /// CLI の `--connect` / `--zenoh-mode` はここへ落ちる。
    #[cfg(feature = "zenoh")]
    pub zenoh_overrides: Vec<(String, String)>,
    /// reiny が `tracing_subscriber` をグローバル登録するか。
    ///
    /// 自前の subscriber(ログ収集レイヤなど)を持つ launch は `false` にする。`true` のまま
    /// 先を越されると reiny 側は黙って何もしない(`try_init` は後勝ちしない)ため、
    /// 「reiny より先に入れる」順序依存を抱え込むことになる。
    pub install_tracing: bool,
    /// `install_tracing` が true のときのログレベル。
    pub log_level: Level,
    /// tokio のワーカースレッド数(未指定は tokio 既定 = CPU 数)。
    pub worker_threads: Option<usize>,
    /// `--config <path>`。[`Cloudy::config_table`](crate::Cloudy::config_table) の原データ。
    pub config_path: Option<PathBuf>,
    /// reiny が解釈しなかった引数。そのまま [`Cloudy::extra_args`](crate::Cloudy::extra_args) へ。
    pub extra_args: Vec<String>,
}

impl RuntimeOptions {
    /// 既定値。`id` 以外は `--` 引数を見ない素の状態。
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            domain: std::env::var(DOMAIN_ENV).unwrap_or_else(|_| DEFAULT_DOMAIN.to_string()),
            engine: None,
            #[cfg(feature = "zenoh")]
            zenoh: ZenohSource::Default,
            #[cfg(feature = "zenoh")]
            zenoh_overrides: Vec::new(),
            install_tracing: true,
            log_level: Level::INFO,
            worker_threads: None,
            config_path: None,
            extra_args: Vec::new(),
        }
    }

    /// プロセス引数から組む。`#[reiny::main]` が使う経路。
    ///
    /// 解釈するのは `--id` / `--name` / `--log-level` / `--config` / `--domain` /
    /// `--zenoh-config` / `--connect` / `--zenoh-mode` だけ。**未知の引数はエラーにせず**
    /// [`RuntimeOptions::extra_args`] へ落とす —— launch 固有の引数を reiny は知りようがなく、
    /// 厳格化すると既存の launch が全部落ちる。タイポ検出は launch 側の引数パーサの仕事。
    /// zenoh 抜きのビルドでは zenoh 系の引数は警告して無視する。
    #[must_use]
    pub fn from_args(default_id: &str) -> Self {
        Self::from_arg_list(default_id, std::env::args().skip(1))
    }

    fn from_arg_list<I: IntoIterator<Item = String>>(default_id: &str, args: I) -> Self {
        let mut opts = Self::new(default_id);
        let mut args = args.into_iter();
        #[cfg(feature = "zenoh")]
        let mut connect: Vec<String> = Vec::new();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                // ランチャはインスタンス id を渡す(連番、例 pong-2)。`--name` も同義で受ける。
                "--id" | "--name" => take(&mut args, &mut opts.id),
                "--domain" => take(&mut args, &mut opts.domain),
                "--log-level" => {
                    if let Some(v) = args.next()
                        && let Ok(level) = Level::from_str(&v)
                    {
                        opts.log_level = level;
                    }
                }
                "--config" => opts.config_path = args.next().map(PathBuf::from),
                "--zenoh-config" | "--connect" | "--zenoh-mode" => {
                    let value = args.next();
                    #[cfg(feature = "zenoh")]
                    match (arg.as_str(), value) {
                        ("--zenoh-config", Some(v)) => {
                            opts.zenoh = ZenohSource::File(PathBuf::from(v));
                        }
                        ("--connect", Some(v)) => connect.push(v),
                        ("--zenoh-mode", Some(v)) => opts
                            .zenoh_overrides
                            .push(("mode".to_string(), format!("\"{v}\""))),
                        _ => {}
                    }
                    #[cfg(not(feature = "zenoh"))]
                    {
                        let _ = value;
                        tracing::warn!(
                            arg,
                            "ignored: this launch was built without the zenoh engine"
                        );
                    }
                }
                _ => opts.extra_args.push(arg),
            }
        }

        #[cfg(feature = "zenoh")]
        if !connect.is_empty() {
            let list = connect
                .iter()
                .map(|e| format!("\"{e}\""))
                .collect::<Vec<_>>()
                .join(",");
            opts.zenoh_overrides
                .push(("connect/endpoints".to_string(), format!("[{list}]")));
        }
        opts
    }

    /// `zenoh` の出どころに `zenoh_overrides` を重ねた zenoh 設定を組む。
    ///
    /// [`Cloudy::open`] がセッションを開く直前に通るのと同じ経路。launch ではないが launch と同じ
    /// fabric に乗りたいツール(`reiny bag` など)が、`--zenoh-config` / `--connect` の
    /// 解釈を写さずに済むための口。
    #[cfg(feature = "zenoh")]
    pub fn zenoh_config(&self) -> Result<zenoh::Config> {
        let mut config = self.zenoh.to_config()?;
        for (key, value) in &self.zenoh_overrides {
            config
                .insert_json5(key, value)
                .map_err(anyhow::Error::msg)
                .map_err(|e| e.context(format!("applying zenoh override {key}={value}")))?;
        }
        Ok(config)
    }

    /// `engine` が無ければ既定のエンジン(zenoh)を開く。
    #[allow(clippy::unused_async)] // zenoh 抜きのビルドでは await 地点が無い。
    async fn take_engine(&mut self) -> Result<Arc<dyn Engine>> {
        if let Some(engine) = self.engine.take() {
            return Ok(engine);
        }
        #[cfg(feature = "zenoh")]
        {
            let engine = crate::engine::Zenoh::open(self.zenoh_config()?).await?;
            Ok(Arc::new(engine))
        }
        #[cfg(not(feature = "zenoh"))]
        {
            anyhow::bail!(
                "no engine: built without the zenoh engine, so RuntimeOptions::engine must be set"
            )
        }
    }
}

fn take<I: Iterator<Item = String>>(args: &mut I, slot: &mut String) {
    if let Some(v) = args.next() {
        *slot = v;
    }
}

impl Cloudy {
    /// オプションからエンジンを開き(または `opts.engine` を受け取り)、`Cloudy` を組む。
    ///
    /// tokio runtime の中で呼ぶ。シグナルは見ない —— `#[tokio::test]` の中で
    /// [`crate::engine::Local`] を挿して launch を回す入口であり、bridge が 2 本目を開く入口。
    /// プロセスの入口は [`run_with`]。
    pub async fn open(mut opts: RuntimeOptions) -> Result<Self> {
        validate_segment("--id", &opts.id)?;
        validate_segment("--domain", &opts.domain)?;
        let config = load_config(opts.config_path.as_deref());
        let engine = opts.take_engine().await?;
        tracing::info!(id = %opts.id, domain = %opts.domain, "reiny launch up");
        Self::new(
            engine,
            opts.id,
            opts.domain,
            Shutdown::new(),
            config,
            opts.extra_args,
        )
        .await
    }
}

/// tokio ランタイムを建て、エンジンとシャットダウン(Ctrl+C / SIGTERM)を用意して
/// 利用側の `async fn main(cloudy)` を実行する。
pub fn run_with<F, Fut>(opts: RuntimeOptions, user: F) -> Result<()>
where
    F: FnOnce(Cloudy) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    if opts.install_tracing {
        let _ = tracing_subscriber::fmt()
            .with_max_level(opts.log_level)
            .try_init();
    }

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    if let Some(n) = opts.worker_threads {
        builder.worker_threads(n);
    }
    let rt = builder.enable_all().build()?;

    rt.block_on(async move {
        let cloudy = Cloudy::open(opts).await?;
        let shutdown = cloudy.shutdown_handle();
        tokio::spawn(async move {
            wait_for_signal().await;
            shutdown.trigger();
        });
        user(cloudy).await
    })
}

/// `--config <path>` を読み、TOML table として parse する。読めない/壊れている場合は
/// 警告して `None`(= `[config]` の既定値だけを使う)。
fn load_config(path: Option<&Path>) -> Option<toml::Table> {
    let path = path?;
    match std::fs::read_to_string(path) {
        Ok(text) => match text.parse::<toml::Table>() {
            Ok(table) => Some(table),
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "ignoring unparsable --config");
                None
            }
        },
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "ignoring unreadable --config");
            None
        }
    }
}

/// 終了シグナルを待つ。unix では SIGTERM も見る(ランチャ・コンテナ・systemd が使うのはこちら)。
/// シグナルを購読できない環境では永久に待つ —— 待てないことを「即終了」に化けさせない。
#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut term = signal(SignalKind::terminate()).ok();
    match term.as_mut() {
        Some(term) => {
            tokio::select! {
                r = tokio::signal::ctrl_c() => {
                    if r.is_err() { return std::future::pending().await; }
                    tracing::info!("Ctrl+C received; shutting down");
                }
                _ = term.recv() => tracing::info!("SIGTERM received; shutting down"),
            }
        }
        None => wait_for_ctrl_c().await,
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    wait_for_ctrl_c().await;
}

async fn wait_for_ctrl_c() {
    if tokio::signal::ctrl_c().await.is_err() {
        // ハンドラを張れないなら「今すぐ終了」ではなく「シグナルは来ない」を選ぶ。
        return std::future::pending().await;
    }
    tracing::info!("Ctrl+C received; shutting down");
}

#[cfg(test)]
#[allow(clippy::expect_used)] // テストは panic で失敗を表現してよい
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> RuntimeOptions {
        RuntimeOptions::from_arg_list("launch", args.iter().map(|s| (*s).to_string()))
    }

    #[test]
    fn known_flags_are_consumed() {
        let o = parse(&[
            "--name",
            "pong-2",
            "--domain",
            "lab",
            "--log-level",
            "debug",
            "--config",
            "g.toml",
        ]);
        assert_eq!(o.id, "pong-2");
        assert_eq!(o.domain, "lab");
        assert_eq!(o.log_level, Level::DEBUG);
        assert_eq!(o.config_path, Some(PathBuf::from("g.toml")));
        assert!(o.extra_args.is_empty());
    }

    #[test]
    fn unknown_args_pass_through_in_order() {
        let o = parse(&["--port", "50051", "--name", "ctrl", "--fast"]);
        assert_eq!(o.id, "ctrl");
        assert_eq!(o.extra_args, ["--port", "50051", "--fast"]);
    }

    #[cfg(feature = "zenoh")]
    #[test]
    fn connect_endpoints_become_one_json5_override() {
        let o = parse(&[
            "--connect",
            "tcp/1.2.3.4:7447",
            "--connect",
            "tcp/5.6.7.8:7447",
            "--zenoh-mode",
            "client",
        ]);
        assert!(
            o.zenoh_overrides
                .contains(&("mode".to_string(), "\"client\"".to_string()))
        );
        let (key, value) = o
            .zenoh_overrides
            .iter()
            .find(|(k, _)| k == "connect/endpoints")
            .expect("connect override present");
        assert_eq!(key, "connect/endpoints");
        assert_eq!(value, "[\"tcp/1.2.3.4:7447\",\"tcp/5.6.7.8:7447\"]");
    }

    #[cfg(feature = "zenoh")]
    #[test]
    fn zenoh_config_flag_selects_file_source() {
        let o = parse(&["--zenoh-config", "z.json5"]);
        assert!(matches!(o.zenoh, ZenohSource::File(p) if p == *Path::new("z.json5")));
    }

    /// zenoh 系の引数は(feature の有無に関わらず)値ごと消費され、`extra_args` に漏れない。
    #[test]
    fn zenoh_args_never_leak_into_extra_args() {
        let o = parse(&[
            "--connect",
            "tcp/1.2.3.4:7447",
            "--zenoh-mode",
            "client",
            "--x",
        ]);
        assert_eq!(o.extra_args, ["--x"]);
    }
}
