//! `Component` トレイトと、それを単一プロセスとして動かす `run_component`。

use clap::{Args, Command, FromArgMatches};
use tracing::Level;

use crate::Shutdown;

/// 全コンポーネント bin 共通の最小引数。
#[derive(Args, Debug, Clone)]
pub struct CommonArgs {
    /// ログ出力レベル (trace/debug/info/warn/error)。
    #[arg(long, default_value = "info")]
    pub log_level: Level,

    /// ログ/診断で識別するインスタンス名（未指定時は `Component::KIND`）。
    #[arg(long)]
    pub name: Option<String>,
}

/// ホスト非依存のコンポーネント。`run()` はどのスレッドからでも呼べ、
/// 自前で必要なランタイムを建て、`Shutdown` で終了する。
pub trait Component: Sized {
    /// 種別名（既定インスタンス名・ログ用）。例: "app" / "gui" / "policy"。
    const KIND: &'static str;

    /// コンポーネント固有の CLI 引数（`--config` 等）。
    type Args: Args;

    /// 共通引数と固有引数からコンポーネントを構築する。
    fn build(common: &CommonArgs, args: Self::Args) -> anyhow::Result<Self>;

    /// シャットダウンされるまで動作する（ブロッキング）。
    fn run(self, shutdown: Shutdown) -> anyhow::Result<()>;
}

/// `CommonArgs` と `C::Args` を合成した clap コマンドを組み立てる。
/// （clap derive は総称型を扱えないため builder API を使う。）
fn build_command<C: Component>() -> Command {
    let cmd = Command::new(C::KIND);
    let cmd = CommonArgs::augment_args(cmd);
    <C::Args as Args>::augment_args(cmd)
}

/// 各 bin シムが呼ぶ唯一のエントリ:
/// 引数パース → tracing 初期化 → 起動ログ → Ctrl+C ハンドラ → build → run。
pub fn run_component<C: Component>() -> anyhow::Result<()> {
    let matches = build_command::<C>().get_matches();
    let common = CommonArgs::from_arg_matches(&matches)
        .map_err(|e| anyhow::anyhow!("failed to parse common args: {e}"))?;
    let args = <C::Args as FromArgMatches>::from_arg_matches(&matches)
        .map_err(|e| anyhow::anyhow!("failed to parse component args: {e}"))?;

    init_tracing(common.log_level);
    let name = common.name.clone().unwrap_or_else(|| C::KIND.to_string());
    tracing::info!("starting component '{name}' (kind={})", C::KIND);

    // Ctrl+C → shutdown。コンソール配下の全子プロセスへ OS が配送するため、
    // 各コンポーネントが自分で綺麗に終了できる。
    let shutdown = Shutdown::new();
    let sd = shutdown.clone();
    if let Err(e) = ctrlc::set_handler(move || sd.trigger()) {
        tracing::warn!("failed to install Ctrl+C handler: {e}");
    }

    let component = C::build(&common, args)?;
    let result = component.run(shutdown);
    match result {
        Ok(()) => tracing::info!("component '{name}' stopped"),
        Err(ref e) => tracing::error!("component '{name}' stopped with error: {e:#}"),
    }
    result
}

/// プロセスにつき一度だけグローバル subscriber を設定する（失敗は無視）。
fn init_tracing(level: Level) {
    use tracing_subscriber::FmtSubscriber;
    let subscriber = FmtSubscriber::builder().with_max_level(level).finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 引数なしの固有引数（テスト用ダミー）。
    #[derive(Args, Debug)]
    struct NoArgs;

    struct Dummy;
    impl Component for Dummy {
        const KIND: &'static str = "dummy";
        type Args = NoArgs;
        fn build(_c: &CommonArgs, _a: Self::Args) -> anyhow::Result<Self> {
            Ok(Dummy)
        }
        fn run(self, _s: Shutdown) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn parses_common_args() {
        let matches = build_command::<Dummy>()
            .try_get_matches_from(["dummy", "--log-level", "debug", "--name", "foo"])
            .unwrap();
        let common = CommonArgs::from_arg_matches(&matches).unwrap();
        assert_eq!(common.log_level, Level::DEBUG);
        assert_eq!(common.name.as_deref(), Some("foo"));
    }

    #[test]
    fn common_args_have_defaults() {
        let matches = build_command::<Dummy>()
            .try_get_matches_from(["dummy"])
            .unwrap();
        let common = CommonArgs::from_arg_matches(&matches).unwrap();
        assert_eq!(common.log_level, Level::INFO);
        assert!(common.name.is_none());
    }
}
