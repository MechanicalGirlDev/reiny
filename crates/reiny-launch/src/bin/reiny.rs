//! `reiny` — launch config の `[grain]` 節から grain 群を一括起動する。

use std::path::PathBuf;

use clap::Parser;
use tracing::Level;

use reiny_launch::LaunchPlan;

#[derive(Parser, Debug)]
#[command(name = "reiny")]
#[command(about = "Launch reiny grains from a launch config's [grain] section")]
struct Args {
    /// 起動する launch config へのパス。`[grain]` 節を読む。
    #[arg(long)]
    config: PathBuf,

    /// grain bin を探すディレクトリ(既定: reiny と同じ場所)。
    #[arg(long)]
    bin_dir: Option<PathBuf>,

    /// ランチャ自身のログレベル(per-grain override の無い子にも伝播する)。
    #[arg(long, default_value = "info")]
    log_level: Level,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(args.log_level)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let plan = LaunchPlan::from_launch_config(&args.config)?;
    // ランチャのログレベルを、override の無い子へ既定として伝播する。
    let level = args.log_level.to_string();
    reiny_launch::run_launch(&plan, args.bin_dir, Some(&level))
}
