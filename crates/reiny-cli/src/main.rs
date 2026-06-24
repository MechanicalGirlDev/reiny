//! `reiny` — grain 雛形生成(`new`/`init`/`add`)・`build`・`run`・`compress` を束ねる CLI。
//!
//! 後方互換: `reiny --config <launch.toml>` と `reiny <launch.toml>`(位置引数)は
//! `reiny run <launch.toml>` と同義。さらに自分の実行ファイル名が `reiny` 以外(= `compress
//! --launcher` でリネームされた配布物)のときは、引数なしで隣の `<basename>.toml` を起動する。

mod buildcmd;
mod checkcmd;
mod compress;
mod runcmd;
mod scaffold;

use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "reiny",
    version,
    about = "reiny grain CLI: new / init / add / check / build / run / compress"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// 新規ディレクトリに grain 雛形を作る(cargo new 相当)。
    New {
        /// 作成するプロジェクトのパス。
        path: PathBuf,
        /// 公開する型名。proto・[publications]・publish 行まで用意する。
        #[arg(long)]
        publish: Option<String>,
        /// プロジェクト名(省略時はディレクトリ名)。
        #[arg(long)]
        name: Option<String>,
    },
    /// 既存ディレクトリにその場で grain 雛形を足す(cargo init 相当)。
    Init {
        /// 対象ディレクトリ(省略時はカレント)。
        path: Option<PathBuf>,
        /// 公開する型名。
        #[arg(long)]
        publish: Option<String>,
        /// プロジェクト名(省略時はディレクトリ名)。
        #[arg(long)]
        name: Option<String>,
    },
    /// カレント grain の Reiny.toml [dependencies] に相手を追記する(cargo add --path 相当)。
    Add {
        /// 依存先プロジェクトへのパス。
        path: PathBuf,
    },
    /// Reiny.toml を解決し、型 → トピック対応とモードを表示する(proto はコンパイルしない)。
    Check {
        /// 対象ディレクトリ(省略時はカレントから上方へ Reiny.toml を探す)。
        path: Option<PathBuf>,
    },
    /// Reiny.toml 駆動の codegen を走らせてビルドする(cargo build のラッパ)。
    Build {
        /// リリースビルド。
        #[arg(long)]
        release: bool,
        /// `--` の後ろは cargo build にそのまま渡す。
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// launch config から grain 群をまとめて起動する。
    Run {
        /// launch config(`[grain]` 節)へのパス。
        config: PathBuf,
        /// grain bin を探すディレクトリ(既定は launch config 基準で自動探索)。
        #[arg(long)]
        bin_dir: Option<PathBuf>,
        /// ランチャ既定のログレベル。
        #[arg(long, default_value = "info")]
        log_level: String,
    },
    /// 動かすのに要るものだけ(grain + 依存ライブラリ + config + ランチャ)を 1 ディレクトリへ束ねる。
    Compress {
        /// launch config へのパス。
        config: PathBuf,
        /// 出力ディレクトリ。
        #[arg(long, default_value = "dist")]
        out: PathBuf,
        /// 同梱するランチャをこの名前にリネームし、`<name>.toml` に揃える。
        #[arg(long)]
        launcher: Option<String>,
        /// システムライブラリも同梱する。
        #[arg(long)]
        include_system: bool,
    },
}

fn main() -> Result<()> {
    // 1. リネームされたランチャの自己起動(`./ping-pong` → 隣の `ping-pong.toml`)。
    if let Some(config) = renamed_launcher_config()? {
        init_tracing("info");
        return runcmd::run_self(&config);
    }

    // 2. 後方互換: `reiny --config X` / `reiny X.toml` を run に振り向ける。
    let argv: Vec<String> = std::env::args().collect();
    if let Some(config) = backward_compat_config(&argv) {
        init_tracing("info");
        return runcmd::run(&config, None, "info");
    }

    // 3. 通常のサブコマンド。
    let cli = Cli::parse();
    match cli.command {
        Command::New {
            path,
            publish,
            name,
        } => scaffold::new(&path, publish.as_deref(), name.as_deref()),
        Command::Init {
            path,
            publish,
            name,
        } => scaffold::init(path.as_deref(), publish.as_deref(), name.as_deref()),
        Command::Add { path } => scaffold::add(&path),
        Command::Check { path } => checkcmd::check(path.as_deref()),
        Command::Build { release, args } => buildcmd::build(release, &args),
        Command::Run {
            config,
            bin_dir,
            log_level,
        } => {
            init_tracing(&log_level);
            runcmd::run(&config, bin_dir, &log_level)
        }
        Command::Compress {
            config,
            out,
            launcher,
            include_system,
        } => compress::compress(&config, &out, launcher.as_deref(), include_system),
    }
}

/// 実行ファイル名が `reiny` 以外なら、隣の `<basename>.toml` を launch config として返す。
fn renamed_launcher_config() -> Result<Option<PathBuf>> {
    let exe = std::env::current_exe().context("resolving current_exe")?;
    let base = exe.file_stem().map(|s| s.to_string_lossy().into_owned());
    let Some(base) = base else {
        return Ok(None);
    };
    if base == "reiny" {
        return Ok(None);
    }
    let dir = exe.parent().unwrap_or_else(|| Path::new("."));
    let config = dir.join(format!("{base}.toml"));
    if config.is_file() {
        Ok(Some(config))
    } else {
        anyhow::bail!(
            "launcher '{base}' expects {} next to it (renamed launcher reads <name>.toml)",
            config.display()
        )
    }
}

/// 後方互換の launch 起動形を検出する。`reiny --config X` / `reiny X`(サブコマンドでない位置引数)。
fn backward_compat_config(argv: &[String]) -> Option<PathBuf> {
    const SUBCOMMANDS: [&str; 8] = [
        "new", "init", "add", "check", "build", "run", "compress", "help",
    ];
    let first = argv.get(1)?;
    if first == "--config" {
        return argv.get(2).map(PathBuf::from);
    }
    if first.starts_with('-') {
        return None; // --help / --version / 不明フラグは clap に任せる。
    }
    if SUBCOMMANDS.contains(&first.as_str()) {
        return None;
    }
    Some(PathBuf::from(first)) // 位置引数のパス = run のショートハンド。
}

fn init_tracing(level: &str) {
    let level = tracing::Level::from_str(level).unwrap_or(tracing::Level::INFO);
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(level)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)] // テストは panic で失敗を表現してよい
mod tests {
    use super::*;

    fn args(items: &[&str]) -> Vec<String> {
        std::iter::once("reiny".to_string())
            .chain(items.iter().map(|s| (*s).to_string()))
            .collect()
    }

    #[test]
    fn config_flag_routes_to_run() {
        let a = args(&["--config", "ping-pong.toml"]);
        assert_eq!(
            backward_compat_config(&a),
            Some(PathBuf::from("ping-pong.toml"))
        );
    }

    #[test]
    fn positional_toml_routes_to_run() {
        let a = args(&["ping-pong.toml"]);
        assert_eq!(
            backward_compat_config(&a),
            Some(PathBuf::from("ping-pong.toml"))
        );
    }

    #[test]
    fn subcommands_are_not_backward_compat() {
        for sub in ["new", "init", "add", "check", "build", "run", "compress"] {
            assert_eq!(backward_compat_config(&args(&[sub])), None, "{sub}");
        }
    }

    #[test]
    fn flags_defer_to_clap() {
        assert_eq!(backward_compat_config(&args(&["--help"])), None);
        assert_eq!(backward_compat_config(&args(&[])), None);
    }
}
