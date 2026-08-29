//! `reiny` — the CLI tying together launch scaffolding (`new`/`init`/`add`), `build`, `run` and `compress`.
//!
//! Backwards compatibility: `reiny --config <launch.toml>` and `reiny <launch.toml>` (a bare
//! positional) both mean `reiny run <launch.toml>`. And when our own executable is not named `reiny`
//! (= an artifact renamed by `compress --launcher`), it starts the neighbouring `<basename>.toml` with no arguments.

mod bagcmd;
mod bridgecmd;
mod buildcmd;
mod bus;
mod checkcmd;
mod codec;
mod compress;
mod flowart;
mod runcmd;
mod scaffold;
mod servicecmd;
mod topiccmd;

use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "reiny",
    version,
    about = "reiny launch CLI: new / init / add / check / build / run / compress / bag / topic / node / service / bridge"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Scaffold a launch in a new directory (the equivalent of cargo new).
    New {
        /// The path of the project to create.
        path: PathBuf,
        /// The type name to publish. The proto, `[publications]` and the publishing line are all set up.
        #[arg(long)]
        publish: Option<String>,
        /// The project name (defaults to the directory name).
        #[arg(long)]
        name: Option<String>,
    },
    /// Add a launch scaffold to an existing directory in place (the equivalent of cargo init).
    Init {
        /// The target directory (defaults to the current one).
        path: Option<PathBuf>,
        /// The type name to publish.
        #[arg(long)]
        publish: Option<String>,
        /// The project name (defaults to the directory name).
        #[arg(long)]
        name: Option<String>,
    },
    /// Add another project to the current launch's Reiny.toml `[dependencies]` (the equivalent of cargo add --path).
    Add {
        /// The path to the project to depend on.
        path: PathBuf,
    },
    /// Resolve a Reiny.toml and print the type → topic mapping and the layout mode (no proto is compiled).
    Check {
        /// The target directory (unset = search upward from the current one for a Reiny.toml).
        path: Option<PathBuf>,
    },
    /// Run the Reiny.toml-driven codegen and build (a wrapper around cargo build).
    Build {
        /// A release build.
        #[arg(long)]
        release: bool,
        /// Everything after `--` is passed to cargo build verbatim.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Start a whole set of launches from a launch config.
    Run {
        /// The path to the launch config (its `[launch]` section).
        config: PathBuf,
        /// Where to look for launch bins (default: derived from the launch config's location).
        #[arg(long)]
        bin_dir: Option<PathBuf>,
        /// The launcher's default log level.
        #[arg(long, default_value = "info")]
        log_level: String,
        /// How many seconds to wait for a `depends_on` to appear on the bus (0 = do not wait).
        #[arg(long, default_value_t = runcmd::DEFAULT_READY_TIMEOUT)]
        ready_timeout: f64,
    },
    /// Gather only what it takes to run (launches + their libraries + configs + the launcher) into one directory.
    Compress {
        /// The path to the launch config.
        config: PathBuf,
        /// The output directory.
        #[arg(long, default_value = "dist")]
        out: PathBuf,
        /// Rename the bundled launcher to this and line the config up as `<name>.toml`.
        #[arg(long)]
        launcher: Option<String>,
        /// Bundle the system libraries too.
        #[arg(long)]
        include_system: bool,
    },
    /// Record / replay / summarize the bus (the equivalent of rosbag2; the format is MCAP).
    Bag(bagcmd::BagArgs),
    /// The live types, their receive rates and their bandwidth (the equivalent of `ros2 topic`).
    Topic(topiccmd::TopicArgs),
    /// The live launches, listed and described (the equivalent of `ros2 node`).
    Node(topiccmd::NodeArgs),
    /// The live services, listed and called with JSON (the equivalent of `ros2 service`).
    Service(servicecmd::ServiceArgs),
    /// Stand up a raw bridge between zenoh and another engine (serial / udp / iceoryx2).
    Bridge(bridgecmd::BridgeArgs),
}

fn main() -> Result<()> {
    // 1. A renamed launcher starting itself (`./ping-pong` → the neighbouring `ping-pong.toml`).
    if let Some(config) = renamed_launcher_config()? {
        init_tracing("info");
        return runcmd::run_self(&config);
    }

    // 2. Backwards compatibility: `reiny --config X` / `reiny X.toml` are routed to run.
    let argv: Vec<String> = std::env::args().collect();
    if let Some(config) = backward_compat_config(&argv) {
        init_tracing("info");
        return runcmd::run(&config, None, "info", runcmd::DEFAULT_READY_TIMEOUT);
    }

    // 3. The ordinary subcommands.
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
            ready_timeout,
        } => {
            init_tracing(&log_level);
            runcmd::run(&config, bin_dir, &log_level, ready_timeout)
        }
        Command::Compress {
            config,
            out,
            launcher,
            include_system,
        } => compress::compress(&config, &out, launcher.as_deref(), include_system),
        Command::Bag(bag) => {
            init_tracing("info");
            bagcmd::run(bag)
        }
        Command::Topic(topic) => {
            init_tracing("warn");
            topiccmd::run_topic(topic)
        }
        Command::Node(node) => {
            init_tracing("warn");
            topiccmd::run_node(node)
        }
        Command::Service(service) => {
            init_tracing("warn");
            servicecmd::run(service)
        }
        Command::Bridge(bridge) => {
            init_tracing("info");
            bridgecmd::run(bridge)
        }
    }
}

/// When our executable is not named `reiny`, return the neighbouring `<basename>.toml` as the launch config.
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

/// Detect the backwards-compatible launch forms: `reiny --config X` / `reiny X` (a positional that is not a subcommand).
fn backward_compat_config(argv: &[String]) -> Option<PathBuf> {
    const SUBCOMMANDS: [&str; 13] = [
        "new", "init", "add", "check", "build", "run", "compress", "bag", "topic", "node",
        "service", "bridge", "help",
    ];
    let first = argv.get(1)?;
    if first == "--config" {
        return argv.get(2).map(PathBuf::from);
    }
    if first.starts_with('-') {
        return None; // --help / --version / an unknown flag: leave it to clap.
    }
    if SUBCOMMANDS.contains(&first.as_str()) {
        return None;
    }
    Some(PathBuf::from(first)) // a positional path = shorthand for run.
}

fn init_tracing(level: &str) {
    let level = tracing::Level::from_str(level).unwrap_or(tracing::Level::INFO);
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(level)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)] // tests may fail by panicking
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
        for sub in [
            "new", "init", "add", "check", "build", "run", "compress", "bag", "topic", "node",
            "service", "bridge",
        ] {
            assert_eq!(backward_compat_config(&args(&[sub])), None, "{sub}");
        }
    }

    #[test]
    fn flags_defer_to_clap() {
        assert_eq!(backward_compat_config(&args(&["--help"])), None);
        assert_eq!(backward_compat_config(&args(&[])), None);
    }
}
