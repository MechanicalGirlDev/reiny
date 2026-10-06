//! Module deployment, typed bus inspection and build-time schema tools.

mod bagcmd;
mod bridgecmd;
mod buildcmd;
mod bus;
mod checkcmd;
mod codec;
mod compress;
mod flowart;
mod modulecmd;
mod scaffold;
mod servicecmd;
mod topiccmd;

use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "reiny",
    version,
    about = "Typed module deployment and pub/sub tools"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Args)]
struct StartArgs {
    /// Root module directory or its main.yaml.
    #[arg(default_value = ".")]
    path: PathBuf,
    /// Leave the deployment owner running after readiness is confirmed.
    #[arg(long)]
    detach: bool,
    /// Override the prebuilt binary directory; declared Cargo builds are unchanged.
    #[arg(long)]
    bin_dir: Option<PathBuf>,
    /// Supervisor log level.
    #[arg(long, default_value = "info")]
    log_level: String,
    /// Deadline in seconds for explicit module readiness.
    #[arg(long, default_value_t = 30.0)]
    ready_timeout: f64,
}

#[derive(Subcommand)]
enum Command {
    /// Create a standalone module with main.yaml and a Cargo build declaration.
    New {
        path: PathBuf,
        #[arg(long)]
        publish: Option<String>,
        #[arg(long)]
        name: Option<String>,
    },
    /// Add a standalone module scaffold without replacing existing files.
    Init {
        path: Option<PathBuf>,
        #[arg(long)]
        publish: Option<String>,
        #[arg(long)]
        name: Option<String>,
    },
    /// Add a local build-time schema dependency.
    Add { path: PathBuf },
    /// Check module connections or describe the build-time schema without compiling.
    Check { path: Option<PathBuf> },
    /// Prepare declared build artifacts without starting application processes.
    Build {
        #[arg(long)]
        release: bool,
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Resolve sources and show desired modules and connections without building.
    Plan {
        #[arg(default_value = ".")]
        path: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Prepare and reconcile a deployment; unchanged modules keep their processes.
    Apply(StartArgs),
    /// Run a deployment in the foreground (the same managed contract as apply).
    Run(StartArgs),
    /// Read actual state from the live owner, not a stale PID file.
    Status {
        #[arg(default_value = ".")]
        path: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Cooperatively stop and reap the deployment's owned processes.
    Stop {
        #[arg(default_value = ".")]
        path: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Explicitly update Git module refs and the revision lock.
    Update {
        #[arg(default_value = ".")]
        path: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Build and bundle a self-contained module deployment.
    Compress {
        #[arg(default_value = ".")]
        config: PathBuf,
        #[arg(long, default_value = "dist")]
        out: PathBuf,
        #[arg(long)]
        launcher: Option<String>,
        #[arg(long)]
        include_system: bool,
    },
    /// Record, replay or inspect typed MCAP data.
    Bag(bagcmd::BagArgs),
    /// Inspect or publish typed topics, including nested module namespaces.
    Topic(topiccmd::TopicArgs),
    /// Inspect live executable namespaces.
    Node(topiccmd::NodeArgs),
    /// Inspect and call typed services.
    Service(servicecmd::ServiceArgs),
    /// Bridge communication engines.
    Bridge(bridgecmd::BridgeArgs),
    /// Internal authenticated deployment owner.
    #[command(name = "__supervise", hide = true)]
    Supervise { prepared: PathBuf },
}

fn main() -> Result<()> {
    let arguments: Vec<String> = std::env::args().collect();
    if let Some(path) = positional_module(&arguments) {
        init_tracing("info");
        return modulecmd::apply(&path, false, None, 30.0);
    }
    if arguments.len() == 1 {
        let exe = std::env::current_exe().context("resolving executable")?;
        if exe.file_stem().is_some_and(|name| name != "reiny")
            && let Some(dir) = exe.parent()
            && dir.join("main.yaml").is_file()
        {
            init_tracing("info");
            return modulecmd::apply(dir, false, None, 30.0);
        }
    }
    match Cli::parse().command {
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
        Command::Plan { path, json } => modulecmd::plan(&path, false, json),
        Command::Update { path, json } => modulecmd::plan(&path, true, json),
        Command::Apply(args) | Command::Run(args) => {
            init_tracing(&args.log_level);
            modulecmd::apply(&args.path, args.detach, args.bin_dir, args.ready_timeout)
        }
        Command::Status { path, json } => modulecmd::status(&path, json),
        Command::Stop { path, json } => modulecmd::stop(&path, json),
        Command::Supervise { prepared } => {
            init_tracing("info");
            modulecmd::supervise(&prepared)
        }
        Command::Compress {
            config,
            out,
            launcher,
            include_system,
        } => compress::compress(&config, &out, launcher.as_deref(), include_system),
        Command::Bag(args) => {
            init_tracing("info");
            bagcmd::run(args)
        }
        Command::Topic(args) => {
            init_tracing("info");
            topiccmd::run_topic(args)
        }
        Command::Node(args) => {
            init_tracing("info");
            topiccmd::run_node(args)
        }
        Command::Service(args) => {
            init_tracing("info");
            servicecmd::run(args)
        }
        Command::Bridge(args) => {
            init_tracing("info");
            bridgecmd::run(args)
        }
    }
}

fn positional_module(argv: &[String]) -> Option<PathBuf> {
    const COMMANDS: &[&str] = &[
        "new",
        "init",
        "add",
        "check",
        "build",
        "plan",
        "apply",
        "run",
        "status",
        "stop",
        "update",
        "compress",
        "bag",
        "topic",
        "node",
        "service",
        "bridge",
        "help",
        "__supervise",
    ];
    let first = argv.get(1)?;
    if first == "--config" {
        return argv.get(2).map(PathBuf::from);
    }
    (!first.starts_with('-') && !COMMANDS.contains(&first.as_str())).then(|| PathBuf::from(first))
}

fn init_tracing(level: &str) {
    let level = tracing::Level::from_str(level).unwrap_or(tracing::Level::INFO);
    if let Err(error) = tracing_subscriber::fmt()
        .with_max_level(level)
        .with_writer(std::io::stderr)
        .try_init()
    {
        eprintln!("tracing already initialized: {error}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn module_commands_are_not_positional_paths() {
        for command in [
            "plan",
            "apply",
            "status",
            "stop",
            "update",
            "__supervise",
            "bag",
        ] {
            assert_eq!(positional_module(&["reiny".into(), command.into()]), None);
        }
    }

    #[test]
    fn positional_directory_selects_its_module() {
        assert_eq!(
            positional_module(&["reiny".into(), "projects/robot".into()]),
            Some(PathBuf::from("projects/robot")),
        );
    }

    #[test]
    fn global_flags_remain_clap_arguments() {
        assert_eq!(positional_module(&["reiny".into(), "--help".into()]), None);
        assert_eq!(
            positional_module(&["reiny".into(), "--version".into()]),
            None
        );
    }

    #[test]
    fn config_alias_does_not_change_module_loading_rules() {
        assert_eq!(
            positional_module(&["reiny".into(), "--config".into(), "main.yaml".into()]),
            Some(PathBuf::from("main.yaml")),
        );
    }
}
