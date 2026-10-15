//! Module deployment, typed bus inspection and build-time schema tools.

mod applycmd;
mod bagcmd;
mod bridgecmd;
mod buildcmd;
mod bundle;
mod bus;
mod checkcmd;
mod codec;
mod control;
mod flowart;
mod modulecmd;
mod preparecmd;
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
    #[arg(conflicts_with = "prepared")]
    path: Option<PathBuf>,
    /// Apply verified frozen artifacts without resolving or rebuilding sources.
    #[arg(long, conflicts_with_all = ["path", "bin_dir", "release", "build_cache", "jobs"])]
    prepared: Option<PathBuf>,
    /// Leave the deployment owner running after readiness is confirmed.
    #[arg(long, conflicts_with = "control_stdin")]
    detach: bool,
    #[command(flatten)]
    build: preparecmd::BuildOptions,
    /// Emit versioned JSON lines exclusively on stdout.
    #[arg(long, value_enum)]
    events: Option<preparecmd::Events>,
    /// Accept cancellation controls; stdin EOF also cancels.
    #[arg(long)]
    control_stdin: bool,
    /// Supervisor log level.
    #[arg(long, default_value = "info")]
    log_level: String,
    /// Deadline in seconds for explicit module readiness.
    #[arg(long, default_value_t = 30.0)]
    ready_timeout: f64,
}

#[derive(Subcommand)]
enum Command {
    /// Report exact preparation and controlled-apply capabilities.
    Capabilities {
        #[arg(long)]
        json: bool,
    },
    /// Freeze a deployment into a new verified prepared file.
    Prepare {
        path: PathBuf,
        #[arg(long)]
        out: PathBuf,
        #[arg(long, value_enum)]
        events: Option<preparecmd::Events>,
        #[arg(long)]
        control_stdin: bool,
        #[command(flatten)]
        build: preparecmd::BuildOptions,
    },
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
        #[arg(long, value_enum)]
        events: Option<preparecmd::Events>,
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
    Bundle {
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

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error:#}");
            if matches!(
                error.downcast_ref::<reiny_launch::PrepareError>(),
                Some(reiny_launch::PrepareError::Cancelled)
            ) {
                std::process::ExitCode::from(130)
            } else {
                std::process::ExitCode::FAILURE
            }
        }
    }
}

fn run() -> Result<()> {
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
    dispatch(Cli::parse().command)
}

fn dispatch(command: Command) -> Result<()> {
    match command {
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
        Command::Capabilities { json } => capabilities(json),
        Command::Prepare {
            path,
            out,
            events,
            control_stdin,
            build,
        } => {
            init_tracing("info");
            let control = control::Control::install(control_stdin)?;
            preparecmd::prepare(
                &path,
                Some(&out),
                &build,
                &control,
                &preparecmd::Protocol::new(events),
            )?;
            Ok(())
        }
        Command::Build {
            release,
            events,
            args,
        } => buildcmd::build(release, events, &args),
        Command::Plan { path, json } => modulecmd::plan(&path, false, json),
        Command::Update { path, json } => modulecmd::plan(&path, true, json),
        Command::Apply(args) | Command::Run(args) => {
            init_tracing(&args.log_level);
            modulecmd::apply_args(args)
        }
        Command::Status { path, json } => modulecmd::status(&path, json),
        Command::Stop { path, json } => modulecmd::stop(&path, json),
        Command::Supervise { prepared } => {
            init_tracing("info");
            modulecmd::supervise(&prepared)
        }
        Command::Bundle {
            config,
            out,
            launcher,
            include_system,
        } => {
            let control = control::Control::install(false)?;
            bundle::bundle_with_control(
                &config,
                &out,
                launcher.as_deref(),
                include_system,
                &control,
            )
        }
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

fn capabilities(json: bool) -> Result<()> {
    #[derive(serde::Serialize)]
    struct Capabilities {
        reiny_version: &'static str,
        prepare_protocol: u32,
        prepared_file: u32,
        apply_protocol: u32,
        build_kinds: [&'static str; 2],
        controlled_apply: bool,
    }
    let capabilities = Capabilities {
        reiny_version: reiny_launch::VERSION,
        prepare_protocol: reiny_launch::PREPARE_PROTOCOL_VERSION,
        prepared_file: reiny_launch::PREPARED_FILE_VERSION,
        apply_protocol: reiny_launch::APPLY_PROTOCOL_VERSION,
        build_kinds: ["cargo", "command"],
        controlled_apply: true,
    };
    if json {
        println!("{}", serde_json::to_string(&capabilities)?);
    } else {
        println!("{}", serde_json::to_string_pretty(&capabilities)?);
    }
    Ok(())
}

fn positional_module(argv: &[String]) -> Option<PathBuf> {
    const COMMANDS: &[&str] = &[
        "new",
        "init",
        "add",
        "check",
        "build",
        "prepare",
        "capabilities",
        "plan",
        "apply",
        "run",
        "status",
        "stop",
        "update",
        "bundle",
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
