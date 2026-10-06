//! The launch runtime's startup options and entry points.
//!
//! `#[reiny::main]` does nothing but call [`RuntimeOptions::from_args`] → [`run_with`], so building
//! the options yourself gets you the same entry point as a library. When the tokio runtime is yours
//! already (tests, a bridge), [`Cloudy::open`] is the async entry point.

use std::future::Future;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use tracing::Level;

use crate::bindings::ModuleBindings;
use crate::engine::Engine;
use crate::managed::MODULE_REPORT_ENV;
use crate::{Cloudy, DEFAULT_DOMAIN, Result};

mod startup;

/// The environment variable key for giving the domain. The way to split systems apart without editing
/// a config — parallel CI jobs, for instance.
pub const DOMAIN_ENV: &str = "REINY_DOMAIN";

/// Where a zenoh session's configuration comes from.
///
/// reiny **does not wrap** zenoh's configuration schema. Wrapping it would create an obligation to
/// track zenoh's settings, which betrays the premise that thinness is the value.
#[cfg(feature = "zenoh")]
pub enum ZenohSource {
    /// `zenoh::Config::default()` (as in 0.2).
    Default,
    /// `zenoh::Config::from_env()`.
    Env,
    /// `zenoh::Config::from_file()` (JSON5 / JSON / YAML).
    File(PathBuf),
    /// One the caller assembled.
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

/// The launch runtime's startup options.
pub struct RuntimeOptions {
    /// The instance namespace. Slash-separated module paths are preserved in the key's source.
    pub id: String,
    /// The logical namespace. It becomes the key's `<domain>` segment.
    pub domain: String,
    /// The engine to use. `None` opens zenoh (feature `zenoh`) from `zenoh` / `zenoh_overrides`.
    /// Tests slot [`crate::engine::Local`] in here; a bridge slots in an engine of its own.
    pub engine: Option<Arc<dyn Engine>>,
    /// Where the zenoh session configuration comes from.
    #[cfg(feature = "zenoh")]
    pub zenoh: ZenohSource,
    /// The (key, json5) pairs applied with `Config::insert_json5` after `zenoh` is assembled.
    /// The CLI's `--connect` / `--zenoh-mode` land here.
    #[cfg(feature = "zenoh")]
    pub zenoh_overrides: Vec<(String, String)>,
    /// Whether reiny installs a global `tracing_subscriber`.
    ///
    /// A launch with a subscriber of its own (a log-collecting layer, say) sets it to `false`. Left
    /// `true`, reiny silently does nothing if it is beaten to it (`try_init` does not win when it runs
    /// later), which means taking on an ordering dependency on "install it before reiny does".
    pub install_tracing: bool,
    /// The log level, when `install_tracing` is true.
    pub log_level: Level,
    /// tokio's worker thread count (unset = tokio's default, the CPU count).
    pub worker_threads: Option<usize>,
    /// `--config <path>`. The raw material for [`Cloudy::config_table`](crate::Cloudy::config_table).
    pub config_path: Option<PathBuf>,
    /// A resolved named-port contract supplied directly by an embedding supervisor.
    pub module_bindings: Option<ModuleBindings>,
    /// The absolute JSON contract supplied by `--module-bindings`.
    pub module_bindings_path: Option<PathBuf>,
    /// Optional absolute runtime report path, defaulting to `REINY_MODULE_REPORT`.
    pub module_report_path: Option<PathBuf>,
    /// The arguments reiny did not interpret. Passed straight to [`Cloudy::extra_args`](crate::Cloudy::extra_args).
    pub extra_args: Vec<String>,
    argument_errors: Vec<String>,
    bindings_requested: bool,
}

impl RuntimeOptions {
    /// The defaults. Apart from `id`, the bare state that has looked at no `--` argument.
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
            module_bindings: None,
            module_bindings_path: None,
            module_report_path: std::env::var_os(MODULE_REPORT_ENV).map(PathBuf::from),
            extra_args: Vec::new(),
            argument_errors: Vec::new(),
            bindings_requested: false,
        }
    }

    /// Build from the process arguments. The path `#[reiny::main]` takes.
    ///
    /// The only ones interpreted are `--id` / `--name` / `--log-level` / `--config` / `--domain` /
    /// `--module-bindings` / `--zenoh-config` / `--connect` / `--zenoh-mode`.
    /// **An unknown argument is not an error**; it
    /// lands in [`RuntimeOptions::extra_args`] — reiny cannot know a launch's own arguments, and being
    /// strict would break every existing launch. Catching typos is the launch's argument parser's job.
    /// In a build without zenoh, the zenoh arguments are warned about and ignored.
    #[must_use]
    pub fn from_args(default_id: &str) -> Self {
        Self::from_arg_list(default_id, std::env::args().skip(1))
    }

    fn from_arg_list<I: IntoIterator<Item = String>>(default_id: &str, args: I) -> Self {
        let mut opts = Self::new(default_id);
        let mut args = args.into_iter().peekable();
        #[cfg(feature = "zenoh")]
        let mut connect: Vec<String> = Vec::new();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                // The launcher passes an instance id (numbered, e.g. pong-2). `--name` means the same.
                "--id" | "--name" | "--domain" | "--config" | "--module-bindings" => {
                    if arg == "--module-bindings" {
                        if opts.bindings_requested {
                            opts.argument_errors
                                .push("repeated --module-bindings".to_string());
                        }
                        opts.bindings_requested = true;
                    }
                    match args.next_if(|value| !value.starts_with("--")) {
                        Some(value) => match arg.as_str() {
                            "--id" | "--name" => opts.id = value,
                            "--domain" => opts.domain = value,
                            "--config" => opts.config_path = Some(PathBuf::from(value)),
                            // The enclosing match limits this arm to --module-bindings.
                            _ => opts.module_bindings_path = Some(PathBuf::from(value)),
                        },
                        None => opts.argument_errors.push(format!("{arg} requires a value")),
                    }
                }
                "--log-level" => {
                    if let Some(v) = args.next()
                        && let Ok(level) = Level::from_str(&v)
                    {
                        opts.log_level = level;
                    }
                }
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

    /// The zenoh configuration: the `zenoh` source with `zenoh_overrides` layered on top.
    ///
    /// The very path [`Cloudy::open`] goes through just before opening a session. The door for a tool
    /// that is not a launch but wants to be on the same fabric as one (`reiny bag` …) to avoid copying
    /// how `--zenoh-config` / `--connect` are interpreted.
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

    /// Open the default engine (zenoh) when there is no `engine`.
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
            std::future::ready(Err(anyhow::anyhow!(
                "no engine: built without the zenoh engine, so RuntimeOptions::engine must be set"
            )))
            .await
        }
    }
}

/// Build a tokio runtime, set up the engine and shutdown (Ctrl+C / SIGTERM), and run the caller's
/// `async fn main(cloudy)`.
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

/// Wait for a termination signal. On unix it watches SIGTERM too (what launchers, containers and
/// systemd use). Where signals cannot be subscribed to it waits forever — "cannot wait" must never
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
        // If no handler can be installed, choose "the signal never comes" over "exit right now".
        return std::future::pending().await;
    }
    tracing::info!("Ctrl+C received; shutting down");
}

#[cfg(test)]
#[allow(clippy::expect_used)] // tests may fail by panicking
mod tests;
