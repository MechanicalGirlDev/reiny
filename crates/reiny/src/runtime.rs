//! The launch runtime's startup options and entry points.
//!
//! `#[reiny::main]` does nothing but call [`RuntimeOptions::from_args`] → [`run_with`], so building
//! the options yourself gets you the same entry point as a library. When the tokio runtime is yours
//! already (tests, a bridge), [`Cloudy::open`] is the async entry point.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use tracing::Level;

use crate::engine::Engine;
use crate::shutdown::Shutdown;
use crate::{Cloudy, DEFAULT_DOMAIN, Result, validate_segment};

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
    /// The instance id. It becomes the key's `<id>` segment.
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
    /// The arguments reiny did not interpret. Passed straight to [`Cloudy::extra_args`](crate::Cloudy::extra_args).
    pub extra_args: Vec<String>,
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
            extra_args: Vec::new(),
        }
    }

    /// Build from the process arguments. The path `#[reiny::main]` takes.
    ///
    /// The only ones interpreted are `--id` / `--name` / `--log-level` / `--config` / `--domain` /
    /// `--zenoh-config` / `--connect` / `--zenoh-mode`. **An unknown argument is not an error**; it
    /// lands in [`RuntimeOptions::extra_args`] — reiny cannot know a launch's own arguments, and being
    /// strict would break every existing launch. Catching typos is the launch's argument parser's job.
    /// In a build without zenoh, the zenoh arguments are warned about and ignored.
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
                // The launcher passes an instance id (numbered, e.g. pong-2). `--name` means the same.
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
    #[allow(clippy::unused_async)] // a build without zenoh has no await point here
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
    /// Open the engine from the options (or take `opts.engine`) and build a `Cloudy`.
    ///
    /// Call it inside a tokio runtime. It watches no signals — it is the entry point for slotting
    /// [`crate::engine::Local`] into a `#[tokio::test]` and running a launch, and the one a bridge uses
    /// to open its second `Cloudy`. The process's entry point is [`run_with`].
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

/// Read `--config <path>` and parse it into a TOML table. Unreadable or broken: warn and return `None`
/// (= use only `[config]`'s defaults).
fn load_config(path: Option<&Path>) -> Option<toml::Table> {
    let path = path?;
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "ignoring unreadable --config");
            return None;
        }
    };
    match parse_config(path, &text) {
        Ok(table) => Some(table),
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "ignoring unparsable --config");
            None
        }
    }
}

/// YAML is the format; `.toml` and `.json` are read by extension. All three deserialize into the
/// same `toml::Table`, so the generated `config()` never sees the difference. A YAML `null` has no
/// TOML counterpart and rejects the whole file.
fn parse_config(path: &Path, text: &str) -> Result<toml::Table> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase);
    Ok(match ext.as_deref() {
        Some("toml") => text.parse::<toml::Table>()?,
        Some("json") => serde_json::from_str(text)?,
        _ => serde_yaml::from_str(text)?,
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
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> RuntimeOptions {
        RuntimeOptions::from_arg_list("launch", args.iter().map(|s| (*s).to_string()))
    }

    /// YAML is the default (any extension but `.toml` / `.json`), and every format lands in the
    /// same table. A file in the wrong format is an error (not an empty table).
    #[test]
    fn config_format_follows_the_extension() {
        let yaml =
            parse_config(Path::new("c.yaml"), "reply: PONG!\ndelay_ms: 250\n").expect("yaml");
        assert_eq!(yaml["reply"].as_str(), Some("PONG!"));
        assert_eq!(yaml["delay_ms"].as_integer(), Some(250));
        let bare = parse_config(Path::new("pong.config"), "delay_ms: 250\n").expect("no ext");
        assert_eq!(bare["delay_ms"].as_integer(), Some(250));
        let json = parse_config(
            Path::new("c.JSON"),
            r#"{"reply": "PONG!", "delay_ms": 250}"#,
        )
        .expect("json");
        assert_eq!(json["reply"].as_str(), Some("PONG!"));
        assert_eq!(json["delay_ms"].as_integer(), Some(250));
        let toml = parse_config(Path::new("c.toml"), "delay_ms = 250\n").expect("toml");
        assert_eq!(toml["delay_ms"].as_integer(), Some(250));
        assert!(parse_config(Path::new("c.yml"), "delay_ms = 250\n").is_err());
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

    /// A zenoh argument is consumed together with its value (feature or no feature) and never leaks
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
