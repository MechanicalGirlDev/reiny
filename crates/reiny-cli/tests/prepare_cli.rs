//! Real CLI processes: protocol framing, cancellation, frozen apply and leases.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use reiny::zenoh::{self, Wait};
use reiny_launch::{ApplyEvent, PrepareEvent, PreparedDeployment};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, Lines};

const BIN: &str = env!("CARGO_BIN_EXE_reiny");
const DEADLINE: Duration = Duration::from_secs(45);
const FIXTURE: &str = r#"
use std::{env,fs,io::{Read,Write},net::TcpStream,process::Command};
fn main() {
    let args: Vec<_> = env::args().collect();
    let value = |name: &str| args.windows(2).find(|p| p[0] == name).map(|p| p[1].clone());
    if let Some(address) = value("--child") {
        let mut socket = TcpStream::connect(address).unwrap();
        writeln!(socket, "child {}", std::process::id()).unwrap();
        let mut byte = [0]; let _ = socket.read(&mut byte); return;
    }
    if let Some(output) = value("--build") {
        println!("builder stdout is not protocol");
        eprintln!("builder stderr");
        let mut marker = fs::OpenOptions::new().create(true).append(true).open("invocations").unwrap();
        writeln!(marker, "build").unwrap();
        if let Some(address) = value("--gate") {
            Command::new(env::current_exe().unwrap()).args(["--child", &address]).spawn().unwrap();
            let mut socket = TcpStream::connect(address).unwrap();
            writeln!(socket, "builder {}", std::process::id()).unwrap();
            let mut byte = [0]; socket.read_exact(&mut byte).unwrap();
        }
        if value("--fail").is_some() { std::process::exit(23); }
        fs::copy(env::current_exe().unwrap(), output).unwrap(); return;
    }
    if let Some(report) = value("--report") {
        let path = env::var("REINY_MODULE_REPORT").unwrap();
        fs::write(format!("{path}.tmp"), report).unwrap();
        fs::rename(format!("{path}.tmp"), path).unwrap();
    }
    let mut socket = TcpStream::connect(value("--address").unwrap()).unwrap();
    writeln!(socket, "app {}", std::process::id()).unwrap();
    let mut byte = [0]; let _ = socket.read(&mut byte);
}
"#;

struct Fixture {
    root: tempfile::TempDir,
    builder: PathBuf,
    domain: String,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        match reiny_launch::DeploymentClient::find(self.root.path()) {
            Ok(Some(client)) => {
                if let Err(error) = client.stop() {
                    eprintln!("fixture owner cleanup: {error:#}");
                }
            }
            Ok(None) => {}
            Err(error) => eprintln!("fixture endpoint cleanup: {error:#}"),
        }
    }
}

impl Fixture {
    fn new() -> Result<Self> {
        let root = tempfile::tempdir()?;
        let source = root.path().join("fixture.rs");
        let builder = root
            .path()
            .join(format!("builder{}", std::env::consts::EXE_SUFFIX));
        std::fs::write(&source, FIXTURE)?;
        let output = Command::new("rustc")
            .arg("--edition=2024")
            .arg(source)
            .arg("-o")
            .arg(&builder)
            .output()?;
        ensure!(
            output.status.success(),
            "fixture compilation: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let domain = format!(
            "test-{}",
            root.path()
                .file_name()
                .context("fixture directory")?
                .to_string_lossy()
                .trim_start_matches('.')
        );
        Ok(Self {
            root,
            builder,
            domain,
        })
    }

    fn manifest(&self, gate: Option<String>, runtime: Option<(&str, &str)>) -> Result<()> {
        let mut argv = vec![
            self.builder.display().to_string(),
            "--build".into(),
            format!("app{}", std::env::consts::EXE_SUFFIX),
        ];
        if let Some(gate) = gate {
            argv.extend(["--gate".into(), gate]);
        }
        let runtime_args = runtime.map_or_else(Vec::new, |(address, _)| vec![
            "--address".to_owned(), address.to_owned(), "--report".to_owned(),
            json!({"version":3,"fingerprint_version":2,"namespace":"cli","endpoint_namespace":"cli","inputs":{},"outputs":{}}).to_string(),
        ]);
        let provider = runtime.map_or(json!({"type":"process"}), |(_, endpoint)| {
            json!({"type":"process","connect":[endpoint],"zenoh_config":"zenoh.json"})
        });
        std::fs::write(
            self.root.path().join("zenoh.json"),
            r#"{"listen":{"endpoints":[]},"scouting":{"multicast":{"enabled":false}}}"#,
        )?;
        std::fs::write(
            self.root.path().join("main.yaml"),
            serde_yaml::to_string(&json!({
            "version":3, "deployment":"cli", "domain":self.domain,
                "providers":{"process":provider},
            "run":{"provider":"process","bin":"app","args":runtime_args},
                "build":{"type":"command","argv":argv,"outputs":{"app":format!("app{}",std::env::consts::EXE_SUFFIX)}}
            }))?,
        )?;
        Ok(())
    }

    fn prepared(&self) -> PathBuf {
        self.root.path().join("prepared.json")
    }

    fn ready_key(&self) -> String {
        format!("reiny/v3/{}/cli/@ready", self.domain)
    }

    fn prepare(&self) -> Result<Vec<Value>> {
        let output = Command::new(BIN)
            .arg("prepare")
            .arg(self.root.path())
            .arg("--out")
            .arg(self.prepared())
            .args(["--events", "jsonl"])
            .stdin(Stdio::null())
            .output()?;
        ensure!(
            output.status.success(),
            "prepare: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        protocol(&output.stdout, "success")
    }
}

fn protocol(stdout: &[u8], status: &str) -> Result<Vec<Value>> {
    ensure!(stdout.last() == Some(&b'\n'), "missing final newline");
    let events: Vec<Value> = std::str::from_utf8(stdout)?
        .lines()
        .map(serde_json::from_str)
        .collect::<std::result::Result<_, _>>()?;
    for (index, event) in events.iter().enumerate() {
        assert_eq!(event["v"], 1);
        assert_eq!(event["seq"], u64::try_from(index)? + 1);
        if events
            .first()
            .is_some_and(|first| first["event"] == "apply_started")
        {
            let mut payload = event.clone();
            let fields = payload.as_object_mut().context("apply event object")?;
            let _version = fields.remove("v");
            let _sequence = fields.remove("seq");
            let _event: ApplyEvent = serde_json::from_value(payload)?;
        }
    }
    assert_eq!(
        events
            .iter()
            .filter(|event| event["event"] == "finished")
            .count(),
        1
    );
    assert_eq!(events.last().context("terminal")?["status"], status);
    Ok(events)
}

struct Cli {
    child: tokio::process::Child,
    lines: Lines<BufReader<tokio::process::ChildStdout>>,
    stdout: Vec<u8>,
    stderr: tempfile::NamedTempFile,
    owner_log: PathBuf,
}

impl Cli {
    fn spawn(command: &str, fixture: &Fixture) -> Result<Self> {
        Self::spawn_with(command, fixture, |_| {})
    }

    fn spawn_with(
        command: &str,
        fixture: &Fixture,
        configure: impl FnOnce(&mut tokio::process::Command),
    ) -> Result<Self> {
        let stderr = tempfile::NamedTempFile::new()?;
        let mut process = tokio::process::Command::new(BIN);
        configure(&mut process);
        process.arg(if command == "apply-detached" {
            "apply"
        } else {
            command
        });
        if command == "prepare" {
            process
                .arg(fixture.root.path())
                .arg("--out")
                .arg(fixture.prepared());
        } else {
            process
                .arg("--prepared")
                .arg(fixture.prepared())
                .args(["--ready-timeout", "20"]);
            // A prepared apply cannot use any compiler or the original builder.
            process.env("CARGO", fixture.root.path().join("compiler-does-not-exist"));
        }
        process.args(["--events", "jsonl"]);
        if command == "apply-detached" {
            process.arg("--detach");
        } else {
            process.arg("--control-stdin");
        }
        let mut child = process
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(stderr.reopen()?)
            .kill_on_drop(true)
            .spawn()?;
        let lines = BufReader::new(child.stdout.take().context("stdout")?).lines();
        Ok(Self {
            child,
            lines,
            stdout: Vec::new(),
            stderr,
            owner_log: fixture.root.path().join(".reiny/owner.log"),
        })
    }

    fn diagnostic(&self) -> String {
        format!(
            "events: {}; stderr: {}; owner: {}",
            String::from_utf8_lossy(&self.stdout),
            std::fs::read_to_string(self.stderr.path()).unwrap_or_default(),
            std::fs::read_to_string(&self.owner_log).unwrap_or_default()
        )
    }

    async fn event(&mut self, name: &str) -> Result<Value> {
        loop {
            let line = tokio::time::timeout(DEADLINE, self.lines.next_line())
                .await
                .with_context(|| format!("waiting for {name}; {}", self.diagnostic()))??
                .with_context(|| {
                    format!(
                        "CLI ended: {}",
                        std::fs::read_to_string(self.stderr.path()).unwrap_or_default()
                    )
                })?;
            self.stdout.extend_from_slice(line.as_bytes());
            self.stdout.push(b'\n');
            let event: Value = serde_json::from_str(&line)?;
            if event["event"] == name {
                return Ok(event);
            }
        }
    }

    async fn cancel(&mut self, input: &[u8]) -> Result<()> {
        self.child
            .stdin
            .as_mut()
            .context("stdin")?
            .write_all(input)
            .await?;
        Ok(())
    }

    async fn finish(mut self, status: &str, code: i32) -> Result<Vec<Value>> {
        while let Some(line) = tokio::time::timeout(DEADLINE, self.lines.next_line())
            .await
            .with_context(|| format!("waiting for CLI stdout EOF; {}", self.diagnostic()))??
        {
            self.stdout.extend_from_slice(line.as_bytes());
            self.stdout.push(b'\n');
            if serde_json::from_str::<Value>(&line)?["event"] == "finished" {
                break;
            }
        }
        let exit = tokio::time::timeout(DEADLINE, self.child.wait())
            .await
            .with_context(|| format!("waiting for CLI process exit; {}", self.diagnostic()))??;
        while let Some(line) = tokio::time::timeout(DEADLINE, self.lines.next_line())
            .await
            .context("waiting for stdout EOF after CLI process exit")??
        {
            self.stdout.extend_from_slice(line.as_bytes());
            self.stdout.push(b'\n');
        }
        assert_eq!(
            exit.code(),
            Some(code),
            "{}",
            std::fs::read_to_string(self.stderr.path())?
        );
        protocol(&self.stdout, status)
    }
}

#[cfg(windows)]
#[tokio::test]
async fn console_interrupt_cancels_and_reaps_the_running_builder_tree() -> Result<()> {
    // Given a CLI in its own console, so the signal cannot reach the test runner.
    let fixture = Fixture::new()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    fixture.manifest(Some(listener.local_addr()?.to_string()), None)?;
    let cli = Cli::spawn_with("prepare", &fixture, |command| {
        command.creation_flags(0x0000_0010); // CREATE_NEW_CONSOLE
    })?;
    let first = connection(&listener)
        .await
        .context("console fixture first process")?;
    let second = connection(&listener)
        .await
        .context("console fixture second process")?;
    // When an attached helper sends a console interrupt to the registered handler.
    let script = format!(
        r#"
Add-Type -TypeDefinition 'using System; using System.Runtime.InteropServices; public static class Signal {{ public delegate bool Handler(uint kind); public static Handler Ignore = (kind) => true; [DllImport("kernel32.dll",SetLastError=true)] public static extern bool FreeConsole(); [DllImport("kernel32.dll",SetLastError=true)] public static extern bool AttachConsole(uint pid); [DllImport("kernel32.dll",SetLastError=true)] public static extern bool SetConsoleCtrlHandler(Handler handler, bool add); [DllImport("kernel32.dll",SetLastError=true)] public static extern bool GenerateConsoleCtrlEvent(uint kind, uint group); }}'
[Signal]::FreeConsole() | Out-Null
if (-not [Signal]::AttachConsole({pid})) {{ exit 2 }}
if (-not [Signal]::SetConsoleCtrlHandler([Signal]::Ignore, $true)) {{ exit 3 }}
if (-not [Signal]::GenerateConsoleCtrlEvent(1, 0)) {{ exit 4 }}
exit 0
"#,
        pid = cli.child.id().context("CLI PID")?
    );
    let signal = Command::new("powershell.exe")
        .args(["-NoProfile", "-NonInteractive", "-Command", &script])
        .output()?;
    ensure!(
        signal.status.success(),
        "console signal helper {}: {}",
        signal.status,
        String::from_utf8_lossy(&signal.stderr)
    );
    cli.finish("cancelled", 130)
        .await
        .context("console interrupt terminal")?;
    // Then the signal handler finishes only after both owned processes are gone.
    closed(first).await?;
    closed(second).await?;
    assert!(!fixture.prepared().exists());
    Ok(())
}

async fn connection(
    listener: &tokio::net::TcpListener,
) -> Result<BufReader<tokio::net::TcpStream>> {
    let (socket, _) = tokio::time::timeout(DEADLINE, listener.accept())
        .await
        .context("waiting for fixture connection")??;
    let mut socket = BufReader::new(socket);
    let mut line = String::new();
    tokio::time::timeout(DEADLINE, socket.read_line(&mut line))
        .await
        .context("waiting for fixture identity")??;
    ensure!(!line.is_empty(), "fixture did not identify itself");
    Ok(socket)
}

async fn closed(mut socket: BufReader<tokio::net::TcpStream>) -> Result<()> {
    let mut byte = [0u8; 1];
    let result = tokio::time::timeout(DEADLINE, socket.read(&mut byte))
        .await
        .context("waiting for fixture process socket closure")?;
    assert!(
        matches!(result, Ok(0)) || result.is_err(),
        "fixture process still owns its socket"
    );
    Ok(())
}

#[test]
fn command_prepare_publishes_verified_output_and_only_json_events() -> Result<()> {
    // Given a command builder that writes non-JSON on both output streams.
    let fixture = Fixture::new()?;
    fixture.manifest(None, None)?;
    // When preparing through the executable with closed, uncontrolled stdin.
    let events = fixture.prepare()?;
    // Then stdout is exclusively the shared event protocol and output verifies.
    assert_eq!(events[0]["event"], "started");
    for mut event in events {
        let object = event.as_object_mut().context("event object")?;
        object.remove("v");
        object.remove("seq");
        serde_json::from_value::<PrepareEvent>(event)?;
    }
    PreparedDeployment::load_file(&fixture.prepared())?;
    assert_eq!(
        std::fs::read_to_string(fixture.root.path().join("invocations"))?,
        "build\n"
    );
    Ok(())
}

#[tokio::test]
async fn cancellation_reaps_command_and_descendant_before_terminal() -> Result<()> {
    // Given a builder and descendant blocked on externally owned sockets.
    let fixture = Fixture::new()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    fixture.manifest(Some(listener.local_addr()?.to_string()), None)?;
    let mut cli = Cli::spawn("prepare", &fixture)?;
    let first = connection(&listener).await?;
    let second = connection(&listener).await?;
    // When the parent sends the exact cancellation frame.
    cli.cancel(b"{\"v\":1,\"cmd\":\"cancel\"}\n").await?;
    cli.finish("cancelled", 130).await?;
    // Then both process sockets are closed and no output was published.
    closed(first).await?;
    closed(second).await?;
    assert!(!fixture.prepared().exists());
    Ok(())
}

#[tokio::test]
async fn controlled_eof_cancels_a_running_builder() -> Result<()> {
    // Given a running controlled preparation.
    let fixture = Fixture::new()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    fixture.manifest(Some(listener.local_addr()?.to_string()), None)?;
    let mut cli = Cli::spawn("prepare", &fixture)?;
    let first = connection(&listener).await?;
    let second = connection(&listener).await?;
    // When the controlling pipe closes without a cancel frame.
    drop(cli.child.stdin.take());
    cli.finish("cancelled", 130).await?;
    // Then EOF has the same cleanup and publication semantics.
    closed(first).await?;
    closed(second).await?;
    assert!(!fixture.prepared().exists());
    Ok(())
}

#[tokio::test]
async fn malformed_control_is_failure_and_reaps_the_builder_tree() -> Result<()> {
    // Given a running builder with a live descendant.
    let fixture = Fixture::new()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    fixture.manifest(Some(listener.local_addr()?.to_string()), None)?;
    let mut cli = Cli::spawn("prepare", &fixture)?;
    let first = connection(&listener).await?;
    let second = connection(&listener).await?;
    // When the protocol version is unsupported.
    cli.cancel(b"{\"v\":2,\"cmd\":\"cancel\"}\n").await?;
    cli.finish("failed", 1).await?;
    // Then failure does not leave a running builder or stale prepared file.
    closed(first).await?;
    closed(second).await?;
    assert!(!fixture.prepared().exists());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn sigint_cancels_and_reaps_the_running_builder_tree() -> Result<()> {
    // Given a running builder and descendant with observable lifetimes.
    let fixture = Fixture::new()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    fixture.manifest(Some(listener.local_addr()?.to_string()), None)?;
    let cli = Cli::spawn("prepare", &fixture)?;
    let first = connection(&listener).await?;
    let second = connection(&listener).await?;
    // When the actual OS interrupt signal reaches the CLI.
    let signal = Command::new("kill")
        .args(["-INT", &cli.child.id().context("CLI PID")?.to_string()])
        .status()?;
    ensure!(signal.success(), "sending SIGINT");
    cli.finish("cancelled", 130).await?;
    // Then signal cancellation has the same tree cleanup guarantee.
    closed(first).await?;
    closed(second).await?;
    assert!(!fixture.prepared().exists());
    Ok(())
}

#[test]
fn failed_command_cannot_publish_a_previous_output() -> Result<()> {
    // Given a previous mutable build output and a command that fails after starting.
    let fixture = Fixture::new()?;
    fixture.manifest(None, None)?;
    let manifest = fixture.root.path().join("main.yaml");
    let mut value: Value = serde_yaml::from_slice(&std::fs::read(&manifest)?)?;
    value["build"]["argv"]
        .as_array_mut()
        .context("argv")?
        .extend([json!("--fail"), json!("23")]);
    std::fs::write(manifest, serde_yaml::to_string(&value)?)?;
    std::fs::copy(
        &fixture.builder,
        fixture
            .root
            .path()
            .join(format!("app{}", std::env::consts::EXE_SUFFIX)),
    )?;
    // When the command exits nonzero.
    let output = Command::new(BIN)
        .arg("prepare")
        .arg(fixture.root.path())
        .arg("--out")
        .arg(fixture.prepared())
        .args(["--events", "jsonl"])
        .output()?;
    // Then preparation fails instead of freezing the stale target.
    assert_eq!(output.status.code(), Some(1));
    protocol(&output.stdout, "failed")?;
    assert!(!fixture.prepared().exists());
    Ok(())
}

#[test]
fn tampered_prepared_apply_fails_without_repair_or_owner_start() -> Result<()> {
    // Given a valid prepared envelope whose immutable executable is then altered.
    let fixture = Fixture::new()?;
    fixture.manifest(None, None)?;
    fixture.prepare()?;
    let prepared = PreparedDeployment::load_file(&fixture.prepared())?;
    std::fs::write(&prepared.nodes[0].executable, b"tampered")?;
    // When apply consumes the public file.
    let output = Command::new(BIN)
        .args(["apply", "--prepared"])
        .arg(fixture.prepared())
        .args(["--events", "jsonl", "--detach"])
        .output()?;
    // Then integrity failure is terminal and neither compilation nor owner startup occurs.
    assert_eq!(output.status.code(), Some(1));
    protocol(&output.stdout, "failed")?;
    assert_eq!(
        std::fs::read_to_string(fixture.root.path().join("invocations"))?,
        "build\n"
    );
    assert!(!fixture.root.path().join(".reiny/control.json").exists());
    Ok(())
}

#[test]
fn resolution_failure_and_existing_output_have_one_terminal() -> Result<()> {
    // Given an invalid deployment and an unrelated existing output.
    let root = tempfile::tempdir()?;
    let out = root.path().join("prepared.json");
    std::fs::write(&out, "preserve")?;
    // When either resolution or output validation fails.
    for output in [&out, &root.path().join("new.json")] {
        let result = Command::new(BIN)
            .arg("prepare")
            .arg(root.path())
            .arg("--out")
            .arg(output)
            .args(["--events", "jsonl"])
            .output()?;
        // Then framing remains complete and the preexisting output is untouched.
        assert_eq!(result.status.code(), Some(1));
        let events = protocol(&result.stdout, "failed")?;
        assert_eq!(events[0]["event"], "started");
    }
    assert_eq!(std::fs::read_to_string(out)?, "preserve");
    Ok(())
}

fn fabric() -> Result<zenoh::Session> {
    let mut config = zenoh::Config::default();
    config
        .insert_json5("listen/endpoints", r#"["tcp/127.0.0.1:0"]"#)
        .map_err(anyhow::Error::msg)?;
    config
        .insert_json5("scouting/multicast/enabled", "false")
        .map_err(anyhow::Error::msg)?;
    zenoh::open(config).wait().map_err(anyhow::Error::msg)
}

fn endpoint(session: &zenoh::Session) -> Result<String> {
    session
        .info()
        .locators()
        .wait()
        .first()
        .map(ToString::to_string)
        .context("Zenoh listener")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn prepared_apply_reaches_ready_without_builders_and_eof_reaps_owner() -> Result<()> {
    // Given immutable bytes and a separately controlled readiness declaration.
    let fixture = Fixture::new()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let session = fabric()?;
    fixture.manifest(
        None,
        Some((&listener.local_addr()?.to_string(), &endpoint(&session)?)),
    )?;
    fixture.prepare()?;
    std::fs::remove_file(&fixture.builder)?;
    std::fs::remove_file(
        fixture
            .root
            .path()
            .join(format!("app{}", std::env::consts::EXE_SUFFIX)),
    )?;
    let mut cli = Cli::spawn("apply", &fixture)?;
    let app = connection(&listener).await?;
    let ready = session
        .liveliness()
        .declare_token(fixture.ready_key())
        .wait()
        .map_err(anyhow::Error::msg)?;
    cli.event("ready").await?;
    // When the controlling parent closes stdin after readiness.
    drop(cli.child.stdin.take());
    cli.finish("cancelled", 130).await?;
    // Then no rebuild occurred and both app and owner are gone.
    closed(app).await?;
    drop(ready);
    assert!(!fixture.root.path().join(".reiny/control.json").exists());
    assert_eq!(
        std::fs::read_to_string(fixture.root.path().join("invocations"))?,
        "build\n"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn external_stop_completes_foreground_apply_without_owner_disappearance_failure() -> Result<()>
{
    // Given a ready foreground deployment and a separate authenticated client.
    let fixture = Fixture::new()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let session = fabric()?;
    fixture.manifest(
        None,
        Some((&listener.local_addr()?.to_string(), &endpoint(&session)?)),
    )?;
    fixture.prepare()?;
    let ready = session
        .liveliness()
        .declare_token(fixture.ready_key())
        .wait()
        .map_err(anyhow::Error::msg)?;
    let mut cli = Cli::spawn("apply", &fixture)?;
    let app = connection(&listener).await?;
    cli.event("ready").await?;
    let client = reiny_launch::DeploymentClient::find(fixture.root.path())?.context("owner")?;

    // When another client stops and reaps the deployment.
    client.stop()?;

    // Then the attached CLI observes a clean terminal, not a failed reconnection.
    cli.finish("stopped", 0).await?;
    closed(app).await?;
    drop(ready);
    assert!(!fixture.root.path().join(".reiny/control.json").exists());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn cancellation_during_readiness_stops_the_published_owner() -> Result<()> {
    // Given a published owner whose app has not declared readiness.
    let fixture = Fixture::new()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let session = fabric()?;
    fixture.manifest(
        None,
        Some((&listener.local_addr()?.to_string(), &endpoint(&session)?)),
    )?;
    fixture.prepare()?;
    let mut cli = Cli::spawn("apply", &fixture)?;
    let app = connection(&listener).await?;
    cli.event("owner_listening").await?;
    // When cancellation arrives while wait_ready is blocked.
    cli.cancel(b"{\"v\":1,\"cmd\":\"cancel\"}\n").await?;
    let events = cli.finish("cancelled", 130).await?;
    // Then no late Ready appears and cleanup precedes the terminal event.
    assert!(!events.iter().any(|event| event["event"] == "ready"));
    closed(app).await?;
    assert!(!fixture.root.path().join(".reiny/control.json").exists());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn cancellation_at_apply_start_cannot_leave_a_detached_owner() -> Result<()> {
    // Given a frozen deployment before bootstrap has been accepted.
    let fixture = Fixture::new()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let session = fabric()?;
    fixture.manifest(
        None,
        Some((&listener.local_addr()?.to_string(), &endpoint(&session)?)),
    )?;
    fixture.prepare()?;
    let mut cli = Cli::spawn("apply", &fixture)?;
    cli.event("apply_started").await?;
    // When cancellation races with owner creation and rendezvous.
    cli.cancel(b"{\"v\":1,\"cmd\":\"cancel\"}\n").await?;
    let events = cli.finish("cancelled", 130).await?;
    // Then no ready event or authenticated live owner survives the terminal.
    assert!(!events.iter().any(|event| event["event"] == "ready"));
    assert!(reiny_launch::DeploymentClient::find(fixture.root.path())?.is_none());
    let prepared_dir = fixture.root.path().join(".reiny/prepared");
    assert!(!prepared_dir.exists() || std::fs::read_dir(prepared_dir)?.next().is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn malformed_apply_control_stops_a_ready_deployment() -> Result<()> {
    // Given a ready deployment owned by the foreground CLI.
    let fixture = Fixture::new()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let session = fabric()?;
    fixture.manifest(
        None,
        Some((&listener.local_addr()?.to_string(), &endpoint(&session)?)),
    )?;
    fixture.prepare()?;
    let mut cli = Cli::spawn("apply", &fixture)?;
    let app = connection(&listener).await?;
    let ready = session
        .liveliness()
        .declare_token(fixture.ready_key())
        .wait()
        .map_err(anyhow::Error::msg)?;
    cli.event("ready").await?;
    // When a control message contains an unknown field.
    cli.cancel(b"{\"v\":1,\"cmd\":\"cancel\",\"extra\":true}\n")
        .await?;
    cli.finish("failed", 1).await?;
    // Then the protocol failure still performs complete owned cleanup.
    closed(app).await?;
    drop(ready);
    assert!(!fixture.root.path().join(".reiny/control.json").exists());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn detached_prepared_apply_survives_rejected_controlled_adoption() -> Result<()> {
    // Given a prepared deployment whose owner is explicitly detached.
    let fixture = Fixture::new()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let session = fabric()?;
    fixture.manifest(
        None,
        Some((&listener.local_addr()?.to_string(), &endpoint(&session)?)),
    )?;
    fixture.prepare()?;
    let ready = session
        .liveliness()
        .declare_token(fixture.ready_key())
        .wait()
        .map_err(anyhow::Error::msg)?;
    let detached = Cli::spawn("apply-detached", &fixture)?;
    let app = connection(&listener).await?;
    detached.finish("stopped", 0).await?;
    let client =
        reiny_launch::DeploymentClient::find(fixture.root.path())?.context("detached owner")?;
    // When another controlled request tries to adopt that independently owned deployment.
    let rejected = Cli::spawn("apply", &fixture)?.finish("failed", 1).await;
    let still_alive = client.status();
    let stopped = client.stop();
    // Then rejection leaves the owner untouched; only its authenticated stop ends it.
    rejected?;
    assert!(still_alive?.owner_alive);
    stopped?;
    closed(app).await?;
    drop(ready);
    assert_eq!(
        std::fs::read_to_string(fixture.root.path().join("invocations"))?,
        "build\n"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn forced_apply_death_revokes_the_authenticated_parent_lease() -> Result<()> {
    // Given a foreground owner and a registered stop subscription.
    let fixture = Fixture::new()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let session = fabric()?;
    fixture.manifest(
        None,
        Some((&listener.local_addr()?.to_string(), &endpoint(&session)?)),
    )?;
    fixture.prepare()?;
    let mut cli = Cli::spawn("apply", &fixture)?;
    let app = connection(&listener).await?;
    let ready = session
        .liveliness()
        .declare_token(fixture.ready_key())
        .wait()
        .map_err(anyhow::Error::msg)?;
    cli.event("ready").await?;
    let client = reiny_launch::DeploymentClient::find(fixture.root.path())?.context("owner")?;
    let stopped = client.watch_stopped()?;
    // When the foreground CLI is forcibly terminated, without a control frame.
    cli.child.kill().await?;
    // Then Unix lease EOF or the Windows owning job cleans the app.
    closed(app).await?;
    let waiting = tokio::task::spawn_blocking(move || stopped.wait());
    let status = tokio::time::timeout(DEADLINE, waiting).await??;
    #[cfg(unix)]
    assert_eq!(status?.phase, reiny_launch::DeploymentPhase::Stopped);
    #[cfg(windows)]
    if let Ok(status) = status {
        assert_eq!(status.phase, reiny_launch::DeploymentPhase::Stopped);
    }
    drop(ready);
    Ok(())
}

#[test]
fn cargo_prepare_reports_real_artifacts_and_reuses_its_cache() -> Result<()> {
    // Given a tiny Cargo package and an explicit persistent cache.
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("src"))?;
    std::fs::write(
        root.path().join("Cargo.toml"),
        "[workspace]\n[package]\nname='cli-cargo'\nversion='0.0.0'\nedition='2024'\n",
    )?;
    std::fs::write(
        root.path().join("src/main.rs"),
        "fn main() { println!(\"compiled\"); }\n",
    )?;
    std::fs::write(
        root.path().join("main.yaml"),
        "version: 3\ndeployment: cli\nproviders: {process: {type: process}}\nrun: {provider: process, bin: cli-cargo}\nbuild: {type: cargo, profile: dev, locked: false}\n",
    )?;
    let cache = root.path().join("cache");
    // When the same declaration is prepared twice into distinct public files.
    let mut compiled = BTreeMap::new();
    for index in 0..2 {
        let out = root.path().join(format!("prepared-{index}.json"));
        let result = Command::new(BIN)
            .arg("prepare")
            .arg(root.path())
            .arg("--out")
            .arg(&out)
            .arg("--build-cache")
            .arg(&cache)
            .args(["--events", "jsonl", "--jobs", "2"])
            .output()?;
        ensure!(
            result.status.success(),
            "Cargo prepare: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        compiled.insert(index, protocol(&result.stdout, "success")?);
        PreparedDeployment::load_file(&out)?;
    }
    // Then Cargo's actual artifact freshness changes, not an invented cache hit.
    assert!(
        compiled[&0]
            .iter()
            .any(|event| event["event"] == "compiled" && event["fresh"] == false)
    );
    assert!(
        compiled[&1]
            .iter()
            .any(|event| event["event"] == "compiled" && event["fresh"] == true)
    );
    Ok(())
}

#[test]
fn prepared_apply_rejects_build_flags_and_invalid_control_modes() -> Result<()> {
    // Given the mutually exclusive prepared/source and detached/controlled modes.
    // When callers combine incompatible flags.
    for args in [
        vec!["apply", "--prepared", "p.json", "source"],
        vec!["apply", "--prepared", "p.json", "--release"],
        vec!["apply", "--prepared", "p.json", "--jobs", "2"],
        vec!["apply", "--prepared", "p.json", "--bin-dir", "bin"],
        vec![
            "apply",
            "--prepared",
            "p.json",
            "--detach",
            "--control-stdin",
        ],
        vec!["prepare", ".", "--out", "p.json", "--jobs", "0"],
    ] {
        let output = Command::new(BIN).args(args).output()?;
        // Then clap rejects usage before any build or owner startup.
        assert_eq!(output.status.code(), Some(2));
        assert_eq!(output.stdout, Vec::<u8>::new());
    }
    Ok(())
}
