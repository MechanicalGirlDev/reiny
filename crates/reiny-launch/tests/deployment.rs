//! Process and authenticated-control integration tests; no bus engine is linked.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use reiny_core::bindings::{
    CONTRACT_VERSION, EndpointContract, InputBinding, ModuleBindings, ModuleReport, OutputBinding,
    PortKind, PortReport, QosProfile, ResponseReport,
};
use reiny_launch::{
    DeploymentClient, DeploymentPhase, DeploymentStatus, FailurePolicy, ModuleObserver,
    PreparedDeployment, PreparedModule, ProviderKind, ProviderSpec, RestartPolicy, RunKind,
    RunSpec, last_status, serve,
};

const DEADLINE: Duration = Duration::from_secs(15);

// The fixture actually parses the supplied bindings path, writes its own report,
// announces readiness over a separate observer transport, and obeys stop/exit.
const FIXTURE: &str = r#"
use std::{env,fs,io::{Read,Write},net::TcpStream};
fn main() {
    let args: Vec<_> = env::args().collect();
    if args.get(1).map(String::as_str) == Some("--descendant") {
        let mut socket = TcpStream::connect(&args[2]).unwrap();
        writeln!(socket,"{} {}", args[3], std::process::id()).unwrap();
        let mut byte = [0];
        while socket.read_exact(&mut byte).is_ok() {}
        return;
    }
    let value = |key: &str| args.windows(2).find(|pair| pair[0] == key).map(|pair| pair[1].clone()).unwrap();
    assert_eq!(env::var("REINY_BUNDLE_DIR").unwrap(), value("--fixture-bundle"));
    let config = value("--fixture-config");
    if config.is_empty() {
        assert!(env::var_os("REINY_CONFIG_DIR").is_none());
    } else {
        assert_eq!(env::var("REINY_CONFIG_DIR").unwrap(), config);
    }
    let namespace = value("--name");
    let bindings = fs::read_to_string(value("--module-bindings")).unwrap();
    assert!(bindings.contains(&namespace));
    let mode = value("--fixture-mode");
    let report = value("--fixture-report");
    if mode != "no-report" && mode != "no-report-task" {
        let path = env::var("REINY_MODULE_REPORT").unwrap();
        let temporary = format!("{path}.tmp");
        fs::write(&temporary, report).unwrap();
        fs::rename(temporary, path).unwrap();
    }
    if mode == "tree" {
        std::process::Command::new(env::current_exe().unwrap())
            .args(["--descendant", &value("--fixture-address"), &format!("{namespace}/descendant")])
            .spawn().unwrap();
    }
    let mut socket = TcpStream::connect(value("--fixture-address")).unwrap();
    writeln!(socket,"{} {}", namespace, std::process::id()).unwrap();
    if mode == "no-report-task" { return; }
    loop {
        let mut byte = [0];
        if socket.read_exact(&mut byte).is_err() { return; }
        match byte[0] {
            b's' if mode != "ignore-stop" => return,
            b'x' => std::process::exit(7),
            b'q' => return,
            _ => {}
        }
    }
}
"#;

struct FixtureBinary {
    directory: tempfile::TempDir,
    executable: PathBuf,
}

fn fixture_binary() -> Result<FixtureBinary> {
    // Rustc is independent of Cargo's workspace target lock.
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("fixture.rs");
    let executable = directory
        .path()
        .join(format!("fixture{}", std::env::consts::EXE_SUFFIX));
    std::fs::write(&source, FIXTURE)?;
    let output = Command::new("rustc")
        .arg("--edition=2024")
        .arg(&source)
        .arg("-o")
        .arg(&executable)
        .output()?;
    anyhow::ensure!(
        output.status.success(),
        "fixture rustc failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(FixtureBinary {
        directory,
        executable,
    })
}

#[derive(Default)]
struct Observation {
    sockets: BTreeMap<String, TcpStream>,
    pids: BTreeMap<String, u32>,
    fail: bool,
    configured: BTreeSet<String>,
    stop_requests: BTreeSet<String>,
    root: Option<PathBuf>,
    stop_snapshots: BTreeMap<String, DeploymentStatus>,
}

struct Observer {
    state: Mutex<Observation>,
    changed: Condvar,
}

impl Observer {
    fn socket(&self, namespace: &str) -> Result<TcpStream> {
        let state = self
            .state
            .lock()
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        Ok(state
            .sockets
            .get(namespace)
            .context("fixture not connected")?
            .try_clone()?)
    }

    fn send(&self, namespace: &str, command: u8) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        state
            .sockets
            .get_mut(namespace)
            .context("fixture not connected")?
            .write_all(&[command])?;
        Ok(())
    }

    fn wait_pid(&self, namespace: &str, different: Option<u32>) -> Result<u32> {
        let state = self
            .state
            .lock()
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        let (state, timeout) = self
            .changed
            .wait_timeout_while(state, DEADLINE, |state| {
                state
                    .pids
                    .get(namespace)
                    .is_none_or(|pid| Some(*pid) == different)
            })
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        anyhow::ensure!(!timeout.timed_out(), "fixture connection timed out");
        state
            .pids
            .get(namespace)
            .copied()
            .context("missing fixture pid")
    }
}

impl ModuleObserver for Observer {
    fn configure(&self, prepared: &PreparedDeployment) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        state.root = Some(prepared.root.clone());
        state.configured = prepared
            .nodes
            .iter()
            .map(|node| node.namespace.clone())
            .collect();
        Ok(())
    }

    fn ready(&self, namespace: &str) -> Result<bool> {
        let state = self
            .state
            .lock()
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        anyhow::ensure!(!state.fail, "injected observer transport failure");
        anyhow::ensure!(
            state.configured.contains(namespace),
            "namespace was not configured"
        );
        Ok(state.sockets.contains_key(namespace))
    }

    fn request_stop(&self, namespace: &str) -> Result<()> {
        {
            let mut state = self
                .state
                .lock()
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            anyhow::ensure!(
                state.configured.contains(namespace),
                "stop namespace was not configured"
            );
            state.stop_requests.insert(namespace.to_owned());
            let root = state.root.as_ref().context("observer root missing")?;
            let status = last_status(root)?.context("stopping snapshot missing")?;
            state.stop_snapshots.insert(namespace.to_owned(), status);
        }
        self.send(namespace, b's')
    }
}

struct Harness {
    binary: FixtureBinary,
    directory: tempfile::TempDir,
    observer: Arc<Observer>,
    address: std::net::SocketAddr,
    listener: Option<JoinHandle<Result<()>>>,
    owner: Option<JoinHandle<Result<()>>>,
    stop: Arc<AtomicBool>,
    listener_stop: Arc<AtomicBool>,
    client: Option<DeploymentClient>,
}

impl Harness {
    fn new() -> Result<Self> {
        let binary = fixture_binary()?;
        let directory = tempfile::tempdir()?;
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let address = listener.local_addr()?;
        let observer = Arc::new(Observer {
            state: Mutex::new(Observation::default()),
            changed: Condvar::new(),
        });
        let shared = Arc::clone(&observer);
        let listener_stop = Arc::new(AtomicBool::new(false));
        let should_stop = Arc::clone(&listener_stop);
        let listener = std::thread::spawn(move || -> Result<()> {
            while !should_stop.load(Ordering::Acquire) {
                let (stream, _) = listener.accept().context("fixture accept")?;
                if should_stop.load(Ordering::Acquire) {
                    break;
                }
                stream.set_read_timeout(Some(DEADLINE))?;
                stream.set_write_timeout(Some(DEADLINE))?;
                let mut reader = BufReader::new(stream);
                let mut header = String::new();
                reader.read_line(&mut header).context("fixture header")?;
                let (namespace, pid) = header
                    .trim()
                    .split_once(' ')
                    .context("fixture header fields")?;
                let pid = pid.parse().context("fixture PID")?;
                let mut state = shared
                    .state
                    .lock()
                    .map_err(|error| anyhow::anyhow!("{error}"))?;
                state.pids.insert(namespace.to_owned(), pid);
                state
                    .sockets
                    .insert(namespace.to_owned(), reader.into_inner());
                shared.changed.notify_all();
            }
            Ok(())
        });
        Ok(Self {
            binary,
            directory,
            observer,
            address,
            listener: Some(listener),
            owner: None,
            stop: Arc::new(AtomicBool::new(false)),
            listener_stop,
            client: None,
        })
    }

    fn prepared(&self, count: usize) -> Result<PreparedDeployment> {
        let root = std::fs::canonicalize(self.directory.path())?;
        let deployment = format!(
            "test-{}",
            self.directory
                .path()
                .file_name()
                .context("temp name")?
                .to_string_lossy()
        );
        let mut nodes = Vec::new();
        for index in 0..count {
            let namespace = format!("{deployment}/node{index}");
            let bindings = ModuleBindings {
                version: CONTRACT_VERSION,
                namespace: namespace.clone(),
                endpoint_namespace: namespace.clone(),
                inputs: BTreeMap::new(),
                outputs: BTreeMap::new(),
                children: BTreeMap::new(),
                executables: vec!["fixture".into()],
            };
            let report = ModuleReport {
                version: CONTRACT_VERSION,
                namespace: namespace.clone(),
                inputs: BTreeMap::new(),
                outputs: BTreeMap::new(),
            };
            nodes.push(PreparedModule {
                namespace,
                module_dir: root.clone(),
                executable: self.binary.executable.clone(),
                bundle_dir: self.binary.directory.path().to_path_buf(),
                config_dir: None,
                config_bundle_dir: None,
                run: RunSpec {
                    kind: RunKind::Service,
                    provider: "process".into(),
                    bin: "fixture".into(),
                    config: None,
                    args: vec![
                        "--fixture-address".into(),
                        self.address.to_string(),
                        "--fixture-mode".into(),
                        "normal".into(),
                        "--fixture-report".into(),
                        serde_json::to_string(&report)?,
                        "--fixture-bundle".into(),
                        self.binary.directory.path().display().to_string(),
                        "--fixture-config".into(),
                        String::new(),
                    ],
                    companions: Vec::new(),
                    config_assets: Vec::new(),
                    restart: RestartPolicy::Manual,
                    on_failure: FailurePolicy::Report,
                },
                provider: ProviderSpec {
                    kind: ProviderKind::Process,
                    bin_dir: root.clone(),
                    zenoh_config: None,
                    connect: Vec::new(),
                },
                bindings,
                fingerprint: format!("initial-{index}"),
            });
        }
        Ok(PreparedDeployment {
            root,
            deployment,
            domain: "deployment-tests".into(),
            nodes,
            resources: BTreeMap::new(),
        })
    }

    fn start(&mut self, prepared: PreparedDeployment) -> Result<DeploymentClient> {
        let root = prepared.root.clone();
        let observer = Arc::clone(&self.observer);
        let stop = Arc::clone(&self.stop);
        let (send, receive) = mpsc::sync_channel(1);
        self.owner = Some(std::thread::spawn(move || {
            serve(prepared, observer.as_ref(), &stop, &|| {
                send.send(()).context("bootstrap receiver disappeared")
            })
        }));
        receive.recv_timeout(DEADLINE).context("owner bootstrap")?;
        let client = DeploymentClient::find(&root)?.context("owner not found after bootstrap")?;
        self.client = Some(client.clone());
        Ok(client)
    }

    fn join_owner(&mut self) -> Result<()> {
        self.owner
            .take()
            .context("owner not running")?
            .join()
            .map_err(|_| anyhow::anyhow!("owner panicked"))?
    }
}

#[test]
fn compiled_fixture_directory_is_removed_with_its_last_owner() -> Result<()> {
    let harness = Harness::new()?;
    let directory = harness.binary.directory.path().to_path_buf();
    assert!(directory.join("fixture.rs").is_file());
    drop(harness);
    assert!(!directory.exists());
    Ok(())
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(owner) = self.owner.take() {
            match owner.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => eprintln!("owner teardown: {error:#}"),
                Err(error) => eprintln!("owner teardown panic: {error:?}"),
            }
        }
        self.listener_stop.store(true, Ordering::Release);
        match TcpStream::connect(self.address) {
            Ok(stream) => drop(stream),
            Err(error) => eprintln!("listener teardown: {error}"),
        }
        if let Some(listener) = self.listener.take() {
            match listener.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => eprintln!("listener teardown: {error:#}"),
                Err(error) => eprintln!("listener teardown panic: {error:?}"),
            }
        }
    }
}

fn wait_phase(client: &DeploymentClient, phase: DeploymentPhase) -> Result<DeploymentStatus> {
    let deadline = Instant::now() + DEADLINE;
    let mut status = client.status()?;
    while status.phase != phase {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .context("state event deadline")?;
        status = client.wait_for_revision(status.revision, remaining)?;
    }
    Ok(status)
}

#[test]
fn duplicate_apply_keeps_actual_pid_and_generation() -> Result<()> {
    // Given an actually running, contract-ready fixture.
    let mut harness = Harness::new()?;
    let prepared = harness.prepared(1)?;
    let client = harness.start(prepared.clone())?;
    let before = client.wait_ready(DEADLINE)?;
    // When the identical desired state is applied again.
    let after = client.apply(&prepared)?;
    // Then the same owned generation remains running.
    assert_eq!(after.modules, before.modules);
    assert_eq!(after.owner_generation, before.owner_generation);
    assert_eq!(
        harness
            .observer
            .wait_pid(&prepared.nodes[0].namespace, None)?,
        before.modules[&prepared.nodes[0].namespace]
            .pid
            .context("actual pid")?
    );
    Ok(())
}

#[test]
fn update_replaces_only_changed_and_removed_modules() -> Result<()> {
    // Given three live modules.
    let mut harness = Harness::new()?;
    let mut prepared = harness.prepared(3)?;
    let client = harness.start(prepared.clone())?;
    let before = client.wait_ready(DEADLINE)?;
    let unchanged = prepared.nodes[0].namespace.clone();
    let changed = prepared.nodes[1].namespace.clone();
    let removed = prepared.nodes[2].namespace.clone();
    prepared.nodes.pop();
    prepared.nodes[1].fingerprint = "changed".into();
    let added = harness.prepared(4)?.nodes.pop().context("added node")?;
    let added_namespace = added.namespace.clone();
    prepared.nodes.push(added);
    // When a delta changes one node and removes another.
    client.apply(&prepared)?;
    let after = client.wait_ready(DEADLINE)?;
    // Then only the changed module has a fresh owned process generation.
    assert_eq!(after.modules[&unchanged], before.modules[&unchanged]);
    assert_ne!(
        after.modules[&changed].generation,
        before.modules[&changed].generation
    );
    assert_ne!(after.modules[&changed].pid, before.modules[&changed].pid);
    assert!(!after.modules.contains_key(&removed));
    assert!(after.modules[&added_namespace].pid.is_some());
    let observation = harness
        .observer
        .state
        .lock()
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    assert!(observation.stop_requests.contains(&changed));
    assert!(observation.stop_requests.contains(&removed));
    Ok(())
}

#[test]
fn suspension_stays_latched_until_explicit_apply() -> Result<()> {
    // Given a failure-suspending deployment with an always-restart module.
    let mut harness = Harness::new()?;
    let mut prepared = harness.prepared(2)?;
    prepared.nodes[0].run.restart = RestartPolicy::Always;
    prepared.nodes[0].run.on_failure = FailurePolicy::SuspendDeployment;
    let namespace = prepared.nodes[0].namespace.clone();
    let client = harness.start(prepared.clone())?;
    let before = client.wait_ready(DEADLINE)?;
    // When the child exits unsuccessfully.
    harness.observer.send(&namespace, b'x')?;
    let suspended = wait_phase(&client, DeploymentPhase::Suspended)?;
    // Then it remains suspended and all children are reaped despite restart policy.
    assert!(
        suspended
            .modules
            .values()
            .all(|module| module.pid.is_none())
    );
    assert!(client.wait_ready(DEADLINE).is_err());
    assert_eq!(client.status()?.phase, DeploymentPhase::Suspended);
    client.apply(&prepared)?;
    let resumed = client.wait_ready(DEADLINE)?;
    assert!(resumed.modules[&namespace].generation > before.modules[&namespace].generation);
    Ok(())
}

#[test]
fn manual_exit_stays_down_until_apply() -> Result<()> {
    // Given a ready manual-restart module.
    let mut harness = Harness::new()?;
    let prepared = harness.prepared(2)?;
    let namespace = prepared.nodes[0].namespace.clone();
    let client = harness.start(prepared.clone())?;
    let before = client.wait_ready(DEADLINE)?;
    // When the child fails.
    harness.observer.send(&namespace, b'x')?;
    let degraded = wait_phase(&client, DeploymentPhase::Degraded)?;
    // Then the desired module has no actual PID, and explicit apply restarts it.
    assert_eq!(degraded.modules[&namespace].pid, None);
    client.apply(&prepared)?;
    assert!(
        client.wait_ready(DEADLINE)?.modules[&namespace].generation
            > before.modules[&namespace].generation
    );
    Ok(())
}

#[test]
fn on_failure_restarts_the_failed_owned_generation() -> Result<()> {
    // Given a ready on-failure module.
    let mut harness = Harness::new()?;
    let mut prepared = harness.prepared(1)?;
    prepared.nodes[0].run.restart = RestartPolicy::OnFailure;
    let namespace = prepared.nodes[0].namespace.clone();
    let client = harness.start(prepared)?;
    let before = client.wait_ready(DEADLINE)?;
    let pid = before.modules[&namespace].pid.context("pid")?;
    // When its process exits unsuccessfully.
    harness.observer.send(&namespace, b'x')?;
    harness.observer.wait_pid(&namespace, Some(pid))?;
    // Then the supervisor owns a fresh, ready generation.
    let after = client.wait_ready(DEADLINE)?;
    assert!(after.modules[&namespace].generation > before.modules[&namespace].generation);
    assert_ne!(after.modules[&namespace].pid, Some(pid));
    Ok(())
}

#[test]
fn always_restarts_after_a_successful_unrequested_exit() -> Result<()> {
    // Given a ready always-restart module.
    let mut harness = Harness::new()?;
    let mut prepared = harness.prepared(1)?;
    prepared.nodes[0].run.restart = RestartPolicy::Always;
    let namespace = prepared.nodes[0].namespace.clone();
    let client = harness.start(prepared)?;
    let before = client.wait_ready(DEADLINE)?;
    let pid = before.modules[&namespace].pid.context("pid")?;
    // When its process exits successfully without an owner stop request.
    harness.observer.send(&namespace, b'q')?;
    harness.observer.wait_pid(&namespace, Some(pid))?;
    // Then the owner starts another actual process despite the successful exit.
    let after = client.wait_ready(DEADLINE)?;
    assert!(after.modules[&namespace].generation > before.modules[&namespace].generation);
    assert_ne!(after.modules[&namespace].pid, Some(pid));
    Ok(())
}

#[test]
fn successful_service_exit_is_a_failure_and_on_failure_restarts() -> Result<()> {
    for restart in [RestartPolicy::Manual, RestartPolicy::OnFailure] {
        let mut harness = Harness::new()?;
        let mut prepared = harness.prepared(1)?;
        prepared.nodes[0].run.restart = restart;
        let namespace = prepared.nodes[0].namespace.clone();
        let client = harness.start(prepared)?;
        let before = client.wait_ready(DEADLINE)?;
        harness.observer.send(&namespace, b'q')?;
        match restart {
            RestartPolicy::Manual => {
                let failed = wait_phase(&client, DeploymentPhase::Failed)?;
                assert_eq!(failed.modules[&namespace].phase, DeploymentPhase::Failed);
                assert!(failed.modules[&namespace].pid.is_none());
                assert!(failed.modules[&namespace].error.is_some());
            }
            RestartPolicy::OnFailure => {
                harness
                    .observer
                    .wait_pid(&namespace, before.modules[&namespace].pid)?;
                let restarted = client.wait_ready(DEADLINE)?;
                assert!(
                    restarted.modules[&namespace].generation
                        > before.modules[&namespace].generation
                );
            }
            RestartPolicy::Always => unreachable!(),
        }
    }
    Ok(())
}

#[test]
fn successful_task_completes_without_restart_or_suspension() -> Result<()> {
    let mut harness = Harness::new()?;
    let mut prepared = harness.prepared(1)?;
    prepared.nodes[0].run.kind = RunKind::Task;
    prepared.nodes[0].run.restart = RestartPolicy::Always;
    prepared.nodes[0].run.on_failure = FailurePolicy::SuspendDeployment;
    prepared.nodes[0].run.args[3] = "tree".into();
    let namespace = prepared.nodes[0].namespace.clone();
    let descendant = format!("{namespace}/descendant");
    let client = harness.start(prepared.clone())?;
    let before = client.wait_ready(DEADLINE)?;
    harness.observer.wait_pid(&descendant, None)?;
    let mut connection = harness.observer.socket(&descendant)?;
    harness.observer.send(&namespace, b'q')?;
    let completed = wait_phase(&client, DeploymentPhase::Completed)?;
    assert_eq!(
        completed.modules[&namespace].phase,
        DeploymentPhase::Completed
    );
    assert_eq!(completed.modules[&namespace].pid, None);
    assert_eq!(completed.modules[&namespace].actual_fingerprint, None);
    assert_eq!(completed.modules[&namespace].error, None);
    assert_disconnected(&mut connection)?;
    assert_eq!(
        client.wait_ready(DEADLINE)?.phase,
        DeploymentPhase::Completed
    );
    assert_eq!(client.apply(&prepared)?.modules, completed.modules);
    assert_eq!(
        completed.modules[&namespace].generation,
        before.modules[&namespace].generation
    );
    assert_eq!(serde_json::to_value(&completed)?["phase"], "completed");
    prepared.nodes[0].fingerprint = "changed-task".into();
    client.apply(&prepared)?;
    assert!(
        client.wait_ready(DEADLINE)?.modules[&namespace].generation
            > completed.modules[&namespace].generation
    );
    Ok(())
}

#[test]
fn unsuccessful_task_fails_and_completed_task_keeps_services_ready() -> Result<()> {
    for command in *b"xq" {
        let mut harness = Harness::new()?;
        let mut prepared = harness.prepared(2)?;
        prepared.nodes[0].run.kind = RunKind::Task;
        let namespace = prepared.nodes[0].namespace.clone();
        let client = harness.start(prepared)?;
        let before = client.wait_ready(DEADLINE)?;
        harness.observer.send(&namespace, command)?;
        let deadline = Instant::now() + DEADLINE;
        let mut status = before;
        while status.modules[&namespace].pid.is_some() {
            status = client.wait_for_revision(
                status.revision,
                deadline
                    .checked_duration_since(Instant::now())
                    .context("task exit event")?,
            )?;
        }
        if command == b'q' {
            assert_eq!(status.phase, DeploymentPhase::Ready);
            assert_eq!(status.modules[&namespace].phase, DeploymentPhase::Completed);
        } else {
            assert_eq!(status.phase, DeploymentPhase::Degraded);
            assert_eq!(status.modules[&namespace].phase, DeploymentPhase::Failed);
        }
    }
    Ok(())
}

#[test]
fn finite_task_can_complete_before_readiness() -> Result<()> {
    let mut harness = Harness::new()?;
    let mut prepared = harness.prepared(1)?;
    prepared.nodes[0].run.kind = RunKind::Task;
    prepared.nodes[0].run.args[3] = "no-report-task".into();
    let namespace = prepared.nodes[0].namespace.clone();
    let client = harness.start(prepared)?;
    let completed = client.wait_ready(DEADLINE)?;
    assert_eq!(completed.phase, DeploymentPhase::Completed);
    assert_eq!(completed.modules[&namespace].report, None);
    assert_eq!(completed.modules[&namespace].pid, None);
    Ok(())
}

fn terminal_notification(
    client: &DeploymentClient,
    trigger: impl FnOnce() -> Result<()>,
) -> Result<DeploymentStatus> {
    let subscription = client.watch_terminal()?;
    let (send, receive) = mpsc::sync_channel(1);
    let waiting = std::thread::spawn(move || {
        send.send(subscription.wait())
            .map_err(|_| anyhow::anyhow!("terminal subscription receiver disappeared"))
    });
    trigger()?;
    let status = receive
        .recv_timeout(DEADLINE)
        .context("terminal state event")??;
    waiting
        .join()
        .map_err(|_| anyhow::anyhow!("terminal subscription panic"))??;
    Ok(status)
}

#[test]
fn terminal_subscription_notifies_task_completion_without_stopping_owner() -> Result<()> {
    let mut harness = Harness::new()?;
    let mut prepared = harness.prepared(1)?;
    prepared.nodes[0].run.kind = RunKind::Task;
    prepared.nodes[0].run.args[3] = "tree".into();
    let namespace = prepared.nodes[0].namespace.clone();
    let descendant = format!("{namespace}/descendant");
    let client = harness.start(prepared.clone())?;
    assert_eq!(client.wait_ready(DEADLINE)?.phase, DeploymentPhase::Ready);
    harness.observer.wait_pid(&descendant, None)?;
    let mut descendant_connection = harness.observer.socket(&descendant)?;
    let stop_subscription = client.watch_stopped()?;
    let completed = terminal_notification(&client, || harness.observer.send(&namespace, b'q'))?;
    assert_eq!(completed.phase, DeploymentPhase::Completed);
    assert!(completed.owner_alive);
    assert!(
        completed
            .modules
            .values()
            .all(|module| module.pid.is_none())
    );
    assert_disconnected(&mut descendant_connection)?;
    assert_eq!(client.status()?.phase, DeploymentPhase::Completed);
    assert_eq!(terminal_notification(&client, || Ok(()))?, completed);
    prepared.nodes[0].fingerprint = "next-task-generation".into();
    client.apply(&prepared)?;
    let restarted = client.wait_ready(DEADLINE)?;
    assert!(restarted.modules[&namespace].generation > completed.modules[&namespace].generation);
    assert_eq!(client.stop()?.phase, DeploymentPhase::Stopped);
    assert_eq!(stop_subscription.wait()?.phase, DeploymentPhase::Stopped);
    Ok(())
}

#[test]
fn terminal_subscription_registered_after_completion_returns_immediately() -> Result<()> {
    let mut harness = Harness::new()?;
    let mut prepared = harness.prepared(1)?;
    prepared.nodes[0].run.kind = RunKind::Task;
    prepared.nodes[0].run.args[3] = "no-report-task".into();
    let client = harness.start(prepared)?;
    assert_eq!(
        client.wait_ready(DEADLINE)?.phase,
        DeploymentPhase::Completed
    );
    let completed = terminal_notification(&client, || Ok(()))?;
    assert_eq!(completed.phase, DeploymentPhase::Completed);
    assert!(completed.owner_alive);
    Ok(())
}

fn connected_fixture(
    harness: &Harness,
    publishers: usize,
    rpc: bool,
) -> Result<PreparedDeployment> {
    let mut prepared = harness.prepared(publishers + 1)?;
    let contract = EndpointContract {
        type_name: "Ping".into(),
        kind: if rpc { PortKind::Rpc } else { PortKind::Stream },
        response: rpc.then(|| "Pong".into()),
        ..EndpointContract::default()
    };
    let sources = prepared.nodes[..publishers]
        .iter()
        .map(|node| node.namespace.clone())
        .collect();
    for (index, node) in prepared.nodes.iter_mut().enumerate() {
        let port = PortReport {
            type_name: "test.Ping".into(),
            schema: 42,
            contract: contract.clone(),
            response: rpc.then(|| ResponseReport {
                type_name: "test.Pong".into(),
                schema: 84,
            }),
        };
        let mut report = ModuleReport {
            version: CONTRACT_VERSION,
            namespace: node.namespace.clone(),
            inputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
        };
        if index == publishers {
            node.bindings.inputs.insert(
                "in".into(),
                InputBinding {
                    contract: contract.clone(),
                    sources,
                },
            );
            report.inputs.insert("in".into(), port);
            node.run.args[5] = serde_json::to_string(&report)?;
            break;
        }
        node.bindings.outputs.insert(
            "out".into(),
            OutputBinding {
                contract: contract.clone(),
            },
        );
        report.outputs.insert("out".into(), port);
        node.run.args[5] = serde_json::to_string(&report)?;
    }
    Ok(prepared)
}

#[test]
fn every_fan_in_source_must_report_the_same_schema() -> Result<()> {
    for mismatch in [false, true] {
        let mut harness = Harness::new()?;
        let mut prepared = connected_fixture(&harness, 2, false)?;
        if mismatch {
            let mut report: ModuleReport = serde_json::from_str(&prepared.nodes[1].run.args[5])?;
            report.outputs.get_mut("out").context("output")?.schema += 1;
            prepared.nodes[1].run.args[5] = serde_json::to_string(&report)?;
        }
        let client = harness.start(prepared)?;
        if mismatch {
            assert!(client.wait_ready(DEADLINE).is_err());
            assert_eq!(client.status()?.phase, DeploymentPhase::Failed);
        } else {
            assert_eq!(client.wait_ready(DEADLINE)?.phase, DeploymentPhase::Ready);
        }
    }
    Ok(())
}

#[test]
fn zero_request_and_response_fingerprints_are_valid() -> Result<()> {
    for rpc in [false, true] {
        let mut harness = Harness::new()?;
        let mut prepared = connected_fixture(&harness, 1, rpc)?;
        for node in &mut prepared.nodes {
            let mut report: ModuleReport = serde_json::from_str(&node.run.args[5])?;
            for port in report
                .inputs
                .values_mut()
                .chain(report.outputs.values_mut())
            {
                port.schema = 0;
                if let Some(response) = &mut port.response {
                    response.schema = 0;
                }
            }
            node.run.args[5] = serde_json::to_string(&report)?;
        }
        let client = harness.start(prepared)?;
        assert_eq!(client.wait_ready(DEADLINE)?.phase, DeploymentPhase::Ready);
    }
    Ok(())
}

#[test]
fn rpc_response_fingerprints_and_effective_contracts_are_verified() -> Result<()> {
    for mismatch in 0..5 {
        let mut harness = Harness::new()?;
        let mut prepared = connected_fixture(&harness, 1, true)?;
        let mut report: ModuleReport = serde_json::from_str(&prepared.nodes[0].run.args[5])?;
        let output = report.outputs.get_mut("out").context("output")?;
        match mismatch {
            0 => {}
            1 => output.response.as_mut().context("response")?.schema += 1,
            2 => output.response = None,
            3 => output.contract.qos = QosProfile::Sensor,
            4 => {
                output.contract.qos = QosProfile::Sensor;
                prepared.nodes[0]
                    .bindings
                    .outputs
                    .get_mut("out")
                    .context("binding")?
                    .contract
                    .qos = QosProfile::Sensor;
            }
            _ => unreachable!(),
        }
        prepared.nodes[0].run.args[5] = serde_json::to_string(&report)?;
        let namespace = prepared.nodes[0].namespace.clone();
        let client = harness.start(prepared)?;
        if mismatch == 0 {
            assert_eq!(client.wait_ready(DEADLINE)?.phase, DeploymentPhase::Ready);
        } else {
            let phase = if mismatch == 2 || mismatch == 3 || mismatch == 4 {
                DeploymentPhase::Degraded
            } else {
                DeploymentPhase::Failed
            };
            let status = wait_phase(&client, phase)?;
            assert!(client.wait_ready(Duration::ZERO).is_err());
            assert_eq!(
                status.modules[&namespace].phase,
                if phase == DeploymentPhase::Degraded {
                    DeploymentPhase::Failed
                } else {
                    DeploymentPhase::Stopped
                }
            );
        }
    }
    Ok(())
}

#[test]
fn sensor_output_connects_to_an_independent_receive_policy() -> Result<()> {
    // Given sensor delivery is owned by the producer, not by the consumer queue.
    let mut harness = Harness::new()?;
    let mut prepared = connected_fixture(&harness, 1, false)?;
    prepared.nodes[0]
        .bindings
        .outputs
        .get_mut("out")
        .context("output binding")?
        .contract
        .qos = QosProfile::Sensor;
    let mut report: ModuleReport = serde_json::from_str(&prepared.nodes[0].run.args[5])?;
    report
        .outputs
        .get_mut("out")
        .context("output report")?
        .contract
        .qos = QosProfile::Sensor;
    prepared.nodes[0].run.args[5] = serde_json::to_string(&report)?;
    // When the owner validates the two compiled reports.
    let client = harness.start(prepared)?;
    let status = client.wait_ready(DEADLINE)?;
    // Then the consumer's default FIFO does not invalidate a sensor publisher.
    assert_eq!(status.phase, DeploymentPhase::Ready);
    Ok(())
}

#[test]
fn stop_notification_observes_stopping_before_owned_tree_is_reaped() -> Result<()> {
    // Given a running service with a cooperative stop observer.
    let mut harness = Harness::new()?;
    let prepared = harness.prepared(1)?;
    let namespace = prepared.nodes[0].namespace.clone();
    let client = harness.start(prepared)?;
    client.wait_ready(DEADLINE)?;
    // When stop is requested.
    let stopped = client.stop()?;
    // Then request-time state still owns the PID, while the final state has reaped it.
    let state = harness
        .observer
        .state
        .lock()
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    let snapshot = state
        .stop_snapshots
        .get(&namespace)
        .context("stop snapshot missing")?;
    assert_eq!(snapshot.phase, DeploymentPhase::Stopping);
    assert_eq!(
        snapshot.modules[&namespace].phase,
        DeploymentPhase::Stopping
    );
    assert!(snapshot.modules[&namespace].pid.is_some());
    assert_eq!(stopped.phase, DeploymentPhase::Stopped);
    assert!(stopped.modules[&namespace].pid.is_none());
    Ok(())
}

#[test]
fn v1_readiness_report_is_rejected() -> Result<()> {
    let mut harness = Harness::new()?;
    let mut prepared = harness.prepared(1)?;
    let mut report: ModuleReport = serde_json::from_str(&prepared.nodes[0].run.args[5])?;
    report.version = 1;
    prepared.nodes[0].run.args[5] = serde_json::to_string(&report)?;
    let client = harness.start(prepared)?;
    assert!(client.wait_ready(DEADLINE).is_err());
    assert_eq!(client.status()?.phase, DeploymentPhase::Failed);
    Ok(())
}

#[test]
fn prepared_root_cannot_claim_another_endpoint_namespace() -> Result<()> {
    let harness = Harness::new()?;
    let mut prepared = harness.prepared(1)?;
    prepared.nodes[0].bindings.endpoint_namespace = "different/host".into();
    assert!(
        serve(
            prepared,
            harness.observer.as_ref(),
            &AtomicBool::new(false),
            &|| Ok(()),
        )
        .is_err()
    );
    assert!(
        harness
            .observer
            .state
            .lock()
            .map_err(|error| anyhow::anyhow!("{error}"))?
            .pids
            .is_empty()
    );
    Ok(())
}

#[test]
fn spawn_passes_the_exact_prepared_config_directory() -> Result<()> {
    let mut harness = Harness::new()?;
    let mut prepared = harness.prepared(1)?;
    let directory = prepared.root.join("frozen-config");
    std::fs::create_dir(&directory)?;
    let config = directory.join("config.yaml");
    std::fs::write(&config, "value: frozen\n")?;
    prepared.nodes[0].run.config = Some(config);
    prepared.nodes[0].config_dir = Some(directory.clone());
    prepared.nodes[0].config_bundle_dir = Some(directory.clone());
    prepared.nodes[0].run.args[9] = directory.display().to_string();
    let client = harness.start(prepared)?;
    assert_eq!(client.wait_ready(DEADLINE)?.phase, DeploymentPhase::Ready);
    Ok(())
}

#[test]
fn prepared_executable_cannot_fall_back_outside_the_declared_bundle() -> Result<()> {
    let harness = Harness::new()?;
    let mut prepared = harness.prepared(1)?;
    prepared.nodes[0].bundle_dir = prepared.root.clone();
    assert!(
        serve(
            prepared,
            harness.observer.as_ref(),
            &AtomicBool::new(false),
            &|| Ok(()),
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn incompatible_compiled_schemas_never_become_ready() -> Result<()> {
    // Given equal message names with different actual compiled schemas.
    let mut harness = Harness::new()?;
    let mut prepared = harness.prepared(2)?;
    let source = prepared.nodes[0].namespace.clone();
    prepared.nodes[0].bindings.outputs.insert(
        "out".into(),
        OutputBinding {
            contract: EndpointContract {
                type_name: "Ping".into(),
                ..EndpointContract::default()
            },
        },
    );
    prepared.nodes[1].bindings.inputs.insert(
        "in".into(),
        InputBinding {
            contract: EndpointContract {
                type_name: "Ping".into(),
                ..EndpointContract::default()
            },
            sources: vec![source],
        },
    );
    for (index, node) in prepared.nodes.iter_mut().enumerate() {
        let port = PortReport {
            type_name: "test.Ping".into(),
            schema: u64::try_from(index)? + 1,
            contract: EndpointContract {
                type_name: "Ping".into(),
                ..EndpointContract::default()
            },
            response: None,
        };
        let report = ModuleReport {
            version: CONTRACT_VERSION,
            namespace: node.namespace.clone(),
            inputs: if index == 1 {
                BTreeMap::from([("in".into(), port.clone())])
            } else {
                BTreeMap::new()
            },
            outputs: if index == 0 {
                BTreeMap::from([("out".into(), port)])
            } else {
                BTreeMap::new()
            },
        };
        node.run.args[5] = serde_json::to_string(&report)?;
    }
    // When both actual processes publish readiness and their reports.
    let client = harness.start(prepared)?;
    // Then overall readiness fails and incompatible children are stopped.
    assert!(client.wait_ready(DEADLINE).is_err());
    let status = client.status()?;
    assert_eq!(status.phase, DeploymentPhase::Failed);
    assert!(status.modules.values().all(|module| module.pid.is_none()));
    Ok(())
}

#[test]
fn readiness_requires_a_report_from_the_owned_generation() -> Result<()> {
    // Given a process that announces readiness but never writes its report.
    let mut harness = Harness::new()?;
    let mut prepared = harness.prepared(1)?;
    prepared.nodes[0].run.args[3] = "no-report".into();
    let namespace = prepared.nodes[0].namespace.clone();
    let client = harness.start(prepared)?;
    harness.observer.wait_pid(&namespace, None)?;
    // When readiness is requested with a bounded deadline.
    let result = client.wait_ready(Duration::from_millis(50));
    // Then bus presence alone cannot make the deployment ready.
    assert!(result.is_err());
    assert_eq!(client.status()?.phase, DeploymentPhase::Starting);
    Ok(())
}

#[test]
fn mismatched_report_identity_is_terminal() -> Result<()> {
    // Given a real process reporting a different module identity.
    let mut harness = Harness::new()?;
    let mut prepared = harness.prepared(1)?;
    let report = ModuleReport {
        version: CONTRACT_VERSION,
        namespace: "another/deployment".into(),
        inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
    };
    prepared.nodes[0].run.args[5] = serde_json::to_string(&report)?;
    // When the owner observes the process's report and readiness.
    let client = harness.start(prepared)?;
    // Then readiness fails without accepting the other namespace's contract.
    assert!(client.wait_ready(DEADLINE).is_err());
    assert_eq!(client.status()?.phase, DeploymentPhase::Failed);
    Ok(())
}

#[test]
fn cyclic_connections_start_without_a_readiness_dependency_order() -> Result<()> {
    // Given two modules whose input/output bindings form a communication cycle.
    let mut harness = Harness::new()?;
    let mut prepared = harness.prepared(2)?;
    for index in 0..2 {
        let source = prepared.nodes[1 - index].namespace.clone();
        let node = &mut prepared.nodes[index];
        node.bindings.inputs.insert(
            "in".into(),
            InputBinding {
                contract: EndpointContract {
                    type_name: "Ping".into(),
                    ..EndpointContract::default()
                },
                sources: vec![source],
            },
        );
        node.bindings.outputs.insert(
            "out".into(),
            OutputBinding {
                contract: EndpointContract {
                    type_name: "Ping".into(),
                    ..EndpointContract::default()
                },
            },
        );
        let port = PortReport {
            type_name: "test.Ping".into(),
            schema: 42,
            contract: EndpointContract {
                type_name: "Ping".into(),
                ..EndpointContract::default()
            },
            response: None,
        };
        let report = ModuleReport {
            version: CONTRACT_VERSION,
            namespace: node.namespace.clone(),
            inputs: BTreeMap::from([("in".into(), port.clone())]),
            outputs: BTreeMap::from([("out".into(), port)]),
        };
        node.run.args[5] = serde_json::to_string(&report)?;
    }
    // When the deployment starts both executable modules.
    let client = harness.start(prepared)?;
    // Then both actual compiled contracts reach readiness together.
    let status = client.wait_ready(DEADLINE)?;
    assert_eq!(status.phase, DeploymentPhase::Ready);
    assert_eq!(status.modules.len(), 2);
    assert!(
        status
            .modules
            .values()
            .all(|module| module.report.is_some())
    );
    Ok(())
}

#[test]
fn bootstrap_failure_releases_ownership_without_spawning() -> Result<()> {
    // Given a prepared deployment whose CLI rendezvous fails.
    let harness = Harness::new()?;
    let prepared = harness.prepared(1)?;
    let root = prepared.root.clone();
    // When bootstrap fails before executable reconciliation.
    let result = serve(
        prepared,
        harness.observer.as_ref(),
        &AtomicBool::new(false),
        &|| anyhow::bail!("bootstrap peer disappeared"),
    );
    // Then no processes were started and no endpoint remains authoritative.
    assert!(result.is_err());
    assert!(DeploymentClient::find(&root)?.is_none());
    assert!(
        harness
            .observer
            .state
            .lock()
            .map_err(|error| anyhow::anyhow!("{error}"))?
            .pids
            .is_empty()
    );
    assert_eq!(
        last_status(&root)?.context("last status")?.phase,
        DeploymentPhase::Failed
    );
    Ok(())
}

#[test]
fn stop_acknowledgement_does_not_replace_owned_process_reaping() -> Result<()> {
    // Given a child that deliberately ignores a delivered stop request.
    let mut harness = Harness::new()?;
    let mut prepared = harness.prepared(1)?;
    prepared.nodes[0].run.args[3] = "ignore-stop".into();
    let root = prepared.root.clone();
    let namespace = prepared.nodes[0].namespace.clone();
    let client = harness.start(prepared)?;
    client.wait_ready(DEADLINE)?;
    let mut child_connection = harness.observer.socket(&namespace)?;
    // When stop is requested, including an independently attached subscriber.
    let following = client.watch_stopped()?;
    let terminal = client.watch_terminal()?;
    let follow = std::thread::spawn(move || following.wait());
    let stopped = client.stop()?;
    harness.join_owner()?;
    // Then cleanup is complete before the response and endpoint disappears.
    assert_eq!(stopped.phase, DeploymentPhase::Stopped);
    assert!(stopped.modules.values().all(|module| module.pid.is_none()));
    assert_disconnected(&mut child_connection)?;
    let subscription = follow
        .join()
        .map_err(|_| anyhow::anyhow!("follow panic"))??;
    assert_eq!(subscription.phase, DeploymentPhase::Stopped);
    assert_eq!(terminal.wait()?.phase, DeploymentPhase::Stopped);
    assert!(DeploymentClient::find(&root)?.is_none());
    assert!(!last_status(&root)?.context("last status")?.owner_alive);
    Ok(())
}

fn assert_disconnected(stream: &mut TcpStream) -> Result<()> {
    stream.set_read_timeout(Some(DEADLINE))?;
    let mut byte = [0];
    match stream.read(&mut byte) {
        Ok(0) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => Ok(()),
        other => anyhow::bail!("owned process connection remained open: {other:?}"),
    }
}

#[test]
fn stop_cleans_descendants_after_cooperative_parent_exit() -> Result<()> {
    // Given a cooperative leader with a non-cooperative descendant.
    let mut harness = Harness::new()?;
    let mut prepared = harness.prepared(1)?;
    prepared.nodes[0].run.args[3] = "tree".into();
    let descendant = format!("{}/descendant", prepared.nodes[0].namespace);
    let client = harness.start(prepared)?;
    client.wait_ready(DEADLINE)?;
    harness.observer.wait_pid(&descendant, None)?;
    let mut connection = harness.observer.socket(&descendant)?;
    // When the leader acknowledges stop and exits.
    client.stop()?;
    // Then the still-owned job/group also terminates its descendant.
    assert_disconnected(&mut connection)?;
    Ok(())
}

#[test]
fn unauthenticated_stop_has_no_side_effects() -> Result<()> {
    // Given a live authenticated owner.
    let mut harness = Harness::new()?;
    let prepared = harness.prepared(1)?;
    let root = prepared.root.clone();
    let client = harness.start(prepared)?;
    let before = client.wait_ready(DEADLINE)?;
    let control: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join(".reiny/control.json"))?)?;
    let mut stream = TcpStream::connect(control["address"].as_str().context("address")?)?;
    stream.set_read_timeout(Some(DEADLINE))?;
    let request = serde_json::to_vec(&serde_json::json!({
        "token": "incorrect", "owner_generation": control["owner_generation"],
        "action": {"operation": "stop"}
    }))?;
    // When an unauthenticated caller submits a mutating request.
    stream.write_all(&u32::try_from(request.len())?.to_be_bytes())?;
    stream.write_all(&request)?;
    let mut length = [0; 4];
    stream.read_exact(&mut length)?;
    let mut bytes = vec![0; usize::try_from(u32::from_be_bytes(length))?];
    stream.read_exact(&mut bytes)?;
    // Then the request is rejected and the exact owned process remains alive.
    let response: serde_json::Value = serde_json::from_slice(&bytes)?;
    assert!(response["error"].is_string());
    assert_eq!(client.status()?.modules, before.modules);
    assert!(
        serde_json::to_value(client.status()?)?
            .get("token")
            .is_none()
    );
    Ok(())
}

#[test]
fn global_identity_lock_rejects_a_second_root() -> Result<()> {
    // Given an owner holding a (domain, deployment) identity.
    let mut harness = Harness::new()?;
    let prepared = harness.prepared(1)?;
    let client = harness.start(prepared.clone())?;
    let before = client.wait_ready(DEADLINE)?;
    let second = tempfile::tempdir()?;
    let mut competing = prepared;
    competing.root = std::fs::canonicalize(second.path())?;
    // When another root attempts to own the same identity.
    let result = serve(
        competing,
        harness.observer.as_ref(),
        &AtomicBool::new(false),
        &|| Ok(()),
    );
    // Then the OS lock rejects it without modifying the first owner's process.
    assert!(result.is_err());
    assert_eq!(client.status()?.modules, before.modules);
    Ok(())
}

#[test]
fn owner_error_cleans_children_and_invalidates_old_client() -> Result<()> {
    // Given a running child and its exact owner capability.
    let mut harness = Harness::new()?;
    let prepared = harness.prepared(1)?;
    let root = prepared.root.clone();
    let client = harness.start(prepared)?;
    client.wait_ready(DEADLINE)?;
    // When the observer transport fails the owner loop.
    harness
        .observer
        .state
        .lock()
        .map_err(|error| anyhow::anyhow!("{error}"))?
        .fail = true;
    assert!(harness.join_owner().is_err());
    // Then owned children were reaped and no stale PID grants authority.
    assert!(client.status().is_err());
    assert!(DeploymentClient::find(&root)?.is_none());
    let state = last_status(&root)?.context("last status")?;
    assert_eq!(state.phase, DeploymentPhase::Failed);
    assert!(!state.owner_alive);
    assert!(state.modules.values().all(|module| module.pid.is_none()));
    Ok(())
}

#[test]
fn stop_persistence_failure_reaches_caller_and_registered_subscriber() -> Result<()> {
    let mut harness = Harness::new()?;
    let prepared = harness.prepared(1)?;
    let namespace = prepared.nodes[0].namespace.clone();
    let state_path = prepared.root.join(".reiny/state.json");
    let client = harness.start(prepared)?;
    client.wait_ready(DEADLINE)?;
    let mut connection = harness.observer.socket(&namespace)?;
    let subscription = client.watch_stopped()?;
    let terminal = client.watch_terminal()?;
    std::fs::remove_file(&state_path)?;
    std::fs::create_dir(&state_path)?;
    let stop_error = client
        .stop()
        .err()
        .context("state directory cannot be replaced by a file")?;
    let subscribed_error = subscription
        .wait()
        .err()
        .context("failed stop cannot be acknowledged")?;
    let terminal_error = terminal
        .wait()
        .err()
        .context("failed stop is not a clean terminal state")?;
    let owner_error = harness
        .join_owner()
        .err()
        .context("failed persistence must escape serve")?;
    let path = state_path.display().to_string();
    assert!(format!("{stop_error:#}").contains(&path));
    assert!(format!("{subscribed_error:#}").contains(&path));
    assert!(format!("{terminal_error:#}").contains(&path));
    assert!(format!("{owner_error:#}").contains(&path));
    assert_disconnected(&mut connection)?;
    Ok(())
}

#[test]
fn endpoint_cleanup_error_is_not_hidden_by_owner_failure() -> Result<()> {
    let mut harness = Harness::new()?;
    let prepared = harness.prepared(1)?;
    let control_path = prepared.root.join(".reiny/control.json");
    let root = prepared.root.clone();
    let client = harness.start(prepared)?;
    client.wait_ready(DEADLINE)?;
    std::fs::remove_file(&control_path)?;
    std::fs::create_dir(&control_path)?;
    harness
        .observer
        .state
        .lock()
        .map_err(|error| anyhow::anyhow!("{error}"))?
        .fail = true;
    let error = harness
        .join_owner()
        .err()
        .context("both owner and endpoint cleanup failed")?;
    assert!(format!("{error:#}").contains(&control_path.display().to_string()));
    let state = last_status(&root)?.context("failed status")?;
    assert_eq!(state.phase, DeploymentPhase::Failed);
    assert!(state.modules.values().all(|module| module.pid.is_none()));
    assert!(
        state
            .error
            .context("cleanup error")?
            .contains(&control_path.display().to_string())
    );
    Ok(())
}

#[cfg(windows)]
struct OwnerProcess(std::process::Child);

#[cfg(windows)]
impl Drop for OwnerProcess {
    fn drop(&mut self) {
        if let Err(error) = self.0.kill() {
            eprintln!("owner process teardown: {error}");
        }
        if let Err(error) = self.0.wait() {
            eprintln!("owner process reap: {error}");
        }
    }
}

/// Exercise owner startup/stop and provide the subprocess owner entry point.
#[cfg(windows)]
#[test]
fn subprocess_owner_fixture() -> Result<()> {
    struct PendingObserver;
    impl ModuleObserver for PendingObserver {
        fn ready(&self, _namespace: &str) -> Result<bool> {
            Ok(false)
        }
        fn request_stop(&self, _namespace: &str) -> Result<()> {
            Ok(())
        }
    }
    if std::env::var_os("REINY_TEST_PREPARED").is_none() {
        let mut harness = Harness::new()?;
        let prepared = harness.prepared(1)?;
        let client = harness.start(prepared)?;
        assert_eq!(client.wait_ready(DEADLINE)?.phase, DeploymentPhase::Ready);
        assert_eq!(client.stop()?.phase, DeploymentPhase::Stopped);
        harness.join_owner()?;
        return Ok(());
    }
    let prepared = serde_json::from_str(&std::env::var("REINY_TEST_PREPARED")?)?;
    serve(prepared, &PendingObserver, &AtomicBool::new(false), &|| {
        println!("OWNER_LISTENING");
        std::io::stdout().flush()?;
        Ok(())
    })
}

#[cfg(windows)]
#[test]
fn abrupt_owner_disappearance_closes_the_owned_windows_job() -> Result<()> {
    // Given a separate owner process and a fixture with a descendant.
    let mut harness = Harness::new()?;
    let mut prepared = harness.prepared(1)?;
    prepared.nodes[0].run.args[3] = "tree".into();
    let namespace = prepared.nodes[0].namespace.clone();
    let descendant = format!("{namespace}/descendant");
    let root = prepared.root.clone();
    let mut process = OwnerProcess(
        Command::new(std::env::current_exe()?)
            .args(["--exact", "subprocess_owner_fixture", "--nocapture"])
            .env("REINY_TEST_PREPARED", serde_json::to_string(&prepared)?)
            .env("REINY_BUNDLE_DIR", "stale-inherited-bundle")
            .env("REINY_CONFIG_DIR", "stale-inherited-config")
            .stdout(std::process::Stdio::piped())
            .spawn()?,
    );
    let stdout = process.0.stdout.take().context("owner stdout")?;
    let (send, receive) = mpsc::sync_channel(1);
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if line?.contains("OWNER_LISTENING") {
                send.send(()).context("bootstrap test receiver")?;
                return Ok::<_, anyhow::Error>(());
            }
        }
        anyhow::bail!("owner exited before bootstrap")
    });
    receive.recv_timeout(DEADLINE)?;
    reader
        .join()
        .map_err(|_| anyhow::anyhow!("bootstrap reader panic"))??;
    harness.observer.wait_pid(&namespace, None)?;
    harness.observer.wait_pid(&descendant, None)?;
    let mut child = harness.observer.socket(&namespace)?;
    let mut grandchild = harness.observer.socket(&descendant)?;
    let client = DeploymentClient::find(&root)?.context("subprocess owner")?;
    let subscription = client.watch_stopped()?;
    // When the OS terminates the exact owner without executing Rust Drop.
    process.0.kill()?;
    process.0.wait()?;
    // Then the kernel closes its kill-on-close job, including descendants.
    assert_disconnected(&mut child)?;
    assert_disconnected(&mut grandchild)?;
    assert!(subscription.wait().is_err());
    assert!(client.status().is_err());
    assert!(DeploymentClient::find(&root)?.is_none());
    // The stale endpoint and lock do not prevent a new owner incarnation.
    let replacement = harness.start(prepared)?;
    assert_eq!(
        replacement.wait_ready(DEADLINE)?.phase,
        DeploymentPhase::Ready
    );
    Ok(())
}
