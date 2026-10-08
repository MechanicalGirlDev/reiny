use super::*;
use crate::OwnedChild;
use crate::bindings::PortDelegation;
use crate::engine::READY_CHUNK;
use crate::managed::owned::delegated_bindings;
use std::process::Stdio;

fn host_bindings() -> ModuleBindings {
    let mut binding = bindings(SENDER);
    binding
        .inputs
        .insert("incoming".into(), input("deployment/source"));
    binding.outputs.insert(
        "outgoing".into(),
        OutputBinding {
            contract: contract("test.Probe"),
        },
    );
    binding.children.insert(
        "driver".into(),
        PortDelegation {
            inputs: vec!["incoming".into()],
            outputs: vec!["outgoing".into()],
        },
    );
    binding.executables = vec!["helper".into()];
    binding
}

// The child OS process blocks on its owned stdin. Its managed endpoints use a real Local
// engine in this test process, so process exit and bus readiness can be exercised independently.
async fn spawn_fixture(bus: Arc<Local>, directory: &Directory) -> (Cloudy, OwnedChild) {
    let mut host = Cloudy::open(options(bus, host_bindings()))
        .await
        .expect("host");
    host.module.bundle_dir = Some(directory.0.clone());
    #[cfg(windows)]
    let source = PathBuf::from(std::env::var_os("SystemRoot").expect("Windows directory"))
        .join("System32/cmd.exe");
    #[cfg(not(windows))]
    let source = PathBuf::from("/bin/sh");
    let destination = directory.0.join(if cfg!(windows) {
        "helper.exe"
    } else {
        "helper"
    });
    std::fs::copy(source, &destination).expect("stage helper");
    let executable = host.artifact("helper").expect("declared artifact");
    let mut command = tokio::process::Command::new(executable);
    #[cfg(windows)]
    command.args(["/D", "/C", "set", "/P", "reiny_fixture="]);
    #[cfg(not(windows))]
    command.args(["-c", "read reiny_fixture"]);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let child = host
        .spawn_owned_child("driver", &mut command, &directory.0)
        .expect("spawn owned");
    (host, child)
}

async fn open_child(bus: Arc<Local>, directory: &Directory) -> Cloudy {
    let child_bindings: ModuleBindings = serde_json::from_slice(
        &std::fs::read(directory.0.join("driver/bindings.json")).expect("child bindings"),
    )
    .expect("parse child bindings");
    let mut opts = options(bus, child_bindings);
    opts.module_report_path = Some(directory.0.join("driver/report.json"));
    Cloudy::open(opts).await.expect("child context")
}

fn watch_ready(
    bus: &Local,
    namespace: &str,
) -> (
    crate::engine::Guard,
    tokio::sync::mpsc::UnboundedReceiver<Presence>,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let key = Key::topic(DOMAIN, Some(namespace), READY_CHUNK);
    let guard = bus
        .watch_alive(
            &key,
            Box::new(move |event| {
                let _ = tx.send(event);
            }),
        )
        .expect("readiness watch");
    (guard, rx)
}

#[tokio::test]
async fn delegation_is_exclusive_and_child_runtime_identity_is_distinct() {
    let bus = Arc::new(Local::new());
    let binding = host_bindings();
    let child_binding = delegated_bindings(&binding, "driver").expect("delegation");
    assert_eq!(child_binding.namespace, format!("{SENDER}/owned/driver"));
    assert_eq!(child_binding.endpoint_namespace, SENDER);
    assert!(child_binding.children.is_empty());
    assert_eq!(child_binding.executables, binding.executables);
    let host = Cloudy::open(options(bus.clone(), binding))
        .await
        .expect("host");
    assert!(host.input::<Probe>("incoming").is_err());
    assert!(host.output::<Probe>("outgoing").is_err());
    assert!(host.ready().is_err());
    let child = Cloudy::open(options(bus.clone(), child_binding))
        .await
        .expect("child");
    let output = child.output::<Probe>("outgoing").expect("child output");
    let mut incoming = {
        let receiver = consumer(bus).await;
        receiver.input::<Probe>("incoming").expect("receiver")
    };
    output.send(Probe { value: 8 }).await.expect("child send");
    let envelope = timeout(PATIENCE, incoming.recv_envelope())
        .await
        .expect("receive")
        .expect("sample");
    assert_eq!(
        (envelope.source.as_str(), envelope.value.value),
        (SENDER, 8)
    );
    assert!(child.output::<Probe>("undeclared").is_err());
}

#[tokio::test]
async fn host_requires_child_report_and_live_readiness_then_withdraws_on_port_loss() {
    let directory = Directory::new();
    let bus = Arc::new(Local::new());
    let (mut host, mut process) = spawn_fixture(bus.clone(), &directory).await;
    host.module.report_path = Some(directory.0.join("host-report.json"));
    let child = open_child(bus.clone(), &directory).await;
    let incoming = child.input::<Probe>("incoming").expect("child input");
    let output = child.output::<Probe>("outgoing").expect("child output");
    assert!(host.ready().is_err());
    child.ready().expect("child initialization complete");
    // The token alone cannot satisfy the host: the child report must be accepted first.
    assert!(host.ready().is_err());
    timeout(PATIENCE, process.wait_ready())
        .await
        .expect("bounded child readiness")
        .expect("validated report");
    let (_watch, mut events) = watch_ready(&bus, SENDER);
    host.ready().expect("host initialization complete");
    let key = Key::topic(DOMAIN, Some(SENDER), READY_CHUNK);
    assert_eq!(
        timeout(PATIENCE, events.recv()).await.expect("joined"),
        Some(Presence::Joined(key.clone()))
    );
    let report: ModuleReport = serde_json::from_slice(
        &std::fs::read(directory.0.join("host-report.json")).expect("host report"),
    )
    .expect("parse report");
    assert_eq!(report.namespace, SENDER);
    assert_eq!(
        report.outputs["outgoing"].schema,
        Probe::SCHEMA.expect("schema")
    );
    assert_eq!(report.outputs["outgoing"].contract, contract("test.Probe"));
    // The watch predates the child handle loss; no timer luck determines the assertion.
    drop(output);
    assert_eq!(
        timeout(PATIENCE, events.recv())
            .await
            .expect("host readiness withdrawn"),
        Some(Presence::Left(key))
    );
    timeout(PATIENCE, host.shutdown())
        .await
        .expect("host failed");
    timeout(PATIENCE, process.wait())
        .await
        .expect("child reaped")
        .expect("exit evidence");
    assert!(host.ready().is_err());
    drop(incoming);
}

#[tokio::test]
async fn process_exit_with_a_stale_child_token_withdraws_host_readiness() {
    let directory = Directory::new();
    let bus = Arc::new(Local::new());
    let (host, mut process) = spawn_fixture(bus.clone(), &directory).await;
    let child = open_child(bus.clone(), &directory).await;
    let _incoming = child.input::<Probe>("incoming").expect("input");
    let _outgoing = child.output::<Probe>("outgoing").expect("output");
    child.ready().expect("child ready");
    timeout(PATIENCE, process.wait_ready())
        .await
        .expect("bounded readiness")
        .expect("report");
    let (_watch, mut events) = watch_ready(&bus, SENDER);
    host.ready().expect("host ready");
    let key = Key::topic(DOMAIN, Some(SENDER), READY_CHUNK);
    assert_eq!(
        timeout(PATIENCE, events.recv()).await.expect("joined"),
        Some(Presence::Joined(key.clone()))
    );
    let _status = timeout(PATIENCE, process.kill())
        .await
        .expect("kill reaps")
        .expect("exit status");
    assert_eq!(
        timeout(PATIENCE, events.recv()).await.expect("left"),
        Some(Presence::Left(key))
    );
    assert!(host.ready().is_err());
    assert_ne!(
        bus.alive(
            &Key::topic(DOMAIN, Some(process.namespace()), READY_CHUNK),
            PATIENCE
        )
        .await
        .expect("stale token")
        .len(),
        0
    );
}

#[tokio::test]
async fn a_ready_child_cannot_substitute_a_report_from_another_namespace() {
    let directory = Directory::new();
    let bus = Arc::new(Local::new());
    let (host, mut process) = spawn_fixture(bus.clone(), &directory).await;
    let child = open_child(bus, &directory).await;
    let _incoming = child.input::<Probe>("incoming").expect("input");
    let _outgoing = child.output::<Probe>("outgoing").expect("output");
    child.ready().expect("child ready");
    let path = directory.0.join("driver/report.json");
    let mut report: ModuleReport =
        serde_json::from_slice(&std::fs::read(&path).expect("report")).expect("parse");
    report.namespace = SENDER.into();
    std::fs::write(path, serde_json::to_vec(&report).expect("serialize")).expect("wrong identity");
    assert!(
        timeout(PATIENCE, process.wait_ready())
            .await
            .expect("bounded validation")
            .is_err()
    );
    assert!(host.ready().is_err());
    timeout(PATIENCE, process.kill())
        .await
        .expect("reap")
        .expect("status");
}

#[tokio::test]
async fn artifacts_require_a_staged_allowlisted_path_and_delegation_is_not_repeatable() {
    let directory = Directory::new();
    let bus = Arc::new(Local::new());
    let (host, mut child) = spawn_fixture(bus, &directory).await;
    assert!(host.artifact("not-declared").is_err());
    assert!(host.artifact("../helper").is_err());
    let mut command = tokio::process::Command::new(host.artifact("helper").expect("helper"));
    assert!(
        host.spawn_owned_child("driver", &mut command, &directory.0)
            .is_err()
    );
    assert!(
        host.spawn_owned_child("undeclared", &mut command, &directory.0)
            .is_err()
    );
    let mut command = tokio::process::Command::new("helper");
    assert!(
        host.spawn_owned_child("driver", &mut command, &directory.0)
            .is_err()
    );
    timeout(PATIENCE, child.kill())
        .await
        .expect("reap")
        .expect("status");
}

#[tokio::test]
async fn host_shutdown_delivers_child_stop_before_enforcing_process_reaping() {
    // Given a real blocked process and its distinct managed control endpoint.
    let directory = Directory::new();
    let bus = Arc::new(Local::new());
    let (host, mut process) = spawn_fixture(bus.clone(), &directory).await;
    let child = open_child(bus, &directory).await;
    // When the host receives cooperative shutdown.
    host.shutdown_now();
    // Then the child's SDK observes stop before the noncooperative OS fixture is reaped.
    timeout(PATIENCE, child.shutdown())
        .await
        .expect("child stop delivered");
    timeout(PATIENCE, process.wait())
        .await
        .expect("bounded reaping")
        .expect("reaped process");
}

#[cfg(feature = "zenoh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn child_zenoh_configuration_connects_to_host_without_rebinding_its_listener() {
    let mut config = zenoh::Config::default();
    config
        .insert_json5("listen/endpoints", r#"["tcp/127.0.0.1:37463"]"#)
        .expect("listener");
    config
        .insert_json5("scouting/multicast/enabled", "false")
        .expect("no multicast");
    let binding = host_bindings();
    let mut opts = RuntimeOptions::new(SENDER);
    opts.domain = DOMAIN.into();
    opts.module_report_path = None;
    opts.module_bindings = Some(binding.clone());
    opts.zenoh = crate::ZenohSource::Config(Box::new(config));
    let host = Cloudy::open(opts).await.expect("listening host");
    let child_config = host
        .module
        .child_zenoh_config
        .as_ref()
        .expect("child fabric config");
    let parsed: serde_json::Value = serde_json::from_str(child_config).expect("config JSON");
    assert_eq!(parsed["listen"]["endpoints"], serde_json::json!([]));
    assert_eq!(
        parsed["connect"]["endpoints"],
        serde_json::json!(["tcp/127.0.0.1:37463"])
    );
    let child_binding = delegated_bindings(&binding, "driver").expect("delegation");
    let key = Key::topic(DOMAIN, Some(&child_binding.namespace), READY_CHUNK);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let _watch = host
        .engine
        .watch_alive(
            &key,
            Box::new(move |event| {
                let _ = tx.send(event);
            }),
        )
        .expect("watch before child");
    let mut opts = RuntimeOptions::new(&child_binding.namespace);
    opts.domain = DOMAIN.into();
    opts.module_report_path = None;
    opts.module_bindings = Some(child_binding);
    opts.zenoh = crate::ZenohSource::Config(Box::new(
        zenoh::Config::from_json5(child_config).expect("child config"),
    ));
    let child = Cloudy::open(opts)
        .await
        .expect("child opens without listener collision");
    let _input = child.input::<Probe>("incoming").expect("child input");
    let _output = child.output::<Probe>("outgoing").expect("child output");
    child.ready().expect("child ready");
    assert_eq!(
        timeout(PATIENCE, rx.recv())
            .await
            .expect("same fabric readiness"),
        Some(Presence::Joined(key))
    );
}
