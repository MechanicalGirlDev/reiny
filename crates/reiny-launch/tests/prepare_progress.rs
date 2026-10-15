//! Real builders, event ordering, cache identity, cancellation and byte integrity.

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use reiny_launch::{
    BuildSpec, CancelToken, CargoBuild, CommandBuild, DeploymentPlan, GroupKind, GroupState,
    PrepareError, PrepareEvent, PrepareObserver, PrepareOptions, PrepareStatus, PreparedDeployment,
};

const DEADLINE: Duration = Duration::from_secs(30);

#[derive(Default)]
struct Recorder {
    events: Mutex<Vec<PrepareEvent>>,
    cancel_planned: Option<CancelToken>,
    running: Option<mpsc::Sender<String>>,
}

impl PrepareObserver for Recorder {
    fn event(&self, event: &PrepareEvent) {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event.clone());
        if matches!(event, PrepareEvent::Planned { .. })
            && let Some(cancel) = &self.cancel_planned
        {
            cancel.cancel();
        }
        if let PrepareEvent::Group {
            id,
            state: GroupState::Running,
            ..
        } = event
            && let Some(running) = &self.running
        {
            let _ = running.send(id.clone());
        }
    }
}

impl Recorder {
    fn events(&self) -> Vec<PrepareEvent> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn complete(&self, expected: PrepareStatus) -> Result<()> {
        let events = self.events();
        assert_eq!(events.first(), Some(&PrepareEvent::Started));
        assert!(
            matches!(events.last(), Some(PrepareEvent::Finished { status, .. }) if *status == expected)
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, PrepareEvent::Finished { .. }))
                .count(),
            1
        );
        let mut planned = None;
        let mut states: BTreeMap<&str, Vec<GroupState>> = BTreeMap::new();
        for event in &events {
            // The exact exported enum is also the protocol payload.
            let encoded = serde_json::to_vec(event)?;
            assert_eq!(serde_json::from_slice::<PrepareEvent>(&encoded)?, *event);
            match event {
                PrepareEvent::Planned { total_groups } => planned = Some(*total_groups),
                PrepareEvent::Group { id, state, .. } => {
                    assert!(planned.is_some());
                    states.entry(id).or_default().push(*state);
                }
                _ => {}
            }
        }
        if let Some(total) = planned {
            assert_eq!(states.len(), total);
        }
        for transitions in states.values() {
            assert!(
                matches!(
                    transitions.as_slice(),
                    [GroupState::Queued, GroupState::Cancelled]
                        | [
                            GroupState::Queued,
                            GroupState::Running,
                            GroupState::Cached
                                | GroupState::Done
                                | GroupState::Failed
                                | GroupState::Cancelled
                        ]
                ),
                "{transitions:?}"
            );
        }
        Ok(())
    }
}

fn prebuilt() -> Result<(tempfile::TempDir, DeploymentPlan)> {
    let root = tempfile::tempdir()?;
    fs::create_dir(root.path().join("bin"))?;
    fs::create_dir(root.path().join("config"))?;
    for name in ["app", "helper"] {
        fs::write(
            root.path()
                .join("bin")
                .join(format!("{name}{}", std::env::consts::EXE_SUFFIX)),
            name,
        )?;
    }
    fs::write(root.path().join("bin/native.dll"), b"runtime library")?;
    fs::write(root.path().join("config/app.yaml"), b"asset: input.bin")?;
    fs::write(root.path().join("config/input.bin"), b"asset")?;
    fs::write(root.path().join("zenoh.json5"), b"{}")?;
    fs::write(root.path().join("resource.bin"), b"resource")?;
    fs::write(
        root.path().join("main.yaml"),
        r"version: 3
deployment: fixture
providers:
  process: {type: process, bin_dir: bin, zenoh_config: zenoh.json5}
  files: {type: artifact}
run: {provider: process, bin: app, companions: [helper], config: config/app.yaml, config_assets: [input.bin]}
resources:
  table: {type: artifact.file, provider: files, source: resource.bin}
",
    )?;
    let plan = DeploymentPlan::load(root.path(), false)?;
    Ok((root, plan))
}

#[test]
fn complete_events_and_prepared_envelope_reject_every_tampered_input() -> Result<()> {
    let (root, plan) = prebuilt()?;
    let recorder = Recorder::default();
    let prepared = plan.prepare_with(&PrepareOptions {
        observer: Some(&recorder),
        jobs: 3,
        ..PrepareOptions::default()
    })?;
    recorder.complete(PrepareStatus::Success)?;
    let output = root.path().join("prepared.json");
    prepared.write_file(&output)?;
    PreparedDeployment::load_file(&output)?;
    assert!(prepared.write_file(&output).is_err());
    let node = &prepared.nodes[0];
    let paths = [
        node.executable.clone(),
        node.bundle_dir
            .join(format!("helper{}", std::env::consts::EXE_SUFFIX)),
        node.bundle_dir.join("native.dll"),
        node.run.config.clone().context("config")?,
        node.config_dir
            .as_ref()
            .context("config dir")?
            .join("input.bin"),
        node.provider.zenoh_config.clone().context("Zenoh config")?,
        prepared.resources["fixture/table"].clone(),
    ];
    for path in paths {
        let original = fs::read(&path)?;
        fs::write(&path, b"tampered")?;
        assert!(prepared.verify().is_err(), "{}", path.display());
        assert!(PreparedDeployment::load_file(&output).is_err());
        fs::write(&path, original)?;
    }
    fs::write(node.bundle_dir.join("extra.bin"), b"unrecorded")?;
    assert!(prepared.verify().is_err());
    fs::remove_file(node.bundle_dir.join("extra.bin"))?;
    let original = fs::read(&output)?;
    let envelope: serde_json::Value = serde_json::from_slice(&original)?;
    for (field, value) in [
        ("version", serde_json::json!(2)),
        ("prepare_protocol", serde_json::json!(2)),
        ("reiny_version", serde_json::json!("0.0.0")),
        ("payload_sha256", serde_json::json!("0".repeat(64))),
        ("unknown", serde_json::json!(true)),
    ] {
        let mut changed = envelope.clone();
        changed[field] = value;
        fs::write(&output, serde_json::to_vec(&changed)?)?;
        assert!(PreparedDeployment::load_file(&output).is_err());
    }
    let mut changed = envelope;
    changed["deployment"]["nodes"][0]["run"]["args"] = serde_json::json!(["changed"]);
    fs::write(&output, serde_json::to_vec(&changed)?)?;
    assert!(PreparedDeployment::load_file(&output).is_err());
    fs::write(&output, original)?;
    fs::remove_dir_all(root.path().join("bin"))?;
    fs::remove_dir_all(root.path().join("config"))?;
    fs::remove_file(root.path().join("resource.bin"))?;
    fs::remove_file(root.path().join("zenoh.json5"))?;
    PreparedDeployment::load_file(&output)?;
    Ok(())
}

#[test]
fn invalid_jobs_precancel_and_invalid_assets_have_complete_terminals() -> Result<()> {
    let (_root, mut plan) = prebuilt()?;
    for jobs in [0, 1] {
        let recorder = Recorder::default();
        let cancel = CancelToken::default();
        if jobs == 1 {
            cancel.cancel();
        }
        assert!(
            plan.prepare_with(&PrepareOptions {
                jobs,
                cancel,
                observer: Some(&recorder),
                ..PrepareOptions::default()
            })
            .is_err()
        );
        recorder.complete(if jobs == 0 {
            PrepareStatus::Failed
        } else {
            PrepareStatus::Cancelled
        })?;
    }
    plan.nodes[0].run.config_assets.push("missing".into());
    let recorder = Recorder::default();
    assert!(
        plan.prepare_with(&PrepareOptions {
            observer: Some(&recorder),
            ..PrepareOptions::default()
        })
        .is_err()
    );
    assert!(
        !recorder
            .events()
            .iter()
            .any(|event| matches!(event, PrepareEvent::Group { .. }))
    );
    recorder.complete(PrepareStatus::Failed)?;
    let cancel = CancelToken::default();
    cancel.cancel();
    assert!(matches!(
        DeploymentPlan::load_with(Path::new("not-present"), false, &cancel),
        Err(PrepareError::Cancelled)
    ));
    Ok(())
}

fn cargo_plan() -> Result<(tempfile::TempDir, DeploymentPlan)> {
    let (root, mut plan) = prebuilt()?;
    fs::create_dir_all(root.path().join("src/bin"))?;
    fs::write(
        root.path().join("Cargo.toml"),
        r#"[workspace]
[package]
name = "prepare-fixture"
version = "0.0.0"
edition = "2021"
[features]
default = []
variant = []
"#,
    )?;
    fs::write(
        root.path().join("Cargo.lock"),
        "version = 4\n[[package]]\nname = \"prepare-fixture\"\nversion = \"0.0.0\"\n",
    )?;
    for name in ["app", "helper"] {
        fs::write(
            root.path().join(format!("src/bin/{name}.rs")),
            "fn main() { println!(\"first\"); }\n",
        )?;
    }
    plan.nodes[0].build = Some(BuildSpec::Cargo(CargoBuild {
        profile: "dev".into(),
        ..CargoBuild::default()
    }));
    let mut duplicate = plan.nodes[0].clone();
    duplicate.namespace = "fixture/duplicate".into();
    duplicate
        .bindings
        .namespace
        .clone_from(&duplicate.namespace);
    duplicate
        .bindings
        .endpoint_namespace
        .clone_from(&duplicate.namespace);
    plan.nodes.push(duplicate);
    Ok((root, plan))
}

#[test]
fn real_cargo_groups_exact_bins_and_reuses_cache_across_staging_roots() -> Result<()> {
    let (root, plan) = cargo_plan()?;
    let cache = tempfile::tempdir()?;
    let recorder = Recorder::default();
    let first = plan.prepare_with(&PrepareOptions {
        observer: Some(&recorder),
        build_cache: Some(cache.path().into()),
        jobs: 2,
        ..PrepareOptions::default()
    })?;
    recorder.complete(PrepareStatus::Success)?;
    assert_eq!(
        recorder
            .events()
            .iter()
            .filter(|event| matches!(
                event,
                PrepareEvent::Group {
                    kind: GroupKind::Cargo,
                    state: GroupState::Running,
                    ..
                }
            ))
            .count(),
        1
    );
    assert_eq!(first.nodes[0].executable, first.nodes[1].executable);
    let other_root = tempfile::tempdir()?;
    let mut second_plan = plan.clone();
    second_plan.root = other_root.path().to_owned();
    let recorder = Recorder::default();
    let second = second_plan.prepare_with(&PrepareOptions {
        observer: Some(&recorder),
        build_cache: Some(cache.path().into()),
        ..PrepareOptions::default()
    })?;
    recorder.complete(PrepareStatus::Success)?;
    assert!(recorder.events().iter().any(|event| matches!(
        event,
        PrepareEvent::Group {
            kind: GroupKind::Cargo,
            state: GroupState::Cached,
            ..
        }
    )));
    assert!(
        recorder
            .events()
            .iter()
            .filter_map(
                |event| if let PrepareEvent::Compiled { fresh, .. } = event {
                    Some(*fresh)
                } else {
                    None
                }
            )
            .all(|fresh| fresh)
    );
    assert_ne!(first.nodes[0].executable, second.nodes[0].executable);
    fs::write(
        root.path().join("src/bin/app.rs"),
        "fn main() { println!(\"second\"); }\n",
    )?;
    let third = plan.prepare_with(&PrepareOptions {
        build_cache: Some(cache.path().into()),
        ..PrepareOptions::default()
    })?;
    for (prepared, expected) in [
        (&first, b"first\n".as_slice()),
        (&second, b"first\n".as_slice()),
        (&third, b"second\n".as_slice()),
    ] {
        let output = Command::new(&prepared.nodes[0].executable).output()?;
        assert!(output.status.success());
        assert_eq!(output.stdout, expected);
    }
    fs::write(
        root.path().join("src/bin/app.rs"),
        "compile_error!(\"no stale fallback\"); fn main() {}\n",
    )?;
    let recorder = Recorder::default();
    assert!(
        plan.prepare_with(&PrepareOptions {
            observer: Some(&recorder),
            build_cache: Some(cache.path().into()),
            ..PrepareOptions::default()
        })
        .is_err()
    );
    recorder.complete(PrepareStatus::Failed)?;
    first.verify()?;
    third.verify()?;
    Ok(())
}

#[test]
fn incompatible_cargo_settings_never_merge_and_queued_cancel_never_starts() -> Result<()> {
    let (root, plan) = cargo_plan()?;
    fs::create_dir(root.path().join("other-cwd"))?;
    for change in 0..7 {
        let mut plan = plan.clone();
        let Some(BuildSpec::Cargo(build)) = &mut plan.nodes[1].build else {
            anyhow::bail!("Cargo fixture")
        };
        match change {
            0 => build.features = vec!["variant".into()],
            1 => build.default_features = false,
            2 => build.locked = false,
            3 => build.package = Some("another-package".into()),
            4 => build.target = Some("another-target".into()),
            5 => {
                build
                    .env
                    .insert("RUSTFLAGS".into(), "-C debuginfo=0".into());
            }
            _ => build.cwd = Some(root.path().join("other-cwd")),
        }
        let cancel = CancelToken::default();
        let recorder = Recorder {
            cancel_planned: Some(cancel.clone()),
            ..Recorder::default()
        };
        assert!(matches!(
            plan.prepare_with(&PrepareOptions {
                cancel,
                observer: Some(&recorder),
                jobs: 2,
                ..PrepareOptions::default()
            }),
            Err(PrepareError::Cancelled)
        ));
        recorder.complete(PrepareStatus::Cancelled)?;
        assert_eq!(
            recorder
                .events()
                .iter()
                .filter(|event| matches!(
                    event,
                    PrepareEvent::Group {
                        kind: GroupKind::Cargo,
                        state: GroupState::Queued,
                        ..
                    }
                ))
                .count(),
            2
        );
        assert!(!recorder.events().iter().any(|event| matches!(
            event,
            PrepareEvent::Group {
                state: GroupState::Running,
                ..
            }
        )));
    }
    Ok(())
}

#[test]
fn distinct_cargo_feature_sets_produce_distinct_real_executables() -> Result<()> {
    let (root, mut plan) = cargo_plan()?;
    fs::write(
        root.path().join("src/bin/app.rs"),
        "fn main() { println!(\"{}\", if cfg!(feature = \"variant\") { \"variant\" } else { \"base\" }); }\n",
    )?;
    let Some(BuildSpec::Cargo(build)) = &mut plan.nodes[1].build else {
        anyhow::bail!("Cargo fixture")
    };
    build.features = vec!["variant".into(), "variant".into()];
    let recorder = Recorder::default();
    let prepared = plan.prepare_with(&PrepareOptions {
        observer: Some(&recorder),
        jobs: 2,
        build_cache: Some(std::env::temp_dir().join("b")),
        ..PrepareOptions::default()
    })?;
    recorder.complete(PrepareStatus::Success)?;
    assert_eq!(
        recorder
            .events()
            .iter()
            .filter(|event| matches!(
                event,
                PrepareEvent::Group {
                    kind: GroupKind::Cargo,
                    state: GroupState::Running,
                    ..
                }
            ))
            .count(),
        2
    );
    assert_ne!(prepared.nodes[0].executable, prepared.nodes[1].executable);
    for (node, expected) in prepared
        .nodes
        .iter()
        .zip([b"base\n".as_slice(), b"variant\n".as_slice()])
    {
        let output = Command::new(&node.executable).output()?;
        assert!(output.status.success());
        assert_eq!(output.stdout, expected);
    }
    Ok(())
}

#[test]
fn same_bin_from_multiple_workspace_packages_is_ambiguous() -> Result<()> {
    let (root, mut plan) = prebuilt()?;
    fs::write(
        root.path().join("Cargo.toml"),
        "[workspace]\nmembers = [\"a\", \"b\"]\nresolver = \"2\"\n",
    )?;
    fs::write(
        root.path().join("Cargo.lock"),
        "version = 4\n[[package]]\nname = \"a\"\nversion = \"0.0.0\"\n[[package]]\nname = \"b\"\nversion = \"0.0.0\"\n",
    )?;
    for package in ["a", "b"] {
        let directory = root.path().join(package);
        fs::create_dir(&directory)?;
        fs::write(
            directory.join("Cargo.toml"),
            format!(
                "[package]\nname = \"{package}\"\nversion = \"0.0.0\"\nedition = \"2021\"\n[[bin]]\nname = \"app\"\npath = \"main.rs\"\n"
            ),
        )?;
        fs::write(directory.join("main.rs"), "fn main() {}\n")?;
    }
    plan.nodes[0].run.companions.clear();
    plan.nodes[0].bindings.executables = vec!["app".into()];
    plan.nodes[0].build = Some(BuildSpec::Cargo(CargoBuild {
        profile: "dev".into(),
        ..CargoBuild::default()
    }));
    let recorder = Recorder::default();
    assert!(
        plan.prepare_with(&PrepareOptions {
            observer: Some(&recorder),
            build_cache: Some(std::env::temp_dir().join("b")),
            ..PrepareOptions::default()
        })
        .is_err()
    );
    recorder.complete(PrepareStatus::Failed)?;
    Ok(())
}

struct Builder {
    _directory: tempfile::TempDir,
    executable: PathBuf,
}

fn builder() -> Result<Arc<Builder>> {
    static BUILDER: Mutex<Option<Arc<Builder>>> = Mutex::new(None);
    let mut state = BUILDER
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(builder) = state.as_ref() {
        return Ok(Arc::clone(builder));
    }
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("builder.rs");
    let executable = directory
        .path()
        .join(format!("builder{}", std::env::consts::EXE_SUFFIX));
    fs::write(
        &source,
        r#"
use std::{env, fs, io::{Read,Write}, net::TcpStream, process::Command};
fn main() {
    let mode = env::args().nth(1).unwrap_or_default();
    if mode == "app" { println!("command application"); return; }
    if mode == "descendant" {
        let mut socket = TcpStream::connect(env::var("GATE").unwrap()).unwrap();
        writeln!(socket,"descendant {}",std::process::id()).unwrap();
        let mut byte = [0]; while socket.read_exact(&mut byte).is_ok() {}
        return;
    }
    let mut counter = fs::OpenOptions::new().create(true).append(true).open("invocations").unwrap();
    writeln!(counter,"run").unwrap();
    if mode == "fail" { std::process::exit(23); }
    if mode == "tree" || mode == "tree-fail" || mode == "tree-success" { Command::new(env::current_exe().unwrap()).arg("descendant").spawn().unwrap(); }
    if mode == "tree" || mode == "tree-fail" || mode == "tree-success" || mode == "gate" || mode == "fail-gate" {
        let mut socket = TcpStream::connect(env::var("GATE").unwrap()).unwrap();
        writeln!(socket,"{mode} {}",std::process::id()).unwrap();
        let mut byte = [0]; socket.read_exact(&mut byte).unwrap();
        if mode == "fail-gate" || mode == "tree-fail" { std::process::exit(23); }
    }
    if mode == "pipes" {
        for _ in 0..2000 { println!("{}", "o".repeat(128)); eprintln!("{}", "e".repeat(128)); }
    }
    fs::create_dir_all("out").unwrap();
    for name in ["app","helper"] {
        fs::copy(env::current_exe().unwrap(),format!("out/{name}{}",env::consts::EXE_SUFFIX)).unwrap();
    }
    fs::write("out/data.bin",b"declared data").unwrap();
}
"#,
    )?;
    let output = Command::new("rustc")
        .arg("--edition=2024")
        .arg(&source)
        .arg("-o")
        .arg(&executable)
        .output()?;
    ensure!(
        output.status.success(),
        "builder fixture rustc failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let builder = Arc::new(Builder {
        _directory: directory,
        executable,
    });
    *state = Some(Arc::clone(&builder));
    Ok(builder)
}

fn command_plan(mode: &str) -> Result<(tempfile::TempDir, DeploymentPlan, Arc<Builder>)> {
    let builder = builder()?;
    let (root, mut plan) = prebuilt()?;
    plan.nodes[0].build = Some(BuildSpec::Command(CommandBuild {
        argv: vec![
            builder.executable.to_string_lossy().into_owned(),
            mode.into(),
        ],
        cwd: root.path().into(),
        env: BTreeMap::new(),
        outputs: ["app", "helper"]
            .into_iter()
            .map(|name| {
                (
                    name.into(),
                    PathBuf::from(format!("out/{name}{}", std::env::consts::EXE_SUFFIX)),
                )
            })
            .collect(),
        files: BTreeMap::from([(
            PathBuf::from("data/table.bin"),
            PathBuf::from("out/data.bin"),
        )]),
    }));
    Ok((root, plan, builder))
}

#[test]
fn command_runs_each_prepare_drains_both_pipes_and_freezes_declared_files() -> Result<()> {
    let (root, mut plan, _builder) = command_plan("pipes")?;
    let mut duplicate = plan.nodes[0].clone();
    duplicate.namespace = "fixture/duplicate".into();
    duplicate
        .bindings
        .namespace
        .clone_from(&duplicate.namespace);
    duplicate
        .bindings
        .endpoint_namespace
        .clone_from(&duplicate.namespace);
    plan.nodes.push(duplicate);
    for _ in 0..2 {
        let recorder = Recorder::default();
        let prepared = plan.prepare_with(&PrepareOptions {
            observer: Some(&recorder),
            jobs: 3,
            ..PrepareOptions::default()
        })?;
        recorder.complete(PrepareStatus::Success)?;
        assert_eq!(
            recorder
                .events()
                .iter()
                .filter(|event| matches!(
                    event,
                    PrepareEvent::Group {
                        kind: GroupKind::Command,
                        state: GroupState::Running,
                        ..
                    }
                ))
                .count(),
            1
        );
        assert!(!recorder.events().iter().any(|event| matches!(
            event,
            PrepareEvent::Group {
                kind: GroupKind::Command,
                state: GroupState::Cached,
                ..
            }
        )));
        assert_eq!(
            fs::read(prepared.nodes[0].bundle_dir.join("data/table.bin"))?,
            b"declared data"
        );
        assert_eq!(
            Command::new(&prepared.nodes[0].executable)
                .arg("app")
                .output()?
                .stdout,
            b"command application\n"
        );
        fs::remove_dir_all(root.path().join("out"))?;
        prepared.verify()?;
    }
    assert_eq!(
        fs::read_to_string(root.path().join("invocations"))?
            .lines()
            .count(),
        2
    );
    Ok(())
}

fn connect_gate(listener: TcpListener, count: usize) -> mpsc::Receiver<Result<Vec<TcpStream>>> {
    let (send, receive) = mpsc::channel();
    std::thread::spawn(move || {
        let result = (|| {
            let mut streams = Vec::new();
            for _ in 0..count {
                let (mut stream, _) = listener.accept()?;
                stream.set_read_timeout(Some(DEADLINE))?;
                let mut line = String::new();
                BufReader::new(&mut stream).read_line(&mut line)?;
                ensure!(
                    line.split_whitespace()
                        .nth(1)
                        .context("fixture PID")?
                        .parse::<u32>()?
                        > 0,
                    "invalid PID"
                );
                streams.push((line.starts_with("descendant "), stream));
            }
            // Release descendants first: releasing a leader may immediately
            // terminate its children and close their control sockets.
            streams.sort_by_key(|(descendant, _)| !*descendant);
            Ok(streams.into_iter().map(|(_, stream)| stream).collect())
        })();
        let _ = send.send(result);
    });
    receive
}

#[test]
fn cancellation_reaps_command_and_grandchild_and_same_root_is_busy() -> Result<()> {
    let (root, mut plan, _builder) = command_plan("tree")?;
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let Some(BuildSpec::Command(build)) = &mut plan.nodes[0].build else {
        anyhow::bail!("command fixture")
    };
    build
        .env
        .insert("GATE".into(), listener.local_addr()?.to_string());
    let gate = connect_gate(listener, 2);
    let cancel = CancelToken::default();
    let recorder = Arc::new(Recorder::default());
    let worker = {
        let plan = plan.clone();
        let cancel = cancel.clone();
        let recorder = Arc::clone(&recorder);
        std::thread::spawn(move || {
            plan.prepare_with(&PrepareOptions {
                cancel,
                observer: Some(recorder.as_ref()),
                ..PrepareOptions::default()
            })
        })
    };
    let streams = gate.recv_timeout(DEADLINE)??;
    let busy_events = Recorder::default();
    assert!(matches!(
        plan.prepare_with(&PrepareOptions {
            observer: Some(&busy_events),
            ..PrepareOptions::default()
        }),
        Err(PrepareError::Busy)
    ));
    busy_events.complete(PrepareStatus::Failed)?;
    cancel.cancel();
    let result = worker
        .join()
        .map_err(|_| anyhow::anyhow!("preparation panicked"))?;
    assert!(matches!(result, Err(PrepareError::Cancelled)), "{result:?}");
    recorder.complete(PrepareStatus::Cancelled)?;
    for mut stream in streams {
        let mut byte = [0];
        match stream.read(&mut byte) {
            Ok(0) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
                ) => {}
            other => anyhow::bail!("descendant remained connected after cancellation: {other:?}"),
        }
    }
    assert!(!root.path().join("out").exists());
    Ok(())
}

#[test]
fn declared_command_errors_never_return_stale_outputs() -> Result<()> {
    let (root, plan, _builder) = command_plan("normal")?;
    let first = plan.prepare()?;
    for change in 0..6 {
        let mut broken = plan.clone();
        let Some(BuildSpec::Command(build)) = &mut broken.nodes[0].build else {
            anyhow::bail!("command fixture")
        };
        match change {
            0 => build.argv[1] = "fail".into(),
            1 => {
                build.outputs.remove("helper");
            }
            2 => {
                build.outputs.insert("app".into(), "missing".into());
            }
            3 => {
                build.outputs.insert("app".into(), "out".into());
            }
            4 => {
                build
                    .files
                    .insert("../escape".into(), "out/data.bin".into());
            }
            _ => {
                build.outputs.insert("APP".into(), "out/app".into());
            }
        }
        let recorder = Recorder::default();
        assert!(
            broken
                .prepare_with(&PrepareOptions {
                    observer: Some(&recorder),
                    ..PrepareOptions::default()
                })
                .is_err()
        );
        recorder.complete(PrepareStatus::Failed)?;
        first.verify()?;
    }
    assert!(root.path().join("invocations").exists());
    for yaml in [
        "type: command\nargv: [tool]\noutputs: {app: a}\nunknown: true\n",
        "type: command\nargv: [tool]\noutputs: {app: a, app: b}\n",
        "type: cargo\nunknown: true\n",
    ] {
        assert!(serde_yaml::from_str::<BuildSpec>(yaml).is_err());
    }
    Ok(())
}

#[test]
fn cancellation_while_waiting_for_source_lock_is_responsive() -> Result<()> {
    let (root, _plan) = prebuilt()?;
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.path().join(".reiny/source.lock"))?;
    lock.lock()?;
    let cancel = CancelToken::default();
    let (entered, receive) = mpsc::channel();
    let (finished, result) = mpsc::channel();
    let path = root.path().to_owned();
    let token = cancel.clone();
    let worker = std::thread::spawn(move || {
        let _ = entered.send(());
        let _ = finished.send(DeploymentPlan::load_with(&path, false, &token));
    });
    receive.recv_timeout(DEADLINE)?;
    cancel.cancel();
    assert!(matches!(
        result.recv_timeout(DEADLINE)?,
        Err(PrepareError::Cancelled)
    ));
    worker
        .join()
        .map_err(|_| anyhow::anyhow!("resolver panicked"))?;
    lock.unlock()?;
    Ok(())
}

#[test]
fn publication_and_cancel_share_one_terminal_decision() -> Result<()> {
    let cancel = CancelToken::default();
    let (published, receive) = mpsc::channel();
    assert_eq!(
        cancel.publish(|| {
            published.send(7)?;
            Ok(7)
        })?,
        7
    );
    assert_eq!(receive.recv_timeout(DEADLINE)?, 7);
    cancel.cancel();
    assert!(matches!(
        cancel.publish(|| Ok(())),
        Err(PrepareError::Cancelled)
    ));
    Ok(())
}

#[test]
fn bounded_jobs_run_independent_commands_but_serialize_shared_cwd() -> Result<()> {
    for shared_cwd in [false, true] {
        let (root, mut plan, _builder) = command_plan("gate")?;
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let address = listener.local_addr()?.to_string();
        let count = if shared_cwd { 2 } else { 3 };
        let template = plan.nodes[0].clone();
        plan.nodes.clear();
        for index in 0..count {
            let mut node = template.clone();
            node.namespace = format!("fixture/node{index}");
            node.bindings.namespace.clone_from(&node.namespace);
            node.bindings.endpoint_namespace.clone_from(&node.namespace);
            let Some(BuildSpec::Command(build)) = &mut node.build else {
                anyhow::bail!("command fixture")
            };
            build.env.insert("GATE".into(), address.clone());
            build.env.insert("INSTANCE".into(), index.to_string());
            if !shared_cwd {
                build.cwd = root.path().join(format!("cwd{index}"));
                fs::create_dir(&build.cwd)?;
            }
            plan.nodes.push(node);
        }
        let (accepted, sockets) = mpsc::channel();
        let acceptor = std::thread::spawn(move || -> Result<()> {
            for _ in 0..count {
                let (mut socket, _) = listener.accept()?;
                socket.set_read_timeout(Some(DEADLINE))?;
                let mut ready = String::new();
                BufReader::new(&mut socket).read_line(&mut ready)?;
                accepted.send(socket)?;
            }
            Ok(())
        });
        let (running, started) = mpsc::channel();
        let recorder = Arc::new(Recorder {
            running: Some(running),
            ..Recorder::default()
        });
        let (done, result) = mpsc::channel();
        let worker = {
            let recorder = Arc::clone(&recorder);
            std::thread::spawn(move || {
                let _ = done.send(plan.prepare_with(&PrepareOptions {
                    jobs: 2,
                    observer: Some(recorder.as_ref()),
                    ..PrepareOptions::default()
                }));
            })
        };
        // Both scheduler workers have taken a group, but shared cwd has only
        // one process. Independent cwd fills exactly the configured job bound.
        started.recv_timeout(DEADLINE)?;
        started.recv_timeout(DEADLINE)?;
        let mut first = sockets.recv_timeout(DEADLINE)?;
        if shared_cwd {
            assert_eq!(
                fs::read_to_string(root.path().join("invocations"))?
                    .lines()
                    .count(),
                1
            );
            first.write_all(b"g")?;
            sockets.recv_timeout(DEADLINE)?.write_all(b"g")?;
        } else {
            let mut second = sockets.recv_timeout(DEADLINE)?;
            assert_eq!(
                recorder
                    .events()
                    .iter()
                    .filter(|event| matches!(
                        event,
                        PrepareEvent::Group {
                            state: GroupState::Running,
                            ..
                        }
                    ))
                    .count(),
                2
            );
            first.write_all(b"g")?;
            let mut third = sockets.recv_timeout(DEADLINE)?;
            second.write_all(b"g")?;
            third.write_all(b"g")?;
        }
        result.recv_timeout(DEADLINE)??.verify()?;
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("preparation panicked"))?;
        acceptor
            .join()
            .map_err(|_| anyhow::anyhow!("acceptor panicked"))??;
        recorder.complete(PrepareStatus::Success)?;
    }
    Ok(())
}

#[test]
fn cancellation_reaps_a_live_git_fetch_and_does_not_publish_pins() -> Result<()> {
    let root = tempfile::tempdir()?;
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    fs::write(
        root.path().join("main.yaml"),
        format!(
            "version: 3\ndeployment: acquire\nmodules:\n  remote:\n    source: {{git: 'http://{}/repository', ref: main}}\n",
            listener.local_addr()?
        ),
    )?;
    let (request, incoming) = mpsc::channel();
    let acceptor = std::thread::spawn(move || -> Result<()> {
        let (mut socket, _) = listener.accept()?;
        socket.set_read_timeout(Some(DEADLINE))?;
        let mut line = String::new();
        let mut reader = BufReader::new(&mut socket);
        loop {
            line.clear();
            ensure!(
                reader.read_line(&mut line)? > 0,
                "Git disconnected before request"
            );
            if line == "\r\n" {
                break;
            }
        }
        request.send(socket)?;
        Ok(())
    });
    let cancel = CancelToken::default();
    let (done, finished) = mpsc::channel();
    let worker = {
        let token = cancel.clone();
        let path = root.path().to_owned();
        std::thread::spawn(move || {
            let _ = done.send(DeploymentPlan::load_with(&path, false, &token));
        })
    };
    let mut socket = incoming.recv_timeout(DEADLINE)?;
    cancel.cancel();
    assert!(matches!(
        finished.recv_timeout(DEADLINE)?,
        Err(PrepareError::Cancelled)
    ));
    worker
        .join()
        .map_err(|_| anyhow::anyhow!("resolver panicked"))?;
    acceptor
        .join()
        .map_err(|_| anyhow::anyhow!("acceptor panicked"))??;
    let mut byte = [0];
    match socket.read(&mut byte) {
        Ok(0) => {}
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
            ) => {}
        other => anyhow::bail!("Git descendant retained its connection: {other:?}"),
    }
    assert!(!root.path().join("lock.yaml").exists());
    Ok(())
}

#[test]
fn owner_stop_at_rendezvous_prevents_configuration_and_reconciliation() -> Result<()> {
    struct NoStartup;
    impl reiny_launch::ModuleObserver for NoStartup {
        fn configure(&self, _: &PreparedDeployment) -> Result<()> {
            anyhow::bail!("configuration must not run after stop")
        }
        fn ready(&self, _: &str) -> Result<bool> {
            anyhow::bail!("no application may start")
        }
        fn request_stop(&self, _: &str) -> Result<()> {
            anyhow::bail!("no application exists")
        }
    }
    let (_root, plan) = prebuilt()?;
    let prepared = plan.prepare()?;
    let stop = std::sync::atomic::AtomicBool::new(false);
    reiny_launch::serve(prepared, &NoStartup, &stop, &|| {
        stop.store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    })?;
    Ok(())
}

#[test]
fn cancelling_cargo_reaps_build_script_and_its_descendants() -> Result<()> {
    let (root, mut plan) = cargo_plan()?;
    let builder = builder()?;
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let address = listener.local_addr()?.to_string();
    plan.nodes.truncate(1);
    let Some(BuildSpec::Cargo(build)) = &mut plan.nodes[0].build else {
        anyhow::bail!("Cargo fixture")
    };
    build.env.insert("GATE".into(), address);
    build.env.insert(
        "BUILDER".into(),
        builder.executable.to_string_lossy().into_owned(),
    );
    fs::write(
        root.path().join("build.rs"),
        r#"
fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let status = std::process::Command::new(std::env::var("BUILDER").unwrap())
        .arg("tree").status().unwrap();
    assert!(status.success());
}
"#,
    )?;
    let gate = connect_gate(listener, 2);
    let cancel = CancelToken::default();
    let recorder = Arc::new(Recorder::default());
    let (done, finished) = mpsc::channel();
    let worker = {
        let token = cancel.clone();
        let recorder = Arc::clone(&recorder);
        std::thread::spawn(move || {
            let _ = done.send(plan.prepare_with(&PrepareOptions {
                cancel: token,
                observer: Some(recorder.as_ref()),
                // MSVC object paths for build scripts need the same short
                // persistent compiler base that source-mode callers supply.
                build_cache: Some(std::env::temp_dir().join("b")),
                ..PrepareOptions::default()
            }));
        })
    };
    let streams = gate.recv_timeout(DEADLINE).with_context(|| {
        format!(
            "Cargo build-script gate did not open; events: {:?}",
            recorder.events()
        )
    })??;
    cancel.cancel();
    assert!(matches!(
        finished
            .recv_timeout(DEADLINE)
            .context("Cargo tree did not finish after cancellation")?,
        Err(PrepareError::Cancelled)
    ));
    worker
        .join()
        .map_err(|_| anyhow::anyhow!("preparer panicked"))?;
    recorder.complete(PrepareStatus::Cancelled)?;
    for stream in streams {
        closed(stream)?;
    }
    Ok(())
}

fn closed(mut stream: TcpStream) -> Result<()> {
    let mut byte = [0];
    match stream.read(&mut byte) {
        Ok(0) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
            ) =>
        {
            Ok(())
        }
        other => anyhow::bail!("descendant retained a live connection: {other:?}"),
    }
}

#[test]
fn failed_parallel_builder_reaps_other_groups_without_cancelling_the_caller() -> Result<()> {
    let (root, mut plan, _builder) = command_plan("tree")?;
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let address = listener.local_addr()?.to_string();
    let Some(BuildSpec::Command(build)) = &mut plan.nodes[0].build else {
        anyhow::bail!("command fixture")
    };
    build.env.insert("GATE".into(), address.clone());
    let mut failing = plan.nodes[0].clone();
    failing.namespace = "fixture/failing".into();
    failing.bindings.namespace.clone_from(&failing.namespace);
    failing
        .bindings
        .endpoint_namespace
        .clone_from(&failing.namespace);
    let Some(BuildSpec::Command(build)) = &mut failing.build else {
        anyhow::bail!("command fixture")
    };
    build.argv[1] = "fail-gate".into();
    build.cwd = root.path().join("failing");
    fs::create_dir(&build.cwd)?;
    plan.nodes.push(failing);
    let (accepted, incoming) = mpsc::channel();
    let acceptor = std::thread::spawn(move || -> Result<()> {
        for _ in 0..3 {
            let (mut stream, _) = listener.accept()?;
            stream.set_read_timeout(Some(DEADLINE))?;
            let mut ready = String::new();
            BufReader::new(&mut stream).read_line(&mut ready)?;
            accepted.send((ready.starts_with("fail-gate "), stream))?;
        }
        Ok(())
    });
    let cancel = CancelToken::default();
    let recorder = Arc::new(Recorder::default());
    let (done, finished) = mpsc::channel();
    let worker = {
        let token = cancel.clone();
        let recorder = Arc::clone(&recorder);
        std::thread::spawn(move || {
            let _ = done.send(plan.prepare_with(&PrepareOptions {
                cancel: token,
                observer: Some(recorder.as_ref()),
                jobs: 2,
                ..PrepareOptions::default()
            }));
        })
    };
    let mut streams = Vec::new();
    for _ in 0..3 {
        streams.push(incoming.recv_timeout(DEADLINE)?);
    }
    for (failing, stream) in &mut streams {
        if *failing {
            stream.write_all(b"f")?;
        }
    }
    assert!(matches!(
        finished.recv_timeout(DEADLINE)?,
        Err(PrepareError::Failed(_))
    ));
    assert!(!cancel.is_cancelled());
    worker
        .join()
        .map_err(|_| anyhow::anyhow!("preparer panicked"))?;
    acceptor
        .join()
        .map_err(|_| anyhow::anyhow!("acceptor panicked"))??;
    recorder.complete(PrepareStatus::Failed)?;
    for (_, stream) in streams {
        closed(stream)?;
    }
    Ok(())
}

#[test]
fn failed_leader_reaps_a_descendant_that_keeps_diagnostic_pipes_open() -> Result<()> {
    let (_root, mut plan, _builder) = command_plan("tree-fail")?;
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let Some(BuildSpec::Command(build)) = &mut plan.nodes[0].build else {
        anyhow::bail!("command fixture")
    };
    build
        .env
        .insert("GATE".into(), listener.local_addr()?.to_string());
    let gate = connect_gate(listener, 2);
    let (done, finished) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let _ = done.send(plan.prepare());
    });
    let mut streams = gate.recv_timeout(DEADLINE)??;
    // The descendant deliberately keeps reading after this byte. Only its
    // leader exits, leaving the inherited stdout/stderr handles open.
    for stream in &mut streams {
        stream.write_all(b"f")?;
    }
    assert!(finished.recv_timeout(DEADLINE)?.is_err());
    worker
        .join()
        .map_err(|_| anyhow::anyhow!("preparer panicked"))?;
    for stream in streams {
        closed(stream)?;
    }
    Ok(())
}

#[test]
fn successful_leader_reaps_a_descendant_that_keeps_diagnostic_pipes_open() -> Result<()> {
    let (_root, mut plan, _builder) = command_plan("tree-success")?;
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let Some(BuildSpec::Command(build)) = &mut plan.nodes[0].build else {
        anyhow::bail!("command fixture")
    };
    build
        .env
        .insert("GATE".into(), listener.local_addr()?.to_string());
    let gate = connect_gate(listener, 2);
    let cancel = CancelToken::default();
    let (done, finished) = mpsc::channel();
    let worker = {
        let token = cancel.clone();
        std::thread::spawn(move || {
            let _ = done.send(plan.prepare_with(&PrepareOptions {
                cancel: token,
                ..PrepareOptions::default()
            }));
        })
    };
    let mut streams = gate.recv_timeout(DEADLINE)??;
    for stream in &mut streams {
        stream.write_all(b"f")?;
    }
    let outcome = finished.recv_timeout(DEADLINE);
    let cleanup = if outcome.is_err() {
        cancel.cancel();
        Some(finished.recv_timeout(DEADLINE)?)
    } else {
        None
    };
    worker
        .join()
        .map_err(|_| anyhow::anyhow!("preparer panicked"))?;
    for stream in streams {
        closed(stream)?;
    }
    if let Some(cleanup) = cleanup {
        assert!(matches!(cleanup, Err(PrepareError::Cancelled)));
    }
    let prepared = outcome.context("successful builder retained its background descendant")??;
    prepared.verify()?;
    Ok(())
}

#[test]
fn build_lock_contention_cancels_without_touching_another_transactions_partials() -> Result<()> {
    let (root, plan, _builder) = command_plan("normal")?;
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.path().join(".reiny/command.lock"))?;
    lock.lock()?;
    let foreign = root.path().join(".reiny/build/.bundle-another-transaction");
    fs::create_dir_all(&foreign)?;
    fs::write(foreign.join("sentinel"), b"owned by another call")?;
    let (running, started) = mpsc::channel();
    let recorder = Arc::new(Recorder {
        running: Some(running),
        ..Recorder::default()
    });
    let cancel = CancelToken::default();
    let (done, finished) = mpsc::channel();
    let worker = {
        let recorder = Arc::clone(&recorder);
        let token = cancel.clone();
        std::thread::spawn(move || {
            let _ = done.send(plan.prepare_with(&PrepareOptions {
                cancel: token,
                observer: Some(recorder.as_ref()),
                ..PrepareOptions::default()
            }));
        })
    };
    started.recv_timeout(DEADLINE)?;
    cancel.cancel();
    assert!(matches!(
        finished.recv_timeout(DEADLINE)?,
        Err(PrepareError::Cancelled)
    ));
    worker
        .join()
        .map_err(|_| anyhow::anyhow!("preparer panicked"))?;
    recorder.complete(PrepareStatus::Cancelled)?;
    lock.unlock()?;
    assert!(!root.path().join("invocations").exists());
    assert_eq!(
        fs::read(foreign.join("sentinel"))?,
        b"owned by another call"
    );
    Ok(())
}
