//! A real C executable, inherited bootstrap, authenticated loopback, and child reap.
#![cfg(target_os = "linux")]
#![cfg(not(all(miri, feature = "component-ffi-skip-miri")))]

use std::process::Stdio;
use std::time::Duration;

use reiny::component::process::{Bootstrap, encode_payload, protocol};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

const BUDGET: Duration = Duration::from_secs(15);

#[path = "component_process/owner.rs"]
mod owner;
use owner::Owner;

fn native_library() -> anyhow::Result<std::path::PathBuf> {
    let library = match std::env::var_os("REINY_FFI_TEST_LIBRARY") {
        Some(path) => std::path::PathBuf::from(path),
        None => std::env::current_exe()?.with_file_name("libreiny_ffi.so"),
    };
    Ok(library.canonicalize()?)
}

async fn verify_loaded_library(child_id: u32) -> anyhow::Result<()> {
    let library = native_library()?;
    let maps = tokio::fs::read_to_string(format!("/proc/{child_id}/maps")).await?;
    let suffix = format!(" {}", library.display());
    anyhow::ensure!(
        maps.lines().any(|line| line.ends_with(&suffix)),
        "C child did not map the exact feature-matched library {}",
        library.display()
    );
    let hash = std::process::Command::new("sha256sum")
        .arg(&library)
        .output()?;
    anyhow::ensure!(hash.status.success(), "native library hash failed");
    eprintln!(
        "C_PROCESS_LIBRARY {}",
        String::from_utf8(hash.stdout)?.trim()
    );
    Ok(())
}

fn compile(directory: &std::path::Path) -> anyhow::Result<std::path::PathBuf> {
    let crate_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let library = native_library()?;
    let client = directory.join("component-process");
    let output = std::process::Command::new("cc")
        .args(["-std=c11", "-Wall", "-Wextra", "-Werror", "-pthread"])
        .arg("-I")
        .arg(crate_dir.join("include"))
        .arg(crate_dir.join("tests/component_process.c"))
        // Cargo puts target/debug before deps in LD_LIBRARY_PATH. An absolute
        // DT_NEEDED path prevents a prior no-default-features build being used.
        .arg(library)
        .arg("-o")
        .arg(&client)
        .output()?;
    anyhow::ensure!(
        output.status.success(),
        "C process compile failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(client)
}

async fn exercise(client: &std::path::Path, premature_run: bool) -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let identity = protocol::SessionIdentity {
        owner_generation: protocol::OwnerGeneration(1),
        graph_generation: protocol::GraphGeneration(2),
        host_id: protocol::HostId("foreign-test".into()),
        host_generation: protocol::HostGeneration(3),
        graph_digest: protocol::PreparedDigest("graph".into()),
        bundle_digest: protocol::PreparedDigest("bundle".into()),
    };
    let nonce = protocol::Nonce::new([17; 32]);
    let bootstrap = Bootstrap {
        address: listener.local_addr()?,
        identity: identity.clone(),
        nonce: nonce.clone(),
    };
    let mut child = tokio::process::Command::new(client)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let child_id = child
        .id()
        .ok_or_else(|| anyhow::anyhow!("child has no pid"))?;
    assert_ne!(child_id, std::process::id());
    let mut stdout = BufReader::new(
        child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("no stdout pipe"))?,
    );
    let mut transcript = String::new();
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("no bootstrap pipe"))?;
    let payload = encode_payload(&bootstrap)?;
    stdin.write_all(&protocol::frame_header(&payload)?).await?;
    stdin.write_all(&payload).await?;
    drop(stdin);
    let (stream, _) = tokio::time::timeout(BUDGET, listener.accept()).await??;
    let mut owner = Owner {
        stream,
        identity,
        sequence: 0,
        peer: None,
    };
    owner.prepare(&nonce).await?;
    verify_loaded_library(child_id).await?;
    if !premature_run {
        owner.send(protocol::OwnerMessage::Commit {}).await?;
        assert_eq!(owner.receive().await?, protocol::HostMessage::Committed {});
    }
    owner.send(protocol::OwnerMessage::Run {}).await?;
    if !premature_run {
        assert_eq!(owner.receive().await?, protocol::HostMessage::Running {});
        // Running acknowledges admission, not the first foreign callback poll.
        // Observe that exact callback event before requesting cancellation.
        for expected in ["created\n", "prepared\n", "running\n"] {
            let mut line = String::new();
            tokio::time::timeout(BUDGET, stdout.read_line(&mut line)).await??;
            assert_eq!(line, expected);
            transcript.push_str(&line);
        }
        owner
            .send(protocol::OwnerMessage::Stop {
                reason: protocol::StopReason::Requested,
                remaining_budget: protocol::Milliseconds(1000),
            })
            .await?;
    }
    let outcome = loop {
        match owner.receive().await? {
            protocol::HostMessage::Failure(_) => {}
            protocol::HostMessage::Outcome(outcome) => break outcome,
            message => anyhow::bail!("unexpected terminal message: {message:?}"),
        }
    };
    if premature_run {
        assert!(matches!(
            outcome,
            protocol::HostOutcome::Failed {
                primary: protocol::HostFailure {
                    code: protocol::FailureCode::Transition,
                    ..
                },
                ..
            }
        ));
    } else {
        assert_eq!(outcome, protocol::HostOutcome::Stopped {});
    }
    let output = tokio::time::timeout(BUDGET, child.wait_with_output()).await??;
    anyhow::ensure!(
        output.status.success(),
        "foreign process failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    tokio::time::timeout(BUDGET, stdout.read_to_string(&mut transcript)).await??;
    check_transcript(&transcript, premature_run, &outcome)
}

fn check_transcript(
    transcript: &str,
    premature_run: bool,
    outcome: &protocol::HostOutcome,
) -> anyhow::Result<()> {
    let mut lines = transcript.lines();
    assert_eq!(lines.next(), Some("created"));
    assert_eq!(lines.next(), Some("prepared"));
    if !premature_run {
        assert_eq!(lines.next(), Some("running"));
    }
    assert_eq!(lines.next(), Some("stopped"));
    assert_eq!(lines.next(), Some("destroyed"));
    let json = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("no outcome JSON"))?;
    let copied: protocol::HostOutcome = serde_json::from_str(json)?;
    assert_eq!(&copied, outcome);
    assert_eq!(lines.next(), None);
    Ok(())
}

#[tokio::test]
async fn foreign_process_authenticates_admits_and_reaps() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let client = compile(directory.path())?;
    exercise(&client, false).await
}

#[tokio::test]
async fn foreign_process_rejects_run_before_commit_without_callback_effect() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let client = compile(directory.path())?;
    exercise(&client, true).await
}
