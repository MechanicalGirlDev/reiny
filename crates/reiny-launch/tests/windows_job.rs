//! Native process handles prove that closed pipes do not imply tree completion.
#![cfg(windows)]

use std::future::poll_fn;
use std::io::{Read, Write};
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::process::Stdio;
use std::task::Poll;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use process_wrap::tokio::{ChildWrapper, CommandWrap};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use windows_sys::Win32::{
    Foundation::{HANDLE_FLAG_INHERIT, SetHandleInformation, WAIT_OBJECT_0, WAIT_TIMEOUT},
    System::Threading::{OpenProcess, SYNCHRONIZATION_SYNCHRONIZE, WaitForSingleObject},
};

const DEADLINE: Duration = Duration::from_secs(30);
const ROLE: &str = "REINY_WINDOWS_TREE_FIXTURE";
const ADDRESS: &str = "REINY_WINDOWS_TREE_ADDRESS";

#[test]
fn fixture_process() -> Result<()> {
    let Ok(role) = std::env::var(ROLE) else {
        return Ok(());
    };
    let address = std::env::var(ADDRESS)?;
    let mut socket = std::net::TcpStream::connect(&address)?;
    socket.set_read_timeout(Some(DEADLINE))?;
    let _descendant = if role == "leader" {
        for handle in [
            std::io::stdout().as_raw_handle(),
            std::io::stderr().as_raw_handle(),
        ] {
            // SAFETY: these are the leader's valid standard handles. Prevent
            // unrelated inherited copies from keeping its pipes open in the child.
            let changed = unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0) };
            ensure!(
                changed != 0,
                "clear pipe inheritance: {}",
                std::io::Error::last_os_error()
            );
        }
        Some(
            std::process::Command::new(std::env::current_exe()?)
                .args(["--exact", "fixture_process", "--nocapture"])
                .env(ROLE, "descendant")
                .env(ADDRESS, &address)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()?,
        )
    } else {
        None
    };
    writeln!(socket, "{role} {}", std::process::id())?;
    let mut byte = [0];
    socket.read_exact(&mut byte)?;
    Ok(())
}

struct Peer {
    role: String,
    pid: u32,
    handle: OwnedHandle,
    socket: tokio::net::TcpStream,
}

impl Peer {
    async fn accept(listener: &tokio::net::TcpListener) -> Result<Self> {
        let (socket, _) = tokio::time::timeout(DEADLINE, listener.accept()).await??;
        let mut socket = BufReader::new(socket);
        let mut line = String::new();
        tokio::time::timeout(DEADLINE, socket.read_line(&mut line)).await??;
        let (role, pid) = line.trim().split_once(' ').context("fixture identity")?;
        let pid = pid.parse()?;
        // SAFETY: the fixture remains gated on its socket until this test releases
        // it. The requested access only observes termination; the handle pins identity.
        let handle = unsafe { OpenProcess(SYNCHRONIZATION_SYNCHRONIZE, 0, pid) };
        ensure!(
            !handle.is_null(),
            "open fixture process: {}",
            std::io::Error::last_os_error()
        );
        // SAFETY: OpenProcess returned a new non-null owned handle above.
        let handle = unsafe { OwnedHandle::from_raw_handle(handle) };
        Ok(Self {
            role: role.to_owned(),
            pid,
            handle,
            socket: socket.into_inner(),
        })
    }

    fn state(&self, timeout: u32) -> u32 {
        // SAFETY: this process handle remains owned for the whole native wait.
        unsafe { WaitForSingleObject(self.handle.as_raw_handle(), timeout) }
    }
}

async fn tree() -> Result<(Box<dyn ChildWrapper>, Peer, Peer)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let mut command = tokio::process::Command::new(std::env::current_exe()?);
    command
        .args(["--exact", "fixture_process", "--nocapture"])
        .env(ROLE, "leader")
        .env(ADDRESS, listener.local_addr()?.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut command = CommandWrap::from(command);
    command.wrap(process_wrap::tokio::KillOnDrop);
    command.wrap(process_wrap::tokio::JobObject);
    command.wrap(reiny_launch::WindowsJobCompletion::default());
    let child = command.spawn()?;
    let first = Peer::accept(&listener).await?;
    let second = Peer::accept(&listener).await?;
    assert_eq!(first.state(0), WAIT_TIMEOUT);
    assert_eq!(second.state(0), WAIT_TIMEOUT);
    Ok((child, first, second))
}

#[test]
fn tree_wait_stays_pending_after_leader_and_pipes_close_then_reaps_on_cancel() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        // Given: both identities and native handles are captured while the
        // leader and its pipe-free descendant are blocked on separate sockets.
        let (mut child, mut first, mut second) = tree().await?;
        let mut stdout = child.stdout().take().context("fixture stdout")?;
        let mut stderr = child.stderr().take().context("fixture stderr")?;
        let (leader, descendant) = if first.role == "leader" {
            (&mut first, &mut second)
        } else {
            (&mut second, &mut first)
        };
        assert_eq!(descendant.role, "descendant");
        leader.socket.write_all(b"x").await?;
        // These owned wrappers expose the native leader beneath the job layer.
        let status = child.inner_mut().inner_mut().wait().await?;
        assert!(status.success());
        let (mut output, mut errors) = (Vec::new(), Vec::new());
        tokio::try_join!(
            stdout.read_to_end(&mut output),
            stderr.read_to_end(&mut errors)
        )?;
        assert_eq!(leader.state(0), WAIT_OBJECT_0);
        assert_eq!(descendant.state(0), WAIT_TIMEOUT);

        // When: poll completion with the descendant still gated, cancel that
        // wait future, then kill and resume waiting on the same owned child.
        let mut wait = child.wait();
        let premature = poll_fn(|context| Poll::Ready(wait.as_mut().poll(context))).await;
        drop(wait);
        child.start_kill()?;
        let reaped = child.wait().await?;
        let leader_after = leader.state(0);
        let descendant_after = descendant.state(0);
        // Even the deliberately failing old-dependency run cleans up by the
        // already captured handles, without assuming EOF or a PID means exit.
        assert_eq!(leader.state(30_000), WAIT_OBJECT_0);
        assert_eq!(descendant.state(30_000), WAIT_OBJECT_0);
        println!(
            "WINDOWS_TREE_RECEIPT leader={} handle={:?} descendant={} handle={:?} \
             premature={premature:?} leader_after={leader_after} descendant_after={descendant_after}",
            leader.pid,
            leader.handle.as_raw_handle(),
            descendant.pid,
            descendant.handle.as_raw_handle()
        );

        // Then: no terminal result precedes tree exit; a cancelled wait remains
        // resumable, and completion is repeatable without consuming another event.
        assert!(premature.is_pending(), "tree wait returned with a live descendant");
        assert_eq!(leader_after, WAIT_OBJECT_0);
        assert_eq!(descendant_after, WAIT_OBJECT_0);
        assert_eq!(child.wait().await?, reaped);
        assert_eq!(child.try_wait()?, Some(reaped));
        Ok(())
    })
}

#[test]
fn drop_joins_the_entire_tree_without_an_active_runtime() -> Result<()> {
    // Given: both live processes are gated and pinned by native handles.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (mut child, first, second) = runtime.block_on(tree())?;
    assert_eq!(child.try_wait()?, None);

    // When: drop the owning wrapper outside any entered Tokio runtime.
    drop(child);

    // Then: both handles are already signaled, not merely their pipes closed.
    assert_eq!(first.state(0), WAIT_OBJECT_0);
    assert_eq!(second.state(0), WAIT_OBJECT_0);
    println!(
        "WINDOWS_TREE_DROP first={} handle={:?} second={} handle={:?} both_signaled=true",
        first.pid,
        first.handle.as_raw_handle(),
        second.pid,
        second.handle.as_raw_handle()
    );
    Ok(())
}
