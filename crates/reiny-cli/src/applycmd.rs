//! Foreground owner lifetime, including bootstrap and its authenticated lease.

use std::io::Write;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use process_wrap::tokio::{ChildWrapper, CommandWrap};
use reiny_launch::{
    ApplyEvent, ApplyStatus, DeploymentClient, DeploymentPhase, DeploymentStatus,
    PreparedDeployment, TerminalSubscription,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

use crate::control::Control;
use crate::modulecmd::{OwnerReady, Rendezvous};
use crate::preparecmd::Protocol;

const STOP_DEADLINE: Duration = Duration::from_secs(30);

pub(crate) fn failed(protocol: &Protocol, error: anyhow::Error) -> Result<()> {
    protocol.emit(&ApplyEvent::ApplyStarted {
        deployment: String::new(),
    })?;
    finish(protocol, Err(error))
}

pub(crate) fn apply(
    prepared: &PreparedDeployment,
    detach: bool,
    controlled: bool,
    timeout: Duration,
    control: &Control,
    protocol: &Protocol,
) -> Result<()> {
    protocol.emit(&ApplyEvent::ApplyStarted {
        deployment: prepared.deployment.clone(),
    })?;
    let mut owner = None;
    let mut client = None;
    let mut stopped = false;
    let result = (|| {
        control.check()?;
        prepared.verify()?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let existing = interruptible(
            &runtime,
            control,
            || DeploymentClient::find(&prepared.root),
            || Ok(()),
        )?;
        if controlled {
            anyhow::ensure!(
                existing.is_none(),
                "controlled apply requires a new owner; stop the existing deployment first"
            );
        }
        client = existing;
        if let Some(existing) = &client {
            control.check()?;
            interruptible(
                &runtime,
                control,
                || existing.apply(prepared),
                || existing.stop().map(|_| ()),
            )?;
            protocol.emit(&ApplyEvent::OwnerListening {
                pid: existing.status()?.owner_pid,
            })?;
        } else {
            owner = Some(Owner::start(prepared, !detach, timeout, control, protocol)?);
            control.check()?;
            client = interruptible(
                &runtime,
                control,
                || DeploymentClient::find(&prepared.root),
                || {
                    if let Some(owner) = &owner {
                        owner.revoke()?;
                    }
                    Ok(())
                },
            )?;
        }
        let client = client
            .as_ref()
            .context("owner rendezvous completed without a control endpoint")?;
        let (ready, terminal) =
            observe_ready(client, detach, timeout, &runtime, control, protocol)?;
        if detach {
            control.publish(|| {
                if let Some(owner) = &mut owner {
                    owner.detach();
                }
                Ok(())
            })?;
            return Ok(());
        }
        let final_state = follow(
            client,
            ready,
            terminal,
            owner.as_ref(),
            &runtime,
            control,
            protocol,
        )?;
        stopped = final_state.phase == DeploymentPhase::Stopped;
        Ok(())
    })();
    // A controlled owner never survives any return path, even natural task completion.
    let mut result = result.map_err(|error| control.failure(error));
    if !detach || result.is_err() {
        let deadline = control.cancelled_at().unwrap_or_else(Instant::now) + STOP_DEADLINE;
        if !stopped
            && let Some(client) = &client
            && let Err(error) = client.stop()
            && owner.is_none()
            && result.is_ok()
        {
            result = Err(error.context("stopping deployment"));
        }
        if let Some(owner) = &mut owner
            && let Err(error) = owner.reap(deadline.saturating_duration_since(Instant::now()))
        {
            result = Err(error.context("reaping deployment owner"));
        }
    }
    result = match result {
        Ok(()) => control.check(),
        Err(error) => Err(control.failure(error)),
    };
    finish(protocol, result)
}

fn observe_ready(
    client: &DeploymentClient,
    detach: bool,
    timeout: Duration,
    runtime: &tokio::runtime::Runtime,
    control: &Control,
    protocol: &Protocol,
) -> Result<(DeploymentStatus, Option<TerminalSubscription>)> {
    // Register before Ready is exposed: another authenticated client may
    // immediately stop the owner and remove its listening endpoint.
    let terminal = if detach {
        None
    } else {
        Some(interruptible(
            runtime,
            control,
            || client.watch_terminal(),
            || client.stop().map(|_| ()),
        )?)
    };
    // Stop requests run beside blocking control requests, never behind readiness.
    let ready = interruptible(
        runtime,
        control,
        || client.wait_ready(timeout),
        || client.stop().map(|_| ()),
    )?;
    control.active(|| {
        protocol.emit(&ApplyEvent::Ready {
            status: ready.clone(),
        })
    })?;
    if !protocol.enabled() {
        println!("{}", serde_json::to_string_pretty(&ready)?);
    }
    Ok((ready, terminal))
}

fn follow(
    client: &DeploymentClient,
    mut state: DeploymentStatus,
    mut terminal: Option<TerminalSubscription>,
    owner: Option<&Owner>,
    runtime: &tokio::runtime::Runtime,
    control: &Control,
    protocol: &Protocol,
) -> Result<DeploymentStatus> {
    while !matches!(
        state.phase,
        DeploymentPhase::Stopped | DeploymentPhase::Completed
    ) {
        control.check()?;
        state = interruptible(
            runtime,
            control,
            || {
                // A long-poll deadline is not deployment failure; authenticate
                // current state before registering the next revision wait.
                client
                    .wait_for_revision(state.revision, STOP_DEADLINE)
                    .or_else(|_| client.status())
                    .or_else(|error| {
                        terminal
                            .take()
                            .context("foreground terminal subscription missing")?
                            .wait()
                            .with_context(|| {
                                format!(
                                    "observing shutdown after control request failed: {error:#}"
                                )
                            })
                    })
            },
            || {
                if let Some(owner) = owner {
                    owner.revoke()?;
                }
                client.stop().map(|_| ())
            },
        )?;
        protocol.emit(&ApplyEvent::Status {
            status: state.clone(),
        })?;
        match state.phase {
            DeploymentPhase::Stopped | DeploymentPhase::Completed => break,
            DeploymentPhase::Failed => {
                anyhow::bail!("{}", state.error.as_deref().unwrap_or("deployment failed"))
            }
            DeploymentPhase::Starting
            | DeploymentPhase::Ready
            | DeploymentPhase::Stopping
            | DeploymentPhase::Degraded
            | DeploymentPhase::Suspended => {}
        }
    }
    if !protocol.enabled() {
        println!("{}", serde_json::to_string_pretty(&state)?);
    }
    Ok(state)
}

fn finish(protocol: &Protocol, result: Result<()>) -> Result<()> {
    let (status, error) = match &result {
        Ok(()) => (ApplyStatus::Stopped, None),
        Err(error) => (
            if matches!(
                error.downcast_ref::<reiny_launch::PrepareError>(),
                Some(reiny_launch::PrepareError::Cancelled)
            ) {
                ApplyStatus::Cancelled
            } else {
                ApplyStatus::Failed
            },
            Some(format!("{error:#}")),
        ),
    };
    protocol.emit(&ApplyEvent::Finished { status, error })?;
    result
}

/// Keep the synchronous client on a scoped thread; cancellation closes the lease
/// and sends authenticated stop from the caller, waking pending owner requests.
fn interruptible<T: Send>(
    runtime: &tokio::runtime::Runtime,
    control: &Control,
    operation: impl FnOnce() -> Result<T> + Send,
    on_cancel: impl FnOnce() -> Result<()>,
) -> Result<T> {
    // The client request has its own bounded deadline. A cancellation wake is
    // observed immediately by the operation's caller through the returned result.
    std::thread::scope(|scope| {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        scope.spawn(|| {
            let result = operation();
            if sender.send(result).is_err() {
                tracing::debug!("cancelled owner request completed");
            }
        });
        runtime.block_on(async {
            tokio::select! {
                biased;
                () = control.cancelled() => {
                    if let Err(error) = on_cancel() {
                        eprintln!("cancelling owner request: {error:#}");
                    }
                    control.check().and_then(|()| anyhow::bail!("cancelled owner request"))
                },
                result = receiver => result.context("owner request thread exited")?,
            }
        })
    })
}

struct Owner {
    child: Box<dyn ChildWrapper>,
    runtime: tokio::runtime::Runtime,
    lease: Option<std::net::TcpStream>,
    prepared_path: PathBuf,
    reaped: bool,
}

impl Owner {
    fn start(
        prepared: &PreparedDeployment,
        retain_lease: bool,
        timeout: Duration,
        control: &Control,
        protocol: &Protocol,
    ) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let listener = runtime.block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))?;
        let rendezvous = Rendezvous {
            address: listener.local_addr()?,
            nonce: nonce()?,
            lease: retain_lease,
        };
        let directory = prepared.root.join(".reiny/prepared");
        std::fs::create_dir_all(&directory)?;
        let mut file = tempfile::Builder::new()
            .prefix("owner-")
            .suffix(".json")
            .tempfile_in(directory)?;
        serde_json::to_writer(&mut file, prepared)?;
        file.flush()?;
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(prepared.root.join(".reiny/owner.log"))?;
        let mut command = tokio::process::Command::new(std::env::current_exe()?);
        command
            .arg("__supervise")
            .arg(file.path())
            .env(
                "REINY_OWNER_RENDEZVOUS",
                serde_json::to_string(&rendezvous)?,
            )
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log);
        let mut command = CommandWrap::from(command);
        #[cfg(unix)]
        command.wrap(process_wrap::tokio::ProcessGroup::leader());
        if retain_lease {
            command.wrap(process_wrap::tokio::KillOnDrop);
            #[cfg(windows)]
            {
                command.wrap(process_wrap::tokio::JobObject);
                command.wrap(reiny_launch::WindowsJobCompletion::default());
            }
        }
        control.check()?;
        #[cfg(windows)]
        prevent_stdio_inheritance()?;
        let child = {
            let _entered = runtime.enter();
            command.spawn().context("starting deployment owner")?
        };
        let mut owner = Self {
            child,
            runtime,
            lease: None,
            prepared_path: file.path().to_owned(),
            reaped: false,
        };
        let pid = owner.child.id().context("owner process has no PID")?;
        let stream = owner.runtime.block_on(async {
            tokio::select! {
                biased;
                () = control.cancelled() => {
                    control.check()?;
                    anyhow::bail!("owner bootstrap cancelled")
                },
                ready = tokio::time::timeout(timeout, async {
                    let (stream, _) = listener.accept().await?;
                    let mut reader = BufReader::new(stream.take(4097));
                    let mut line = String::new();
                    reader.read_line(&mut line).await?;
                    anyhow::ensure!(line.len() <= 4096 && line.ends_with('\n'), "invalid owner rendezvous frame");
                    let ready: OwnerReady = serde_json::from_str(&line)?;
                    anyhow::ensure!(ready.nonce == rendezvous.nonce && ready.pid == pid, "invalid owner rendezvous");
                    control.check()?;
                    let mut stream = reader.into_inner().into_inner().into_std()?;
                    stream.set_nonblocking(false)?;
                    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
                    control.active(|| {
                        protocol.emit(&ApplyEvent::OwnerListening { pid })?;
                        stream.write_all(b"accepted\n")?;
                        Ok(())
                    })?;
                    Ok::<_, anyhow::Error>(stream)
                }) => ready.context("owner bootstrap timed out")?,
                exited = owner.child.inner_mut().inner_mut().wait() => Err(anyhow::anyhow!("owner exited before bootstrap: {}", exited?)),
            }
        }).with_context(|| format!("owner log: {}", prepared.root.join(".reiny/owner.log").display()))?;
        if retain_lease {
            owner.lease = Some(stream);
        }
        file.close()?;
        owner.prepared_path.clear();
        Ok(owner)
    }

    fn detach(&mut self) {
        self.reaped = true;
    }

    fn reap(&mut self, timeout: Duration) -> Result<()> {
        self.lease.take();
        match self.runtime.block_on(async {
            tokio::time::timeout(timeout, self.child.inner_mut().inner_mut().wait()).await
        }) {
            Ok(result) => {
                result.context("waiting for deployment owner")?;
            }
            Err(_) => return self.terminate(),
        }
        // The leader-only deadline above is not tree completion. Terminate any
        // stragglers and retain the completion wrapper through its final wait.
        self.terminate()
    }

    fn revoke(&self) -> Result<()> {
        if let Some(lease) = &self.lease {
            lease.shutdown(std::net::Shutdown::Both)?;
        }
        Ok(())
    }

    fn terminate(&mut self) -> Result<()> {
        let killed = match self.child.start_kill() {
            Ok(()) => Ok(()),
            #[cfg(unix)]
            Err(error) if error.raw_os_error() == Some(3) => Ok(()),
            Err(error) => Err(error).context("terminating owner process tree"),
        };
        let waited = self
            .runtime
            .block_on(self.child.wait())
            .context("reaping owner process tree");
        self.reaped = waited.is_ok();
        killed.and(waited.map(|_| ()))
    }
}

/// Keep the caller's protocol pipes out of the owner and all its descendants.
/// Rust's explicit child stdio is duplicated separately by `Command::spawn`.
#[cfg(windows)]
fn prevent_stdio_inheritance() -> Result<()> {
    use std::os::windows::io::{AsHandle, AsRawHandle};
    use windows_sys::Win32::Foundation::{HANDLE_FLAG_INHERIT, SetHandleInformation};

    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let stderr = std::io::stderr();
    for handle in [stdin.as_handle(), stdout.as_handle(), stderr.as_handle()] {
        // SAFETY: The standard-I/O object lends a live handle for this call.
        // SetHandleInformation changes flags only; it does not dereference Rust
        // memory, replace the handle, or transfer/close its ownership.
        let changed =
            unsafe { SetHandleInformation(handle.as_raw_handle(), HANDLE_FLAG_INHERIT, 0) };
        if changed == 0 {
            return Err(std::io::Error::last_os_error())
                .context("preventing inherited protocol handles");
        }
    }
    Ok(())
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.lease.take();
        if !self.reaped
            && let Err(error) = self.terminate()
        {
            eprintln!("owner cleanup: {error:#}");
        }
        if !self.prepared_path.as_os_str().is_empty()
            && self.prepared_path.exists()
            && let Err(error) = std::fs::remove_file(&self.prepared_path)
        {
            eprintln!("bootstrap file cleanup: {error}");
        }
    }
}

fn nonce() -> Result<String> {
    use std::fmt::Write as _;
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(anyhow::Error::msg)?;
    let mut token = String::with_capacity(64);
    for byte in bytes {
        write!(&mut token, "{byte:02x}")?;
    }
    Ok(token)
}
