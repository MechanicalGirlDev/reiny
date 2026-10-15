//! One cancellable, pipe-draining process-tree runner for builders and acquisition.

use std::process::{Command, ExitStatus, Stdio};

use anyhow::{Context, Result};
use process_wrap::tokio::{ChildWrapper, CommandWrap};
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::progress::{CancelToken, PrepareError};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stream {
    Stdout,
    Stderr,
}

struct Tree {
    child: Box<dyn ChildWrapper>,
    runtime: tokio::runtime::Runtime,
    reaped: bool,
}

impl Tree {
    fn terminate(&mut self) -> Result<()> {
        let killed = match self.child.start_kill() {
            Ok(()) => Ok(()),
            #[cfg(unix)]
            Err(error) if error.raw_os_error() == Some(3) => Ok(()),
            Err(error) => Err(error).context("terminate builder process tree"),
        };
        let waited = self
            .runtime
            .block_on(self.child.wait())
            .context("reap builder process tree");
        self.reaped = waited.is_ok();
        killed.and(waited.map(|_| ()))
    }
}

impl Drop for Tree {
    fn drop(&mut self) {
        if !self.reaped
            && let Err(error) = self.terminate()
        {
            tracing::error!(%error, "failed to reap builder process tree");
        }
    }
}

pub(crate) fn run(
    command: Command,
    cancel: &CancelToken,
    line: impl FnMut(Stream, &str) -> Result<()> + Send,
) -> Result<ExitStatus> {
    // The synchronous API is also callable from an existing Tokio runtime.
    // Keep this private runtime on its own thread rather than nesting block_on.
    std::thread::scope(|scope| {
        scope
            .spawn(|| run_inner(command, cancel, line))
            .join()
            .map_err(|_| anyhow::anyhow!("process runner panicked"))?
    })
}

fn run_inner(
    mut command: Command,
    cancel: &CancelToken,
    mut line: impl FnMut(Stream, &str) -> Result<()>,
) -> Result<ExitStatus> {
    cancel.check()?;
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut command = CommandWrap::from(tokio::process::Command::from(command));
    command.wrap(process_wrap::tokio::KillOnDrop);
    #[cfg(unix)]
    command.wrap(process_wrap::tokio::ProcessGroup::leader());
    #[cfg(windows)]
    {
        command.wrap(process_wrap::tokio::JobObject);
        command.wrap(crate::WindowsJobCompletion::default());
    }
    let child = {
        let _entered = runtime.enter();
        command
            .spawn()
            .context("spawn owned builder/acquisition process")?
    };
    let mut tree = Tree {
        child,
        runtime,
        reaped: false,
    };
    let stdout = tree.child.stdout().take().context("child stdout missing")?;
    let stderr = tree.child.stderr().take().context("child stderr missing")?;
    let mut stdout = BufReader::new(stdout).lines();
    let mut stderr = BufReader::new(stderr).lines();
    let result = tree.runtime.block_on(async {
        let drain = async {
            let (mut out_open, mut err_open) = (true, true);
            while out_open || err_open {
                tokio::select! {
                    output = stdout.next_line(), if out_open => match output? {
                        Some(text) => line(Stream::Stdout, &text)?,
                        None => out_open = false,
                    },
                    output = stderr.next_line(), if err_open => match output? {
                        Some(text) => line(Stream::Stderr, &text)?,
                        None => err_open = false,
                    },
                }
            }
            Ok::<(), anyhow::Error>(())
        };
        // Drain both pipes alongside the retained whole-tree completion.
        let completion = async {
            let (status, ()) = tokio::try_join!(
                async {
                    // Windows has completion + JobObject layers. On Unix the
                    // native child returns itself from the second inner_mut.
                    let status = tree.child.inner_mut().inner_mut().wait().await?;
                    // The builder transaction ends with its leader, including
                    // compiler telemetry or command descendants holding pipes.
                    match tree.child.start_kill() {
                        Ok(()) => {}
                        #[cfg(unix)]
                        Err(error) if error.raw_os_error() == Some(3) => {}
                        Err(error) => return Err(error.into()),
                    }
                    tree.child.wait().await?;
                    Ok::<_, anyhow::Error>(status)
                },
                drain,
            )?;
            Ok::<_, anyhow::Error>(status)
        };
        tokio::select! {
            biased;
            () = cancel.cancelled() => Err(PrepareError::Cancelled.into()),
            result = completion => result,
        }
    });
    if result.is_ok() {
        tree.reaped = true;
    } else if let Err(cleanup) = tree.terminate() {
        return result.map_err(|error| error.context(format!("builder cleanup: {cleanup:#}")));
    }
    result
}
