use super::{Arc, ChildWrapper, Command, CommandWrap, Context, Instant, PathBuf, Result};

pub(super) struct OwnedProcess {
    pub(super) child: Box<dyn ChildWrapper>,
    pub(super) observation: ProcessObservation,
    pub(super) reaped: bool,
    pub(super) runtime: Arc<tokio::runtime::Runtime>,
}

pub(super) enum ProcessObservation {
    Module {
        report_path: PathBuf,
        started: Instant,
    },
    Hosted,
}

impl OwnedProcess {
    pub(super) fn spawn(
        command: Command,
        observation: ProcessObservation,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Result<Self> {
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
            command.spawn()?
        };
        Ok(Self {
            child,
            observation,
            reaped: false,
            runtime,
        })
    }

    pub(super) fn terminate(&mut self) -> Result<()> {
        let killed = match self.child.start_kill() {
            Ok(()) => Ok(()),
            // ESRCH means the owned Unix process group no longer exists.
            #[cfg(unix)]
            Err(error) if error.raw_os_error() == Some(3) => Ok(()),
            Err(error) => Err(error).context("terminating owned process tree"),
        };
        let waited = self
            .runtime
            .block_on(self.child.wait())
            .context("reaping owned process tree");
        self.reaped = waited.is_ok();
        killed.and(waited.map(|_| ()))
    }
}

impl Drop for OwnedProcess {
    fn drop(&mut self) {
        if !self.reaped
            && let Err(error) = self.terminate()
        {
            tracing::error!(%error, "could not reap owned process tree");
        }
    }
}
