use super::{
    Action, Context, Control, DeploymentPhase, DeploymentStatus, Duration, ErrorKind, IO_TIMEOUT,
    Path, PreparedDeployment, Request, Response, Result, START_GRACE, STATUS_VERSION, TcpStream,
    bail, ensure, read_frame, write_frame,
};

/// An authenticated connection capability for one owner incarnation.
///
/// This type deliberately does not implement `Debug` or `Serialize`.
#[derive(Clone)]
pub struct DeploymentClient {
    control: Control,
}

impl DeploymentClient {
    /// Find and authenticate the owner of a root. Stale files return `None`.
    pub fn find(root: &Path) -> Result<Option<Self>> {
        let bytes = match std::fs::read(root.join(".reiny/control.json")) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let control: Control = serde_json::from_slice(&bytes)?;
        ensure!(
            control.version == 1 && control.address.ip().is_loopback(),
            "invalid owner control address"
        );
        let client = Self { control };
        // A newly published endpoint can still be reconciling its children.
        // Use the same bound as apply, rather than treating slow startup as stale.
        match client.request(Action::Status, Some(START_GRACE)) {
            Ok(_) => Ok(Some(client)),
            Err(error) => {
                tracing::debug!(%error, "deployment owner is unavailable");
                Ok(None)
            }
        }
    }

    /// Read live state from the authenticated owner.
    pub fn status(&self) -> Result<DeploymentStatus> {
        self.request(Action::Status, Some(IO_TIMEOUT))
    }

    /// Reconcile a prepared desired state, preserving unchanged processes.
    ///
    /// A successful response confirms reconciliation, not application readiness.
    /// Use [`Self::wait_ready`] to verify application contracts.
    pub fn apply(&self, prepared: &PreparedDeployment) -> Result<DeploymentStatus> {
        self.request(
            Action::Apply {
                prepared: Box::new(prepared.clone()),
            },
            Some(START_GRACE),
        )
    }

    /// Stop all owned process trees; returns only after they have been reaped.
    pub fn stop(&self) -> Result<DeploymentStatus> {
        self.request(Action::Stop, Some(START_GRACE))
    }

    /// Subscribe until shutdown has reaped all owned children.
    ///
    /// Owner disappearance or an autonomous owner error wakes this call with an
    /// error. There is no client polling or read timeout for this subscription.
    pub fn wait_stopped(&self) -> Result<DeploymentStatus> {
        self.watch_stopped()?.wait()
    }

    /// Register a termination subscription before triggering another action.
    ///
    /// Registration is acknowledged by the owner; the returned subscription can
    /// be moved to a waiting thread without a subscribe/stop race.
    pub fn watch_stopped(&self) -> Result<StopSubscription> {
        let mut stream = self.open(Action::WaitStopped, Some(IO_TIMEOUT))?;
        decode_response(&mut stream, &self.control.owner_generation)?;
        stream.set_read_timeout(None)?;
        Ok(StopSubscription {
            stream,
            owner_generation: self.control.owner_generation.clone(),
        })
    }

    /// Wait until shutdown or successful completion has reaped all owned trees.
    ///
    /// Successful task completion leaves the owner available for later apply.
    pub fn wait_terminal(&self) -> Result<DeploymentStatus> {
        self.watch_terminal()?.wait()
    }

    /// Register a shutdown/completion subscription before triggering an action.
    ///
    /// Registration is acknowledged. Already-completed deployments notify the
    /// subscription immediately; owner failure or disappearance returns an error.
    pub fn watch_terminal(&self) -> Result<TerminalSubscription> {
        let mut stream = self.open(Action::WaitTerminal, Some(IO_TIMEOUT))?;
        decode_response(&mut stream, &self.control.owner_generation)?;
        stream.set_read_timeout(None)?;
        Ok(TerminalSubscription {
            stream,
            owner_generation: self.control.owner_generation.clone(),
        })
    }

    /// Wait on the owner's state events, rather than polling status files.
    pub fn wait_ready(&self, timeout: Duration) -> Result<DeploymentStatus> {
        self.request(
            Action::WaitReady {
                timeout_ms: u64::try_from(timeout.as_millis())?,
            },
            Some(timeout.saturating_add(IO_TIMEOUT)),
        )
    }

    /// Wait until the owner publishes a revision newer than `revision`.
    pub fn wait_for_revision(&self, revision: u64, timeout: Duration) -> Result<DeploymentStatus> {
        self.request(
            Action::WaitRevision {
                revision,
                timeout_ms: u64::try_from(timeout.as_millis())?,
            },
            Some(timeout.saturating_add(IO_TIMEOUT)),
        )
    }

    fn request(&self, action: Action, timeout: Option<Duration>) -> Result<DeploymentStatus> {
        let mut stream = self.open(action, timeout)?;
        decode_response(&mut stream, &self.control.owner_generation)
    }

    fn open(&self, action: Action, timeout: Option<Duration>) -> Result<TcpStream> {
        let mut stream = TcpStream::connect_timeout(&self.control.address, IO_TIMEOUT)
            .context("deployment owner disappeared")?;
        stream.set_read_timeout(timeout)?;
        stream.set_write_timeout(Some(IO_TIMEOUT))?;
        write_frame(
            &mut stream,
            &Request {
                token: self.control.token.clone(),
                owner_generation: self.control.owner_generation.clone(),
                action,
            },
        )?;
        Ok(stream)
    }
}

/// An already-registered, single-connection termination subscription.
pub struct StopSubscription {
    stream: TcpStream,
    owner_generation: String,
}

impl StopSubscription {
    /// Wait for exact child cleanup, or an owner failure/disappearance.
    pub fn wait(mut self) -> Result<DeploymentStatus> {
        let status = decode_response(&mut self.stream, &self.owner_generation)?;
        ensure!(
            status.phase == DeploymentPhase::Stopped,
            "deployment did not stop cleanly"
        );
        Ok(status)
    }
}

/// An acknowledged subscription to successful task completion or shutdown.
pub struct TerminalSubscription {
    pub(super) stream: TcpStream,
    pub(super) owner_generation: String,
}

impl TerminalSubscription {
    /// Wait until all owned trees have been reaped, or the owner fails/disappears.
    pub fn wait(mut self) -> Result<DeploymentStatus> {
        let status = decode_response(&mut self.stream, &self.owner_generation)?;
        ensure!(
            matches!(
                status.phase,
                DeploymentPhase::Stopped | DeploymentPhase::Completed
            ),
            "deployment did not reach a clean terminal state"
        );
        Ok(status)
    }
}

pub(super) fn decode_response(
    stream: &mut TcpStream,
    owner_generation: &str,
) -> Result<DeploymentStatus> {
    let response: Response =
        read_frame(stream).context("deployment owner disappeared or stopped responding")?;
    if let Some(error) = response.error {
        bail!("{error}");
    }
    let status = response.status.context("owner returned no status")?;
    ensure!(
        matches!(status.version, STATUS_VERSION | 3),
        "unsupported deployment status version"
    );
    ensure!(
        status.owner_generation == owner_generation,
        "owner incarnation changed"
    );
    Ok(status)
}

/// Read the last observation without claiming that its owner or PIDs are alive.
pub fn last_status(root: &Path) -> Result<Option<DeploymentStatus>> {
    let bytes = match std::fs::read(root.join(".reiny/state.json")) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut status: DeploymentStatus = serde_json::from_slice(&bytes)?;
    ensure!(
        matches!(status.version, STATUS_VERSION | 3),
        "unsupported deployment status version"
    );
    status.owner_alive = false;
    Ok(Some(status))
}
