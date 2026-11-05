use super::{
    Action, AtomicBool, Connection, DeploymentPhase, DeploymentStatus, Duration, ErrorKind,
    IO_TIMEOUT, Instant, MAX_CONNECTIONS, Ordering, Owner, Result, TICK, Waiter, authenticated,
    receive, reply,
};

impl Owner<'_> {
    pub(super) fn run(&mut self, stop: &AtomicBool) -> Result<()> {
        let mut connections: Vec<Connection> = Vec::new();
        while !stop.load(Ordering::Acquire) {
            self.observe()?;
            self.publish()?;
            for _ in 0..MAX_CONNECTIONS {
                match self.listener.accept() {
                    Ok((stream, _)) => {
                        if connections.len().saturating_add(self.waiters.len()) < MAX_CONNECTIONS {
                            stream.set_nonblocking(true)?;
                            connections.push(Connection {
                                stream,
                                bytes: Vec::new(),
                                deadline: Instant::now() + IO_TIMEOUT,
                            });
                        }
                    }
                    Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                    Err(error) => return Err(error.into()),
                }
            }
            let mut index = 0;
            while index < connections.len() {
                match receive(&mut connections[index]) {
                    Ok(Some(request)) => {
                        let connection = connections.swap_remove(index);
                        if !authenticated(&self.control, &request) {
                            reply(
                                connection.stream,
                                Err(anyhow::anyhow!("authentication failed")),
                            );
                            continue;
                        }
                        match request.action {
                            Action::Status => reply(connection.stream, Ok(self.status.clone())),
                            Action::Apply { prepared } => {
                                let result = self.apply(*prepared).map(|()| self.status.clone());
                                reply(connection.stream, result);
                            }
                            Action::Stop => {
                                let result = (|| {
                                    self.stop_all()?;
                                    self.remove_endpoint()?;
                                    self.status.phase = DeploymentPhase::Stopped;
                                    self.status.owner_alive = false;
                                    self.publish()
                                })();
                                if let Err(error) = result {
                                    reply(connection.stream, Err(anyhow::anyhow!("{error:#}")));
                                    return Err(error);
                                }
                                reply(connection.stream, Ok(self.status.clone()));
                                return Ok(());
                            }
                            action @ (Action::WaitStopped | Action::WaitTerminal) => {
                                reply(connection.stream.try_clone()?, Ok(self.status.clone()));
                                self.waiters.push(Waiter {
                                    stream: connection.stream,
                                    action,
                                    deadline: None,
                                });
                            }
                            action @ (Action::WaitReady { timeout_ms }
                            | Action::WaitRevision { timeout_ms, .. }) => {
                                let timeout =
                                    Duration::from_millis(timeout_ms).min(Duration::from_hours(1));
                                self.waiters.push(Waiter {
                                    stream: connection.stream,
                                    action,
                                    deadline: Some(Instant::now() + timeout),
                                });
                            }
                        }
                    }
                    Ok(None) => index += 1,
                    Err(error) => {
                        tracing::debug!(%error, "discarding invalid owner request");
                        connections.swap_remove(index);
                    }
                }
            }
            self.notify_waiters();
            std::thread::park_timeout(TICK);
        }
        Ok(())
    }

    pub(super) fn notify_waiters(&mut self) {
        let mut index = 0;
        while index < self.waiters.len() {
            let outcome = self.wait_outcome(&self.waiters[index]);
            if let Some(outcome) = outcome {
                let waiter = self.waiters.swap_remove(index);
                reply(waiter.stream, outcome);
            } else {
                index += 1;
            }
        }
    }

    pub(super) fn wait_outcome(&self, waiter: &Waiter) -> Option<Result<DeploymentStatus>> {
        match waiter.action {
            Action::WaitReady { .. } => match self.status.phase {
                DeploymentPhase::Ready | DeploymentPhase::Completed => {
                    return Some(Ok(self.status.clone()));
                }
                DeploymentPhase::Failed
                | DeploymentPhase::Suspended
                | DeploymentPhase::Stopped
                | DeploymentPhase::Stopping => {
                    return Some(Err(anyhow::anyhow!(
                        "deployment {:?}: {}",
                        self.status.phase,
                        self.status.error.as_deref().unwrap_or("not ready")
                    )));
                }
                DeploymentPhase::Starting | DeploymentPhase::Degraded => {}
            },
            Action::WaitRevision { revision, .. } if self.status.revision > revision => {
                return Some(Ok(self.status.clone()));
            }
            Action::WaitTerminal
                if matches!(
                    self.status.phase,
                    DeploymentPhase::Stopped | DeploymentPhase::Completed
                ) =>
            {
                return Some(Ok(self.status.clone()));
            }
            Action::WaitRevision { .. } | Action::WaitStopped | Action::WaitTerminal => {}
            Action::Status | Action::Apply { .. } | Action::Stop => {
                return Some(Err(anyhow::anyhow!("invalid wait request")));
            }
        }
        waiter
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
            .then(|| Err(anyhow::anyhow!("deployment wait timed out")))
    }
}
