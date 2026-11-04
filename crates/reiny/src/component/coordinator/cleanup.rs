use super::{
    Command, ComponentPhase, Coordinator, Event, HostError, HostErrorCode, HostOutcome, HostPhase,
    StopReason, phase,
};

impl Coordinator {
    pub(super) async fn cleanup(
        &mut self,
        primary: Option<HostError>,
        reason: StopReason,
    ) -> HostOutcome {
        if let Some(gate) = &self.step_gate {
            gate.stop();
        }
        if let Some(error) = &primary {
            self.record(error);
        }
        self.status
            .send_modify(|status| status.phase = HostPhase::Stopping);
        self.ready.clear();
        self.prepared.clear();
        self.commands.send_replace(Command::Stop(reason));
        for context in self.contexts.values() {
            context.inner.shutdown_now();
        }
        let mut cleanup = Vec::new();
        let deadline = tokio::time::sleep(self.options.stop_timeout);
        tokio::pin!(deadline);
        let mut events_open = true;
        while !self.tasks.is_empty() {
            tokio::select! {
                biased;
                () = &mut deadline => break,
                event = self.events.recv(), if events_open => match event {
                    Some(Event::Opened(context)) => context.inner.shutdown_now(),
                    Some(Event::Prepared(_, ticket)) => drop(ticket),
                    Some(Event::Failure(error)) => {
                        self.record(&error);
                        if !primary.as_ref().is_some_and(|first| {
                            first.code == error.code && first.instance == error.instance
                        }) {
                            cleanup.push(error);
                        }
                    }
                    Some(Event::StopRequested) => {}
                    None => events_open = false,
                },
                joined = self.tasks.join_next_with_id() => {
                    if let Some(joined) = joined {
                        let (id, error) = match joined {
                            Ok(result) => result,
                            Err(error) => (error.id(), Some(HostError::new(
                                HostErrorCode::Panic,
                                self.pending.get(&error.id()).map(String::as_str),
                                error.to_string(),
                            ))),
                        };
                        if let Some(error) = error {
                            self.record(&error);
                            cleanup.push(error);
                        }
                        if let Some(id) = self.pending.remove(&id) {
                            self.status.send_modify(|status| {
                                if let Some(component) = status.components.get_mut(&id) {
                                    component.phase = if component.error.is_some() {
                                        ComponentPhase::Failed
                                    } else { ComponentPhase::Stopped };
                                }
                            });
                        }
                    }
                }
            }
        }
        if !self
            .join_step_driver(deadline.deadline(), &mut cleanup)
            .await
        {
            return HostOutcome::Unresponsive {
                primary,
                cleanup,
                instances: self
                    .bindings
                    .iter()
                    .map(|bindings| bindings.namespace.clone())
                    .collect(),
            };
        }
        if !self.pending.is_empty() {
            let mut instances = self.pending.values().cloned().collect::<Vec<_>>();
            instances.sort();
            for id in &instances {
                phase(&self.status, id, ComponentPhase::Unresponsive);
            }
            self.tasks.abort_all();
            return HostOutcome::Unresponsive {
                primary,
                cleanup,
                instances,
            };
        }
        match primary {
            Some(primary) => HostOutcome::Failed { primary, cleanup },
            None if cleanup.is_empty() => HostOutcome::Stopped,
            None => {
                let primary = cleanup.remove(0);
                HostOutcome::Failed { primary, cleanup }
            }
        }
    }

    async fn join_step_driver(
        &mut self,
        deadline: tokio::time::Instant,
        cleanup: &mut Vec<HostError>,
    ) -> bool {
        let Some(mut driver) = self.step_driver.take() else {
            return true;
        };
        match tokio::time::timeout_at(deadline, &mut driver).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                cleanup.push(HostError::new(HostErrorCode::Step, None, error.to_string()));
            }
            Err(_) => {
                driver.abort();
                return false;
            }
        }
        true
    }
}
