use super::*;

impl ComponentFactory for Factory {
    fn create(&self, _: Option<&toml::Table>) -> crate::Result<Box<dyn Component>> {
        match self.behavior {
            Behavior::FactoryError => anyhow::bail!("factory failure"),
            Behavior::FactoryPanic => std::panic::resume_unwind(Box::new("factory panic")),
            Behavior::Idle
            | Behavior::PrepareError
            | Behavior::PreparePanic
            | Behavior::PrepareWait
            | Behavior::RunError
            | Behavior::RunPanic
            | Behavior::RunReturn
            | Behavior::StopError
            | Behavior::StopPanic
            | Behavior::StopWait => Ok(Box::new(TestComponent {
                behavior: self.behavior,
                records: self.records.clone(),
                gate: self.gate.clone(),
                id: String::new(),
            })),
        }
    }
}

impl TestComponent {
    fn record(&self, stage: Stage) {
        self.records
            .try_send(Record {
                id: self.id.clone(),
                stage,
            })
            .expect("record stage");
    }
}

impl Component for TestComponent {
    fn prepare<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            self.id = ctx.id().to_owned();
            self.record(Stage::Prepare);
            match self.behavior {
                Behavior::PrepareError => anyhow::bail!("prepare failure"),
                Behavior::PreparePanic => std::panic::resume_unwind(Box::new("prepare panic")),
                Behavior::PrepareWait => {
                    self.gate.wait_for(|open| *open).await?;
                }
                Behavior::Idle
                | Behavior::FactoryError
                | Behavior::FactoryPanic
                | Behavior::RunError
                | Behavior::RunPanic
                | Behavior::RunReturn
                | Behavior::StopError
                | Behavior::StopPanic
                | Behavior::StopWait => {}
            }
            Ok(())
        })
    }

    fn run<'a>(&'a mut self, _: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            self.record(Stage::Run);
            match self.behavior {
                Behavior::RunError => anyhow::bail!("run failure"),
                Behavior::RunPanic => std::panic::resume_unwind(Box::new("run panic")),
                Behavior::RunReturn => return Ok(()),
                Behavior::Idle
                | Behavior::FactoryError
                | Behavior::FactoryPanic
                | Behavior::PrepareError
                | Behavior::PreparePanic
                | Behavior::PrepareWait
                | Behavior::StopError
                | Behavior::StopPanic
                | Behavior::StopWait => {}
            }
            std::future::pending().await
        })
    }

    fn stop<'a>(&'a mut self, _: &'a ComponentContext, reason: StopReason) -> ComponentFuture<'a> {
        Box::pin(async move {
            self.record(Stage::Stop(reason));
            match self.behavior {
                Behavior::StopError => anyhow::bail!("stop failure"),
                Behavior::StopPanic => std::panic::resume_unwind(Box::new("stop panic")),
                Behavior::StopWait => {
                    self.gate.wait_for(|open| *open).await?;
                }
                Behavior::Idle
                | Behavior::FactoryError
                | Behavior::FactoryPanic
                | Behavior::PrepareError
                | Behavior::PreparePanic
                | Behavior::PrepareWait
                | Behavior::RunError
                | Behavior::RunPanic
                | Behavior::RunReturn => {}
            }
            Ok(())
        })
    }
}

impl Drop for TestComponent {
    fn drop(&mut self) {
        match self.records.try_send(Record {
            id: self.id.clone(),
            stage: Stage::Dropped,
        }) {
            Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                unreachable!("drop observation queue is full")
            }
        }
    }
}
