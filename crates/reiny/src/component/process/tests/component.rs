use tokio::sync::mpsc;

use crate::component::{
    Component, ComponentContext, ComponentFactory, ComponentFuture, StopReason,
};

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Event {
    Prepare(String, Option<std::path::PathBuf>),
    Run,
    Stop,
}

#[derive(Clone, Copy)]
pub(super) enum Behavior {
    Idle,
    WaitPrepare,
    FailPrepare,
}

pub(super) struct Factory {
    pub events: mpsc::Sender<Event>,
    pub behavior: Behavior,
    pub constructed: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl ComponentFactory for Factory {
    fn create(&self, _: Option<&toml::Table>) -> crate::Result<Box<dyn Component>> {
        self.constructed
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Box::new(Self {
            events: self.events.clone(),
            behavior: self.behavior,
            constructed: std::sync::Arc::clone(&self.constructed),
        }))
    }
}

impl Component for Factory {
    fn prepare<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            self.events
                .send(Event::Prepare(
                    ctx.id().to_owned(),
                    ctx.config_dir().map(std::path::Path::to_path_buf),
                ))
                .await?;
            match self.behavior {
                Behavior::Idle => Ok(()),
                Behavior::WaitPrepare => std::future::pending().await,
                Behavior::FailPrepare => Err(anyhow::anyhow!("fixture prepare failure")),
            }
        })
    }

    fn run<'a>(&'a mut self, ctx: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            self.events.send(Event::Run).await?;
            ctx.shutdown().await;
            Ok(())
        })
    }

    fn stop<'a>(&'a mut self, _: &'a ComponentContext, _: StopReason) -> ComponentFuture<'a> {
        Box::pin(async move {
            self.events.send(Event::Stop).await?;
            Ok(())
        })
    }
}
