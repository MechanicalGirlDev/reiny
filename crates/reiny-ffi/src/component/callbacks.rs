use super::{
    Arc, CALLBACK, CLOSED, ComponentContext, Context, Error, FactoryV1, INVALID, Mutex, Object,
    Result, failure, insert, lock, object, variant, wrong,
};
use reiny::component::{Component, ComponentFactory, ComponentFuture, StopReason};
use std::sync::mpsc;
use tokio::sync::oneshot;

mod lane;
mod step;
use lane::Lane;

pub(super) struct Factory {
    table: FactoryV1,
    pub(super) revision: String,
    released: Option<Arc<std::sync::atomic::AtomicU8>>,
    pub(super) step: Option<super::steps::Registration>,
}

impl Factory {
    pub(super) fn new(
        table: FactoryV1,
        revision: String,
        released: Option<Arc<std::sync::atomic::AtomicU8>>,
    ) -> Self {
        Self {
            table,
            revision,
            released,
            step: None,
        }
    }
}

impl Drop for Factory {
    fn drop(&mut self) {
        if let Some(release) = self.table.release_factory {
            // SAFETY: [lifetime/FFI] successful registration transfers exactly
            // one factory-data claim; every adapter/lane retains this Arc.
            // The header requires this finalizer to be nonblocking and not unwind.
            unsafe {
                release(self.table.factory_data);
            }
        }
        if let Some(released) = &self.released {
            // No foreign code or lock participates in notification. In
            // particular a Python GIL is never acquired on this owner thread.
            released.store(2, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

#[derive(Default)]
pub(super) struct Lanes {
    active: Mutex<usize>,
    changed: tokio::sync::Notify,
}

impl Lanes {
    fn add(self: &Arc<Self>) -> Result<LaneClaim> {
        *lock(&self.active)? += 1;
        Ok(LaneClaim(self.clone()))
    }

    pub(super) async fn joined(&self) -> Result<()> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if *lock(&self.active)? == 0 {
                return Ok(());
            }
            changed.await;
        }
    }
}

struct LaneClaim(Arc<Lanes>);

impl Drop for LaneClaim {
    fn drop(&mut self) {
        match self.0.active.lock() {
            Ok(mut active) => *active -= 1,
            Err(error) => tracing::error!(%error, "lane count poisoned"),
        }
        self.0.changed.notify_waiters();
    }
}

pub(super) struct Registered {
    factory: Arc<Factory>,
    lanes: Arc<Lanes>,
    // Field order matters: factory finalization precedes this quiescence claim.
    _claim: LaneClaim,
}

impl Registered {
    pub(super) fn new(factory: Arc<Factory>, lanes: Arc<Lanes>) -> Result<Self> {
        let claim = lanes.add()?;
        Ok(Self {
            factory,
            lanes,
            _claim: claim,
        })
    }
}

impl ComponentFactory for Registered {
    fn create(&self, _config: Option<&toml::Table>) -> reiny::Result<Box<dyn Component>> {
        Ok(Box::new(Adapter {
            factory: self.factory.clone(),
            lanes: self.lanes.clone(),
            sender: None,
            context: None,
            _claim: self.lanes.add()?,
        }))
    }
}

enum Command {
    Step(
        reiny::step::StepTick,
        reiny::step::StepInputs,
        oneshot::Sender<Result<reiny::step::StepOutputs>>,
    ),
    Prepare(oneshot::Sender<Result<()>>),
    Run(oneshot::Sender<Result<()>>),
    Stop(u32, oneshot::Sender<Result<()>>),
}

struct Adapter {
    factory: Arc<Factory>,
    lanes: Arc<Lanes>,
    sender: Option<mpsc::Sender<Command>>,
    context: Option<Arc<Context>>,
    // Also cover adapter drops after the host has published Unresponsive.
    _claim: LaneClaim,
}

impl Adapter {
    fn start(&mut self, context: &ComponentContext) -> Result<()> {
        let context = Arc::new(Context::new(context.clone()));
        let claim = self.lanes.add()?;
        let id = insert(Object::Context(context.clone()), true)?;
        let (sender, receiver) = mpsc::channel();
        let factory = self.factory.clone();
        let thread_context = context.clone();
        match std::thread::Builder::new()
            .name("reiny-component".into())
            .spawn(move || {
                let mut lane = Lane {
                    factory,
                    context: thread_context,
                    id,
                    instance: std::ptr::null_mut(),
                };
                lane.serve(&receiver);
                drop(lane);
                drop(claim);
            }) {
            Ok(join) => drop(join),
            Err(error) => {
                drop(crate::c_api::remove(id, true).map_err(failure)?);
                return Err(failure(error));
            }
        }
        self.context = Some(context);
        self.sender = Some(sender);
        Ok(())
    }

    async fn command(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<()>>) -> Command,
    ) -> reiny::Result<()> {
        let (reply, result) = oneshot::channel();
        self.sender
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("component lane not started"))?
            .send(command(reply))
            .map_err(|_| anyhow::anyhow!("component lane closed"))?;
        result
            .await
            .map_err(|_| anyhow::anyhow!("component lane terminated"))??;
        Ok(())
    }
}

impl Component for Adapter {
    fn prepare<'a>(&'a mut self, context: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move {
            self.start(context)?;
            self.command(Command::Prepare).await
        })
    }

    fn run<'a>(&'a mut self, _context: &'a ComponentContext) -> ComponentFuture<'a> {
        Box::pin(async move { self.command(Command::Run).await })
    }

    fn stop<'a>(
        &'a mut self,
        _context: &'a ComponentContext,
        reason: StopReason,
    ) -> ComponentFuture<'a> {
        if let Some(context) = &self.context {
            context.close();
        }
        Box::pin(async move {
            if self.sender.is_none() {
                return Ok(());
            }
            let reason = match reason {
                StopReason::Requested => 1,
                StopReason::StartupFailure => 2,
                StopReason::ComponentFailure => 3,
            };
            self.command(|reply| Command::Stop(reason, reply)).await
        })
    }
}

impl Drop for Adapter {
    fn drop(&mut self) {
        if let Some(context) = &self.context {
            context.close();
        }
        // Closing the channel makes the lane clean up after its current callback.
        self.sender.take();
    }
}
