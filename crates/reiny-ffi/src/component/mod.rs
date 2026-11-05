//! Additive executable ABI. The C contract lives in `reiny_component.h`.
//!
//! Callback lanes retain their context and factory until actual teardown, even
//! when the managed host times out. Handles share the legacy monotonic ID space.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use reiny::component::{ComponentContext, HostHandle};
use tokio::sync::watch;

mod abi;
mod callbacks;
mod host;
mod jobs;
mod ports;
mod steps;
mod support;
use support::{
    BUFFER, CALLBACK, CLOSED, COMPLETED, ERROR, Error, FAILURE, HANDLE, INVALID, KIND, NO_REPLY,
    PANIC, REMOTE, Result, SCHEMA, STATE, TIMEOUT, UNSUPPORTED, boundary, budget, failure, lock,
    ordinary, runtime, wait,
};
#[cfg(test)]
mod tests;

pub(crate) use abi::{Bytes, FactoryV1};
use callbacks::{Factory, Lanes};

#[derive(Clone)]
pub(crate) struct Stored {
    object: Arc<Object>,
    pub(crate) borrowed: bool,
}

type OperationResult = std::result::Result<Vec<u8>, (i32, String)>;

enum Object {
    JobOwner(tokio::sync::Mutex<reiny::job::JobOwner>),
    JobWork(Arc<jobs::Work>),
    Job(jobs::Job),
    JobContext(Arc<jobs::Context>),
    JobSink(Mutex<jobs::Sink>),
    JobSnapshot(jobs::Snapshot),
    JobResult(Arc<jobs::Outcome>),
    JobObserver(tokio::sync::Mutex<jobs::Handle>),
    StepSchema(reiny::step::StepSchema),
    StepGraph(reiny::step::StepGraph),
    StepValue(reiny::step::StepValue),
    StepInputs(reiny::step::StepInputs),
    StepSink(Mutex<steps::Sink>),
    StepRequest(reiny::step::StepRequest),
    StepHandle(reiny::component::StepHandle),
    StepRun(reiny::component::StepRun),
    StepResult(std::result::Result<Arc<reiny::step::StepReceipt>, reiny::component::StepRunError>),
    Registry(Mutex<Registry>),
    ReleaseToken(Arc<std::sync::atomic::AtomicU8>),
    Schema(reiny::encoded::EncodedType),
    Graph(host::Graph),
    Host(Host),
    Context(Arc<Context>),
    ErrorSink(Mutex<Option<Error>>),
    Buffer(Vec<u8>),
    Publisher(Endpoint<reiny::DeclaredPublisher>),
    Subscriber(Endpoint<tokio::sync::Mutex<reiny::RawSubscriber>>),
    Client(Endpoint<reiny::DeclaredRpcClient>),
    Server(Endpoint<tokio::sync::Mutex<reiny::DeclaredRpcServer>>),
    Request(Endpoint<Mutex<Option<reiny::DeclaredRequest>>>),
    Operation(watch::Receiver<Option<OperationResult>>),
    Message(crate::Message),
    Observer(tokio::sync::Mutex<watch::Receiver<reiny::component::HostStatus>>),
    Presence(Endpoint<tokio::sync::Mutex<reiny::RawPresence>>),
}

struct Registry {
    sealed: bool,
    factories: std::collections::BTreeMap<String, Arc<Factory>>,
}

struct Host {
    handle: Arc<HostHandle>,
    lanes: Arc<Lanes>,
}

impl Drop for Host {
    fn drop(&mut self) {
        if let Ok(runtime) = runtime() {
            let handle = self.handle.clone();
            runtime.spawn(async move {
                handle.stop().await;
            });
        }
    }
}

struct Context {
    inner: ComponentContext,
    phase: Mutex<u32>,
    shutdown: watch::Sender<bool>,
}

impl Context {
    fn new(inner: ComponentContext) -> Self {
        Self {
            inner,
            phase: Mutex::new(0),
            shutdown: watch::channel(false).0,
        }
    }

    fn close(&self) {
        match self.phase.lock() {
            Ok(mut phase) => *phase = 2,
            Err(error) => tracing::error!(%error, "component phase poisoned during shutdown"),
        }
        self.shutdown.send_replace(true);
    }

    fn admit(&self, preparing: bool) -> Result<()> {
        match *lock(&self.phase)? {
            0 if preparing => Ok(()),
            1 => Ok(()),
            0 => Err(Error::new(
                STATE,
                "communication effects are forbidden during preparation",
            )),
            _ => Err(Error::new(
                CLOSED,
                "component communication admission is closed",
            )),
        }
    }

    async fn stopped(&self) {
        let mut stop = self.shutdown.subscribe();
        loop {
            if *stop.borrow_and_update() {
                return;
            }
            if stop.changed().await.is_err() {
                return;
            }
        }
    }
}

struct Endpoint<T> {
    context: Arc<Context>,
    inner: T,
}

fn object(id: u64) -> Result<Arc<Object>> {
    match crate::c_api::get(id).map_err(|e| Error::new(HANDLE, e))? {
        crate::c_api::Object::Executable(value) => Ok(value.object),
        _ => Err(Error::new(KIND, "not an executable handle")),
    }
}

fn insert(value: Object, borrowed: bool) -> Result<u64> {
    crate::c_api::insert(crate::c_api::Object::Executable(Stored {
        object: Arc::new(value),
        borrowed,
    }))
    .map_err(failure)
}

struct Reservation(u64);

impl Reservation {
    fn new() -> Result<Self> {
        crate::c_api::insert(crate::c_api::Object::Reserved)
            .map(Self)
            .map_err(failure)
    }

    fn publish(self, object: Object) -> Result<u64> {
        crate::c_api::replace_reserved(
            self.0,
            crate::c_api::Object::Executable(Stored {
                object: Arc::new(object),
                borrowed: false,
            }),
        )
        .map_err(failure)?;
        let id = self.0;
        std::mem::forget(self);
        Ok(id)
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if let Err(error) = crate::c_api::remove(self.0, true) {
            tracing::error!(%error, "cannot remove executable result reservation");
        }
    }
}

fn wrong() -> Error {
    Error::new(KIND, "wrong executable handle kind")
}

macro_rules! variant {
    ($value:expr, $variant:ident) => {
        match $value.as_ref() {
            Object::$variant(value) => value,
            _ => return Err(wrong()),
        }
    };
}
use variant;

macro_rules! export {
    ($name:ident($($arg:ident: $ty:ty),*) $body:block) => {
        /// # Safety
        /// Inputs and outputs must satisfy `reiny_component.h`: readable inputs,
        /// aligned writable nonaliasing outputs, and valid callback code lifetimes.
        #[unsafe(no_mangle)]
        pub(crate) unsafe extern "C" fn $name($($arg: $ty),*) -> i32 {
            boundary(|| {
                // SAFETY: [FFI/aliasing] the header requires valid allocations
                // for the whole call. Helpers check null, lengths and alignment.
                unsafe { $body }
            })
        }
    };
}
use export;
