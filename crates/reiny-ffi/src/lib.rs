//! Foreign-language handles for prepared, named module endpoints.
//! Synchronous methods run on ordinary threads; endpoint handles retain the session.

use reiny::{Cloudy, DeclaredPublisher, RawEnvelope, RawSubscriber, RuntimeOptions};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::{Builder, Handle, Runtime};

/// Ergonomic C ABI shared by C, C++, JavaScript, and C#.
pub mod c_api;
#[cfg(test)]
mod tests;
uniffi::setup_scaffolding!();

/// Failure crossing a foreign-language boundary.
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum FfiError {
    /// Runtime, endpoint contract, or argument failure.
    #[error("{message}")]
    Failure {
        /// Underlying error.
        message: String,
    },
}

fn failure(error: impl std::fmt::Display) -> FfiError {
    FfiError::Failure {
        message: error.to_string(),
    }
}

fn ordinary_thread() -> Result<(), FfiError> {
    if Handle::try_current().is_ok() {
        return Err(failure(
            "synchronous reiny FFI must run outside a Tokio runtime; use spawn_blocking",
        ));
    }
    Ok(())
}

fn bindings_path(path: String) -> Result<std::path::PathBuf, FfiError> {
    let path = std::path::PathBuf::from(path);
    if !path.is_absolute() {
        return Err(failure("module bindings path must be absolute"));
    }
    Ok(path)
}

/// Encoded protobuf and validated source metadata.
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct Message {
    /// Protobuf bytes, including embedded NULs.
    pub payload: Vec<u8>,
    /// Exact sending module namespace.
    pub source: String,
    /// Mandatory declared fingerprint.
    pub schema: Option<u64>,
    /// Optional Unix nanosecond timestamp.
    pub timestamp: Option<u64>,
}

impl From<RawEnvelope> for Message {
    fn from(sample: RawEnvelope) -> Self {
        Self {
            payload: sample.payload,
            source: sample.source,
            schema: sample.schema,
            timestamp: sample.timestamp,
        }
    }
}

/// Isolated in-process bus for prepared modules.
#[derive(uniffi::Object)]
pub struct LocalBus {
    engine: Arc<reiny::engine::Local>,
}

#[uniffi::export]
impl LocalBus {
    /// Create an isolated local transport.
    #[uniffi::constructor]
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            engine: Arc::new(reiny::engine::Local::new()),
        })
    }

    /// Start a module using its absolute prepared bindings JSON path.
    pub fn connect(
        &self,
        id: String,
        domain: String,
        module_bindings_path: String,
    ) -> Result<Arc<Session>, FfiError> {
        let mut options = RuntimeOptions::new(id);
        options.domain = domain;
        options.engine = Some(self.engine.clone());
        options.module_bindings_path = Some(bindings_path(module_bindings_path)?);
        Session::start(options)
    }
}

/// Managed module session with its own synchronous runtime.
#[derive(uniffi::Object)]
pub struct Session {
    cloudy: Cloudy,
    runtime: Runtime,
}

impl Session {
    fn start(mut options: RuntimeOptions) -> Result<Arc<Self>, FfiError> {
        ordinary_thread()?;
        options.install_tracing = false;
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(failure)?;
        let cloudy = runtime.block_on(Cloudy::open(options)).map_err(failure)?;
        Ok(Arc::new(Self { cloudy, runtime }))
    }

    fn wait<T>(&self, future: impl Future<Output = T>) -> Result<T, FfiError> {
        ordinary_thread()?;
        Ok(self.runtime.block_on(future))
    }
}

#[uniffi::export]
impl Session {
    /// Open Zenoh using a prepared module contract and optional network configuration.
    #[uniffi::constructor]
    pub fn open(
        id: String,
        domain: String,
        zenoh_config: Option<String>,
        module_bindings_path: String,
    ) -> Result<Arc<Self>, FfiError> {
        let path = bindings_path(module_bindings_path)?;
        #[cfg(feature = "zenoh")]
        {
            let mut options = RuntimeOptions::new(id);
            options.domain = domain;
            options.module_bindings_path = Some(path);
            if let Some(config) = zenoh_config {
                options.zenoh = reiny::ZenohSource::File(config.into());
            }
            Self::start(options)
        }
        #[cfg(not(feature = "zenoh"))]
        {
            let _ = (id, domain, zenoh_config, path);
            Err(failure("reiny-ffi was built without the zenoh feature"))
        }
    }

    /// Module namespace.
    #[must_use]
    pub fn id(&self) -> String {
        self.cloudy.id().to_owned()
    }
    /// Logical domain.
    #[must_use]
    pub fn domain(&self) -> String {
        self.cloudy.domain().to_owned()
    }

    /// Open a named output with actual protobuf message name and mandatory fingerprint.
    pub fn publisher(
        self: &Arc<Self>,
        name: &str,
        type_name: &str,
        schema: u64,
    ) -> Result<Arc<Publisher>, FfiError> {
        ordinary_thread()?;
        let _runtime = self.runtime.enter();
        let sender = self
            .cloudy
            .raw_output(name, type_name, schema)
            .map_err(failure)?;
        Ok(Arc::new(Publisher {
            sender,
            session: self.clone(),
        }))
    }

    /// Open a named input from exactly its prepared sources.
    pub fn subscriber(
        self: &Arc<Self>,
        name: &str,
        type_name: &str,
        schema: u64,
    ) -> Result<Arc<Subscription>, FfiError> {
        ordinary_thread()?;
        let _runtime = self.runtime.enter();
        let receiver = self
            .cloudy
            .raw_input(name, type_name, schema)
            .map_err(failure)?;
        Ok(Arc::new(Subscription {
            receiver: tokio::sync::Mutex::new(receiver),
            session: self.clone(),
        }))
    }

    /// Validate all registered endpoints and announce managed readiness.
    pub fn ready(&self) -> Result<(), FfiError> {
        ordinary_thread()?;
        let _runtime = self.runtime.enter();
        self.cloudy.ready().map_err(failure)
    }

    /// Request cooperative shutdown.
    pub fn shutdown(&self) {
        self.cloudy.shutdown_now();
    }
}

/// An owned declared output retaining its module.
#[derive(uniffi::Object)]
pub struct Publisher {
    sender: DeclaredPublisher,
    session: Arc<Session>,
}

#[uniffi::export]
impl Publisher {
    /// Send protobuf bytes with the declared fingerprint.
    pub fn send(&self, payload: Vec<u8>) -> Result<(), FfiError> {
        ordinary_thread()?;
        let _runtime = self.session.runtime.enter();
        self.sender.send(payload).map_err(failure)
    }
}

/// Named input with contract-owned receive buffering.
#[derive(uniffi::Object)]
pub struct Subscription {
    receiver: tokio::sync::Mutex<RawSubscriber>,
    session: Arc<Session>,
}

#[uniffi::export]
impl Subscription {
    /// Receive within the deadline, including time waiting for concurrent callers.
    pub fn receive(&self, timeout_ms: u64) -> Result<Option<Message>, FfiError> {
        self.session.wait(async {
            tokio::select! {
                biased;
                () = self.session.cloudy.shutdown() => None,
                result = tokio::time::timeout(Duration::from_millis(timeout_ms), async {
                    self.receiver.lock().await.recv().await.map(Message::from)
                }) => result.ok().flatten(),
            }
        })
    }
}
