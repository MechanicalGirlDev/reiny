//! Multilingual reiny handles, exported through `UniFFI` and an additive C ABI.
//!
//! Messages carry the same Protobuf bytes and type names as Rust launches.
//! These methods are synchronous: call them on an ordinary thread, not inside
//! a Tokio task. Each session owns its runtime; children keep their session alive.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use reiny::engine::{Guard, Key, RawPublisher, SUB_CHUNK, Sample};
use reiny::{Cloudy, Qos, RuntimeOptions};
use tokio::runtime::{Builder, Handle, Runtime};

/// Ergonomic C ABI shared by the C, C++, JavaScript, and C# bindings.
pub mod c_api;
#[cfg(test)]
mod tests;

uniffi::setup_scaffolding!();

/// Failures crossing a foreign-language boundary.
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum FfiError {
    /// A bus, runtime, or argument error.
    #[error("{message}")]
    Failure {
        /// The underlying failure, including its context.
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

fn segment(name: &str, value: &str) -> Result<(), FfiError> {
    if value.is_empty()
        || value
            .chars()
            .any(|c| matches!(c, '/' | '*' | '?' | '#' | '$' | '@') || c.is_whitespace())
    {
        return Err(failure(format!(
            "{name} must be a nonempty key segment without wildcards, separators, or whitespace"
        )));
    }
    Ok(())
}

/// An encoded message and the provenance supplied by the engine.
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct Message {
    /// Protobuf wire bytes; binary data and embedded NULs are preserved.
    pub payload: Vec<u8>,
    /// Complete sending launch namespace.
    pub source: String,
    /// Optional Protobuf schema fingerprint, matching `Topic::SCHEMA`.
    pub schema: Option<u64>,
    /// Optional engine timestamp in Unix nanoseconds.
    pub timestamp: Option<u64>,
}

impl From<Sample> for Message {
    fn from(sample: Sample) -> Self {
        Self {
            payload: sample.payload,
            source: sample.key.source.unwrap_or_default(),
            schema: sample
                .attachment
                .as_deref()
                .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
                .map(u64::from_le_bytes),
            timestamp: sample.timestamp,
        }
    }
}

/// An isolated in-process bus. Connect several sessions to the same object.
#[derive(uniffi::Object)]
pub struct LocalBus {
    engine: Arc<reiny::engine::Local>,
}

#[uniffi::export]
impl LocalBus {
    /// Create a bus without network configuration or discovery.
    #[uniffi::constructor]
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            engine: Arc::new(reiny::engine::Local::new()),
        })
    }

    /// Start a launch on this bus. Different domains remain isolated.
    pub fn connect(&self, id: String, domain: String) -> Result<Arc<Session>, FfiError> {
        let mut options = RuntimeOptions::new(id);
        options.domain = domain;
        options.engine = Some(self.engine.clone());
        Session::start(options)
    }
}

/// A reiny launch with its own runtime and launch-presence token.
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

    fn key(&self, topic: String, source: Option<String>) -> Key {
        Key {
            domain: self.cloudy.domain().to_owned(),
            source,
            ty: Some(topic),
            chunk: None,
        }
    }
}

#[uniffi::export]
impl Session {
    /// Open Zenoh, optionally loading a Zenoh JSON5/JSON/YAML configuration file.
    /// A build without the `zenoh` feature returns an error rather than changing engines.
    #[uniffi::constructor]
    pub fn open(
        id: String,
        domain: String,
        zenoh_config: Option<String>,
    ) -> Result<Arc<Self>, FfiError> {
        #[cfg(feature = "zenoh")]
        {
            let mut options = RuntimeOptions::new(id);
            options.domain = domain;
            if let Some(path) = zenoh_config {
                options.zenoh = reiny::ZenohSource::File(path.into());
            }
            Self::start(options)
        }
        #[cfg(not(feature = "zenoh"))]
        {
            let _ = (id, domain, zenoh_config);
            Err(failure("reiny-ffi was built without the zenoh feature"))
        }
    }

    /// Our launch id.
    #[must_use]
    pub fn id(&self) -> String {
        self.cloudy.id().to_owned()
    }

    /// Our logical namespace.
    #[must_use]
    pub fn domain(&self) -> String {
        self.cloudy.domain().to_owned()
    }

    /// Declare a publisher for a `Topic::TYPE` name, not an arbitrary key.
    /// The optional schema is attached as eight little-endian bytes, just as Rust sends it.
    pub fn publisher(
        self: &Arc<Self>,
        topic: String,
        schema: Option<u64>,
    ) -> Result<Arc<Publisher>, FfiError> {
        segment("type", &topic)?;
        let engine = self.cloudy.engine();
        let key = self.key(topic, Some(self.cloudy.id().to_owned()));
        let publisher = engine.publisher(&key, &Qos::DEFAULT).map_err(failure)?;
        let presence = engine.declare_alive(&key).map_err(failure)?;
        Ok(Arc::new(Publisher {
            sender: publisher,
            schema,
            _presence: presence,
            session: self.clone(),
        }))
    }

    /// Subscribe by type name, optionally accepting only the given launch id.
    /// The 256-message FIFO applies backpressure, matching reiny's default buffer.
    pub fn subscriber(
        self: &Arc<Self>,
        topic: String,
        source: Option<String>,
    ) -> Result<Arc<Subscription>, FfiError> {
        segment("type", &topic)?;
        let engine = self.cloudy.engine();
        let key = self.key(topic, source);
        let (sender, receiver) = flume::bounded(256);
        let subscription = engine
            .subscribe(
                &key,
                Box::new(move |sample| {
                    // A dropped receiver is normal undeclaration, not a delivery failure.
                    if let Err(error) = sender.send(sample) {
                        tracing::debug!(%error, "FFI subscription closed");
                    }
                }),
            )
            .map_err(failure)?;
        let presence = engine
            .declare_alive(
                &Key {
                    source: Some(self.cloudy.id().to_owned()),
                    ..key
                }
                .with_chunk(SUB_CHUNK),
            )
            .map_err(failure)?;
        Ok(Arc::new(Subscription {
            receiver,
            _declaration: subscription,
            _presence: presence,
            session: self.clone(),
        }))
    }

    /// Sorted, deduplicated ids publishing this type in our domain.
    pub fn publishers(&self, topic: String, timeout_ms: u64) -> Result<Vec<String>, FfiError> {
        segment("type", &topic)?;
        let key = self.key(topic, None);
        let mut ids: Vec<String> = self
            .wait(
                self.cloudy
                    .engine()
                    .alive(&key, Duration::from_millis(timeout_ms)),
            )?
            .map_err(failure)?
            .into_iter()
            .filter_map(|key| key.source)
            .collect();
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    /// Request cooperative shutdown; current and future receives return `None`.
    pub fn shutdown(&self) {
        self.cloudy.shutdown_now();
    }
}

/// A publisher. Its declaration and session stay alive until the last reference is released.
#[derive(uniffi::Object)]
pub struct Publisher {
    sender: Box<dyn RawPublisher>,
    schema: Option<u64>,
    _presence: Guard,
    session: Arc<Session>,
}

#[uniffi::export]
impl Publisher {
    /// Send encoded Protobuf bytes to the declared type.
    pub fn send(&self, payload: Vec<u8>) -> Result<(), FfiError> {
        let _runtime = self.session.runtime.enter();
        self.sender
            .put(
                payload,
                self.schema.map(|value| value.to_le_bytes().to_vec()),
            )
            .map_err(failure)
    }
}

/// A subscription with a bounded FIFO and cancel-safe, deadline-bounded receive.
#[derive(uniffi::Object)]
pub struct Subscription {
    receiver: flume::Receiver<Sample>,
    _declaration: Guard,
    _presence: Guard,
    session: Arc<Session>,
}

#[uniffi::export]
impl Subscription {
    /// Wait up to `timeout_ms`. `None` means timeout or cooperative shutdown.
    /// Multiple callers share the FIFO; each message is delivered to exactly one caller.
    pub fn receive(&self, timeout_ms: u64) -> Result<Option<Message>, FfiError> {
        self.session.wait(async {
            tokio::select! {
                biased;
                () = self.session.cloudy.shutdown() => None,
                result = tokio::time::timeout(
                    Duration::from_millis(timeout_ms),
                    self.receiver.recv_async(),
                ) => result.ok().and_then(Result::ok).map(Message::from),
            }
        })
    }
}
