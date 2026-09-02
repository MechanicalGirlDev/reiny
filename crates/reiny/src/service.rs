//! Typed request/response — **the request type is the service's address**.
//!
//! ```text
//! server : respond  reiny/<domain>/<id>/<Req>      (+ liveliness  reiny/<domain>/<id>/<Req>/@service)
//! client : query    reiny/<domain>/<id|*>/<Req>    payload = Req, reply payload = Req::Response
//! ```
//!
//! It is the same key shape as publishing, so addressing ([`CallerBuilder::to`]) and presence
//! ([`Cloudy::servers`] / [`Cloudy::watch_servers`]) work through the same machinery as pub/sub.
//! It can even share a key with a latched publisher (`pubsub.rs`) — they are told apart by **whether
//! there is a payload**: a latched subscriber's `get` carries none, a service call always does.
//!
//! Correlation, timeouts and "dropped without answering = no replies" belong to the engine's query, so
//! all reiny adds is encode / decode and the fingerprint check.

use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use prost::Message;

use crate::engine::{Engine, Guard, Key, QueryParams, RawQuery, SERVICE_CHUNK};
use crate::pubsub::{attachment_fingerprint, declare_schema, fingerprint};
use crate::shutdown::Shutdown;
use crate::{Cloudy, Result, Service, Topic};

/// The same as zenoh's `get` default.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
/// How far behind our own deadline the engine's query deadline is put (so our timer fires first).
const ENGINE_TIMEOUT_MARGIN: Duration = Duration::from_secs(1);
/// How many received requests are buffered. The same as zenoh's default queryable channel.
const QUERY_CAPACITY: usize = 256;

// ---------------------------------------------------------------------------
// server
// ---------------------------------------------------------------------------

/// A typed server. Obtained from [`Cloudy::serve`].
///
/// Dropping it drops the liveliness token too, and [`Cloudy::watch_servers`] reports a `Left`.
pub struct Server<S> {
    rx: flume::Receiver<Box<dyn RawQuery>>,
    /// The queryable handle (merely held).
    _guard: Guard,
    /// The presence token that lives and dies with the server (merely held).
    _token: Guard,
    /// The `@schema` queryable, declared only when the request / response have a `DESCRIPTOR` (merely held).
    _schema: Vec<Guard>,
    /// Our own key. Replies go back on it (it intersects the query key `reiny/<domain>/*/<Req>`).
    key: Key,
    shutdown: Shutdown,
    _marker: PhantomData<S>,
}

impl<S: Service> Server<S> {
    pub(crate) fn declare(cloudy: &Cloudy) -> Result<Self> {
        let engine = cloudy.engine();
        let caps = engine.caps();
        if !(caps.query && caps.liveliness) {
            anyhow::bail!(
                "server of {}: this engine has no query / liveliness, which services need",
                S::TYPE
            );
        }
        let key = cloudy.key_for(Some(cloudy.id()), S::TYPE);
        let (tx, rx) = flume::bounded(QUERY_CAPACITY);
        let guard = engine.respond(
            &key,
            Box::new(move |query| {
                // When it is full, block the engine's thread (as zenoh's queryable channel does).
                let _ = tx.send(query);
            }),
        )?;
        let token = engine.declare_alive(&key.with_chunk(SERVICE_CHUNK))?;
        // `reiny service call` needs both descriptors to translate between JSON and proto.
        let mut schema = Vec::new();
        for descriptor in [S::DESCRIPTOR, S::Response::DESCRIPTOR]
            .into_iter()
            .flatten()
        {
            schema.push(declare_schema(cloudy, &key, descriptor)?);
        }
        tracing::debug!(key = %key, "service declared");
        Ok(Self {
            rx,
            _guard: guard,
            _token: token,
            _schema: schema,
            key,
            shutdown: cloudy.shutdown_handle(),
            _marker: PhantomData,
        })
    }

    /// Wait for the next request. `None` on shutdown or once the channel ends.
    ///
    /// A request that does not decode is warned about and skipped (the caller gets a `reply_err`).
    /// **A fingerprint mismatch is a `reply_err` too** — dropping it silently, as a subscription would,
    /// would leave the caller misreading "no replies" as "no server". A payload-less query (a latched
    pub async fn recv(&mut self) -> Option<Request<S>> {
        loop {
            let query = tokio::select! {
                biased;
                () = self.shutdown.wait() => return None,
                query = self.rx.recv_async() => query.ok()?,
            };
            let Some(payload) = query.payload() else {
                continue; // drop = finalize
            };
            if let (Some(mine), Some(theirs)) =
                (S::SCHEMA, attachment_fingerprint(query.attachment()))
                && theirs != mine
            {
                tracing::warn!(
                    ty = S::TYPE,
                    expected = format!("{mine:016x}"),
                    received = format!("{theirs:016x}"),
                    "schema fingerprint mismatch on request; replying with error"
                );
                reply_err(
                    query,
                    format!(
                        "schema fingerprint mismatch: expected {mine:016x}, received {theirs:016x}"
                    ),
                );
                continue;
            }
            match S::decode(payload) {
                Ok(value) => {
                    return Some(Request {
                        value,
                        query,
                        key: self.key.clone(),
                    });
                }
                Err(e) => {
                    tracing::warn!(ty = S::TYPE, error = %e, "skipping undecodable request");
                    reply_err(query, format!("undecodable request: {e}"));
                }
            }
        }
    }
}

fn reply_err(query: Box<dyn RawQuery>, message: String) {
    let key = query.key().clone();
    if let Err(e) = query.reply_err(message.into_bytes()) {
        tracing::warn!(key = %key, error = %e, "reply_err failed");
    }
}

/// A received request. Consume it with either [`Request::reply`] or [`Request::reply_err`].
///
/// Dropping it without calling either makes the engine finalize the query, and the caller receives a
/// [`CallError::NoReply`] — forgetting to answer never turns into a hang.
pub struct Request<S: Service> {
    /// The decoded request itself.
    pub value: S,
    query: Box<dyn RawQuery>,
    key: Key,
}

// `reply` / `reply_err` have been `async fn` since 0.4. No engine today has an await point in them, but
// the shape is kept for the day one really does wait.
#[allow(clippy::unused_async)]
impl<S: Service> Request<S> {
    /// Send the reply. With an `S::Response::SCHEMA`, the fingerprint rides in the attachment.
    pub async fn reply(self, response: S::Response) -> Result<()> {
        self.query.reply(
            &self.key,
            response.encode_to_vec(),
            fingerprint(S::Response::SCHEMA),
        )
    }

    /// Answer with an error. The caller receives this string as [`CallError::Remote`].
    pub async fn reply_err(self, message: impl Into<String>) -> Result<()> {
        self.query.reply_err(message.into().into_bytes())
    }
}

// ---------------------------------------------------------------------------
// client
// ---------------------------------------------------------------------------

/// The builder [`Cloudy::caller`] returns. With nothing set it is [`Cloudy::call`].
#[must_use = "a builder does nothing until .build()"]
pub struct CallerBuilder<'a, S> {
    cloudy: &'a Cloudy,
    to: Option<String>,
    timeout: Duration,
    _marker: PhantomData<S>,
}

impl<'a, S> CallerBuilder<'a, S> {
    pub(crate) fn new(cloudy: &'a Cloudy) -> Self {
        Self {
            cloudy,
            to: None,
            timeout: DEFAULT_TIMEOUT,
            _marker: PhantomData,
        }
    }

    /// Fire only at one launch id's server. Unset, it fires at every server in the domain and
    /// **takes the first reply** — with two or more servers around, pick here.
    pub fn to(mut self, id: impl Into<String>) -> Self {
        self.to = Some(id.into());
        self
    }

    /// The limit on waiting for a reply (default 10 s, zenoh's own). Past it, [`CallError::Timeout`].
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Build the caller. It declares nothing (a query is fired per call), so it cannot fail.
    #[must_use]
    pub fn build(self) -> Caller<S>
    where
        S: Service,
    {
        Caller {
            engine: Arc::clone(self.cloudy.engine()),
            key: self.cloudy.key_for(self.to.as_deref(), S::TYPE),
            timeout: self.timeout,
            _marker: PhantomData,
        }
    }
}

/// A typed client, built by [`Cloudy::caller`]. It borrows no `Cloudy`, so it can live in a struct.
pub struct Caller<S> {
    engine: Arc<dyn Engine>,
    key: Key,
    timeout: Duration,
    _marker: PhantomData<S>,
}

impl<S: Service> Caller<S> {
    /// Send the request and wait for the reply.
    pub async fn call(&self, request: S) -> std::result::Result<S::Response, CallError> {
        // Our own timer decides the deadline (→ `Timeout`) and the engine's deadline sits outside it —
        // zenoh shows its own expiry as nothing but "no replies", indistinguishable from `NoReply`.
        let params = QueryParams {
            payload: Some(request.encode_to_vec()),
            attachment: fingerprint(S::SCHEMA),
            timeout: self.timeout + ENGINE_TIMEOUT_MARGIN,
        };
        let mut replies = self
            .engine
            .query(&self.key, params)
            .map_err(CallError::Engine)?;
        let Some(reply) = tokio::time::timeout(self.timeout, replies.next())
            .await
            .map_err(|_| CallError::Timeout)?
        else {
            // The stream closed = every matching responder finalized (no server, or dropped unanswered).
            return Err(CallError::NoReply);
        };
        match reply {
            Ok(sample) => {
                if let (Some(expected), Some(received)) = (
                    S::Response::SCHEMA,
                    attachment_fingerprint(sample.attachment.as_deref()),
                ) && expected != received
                {
                    return Err(CallError::Schema { expected, received });
                }
                S::Response::decode(sample.payload.as_slice()).map_err(CallError::Decode)
            }
            Err(message) => Err(CallError::Remote(
                String::from_utf8_lossy(&message).into_owned(),
            )),
        }
    }
}

/// A [`Caller::call`] failure. `Remote` / `NoReply` are kept apart because callers do branch on
/// "there is no server" versus "the server refused" (whether to enable a GUI button, say).
#[derive(Debug)]
pub enum CallError {
    /// Not one reply arrived: no server, or a server that dropped the request without answering.
    NoReply,
    /// No reply before the timeout.
    Timeout,
    /// The server called [`Request::reply_err`]. The payload is the string it passed.
    Remote(String),
    /// The reply's fingerprint differs from `Response::SCHEMA`.
    Schema {
        /// Our own `Response::SCHEMA`.
        expected: u64,
        /// The fingerprint that rode on the reply.
        received: u64,
    },
    /// The reply does not decode as `Response`.
    Decode(prost::DecodeError),
    /// An engine-layer error.
    Engine(anyhow::Error),
}

impl fmt::Display for CallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoReply => write!(f, "no reply (no server, or the server dropped the request)"),
            Self::Timeout => write!(f, "timed out waiting for a reply"),
            Self::Remote(msg) => write!(f, "server replied with error: {msg}"),
            Self::Schema { expected, received } => write!(
                f,
                "response schema fingerprint mismatch: expected {expected:016x}, received {received:016x}"
            ),
            Self::Decode(e) => write!(f, "undecodable response: {e}"),
            Self::Engine(e) => write!(f, "engine: {e}"),
        }
    }
}

impl std::error::Error for CallError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Decode(e) => Some(e),
            Self::Engine(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)] // tests may fail by panicking
mod tests {
    use super::*;

    /// The engine's deadline has to sit **behind** ours, or the engine's expiry (which looks exactly
    /// like "no replies") would win the race and a timeout would be reported as `NoReply`.
    #[test]
    fn our_deadline_fires_before_the_engine_one() {
        assert!(ENGINE_TIMEOUT_MARGIN > Duration::ZERO);
        for timeout in [
            Duration::from_millis(1),
            Duration::from_secs(1),
            DEFAULT_TIMEOUT,
        ] {
            assert!(timeout < timeout + ENGINE_TIMEOUT_MARGIN);
        }
    }

    /// Every variant says which failure it is. `NoReply` and `Remote` in particular are what callers
    /// branch on, so neither may read as the other.
    #[test]
    fn call_errors_describe_themselves() {
        assert!(CallError::NoReply.to_string().contains("no reply"));
        assert!(CallError::Timeout.to_string().contains("timed out"));
        assert_eq!(
            CallError::Remote("busy".into()).to_string(),
            "server replied with error: busy"
        );
        let schema = CallError::Schema {
            expected: 0xdead_beef,
            received: 0x1234,
        }
        .to_string();
        assert!(schema.contains("00000000deadbeef"), "{schema}");
        assert!(schema.contains("0000000000001234"), "{schema}");
    }

    /// `Decode` and `Engine` carry a cause; the rest are self-contained. `{:#}` on an anyhow chain
    /// relies on this.
    #[test]
    fn only_wrapping_variants_have_a_source() {
        use std::error::Error;
        // Tag 0 is invalid, so this is a real DecodeError without the deprecated constructor.
        let err = <() as prost::Message>::decode(&[0u8][..]).unwrap_err();
        let decode = CallError::Decode(err);
        assert!(decode.source().is_some());
        assert!(
            CallError::Engine(anyhow::anyhow!("boom"))
                .source()
                .is_some()
        );
        assert!(CallError::NoReply.source().is_none());
        assert!(CallError::Timeout.source().is_none());
        assert!(CallError::Remote("x".into()).source().is_none());
    }
}
