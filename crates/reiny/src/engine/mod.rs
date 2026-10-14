//! The engine abstraction — the five primitives reiny asks of the bus underneath.
//!
//! Everything above the bus (encode / decode, the fingerprint check, latched's "presence first, then
//! get", latest-wins, hooking into `Shutdown`, a service's `NoReply` / `Timeout` decision) is reiny's
//! own code; all it leans on the bus for is publish / subscribe / liveliness / queryable + get
//! (`docs/design/0.5.0.md` §1.1). That much is carved out as [`Engine`], and
//! [`ModuleContext`](crate::ModuleContext) holds an `Arc<dyn Engine>`. The default is zenoh ([`Zenoh`], feature
//! `zenoh`); for tests there is an in-process bus ([`Local`]).
//!
//! # The contract
//!
//! - **reiny owns the receive buffers.** An engine only calls a callback per sample / event / query,
//!   and it must call them from a **non-async thread** (which is what zenoh does; `Local` uses a
//!   dedicated one). reiny's callbacks only push onto a channel, and a full Fifo blocks the engine's
//!   thread (exactly as zenoh's `FifoChannel` does).
//! - **Dropping a [`Guard`] undeclares.** A subscriber / token / responder disappears with its handle.
//! - **Everything but [`Engine::alive`] is synchronous.** `await` on a zenoh 1.x builder is equivalent
//!   to `ready(wait())`, so making it async buys nothing. The only thing that really waits is
//!   [`RawReplies::next`] — which must be **cancel-safe** (it sits in a `Subscriber::recv` select arm).
//! - Features an engine lacks are declared through [`Caps`]. reiny turns a missing feature into a
//!   build-time error (never a silent downgrade). The exception is attachments: without them a

use std::any::Any;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{Qos, Result};

#[cfg(any(test, feature = "conformance"))]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
pub mod conformance;
mod local;
#[cfg(any(test, feature = "conformance"))]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
pub mod namespace_conformance;
#[cfg(feature = "zenoh")]
mod zenoh;

pub use local::Local;
#[cfg(feature = "zenoh")]
pub use zenoh::Zenoh;

/// A `Send` boxed future.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
/// A callback an engine invokes. Called from the engine's thread, hence `Send + Sync`.
pub type Callback<T> = Box<dyn Fn(T) + Send + Sync>;
/// A declaration handle. Dropping it undeclares.
pub type Guard = Box<dyn Any + Send + Sync>;
/// One reply to a query. `Err` carries what the other side passed to `reply_err`.
pub type ReplyResult = std::result::Result<Sample, Vec<u8>>;
/// The callback handed to [`Engine::respond`].
pub type QueryCallback = Callback<Box<dyn RawQuery>>;

/// The fixed prefix every key starts with.
pub const KEY_ROOT: &str = "reiny";
/// The verbatim chunk a service's presence token hangs off (`…/<Req>/@service`). It is kept apart from
/// a publisher's token (the type's key itself) so that servers never show up in `publishers::<Req>()`.
pub const SERVICE_CHUNK: &str = "@service";
/// The verbatim chunk a subscriber's presence token hangs off (`…/<T>/@sub`). Kept apart from a
/// publisher's token for the same reason `@service` is: `publishers::<T>()` must stay publishers only.
pub const SUB_CHUNK: &str = "@sub";
/// The presence token of the launch itself (`reiny/<domain>/<id>/@launch`), so that a launch with no
/// publishers at all still appears in `reiny node list`.
pub const LAUNCH_CHUNK: &str = "@launch";
/// The presence token of a launch that has completed managed startup.
pub const READY_CHUNK: &str = "@ready";
/// The queryable that requests a managed launch to stop.
pub const STOP_CHUNK: &str = "@stop";
/// The chunk of the queryable that announces a descriptor (`…/<T>/@schema/<message>`).
pub const SCHEMA_CHUNK: &str = "@schema";

/// A named endpoint's direction and declared port name.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum EndpointSlot {
    /// An output port that publishes a stream or serves RPC requests.
    Output(String),
    /// An input port's receiver presence or schema identity.
    Input(String),
}

/// A v3 named endpoint address, module control, or explicit introspection pattern.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Key {
    /// The logical namespace.
    pub domain: String,
    /// The sending launch's canonical slash-separated namespace. `None` = all depths (`**`).
    pub source: Option<String>,
    /// A concrete named endpoint. `None` is reserved for controls and tooling patterns.
    pub endpoint: Option<EndpointSlot>,
    /// The type segment ([`MessageType::TYPE_NAME`](crate::MessageType::TYPE_NAME)). `None` = all of them (`*`). A launch's
    /// token puts the verbatim `@launch` in the type slot (which `*` does not match).
    pub ty: Option<String>,
    /// The verbatim chunk after the type (`@service` / `@schema/<message>`). It matches neither `*` nor
    /// `**` — the isolation that keeps a type's topic clean. In a pattern it compares segment by segment,
    /// where `*` stands for one non-verbatim segment (`@schema/*` = every message a type describes).
    pub chunk: Option<String>,
}

impl Key {
    /// A concrete output address.
    #[must_use]
    pub fn output(domain: &str, namespace: &str, port: &str, ty: &str) -> Self {
        Self {
            domain: domain.to_string(),
            source: Some(namespace.to_string()),
            endpoint: Some(EndpointSlot::Output(port.to_string())),
            ty: Some(ty.to_string()),
            chunk: None,
        }
    }

    /// A concrete input identity, used for receiver presence and schema facets.
    #[must_use]
    pub fn input(domain: &str, namespace: &str, port: &str, ty: &str) -> Self {
        Self {
            endpoint: Some(EndpointSlot::Input(port.to_string())),
            ..Self::output(domain, namespace, port, ty)
        }
    }

    /// A module control address. Only `@launch`, `@ready`, and `@stop` are valid.
    ///
    /// # Panics
    /// Panics when `control` is not one of the three module control tokens.
    #[must_use]
    pub fn module(domain: &str, namespace: Option<&str>, control: &str) -> Self {
        assert!(
            matches!(control, LAUNCH_CHUNK | READY_CHUNK | STOP_CHUNK),
            "invalid module control"
        );
        Self {
            domain: domain.to_string(),
            source: namespace.map(str::to_string),
            endpoint: None,
            ty: Some(control.to_string()),
            chunk: None,
        }
    }

    /// The declared endpoint name, separate from its payload type.
    #[must_use]
    pub fn port(&self) -> Option<&str> {
        self.endpoint.as_ref().map(|slot| match slot {
            EndpointSlot::Output(port) | EndpointSlot::Input(port) => port.as_str(),
        })
    }

    /// A module's presence token `reiny/v3/<domain>/<namespace>/@launch`.
    #[must_use]
    pub fn launch(domain: &str, id: Option<&str>) -> Self {
        Self::module(domain, id, LAUNCH_CHUNK)
    }

    /// The named-output introspection pattern `reiny/v3/<domain>/**/@out/*/*`.
    /// Append a service, receiver-presence or schema facet with [`Self::with_chunk`].
    /// Module controls never match this pattern or any of its endpoint facets.
    #[must_use]
    pub fn all(domain: &str) -> Self {
        Self {
            domain: domain.to_string(),
            source: None,
            endpoint: None,
            ty: None,
            chunk: None,
        }
    }

    /// Whether the type slot is verbatim (`@launch`).
    #[must_use]
    pub fn is_verbatim_type(&self) -> bool {
        self.ty.as_deref().is_some_and(|t| t.starts_with('@'))
    }

    /// A copy with a chunk appended.
    #[must_use]
    pub fn with_chunk(&self, chunk: impl Into<String>) -> Self {
        Self {
            chunk: Some(chunk.into()),
            ..self.clone()
        }
    }

    /// Validate the canonical address components without turning a concrete source into a pattern.
    ///
    /// # Errors
    /// Returns an error for empty / relative path segments, wildcard concrete sources or types,
    /// reserved source segments, or a chunk without a verbatim boundary.
    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            ordinary_segment(&self.domain),
            "invalid key domain '{}'",
            self.domain
        );
        if let Some(source) = &self.source {
            anyhow::ensure!(
                source.split('/').all(ordinary_segment),
                "invalid key source '{source}': expected a canonical namespace, not a pattern"
            );
        }
        if let Some(ty) = &self.ty {
            anyhow::ensure!(
                ordinary_segment(ty)
                    || matches!(ty.as_str(), LAUNCH_CHUNK | READY_CHUNK | STOP_CHUNK),
                "invalid key type '{ty}'"
            );
        }
        if let Some(port) = self.port() {
            anyhow::ensure!(ordinary_segment(port), "invalid endpoint port '{port}'");
            anyhow::ensure!(
                self.ty.as_deref().is_some_and(ordinary_segment),
                "named endpoints require a payload type"
            );
        }
        if self.is_verbatim_type() {
            anyhow::ensure!(
                self.endpoint.is_none() && self.chunk.is_none(),
                "module controls cannot have endpoint slots or facets"
            );
        }
        if matches!(self.endpoint, Some(EndpointSlot::Input(_))) {
            anyhow::ensure!(
                self.chunk
                    .as_deref()
                    .is_some_and(|chunk| chunk == SUB_CHUNK || chunk.starts_with("@schema/")),
                "input identities require a receiver-presence or schema facet"
            );
        }
        if let Some(chunk) = &self.chunk {
            anyhow::ensure!(
                matches!(chunk.as_str(), SERVICE_CHUNK | SUB_CHUNK)
                    || chunk
                        .strip_prefix("@schema/")
                        .is_some_and(|name| { name == "*" || ordinary_segment(name) }),
                "invalid endpoint facet '{chunk}'"
            );
            anyhow::ensure!(
                !matches!(
                    (&self.endpoint, chunk.as_str()),
                    (Some(EndpointSlot::Output(_)), SUB_CHUNK)
                        | (Some(EndpointSlot::Input(_)), SERVICE_CHUNK)
                ),
                "endpoint facet has the wrong direction"
            );
        }
        Ok(())
    }

    /// Parse a v3 named endpoint or module control, rejecting older wire epochs.
    /// Only a whole `**` namespace or explicit `*` endpoint/type slots denote tooling patterns.
    #[must_use]
    pub fn parse(key: &str) -> Option<Self> {
        let mut parts = key.split('/');
        if parts.next()? != KEY_ROOT || parts.next()? != "v3" {
            return None;
        }
        let domain = parts.next()?.to_string();
        let tail: Vec<&str> = parts.collect();
        let boundary = tail.iter().position(|s| s.starts_with('@'))?;
        if boundary == 0 {
            return None;
        }
        let namespace = tail[..boundary].join("/");
        let source = (namespace != "**").then_some(namespace);
        let (endpoint, ty, chunk) = match tail[boundary] {
            LAUNCH_CHUNK | READY_CHUNK | STOP_CHUNK if tail.len() == boundary + 1 => {
                (None, Some(tail[boundary].to_string()), None)
            }
            "@out" | "@in" => {
                let port = *tail.get(boundary + 1)?;
                let ty = *tail.get(boundary + 2)?;
                if !(port == "*" || ordinary_segment(port))
                    || !(ty == "*" || ordinary_segment(ty))
                    || (tail[boundary] == "@out" && tail.get(boundary + 3) == Some(&SUB_CHUNK))
                {
                    return None;
                }
                if tail[boundary] == "@in"
                    && !matches!(tail.get(boundary + 3), Some(&SUB_CHUNK | &SCHEMA_CHUNK))
                {
                    return None;
                }
                let endpoint = if port == "*" {
                    None
                } else if tail[boundary] == "@out" {
                    Some(EndpointSlot::Output(port.to_string()))
                } else {
                    Some(EndpointSlot::Input(port.to_string()))
                };
                (
                    endpoint,
                    wildcard_to_none(ty),
                    (tail.len() > boundary + 3).then(|| tail[boundary + 3..].join("/")),
                )
            }
            _ => return None,
        };
        let key = Self {
            domain,
            source,
            endpoint,
            ty,
            chunk,
        };
        key.validate().ok()?;
        Some(key)
    }

    /// Whether `key` matches `self` taken as a pattern. A `None` source reaches every namespace depth;
    /// a `None` type is `*` and does not match verbatim slots. Chunks compare segment by segment.
    #[must_use]
    pub fn matches(&self, key: &Key) -> bool {
        let ty_ok = match (&self.ty, &key.ty) {
            (None, Some(t)) => !t.starts_with('@'),
            (None, None) => true,
            (Some(p), other) => other.as_ref() == Some(p),
        };
        let chunk_ok = match (&self.chunk, &key.chunk) {
            (None, None) => true,
            (Some(p), Some(c)) => {
                p.split('/').count() == c.split('/').count()
                    && p.split('/')
                        .zip(c.split('/'))
                        .all(|(p, c)| p == c || (p == "*" && !c.starts_with('@')))
            }
            _ => false,
        };
        self.domain == key.domain
            && self
                .source
                .as_ref()
                .is_none_or(|s| key.source.as_ref() == Some(s))
            && ty_ok
            && self
                .endpoint
                .as_ref()
                .is_none_or(|slot| key.endpoint.as_ref() == Some(slot))
            && chunk_ok
    }
}

fn wildcard_to_none(segment: &str) -> Option<String> {
    (segment != "*").then(|| segment.to_string())
}

fn ordinary_segment(segment: &str) -> bool {
    !matches!(segment, "." | "..") && crate::validate_segment("key segment", segment).is_ok()
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{KEY_ROOT}/v3/{}/{}/",
            self.domain,
            self.source.as_deref().unwrap_or("**")
        )?;
        if self.is_verbatim_type() {
            write!(f, "{}", self.ty.as_deref().unwrap_or(""))?;
        } else {
            let direction = match self.endpoint {
                Some(EndpointSlot::Input(_)) => "@in",
                _ if self.chunk.as_deref() == Some(SUB_CHUNK) => "@in",
                _ => "@out",
            };
            write!(
                f,
                "{direction}/{}/{}",
                self.port().unwrap_or("*"),
                self.ty.as_deref().unwrap_or("*")
            )?;
        }
        if let Some(chunk) = &self.chunk {
            write!(f, "/{chunk}")?;
        }
        Ok(())
    }
}

/// One item an engine delivers. A published sample and a reply to a query take the same shape.
#[derive(Clone, Debug)]
pub struct Sample {
    /// The key it arrived on (a publisher's / responder's concrete key, so `source` is `Some`).
    pub key: Key,
    /// The encoded message itself.
    pub payload: Vec<u8>,
    /// reiny's fingerprint (8 bytes LE), or somebody else's attachment.
    pub attachment: Option<Vec<u8>>,
    /// The send time (unix ns). `None` when the engine has none.
    pub timestamp: Option<u64>,
}

/// A liveliness join or leave.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Presence {
    /// A token went up on this key (existing ones also arrive right after `watch_alive`).
    Joined(Key),
    /// A token went down (dropped, or the whole process did).
    Left(Key),
}

/// What an engine can do. reiny turns anything missing into a build-time error.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)] // it is a set of flags, not a state machine
pub struct Caps {
    /// subscribe / alive / watch with `source: None` (every publisher). A point-to-point link has none.
    pub wildcard_source: bool,
    /// `declare_alive` / `alive` / `watch_alive` (presence).
    pub liveliness: bool,
    /// `respond` / `query` (latched / services / `@schema`).
    pub query: bool,
    /// A sample can carry an attachment (the fingerprint check). Without it the check passes everything.
    pub attachment: bool,
    /// Both explicit `Drop` and `Block` congestion policies can be enforced.
    /// `BackendDefault` is always accepted and makes no congestion guarantee.
    pub congestion_control: bool,
}

impl Caps {
    /// Everything.
    pub const ALL: Caps = Caps {
        wildcard_source: true,
        liveliness: true,
        query: true,
        attachment: true,
        congestion_control: true,
    };
}

/// The arguments to [`Engine::query`].
#[derive(Clone, Debug)]
pub struct QueryParams {
    /// The request itself. `None` is a latched query (payload presence is what tells it from a service).
    pub payload: Option<Vec<u8>>,
    /// The request type's fingerprint.
    pub attachment: Option<Vec<u8>>,
    /// Past this, the engine closes the reply stream.
    pub timeout: Duration,
}

/// What reiny asks of the bus underneath. Five operations: publish / subscribe / liveliness (three) / queryable + get.
pub trait Engine: Send + Sync + 'static {
    /// The features it has.
    fn caps(&self) -> Caps;

    /// Declare a publisher on `key` (a concrete key).
    /// Reject explicit congestion control when [`Caps::congestion_control`] is false.
    fn publisher(&self, key: &Key, qos: &Qos) -> Result<Box<dyn RawPublisher>>;

    /// Call `on_sample` for every sample matching `key` (a pattern).
    fn subscribe(&self, key: &Key, on_sample: Callback<Sample>) -> Result<Guard>;

    /// Raise a presence token on `key` (a concrete key). It falls with the handle, or with the process.
    fn declare_alive(&self, key: &Key) -> Result<Guard>;

    /// The tokens standing on `key` (a pattern). Past `timeout`, return what it has.
    fn alive(&self, key: &Key, timeout: Duration) -> BoxFuture<'_, Result<Vec<Key>>>;

    /// Stream joins / leaves of `key`'s (a pattern) tokens to `on_event`. Existing ones come first as `Joined`.
    fn watch_alive(&self, key: &Key, on_event: Callback<Presence>) -> Result<Guard>;

    /// Answer queries on `key` (a concrete key) through `on_query`. Dropping one unanswered finalizes it.
    /// `on_query`'s type is [`QueryCallback`].
    fn respond(&self, key: &Key, on_query: QueryCallback) -> Result<Guard>;

    /// Fire a query at every responder matching `key` (a pattern).
    fn query(&self, key: &Key, params: QueryParams) -> Result<Box<dyn RawReplies>>;

    /// The downcast door. `cloudy.engine().as_any().downcast_ref::<Zenoh>()`.
    fn as_any(&self) -> &dyn Any;
}

/// The send side [`Engine::publisher`] returns.
pub trait RawPublisher: Send + Sync {
    /// Send one item. Under `Block` it may block until the send path clears.
    /// Success does not promise delivery or retransmission.
    fn put(&self, payload: Vec<u8>, attachment: Option<Vec<u8>>) -> Result<()>;
}

/// A received query. Consume it with [`RawQuery::reply`] / [`RawQuery::reply_err`], or drop it to finalize.
pub trait RawQuery: Send {
    /// The query's key (which may be a pattern).
    fn key(&self) -> &Key;
    /// The request itself. `None` is a latched query.
    fn payload(&self) -> Option<&[u8]>;
    /// The request's attachment.
    fn attachment(&self) -> Option<&[u8]>;
    /// Reply on `key` (the answering side's concrete key).
    fn reply(
        self: Box<Self>,
        key: &Key,
        payload: Vec<u8>,
        attachment: Option<Vec<u8>>,
    ) -> Result<()>;
    /// Answer with an error. The caller receives `Err(message)`.
    fn reply_err(self: Box<Self>, message: Vec<u8>) -> Result<()>;
}

/// The stream of replies to an [`Engine::query`].
pub trait RawReplies: Send {
    /// The next reply. `None` once every responder finalized or `timeout` passed. **Cancel-safe**.
    fn next(&mut self) -> BoxFuture<'_, Option<ReplyResult>>;
}

/// The current time (unix ns). For engines whose samples carry no time of their own.
#[must_use]
pub fn now_unix_ns() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_nanos()).ok())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn key_round_trips_through_the_zenoh_form() {
        for text in [
            "reiny/v3/lab/ctrl/@out/state/RobotState",
            "reiny/v3/lab/**/@out/*/RobotState",
            "reiny/v3/lab/**/@out/*/*",
            "reiny/v3/lab/ctrl/@launch",
            "reiny/v3/lab/**/@launch",
            "reiny/v3/lab/**/@out/*/Add/@service",
            "reiny/v3/lab/ctrl/@out/state/RobotState/@schema/hs.RobotState",
            "reiny/v3/lab/ctrl/@in/state/RobotState/@sub",
        ] {
            let key = Key::parse(text).expect(text);
            assert_eq!(key.to_string(), text);
        }
        assert_eq!(
            Key::parse("reiny/v3/lab/ctrl/@out/state/RobotState")
                .unwrap()
                .source
                .as_deref(),
            Some("ctrl")
        );
        assert_eq!(
            Key::parse("reiny/v3/lab/**/@out/*/RobotState")
                .unwrap()
                .source,
            None
        );
        assert_eq!(
            Key::parse("reiny/v3/lab/ctrl/@launch").unwrap(),
            Key::launch("lab", Some("ctrl"))
        );
        assert_eq!(
            Key::parse("reiny/v3/lab/**/@out/*/*/@service").unwrap(),
            Key::all("lab").with_chunk(SERVICE_CHUNK)
        );
        for bad in [
            "",
            "reiny",
            "reiny/lab",
            "reiny/lab/ctrl",
            "other/lab/ctrl/T",
            "reiny/lab/ctrl/T",
            "reiny/v2/lab/ctrl/@out/p/T",
            "reiny/v3/lab/ctrl/@out/p/T/@sub",
            "reiny/v3/lab/ctrl/@in/p/T/@service",
            "reiny/v3/lab/ctrl/@out/p/T/@unknown",
            "reiny/v3/lab/ctrl/@launch/@service",
            "reiny/v3/lab/ctrl/@out/p/@ready",
            "reiny/v3/lab/ctrl/@out/*/@ready",
            "reiny/v3/lab/ctrl/@out/*/T/@sub",
            "reiny/v3/lab/ctrl/@in/p/T",
            "reiny/v3/lab/ctrl/@in/p/T/@sub/extra",
            "reiny/v3/lab/ctrl/@out/p/T/@schema",
            "reiny/v3/lab/ctrl/@out/p/T/@schema/a/b",
        ] {
            assert!(Key::parse(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn pattern_matching_treats_none_as_star_and_chunks_verbatim() {
        let any = Key {
            ty: Some("T".into()),
            ..Key::all("lab")
        };
        assert!(any.matches(&Key::output("lab", "a", "test", "T")));
        assert!(!any.matches(&Key::output("lab", "a", "test", "U")));
        assert!(!any.matches(&Key::output("other", "a", "test", "T")));
        assert!(!any.matches(&Key::output("lab", "a", "test", "T").with_chunk(SERVICE_CHUNK)));
        assert!(Key::launch("lab", None).matches(&Key::launch("lab", Some("a"))));
        let all = Key::all("lab");
        assert_eq!(all.to_string(), "reiny/v3/lab/**/@out/*/*");
        assert!(all.matches(&Key::output("lab", "a", "test", "T")));
        assert!(!all.matches(&Key::launch("lab", Some("a"))), "verbatim");
        assert!(!all.matches(&Key::output("lab", "a", "test", "T").with_chunk(SERVICE_CHUNK)));
        let services = all.with_chunk(SERVICE_CHUNK);
        assert!(services.matches(&Key::output("lab", "a", "test", "T").with_chunk(SERVICE_CHUNK)));
        assert!(!services.matches(&Key::output("lab", "a", "test", "T")));
        // A subscriber's token is a third, disjoint world: it reaches neither publishers nor servers.
        let subs = all.with_chunk(SUB_CHUNK);
        let sub_key = Key::input("lab", "a", "test", "T").with_chunk(SUB_CHUNK);
        assert!(subs.matches(&sub_key));
        assert!(!subs.matches(&Key::output("lab", "a", "test", "T").with_chunk(SERVICE_CHUNK)));
        assert!(!services.matches(&sub_key));
        assert!(!any.matches(&sub_key), "publishers::<T>() must not see it");
        assert!(!all.matches(&sub_key), "bag record's */* must not see it");
        assert_eq!(sub_key.to_string(), "reiny/v3/lab/a/@in/test/T/@sub");
        // `*` inside a chunk is one non-verbatim segment: how `ModuleContext::schemas` reaches every message.
        let schemas = any.with_chunk(format!("{SCHEMA_CHUNK}/*"));
        let described =
            Key::output("lab", "a", "test", "T").with_chunk(format!("{SCHEMA_CHUNK}/hs.T"));
        assert!(schemas.matches(&described));
        assert!(
            !schemas.matches(&Key::output("lab", "a", "test", "T")),
            "not the latched responder"
        );
        assert!(
            !schemas.matches(&described.with_chunk(SCHEMA_CHUNK)),
            "one segment, not zero"
        );
        assert!(
            !schemas.matches(&described.with_chunk("@schema/a/b")),
            "nor two"
        );
        assert!(
            !any.with_chunk("*").matches(&sub_key),
            "`*` never reaches a verbatim segment"
        );
    }

    #[test]
    fn endpoint_slots_do_not_infer_identity_from_payload_types() {
        let first = Key::output("lab", "module", "first", "pkg.Value");
        let second = Key::output("lab", "module", "second", "pkg.Value");
        assert_eq!(first.port(), Some("first"));
        assert_eq!(second.port(), Some("second"));
        assert!(!first.matches(&second));
        assert!(!second.matches(&first));
        for key in [first, second] {
            assert!(Key::all("lab").matches(&key));
            assert_eq!(Key::parse(&key.to_string()), Some(key));
        }
        for control in [LAUNCH_CHUNK, READY_CHUNK, STOP_CHUNK] {
            let key = Key::module("lab", Some("module"), control);
            assert!(key.port().is_none());
            assert!(!Key::all("lab").matches(&key));
        }
    }
}
