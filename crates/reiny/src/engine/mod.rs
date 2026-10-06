//! The engine abstraction — the five primitives reiny asks of the bus underneath.
//!
//! Everything above the bus (encode / decode, the fingerprint check, latched's "presence first, then
//! get", latest-wins, hooking into `Shutdown`, a service's `NoReply` / `Timeout` decision) is reiny's
//! own code; all it leans on the bus for is publish / subscribe / liveliness / queryable + get
//! (`docs/design/0.5.0.md` §1.1). That much is carved out as [`Engine`], and
//! [`Cloudy`](crate::Cloudy) holds an `Arc<dyn Engine>`. The default is zenoh ([`Zenoh`], feature
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

/// An address. zenoh renders it as `reiny/<domain>/<source>/<ty>[/<chunk>]` (the 0.4 wire).
///
/// Another engine may render it differently — the one thing they share is that `ty` is the address.
/// `source` is not necessarily part of the address either: an engine may implement `subscribe`'s
/// `source: Some(id)` as a **filter** (iceoryx2 has one service per type and puts source in a header).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Key {
    /// The logical namespace.
    pub domain: String,
    /// The sending launch's canonical slash-separated namespace. `None` = all depths (`**`).
    pub source: Option<String>,
    /// The type segment ([`Topic::TYPE`](crate::Topic::TYPE)). `None` = all of them (`*`). A launch's
    /// token puts the verbatim `@launch` in the type slot (which `*` does not match).
    pub ty: Option<String>,
    /// The verbatim chunk after the type (`@service` / `@schema/<message>`). It matches neither `*` nor
    /// `**` — the isolation that keeps a type's topic clean. In a pattern it compares segment by segment,
    /// where `*` stands for one non-verbatim segment (`@schema/*` = every message a type describes).
    pub chunk: Option<String>,
}

impl Key {
    /// A type's key. `source: None` means `**`; `Some` is always an exact namespace.
    #[must_use]
    pub fn topic(domain: &str, source: Option<&str>, ty: &str) -> Self {
        Self {
            domain: domain.to_string(),
            source: source.map(str::to_string),
            ty: Some(ty.to_string()),
            chunk: None,
        }
    }

    /// A launch's presence token `reiny/<domain>/<id>/@launch` (`id: None` = every launch).
    #[must_use]
    pub fn launch(domain: &str, id: Option<&str>) -> Self {
        Self {
            domain: domain.to_string(),
            source: id.map(str::to_string),
            ty: Some(LAUNCH_CHUNK.to_string()),
            chunk: None,
        }
    }

    /// The all-sources, all-types pattern `reiny/<domain>/*/**` (zenoh's canonical `**/*`).
    /// Engine filtering keeps it apart from type chunks and verbatim type slots.
    #[must_use]
    pub fn all(domain: &str) -> Self {
        Self {
            domain: domain.to_string(),
            source: None,
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
        if let Some(chunk) = &self.chunk {
            let mut segments = chunk.split('/');
            let first = segments.next().unwrap_or("");
            anyhow::ensure!(
                first.starts_with('@')
                    && ordinary_segment(&first[1..])
                    && !matches!(first, LAUNCH_CHUNK | READY_CHUNK | STOP_CHUNK)
                    && segments.all(|s| s == "*" || ordinary_segment(s)),
                "invalid key chunk '{chunk}': expected a verbatim chunk followed by ordinary segments"
            );
        }
        Ok(())
    }

    /// Parse `reiny/<domain>/<namespace>/<ty>[/@chunk/…]`. The type is the last segment
    /// before a verbatim chunk, or the terminal `@launch` / `@ready` / `@stop` slot.
    /// `None` when the shape differs. Only `**` denotes an all-depth source pattern.
    #[must_use]
    pub fn parse(key: &str) -> Option<Self> {
        let mut parts = key.split('/');
        if parts.next()? != KEY_ROOT {
            return None;
        }
        let domain = parts.next()?.to_string();
        let tail: Vec<&str> = parts.collect();
        // Zenoh canonicalizes `**/*` into `*/**`, including before a verbatim chunk.
        let (source, ty, chunk) = if tail.starts_with(&["*", "**"]) {
            (None, None, (tail.len() > 2).then(|| tail[2..].join("/")))
        } else {
            let boundary = tail.iter().position(|s| s.starts_with('@'));
            let type_at = match boundary {
                Some(i) if matches!(tail[i], LAUNCH_CHUNK | READY_CHUNK | STOP_CHUNK) => i,
                Some(i) => i.checked_sub(1)?,
                None => tail.len().checked_sub(1)?,
            };
            if type_at == 0 {
                return None; // A namespace is never empty, even though zenoh's ** can match zero.
            }
            let source = tail[..type_at].join("/");
            (
                (source != "**").then_some(source),
                wildcard_to_none(tail[type_at]),
                (tail.len() > type_at + 1).then(|| tail[type_at + 1..].join("/")),
            )
        };
        let key = Self {
            domain,
            source,
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
        if self.source.is_none() && self.ty.is_none() {
            write!(f, "{KEY_ROOT}/{}/*/**", self.domain)?;
        } else {
            write!(
                f,
                "{KEY_ROOT}/{}/{}/{}",
                self.domain,
                self.source.as_deref().unwrap_or("**"),
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
}

impl Caps {
    /// Everything.
    pub const ALL: Caps = Caps {
        wildcard_source: true,
        liveliness: true,
        query: true,
        attachment: true,
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
    /// Send one item. Under `Reliable` it may block until the send path clears.
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
            "reiny/lab/ctrl/RobotState",
            "reiny/lab/**/RobotState",
            "reiny/lab/*/**",
            "reiny/lab/ctrl/@launch",
            "reiny/lab/**/@launch",
            "reiny/lab/**/Add/@service",
            "reiny/lab/ctrl/RobotState/@schema/hs.RobotState",
        ] {
            let key = Key::parse(text).expect(text);
            assert_eq!(key.to_string(), text);
        }
        assert_eq!(
            Key::parse("reiny/lab/ctrl/RobotState")
                .unwrap()
                .source
                .as_deref(),
            Some("ctrl")
        );
        assert_eq!(Key::parse("reiny/lab/**/RobotState").unwrap().source, None);
        assert_eq!(
            Key::parse("reiny/lab/ctrl/@launch").unwrap(),
            Key::launch("lab", Some("ctrl"))
        );
        assert_eq!(
            Key::parse("reiny/lab/*/**/@service").unwrap(),
            Key::all("lab").with_chunk(SERVICE_CHUNK)
        );
        for bad in [
            "",
            "reiny",
            "reiny/lab",
            "reiny/lab/ctrl",
            "other/lab/ctrl/T",
        ] {
            assert!(Key::parse(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn pattern_matching_treats_none_as_star_and_chunks_verbatim() {
        let any = Key::topic("lab", None, "T");
        assert!(any.matches(&Key::topic("lab", Some("a"), "T")));
        assert!(!any.matches(&Key::topic("lab", Some("a"), "U")));
        assert!(!any.matches(&Key::topic("other", Some("a"), "T")));
        assert!(!any.matches(&Key::topic("lab", Some("a"), "T").with_chunk(SERVICE_CHUNK)));
        assert!(Key::launch("lab", None).matches(&Key::launch("lab", Some("a"))));
        let all = Key::all("lab");
        assert_eq!(all.to_string(), "reiny/lab/*/**");
        assert!(all.matches(&Key::topic("lab", Some("a"), "T")));
        assert!(!all.matches(&Key::launch("lab", Some("a"))), "verbatim");
        assert!(!all.matches(&Key::topic("lab", Some("a"), "T").with_chunk(SERVICE_CHUNK)));
        let services = all.with_chunk(SERVICE_CHUNK);
        assert!(services.matches(&Key::topic("lab", Some("a"), "T").with_chunk(SERVICE_CHUNK)));
        assert!(!services.matches(&Key::topic("lab", Some("a"), "T")));
        // A subscriber's token is a third, disjoint world: it reaches neither publishers nor servers.
        let subs = all.with_chunk(SUB_CHUNK);
        let sub_key = Key::topic("lab", Some("a"), "T").with_chunk(SUB_CHUNK);
        assert!(subs.matches(&sub_key));
        assert!(!subs.matches(&Key::topic("lab", Some("a"), "T").with_chunk(SERVICE_CHUNK)));
        assert!(!services.matches(&sub_key));
        assert!(!any.matches(&sub_key), "publishers::<T>() must not see it");
        assert!(!all.matches(&sub_key), "bag record's */* must not see it");
        assert_eq!(sub_key.to_string(), "reiny/lab/a/T/@sub");
        // `*` inside a chunk is one non-verbatim segment: how `Cloudy::schemas` reaches every message.
        let schemas = any.with_chunk(format!("{SCHEMA_CHUNK}/*"));
        let described =
            Key::topic("lab", Some("a"), "T").with_chunk(format!("{SCHEMA_CHUNK}/hs.T"));
        assert!(schemas.matches(&described));
        assert!(
            !schemas.matches(&Key::topic("lab", Some("a"), "T")),
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
}
