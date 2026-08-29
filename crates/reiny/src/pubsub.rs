//! Typed publishers / subscribers, their builders, and presence.
//!
//! The key shape is `reiny/<domain>/<id>/<TYPE>` (publish) and `reiny/<domain>/*/<TYPE>` (subscribe);
//! [`Cloudy::key_for`](crate::Cloudy) is the single place that assembles it. Getting on and off the bus
//! goes through an [`Engine`](crate::engine::Engine) — what lives here is encode / decode, the
//! fingerprint check, latched's "presence first, then get", latest-wins and the receive buffers (Fifo /
//! Ring), and the same implementation runs on every engine.

use std::collections::{HashSet, VecDeque};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use prost::Message;
use tokio::sync::{Notify, mpsc};

use crate::engine::{
    Callback, Engine, Guard, Key, Presence as RawPresence, QueryParams, RawPublisher, RawQuery,
    RawReplies, ReplyResult, SCHEMA_CHUNK, SUB_CHUNK, Sample,
};
use crate::shutdown::Shutdown;
use crate::{Cloudy, Descriptor, Durability, History, Priority, Qos, Reliability, Result, Topic};

/// The depth of the default receive buffer (Fifo). The same as zenoh's `API_DATA_RECEPTION_CHANNEL_SIZE`.
const FIFO_CAPACITY: usize = 256;
/// How long a latched query waits before giving up. The same as zenoh's `get` default.
const LATCHED_TIMEOUT: Duration = Duration::from_secs(10);

/// A received message together with the provenance reiny knows about.
///
/// `source` / `timestamp` were information [`Subscriber::recv`] threw away in 0.2, so getting them out
/// costs nothing extra at run time.
pub struct Envelope<T> {
    /// The decoded message itself.
    pub value: T,
    /// The sending launch's id (the key's `<id>` segment).
    pub source: String,
    /// The send time (unix ns). Present only when the engine has one (zenoh, with timestamping on).
    pub timestamp: Option<u64>,
}

/// `T::SCHEMA` in attachment form (8 bytes, little-endian).
pub(crate) fn fingerprint(schema: Option<u64>) -> Option<Vec<u8>> {
    schema.map(|f| f.to_le_bytes().to_vec())
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
// publisher
// ---------------------------------------------------------------------------

/// The builder [`Cloudy::publisher`] returns. With nothing set it is [`Cloudy::publish`].
#[must_use = "a builder does nothing until .build()"]
pub struct PublisherBuilder<'a, T> {
    cloudy: &'a Cloudy,
    qos: Qos,
    _marker: PhantomData<T>,
}

impl<'a, T> PublisherBuilder<'a, T> {
    pub(crate) fn new(cloudy: &'a Cloudy) -> Self {
        Self {
            cloudy,
            qos: Qos::DEFAULT,
            _marker: PhantomData,
        }
    }

    /// Set the whole `QoS` at once — one of the [`Qos::SENSOR`] / [`Qos::COMMAND`] / [`Qos::STATE`]
    /// profiles, or a [`Qos`] of your own. The sugar called afterwards (`.latched()` …) layers on top.
    pub fn qos(mut self, qos: Qos) -> Self {
        self.qos = qos;
        self
    }

    /// Keep the most recent sample and answer a late subscriber's query with it (latched).
    /// It replaces the task that re-sends "a setting that only needs delivering once at startup" forever.
    /// The history is one sample. = `durability: TransientLocal`.
    pub fn latched(mut self) -> Self {
        self.qos.durability = Durability::TransientLocal;
        self
    }

    /// Whether to drop or to wait under congestion. = `Qos.reliability`.
    pub fn reliability(mut self, reliability: Reliability) -> Self {
        self.qos.reliability = reliability;
        self
    }

    /// Send priority. = `Qos.priority`.
    pub fn priority(mut self, priority: Priority) -> Self {
        self.qos.priority = priority;
        self
    }

    /// Skip batching and send immediately (lower latency, lower throughput). = `Qos.express`.
    pub fn express(mut self, express: bool) -> Self {
        self.qos.express = express;
        self
    }

    /// Declare the publisher. A liveliness token on the same key always comes with it
    /// (the foundation of [`Cloudy::publishers`]; there is no opt-out).
    ///
    /// `history: KeepLast(n > 1)` is an error — a publisher holds at most the one latched sample, and an
    /// n-deep ring is the subscriber's job through `.latest(n)` (it is not silently rounded to 1). A
    /// feature the engine lacks (query, needed by latched; liveliness, needed by presence) errors here too.
    pub fn build(self) -> Result<Publisher<T>>
    where
        T: Message + Topic,
    {
        if let History::KeepLast(n) = self.qos.history
            && n != 1
        {
            anyhow::bail!(
                "publisher of {}: history KeepLast({n}) is not supported — a publisher keeps at \
                 most 1 (latched); use the subscriber's `.latest({n})` for a ring",
                T::TYPE
            );
        }
        let latched = self.qos.durability == Durability::TransientLocal;
        let engine = self.cloudy.engine();
        let caps = engine.caps();
        if !caps.liveliness {
            anyhow::bail!(
                "publisher of {}: this engine has no liveliness, which presence needs",
                T::TYPE
            );
        }
        if latched && !caps.query {
            anyhow::bail!(
                "publisher of {}: this engine has no query support, which latched needs",
                T::TYPE
            );
        }
        let key = self.cloudy.key_for(Some(self.cloudy.id()), T::TYPE);
        let publisher = engine.publisher(&key, &self.qos)?;

        let last: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
        let queryable = if latched {
            Some(declare_latch(
                self.cloudy,
                &key,
                Arc::clone(&last),
                T::SCHEMA,
            )?)
        } else {
            None
        };

        // The token is declared **after** the latched queryable. A subscriber takes presence (this very
        // token) as its cue to ask for the most recent value, so in the other order the query would fly
        // during the instant where "it is there but its queryable has not arrived yet" and come back
        let token = engine.declare_alive(&key)?;
        // `@schema` is for diagnostics, so on an engine without query it is silently skipped rather than
        // being an error (every `reiny-build` generated type carries a DESCRIPTOR).
        let schema = match T::DESCRIPTOR {
            Some(descriptor) if caps.query => Some(declare_schema(self.cloudy, &key, descriptor)?),
            _ => None,
        };

        tracing::debug!(key = %key, latched, "publisher declared");
        Ok(Publisher {
            raw: publisher,
            _token: token,
            queryable,
            _schema: schema,
            last,
            _marker: PhantomData,
        })
    }
}

/// The queryable through which a publisher of a type carrying a [`Topic::DESCRIPTOR`] announces its
/// descriptor set, beside its own key at `<key>/@schema/<message>`.
///
/// `@schema` is a verbatim chunk — it matches neither `*` nor `**`, so it is invisible to anyone
/// subscribing to or recording the type's topic. The only ones who pick it up ask for it explicitly, as
/// `reiny bag record` does with `reiny/<domain>/*/*/@schema/*`.
pub(crate) fn declare_schema(cloudy: &Cloudy, key: &Key, descriptor: Descriptor) -> Result<Guard> {
    let reply_key = key.with_chunk(format!("{SCHEMA_CHUNK}/{}", descriptor.message));
    let callback_key = reply_key.clone();
    cloudy.engine().respond(
        &reply_key,
        Box::new(move |query: Box<dyn RawQuery>| {
            if let Err(e) = query.reply(&callback_key, descriptor.file_set.to_vec(), None) {
                tracing::warn!(key = %callback_key, error = %e, "schema reply failed");
            }
        }),
    )
}

/// The other half of a latched publisher — one queryable on its own publish key, answering with the
/// last value it sent and nothing more.
fn declare_latch(
    cloudy: &Cloudy,
    key: &Key,
    last: Arc<Mutex<Option<Vec<u8>>>>,
    schema: Option<u64>,
) -> Result<Guard> {
    let reply_key = key.clone();
    cloudy.engine().respond(
        key,
        Box::new(move |query: Box<dyn RawQuery>| {
            // A query carrying a payload is a service call (`service.rs`). Ignore it, so that a launch
            // that latched-publishes and serves the same type does not answer a call with its last value.
            if query.payload().is_some() {
                return;
            }
            let Some(bytes) = lock(&last).clone() else {
                return;
            };
            // Carry the same fingerprint the live path does. Without it, only latched replies would slip past the check.
            if let Err(e) = query.reply(&reply_key, bytes, fingerprint(schema)) {
                tracing::warn!(key = %reply_key, error = %e, "latched reply failed");
            }
        }),
    )
}

/// A typed publisher. Obtained from [`Cloudy::publish`] / [`PublisherBuilder::build`].
///
/// Dropping it drops the liveliness token too, so subscribers watching through
/// [`Cloudy::watch_publishers`] get a [`PresenceEvent::Left`].
pub struct Publisher<T> {
    raw: Box<dyn RawPublisher>,
    /// The presence token that lives and dies with the publisher (merely held).
    _token: Guard,
    /// The queryable answering with the most recent value, declared only when latched (merely held).
    queryable: Option<Guard>,
    /// The queryable announcing the descriptor at `@schema`, only when `T::DESCRIPTOR` is set (merely held).
    _schema: Option<Guard>,
    last: Arc<Mutex<Option<Vec<u8>>>>,
    _marker: PhantomData<T>,
}

impl<T: Message + Topic> Publisher<T> {
    /// Encode the message and publish it. With a `T::SCHEMA`, the fingerprint rides in the attachment.
    ///
    /// Under `Reliable` (the default) it blocks while the send path is congested. It is `async` to keep
    /// the shape of the API — no engine today has an await point in here.
    #[allow(clippy::unused_async)] // API since 0.4; kept for the day an engine really does wait.
    pub async fn send(&self, message: T) -> Result<()> {
        let buf = message.encode_to_vec();
        if self.queryable.is_some() {
            *lock(&self.last) = Some(buf.clone());
        }
        self.raw.put(buf, fingerprint(T::SCHEMA))
    }
}

// ---------------------------------------------------------------------------
// subscriber
// ---------------------------------------------------------------------------

/// The builder [`Cloudy::subscriber`] returns. With nothing set it is [`Cloudy::subscribe`].
#[must_use = "a builder does nothing until .build()"]
pub struct SubscriberBuilder<'a, T> {
    cloudy: &'a Cloudy,
    from: Option<String>,
    latched: bool,
    latest: Option<usize>,
    _marker: PhantomData<T>,
}

impl<'a, T> SubscriberBuilder<'a, T> {
    pub(crate) fn new(cloudy: &'a Cloudy) -> Self {
        Self {
            cloudy,
            from: None,
            latched: false,
            latest: None,
            _marker: PhantomData,
        }
    }

    /// Subscribe to one launch id only. For setups where several launches publish the same type and the
    /// subscriber wants to pick which one it listens to.
    pub fn from(mut self, id: impl Into<String>) -> Self {
        self.from = Some(id.into());
        self
    }

    /// Take a latched publisher's most recent value before entering the live subscription.
    ///
    /// The query (`get`) is fired **after seeing the presence of a publisher of that type, not right
    /// after declaring**. A `get` only sees the routing table of that instant, so firing it immediately
    /// misses during the moment where "the session is open but the link to the peer holding the
    /// publisher is not up yet", and then **stays silent forever unless the publisher re-sends** (which
    /// is the entire point of latched, gone). Presence is a **subscription** to liveliness, so
    /// declarations from links established later still arrive — that asymmetry is what closes the hole.
    pub fn latched(mut self) -> Self {
        self.latched = true;
        self
    }

    /// Keep only the most recent `n` samples and **drop the oldest** on overflow (ROS 2's `KEEP_LAST(n)`).
    ///
    /// The default (unset) is a Fifo of 256, and **when it fills, the engine's receive thread blocks and
    /// every subscription in that launch stalls**. A subscription that only reads a high-rate state at
    /// its own pace (a GUI drawing a 100 Hz `RobotState` at frame rate, say) wants `latest(1)`.
    /// Leave command paths on the default — silently dropping is the more dangerous one for control.
    pub fn latest(mut self, n: usize) -> Self {
        self.latest = Some(n.max(1));
        self
    }

    /// Declare the subscriber. A feature the engine lacks (`*` subscription, latched) errors here.
    pub fn build(self) -> Result<Subscriber<T>>
    where
        T: Message + Default + Topic,
    {
        let engine = self.cloudy.engine();
        let caps = engine.caps();
        if self.from.is_none() && !caps.wildcard_source {
            anyhow::bail!(
                "subscriber of {}: this engine is point-to-point; name the source with `.from(id)`",
                T::TYPE
            );
        }
        if self.latched && !(caps.query && caps.liveliness) {
            anyhow::bail!(
                "subscriber of {}: this engine has no query / liveliness, which latched needs",
                T::TYPE
            );
        }
        let key = self.cloudy.key_for(self.from.as_deref(), T::TYPE);

        // reiny owns the receive buffer. The engine's callback only pushes onto it (and counts).
        let counters = Arc::new(Counters::default());
        let (chan, on_sample): (Chan, Callback<Sample>) = if let Some(n) = self.latest {
            let ring = Arc::new(Ring::new(n));
            let sink = Arc::clone(&ring);
            let counters = Arc::clone(&counters);
            (
                Chan::Ring(ring),
                Box::new(move |s| {
                    counters.received.fetch_add(1, Ordering::Relaxed);
                    if sink.push(s) {
                        counters.note_full(T::TYPE, true);
                    }
                }),
            )
        } else {
            let (tx, rx) = flume::bounded(FIFO_CAPACITY);
            let counters = Arc::clone(&counters);
            (
                Chan::Fifo(rx),
                Box::new(move |s| {
                    counters.received.fetch_add(1, Ordering::Relaxed);
                    // The behaviour is unchanged — a full buffer still blocks the engine's thread,
                    // as zenoh's FifoChannel does. `try_send` runs first only so that it can be counted.
                    if let Err(flume::TrySendError::Full(s)) = tx.try_send(s) {
                        counters.note_full(T::TYPE, false);
                        let _ = tx.send(s);
                    }
                }),
            )
        };
        let guard = engine.subscribe(&key, on_sample)?;

        // Our own presence: `reiny/<domain>/<our id>/<T>/@sub`, whatever source the subscription
        // itself names. The question it answers is "who listens to this type", not "who listens to
        // whom". Unlike a publisher's token this one is skipped rather than refused on an engine
        // without liveliness — it changes nothing about how the subscription behaves.
        let mine = self.cloudy.key_for(Some(self.cloudy.id()), T::TYPE);
        let sub_token = if caps.liveliness {
            Some(engine.declare_alive(&mine.with_chunk(SUB_CHUNK))?)
        } else {
            None
        };
        // A subscriber describes its type on the bus too, so `reiny topic pub` can encode for a
        // launch that only listens — during bring-up the publisher of that type is typically the
        // thing that is not running yet.
        let schema = match T::DESCRIPTOR {
            Some(descriptor) if caps.query => Some(declare_schema(self.cloudy, &mine, descriptor)?),
            _ => None,
        };

        // The latched query waits for presence before firing (see the comment on `latched()`).
        // Watching starts **after** subscribing, so live samples arriving while we wait are not lost.
        let presence = if self.latched {
            Some(self.cloudy.watch_key::<T>(&key)?)
        } else {
            None
        };

        tracing::debug!(key = %key, latched = self.latched, latest = ?self.latest, "subscriber declared");
        Ok(Subscriber {
            chan,
            _guard: guard,
            _sub_token: sub_token,
            _schema: schema,
            counters,
            engine: Arc::clone(engine),
            key,
            latched: None,
            latched_done: true,
            presence,
            presence_done: !self.latched,
            queried: HashSet::new(),
            seen: HashSet::new(),
            warned: HashSet::new(),
            shutdown: self.cloudy.shutdown_handle(),
            _marker: PhantomData,
        })
    }
}

/// What a subscriber's receive buffer has done since it was declared.
///
/// Both buffers are quiet by design: the default Fifo of [`FIFO_CAPACITY`] **blocks the engine's
/// receive thread** when it fills (which stalls every subscription in the launch), and
/// [`SubscriberBuilder::latest`]'s ring never blocks but throws the oldest sample away instead.
/// Neither leaves a trace, so these counters — with the one-shot `warn!` beside them — are the only
/// way to learn that "the robot freezes now and then" is a full buffer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SubscriberStats {
    /// Samples the engine handed to this subscriber's buffer (the dropped ones included).
    pub received: u64,
    /// Samples the ring threw away because it was full (`latest(n)` only).
    pub dropped: u64,
    /// Times the engine's thread had to wait on a full Fifo (the default buffer only).
    pub blocked: u64,
}

/// The counters behind [`SubscriberStats`], shared with the engine's callback.
#[derive(Default)]
struct Counters {
    received: AtomicU64,
    dropped: AtomicU64,
    blocked: AtomicU64,
    /// Whether the one-shot warning has already gone out.
    warned: AtomicBool,
}

impl Counters {
    fn snapshot(&self) -> SubscriberStats {
        SubscriberStats {
            received: self.received.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            blocked: self.blocked.load(Ordering::Relaxed),
        }
    }

    /// Count a full buffer and say so **once**. Warning per sample buries the log; warning never is
    /// how a stall stays unexplained — the same trade the fingerprint-mismatch warning already makes.
    fn note_full(&self, ty: &'static str, dropping: bool) {
        let counter = if dropping {
            &self.dropped
        } else {
            &self.blocked
        };
        counter.fetch_add(1, Ordering::Relaxed);
        if self.warned.swap(true, Ordering::Relaxed) {
            return;
        }
        if dropping {
            tracing::warn!(
                ty,
                "receive ring is full; dropping the oldest sample. Read faster or raise `.latest(n)`"
            );
        } else {
            tracing::warn!(
                ty,
                capacity = FIFO_CAPACITY,
                "receive buffer is full; the engine's thread is blocked, which stalls every \
                 subscription in this launch. Read faster or use `.latest(n)`"
            );
        }
    }
}

/// The receive channel. The default is Fifo (blocks when full); [`SubscriberBuilder::latest`] makes it a
/// Ring (drops the oldest when full). It is an enum rather than making `Subscriber<T>` generic over the
/// buffer — the shape of a public type shows up in downstream struct fields.
enum Chan {
    Fifo(flume::Receiver<Sample>),
    Ring(Arc<Ring>),
}

impl Chan {
    /// The next sample. Both are cancel-safe (no value already taken out is held across an await point).
    async fn recv(&self) -> Option<Sample> {
        match self {
            Self::Fifo(rx) => rx.recv_async().await.ok(),
            Self::Ring(ring) => Some(ring.pop().await),
        }
    }
}

/// A ring of the most recent n samples. Drops the oldest when full.
struct Ring {
    queue: Mutex<VecDeque<Sample>>,
    capacity: usize,
    notify: Notify,
}

impl Ring {
    fn new(capacity: usize) -> Self {
        Self {
            queue: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            notify: Notify::new(),
        }
    }

    /// Push one sample, returning whether the oldest had to be thrown away to make room.
    fn push(&self, sample: Sample) -> bool {
        let mut queue = lock(&self.queue);
        let evicted = queue.len() == self.capacity;
        if evicted {
            queue.pop_front();
        }
        queue.push_back(sample);
        drop(queue);
        self.notify.notify_one();
        evicted
    }

    async fn pop(&self) -> Sample {
        loop {
            // Register the waiter before looking at the queue — a push arriving in between is not missed.
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(sample) = lock(&self.queue).pop_front() {
                return sample;
            }
            notified.await;
        }
    }
}

/// A typed subscriber. Obtained from [`Cloudy::subscribe`] / [`SubscriberBuilder::build`].
///
/// `recv` returns `None` on shutdown (Ctrl+C / SIGTERM / [`Cloudy::shutdown_now`]), so a
/// `while let Some(m) = sub.recv().await` loop falls out of its own accord.
///
/// # cancel-safety
///
/// [`Subscriber::recv`] / [`Subscriber::recv_envelope`] are **cancel-safe**: dropping one half-finished
/// in a `tokio::select!` or a `tokio::time::timeout` loses no sample that had arrived — the next `recv`
/// returns it. "Treat silence past a deadline as a disconnect" is written this way (reiny has no
///
/// ```ignore
/// match tokio::time::timeout(Duration::from_secs(1), sub.recv()).await {
///     Ok(Some(m)) => on_message(m),
///     Ok(None) => break,       // shutdown
///     Err(_) => on_stale(),    // there but quiet (gone is what watch_publishers reports)
/// }
/// ```
pub struct Subscriber<T> {
    chan: Chan,
    /// The subscription handle (merely held; dropping it undeclares).
    _guard: Guard,
    /// Our own `…/<T>/@sub` presence token, so `Cloudy::subscribers::<T>()` can see us (merely held).
    _sub_token: Option<Guard>,
    /// The `@schema` queryable describing `T`, when it has a `DESCRIPTOR` (merely held).
    _schema: Option<Guard>,
    /// The receive buffer's counters, shared with the engine's callback.
    counters: Arc<Counters>,
    /// What is needed to fire the latched query again (the engine and the subscription key).
    engine: Arc<dyn Engine>,
    key: Key,
    /// The reply stream of the latched query in flight (fired on seeing presence).
    latched: Option<Box<dyn RawReplies>>,
    /// Whether that reply stream is drained. True when none was ever fired.
    latched_done: bool,
    /// When latched, the stream watching publishers join and leave.
    presence: Option<Presence<T>>,
    /// Whether the presence stream has ended. True from the start when not latched.
    presence_done: bool,
    /// The publisher ids already asked for their most recent value (forgotten on leave, re-asked on return).
    queried: HashSet<String>,
    /// The sources a live sample has already been delivered from. A latched reply arriving later is dropped.
    seen: HashSet<String>,
    /// The sources already warned about a schema fingerprint mismatch (one warning per source).
    warned: HashSet<String>,
    shutdown: Shutdown,
    _marker: PhantomData<T>,
}

impl<T> Subscriber<T> {
    /// What this subscriber's receive buffer has done so far. See [`SubscriberStats`] for why the
    /// numbers matter — a non-zero `dropped` or `blocked` is a real problem, not a statistic.
    #[must_use]
    pub fn stats(&self) -> SubscriberStats {
        self.counters.snapshot()
    }
}

impl<T: Message + Default + Topic> Subscriber<T> {
    /// Receive the next message. `None` once the channel is closed or shutdown was requested.
    ///
    /// A sample that fails to decode (a corrupt payload, or one of another schema) is warned about,
    /// skipped, and reception continues. `None` is returned only as the "nothing more is coming" signal.
    pub async fn recv(&mut self) -> Option<T> {
        self.recv_envelope().await.map(|e| e.value)
    }

    /// The same as [`Subscriber::recv`], but it also gives the source id and the timestamp.
    pub async fn recv_envelope(&mut self) -> Option<Envelope<T>> {
        let Self {
            chan,
            engine,
            key,
            latched,
            latched_done,
            presence,
            presence_done,
            queried,
            seen,
            warned,
            shutdown,
            ..
        } = self;
        loop {
            tokio::select! {
                biased;
                () = shutdown.wait() => return None,
                // The cue that a publisher appeared. Ask that id for its most recent value, once.
                event = recv_presence(presence.as_mut()), if !*presence_done => {
                    match event {
                        Some(PresenceEvent::Joined(id)) => {
                            if queried.insert(id) {
                                let params = QueryParams { payload: None, attachment: None, timeout: LATCHED_TIMEOUT };
                                match engine.query(key, params) {
                                    Ok(replies) => {
                                        *latched = Some(replies);
                                        *latched_done = false;
                                    }
                                    Err(e) => tracing::warn!(key = %key, error = %e, "latched get failed"),
                                }
                            }
                        }
                        Some(PresenceEvent::Left(id)) => { queried.remove(&id); }
                        None => *presence_done = true,
                    }
                }
                // Drain the latched replies first (the stream is short-lived, so nothing starves).
                reply = recv_reply(latched.as_mut()), if !*latched_done => {
                    let Some(reply) = reply else {
                        *latched_done = true;
                        continue;
                    };
                    let Ok(sample) = reply else { continue };
                    // latest-wins: drop a late reply from a source whose live samples we already delivered.
                    let source = sample.key.source.clone().unwrap_or_default();
                    if seen.contains(&source) {
                        continue;
                    }
                    if let Some(envelope) = unwrap_sample::<T>(&sample, source, warned) {
                        return Some(envelope);
                    }
                }
                sample = chan.recv() => {
                    let sample = sample?; // channel closed
                    let source = sample.key.source.clone().unwrap_or_default();
                    // Only remembered while watching for latched (so a late "most recent" can be dropped).
                    if !*presence_done {
                        seen.insert(source.clone());
                    }
                    if let Some(envelope) = unwrap_sample::<T>(&sample, source, warned) {
                        return Some(envelope);
                    }
                }
            }
        }
    }
}

/// Check the fingerprint and decode. `None` for a sample that should be dropped.
fn unwrap_sample<T: Message + Default + Topic>(
    sample: &Sample,
    source: String,
    warned: &mut HashSet<String>,
) -> Option<Envelope<T>> {
    if !schema_matches::<T>(sample, &source, warned) {
        return None;
    }
    let value = decode::<T>(sample)?;
    Some(Envelope {
        value,
        source,
        timestamp: sample.timestamp,
    })
}

/// Turn `Option<&mut replies>` into a future. It sits in a select! branch with a precondition, so `None`
/// is treated as "never arrives" = `pending` (which avoids an unwrap).
async fn recv_reply(replies: Option<&mut Box<dyn RawReplies>>) -> Option<ReplyResult> {
    match replies {
        Some(r) => r.next().await,
        None => std::future::pending().await,
    }
}

/// The presence counterpart of [`recv_reply`]. On a non-latched subscription `None` = `pending`.
async fn recv_presence<T>(presence: Option<&mut Presence<T>>) -> Option<PresenceEvent> {
    match presence {
        Some(p) => p.recv().await,
        None => std::future::pending().await,
    }
}

/// Match the sender's fingerprint from the attachment against our own `T::SCHEMA`.
///
/// It passes through only when there is nothing to compare — either side is `None` (a hand-written
/// `impl Topic`, a sender that carries no fingerprint, an engine with no attachments), or the
/// attachment is not in the known shape (8 bytes, little-endian). Only a mismatch is dropped, and each
/// source is warned about once (warning per sample would bury the log).
fn schema_matches<T: Topic>(sample: &Sample, source: &str, warned: &mut HashSet<String>) -> bool {
    let (Some(mine), Some(theirs)) = (
        T::SCHEMA,
        attachment_fingerprint(sample.attachment.as_deref()),
    ) else {
        return true;
    };
    if theirs == mine {
        return true;
    }
    if warned.insert(source.to_string()) {
        tracing::warn!(
            ty = T::TYPE,
            source,
            expected = format!("{mine:016x}"),
            received = format!("{theirs:016x}"),
            "schema fingerprint mismatch; dropping samples from this source"
        );
    }
    false
}

/// reiny's fingerprint as carried in an attachment (8 bytes LE). `None` when it is absent or shaped differently (someone else's attachment).
pub(crate) fn attachment_fingerprint(attachment: Option<&[u8]>) -> Option<u64> {
    let raw = <[u8; 8]>::try_from(attachment?).ok()?;
    Some(u64::from_le_bytes(raw))
}

fn decode<T: Message + Default + Topic>(sample: &Sample) -> Option<T> {
    match T::decode(sample.payload.as_slice()) {
        Ok(msg) => Some(msg),
        Err(e) => {
            tracing::warn!(ty = T::TYPE, error = %e, "skipping undecodable sample");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// presence
// ---------------------------------------------------------------------------

/// A publisher joining or leaving.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PresenceEvent {
    /// This launch id declared a publisher of that type (existing ones arrive first).
    Joined(String),
    /// The publisher was dropped, or the whole process went away.
    Left(String),
}

/// The event stream [`Cloudy::watch_publishers`] returns.
pub struct Presence<T> {
    rx: mpsc::UnboundedReceiver<PresenceEvent>,
    /// The watch handle (merely held).
    _guard: Guard,
    shutdown: Shutdown,
    _marker: PhantomData<T>,
}

impl<T> Presence<T> {
    pub(crate) fn new(cloudy: &Cloudy, key: &Key) -> Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        let guard = cloudy.engine().watch_alive(
            key,
            Box::new(move |event| {
                let event = match event {
                    RawPresence::Joined(k) => PresenceEvent::Joined(k.source.unwrap_or_default()),
                    RawPresence::Left(k) => PresenceEvent::Left(k.source.unwrap_or_default()),
                };
                let _ = tx.send(event);
            }),
        )?;
        Ok(Self {
            rx,
            _guard: guard,
            shutdown: cloudy.shutdown_handle(),
            _marker: PhantomData,
        })
    }

    /// Wait for the next event. `None` on shutdown or once the channel ends.
    pub async fn recv(&mut self) -> Option<PresenceEvent> {
        tokio::select! {
            biased;
            () = self.shutdown.wait() => None,
            event = self.rx.recv() => event,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)] // tests may fail by panicking
mod tests {
    use super::*;

    /// A type with a fingerprint, and a same-topic twin with a different one — the shape
    /// `reiny-build` produces when two projects happen to name a type alike.
    #[derive(Clone, PartialEq, prost::Message)]
    struct State {
        #[prost(int32, tag = "1")]
        x: i32,
    }
    impl Topic for State {
        const TYPE: &'static str = "PubState";
        const SCHEMA: Option<u64> = Some(0xaaaa_bbbb_cccc_dddd);
    }

    /// The same `TYPE` written by hand, with no fingerprint at all.
    #[derive(Clone, PartialEq, prost::Message)]
    struct StateNoSchema {
        #[prost(int32, tag = "1")]
        x: i32,
    }
    impl Topic for StateNoSchema {
        const TYPE: &'static str = "PubState";
    }

    fn sample(payload: Vec<u8>, attachment: Option<Vec<u8>>) -> Sample {
        Sample {
            key: Key::topic("lab", Some("src"), State::TYPE),
            payload,
            attachment,
            timestamp: None,
        }
    }

    /// The fingerprint travels as 8 bytes little-endian, and anything else in the attachment is
    /// somebody else's — parsing it must not guess.
    #[test]
    fn fingerprint_is_eight_bytes_little_endian() {
        assert_eq!(fingerprint(None), None);
        let bytes = fingerprint(State::SCHEMA).expect("State has a fingerprint");
        assert_eq!(bytes.len(), 8);
        assert_eq!(attachment_fingerprint(Some(&bytes)), State::SCHEMA);

        assert_eq!(attachment_fingerprint(None), None);
        assert_eq!(attachment_fingerprint(Some(&[])), None);
        assert_eq!(attachment_fingerprint(Some(&[1, 2, 3])), None, "too short");
        assert_eq!(attachment_fingerprint(Some(&[0u8; 9])), None, "too long");
    }

    /// The check drops **only** a real mismatch. Everything it cannot compare passes through, which is
    /// what keeps a hand-written `impl Topic` (no `SCHEMA`) and an engine without attachments working.
    #[test]
    fn only_a_real_fingerprint_mismatch_is_dropped() {
        let mut warned = HashSet::new();
        let payload = State { x: 7 }.encode_to_vec();
        let mine = fingerprint(State::SCHEMA);

        // Both sides present and equal: delivered.
        let ok = sample(payload.clone(), mine.clone());
        assert_eq!(
            unwrap_sample::<State>(&ok, "src".into(), &mut warned).map(|e| e.value),
            Some(State { x: 7 })
        );

        // Both sides present and different: dropped, even though the payload would decode fine.
        let bad = sample(payload.clone(), fingerprint(Some(0x1234)));
        assert!(unwrap_sample::<State>(&bad, "src".into(), &mut warned).is_none());

        // No attachment at all: nothing to compare, so it passes.
        let bare = sample(payload.clone(), None);
        assert!(unwrap_sample::<State>(&bare, "src".into(), &mut warned).is_some());

        // Our own side has no fingerprint: passes whatever the sender put there.
        let mut warned2 = HashSet::new();
        assert!(
            unwrap_sample::<StateNoSchema>(&bad, "src".into(), &mut warned2).is_some(),
            "a hand-written impl Topic must keep interoperating"
        );

        // An attachment of another shape is somebody else's, not a mismatch.
        let alien = sample(payload, Some(b"not-a-fingerprint".to_vec()));
        assert!(unwrap_sample::<State>(&alien, "src".into(), &mut warned).is_some());
    }

    /// A mismatch warns once per source, not once per sample — the log is the thing this protects.
    #[test]
    fn a_mismatching_source_is_warned_about_once() {
        let mut warned = HashSet::new();
        let bad = sample(State { x: 1 }.encode_to_vec(), fingerprint(Some(0x1234)));
        for _ in 0..3 {
            assert!(unwrap_sample::<State>(&bad, "src".into(), &mut warned).is_none());
        }
        assert_eq!(warned.len(), 1);
        assert!(unwrap_sample::<State>(&bad, "other".into(), &mut warned).is_none());
        assert_eq!(warned.len(), 2, "each source is warned about separately");
    }

    /// An undecodable payload is skipped rather than killing the subscription — the bytes come off a
    /// bus anyone can write to.
    #[test]
    fn an_undecodable_payload_is_skipped() {
        let mut warned = HashSet::new();
        // A valid fingerprint with garbage behind it: the fingerprint check cannot catch this.
        let broken = sample(vec![0xff, 0xff, 0xff], fingerprint(State::SCHEMA));
        assert!(unwrap_sample::<State>(&broken, "src".into(), &mut warned).is_none());
        assert!(
            warned.is_empty(),
            "that is a decode failure, not a mismatch"
        );
    }

    /// `latest(n)` keeps the newest n and drops the oldest — the opposite of the default Fifo, and the
    /// reason a GUI reading a high-rate state at frame rate does not stall the whole launch.
    #[tokio::test]
    async fn ring_keeps_the_newest_and_drops_the_oldest() {
        let ring = Ring::new(2);
        for x in 1..=4i32 {
            ring.push(sample(State { x }.encode_to_vec(), None));
        }
        let mut got = Vec::new();
        for _ in 0..2 {
            let s = ring.pop().await;
            got.push(State::decode(s.payload.as_slice()).unwrap().x);
        }
        assert_eq!(got, [3, 4], "the two newest, in order");
    }

    /// `pop` registers its waiter before looking at the queue, so a push racing with an empty read is
    /// never missed.
    #[tokio::test]
    async fn ring_pop_waits_for_a_later_push() {
        let ring = std::sync::Arc::new(Ring::new(4));
        let pusher = {
            let ring = std::sync::Arc::clone(&ring);
            tokio::spawn(async move {
                tokio::task::yield_now().await;
                ring.push(sample(State { x: 9 }.encode_to_vec(), None));
            })
        };
        let s = ring.pop().await;
        assert_eq!(State::decode(s.payload.as_slice()).unwrap().x, 9);
        pusher.await.unwrap();
    }
}
