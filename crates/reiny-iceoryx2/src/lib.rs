//! The [`Engine`] that puts reiny on iceoryx2 (same-host shared memory).
//!
//! iceoryx2 has neither wildcards nor liveliness, so nothing about the rendering resembles zenoh's
//! (`docs/design/0.5.0.md` §5):
//!
//! | reiny | iceoryx2 |
//! | --- | --- |
//! | publish to `reiny/<d>/<id>/<T>` | the pub-sub service `reiny/<d>/<T>` (payload `[u8]` + [`Meta`]); the source is in the header |
//! | subscribe to `reiny/<d>/*/<T>` | one subscriber on that same service; `source` is a **filter** applied to the header |
//! | respond / query | the request-response service `reiny/<d>/<T>/q`. The query's key (a pattern) rides in the request header, and a server that does not match drops the request (= finalizes it) |
//! | `declare_alive(key)` | `open_or_create` the pub-sub service `reiny-alive/<key>` and **hold** it. Existing = alive |
//! | `alive` / `watch_alive` | read `Service::list`. `watch` has the engine thread diffing that list every 200 ms (there is no push) |
//!
//! Receiving is one engine thread: it attaches every subscriber's / server's event listener to a
//! `WaitSet`, waits, and on waking `receive()`s until empty and calls the callbacks. `ipc_threadsafe`
//! is used, so a publisher / client sends straight from the caller's thread.
//!
//! # Building it
//!
//! Linux binds libc directly and needs nothing extra. Windows / macOS need **libclang**, because
//! `iceoryx2-pal-posix` runs bindgen (`LIBCLANG_PATH` = the directory holding `libclang.dll` / `.so`).

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use iceoryx2::active_request::ActiveRequest;
use iceoryx2::pending_response::PendingResponse;
use iceoryx2::port::client::Client;
use iceoryx2::port::listener::Listener;
use iceoryx2::port::notifier::Notifier;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::port::server::Server;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use reiny::engine::{
    BoxFuture, Callback, Caps, Engine, Guard, Key, Presence, QueryCallback, QueryParams,
    RawPublisher, RawQuery, RawReplies, ReplyResult, Sample, now_unix_ns,
};
use reiny::{Qos, Reliability, Result};

/// iceoryx2's service flavour. It makes ports `Send + Sync` (at the cost of one internal mutex).
type S = ipc_threadsafe::Service;
type PubSubFactory = iceoryx2::service::port_factory::publish_subscribe::PortFactory<S, [u8], Meta>;
type AliveFactory = iceoryx2::service::port_factory::publish_subscribe::PortFactory<S, u8, ()>;
type EventFactory = iceoryx2::service::port_factory::event::PortFactory<S>;
type RrFactory =
    iceoryx2::service::port_factory::request_response::PortFactory<S, [u8], Meta, [u8], Meta>;
type Sub = Subscriber<S, [u8], Meta>;
type Pub = Publisher<S, [u8], Meta>;
type Srv = Server<S, [u8], Meta, [u8], Meta>;
type Cli = Client<S, [u8], Meta, [u8], Meta>;
type Req = ActiveRequest<S, [u8], Meta, [u8], Meta>;
type Pending = PendingResponse<S, [u8], Meta, [u8], Meta>;

/// The per-service port limit. `open_or_create` fails on a settings mismatch, so every launch uses the
/// same value. iceoryx2 **preallocates a port's shared memory as the product of the limits** (for a
/// client, `max_servers × max_active_requests × max_loaned_requests × slice_len`), so raising them
/// makes creating a port take seconds. `// ponytail: 16 ports per type; measure before raising it`
const PORTS: usize = 16;
/// The subscriber's receive buffer (on iceoryx2's side). One more stage in front of reiny's Fifo (256).
const BUFFER: usize = 64;
/// The payload length a publisher / client / server allocates first. Past it, it reallocates by doubling.
const INITIAL_SLICE: usize = 1024;
/// How many requests one client can have in flight.
const REQUESTS: usize = 4;
/// How often presence (`Service::list`) is read = the upper bound on `watch_alive`'s lag.
const POLL: Duration = Duration::from_millis(200);
/// The poll interval while waiting for a query's reply. `// ponytail: 1 ms poll; wait on a qev listener if a µs call is ever needed`
const REPLY_POLL: Duration = Duration::from_millis(1);

const KEY_MAX: usize = 224;
const ATTACHMENT_MAX: usize = 32;
const FLAG_PAYLOAD: u8 = 1;
const FLAG_ERROR: u8 = 2;

/// The fixed-size header that accompanies a sample / request / response.
///
/// iceoryx2's own header has no time, so the sender puts `unix_ns` in. `key` is the publisher's
/// concrete key on a sample, the query's pattern on a request, and the responder's concrete key on a response.
#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[repr(C)]
pub struct Meta {
    key: [u8; KEY_MAX],
    attachment: [u8; ATTACHMENT_MAX],
    unix_ns: u64,
    key_len: u8,
    attachment_len: u8,
    flags: u8,
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            key: [0; KEY_MAX],
            attachment: [0; ATTACHMENT_MAX],
            unix_ns: 0,
            key_len: 0,
            attachment_len: 0,
            flags: 0,
        }
    }
}

impl Meta {
    fn new(key: &Key, attachment: Option<&[u8]>, flags: u8) -> Result<Self> {
        let text = key.to_string();
        let attachment = attachment.unwrap_or(&[]);
        let key_len = u8::try_from(text.len())
            .ok()
            .filter(|_| text.len() <= KEY_MAX)
            .ok_or_else(|| anyhow::anyhow!("key '{text}' is longer than {KEY_MAX} bytes"))?;
        let attachment_len = u8::try_from(attachment.len())
            .ok()
            .filter(|_| attachment.len() <= ATTACHMENT_MAX)
            .ok_or_else(|| {
                anyhow::anyhow!("attachment is longer than {ATTACHMENT_MAX} bytes (reiny puts 8)")
            })?;
        let mut meta = Self {
            unix_ns: now_unix_ns().unwrap_or(0),
            key_len,
            attachment_len,
            flags,
            ..Self::default()
        };
        meta.key[..text.len()].copy_from_slice(text.as_bytes());
        meta.attachment[..attachment.len()].copy_from_slice(attachment);
        Ok(meta)
    }

    fn key(&self) -> Option<Key> {
        Key::parse(std::str::from_utf8(&self.key[..usize::from(self.key_len)]).ok()?)
    }

    fn attachment(&self) -> Option<Vec<u8>> {
        (self.attachment_len > 0)
            .then(|| self.attachment[..usize::from(self.attachment_len)].to_vec())
    }

    fn timestamp(&self) -> Option<u64> {
        (self.unix_ns != 0).then_some(self.unix_ns)
    }
}

fn err(e: impl std::fmt::Debug) -> anyhow::Error {
    anyhow::anyhow!("iceoryx2: {e:?}")
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn service_name(text: &str) -> Result<ServiceName> {
    ServiceName::new(text)
        .map_err(|e| anyhow::anyhow!("iceoryx2 rejects service name '{text}': {e:?}"))
}

/// The presence service's name. `@` is avoided because whether iceoryx2 accepts it was never checked.
fn alive_name(key: &Key) -> String {
    format!("reiny-alive/{}", key.to_string().replace('@', "_at_"))
}

fn parse_alive(name: &str) -> Option<Key> {
    Key::parse(&name.strip_prefix("reiny-alive/")?.replace("_at_", "@"))
}

fn type_of(key: &Key) -> Result<&str> {
    key.ty.as_deref().ok_or_else(|| {
        anyhow::anyhow!(
            "iceoryx2 engine: key '{key}' names no type (all-types keys are not supported)"
        )
    })
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// engine
// ---------------------------------------------------------------------------

/// The [`Engine`] on top of iceoryx2. Wrap it in an `Arc` and hand it to `RuntimeOptions::engine`.
pub struct Iceoryx2 {
    node: Mutex<Node<S>>,
    config: Config,
    state: Arc<Mutex<State>>,
    /// Wake the engine thread (after a registration or a removal).
    wake: Arc<Notifier<S>>,
    stop: Arc<AtomicBool>,
    /// The per-type request-response client (so a port is not created per query).
    clients: Mutex<HashMap<String, Arc<RrPorts>>>,
}

struct RrPorts {
    client: Cli,
    notifier: Notifier<S>,
}

#[derive(Default)]
struct State {
    subscribers: Vec<SubEntry>,
    servers: Vec<SrvEntry>,
    watchers: Vec<WatchEntry>,
}

struct SubEntry {
    id: u64,
    pattern: Key,
    subscriber: Arc<Sub>,
    listener: Arc<Listener<S>>,
    on_sample: Arc<Callback<Sample>>,
}

struct SrvEntry {
    id: u64,
    key: Key,
    server: Arc<Srv>,
    listener: Arc<Listener<S>>,
    on_query: Arc<QueryCallback>,
}

struct WatchEntry {
    id: u64,
    pattern: Key,
    on_event: Arc<Callback<Presence>>,
    known: HashSet<Key>,
}

impl Iceoryx2 {
    /// With iceoryx2's default configuration.
    pub fn new() -> Result<Self> {
        Self::with_config(Config::default())
    }

    /// With a configuration of your own. Tests change `config.global.prefix` to isolate shared memory.
    pub fn with_config(config: Config) -> Result<Self> {
        let instance = next_id();
        let pid = std::process::id();
        // iceoryx2 does not create its root itself (Linux `/tmp/iceoryx2/`, Windows
        // `C:\Temp\iceoryx2\`) and node creation fails with `InternalError` when it is missing.
        let root = config.global.root_path().to_string();
        std::fs::create_dir_all(&root)
            .map_err(|e| anyhow::anyhow!("creating iceoryx2 root directory {root}: {e}"))?;
        let node = NodeBuilder::new()
            .name(&NodeName::new(&format!("reiny_{pid}_{instance}")).map_err(err)?)
            .signal_handling_mode(SignalHandlingMode::Disabled)
            .config(&config)
            .create::<S>()
            .map_err(|e| err(e).context("creating the iceoryx2 node"))?;
        let wake_service = node
            .service_builder(&service_name(&format!("reiny-wake/{pid}/{instance}"))?)
            .event()
            .open_or_create()
            .map_err(|e| err(e).context("creating the wake event service"))?;
        let wake = Arc::new(
            wake_service
                .notifier_builder()
                .create()
                .map_err(|e| err(e).context("creating the wake notifier"))?,
        );
        let wake_listener = wake_service
            .listener_builder()
            .create()
            .map_err(|e| err(e).context("creating the wake listener"))?;
        let state = Arc::new(Mutex::new(State::default()));
        let stop = Arc::new(AtomicBool::new(false));
        let thread = Thread {
            config: config.clone(),
            state: Arc::clone(&state),
            stop: Arc::clone(&stop),
            wake_listener,
        };
        std::thread::Builder::new()
            .name("reiny-iceoryx2".into())
            .spawn(move || thread.run())
            .map_err(err)?;
        Ok(Self {
            node: Mutex::new(node),
            config,
            state,
            wake,
            stop,
            clients: Mutex::new(HashMap::new()),
        })
    }

    fn pubsub(&self, domain: &str, ty: &str) -> Result<PubSubFactory> {
        lock(&self.node)
            .service_builder(&service_name(&format!("reiny/{domain}/{ty}"))?)
            .publish_subscribe::<[u8]>()
            .user_header::<Meta>()
            .enable_safe_overflow(false)
            .history_size(0)
            .subscriber_max_buffer_size(BUFFER)
            .max_publishers(PORTS)
            .max_subscribers(PORTS)
            .open_or_create()
            .map_err(err)
    }

    fn event(&self, domain: &str, ty: &str, suffix: &str) -> Result<EventFactory> {
        lock(&self.node)
            .service_builder(&service_name(&format!("reiny/{domain}/{ty}/{suffix}"))?)
            .event()
            .max_notifiers(PORTS)
            .max_listeners(PORTS)
            .open_or_create()
            .map_err(err)
    }

    fn rr(&self, domain: &str, ty: &str) -> Result<RrFactory> {
        lock(&self.node)
            .service_builder(&service_name(&format!("reiny/{domain}/{ty}/q"))?)
            .request_response::<[u8], [u8]>()
            .request_user_header::<Meta>()
            .response_user_header::<Meta>()
            .max_servers(PORTS)
            .max_clients(PORTS)
            .max_active_requests_per_client(REQUESTS)
            .max_loaned_requests(REQUESTS)
            .open_or_create()
            .map_err(err)
    }

    fn rr_ports(&self, domain: &str, ty: &str) -> Result<Arc<RrPorts>> {
        let name = format!("{domain}/{ty}");
        if let Some(ports) = lock(&self.clients).get(&name) {
            return Ok(Arc::clone(ports));
        }
        let client = self
            .rr(domain, ty)?
            .client_builder()
            .initial_max_slice_len(INITIAL_SLICE)
            .allocation_strategy(AllocationStrategy::PowerOfTwo)
            .create()
            .map_err(err)?;
        let notifier = self
            .event(domain, ty, "qev")?
            .notifier_builder()
            .create()
            .map_err(err)?;
        let ports = Arc::new(RrPorts { client, notifier });
        lock(&self.clients).insert(name, Arc::clone(&ports));
        Ok(ports)
    }

    fn guard(&self, slot: Slot, id: u64) -> Guard {
        self.wake();
        Box::new(Iox2Guard {
            state: Arc::clone(&self.state),
            wake: Arc::clone(&self.wake),
            slot,
            id,
        })
    }

    fn wake(&self) {
        if let Err(e) = self.wake.notify() {
            tracing::warn!(error = ?e, "iceoryx2: waking the engine thread failed");
        }
    }

    /// The presence keys currently standing. Dead processes' leftovers are cleaned up first.
    fn alive_now(&self, pattern: &Key) -> Vec<Key> {
        let _ = Node::<S>::try_cleanup_dead_nodes(&self.config);
        list_alive(&self.config)
            .into_iter()
            .filter(|k| pattern.matches(k))
            .collect()
    }
}

fn list_alive(config: &Config) -> Vec<Key> {
    let mut keys = Vec::new();
    if let Err(e) = S::list(config, |details| {
        if let Some(key) = parse_alive(details.static_details.name().as_str()) {
            keys.push(key);
        }
        CallbackProgression::Continue
    }) {
        tracing::warn!(error = ?e, "iceoryx2: listing services failed");
    }
    keys
}

impl Drop for Iceoryx2 {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = self.wake.notify();
    }
}

impl Engine for Iceoryx2 {
    fn caps(&self) -> Caps {
        Caps::ALL
    }

    fn publisher(&self, key: &Key, qos: &Qos) -> Result<Box<dyn RawPublisher>> {
        let ty = type_of(key)?;
        let publisher = self
            .pubsub(&key.domain, ty)?
            .publisher_builder()
            .initial_max_slice_len(INITIAL_SLICE)
            .allocation_strategy(AllocationStrategy::PowerOfTwo)
            .backpressure_strategy(match qos.reliability {
                Reliability::Reliable => BackpressureStrategy::RetryUntilDelivered,
                Reliability::BestEffort => BackpressureStrategy::DiscardData,
            })
            .create()
            .map_err(err)?;
        let notifier = self
            .event(&key.domain, ty, "ev")?
            .notifier_builder()
            .create()
            .map_err(err)?;
        Ok(Box::new(Iox2Publisher {
            key: key.clone(),
            publisher,
            notifier,
        }))
    }

    fn subscribe(&self, key: &Key, on_sample: Callback<Sample>) -> Result<Guard> {
        let ty = type_of(key)?;
        let subscriber = self
            .pubsub(&key.domain, ty)?
            .subscriber_builder()
            .buffer_size(BUFFER)
            .create()
            .map_err(err)?;
        let listener = self
            .event(&key.domain, ty, "ev")?
            .listener_builder()
            .create()
            .map_err(err)?;
        let id = next_id();
        lock(&self.state).subscribers.push(SubEntry {
            id,
            pattern: key.clone(),
            subscriber: Arc::new(subscriber),
            listener: Arc::new(listener),
            on_sample: Arc::new(on_sample),
        });
        Ok(self.guard(Slot::Subscriber, id))
    }

    fn declare_alive(&self, key: &Key) -> Result<Guard> {
        let factory: AliveFactory = lock(&self.node)
            .service_builder(&service_name(&alive_name(key))?)
            .publish_subscribe::<u8>()
            .max_publishers(1)
            .max_subscribers(1)
            .open_or_create()
            .map_err(err)?;
        // The service exists exactly while it is held = alive. No port is needed.
        self.wake();
        Ok(Box::new(factory))
    }

    fn alive(&self, key: &Key, _timeout: Duration) -> BoxFuture<'_, Result<Vec<Key>>> {
        let keys = self.alive_now(key);
        Box::pin(std::future::ready(Ok(keys)))
    }

    fn watch_alive(&self, key: &Key, on_event: Callback<Presence>) -> Result<Guard> {
        let id = next_id();
        lock(&self.state).watchers.push(WatchEntry {
            id,
            pattern: key.clone(),
            on_event: Arc::new(on_event),
            known: HashSet::new(),
        });
        Ok(self.guard(Slot::Watcher, id))
    }

    fn respond(&self, key: &Key, on_query: QueryCallback) -> Result<Guard> {
        let ty = type_of(key)?;
        let server = self
            .rr(&key.domain, ty)?
            .server_builder()
            .initial_max_slice_len(INITIAL_SLICE)
            .allocation_strategy(AllocationStrategy::PowerOfTwo)
            .create()
            .map_err(err)?;
        let listener = self
            .event(&key.domain, ty, "qev")?
            .listener_builder()
            .create()
            .map_err(err)?;
        let id = next_id();
        lock(&self.state).servers.push(SrvEntry {
            id,
            key: key.clone(),
            server: Arc::new(server),
            listener: Arc::new(listener),
            on_query: Arc::new(on_query),
        });
        Ok(self.guard(Slot::Server, id))
    }

    fn query(&self, key: &Key, params: QueryParams) -> Result<Box<dyn RawReplies>> {
        let ty = type_of(key)?;
        let started = Instant::now();
        let ports = self.rr_ports(&key.domain, ty)?;
        tracing::trace!(%key, elapsed = ?started.elapsed(), "iceoryx2: query ports ready");
        let flags = if params.payload.is_some() {
            FLAG_PAYLOAD
        } else {
            0
        };
        let meta = Meta::new(key, params.attachment.as_deref(), flags)?;
        let payload = params.payload.unwrap_or_default();
        let mut request = ports.client.loan_slice_uninit(payload.len()).map_err(err)?;
        *request.user_header_mut() = meta;
        let pending = request.write_from_slice(&payload).send().map_err(err)?;
        if let Err(e) = ports.notifier.notify() {
            tracing::warn!(error = ?e, "iceoryx2: notifying servers failed");
        }
        tracing::trace!(
            %key,
            servers = pending.number_of_server_connections(),
            connected = pending.is_connected(),
            "iceoryx2: query sent"
        );
        Ok(Box::new(Iox2Replies {
            pending,
            deadline: tokio::time::Instant::now() + params.timeout,
        }))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// ---------------------------------------------------------------------------
// engine thread
// ---------------------------------------------------------------------------

struct Thread {
    config: Config,
    state: Arc<Mutex<State>>,
    stop: Arc<AtomicBool>,
    wake_listener: Listener<S>,
}

/// The copy of the registered ports the engine thread looks at each round (locked only while copying).
enum Drain {
    Sub(Arc<Sub>, Key, Arc<Callback<Sample>>),
    Srv(Arc<Srv>, Key, Arc<QueryCallback>),
}

impl Thread {
    fn run(self) {
        let waitset = match WaitSetBuilder::new()
            .signal_handling_mode(SignalHandlingMode::Disabled)
            .create::<S>()
        {
            Ok(w) => w,
            Err(e) => {
                tracing::error!(error = ?e, "iceoryx2: creating the WaitSet failed; engine is dead");
                return;
            }
        };
        let mut last_poll: Option<Instant> = None;
        while !self.stop.load(Ordering::Relaxed) {
            let iteration = Instant::now();
            let entries: Vec<(Arc<Listener<S>>, Drain)> = {
                let state = lock(&self.state);
                state
                    .subscribers
                    .iter()
                    .map(|s| {
                        (
                            Arc::clone(&s.listener),
                            Drain::Sub(
                                Arc::clone(&s.subscriber),
                                s.pattern.clone(),
                                Arc::clone(&s.on_sample),
                            ),
                        )
                    })
                    .chain(state.servers.iter().map(|s| {
                        (
                            Arc::clone(&s.listener),
                            Drain::Srv(
                                Arc::clone(&s.server),
                                s.key.clone(),
                                Arc::clone(&s.on_query),
                            ),
                        )
                    }))
                    .collect()
            };
            {
                // Attach again every round — a guard borrows its listener, so re-attaching keeps
                // registrations coming and going from fighting lifetimes. `// ponytail: O(ports) per round`
                let mut guards = Vec::with_capacity(entries.len() + 1);
                for (listener, _) in &entries {
                    match waitset.attach_notification(&**listener) {
                        Ok(guard) => guards.push(guard),
                        Err(e) => {
                            tracing::warn!(error = ?e, "iceoryx2: attaching a listener failed");
                        }
                    }
                }
                let wake = waitset.attach_notification(&self.wake_listener);
                if let Err(e) = &wake {
                    tracing::warn!(error = ?e, "iceoryx2: attaching the wake listener failed");
                }
                let _ = waitset
                    .wait_and_process_once_with_timeout(|_| CallbackProgression::Continue, POLL);
            }
            let mut woke = false;
            let _ = self.wake_listener.try_wait_all(|_| woke = true);
            for (listener, drain) in &entries {
                let _ = listener.try_wait_all(|_| {});
                match drain {
                    Drain::Sub(subscriber, pattern, on_sample) => {
                        drain_subscriber(subscriber, pattern, on_sample);
                    }
                    Drain::Srv(server, key, on_query) => {
                        drain_server(server, key, on_query);
                    }
                }
            }
            let waited = iteration.elapsed();
            if woke || last_poll.is_none_or(|t| t.elapsed() >= POLL) {
                self.poll_presence();
                last_poll = Some(Instant::now());
            }
            let total = iteration.elapsed();
            if total > Duration::from_secs(1) {
                tracing::warn!(
                    ?waited,
                    ?total,
                    woke,
                    "iceoryx2: engine thread iteration stalled"
                );
            }
        }
    }

    fn poll_presence(&self) {
        let _ = Node::<S>::try_cleanup_dead_nodes(&self.config);
        let alive = list_alive(&self.config);
        let mut state = lock(&self.state);
        for watcher in &mut state.watchers {
            let current: HashSet<Key> = alive
                .iter()
                .filter(|k| watcher.pattern.matches(k))
                .cloned()
                .collect();
            for key in current.difference(&watcher.known) {
                tracing::trace!(%key, pattern = %watcher.pattern, "iceoryx2: joined");
                (watcher.on_event)(Presence::Joined(key.clone()));
            }
            for key in watcher.known.difference(&current) {
                tracing::trace!(%key, pattern = %watcher.pattern, "iceoryx2: left");
                (watcher.on_event)(Presence::Left(key.clone()));
            }
            watcher.known = current;
        }
    }
}

fn drain_subscriber(subscriber: &Sub, pattern: &Key, on_sample: &Callback<Sample>) {
    loop {
        let sample = match subscriber.receive() {
            Ok(Some(sample)) => sample,
            Ok(None) => return,
            Err(e) => {
                tracing::warn!(error = ?e, "iceoryx2: receive failed");
                return;
            }
        };
        let meta = sample.user_header();
        let Some(key) = meta.key() else { continue };
        if !pattern.matches(&key) {
            continue;
        }
        on_sample(Sample {
            key,
            payload: sample.payload().to_vec(),
            attachment: meta.attachment(),
            timestamp: meta.timestamp(),
        });
    }
}

fn drain_server(server: &Srv, key: &Key, on_query: &QueryCallback) {
    loop {
        let request = match server.receive() {
            Ok(Some(request)) => request,
            Ok(None) => return,
            Err(e) => {
                tracing::warn!(error = ?e, "iceoryx2: receiving a request failed");
                return;
            }
        };
        let meta = *request.user_header();
        let Some(query_key) = meta.key() else {
            continue;
        };
        // A request addressed elsewhere is dropped = finalized (how a zenoh queryable looks too).
        if !query_key.matches(key) {
            tracing::trace!(%query_key, responder = %key, "iceoryx2: request for another responder");
            continue;
        }
        tracing::trace!(%query_key, responder = %key, "iceoryx2: request received");
        let payload = (meta.flags & FLAG_PAYLOAD != 0).then(|| request.payload().to_vec());
        on_query(Box::new(Iox2Query {
            request,
            key: query_key,
            payload,
            attachment: meta.attachment(),
        }));
    }
}

// ---------------------------------------------------------------------------
// handles
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Slot {
    Subscriber,
    Server,
    Watcher,
}

struct Iox2Guard {
    state: Arc<Mutex<State>>,
    wake: Arc<Notifier<S>>,
    slot: Slot,
    id: u64,
}

impl Drop for Iox2Guard {
    fn drop(&mut self) {
        {
            let mut state = lock(&self.state);
            match self.slot {
                Slot::Subscriber => state.subscribers.retain(|s| s.id != self.id),
                Slot::Server => state.servers.retain(|s| s.id != self.id),
                Slot::Watcher => state.watchers.retain(|w| w.id != self.id),
            }
        }
        let _ = self.wake.notify();
    }
}

struct Iox2Publisher {
    key: Key,
    publisher: Pub,
    notifier: Notifier<S>,
}

impl RawPublisher for Iox2Publisher {
    fn put(&self, payload: Vec<u8>, attachment: Option<Vec<u8>>) -> Result<()> {
        let meta = Meta::new(&self.key, attachment.as_deref(), 0)?;
        let mut sample = self
            .publisher
            .loan_slice_uninit(payload.len())
            .map_err(err)?;
        *sample.user_header_mut() = meta;
        sample.write_from_slice(&payload).send().map_err(err)?;
        self.notifier.notify().map_err(err)?;
        Ok(())
    }
}

struct Iox2Query {
    request: Req,
    key: Key,
    payload: Option<Vec<u8>>,
    attachment: Option<Vec<u8>>,
}

impl RawQuery for Iox2Query {
    fn key(&self) -> &Key {
        &self.key
    }

    fn payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }

    fn attachment(&self) -> Option<&[u8]> {
        self.attachment.as_deref()
    }

    fn reply(
        self: Box<Self>,
        key: &Key,
        payload: Vec<u8>,
        attachment: Option<Vec<u8>>,
    ) -> Result<()> {
        let meta = Meta::new(key, attachment.as_deref(), 0)?;
        let mut response = self.request.loan_slice_uninit(payload.len()).map_err(err)?;
        *response.user_header_mut() = meta;
        response.write_from_slice(&payload).send().map_err(err)
        // `self.request` falls here = finalize.
    }

    fn reply_err(self: Box<Self>, message: Vec<u8>) -> Result<()> {
        let meta = Meta::new(&self.key, None, FLAG_ERROR)?;
        let mut response = self.request.loan_slice_uninit(message.len()).map_err(err)?;
        *response.user_header_mut() = meta;
        response.write_from_slice(&message).send().map_err(err)
    }
}

struct Iox2Replies {
    pending: Pending,
    deadline: tokio::time::Instant,
}

impl RawReplies for Iox2Replies {
    fn next(&mut self) -> BoxFuture<'_, Option<ReplyResult>> {
        Box::pin(async move {
            loop {
                // Look at the connection before receiving: nothing is missed when a "reply then drop" lands in between.
                let connected = self.pending.is_connected();
                match self.pending.receive() {
                    Ok(Some(response)) => {
                        let meta = *response.user_header();
                        let payload = response.payload().to_vec();
                        return Some(if meta.flags & FLAG_ERROR != 0 {
                            Err(payload)
                        } else {
                            let Some(key) = meta.key() else { continue };
                            Ok(Sample {
                                key,
                                payload,
                                attachment: meta.attachment(),
                                timestamp: meta.timestamp(),
                            })
                        });
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(error = ?e, "iceoryx2: receiving a response failed");
                        return None;
                    }
                }
                if !connected || tokio::time::Instant::now() >= self.deadline {
                    tracing::trace!(
                        connected,
                        servers = self.pending.number_of_server_connections(),
                        "iceoryx2: query finished"
                    );
                    return None;
                }
                tokio::time::sleep(REPLY_POLL).await;
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests may fail by panicking
mod tests {
    use super::*;
    use reiny::engine::SERVICE_CHUNK;

    fn key() -> Key {
        Key::topic("lab", Some("ctrl"), "RobotState")
    }

    /// iceoryx2 carries no key of its own — the whole address rides in the fixed-size header, so what
    /// goes in has to come back out exactly, verbatim chunks included.
    #[test]
    fn meta_round_trips_every_kind_of_key() {
        for original in [
            key(),
            Key::topic("lab", None, "RobotState"),
            Key::launch("lab", Some("ctrl")),
            key().with_chunk(SERVICE_CHUNK),
            key().with_chunk("@schema/hs.RobotState"),
        ] {
            let meta = Meta::new(&original, None, 0).expect("it fits");
            assert_eq!(meta.key(), Some(original.clone()), "{original}");
        }
    }

    /// The fingerprint travels in the header too. An absent attachment must come back as `None` rather
    /// than as an empty slice, or the fingerprint check would see "present but different".
    #[test]
    fn an_absent_attachment_is_none_not_empty() {
        let none = Meta::new(&key(), None, 0).unwrap();
        assert_eq!(none.attachment(), None);

        let some = Meta::new(&key(), Some(&0xdead_beef_u64.to_le_bytes()), 0).unwrap();
        assert_eq!(
            some.attachment(),
            Some(0xdead_beef_u64.to_le_bytes().to_vec())
        );

        // An explicitly empty attachment is indistinguishable from none; reiny never sends one.
        let empty = Meta::new(&key(), Some(&[]), 0).unwrap();
        assert_eq!(empty.attachment(), None);
    }

    /// The header is `#[repr(C)]` and fixed-size, so anything that does not fit has to be an error
    /// here rather than a truncated key that silently addresses a different topic.
    #[test]
    fn oversized_key_or_attachment_is_rejected() {
        let long_type = "T".repeat(KEY_MAX);
        let too_long = Key::topic("lab", Some("ctrl"), &long_type);
        let err = Meta::new(&too_long, None, 0).expect_err("the key does not fit");
        assert!(err.to_string().contains("longer than"), "{err}");

        let big = vec![0u8; ATTACHMENT_MAX + 1];
        let err = Meta::new(&key(), Some(&big), 0).expect_err("the attachment does not fit");
        assert!(err.to_string().contains("longer than"), "{err}");

        // Exactly at the limit still fits.
        let edge = vec![0u8; ATTACHMENT_MAX];
        assert_eq!(
            Meta::new(&key(), Some(&edge), 0).unwrap().attachment(),
            Some(edge)
        );
    }

    /// The flags byte is carried through untouched, and a timestamp of zero means "no time" rather
    /// than the epoch.
    #[test]
    fn flags_ride_along_and_a_zero_timestamp_is_none() {
        let meta = Meta::new(&key(), None, 0b0000_0101).unwrap();
        assert_eq!(meta.flags, 0b0000_0101);

        let mut no_time = meta;
        no_time.unix_ns = 0;
        assert_eq!(no_time.timestamp(), None);
        no_time.unix_ns = 42;
        assert_eq!(no_time.timestamp(), Some(42));
    }

    /// A truncated or non-UTF-8 key does not decode. It must be `None`, not a panic — the bytes come
    /// out of shared memory that any process on the host can write to.
    #[test]
    fn a_broken_key_in_the_header_decodes_to_none() {
        let mut meta = Meta::new(&key(), None, 0).unwrap();
        meta.key[0] = 0xff; // not UTF-8
        assert_eq!(meta.key(), None);

        let mut short = Meta::new(&key(), None, 0).unwrap();
        short.key_len = 5; // "reiny" alone is not a key
        assert_eq!(short.key(), None);
    }

    /// Presence is a service name, and `@` is deliberately avoided in one. The escaping has to survive
    /// a round trip for every verbatim chunk reiny uses, or a launch's own token would never be found.
    #[test]
    fn alive_names_escape_at_and_round_trip() {
        for original in [
            Key::launch("lab", Some("ctrl")),
            key(),
            key().with_chunk(SERVICE_CHUNK),
        ] {
            let name = alive_name(&original);
            assert!(name.starts_with("reiny-alive/"), "{name}");
            assert!(!name.contains('@'), "an @ survived into {name}");
            assert_eq!(parse_alive(&name), Some(original.clone()), "{original}");
        }
    }

    /// Anything that is not one of our presence names is not ours to interpret — `Service::list`
    /// returns every service on the host, including other processes' own.
    #[test]
    fn parse_alive_ignores_foreign_names() {
        assert_eq!(parse_alive("reiny/lab/ctrl/RobotState"), None);
        assert_eq!(parse_alive("some-other-service"), None);
        assert_eq!(parse_alive("reiny-alive/not-a-key"), None);
    }

    /// An all-types key has no service to map onto, so it is refused with a message that says so
    /// rather than producing an empty service name.
    #[test]
    fn a_key_without_a_type_is_refused() {
        let err = type_of(&Key::all("lab")).expect_err("no type in an all-types key");
        assert!(err.to_string().contains("names no type"), "{err}");
        assert_eq!(type_of(&key()).unwrap(), "RobotState");
    }
}
