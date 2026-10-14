//! [`LinkEngine`] — a [`Host`] as reiny's [`Engine`].
//!
//! Across the link there is exactly one peer (an MCU). The types that peer declared in its Hello are
//! the entire world this engine shows, and the `Link` is run in bridge mode
//! ([`Host::bridge`](crate::Host::bridge)) — declaring no types of its own and mirroring the
//! peer's declarations instead:
//!
//! | reiny | link |
//! | --- | --- |
//! | subscribe | Data uses the peer namespace and output port; the attachment is its Hello fingerprint |
//! | publish | Only a peer input whose exact source matches the sending output receives data |
//! | presence | derived from the peer's Hello: `@launch`, a token per PUB type, `@sub` per SUB type, `@service` per SERVE type. Disconnected retracts all of them. Our own tokens are local only (the peer never hears about them) |
//! | query (with payload) | `call_raw(hash(namespace, server port))` returns Reply / Error |
//! | query (no payload = latched) | the engine remembers the peer's most recent Data for LATCHED types and answers with it |
//! | respond | A Request goes only to its declared destination server. Dropping it sends `Error("no reply")` |
//!
//! `Caps.attachment = false`: fingerprints travel in Hello, not arbitrary per-frame attachments.
//! Outgoing data and requests must supply a matching fingerprint before reaching the link.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use reiny::engine::{
    BoxFuture, Callback, Caps, EndpointSlot, Engine, Guard, Key, Presence, QueryCallback,
    QueryParams, RawPublisher, RawQuery, RawReplies, ReplyResult, SERVICE_CHUNK, SUB_CHUNK, Sample,
    now_unix_ns,
};
use reiny::{CongestionControl, Qos, Result};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::host::{CallError, Host, HostEvent, PeerType};
use crate::wire::{self, flags};

/// The [`Engine`] on top of a [`Host`]. Wrap it in an `Arc` and hand it to
/// `RuntimeOptions::engine`.
pub struct LinkEngine {
    host: Arc<Host>,
    domain: String,
    state: Arc<Mutex<State>>,
    task: JoinHandle<()>,
}

#[derive(Default)]
struct State {
    peer: Option<PeerInfo>,
    subscribers: Vec<(u64, Key, Callback<Sample>)>,
    responders: Vec<(u64, Key, QueryCallback)>,
    watchers: Vec<(u64, Key, Callback<Presence>)>,
    /// The tokens we declared. The peer never hears about them, but `alive` / `watch_alive` do.
    tokens: Vec<(u64, Key)>,
    /// The peer's most recent Data per LATCHED type (hash → sample). Answers payload-less queries.
    last: HashMap<u32, Sample>,
}

struct PeerInfo {
    id: String,
    types: Vec<PeerType>,
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The presence keys implied by the peer's Hello.
fn peer_keys(domain: &str, peer: &PeerInfo) -> Vec<Key> {
    let mut keys = vec![Key::module(domain, Some(&peer.id), "@launch")];
    for t in &peer.types {
        let topic = Key::output(domain, &peer.id, &t.port, &t.name);
        if t.flags & flags::PUB != 0 {
            keys.push(topic.clone());
        }
        // What the peer subscribes to is presence too: it is how the zenoh side's
        // `subscribers::<T>()` / `reiny topic list` can see what an MCU is listening for.
        if t.flags & flags::SUB != 0 {
            keys.push(Key::input(domain, &peer.id, &t.port, &t.name).with_chunk(SUB_CHUNK));
        }
        if t.flags & flags::SERVE != 0 {
            keys.push(topic.with_chunk(SERVICE_CHUNK));
        }
    }
    keys
}

fn fingerprint(schema: u64) -> Vec<u8> {
    schema.to_le_bytes().to_vec()
}

fn check_fingerprint(attachment: Option<&[u8]>, schema: u64) -> Result<()> {
    anyhow::ensure!(
        attachment == Some(schema.to_le_bytes().as_slice()),
        "link engine: missing or mismatching schema fingerprint"
    );
    Ok(())
}

impl State {
    fn emit(&self, event: &Presence) {
        let key = match event {
            Presence::Joined(k) | Presence::Left(k) => k,
        };
        for (_, pattern, cb) in &self.watchers {
            if pattern.matches(key) {
                cb(event.clone());
            }
        }
    }

    /// Every key currently up (our own tokens plus the peer's Hello).
    fn alive_keys(&self, domain: &str) -> Vec<Key> {
        let mut keys: Vec<Key> = self.tokens.iter().map(|(_, k)| k.clone()).collect();
        if let Some(peer) = &self.peer {
            keys.extend(peer_keys(domain, peer));
        }
        keys
    }
}

impl LinkEngine {
    /// Start running `host`. `domain` is the same one the `ModuleContext` above uses (a link has no domain
    /// of its own). Call it inside a tokio runtime.
    #[must_use]
    pub fn spawn(host: Host, domain: &str) -> Self {
        let host = Arc::new(host);
        let state = Arc::new(Mutex::new(State::default()));
        let task = tokio::spawn(run(
            Arc::clone(&host),
            domain.to_string(),
            Arc::clone(&state),
        ));
        Self {
            host,
            domain: domain.to_string(),
            state,
            task,
        }
    }

    /// The [`Host`] inside.
    #[must_use]
    pub fn host(&self) -> &Host {
        &self.host
    }

    fn guard(&self, slot: Slot, id: u64) -> Guard {
        Box::new(LinkGuard {
            state: Arc::clone(&self.state),
            slot,
            id,
        })
    }
}

impl Drop for LinkEngine {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn type_of(key: &Key) -> Result<&str> {
    key.ty.as_deref().ok_or_else(|| {
        anyhow::anyhow!("link engine: key '{key}' names no type (all-types keys are not supported)")
    })
}

fn endpoint_of(key: &Key) -> Result<u32> {
    type_of(key)?;
    anyhow::ensure!(
        matches!(key.endpoint, Some(EndpointSlot::Output(_))),
        "link engine: an exact output endpoint is required"
    );
    let namespace = key
        .source
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("link engine: an exact namespace is required"))?;
    let port = key
        .port()
        .ok_or_else(|| anyhow::anyhow!("link engine: an output port is required"))?;
    Ok(wire::endpoint_hash(namespace, port))
}

async fn run(host: Arc<Host>, domain: String, state: Arc<Mutex<State>>) {
    while let Some(event) = host.recv().await {
        match event {
            HostEvent::Connected { id, types } => {
                if Key::module(&domain, Some(&id), "@launch")
                    .validate()
                    .is_err()
                    || types.iter().any(|t| {
                        Key::output(&domain, &id, &t.port, &t.name)
                            .validate()
                            .is_err()
                            || wire::endpoint_hash(&id, &t.port) != t.hash
                    })
                {
                    tracing::warn!(%id, "link: peer Hello has an invalid namespace or type identity");
                    drop_peer(&domain, &state);
                    continue;
                }
                let mut st = lock(&state);
                let old = st
                    .peer
                    .take()
                    .map(|p| peer_keys(&domain, &p))
                    .unwrap_or_default();
                let info = PeerInfo { id, types };
                let new = peer_keys(&domain, &info);
                st.peer = Some(info);
                st.last.clear();
                for key in old.iter().filter(|k| !new.contains(k)) {
                    st.emit(&Presence::Left(key.clone()));
                }
                for key in new.iter().filter(|k| !old.contains(k)) {
                    st.emit(&Presence::Joined(key.clone()));
                }
            }
            HostEvent::Disconnected => drop_peer(&domain, &state),
            HostEvent::Data { hash, payload, .. } => {
                let mut st = lock(&state);
                let Some(peer) = &st.peer else { continue };
                let Some(t) = peer
                    .types
                    .iter()
                    .find(|t| t.hash == hash && t.flags & flags::PUB != 0)
                else {
                    continue;
                };
                let sample = Sample {
                    key: Key::output(&domain, &peer.id, &t.port, &t.name),
                    payload,
                    attachment: Some(fingerprint(t.schema)),
                    timestamp: now_unix_ns(),
                };
                if t.flags & flags::LATCHED != 0 {
                    st.last.insert(hash, sample.clone());
                }
                for (_, pattern, cb) in &st.subscribers {
                    if pattern.matches(&sample.key) {
                        cb(sample.clone());
                    }
                }
            }
            HostEvent::Request {
                hash, seq, payload, ..
            } => {
                let st = lock(&state);
                let Some(peer) = &st.peer else { continue };
                let Some(t) = peer.types.iter().find(|t| {
                    t.flags & flags::CALLS != 0
                        && t.sources
                            .iter()
                            .any(|(namespace, port)| wire::endpoint_hash(namespace, port) == hash)
                }) else {
                    let _ = host.reply_err_raw(seq, hash, "undeclared source");
                    continue;
                };
                let Some((namespace, port)) = t.sources.first() else {
                    continue;
                };
                let pattern = Key::output(&domain, namespace, port, &t.name);
                let Some((_, _, cb)) = st.responders.iter().find(|(_, k, _)| *k == pattern) else {
                    let _ = host.reply_err_raw(seq, hash, "no such service");
                    continue;
                };
                cb(Box::new(LinkQuery {
                    host: Arc::clone(&host),
                    seq,
                    hash,
                    key: pattern,
                    payload: Some(payload),
                    attachment: fingerprint(t.schema),
                    response: t.response.clone(),
                    answered: false,
                }));
            }
        }
    }
    // The driver stopped: the transport hit EOF or an error, so the peer is gone even though no
    // `Disconnected` (which is the link's own silence timer) was ever reported. Retract its presence
    // here too, or watchers would keep believing in a peer nothing can reach.
    drop_peer(&domain, &state);
}

/// Forget the peer and retract every presence key its Hello implied.
fn drop_peer(domain: &str, state: &Mutex<State>) {
    let mut st = lock(state);
    if let Some(peer) = st.peer.take() {
        st.last.clear();
        for key in peer_keys(domain, &peer) {
            st.emit(&Presence::Left(key));
        }
    }
}

impl Engine for LinkEngine {
    fn caps(&self) -> Caps {
        Caps {
            wildcard_source: true,
            liveliness: true,
            query: true,
            attachment: true,
            congestion_control: false,
        }
    }

    fn publisher(&self, key: &Key, qos: &Qos) -> Result<Box<dyn RawPublisher>> {
        key.validate()?;
        anyhow::ensure!(
            qos.congestion_control == CongestionControl::BackendDefault,
            "link engine: explicit congestion control is unsupported; a full send buffer returns an error"
        );
        anyhow::ensure!(
            key.domain == self.domain && key.chunk.is_none() && !key.is_verbatim_type(),
            "link engine: key '{key}' is not a data topic in domain '{}'",
            self.domain
        );
        Ok(Box::new(LinkPublisher {
            host: Arc::clone(&self.host),
            state: Arc::clone(&self.state),
            key: key.clone(),
            hash: endpoint_of(key)?,
        }))
    }

    fn subscribe(&self, key: &Key, on_sample: Callback<Sample>) -> Result<Guard> {
        key.validate()?;
        let id = next_id();
        lock(&self.state)
            .subscribers
            .push((id, key.clone(), on_sample));
        Ok(self.guard(Slot::Subscriber, id))
    }

    fn declare_alive(&self, key: &Key) -> Result<Guard> {
        key.validate()?;
        let id = next_id();
        let mut st = lock(&self.state);
        st.tokens.push((id, key.clone()));
        st.emit(&Presence::Joined(key.clone()));
        Ok(self.guard(Slot::Token, id))
    }

    fn alive(&self, key: &Key, _timeout: Duration) -> BoxFuture<'_, Result<Vec<Key>>> {
        if let Err(error) = key.validate() {
            return Box::pin(std::future::ready(Err(error)));
        }
        let keys: Vec<Key> = lock(&self.state)
            .alive_keys(&self.domain)
            .into_iter()
            .filter(|k| key.matches(k))
            .collect();
        Box::pin(std::future::ready(Ok(keys)))
    }

    fn watch_alive(&self, key: &Key, on_event: Callback<Presence>) -> Result<Guard> {
        key.validate()?;
        let id = next_id();
        let mut st = lock(&self.state);
        // Replay what is already declared as Joined first (zenoh's `history(true)`).
        for alive in st.alive_keys(&self.domain) {
            if key.matches(&alive) {
                on_event(Presence::Joined(alive));
            }
        }
        st.watchers.push((id, key.clone(), on_event));
        Ok(self.guard(Slot::Watcher, id))
    }

    fn respond(&self, key: &Key, on_query: QueryCallback) -> Result<Guard> {
        key.validate()?;
        let id = next_id();
        lock(&self.state)
            .responders
            .push((id, key.clone(), on_query));
        Ok(self.guard(Slot::Responder, id))
    }

    fn query(&self, key: &Key, params: QueryParams) -> Result<Box<dyn RawReplies>> {
        key.validate()?;
        let ty = type_of(key)?.to_string();
        let hash = endpoint_of(key)?;
        let Some(payload) = params.payload else {
            // A latched query: the engine is holding the peer's most recent value.
            let hit = lock(&self.state)
                .last
                .get(&hash)
                .filter(|s| key.matches(&s.key))
                .cloned();
            return Ok(Box::new(Ready(hit.map(Ok))));
        };
        let peer_id = lock(&self.state).peer.as_ref().map(|p| p.id.clone());
        let Some(peer_id) = peer_id else {
            return Ok(Box::new(Ready(None)));
        };
        let port = key
            .port()
            .ok_or_else(|| anyhow::anyhow!("missing output port"))?
            .to_string();
        let peer_key = Key::output(&self.domain, &peer_id, &port, &ty);
        if !key.matches(&peer_key) {
            return Ok(Box::new(Ready(None))); // wrong namespace, domain or chunk: no replies
        }
        {
            let st = lock(&self.state);
            let Some(t) = st.peer.as_ref().and_then(|p| {
                p.types
                    .iter()
                    .find(|t| t.hash == hash && t.flags & flags::SERVE != 0 && t.name == ty)
            }) else {
                return Ok(Box::new(Ready(None)));
            };
            check_fingerprint(params.attachment.as_deref(), t.schema)?;
        }
        let host = Arc::clone(&self.host);
        let domain = self.domain.clone();
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let result = match host.call_raw(hash, payload, params.timeout).await {
                Ok((schema, bytes)) => Some(Ok(Sample {
                    key: Key::output(&domain, &peer_id, &port, &ty),
                    payload: bytes,
                    attachment: Some(schema.to_le_bytes().to_vec()),
                    timestamp: now_unix_ns(),
                })),
                Err(CallError::Remote(message)) => Some(Err(message.into_bytes())),
                Err(_) => None, // NoPeerService / Timeout / Closed = no replies
            };
            let _ = tx.send(result);
        });
        Ok(Box::new(Once { rx: Some(rx) }))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Clone, Copy)]
enum Slot {
    Subscriber,
    Responder,
    Watcher,
    Token,
}

struct LinkGuard {
    state: Arc<Mutex<State>>,
    slot: Slot,
    id: u64,
}

impl Drop for LinkGuard {
    fn drop(&mut self) {
        let mut st = lock(&self.state);
        match self.slot {
            Slot::Subscriber => st.subscribers.retain(|(i, ..)| *i != self.id),
            Slot::Responder => st.responders.retain(|(i, ..)| *i != self.id),
            Slot::Watcher => st.watchers.retain(|(i, ..)| *i != self.id),
            Slot::Token => {
                if let Some(pos) = st.tokens.iter().position(|(i, _)| *i == self.id) {
                    let (_, key) = st.tokens.remove(pos);
                    st.emit(&Presence::Left(key));
                }
            }
        }
    }
}

struct LinkPublisher {
    key: Key,
    host: Arc<Host>,
    state: Arc<Mutex<State>>,
    hash: u32,
}

impl RawPublisher for LinkPublisher {
    fn put(&self, payload: Vec<u8>, attachment: Option<Vec<u8>>) -> Result<()> {
        {
            let st = lock(&self.state);
            let Some(t) = st.peer.as_ref().and_then(|p| {
                p.types.iter().find(|t| {
                    t.flags & flags::SUB != 0
                        && t.name == self.key.ty.as_deref().unwrap_or("")
                        && t.sources.iter().any(|(namespace, port)| {
                            self.key.source.as_deref() == Some(namespace.as_str())
                                && self.key.port() == Some(port.as_str())
                        })
                })
            }) else {
                return Ok(()); // no subscriber
            };
            check_fingerprint(attachment.as_deref(), t.schema)?;
        }
        // `Ok(false)` when the peer does not subscribe = simply dropped (publish is fire and forget).
        self.host
            .send_raw(self.hash, &payload)
            .map(|_| ())
            .map_err(|e| anyhow::anyhow!("link: {e}"))
    }
}

struct LinkQuery {
    host: Arc<Host>,
    seq: u8,
    hash: u32,
    key: Key,
    payload: Option<Vec<u8>>,
    attachment: Vec<u8>,
    response: Option<(String, u64)>,
    answered: bool,
}

impl RawQuery for LinkQuery {
    fn key(&self) -> &Key {
        &self.key
    }

    fn payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }

    fn attachment(&self) -> Option<&[u8]> {
        Some(&self.attachment)
    }

    fn reply(
        mut self: Box<Self>,
        key: &Key,
        payload: Vec<u8>,
        attachment: Option<Vec<u8>>,
    ) -> Result<()> {
        let schema = attachment
            .as_deref()
            .and_then(|a| a.try_into().ok())
            .map(u64::from_le_bytes)
            .ok_or_else(|| anyhow::anyhow!("link reply requires a schema fingerprint"))?;
        anyhow::ensure!(
            *key == self.key,
            "link reply must preserve its destination server endpoint"
        );
        anyhow::ensure!(
            self.response
                .as_ref()
                .is_some_and(|(_, expected)| *expected == schema),
            "link response fingerprint mismatch"
        );
        self.answered = true;
        self.host
            .reply_raw(self.seq, self.hash, schema, &payload)
            .map_err(|e| anyhow::anyhow!("link: {e}"))
    }

    fn reply_err(mut self: Box<Self>, message: Vec<u8>) -> Result<()> {
        self.answered = true;
        self.host
            .reply_err_raw(self.seq, self.hash, &String::from_utf8_lossy(&message))
            .map_err(|e| anyhow::anyhow!("link: {e}"))
    }
}

impl Drop for LinkQuery {
    fn drop(&mut self) {
        // A link has no finalize: a request dropped without an answer is turned into an error so
        // the peer is not left waiting (the counterpart of zenoh's "no replies = NoReply").
        if !self.answered {
            let _ = self.host.reply_err_raw(self.seq, self.hash, "no reply");
        }
    }
}

/// A reply stream that ends after the one (or zero) result it already holds.
struct Ready(Option<ReplyResult>);

impl RawReplies for Ready {
    fn next(&mut self) -> BoxFuture<'_, Option<ReplyResult>> {
        Box::pin(std::future::ready(self.0.take()))
    }
}

/// A reply stream that yields `call_raw`'s single result and ends. Cancel-safe (the receiver is
/// polled through `&mut`).
struct Once {
    rx: Option<oneshot::Receiver<Option<ReplyResult>>>,
}

impl RawReplies for Once {
    fn next(&mut self) -> BoxFuture<'_, Option<ReplyResult>> {
        Box::pin(async move {
            let rx = self.rx.as_mut()?;
            let result = rx.await.ok().flatten();
            self.rx = None;
            result
        })
    }
}
