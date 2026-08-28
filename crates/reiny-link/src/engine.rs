//! [`LinkEngine`] —— [`Host`] を reiny の [`Engine`] として。
//!
//! リンクの向こうは 1 台の相手(MCU)。相手が Hello で名乗った型が、この engine が見せる世界の
//! 全部で、`Link` は bridge モード([`Link::as_bridge`](crate::Link::as_bridge))で回す ——
//! 型を名乗らず、相手の宣言の鏡になる:
//!
//! | reiny | link |
//! | --- | --- |
//! | subscribe `reiny/<d>/*/<T>` | 相手からの Data(hash = T)。source は相手の id、attachment は Hello の指紋 |
//! | publish `reiny/<d>/<id>/<T>` | `send_raw(hash(T))`。相手が subscribe していなければ捨てる |
//! | presence | 相手の Hello から: `@grain`、PUB 型のトークン、SERVE 型の `@service`。Disconnected で全部 Left。自分のトークンはローカルにだけ立つ(相手には伝わらない) |
//! | query(payload あり) | `call_raw(hash(S))` → Reply / Error |
//! | query(payload なし = latched) | 相手の LATCHED な型の直近 Data を engine が覚えていて返す |
//! | respond | 相手からの Request(hash = S)を、型 `S` の responder へ。応えずに drop すると相手には `Error("no reply")` |
//!
//! `Caps.attachment = false`: 指紋は Hello で運ぶ(`put` の attachment は捨てる)。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use reiny::engine::{
    BoxFuture, Callback, Caps, Engine, Guard, Key, Presence, QueryCallback, QueryParams,
    RawPublisher, RawQuery, RawReplies, ReplyResult, SERVICE_CHUNK, Sample, now_unix_ns,
};
use reiny::{Qos, Result};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::host::{CallError, Host, HostEvent, PeerType};
use crate::wire::{self, flags};

/// [`Host`] の上の [`Engine`]。`Arc` に包んで `RuntimeOptions::engine` に渡す。
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
    /// 自分が立てたトークン。相手には伝わらないが、`alive` / `watch_alive` には見える。
    tokens: Vec<(u64, Key)>,
    /// 相手の LATCHED な型の直近 Data(hash → sample)。payload の無い query に答える。
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

/// 相手の Hello が意味する presence キー。
fn peer_keys(domain: &str, peer: &PeerInfo) -> Vec<Key> {
    let mut keys = vec![Key::grain(domain, Some(&peer.id))];
    for t in &peer.types {
        let topic = Key::topic(domain, Some(&peer.id), &t.name);
        if t.flags & flags::PUB != 0 {
            keys.push(topic.clone());
        }
        if t.flags & flags::SERVE != 0 {
            keys.push(topic.with_chunk(SERVICE_CHUNK));
        }
    }
    keys
}

fn fingerprint(schema: Option<u64>) -> Option<Vec<u8>> {
    schema.map(|s| s.to_le_bytes().to_vec())
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

    /// いま立っている全キー(自分のトークン + 相手の Hello)。
    fn alive_keys(&self, domain: &str) -> Vec<Key> {
        let mut keys: Vec<Key> = self.tokens.iter().map(|(_, k)| k.clone()).collect();
        if let Some(peer) = &self.peer {
            keys.extend(peer_keys(domain, peer));
        }
        keys
    }
}

impl LinkEngine {
    /// `host` を回し始める。`domain` は上に乗る `Cloudy` と同じもの(リンクには domain が無い)。
    /// tokio runtime の中で呼ぶ。
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

    /// 中の [`Host`]。
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

async fn run(host: Arc<Host>, domain: String, state: Arc<Mutex<State>>) {
    while let Some(event) = host.recv().await {
        match event {
            HostEvent::Connected { id, types } => {
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
            HostEvent::Disconnected => {
                let mut st = lock(&state);
                if let Some(peer) = st.peer.take() {
                    st.last.clear();
                    for key in peer_keys(&domain, &peer) {
                        st.emit(&Presence::Left(key));
                    }
                }
            }
            HostEvent::Data { hash, payload, .. } => {
                let mut st = lock(&state);
                let Some(peer) = &st.peer else { continue };
                let Some(t) = peer.types.iter().find(|t| t.hash == hash) else {
                    continue;
                };
                let sample = Sample {
                    key: Key::topic(&domain, Some(&peer.id), &t.name),
                    payload,
                    attachment: fingerprint(t.schema),
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
            HostEvent::Request { hash, seq, payload } => {
                let st = lock(&state);
                let Some(peer) = &st.peer else { continue };
                // 相手が `calls` で名乗っていない型は名前が分からないので断る。
                let Some(t) = peer.types.iter().find(|t| t.hash == hash) else {
                    let _ = host.reply_err_raw(
                        seq,
                        hash,
                        "unknown request type (declare it with calls())",
                    );
                    continue;
                };
                // リンクの request に宛先は無い: 型 `S` の responder なら誰でもよい。
                let pattern = Key::topic(&domain, None, &t.name);
                let Some((_, _, cb)) = st.responders.iter().find(|(_, k, _)| pattern.matches(k))
                else {
                    let _ = host.reply_err_raw(seq, hash, "no such service");
                    continue;
                };
                cb(Box::new(LinkQuery {
                    host: Arc::clone(&host),
                    seq,
                    hash,
                    key: pattern,
                    payload: Some(payload),
                    answered: false,
                }));
            }
        }
    }
}

impl Engine for LinkEngine {
    fn caps(&self) -> Caps {
        Caps {
            wildcard_source: true,
            liveliness: true,
            query: true,
            attachment: false,
        }
    }

    fn publisher(&self, key: &Key, _qos: &Qos) -> Result<Box<dyn RawPublisher>> {
        Ok(Box::new(LinkPublisher {
            host: Arc::clone(&self.host),
            hash: wire::type_hash(type_of(key)?),
        }))
    }

    fn subscribe(&self, key: &Key, on_sample: Callback<Sample>) -> Result<Guard> {
        let id = next_id();
        lock(&self.state)
            .subscribers
            .push((id, key.clone(), on_sample));
        Ok(self.guard(Slot::Subscriber, id))
    }

    fn declare_alive(&self, key: &Key) -> Result<Guard> {
        let id = next_id();
        let mut st = lock(&self.state);
        st.tokens.push((id, key.clone()));
        st.emit(&Presence::Joined(key.clone()));
        Ok(self.guard(Slot::Token, id))
    }

    fn alive(&self, key: &Key, _timeout: Duration) -> BoxFuture<'_, Result<Vec<Key>>> {
        let keys: Vec<Key> = lock(&self.state)
            .alive_keys(&self.domain)
            .into_iter()
            .filter(|k| key.matches(k))
            .collect();
        Box::pin(std::future::ready(Ok(keys)))
    }

    fn watch_alive(&self, key: &Key, on_event: Callback<Presence>) -> Result<Guard> {
        let id = next_id();
        let mut st = lock(&self.state);
        // 宣言済みは Joined で先に流す(zenoh の `history(true)`)。
        for alive in st.alive_keys(&self.domain) {
            if key.matches(&alive) {
                on_event(Presence::Joined(alive));
            }
        }
        st.watchers.push((id, key.clone(), on_event));
        Ok(self.guard(Slot::Watcher, id))
    }

    fn respond(&self, key: &Key, on_query: QueryCallback) -> Result<Guard> {
        let id = next_id();
        lock(&self.state)
            .responders
            .push((id, key.clone(), on_query));
        Ok(self.guard(Slot::Responder, id))
    }

    fn query(&self, key: &Key, params: QueryParams) -> Result<Box<dyn RawReplies>> {
        let ty = type_of(key)?.to_string();
        let hash = wire::type_hash(&ty);
        let Some(payload) = params.payload else {
            // latched の問い合わせ: 相手の直近値を engine が覚えている。
            let hit = lock(&self.state)
                .last
                .get(&hash)
                .filter(|s| key.matches(&s.key))
                .cloned();
            return Ok(Box::new(Ready(hit.map(Ok))));
        };
        let peer_id = lock(&self.state).peer.as_ref().map(|p| p.id.clone());
        if key
            .source
            .as_deref()
            .is_some_and(|s| peer_id.as_deref() != Some(s))
        {
            return Ok(Box::new(Ready(None))); // 宛先が相手ではない: 応答ゼロ
        }
        let host = Arc::clone(&self.host);
        let domain = self.domain.clone();
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let result = match host.call_raw(hash, payload, params.timeout).await {
                Ok(bytes) => Some(Ok(Sample {
                    key: Key::topic(&domain, peer_id.as_deref(), &ty),
                    payload: bytes,
                    attachment: None,
                    timestamp: now_unix_ns(),
                })),
                Err(CallError::Remote(message)) => Some(Err(message.into_bytes())),
                Err(_) => None, // NoPeerService / Timeout / Closed = 応答ゼロ
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
    host: Arc<Host>,
    hash: u32,
}

impl RawPublisher for LinkPublisher {
    fn put(&self, payload: Vec<u8>, _attachment: Option<Vec<u8>>) -> Result<()> {
        // 相手が subscribe していなければ `Ok(false)` = 捨てるだけ(publish は fire and forget)。
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
        None
    }

    fn reply(
        mut self: Box<Self>,
        _key: &Key,
        payload: Vec<u8>,
        _attachment: Option<Vec<u8>>,
    ) -> Result<()> {
        self.answered = true;
        self.host
            .reply_raw(self.seq, self.hash, &payload)
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
        // リンクに finalize は無い: 応えずに落とされた request は、相手を待たせないよう
        // エラーにして返す(zenoh の「応答ゼロ = NoReply」に当たる)。
        if !self.answered {
            let _ = self.host.reply_err_raw(self.seq, self.hash, "no reply");
        }
    }
}

/// 手持ちの 1 件(または 0 件)で終わる応答列。
struct Ready(Option<ReplyResult>);

impl RawReplies for Ready {
    fn next(&mut self) -> BoxFuture<'_, Option<ReplyResult>> {
        Box::pin(std::future::ready(self.0.take()))
    }
}

/// `call_raw` の結果を 1 件返して終わる応答列。cancel-safe(受け手を `&mut` で poll する)。
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
