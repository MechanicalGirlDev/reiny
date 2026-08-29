//! プロセス内バス —— [`Engine`] の基準実装であり、launch のユニットテストの足場。
//!
//! `Local::new()` を clone して複数の `Cloudy` に渡せば同じバスに乗る。ネットワークもポートも
//! zenoh 設定も要らない。配送は専用スレッド 1 本で、**全ての状態変更も同じスレッドで順に**
//! 処理する —— ロックが無く、宣言と配送の順序がそのまま保たれる。callback は非 async の
//! スレッドから、という [`Engine`] の契約もこれで満たす。
//!
//! `// ponytail: スレッド 1 本、Fifo が満杯なら callback がバスごと止まる。テスト用にはそれでよい`

use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use super::{
    BoxFuture, Callback, Caps, Engine, Guard, Key, Presence, QueryCallback, QueryParams,
    RawPublisher, RawQuery, RawReplies, ReplyResult, Sample, now_unix_ns,
};
use crate::{Qos, Result};

/// プロセス内バス。clone は同じバスを指す。
#[derive(Clone)]
pub struct Local {
    tx: mpsc::UnboundedSender<Op>,
}

impl Default for Local {
    fn default() -> Self {
        Self::new()
    }
}

impl Local {
    /// 新しいバス。配送スレッドは最後の clone(と取っ手)が落ちると終わる。
    #[must_use]
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        // スレッドを建てられない環境は無いものとする(tokio も建てられない)。
        let _ = std::thread::Builder::new()
            .name("reiny-local".into())
            .spawn(move || Bus::default().run(rx));
        Self { tx }
    }

    fn register(&self, op: Op) {
        // 配送スレッドが居ない(= 全 clone が落ちた後)ことは、この `self` が生きている限り無い。
        let _ = self.tx.send(op);
    }

    fn guard(&self, slot: Slot, id: u64) -> Guard {
        Box::new(LocalGuard {
            tx: self.tx.clone(),
            slot,
            id,
        })
    }
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

impl Engine for Local {
    fn caps(&self) -> Caps {
        Caps::ALL
    }

    fn publisher(&self, key: &Key, _qos: &Qos) -> Result<Box<dyn RawPublisher>> {
        Ok(Box::new(LocalPublisher {
            key: key.clone(),
            tx: self.tx.clone(),
        }))
    }

    fn subscribe(&self, key: &Key, on_sample: Callback<Sample>) -> Result<Guard> {
        let id = next_id();
        self.register(Op::Subscribe(id, key.clone(), on_sample));
        Ok(self.guard(Slot::Subscriber, id))
    }

    fn declare_alive(&self, key: &Key) -> Result<Guard> {
        let id = next_id();
        self.register(Op::Token(id, key.clone()));
        Ok(self.guard(Slot::Token, id))
    }

    fn alive(&self, key: &Key, _timeout: Duration) -> BoxFuture<'_, Result<Vec<Key>>> {
        let (tx, rx) = oneshot::channel();
        self.register(Op::Alive(key.clone(), tx));
        Box::pin(async move { Ok(rx.await.unwrap_or_default()) })
    }

    fn watch_alive(&self, key: &Key, on_event: Callback<Presence>) -> Result<Guard> {
        let id = next_id();
        self.register(Op::Watch(id, key.clone(), on_event));
        Ok(self.guard(Slot::Watcher, id))
    }

    fn respond(&self, key: &Key, on_query: QueryCallback) -> Result<Guard> {
        let id = next_id();
        self.register(Op::Respond(id, key.clone(), on_query));
        Ok(self.guard(Slot::Responder, id))
    }

    fn query(&self, key: &Key, params: QueryParams) -> Result<Box<dyn RawReplies>> {
        let (reply, rx) = mpsc::unbounded_channel();
        self.register(Op::Query {
            key: key.clone(),
            payload: params.payload,
            attachment: params.attachment,
            reply,
        });
        Ok(Box::new(LocalReplies {
            rx,
            deadline: tokio::time::Instant::now() + params.timeout,
        }))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Copy)]
enum Slot {
    Subscriber,
    Token,
    Watcher,
    Responder,
}

enum Op {
    Subscribe(u64, Key, Callback<Sample>),
    Publish(Sample),
    Token(u64, Key),
    Watch(u64, Key, Callback<Presence>),
    Respond(u64, Key, QueryCallback),
    Query {
        key: Key,
        payload: Option<Vec<u8>>,
        attachment: Option<Vec<u8>>,
        reply: mpsc::UnboundedSender<ReplyResult>,
    },
    Alive(Key, oneshot::Sender<Vec<Key>>),
    Remove(Slot, u64),
}

/// 配送スレッドだけが触る状態。
#[derive(Default)]
struct Bus {
    subscribers: Vec<(u64, Key, Callback<Sample>)>,
    tokens: Vec<(u64, Key)>,
    watchers: Vec<(u64, Key, Callback<Presence>)>,
    responders: Vec<(u64, Key, QueryCallback)>,
}

impl Bus {
    fn run(mut self, mut rx: mpsc::UnboundedReceiver<Op>) {
        while let Some(op) = rx.blocking_recv() {
            self.handle(op);
        }
    }

    fn handle(&mut self, op: Op) {
        match op {
            Op::Subscribe(id, key, cb) => self.subscribers.push((id, key, cb)),
            Op::Publish(sample) => {
                for (_, pattern, cb) in &self.subscribers {
                    if pattern.matches(&sample.key) {
                        cb(sample.clone());
                    }
                }
            }
            Op::Token(id, key) => {
                self.tokens.push((id, key.clone()));
                self.presence(&Presence::Joined(key));
            }
            Op::Watch(id, key, cb) => {
                // 宣言済みのトークンを Joined で先に流す(zenoh の `history(true)`)。
                for (_, token) in &self.tokens {
                    if key.matches(token) {
                        cb(Presence::Joined(token.clone()));
                    }
                }
                self.watchers.push((id, key, cb));
            }
            Op::Respond(id, key, cb) => self.responders.push((id, key, cb)),
            Op::Query {
                key,
                payload,
                attachment,
                reply,
            } => {
                for (_, responder, cb) in &self.responders {
                    if key.matches(responder) {
                        cb(Box::new(LocalQuery {
                            key: key.clone(),
                            payload: payload.clone(),
                            attachment: attachment.clone(),
                            reply: reply.clone(),
                        }));
                    }
                }
                // `reply` の最後の clone が落ちた時点で応答の列が閉じる = 全 responder が finalize。
            }
            Op::Alive(key, tx) => {
                let keys = self
                    .tokens
                    .iter()
                    .filter(|(_, token)| key.matches(token))
                    .map(|(_, token)| token.clone())
                    .collect();
                let _ = tx.send(keys);
            }
            Op::Remove(slot, id) => match slot {
                Slot::Subscriber => self.subscribers.retain(|(i, ..)| *i != id),
                Slot::Watcher => self.watchers.retain(|(i, ..)| *i != id),
                Slot::Responder => self.responders.retain(|(i, ..)| *i != id),
                Slot::Token => {
                    if let Some(pos) = self.tokens.iter().position(|(i, _)| *i == id) {
                        let (_, key) = self.tokens.remove(pos);
                        self.presence(&Presence::Left(key));
                    }
                }
            },
        }
    }

    fn presence(&self, event: &Presence) {
        let key = match event {
            Presence::Joined(k) | Presence::Left(k) => k,
        };
        for (_, pattern, cb) in &self.watchers {
            if pattern.matches(key) {
                cb(event.clone());
            }
        }
    }
}

struct LocalGuard {
    tx: mpsc::UnboundedSender<Op>,
    slot: Slot,
    id: u64,
}

impl Drop for LocalGuard {
    fn drop(&mut self) {
        let _ = self.tx.send(Op::Remove(self.slot, self.id));
    }
}

struct LocalPublisher {
    key: Key,
    tx: mpsc::UnboundedSender<Op>,
}

impl RawPublisher for LocalPublisher {
    fn put(&self, payload: Vec<u8>, attachment: Option<Vec<u8>>) -> Result<()> {
        self.tx
            .send(Op::Publish(Sample {
                key: self.key.clone(),
                payload,
                attachment,
                timestamp: now_unix_ns(),
            }))
            .map_err(|_| anyhow::anyhow!("local bus is gone"))
    }
}

struct LocalQuery {
    key: Key,
    payload: Option<Vec<u8>>,
    attachment: Option<Vec<u8>>,
    reply: mpsc::UnboundedSender<ReplyResult>,
}

impl RawQuery for LocalQuery {
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
        // 呼び出し側が既に居なければ捨てるだけ(zenoh も同じ)。
        let _ = self.reply.send(Ok(Sample {
            key: key.clone(),
            payload,
            attachment,
            timestamp: now_unix_ns(),
        }));
        Ok(())
    }

    fn reply_err(self: Box<Self>, message: Vec<u8>) -> Result<()> {
        let _ = self.reply.send(Err(message));
        Ok(())
    }
}

struct LocalReplies {
    rx: mpsc::UnboundedReceiver<ReplyResult>,
    deadline: tokio::time::Instant,
}

impl RawReplies for LocalReplies {
    fn next(&mut self) -> BoxFuture<'_, Option<ReplyResult>> {
        Box::pin(async move {
            // mpsc の `recv` は cancel-safe。期限を過ぎたら閉じたことにする(zenoh と同じ見え方)。
            tokio::time::timeout_at(self.deadline, self.rx.recv())
                .await
                .ok()
                .flatten()
        })
    }
}
