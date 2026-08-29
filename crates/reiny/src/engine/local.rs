//! The in-process bus — the reference implementation of [`Engine`] and the scaffolding for a launch's
//!
//! Clone what `Local::new()` returns into several `Cloudy`s and they share one bus. No network, no
//! port, no zenoh configuration. Delivery runs on a single dedicated thread, and **every state change
//! is processed in order on that same thread** — there are no locks, and the order of declarations
//! and deliveries is preserved as written. It also satisfies [`Engine`]'s contract that callbacks come
//!
//! `// ponytail: one thread; a full Fifo stops the whole bus from a callback. Fine for tests.`

use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use super::{
    BoxFuture, Callback, Caps, Engine, Guard, Key, Presence, QueryCallback, QueryParams,
    RawPublisher, RawQuery, RawReplies, ReplyResult, Sample, now_unix_ns,
};
use crate::{Qos, Result};

/// The in-process bus. A clone points at the same bus.
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
    /// A new bus. The delivery thread ends when the last clone (and handle) is dropped.
    #[must_use]
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        // Assume no environment where a thread cannot be spawned (tokio could not start there either).
        let _ = std::thread::Builder::new()
            .name("reiny-local".into())
            .spawn(move || Bus::default().run(rx));
        Self { tx }
    }

    fn register(&self, op: Op) {
        // The delivery thread cannot be gone (= all clones dropped) while this `self` is alive.
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

/// The state only the delivery thread touches.
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
                // Replay the already-declared tokens as Joined first (zenoh's `history(true)`).
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
                // The reply stream closes when the last clone of `reply` drops = every responder finalized.
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
        // If the caller is already gone, just drop it (zenoh does the same).
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
            // mpsc's `recv` is cancel-safe. Past the deadline, treat it as closed (how zenoh looks too).
            tokio::time::timeout_at(self.deadline, self.rx.recv())
                .await
                .ok()
                .flatten()
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)] // tests may fail by panicking
mod tests {
    use super::*;
    use std::sync::mpsc as std_mpsc;
    use std::time::Duration;

    /// Collect what an engine callback delivers. The callbacks run on the bus thread, so the test side
    /// reads them through a channel — which is exactly the shape reiny itself uses.
    fn collector<T: Send + 'static>() -> (Callback<T>, std_mpsc::Receiver<T>) {
        let (tx, rx) = std_mpsc::channel();
        let cb: Callback<T> = Box::new(move |item| {
            let _ = tx.send(item);
        });
        (cb, rx)
    }

    fn recv<T>(rx: &std_mpsc::Receiver<T>) -> T {
        rx.recv_timeout(Duration::from_secs(5))
            .expect("the bus delivered nothing in time")
    }

    fn topic(source: &str) -> Key {
        Key::topic("lab", Some(source), "T")
    }

    /// One dispatcher thread owns all state, so a declaration and the publishes that follow it are
    /// processed in that order: a subscriber declared before a publish always sees it, and the samples
    /// arrive in the order they were sent.
    #[tokio::test]
    async fn declaration_then_publishes_are_ordered() {
        let bus = Local::new();
        let (cb, rx) = collector::<Sample>();
        let _sub = bus.subscribe(&Key::topic("lab", None, "T"), cb).unwrap();

        let publisher = bus.publisher(&topic("a"), &Qos::DEFAULT).unwrap();
        for i in 0..5u8 {
            publisher.put(vec![i], None).unwrap();
        }
        for i in 0..5u8 {
            let sample = recv(&rx);
            assert_eq!(sample.payload, vec![i], "out of order at {i}");
            assert_eq!(sample.key.source.as_deref(), Some("a"));
        }
    }

    /// A `*` subscription takes every source, while one naming a source is a filter.
    #[tokio::test]
    async fn subscription_source_is_a_filter() {
        let bus = Local::new();
        let (any_cb, any_rx) = collector::<Sample>();
        let _any = bus
            .subscribe(&Key::topic("lab", None, "T"), any_cb)
            .unwrap();
        let (one_cb, one_rx) = collector::<Sample>();
        let _one = bus.subscribe(&topic("a"), one_cb).unwrap();

        bus.publisher(&topic("b"), &Qos::DEFAULT)
            .unwrap()
            .put(vec![2], None)
            .unwrap();
        bus.publisher(&topic("a"), &Qos::DEFAULT)
            .unwrap()
            .put(vec![1], None)
            .unwrap();

        // The filtered one only ever sees "a", and it sees it even though "b" was published first.
        assert_eq!(recv(&one_rx).payload, vec![1]);
        assert_eq!(recv(&any_rx).payload, vec![2]);
        assert_eq!(recv(&any_rx).payload, vec![1]);
    }

    /// Dropping a `Guard` undeclares. Nothing reaches a subscriber that is gone.
    #[tokio::test]
    async fn dropping_a_guard_undeclares() {
        let bus = Local::new();
        let (cb, rx) = collector::<Sample>();
        let sub = bus.subscribe(&Key::topic("lab", None, "T"), cb).unwrap();
        let publisher = bus.publisher(&topic("a"), &Qos::DEFAULT).unwrap();
        publisher.put(vec![1], None).unwrap();
        assert_eq!(recv(&rx).payload, vec![1]);

        drop(sub);
        publisher.put(vec![2], None).unwrap();
        // Ordering is total, so a sample delivered to a later subscriber proves the undeclare and the
        // publish above have both been processed.
        let (cb2, rx2) = collector::<Sample>();
        let _sub2 = bus.subscribe(&Key::topic("lab", None, "T"), cb2).unwrap();
        publisher.put(vec![3], None).unwrap();
        assert_eq!(recv(&rx2).payload, vec![3]);
        assert!(
            rx.try_recv().is_err(),
            "an undeclared subscriber still received a sample"
        );
    }

    /// Presence: a token is visible to `alive`, an existing one is replayed to a new watcher as
    /// `Joined` (zenoh's `history(true)`), and dropping it reports `Left`.
    #[tokio::test]
    async fn presence_replays_history_then_reports_leaving() {
        let bus = Local::new();
        let token = bus.declare_alive(&Key::launch("lab", Some("a"))).unwrap();

        let alive = bus
            .alive(&Key::launch("lab", None), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(alive, vec![Key::launch("lab", Some("a"))]);

        let (cb, rx) = collector::<Presence>();
        let _watch = bus.watch_alive(&Key::launch("lab", None), cb).unwrap();
        assert_eq!(recv(&rx), Presence::Joined(Key::launch("lab", Some("a"))));

        drop(token);
        assert_eq!(recv(&rx), Presence::Left(Key::launch("lab", Some("a"))));
        assert!(
            bus.alive(&Key::launch("lab", None), Duration::from_secs(5))
                .await
                .unwrap()
                .is_empty()
        );
    }

    /// A query finalizes when the last responder is done — including when there is no responder at
    /// all. That is what lets `Caller::call` tell `NoReply` from a timeout without waiting for one.
    #[tokio::test]
    async fn a_query_with_no_responder_closes_at_once() {
        let bus = Local::new();
        let params = QueryParams {
            payload: Some(vec![1]),
            attachment: None,
            timeout: Duration::from_secs(30), // far longer than this test may take
        };
        let mut replies = bus.query(&Key::topic("lab", None, "T"), params).unwrap();
        let started = tokio::time::Instant::now();
        assert!(replies.next().await.is_none());
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "it waited for the deadline instead of finalizing"
        );
    }

    /// A responder that drops its query without answering finalizes it too — forgetting to reply is a
    /// `NoReply`, never a hang.
    #[tokio::test]
    async fn a_dropped_query_finalizes_without_a_reply() {
        let bus = Local::new();
        let (tx, rx) = std_mpsc::channel();
        let on_query: QueryCallback = Box::new(move |q| {
            let _ = tx.send(q.payload().map(<[u8]>::to_vec));
            // `q` is dropped right here, unanswered.
        });
        let _responder = bus.respond(&topic("a"), on_query).unwrap();

        let params = QueryParams {
            payload: Some(vec![7]),
            attachment: None,
            timeout: Duration::from_secs(30),
        };
        let mut replies = bus.query(&Key::topic("lab", None, "T"), params).unwrap();
        assert!(replies.next().await.is_none(), "expected no reply");
        assert_eq!(recv(&rx), Some(vec![7]), "the responder did see the query");
    }

    /// A responder answers on its own concrete key, and a reply and an error both come back through
    /// the same stream, which then closes.
    #[tokio::test]
    async fn a_responder_replies_on_its_own_key() {
        let bus = Local::new();
        let on_query: QueryCallback = Box::new(move |q| {
            let payload = q.payload().map(<[u8]>::to_vec).unwrap_or_default();
            if payload == [0] {
                let _ = q.reply_err(b"refused".to_vec());
            } else {
                let key = Key::topic("lab", Some("a"), "T");
                let _ = q.reply(&key, vec![payload[0] + 1], None);
            }
        });
        let _responder = bus.respond(&topic("a"), on_query).unwrap();

        let ask = |payload: Vec<u8>| {
            bus.query(
                &Key::topic("lab", None, "T"),
                QueryParams {
                    payload: Some(payload),
                    attachment: None,
                    timeout: Duration::from_secs(30),
                },
            )
            .unwrap()
        };

        let mut replies = ask(vec![41]);
        let sample = replies
            .next()
            .await
            .expect("a reply")
            .expect("not an error");
        assert_eq!(sample.payload, vec![42]);
        assert_eq!(sample.key.source.as_deref(), Some("a"));
        assert!(replies.next().await.is_none(), "the stream closes after it");

        let mut replies = ask(vec![0]);
        let err = replies
            .next()
            .await
            .expect("a reply")
            .expect_err("an error");
        assert_eq!(err, b"refused".to_vec());
    }
}
