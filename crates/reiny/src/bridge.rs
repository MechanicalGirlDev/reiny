//! raw bridge —— 2 つのエンジンの間で sample / presence / query を**バイトのまま**写す。
//!
//! [`forward`] は同じ id / domain の 2 つの [`Cloudy`] —— `a` と `b`、2 本目は
//! [`Cloudy::with_engine`] で組む —— を受け、両方向に:
//!
//! - **presence**: A に立った publisher / service / launch のトークンを、**同じキー(source も
//!   同じ)**で B に立てる。B から見ると A の launch がそのまま居る。
//! - **sample**: トークンが見えた型を A で購読し、B に同じキーで publish する(attachment =
//!   指紋もそのまま)。
//! - **query**: B に写したトークンごとに B で responder を立て、届いた query を A の**その
//!   source 宛て**の query に転送し、最初の応答を返す。latched も service も同じ経路。
//!
//! ループ防止は 1 規則: **自分が B に写した source から B で見えたもの(sample / トークン)は
//! A のエコーなので写さない。** query は転送先を必ず具体的な source にする —— B の `*` query
//! は B の responder(= A の各 source)それぞれに届き、それぞれが A の同じ source だけに
//! 転送するので、A の bridge responder には当たらない。
//!
//! エンジンの型は問わない(`Local` ↔ `Local` でテストする)。`reiny bridge serial|udp|iceoryx2`
//! はこれを zenoh とリンク / iceoryx2 の間に置く。
//!
//! `// ponytail: A で購読するのは「トークンが見えた型 × 全 source」。B が要る型だけに絞るのは
//! エンジンに「誰が欲しがっているか」を聞く口ができてから`

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::engine::{
    Engine, Guard, Key, Presence, QueryParams, RawPublisher, RawQuery, SERVICE_CHUNK, Sample,
};
use crate::{Cloudy, Qos, Result};

/// 転送した query の期限(zenoh の `get` の既定と同じ)。
const QUERY_TIMEOUT: Duration = Duration::from_secs(10);

/// [`forward`] の取っ手。drop すると両方向の転送と、写したトークン / 購読が全部消える。
pub struct Bridge {
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for Bridge {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// `a` と `b` の間を両方向に繋ぐ。id と domain が同じでなければエラー。tokio runtime の中で呼ぶ。
pub fn forward(a: &Cloudy, b: &Cloudy) -> Result<Bridge> {
    if a.id() != b.id() || a.domain() != b.domain() {
        anyhow::bail!(
            "bridge: both sides must share id and domain (got {}/{} and {}/{})",
            a.domain(),
            a.id(),
            b.domain(),
            b.id()
        );
    }
    let into_b = Arc::new(Injected::default());
    let into_a = Arc::new(Injected::default());
    let ab = Flow::start(a, b, Arc::clone(&into_b), Arc::clone(&into_a))?;
    let ba = Flow::start(b, a, into_a, into_b)?;
    Ok(Bridge {
        tasks: vec![ab, ba],
    })
}

/// ある側に bridge が写し込んだ source(→ トークン数)。写した先で見えても「エコー」と分かる。
#[derive(Default)]
struct Injected(Mutex<HashMap<String, usize>>);

impl Injected {
    fn add(&self, source: &str) {
        *lock(&self.0).entry(source.to_string()).or_insert(0) += 1;
    }

    fn remove(&self, source: &str) {
        let mut map = lock(&self.0);
        if let Some(n) = map.get_mut(source) {
            *n -= 1;
            if *n == 0 {
                map.remove(source);
            }
        }
    }

    fn contains(&self, source: &str) -> bool {
        lock(&self.0).contains_key(source)
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

enum Op {
    Sample(Sample),
    Presence(Presence),
}

/// 片方向(`from` → `into`)の状態。転送 task が所有する。
struct Flow {
    id: String,
    domain: String,
    from: Arc<dyn Engine>,
    into: Arc<dyn Engine>,
    /// この flow が `into` に写した source。
    injected: Arc<Injected>,
    /// 逆向きの flow が `from` に写した source(`from` で見えたらエコー)。
    echoes: Arc<Injected>,
    /// `from` の購読(型ごと)。その型の publisher トークンが 1 つでも見えている間だけ。
    subscriptions: HashMap<String, Guard>,
    type_refs: HashMap<String, usize>,
    /// `into` に写したトークン。
    tokens: HashMap<Key, Guard>,
    /// `into` の publisher(写した source × 型)。
    publishers: HashMap<Key, Box<dyn RawPublisher>>,
    /// `into` の responder(写した source × 型)。latched も service も 1 本で受ける。
    responders: HashMap<Key, Guard>,
    /// `from` の見張り(保持するだけ)。
    _watchers: Vec<Guard>,
    handle: Handle,
    ops: mpsc::UnboundedSender<Op>,
}

impl Flow {
    fn start(
        from: &Cloudy,
        into: &Cloudy,
        injected: Arc<Injected>,
        echoes: Arc<Injected>,
    ) -> Result<JoinHandle<()>> {
        let (ops, rx) = mpsc::unbounded_channel();
        let domain = from.domain().to_string();
        let mut watchers = Vec::new();
        for pattern in [
            Key::all(&domain),
            Key::all(&domain).with_chunk(SERVICE_CHUNK),
            Key::launch(&domain, None),
        ] {
            let tx = ops.clone();
            watchers.push(from.engine().watch_alive(
                &pattern,
                Box::new(move |event| {
                    let _ = tx.send(Op::Presence(event));
                }),
            )?);
        }
        let flow = Self {
            id: from.id().to_string(),
            domain,
            from: Arc::clone(from.engine()),
            into: Arc::clone(into.engine()),
            injected,
            echoes,
            subscriptions: HashMap::new(),
            type_refs: HashMap::new(),
            tokens: HashMap::new(),
            publishers: HashMap::new(),
            responders: HashMap::new(),
            _watchers: watchers,
            handle: Handle::current(),
            ops,
        };
        Ok(tokio::spawn(flow.run(rx)))
    }

    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Op>) {
        while let Some(op) = rx.recv().await {
            match op {
                Op::Sample(sample) => self.sample(sample),
                Op::Presence(Presence::Joined(key)) => self.joined(&key),
                Op::Presence(Presence::Left(key)) => self.left(&key),
            }
        }
    }

    /// 写すべき source か。自分(bridge)と、逆向きに写されたエコーは写さない。
    fn foreign<'k>(&self, key: &'k Key) -> Option<&'k str> {
        let source = key.source.as_deref()?;
        (source != self.id && !self.echoes.contains(source)).then_some(source)
    }

    fn sample(&mut self, sample: Sample) {
        if self.foreign(&sample.key).is_none() {
            return;
        }
        let publisher = match self.publishers.entry(sample.key.clone()) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => {
                match self.into.publisher(e.key(), &Qos::DEFAULT) {
                    Ok(p) => e.insert(p),
                    Err(err) => {
                        tracing::warn!(key = %sample.key, error = %err, "bridge: publisher failed");
                        return;
                    }
                }
            }
        };
        if let Err(err) = publisher.put(sample.payload, sample.attachment) {
            tracing::warn!(key = %sample.key, error = %err, "bridge: forward failed");
        }
    }

    fn joined(&mut self, key: &Key) {
        let Some(source) = self.foreign(key).map(str::to_string) else {
            return;
        };
        if self.tokens.contains_key(key) {
            return;
        }
        // トークンを写す前に「写した」と記録する —— 逆向きの見張りが先に見ても弾けるように。
        self.injected.add(&source);
        match self.into.declare_alive(key) {
            Ok(guard) => {
                self.tokens.insert(key.clone(), guard);
            }
            Err(err) => {
                tracing::warn!(key = %key, error = %err, "bridge: token failed");
                self.injected.remove(&source);
                return;
            }
        }
        tracing::debug!(key = %key, "bridge: joined");
        let topic = Key {
            chunk: None,
            ..key.clone()
        };
        if key.is_verbatim_type() {
            return; // @launch: トークンだけ
        }
        if key.chunk.is_none() {
            // publisher トークン: その型を from で購読する(初めての型なら)。
            if let Some(ty) = key.ty.clone() {
                *self.type_refs.entry(ty.clone()).or_insert(0) += 1;
                if !self.subscriptions.contains_key(&ty) {
                    let tx = self.ops.clone();
                    match self.from.subscribe(
                        &Key::topic(&self.domain, None, &ty),
                        Box::new(move |sample| {
                            let _ = tx.send(Op::Sample(sample));
                        }),
                    ) {
                        Ok(guard) => {
                            self.subscriptions.insert(ty, guard);
                        }
                        Err(err) => tracing::warn!(%ty, error = %err, "bridge: subscribe failed"),
                    }
                }
            }
        }
        // publisher でも service でも、その source × 型への query を from に転送する。
        if !self.responders.contains_key(&topic) {
            let from = Arc::clone(&self.from);
            let target = topic.clone();
            let handle = self.handle.clone();
            let result = self.into.respond(
                &topic,
                Box::new(move |query: Box<dyn RawQuery>| {
                    let from = Arc::clone(&from);
                    let target = target.clone();
                    handle.spawn(relay_query(from, target, query));
                }),
            );
            match result {
                Ok(guard) => {
                    self.responders.insert(topic, guard);
                }
                Err(err) => tracing::warn!(key = %topic, error = %err, "bridge: responder failed"),
            }
        }
    }

    fn left(&mut self, key: &Key) {
        let Some(source) = key.source.clone() else {
            return;
        };
        if self.tokens.remove(key).is_none() {
            return;
        }
        self.injected.remove(&source);
        tracing::debug!(%key, "bridge: left");
        let topic = Key {
            chunk: None,
            ..key.clone()
        };
        if key.chunk.is_none()
            && !key.is_verbatim_type()
            && let Some(ty) = &key.ty
            && let Some(n) = self.type_refs.get_mut(ty)
        {
            *n -= 1;
            if *n == 0 {
                self.type_refs.remove(ty);
                self.subscriptions.remove(ty);
            }
            self.publishers.remove(key);
        }
        let still_needed = self.tokens.contains_key(&topic)
            || self.tokens.contains_key(&topic.with_chunk(SERVICE_CHUNK));
        if !still_needed {
            self.responders.remove(&topic);
        }
    }
}

/// `into` に届いた query を `from` の具体的な source 宛てに転送し、最初の応答を返す。
async fn relay_query(from: Arc<dyn Engine>, target: Key, query: Box<dyn RawQuery>) {
    let params = QueryParams {
        payload: query.payload().map(<[u8]>::to_vec),
        attachment: query.attachment().map(<[u8]>::to_vec),
        timeout: QUERY_TIMEOUT,
    };
    let mut replies = match from.query(&target, params) {
        Ok(replies) => replies,
        Err(err) => {
            tracing::warn!(key = %target, error = %err, "bridge: relayed query failed");
            return; // drop = finalize
        }
    };
    match replies.next().await {
        Some(Ok(sample)) => {
            if let Err(err) = query.reply(&sample.key, sample.payload, sample.attachment) {
                tracing::warn!(key = %target, error = %err, "bridge: relayed reply failed");
            }
        }
        Some(Err(message)) => {
            let _ = query.reply_err(message);
        }
        None => {} // 応答ゼロ: query を drop して finalize
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;
    use crate::engine::Local;
    use crate::{CallError, PresenceEvent, Service, Topic, shutdown::Shutdown};

    #[derive(Clone, PartialEq, prost::Message)]
    struct Probe {
        #[prost(uint32, tag = "1")]
        seq: u32,
    }
    impl Topic for Probe {
        const TYPE: &'static str = "BridgeProbe";
        const SCHEMA: Option<u64> = Some(0x77);
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct Cmd {
        #[prost(uint32, tag = "1")]
        v: u32,
    }
    impl Topic for Cmd {
        const TYPE: &'static str = "BridgeCmd";
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct Add {
        #[prost(int32, tag = "1")]
        a: i32,
        #[prost(int32, tag = "2")]
        b: i32,
    }
    #[derive(Clone, PartialEq, prost::Message)]
    struct Sum {
        #[prost(int32, tag = "1")]
        sum: i32,
    }
    impl Topic for Add {
        const TYPE: &'static str = "BridgeAdd";
    }
    impl Topic for Sum {
        const TYPE: &'static str = "BridgeSum";
    }
    impl Service for Add {
        type Response = Sum;
    }

    const DOMAIN: &str = "br";
    const SETTLE: Duration = Duration::from_millis(300);
    const PATIENCE: Duration = Duration::from_secs(5);

    async fn cloudy(engine: Arc<dyn Engine>, id: &str) -> Cloudy {
        Cloudy::new(
            engine,
            id.to_string(),
            DOMAIN.to_string(),
            Shutdown::new(),
            None,
            Vec::new(),
        )
        .await
        .expect("cloudy")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_to_local() {
        let bus_a = Local::new();
        let bus_b = Local::new();
        let a1 = cloudy(Arc::new(bus_a.clone()), "a1").await;
        let b1 = cloudy(Arc::new(bus_b.clone()), "b1").await;
        let bridge_a = cloudy(Arc::new(bus_a), "bridge").await;
        let bridge_b = bridge_a
            .with_engine(Arc::new(bus_b))
            .await
            .expect("second side");
        let _bridge = forward(&bridge_a, &bridge_b).expect("forward");

        // --- presence + sample: A の publisher が B に見え、sample が source ごと届く ---
        let mut watch = b1.watch_publishers::<Probe>().expect("watch");
        let probe = a1
            .publisher::<Probe>()
            .qos(Qos::STATE)
            .build()
            .expect("publisher");
        assert_eq!(
            timeout(PATIENCE, watch.recv()).await.expect("join"),
            Some(PresenceEvent::Joined("a1".to_string()))
        );
        assert_eq!(b1.publishers::<Probe>().await.expect("publishers"), ["a1"]);
        // 自分の launch も B に写っている。
        let launches = bridge_b
            .engine()
            .alive(&Key::launch(DOMAIN, None), PATIENCE)
            .await
            .expect("alive");
        assert!(launches.iter().any(|k| k.source.as_deref() == Some("a1")));

        let mut plain = b1.subscribe::<Probe>().expect("subscriber");
        tokio::time::sleep(SETTLE).await;
        probe.send(Probe { seq: 1 }).await.expect("send");
        let envelope = timeout(PATIENCE, plain.recv_envelope())
            .await
            .expect("sample")
            .expect("open");
        assert_eq!((envelope.value.seq, envelope.source.as_str()), (1, "a1"));

        // --- latched: B の遅れて来た購読者の query が A の latch へ転送される ---
        let mut late = b1.subscriber::<Probe>().latched().build().expect("latched");
        assert_eq!(
            timeout(PATIENCE, late.recv())
                .await
                .expect("latched")
                .map(|p| p.seq),
            Some(1)
        );

        // --- no echo: A の購読者は自分の sample を 1 回だけ受ける ---
        let mut mine = a1.subscribe::<Probe>().expect("mine");
        tokio::time::sleep(SETTLE).await;
        probe.send(Probe { seq: 2 }).await.expect("send");
        assert_eq!(
            timeout(PATIENCE, mine.recv())
                .await
                .expect("once")
                .map(|p| p.seq),
            Some(2)
        );
        assert!(
            timeout(Duration::from_millis(500), mine.recv())
                .await
                .is_err(),
            "the bridge must not echo A's sample back into A"
        );

        // --- service: B から A の server を呼ぶ(任意 / 宛先指定 / reply_err) ---
        let mut server = a1.serve::<Add>().expect("serve");
        tokio::spawn(async move {
            while let Some(req) = server.recv().await {
                if req.value.b < 0 {
                    req.reply_err("negative").await.expect("reply_err");
                } else {
                    let sum = req.value.a + req.value.b;
                    req.reply(Sum { sum }).await.expect("reply");
                }
            }
        });
        tokio::time::sleep(SETTLE).await;
        assert_eq!(b1.servers::<Add>().await.expect("servers"), ["a1"]);
        assert_eq!(
            timeout(PATIENCE, b1.call::<Add>(Add { a: 2, b: 3 }))
                .await
                .expect("call")
                .expect("sum")
                .sum,
            5
        );
        let caller = b1.caller::<Add>().to("a1").build();
        assert_eq!(caller.call(Add { a: 1, b: 1 }).await.expect("to").sum, 2);
        assert!(matches!(
            caller.call(Add { a: 1, b: -1 }).await,
            Err(CallError::Remote(m)) if m == "negative"
        ));

        // --- 逆方向: B の publisher が A に届く ---
        let cmd = b1.publish::<Cmd>().expect("cmd");
        let mut cmds = a1.subscribe::<Cmd>().expect("cmds");
        tokio::time::sleep(SETTLE).await;
        cmd.send(Cmd { v: 9 }).await.expect("send");
        let envelope = timeout(PATIENCE, cmds.recv_envelope())
            .await
            .expect("cmd")
            .expect("open");
        assert_eq!((envelope.value.v, envelope.source.as_str()), (9, "b1"));

        // --- 離脱が写る ---
        drop(probe);
        assert_eq!(
            timeout(PATIENCE, watch.recv()).await.expect("leave"),
            Some(PresenceEvent::Left("a1".to_string()))
        );
    }
}
