use super::*;
use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::engine::{
    BoxFuture, Callback, Caps, Guard, Key, Presence, QueryCallback, QueryParams, READY_CHUNK,
    RawPublisher, RawReplies, STOP_CHUNK, Sample,
};

mod activation;

struct SecondReady {
    local: Local,
    attempts: AtomicUsize,
    on_second: Box<dyn Fn() -> crate::Result<()> + Send + Sync>,
}

impl Engine for SecondReady {
    fn caps(&self) -> Caps {
        self.local.caps()
    }

    fn publisher(&self, key: &Key, qos: &crate::Qos) -> crate::Result<Box<dyn RawPublisher>> {
        self.local.publisher(key, qos)
    }

    fn subscribe(&self, key: &Key, callback: Callback<Sample>) -> crate::Result<Guard> {
        self.local.subscribe(key, callback)
    }

    fn declare_alive(&self, key: &Key) -> crate::Result<Guard> {
        if key.ty.as_deref() == Some(READY_CHUNK)
            && self.attempts.fetch_add(1, Ordering::SeqCst) == 1
        {
            (self.on_second)()?;
        }
        self.local.declare_alive(key)
    }

    fn alive(&self, key: &Key, timeout: Duration) -> BoxFuture<'_, crate::Result<Vec<Key>>> {
        self.local.alive(key, timeout)
    }

    fn watch_alive(&self, key: &Key, callback: Callback<Presence>) -> crate::Result<Guard> {
        self.local.watch_alive(key, callback)
    }

    fn respond(&self, key: &Key, callback: QueryCallback) -> crate::Result<Guard> {
        self.local.respond(key, callback)
    }

    fn query(&self, key: &Key, params: QueryParams) -> crate::Result<Box<dyn RawReplies>> {
        self.local.query(key, params)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[tokio::test]
async fn a_later_commit_failure_withdraws_prior_readiness_and_runs_nothing() {
    let (registry, mut records, _gate) = fixture(Behavior::Idle);
    let engine = Arc::new(SecondReady {
        local: Local::new(),
        attempts: AtomicUsize::new(0),
        on_second: Box::new(|| anyhow::bail!("injected second readiness transport failure")),
    });
    let handle = ComponentHost::launch(
        registry,
        options(engine.clone()),
        vec![
            instance("app/first", "test"),
            instance("app/second", "test"),
        ],
    )
    .expect("launch");

    let (primary, cleanup) = failure(terminal(&handle).await);

    assert_eq!(primary.code, HostErrorCode::Activate);
    assert_eq!(primary.instance.as_deref(), Some("app/second"));
    assert_eq!(cleanup, Vec::<HostError>::new());
    assert_eq!(engine.attempts.load(Ordering::SeqCst), 2);
    while let Ok(record) = records.try_recv() {
        assert_ne!(record.stage, Stage::Run);
    }
    assert_eq!(
        engine
            .alive(&Key::module("components", None, READY_CHUNK), PATIENCE)
            .await
            .expect("readiness withdrawn"),
        Vec::<Key>::new()
    );
}

#[tokio::test]
async fn a_managed_stop_request_stops_the_entire_host() {
    let (registry, mut records, _gate) = fixture(Behavior::Idle);
    let engine = Arc::new(Local::new());
    let handle = ComponentHost::launch(
        registry,
        options(engine.clone()),
        vec![
            instance("app/first", "test"),
            instance("app/second", "test"),
        ],
    )
    .expect("launch");
    handle.wait_running().await.expect("running");

    let mut replies = engine
        .query(
            &Key::module("components", Some("app/first"), STOP_CHUNK),
            QueryParams {
                payload: None,
                attachment: None,
                timeout: PATIENCE,
            },
        )
        .expect("stop query");
    timeout(PATIENCE, replies.next())
        .await
        .expect("stop reply deadline")
        .expect("stop acknowledged")
        .expect("stop reply");

    assert!(matches!(terminal(&handle).await, HostOutcome::Stopped));
    let mut stopped = Vec::new();
    while let Ok(record) = records.try_recv() {
        if record.stage == Stage::Stop(StopReason::Requested) {
            stopped.push(record.id);
        }
    }
    stopped.sort();
    assert_eq!(stopped, ["app/first", "app/second"]);
}
