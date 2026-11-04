use super::*;
use crate::engine::{
    BoxFuture, Callback, Caps, Guard, QueryCallback, RawPublisher, RawReplies, Sample,
};
use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, mpsc};

struct AdmissionEngine {
    local: Local,
    accept_ready: AtomicBool,
    liveliness: bool,
    before_ready: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl Engine for AdmissionEngine {
    fn caps(&self) -> Caps {
        Caps {
            liveliness: self.liveliness,
            ..Caps::ALL
        }
    }
    fn publisher(&self, key: &Key, qos: &Qos) -> crate::Result<Box<dyn RawPublisher>> {
        self.local.publisher(key, qos)
    }
    fn subscribe(&self, key: &Key, callback: Callback<Sample>) -> crate::Result<Guard> {
        self.local.subscribe(key, callback)
    }
    fn declare_alive(&self, key: &Key) -> crate::Result<Guard> {
        anyhow::ensure!(
            key.ty.as_deref() != Some("@ready") || self.accept_ready.load(Ordering::SeqCst),
            "injected readiness failure"
        );
        if key.ty.as_deref() == Some("@ready")
            && let Some(before_ready) = self.before_ready.lock().expect("ready gate").take()
        {
            before_ready();
        }
        self.local.declare_alive(key)
    }
    fn alive(&self, key: &Key, patience: Duration) -> BoxFuture<'_, crate::Result<Vec<Key>>> {
        self.local.alive(key, patience)
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
async fn failed_readiness_declaration_cancels_preparation_without_committing() {
    let bus = Arc::new(AdmissionEngine {
        local: Local::new(),
        accept_ready: AtomicBool::new(false),
        liveliness: true,
        before_ready: Mutex::new(None),
    });
    let context = hosted(bus.clone(), SENDER).await;
    let _output = context.output::<Probe>("outgoing").expect("output");
    let prepared = context.prepare_admission().expect("prepare");

    assert!(matches!(
        context.commit_admission(prepared),
        Err(AdmissionError::Transport(_))
    ));

    let key = Key::module(DOMAIN, Some(SENDER), "@ready");
    assert_eq!(
        bus.alive(&key, PATIENCE).await.expect("presence"),
        Vec::<Key>::new()
    );
    assert_eq!(*context.hosted_events().borrow(), None);
    bus.accept_ready.store(true, Ordering::SeqCst);
    let _ready = context
        .commit_admission(context.prepare_admission().expect("retry preparation"))
        .expect("retry commit");
    assert_eq!(bus.alive(&key, PATIENCE).await.expect("ready"), vec![key]);
}

#[tokio::test]
async fn hosted_startup_requires_liveliness_before_transport_presence() {
    let bus = Arc::new(AdmissionEngine {
        local: Local::new(),
        accept_ready: AtomicBool::new(true),
        liveliness: false,
        before_ready: Mutex::new(None),
    });

    assert!(matches!(
        ModuleContext::open_hosted(hosted_options(bus.clone(), bindings(SENDER))).await,
        Err(AdmissionError::NoLiveliness)
    ));

    assert_eq!(
        bus.alive(&Key::launch(DOMAIN, Some(SENDER)), PATIENCE)
            .await
            .expect("presence"),
        Vec::<Key>::new()
    );
}

#[tokio::test]
async fn commit_serializes_port_loss_through_readiness_declaration() {
    let bus = Arc::new(AdmissionEngine {
        local: Local::new(),
        accept_ready: AtomicBool::new(true),
        liveliness: true,
        before_ready: Mutex::new(None),
    });
    let context = Arc::new(hosted(bus.clone(), SENDER).await);
    let output = context.output::<Probe>("outgoing").expect("output");
    let prepared = context.prepare_admission().expect("prepare");
    let mut events = context.hosted_events();
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (release, released) = mpsc::sync_channel(1);
    let registry = Arc::downgrade(&context.module.ports);
    *bus.before_ready.lock().expect("install gate") = Some(Box::new(move || {
        // This lock is the admission protocol's linearization boundary, not a test-only seam.
        assert!(matches!(
            registry.upgrade().expect("live context").try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ));
        entered.send(()).expect("notify declaration");
        released
            .recv_timeout(PATIENCE)
            .expect("release declaration");
    }));
    let committing = Arc::clone(&context);
    let commit = tokio::task::spawn_blocking(move || committing.commit_admission(prepared));
    timeout(PATIENCE, entering)
        .await
        .expect("declaration deadline")
        .expect("entered declaration");
    let (dropping, drop_started) = tokio::sync::oneshot::channel();

    let loss = tokio::task::spawn_blocking(move || {
        dropping.send(()).expect("notify drop");
        drop(output);
    });
    timeout(PATIENCE, drop_started)
        .await
        .expect("drop deadline")
        .expect("drop started");
    release.send(()).expect("release declaration");

    let _ready = timeout(PATIENCE, commit)
        .await
        .expect("commit deadline")
        .expect("join commit")
        .expect("commit won lock");
    timeout(PATIENCE, loss)
        .await
        .expect("loss deadline")
        .expect("join loss");
    timeout(PATIENCE, events.changed())
        .await
        .expect("event deadline")
        .expect("loss event");
    assert_eq!(*events.borrow(), Some(HostedEvent::ReadinessLost));
    assert_eq!(
        bus.alive(&Key::module(DOMAIN, Some(SENDER), "@ready"), PATIENCE)
            .await
            .expect("withdrawn presence"),
        Vec::<Key>::new()
    );
}
