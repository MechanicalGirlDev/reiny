//! Deterministic hierarchical namespace conformance, independent of the SDK conformance sleeps.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::timeout;

use super::{Engine, Key, Presence, QueryParams, Sample};
use crate::Qos;

const WAIT: Duration = Duration::from_secs(5);
const DOMAIN: &str = "namespace-conf";

/// Exercise exact and recursive sources, reserved slot isolation, and schema query routing.
/// Declarations and event channels precede every trigger; all waits are bounded.
pub async fn exercise(engine: Arc<dyn Engine>) {
    let a = Key::topic(DOMAIN, Some("deployment/arm/controller"), "NamespaceProbe");
    let b = Key::topic(
        DOMAIN,
        Some("deployment/gripper/controller"),
        "NamespaceProbe",
    );
    let any = Key::topic(DOMAIN, None, "NamespaceProbe");
    let schema = a.with_chunk("@schema/probe.Message");
    let ready = Key::topic(DOMAIN, a.source.as_deref(), "@ready");
    let stop = Key::topic(DOMAIN, a.source.as_deref(), "@stop");

    // Given: a watcher and sample subscriptions are registered before any presence or data.
    let (events_tx, mut events) = mpsc::unbounded_channel();
    let watch = engine
        .watch_alive(
            &Key::all(DOMAIN),
            Box::new(move |event| events_tx.send(event).expect("watch receiver")),
        )
        .expect("watch");
    let (all_tx, mut all) = mpsc::unbounded_channel::<Sample>();
    let _all = engine
        .subscribe(
            &any,
            Box::new(move |sample| all_tx.send(sample).expect("receiver")),
        )
        .expect("recursive subscriber");
    let (exact_tx, mut exact) = mpsc::unbounded_channel::<Sample>();
    let _exact = engine
        .subscribe(
            &a,
            Box::new(move |sample| exact_tx.send(sample).expect("receiver")),
        )
        .expect("exact subscriber");
    let publisher = engine.publisher(&a, &Qos::DEFAULT).expect("publisher");

    // When: reserved tokens, then a concrete nested publisher token and sample are raised.
    let mut tokens = Vec::new();
    for key in [
        Key::launch(DOMAIN, a.source.as_deref()),
        ready.clone(),
        stop.clone(),
        a.with_chunk("@service"),
        a.with_chunk("@sub"),
        schema.clone(),
    ] {
        tokens.push(engine.declare_alive(&key).expect("reserved token"));
    }
    let token = engine.declare_alive(&a).expect("publisher token");
    publisher.put(vec![7], None).expect("put");

    // Then: all-types presence excludes every reserved type/chunk, and samples keep the whole source.
    assert_eq!(
        timeout(WAIT, events.recv()).await.expect("join"),
        Some(Presence::Joined(a.clone()))
    );
    for sample in [
        timeout(WAIT, all.recv())
            .await
            .expect("recursive sample")
            .expect("open"),
        timeout(WAIT, exact.recv())
            .await
            .expect("exact sample")
            .expect("open"),
    ] {
        assert_eq!(sample.key, a);
        assert_eq!(sample.payload, [7]);
    }
    let sibling_token = engine.declare_alive(&b).expect("sibling token");
    let keys = timeout(WAIT, engine.alive(&any, WAIT))
        .await
        .expect("alive")
        .expect("keys");
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&a) && keys.contains(&b));
    assert_eq!(
        timeout(WAIT, engine.alive(&a, WAIT))
            .await
            .expect("alive")
            .expect("keys"),
        std::slice::from_ref(&a)
    );
    let ancestor = Key::topic(DOMAIN, Some("deployment/arm"), "NamespaceProbe");
    assert!(
        timeout(WAIT, engine.alive(&ancestor, WAIT))
            .await
            .expect("alive")
            .expect("keys")
            .is_empty(),
        "an exact namespace never widens to its descendants"
    );

    assert_eq!(
        timeout(WAIT, events.recv()).await.expect("sibling join"),
        Some(Presence::Joined(b.clone()))
    );
    drop(token);
    assert_eq!(
        timeout(WAIT, events.recv()).await.expect("leave"),
        Some(Presence::Left(a.clone()))
    );
    // Undeclare the watcher before teardown can emit leaves to a receiver that is being dropped.
    drop(watch);
    drop((sibling_token, tokens));
    query_routing(engine.as_ref(), a, b).await;
}

async fn query_routing(engine: &dyn Engine, a: Key, b: Key) {
    let any = Key::topic(DOMAIN, None, "NamespaceProbe");
    let schema = a.with_chunk("@schema/probe.Message");
    let ready = Key::topic(DOMAIN, a.source.as_deref(), "@ready");
    let stop = Key::topic(DOMAIN, a.source.as_deref(), "@stop");
    let ancestor = Key::topic(DOMAIN, Some("deployment/arm"), "NamespaceProbe");
    // Given: every concrete responder exists before its query, including the isolated slots.
    let mut responders = Vec::new();
    for (key, payload) in [
        (a.clone(), vec![1]),
        (b.clone(), vec![2]),
        (schema.clone(), vec![3]),
        (ready, vec![4]),
        (stop.clone(), vec![5]),
    ] {
        let reply_key = key.clone();
        responders.push(
            engine
                .respond(
                    &key,
                    Box::new(move |query| {
                        query
                            .reply(&reply_key, payload.clone(), None)
                            .expect("reply");
                    }),
                )
                .expect("responder"),
        );
    }
    // When: query exact, recursive, schema and stop patterns, plus a nonmatching ancestor.
    for (pattern, expected) in [
        (a.clone(), vec![(a.clone(), vec![1])]),
        (
            Key::all(DOMAIN),
            vec![(a.clone(), vec![1]), (b.clone(), vec![2])],
        ),
        (any.with_chunk("@schema/*"), vec![(schema, vec![3])]),
        (Key::topic(DOMAIN, None, "@stop"), vec![(stop, vec![5])]),
        (ancestor, Vec::new()),
    ] {
        let mut replies = engine
            .query(
                &pattern,
                QueryParams {
                    payload: None,
                    attachment: None,
                    timeout: WAIT,
                },
            )
            .expect("query");
        let mut actual = Vec::new();
        while let Some(reply) = timeout(WAIT, replies.next())
            .await
            .expect("query completion")
        {
            let sample = reply.expect("successful reply");
            actual.push((sample.key, sample.payload));
        }
        actual.sort_by(|a, b| a.0.source.cmp(&b.0.source));
        // Then: finalization proves that no other responder matched, without waiting for silence.
        assert_eq!(actual, expected, "{pattern}");
    }
}

#[cfg(test)]
#[tokio::test]
async fn local_namespaces() {
    exercise(Arc::new(super::Local::new())).await;
}

#[cfg(all(test, feature = "zenoh"))]
#[tokio::test(flavor = "multi_thread")]
async fn zenoh_namespaces() {
    // One real session observes its local declarations, so no discovery delay or fixed port is needed.
    let mut config = zenoh::Config::default();
    config
        .insert_json5("scouting/multicast/enabled", "false")
        .expect("config");
    let engine = super::Zenoh::open(config).await.expect("session");
    exercise(Arc::new(engine)).await;
}

#[cfg(test)]
#[test]
fn keys_round_trip_nested_sources_and_reserved_slots() {
    for source in [Some("a"), Some("deployment/robot/arm/controller"), None] {
        for ty in ["Message", "@launch", "@ready", "@stop"] {
            let key = Key::topic("lab", source, ty);
            assert_eq!(Key::parse(&key.to_string()), Some(key.clone()));
            if ty == "Message" {
                for chunk in ["@service", "@sub", "@schema/pkg.Message", "@schema/*"] {
                    let key = key.with_chunk(chunk);
                    assert_eq!(Key::parse(&key.to_string()), Some(key));
                }
            }
        }
    }
    assert_eq!(Key::parse("reiny/lab/**/*"), Some(Key::all("lab")));
    assert_eq!(
        Key::parse(&Key::all("lab").to_string()),
        Some(Key::all("lab"))
    );
    assert_eq!(
        Key::topic("lab", None, "Message").to_string(),
        "reiny/lab/**/Message"
    );
}

#[cfg(test)]
#[test]
fn concrete_sources_reject_patterns_and_noncanonical_paths() {
    for source in [
        "", "/a", "a/", "a//b", ".", "a/../b", "a/*", "a/**", "a/$*", "@a", "a/@b",
    ] {
        let key = Key::topic("lab", Some(source), "Message");
        assert!(key.validate().is_err(), "{source}");
        assert!(Key::parse(&key.to_string()).is_none(), "{source}");
    }
    assert!(
        Key::parse("reiny/lab/*/Message").is_none(),
        "a one-depth wildcard is not widened"
    );
}

#[cfg(test)]
#[test]
fn exact_sources_and_all_type_isolation_are_depth_independent() {
    let key = Key::topic("lab", Some("deployment/robot/controller"), "Message");
    assert!(Key::topic("lab", None, "Message").matches(&key));
    for source in [
        "controller",
        "deployment/robot",
        "deployment/robot/controller/child",
    ] {
        assert!(!Key::topic("lab", Some(source), "Message").matches(&key));
    }
    for ty in ["@launch", "@ready", "@stop"] {
        assert!(!Key::all("lab").matches(&Key::topic("lab", key.source.as_deref(), ty)));
    }
    for chunk in ["@service", "@sub", "@schema/pkg.Message"] {
        assert!(!Key::all("lab").matches(&key.with_chunk(chunk)));
    }
}
