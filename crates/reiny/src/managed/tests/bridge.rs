use super::*;

#[tokio::test]
async fn bridged_namespace_preserves_ready_stop_and_owner_departure() {
    // Given two engines with a bridge and an exact nested-module readiness watch on the far side.
    let left = Arc::new(Local::new());
    let right = Arc::new(Local::new());
    let mut bridge_options = RuntimeOptions::new("bridge");
    bridge_options.domain = DOMAIN.to_string();
    bridge_options.module_report_path = None;
    bridge_options.engine = Some(left.clone());
    let bridge_left = Cloudy::open(bridge_options).await.expect("left bridge");
    let bridge_right = bridge_left
        .with_engine(right.clone())
        .await
        .expect("right bridge");
    let _bridge = crate::bridge::forward(&bridge_left, &bridge_right).expect("bridge");
    let module = Cloudy::open(options(left, bindings(SENDER)))
        .await
        .expect("module");
    let ready = Key::topic(DOMAIN, Some(SENDER), "@ready");
    let (events, mut observed) = tokio::sync::mpsc::unbounded_channel();
    let _watch = right
        .watch_alive(
            &ready,
            Box::new(move |event| {
                events.send(event).expect("observer remains alive");
            }),
        )
        .expect("watch before transition");

    // When the module announces readiness and is stopped from the other engine.
    module.ready().expect("ready");
    assert_eq!(
        timeout(PATIENCE, observed.recv())
            .await
            .expect("mirrored readiness"),
        Some(Presence::Joined(ready.clone()))
    );
    let stop = Key::topic(DOMAIN, Some(SENDER), "@stop");
    let mut replies = right
        .query(
            &stop,
            QueryParams {
                payload: Some(Vec::new()),
                attachment: None,
                timeout: PATIENCE,
            },
        )
        .expect("bridged stop");
    let ack = timeout(PATIENCE, replies.next())
        .await
        .expect("bounded acknowledgement")
        .expect("reply")
        .expect("successful acknowledgement");

    // Then both the request acknowledgement and eventual owner departure retain the exact source.
    assert_eq!(ack.key, stop);
    assert_eq!(ack.payload, b"ack");
    timeout(PATIENCE, module.shutdown())
        .await
        .expect("shutdown requested");
    assert_eq!(
        right
            .alive(&ready, PATIENCE)
            .await
            .expect("owner still alive"),
        std::slice::from_ref(&ready)
    );
    drop(module);
    assert_eq!(
        timeout(PATIENCE, observed.recv())
            .await
            .expect("mirrored departure"),
        Some(Presence::Left(ready))
    );
}
