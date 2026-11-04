use super::*;

#[tokio::test]
async fn retained_native_and_raw_queries_share_one_publication_cache() {
    let bus = Arc::new(Local::new());
    let sender = sender(bus.clone(), true).await;
    let output = sender.output::<Counted>("outgoing").expect("output");
    let other = sender.output::<Counted>("other").expect("other output");
    let value = Arc::new(Counted::new(21));
    other
        .send_shared(Arc::new(Counted::new(99)))
        .await
        .expect("other");
    output
        .send_shared(Arc::clone(&value))
        .await
        .expect("retained");
    barrier(bus.as_ref()).await;
    let receiver = receiver(bus.clone(), true, ReceiveBuffer::Fifo).await;
    let mut first = receiver.input::<Counted>("first").expect("first");
    let mut second = receiver.input::<Counted>("second").expect("second");

    for input in [&mut first, &mut second] {
        let replay = timeout(PATIENCE, input.recv_shared_envelope())
            .await
            .expect("replay")
            .expect("value");
        assert!(Arc::ptr_eq(&value, &replay.value));
        assert_eq!(
            (replay.source.as_str(), replay.source_port.as_str()),
            (SENDER, "outgoing")
        );
        assert!(replay.timestamp.is_some());
        assert_eq!(replay.value.decoded_fields, 0);
    }
    assert_eq!(
        value.encodes(),
        0,
        "retained same-type receivers do not serialize"
    );

    // Raw SDK and engine queries must see real bytes, not a placeholder native tag.
    let mut raw = receiver
        .raw_input(
            "raw",
            Counted::<0>::TYPE_NAME,
            Counted::<0>::SCHEMA_FINGERPRINT,
        )
        .expect("raw");
    let raw = timeout(PATIENCE, raw.recv())
        .await
        .expect("raw replay")
        .expect("value");
    assert_eq!(raw.payload, Probe { value: 21 }.encode_to_vec());
    for _ in 0..2 {
        let mut replies = bus
            .query(
                &key(),
                QueryParams {
                    payload: None,
                    attachment: None,
                    timeout: PATIENCE,
                },
            )
            .expect("query");
        let reply = timeout(PATIENCE, replies.next())
            .await
            .expect("reply")
            .expect("sample")
            .expect("success");
        assert_eq!(reply.payload, raw.payload);
        assert_eq!(reply.attachment, Some(stamp()));
        assert!(
            timeout(PATIENCE, replies.next())
                .await
                .expect("finalize")
                .is_none()
        );
    }
    assert_eq!(
        value.encodes(),
        1,
        "all replay consumers share the original cache"
    );

    let next = Arc::new(Counted::new(22));
    output.send_shared(Arc::clone(&next)).await.expect("live");
    for input in [&mut first, &mut second] {
        let live = timeout(PATIENCE, input.recv_shared())
            .await
            .expect("live")
            .expect("value");
        assert!(Arc::ptr_eq(&next, &live), "no duplicate retained sample");
    }
}

#[tokio::test]
async fn native_replay_checks_reply_route_and_fingerprint_before_downcast() {
    let bus = Arc::new(Local::new());
    let receiver = receiver(bus.clone(), true, ReceiveBuffer::Fifo).await;
    let mut input = receiver.input::<Counted>("first").expect("input");
    let valid = Arc::new(Counted::<0>::new(42));
    let wrong = Arc::new(Counted::<0>::new(99));
    let (tx, mut queries) = tokio::sync::mpsc::unbounded_channel();
    let _responder = bus
        .respond(
            &key(),
            Box::new(move |query| {
                tx.send(query).expect("pending queries");
            }),
        )
        .expect("responder");
    let mut token = bus.declare_alive(&key()).expect("presence");

    for (reply_key, attachment) in [
        (
            Key::output(DOMAIN, SENDER, "other", Counted::<0>::TYPE_NAME),
            Some(stamp()),
        ),
        (key(), Some(0xffff_u64.to_le_bytes().to_vec())),
    ] {
        let query = timeout(PATIENCE, async {
            tokio::select! {
                biased;
                sample = input.recv_shared() => panic!("unexpected value: {sample:?}"),
                query = queries.recv() => query.expect("retained query"),
            }
        })
        .await
        .expect("query");
        query
            .reply_payload(&reply_key, Payload::shared(Arc::clone(&wrong)), attachment)
            .expect("invalid reply");
        drop(token);
        token = bus.declare_alive(&key()).expect("rejoined");
    }
    let query = timeout(PATIENCE, async {
        tokio::select! {
            biased;
            sample = input.recv_shared() => panic!("invalid reply admitted: {sample:?}"),
            query = queries.recv() => query.expect("rejoined query"),
        }
    })
    .await
    .expect("query");
    query
        .reply_payload(&key(), Payload::shared(Arc::clone(&valid)), Some(stamp()))
        .expect("valid reply");
    let actual = timeout(PATIENCE, input.recv_shared())
        .await
        .expect("replay")
        .expect("value");
    assert!(Arc::ptr_eq(&valid, &actual));
    assert_eq!((wrong.encodes(), valid.encodes()), (0, 0));
    drop(token);
}

#[tokio::test]
async fn delayed_native_replay_is_suppressed_until_publisher_rejoins() {
    let bus = Arc::new(Local::new());
    let receiver = receiver(bus.clone(), true, ReceiveBuffer::Fifo).await;
    let mut input = receiver.input::<Counted>("first").expect("input");
    let (tx, mut queries) = tokio::sync::mpsc::unbounded_channel();
    let _responder = bus
        .respond(
            &key(),
            Box::new(move |q| {
                tx.send(q).expect("query receiver");
            }),
        )
        .expect("responder");
    let token = bus.declare_alive(&key()).expect("presence");
    let publisher = bus.publisher(&key(), &Qos::DEFAULT).expect("publisher");
    let query = timeout(PATIENCE, async {
        tokio::select! {
            biased;
            sample = input.recv_shared() => panic!("unexpected value: {sample:?}"),
            query = queries.recv() => query.expect("query"),
        }
    })
    .await
    .expect("query");
    let live = Arc::new(Counted::<0>::new(2));
    publisher
        .put_payload(Payload::shared(Arc::clone(&live)), Some(stamp()))
        .expect("live");
    let actual = timeout(PATIENCE, input.recv_shared())
        .await
        .expect("live")
        .expect("value");
    assert!(Arc::ptr_eq(&live, &actual));
    let stale = Arc::new(Counted::<0>::new(1));
    query
        .reply_payload(&key(), Payload::shared(Arc::clone(&stale)), Some(stamp()))
        .expect("delayed");
    publisher
        .put_payload(Payload::shared(Arc::clone(&live)), Some(stamp()))
        .expect("sentinel");
    let actual = timeout(PATIENCE, input.recv_shared())
        .await
        .expect("sentinel")
        .expect("value");
    assert!(Arc::ptr_eq(&live, &actual));
    drop(token);
    let _rejoined = bus.declare_alive(&key()).expect("rejoined");
    let query = timeout(PATIENCE, async {
        tokio::select! {
            biased;
            sample = input.recv_shared() => panic!("duplicate value: {sample:?}"),
            query = queries.recv() => query.expect("rejoined query"),
        }
    })
    .await
    .expect("query");
    query
        .reply_payload(&key(), Payload::shared(Arc::clone(&stale)), Some(stamp()))
        .expect("replay");
    let actual = timeout(PATIENCE, input.recv_shared())
        .await
        .expect("replay")
        .expect("value");
    assert!(Arc::ptr_eq(&stale, &actual));
    assert_eq!((live.encodes(), stale.encodes()), (0, 0));
}
