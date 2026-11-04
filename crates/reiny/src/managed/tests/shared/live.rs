use super::*;

#[tokio::test]
async fn exact_type_fanout_shares_allocation_without_encoding_or_decoding() {
    let bus = Arc::new(Local::new());
    let sender = sender(bus.clone(), false).await;
    let receiver = consumer(bus.clone()).await;
    let mut input = receiver.input::<Counted>("incoming").expect("input");
    let output = sender.output::<Counted>("outgoing").expect("output");
    let other = sender.output::<Counted>("other").expect("other output");
    sender.ready().expect("ready sender");
    receiver.ready().expect("ready receiver");
    let mut second_binding = bindings("deployment/second-consumer");
    second_binding
        .inputs
        .insert("incoming".into(), super::super::input(SENDER));
    let second_receiver = ModuleContext::open(options(bus.clone(), second_binding))
        .await
        .expect("second receiver");
    let mut second = second_receiver
        .input::<Counted>("incoming")
        .expect("fanout");
    let value = Arc::new(Counted::new(42));

    // Wrong named port, wrong namespace and wrong fingerprints precede a sentinel.
    other.send_shared(Arc::clone(&value)).await.expect("other");
    let foreign = bus
        .publisher(
            &Key::output(
                DOMAIN,
                "deployment/foreign",
                "outgoing",
                Counted::<0>::TYPE_NAME,
            ),
            &Qos::DEFAULT,
        )
        .expect("foreign");
    foreign
        .put_payload(Payload::shared(Arc::clone(&value)), Some(stamp()))
        .expect("foreign send");
    let raw = bus.publisher(&key(), &Qos::DEFAULT).expect("transport");
    for attachment in [None, Some(vec![1]), Some(0xdead_u64.to_le_bytes().to_vec())] {
        raw.put_payload(Payload::shared(Arc::new(Counted::<0>::new(99))), attachment)
            .expect("bad stamp");
    }
    output
        .send_shared(Arc::clone(&value))
        .await
        .expect("send shared");

    let first = timeout(PATIENCE, input.recv_shared_envelope())
        .await
        .expect("receive")
        .expect("sample");
    let second = timeout(PATIENCE, second.recv_shared_envelope())
        .await
        .expect("fanout receive")
        .expect("sample");
    assert!(Arc::ptr_eq(&value, &first.value));
    assert!(Arc::ptr_eq(&first.value, &second.value));
    assert_eq!((value.encodes(), first.value.decoded_fields), (0, 0));
    assert_eq!(
        (first.source.as_str(), first.source_port.as_str()),
        (SENDER, "outgoing")
    );
    assert!(first.timestamp.is_some());
    assert_eq!(first.timestamp, second.timestamp);
    barrier(bus.as_ref()).await;
    assert_eq!(
        input.stats().received,
        4,
        "three bad stamps and one publication, no duplicate"
    );
}

#[tokio::test]
async fn mixed_raw_owned_and_different_rust_types_encode_once_per_publication() {
    let bus = Arc::new(Local::new());
    let sender = sender(bus.clone(), false).await;
    let receiver = receiver(bus.clone(), false, ReceiveBuffer::Fifo).await;
    let output = sender.output::<Counted>("outgoing").expect("output");
    let mut native = receiver.input::<Counted>("first").expect("native");
    let mut owned = receiver.input::<Counted>("owned").expect("owned");
    let mut twin = receiver
        .input::<Counted<1>>("twin")
        .expect("same schema, different Rust type");
    let mut raw = receiver
        .raw_input(
            "raw",
            Counted::<0>::TYPE_NAME,
            Counted::<0>::SCHEMA_FINGERPRINT,
        )
        .expect("raw");
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let _raw_engine = bus
        .subscribe(
            &key(),
            Box::new(move |sample| {
                tx.send(sample).expect("raw engine receiver");
            }),
        )
        .expect("engine subscription");

    for n in [7, 8] {
        let value = Arc::new(Counted::new(n));
        output.send_shared(Arc::clone(&value)).await.expect("send");
        let native = timeout(PATIENCE, native.recv_shared())
            .await
            .expect("native")
            .expect("value");
        let owned = timeout(PATIENCE, owned.recv())
            .await
            .expect("owned")
            .expect("value");
        let twin = timeout(PATIENCE, twin.recv_shared())
            .await
            .expect("twin")
            .expect("value");
        let raw = timeout(PATIENCE, raw.recv())
            .await
            .expect("raw")
            .expect("value");
        let engine = timeout(PATIENCE, rx.recv())
            .await
            .expect("engine")
            .expect("value");
        assert!(Arc::ptr_eq(&value, &native));
        assert_eq!((owned.value(), twin.value()), (n, n));
        assert_eq!((owned.decoded_fields, twin.decoded_fields), (1, 1));
        assert_eq!(raw.payload, Probe { value: n }.encode_to_vec());
        assert_eq!(engine.payload, raw.payload);
        assert_eq!(engine.attachment, Some(stamp()));
        assert_eq!(value.encodes(), 1);
    }
    barrier(bus.as_ref()).await;
    assert_eq!(native.stats().received, 2);
    assert_eq!(raw.stats().received, 2);
    assert!(rx.try_recv().is_err(), "no duplicate raw dispatch");
}

#[tokio::test]
async fn shared_receive_cancellation_and_shutdown_preserve_delivery() {
    use std::future::Future;
    use std::task::Poll;

    let bus = Arc::new(Local::new());
    let sender = producer(bus.clone(), SENDER).await;
    let receiver = consumer(bus).await;
    let output = sender.output::<Counted>("outgoing").expect("output");
    let mut input = receiver.input::<Counted>("incoming").expect("input");
    {
        let mut receive = Box::pin(input.recv_shared());
        std::future::poll_fn(|cx| {
            assert!(receive.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    let value = Arc::new(Counted::new(12));
    let unpolled = output.send_shared(Arc::new(Counted::new(99)));
    output.send_shared(Arc::clone(&value)).await.expect("send");
    let actual = timeout(PATIENCE, input.recv_shared())
        .await
        .expect("receive")
        .expect("value");
    assert!(Arc::ptr_eq(&value, &actual));
    drop(unpolled);
    receiver.shutdown_now();
    assert!(
        timeout(PATIENCE, input.recv_shared())
            .await
            .expect("shutdown")
            .is_none()
    );
    assert_eq!(value.encodes(), 0);
}

#[tokio::test]
async fn native_latest_ring_keeps_newest_in_order_without_encoding() {
    let bus = Arc::new(Local::new());
    let sender = producer(bus.clone(), SENDER).await;
    let receiver = receiver(bus.clone(), false, ReceiveBuffer::Latest(2)).await;
    let output = sender.output::<Counted>("outgoing").expect("output");
    let mut input = receiver.input::<Counted>("first").expect("input");
    let values: Vec<_> = (1..=4).map(|n| Arc::new(Counted::new(n))).collect();
    for value in &values {
        output.send_shared(Arc::clone(value)).await.expect("send");
    }
    barrier(bus.as_ref()).await;
    assert_eq!(
        input.stats(),
        crate::SubscriberStats {
            received: 4,
            dropped: 2,
            blocked: 0
        }
    );
    for expected in &values[2..] {
        let actual = timeout(PATIENCE, input.recv_shared())
            .await
            .expect("receive")
            .expect("value");
        assert!(Arc::ptr_eq(expected, &actual));
    }
    assert!(values.iter().all(|value| value.encodes() == 0));
}

#[tokio::test]
async fn native_fifo_preserves_every_value_across_capacity_and_mixed_sends() {
    let bus = Arc::new(Local::new());
    let sender = producer(bus.clone(), SENDER).await;
    let receiver = consumer(bus.clone()).await;
    let output = sender.output::<Counted>("outgoing").expect("output");
    let mut input = receiver.input::<Counted>("incoming").expect("input");
    let values: Vec<_> = (1..=258).map(|n| Arc::new(Counted::new(n))).collect();
    // More than the 256-entry FIFO; reading must release the dispatcher's backpressure.
    for value in &values {
        output.send_shared(Arc::clone(value)).await.expect("shared");
    }
    output.send(Counted::new(259)).await.expect("byte sentinel");
    for expected in &values {
        let actual = timeout(PATIENCE, input.recv_shared())
            .await
            .expect("receive")
            .expect("value");
        assert!(Arc::ptr_eq(expected, &actual));
    }
    let sentinel = timeout(PATIENCE, input.recv_shared())
        .await
        .expect("sentinel")
        .expect("value");
    assert_eq!((sentinel.value(), sentinel.decoded_fields), (259, 1));
    barrier(bus.as_ref()).await;
    assert_eq!((input.stats().received, input.stats().dropped), (259, 0));
    assert!(values.iter().all(|value| value.encodes() == 0));
}
