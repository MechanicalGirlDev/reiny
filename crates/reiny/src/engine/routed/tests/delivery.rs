use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn mixed_sources_preserve_native_identity_and_wire_provenance() -> Result<()> {
    let pair = Pair::open(37467).await?;
    let a = key(A);
    let b = key(B);
    let (a_signal, a_ready) = subscription_signal(&pair.left, &a)?;
    let (b_signal, b_ready) = subscription_signal(&pair.right, &b)?;
    let (callback, local) = collector();
    let a_sub = pair.a.subscribe_delivery(&Key::all(DOMAIN), callback)?;
    let (callback, remote) = collector();
    let b_sub = pair.b.subscribe_delivery(&Key::all(DOMAIN), callback)?;
    matched(&a_ready, true).await?;
    matched(&b_ready, true).await?;
    let left_publisher = pair.a.publisher(&a, &Qos::DEFAULT)?;
    let right_publisher = pair.b.publisher(&b, &Qos::DEFAULT)?;
    let first = Arc::new(Probe { seq: 41 });
    let second = Arc::new(Probe { seq: 42 });
    let fingerprint = Some(vec![17; 8]);

    left_publisher.put_payload(Payload::shared(Arc::clone(&first)), fingerprint.clone())?;
    right_publisher.put_payload(Payload::shared(Arc::clone(&second)), fingerprint.clone())?;

    for (receiver, native_key, native) in [(&local, &a, &first), (&remote, &b, &second)] {
        let received = [recv(receiver).await?, recv(receiver).await?];
        assert_eq!(
            received
                .iter()
                .map(|sample| &sample.key)
                .collect::<HashSet<_>>(),
            HashSet::from([&a, &b])
        );
        for sample in received {
            assert_eq!(sample.attachment, fingerprint);
            if &sample.key == native_key {
                assert!(Arc::ptr_eq(
                    &sample
                        .payload
                        .downcast::<Probe>()
                        .context("native shared payload")?,
                    native
                ));
            } else {
                assert!(sample.payload.downcast::<Probe>().is_none());
                let expected = if sample.key == a { 41 } else { 42 };
                assert_eq!(Probe::decode(sample.payload.bytes())?.seq, expected);
            }
        }
    }
    // A local dispatcher barrier ensures the native copy cannot remain queued.
    pair.a.alive(&Key::all(DOMAIN), WAIT).await?;
    pair.b.alive(&Key::all(DOMAIN), WAIT).await?;
    assert!(local.try_recv().is_err());
    assert!(remote.try_recv().is_err());
    drop((a_sub, b_sub));
    matched(&a_ready, false).await?;
    matched(&b_ready, false).await?;
    drop((left_publisher, right_publisher, a_signal, b_signal));
    pair.close().await
}

#[tokio::test(flavor = "multi_thread")]
async fn ownership_and_qos_rejection_leave_no_remote_declaration() -> Result<()> {
    let pair = Pair::open(37468).await?;
    for target in [key(B), key("robot/unknown"), Key::all(DOMAIN)] {
        assert!(pair.a.publisher(&target, &Qos::DEFAULT).is_err());
        assert!(pair.a.declare_alive(&target).is_err());
        assert!(pair.a.respond(&target, Box::new(drop)).is_err());
    }
    for qos in [Qos::SENSOR, Qos::COMMAND] {
        assert!(pair.a.publisher(&key(A), &qos).is_err());
    }
    assert!(!pair.a.caps().congestion_control);
    assert!(
        pair.a
            .subscribe(&key("robot/unknown"), Box::new(drop))
            .is_err()
    );
    assert!(pair.a.query(&key("robot/unknown"), params(None)).is_err());
    let schema = Key::all(DOMAIN).with_chunk("@schema/*");
    assert!(pair.a.subscribe(&schema, Box::new(drop)).is_err());
    assert!(pair.a.watch_alive(&schema, Box::new(drop)).is_err());
    assert!(pair.a.alive(&schema, WAIT).await.is_err());
    assert_eq!(
        pair.b.alive(&Key::all(DOMAIN), WAIT).await?,
        Vec::<Key>::new()
    );
    pair.close().await
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_failure_reports_partial_publish_and_rolls_back_declarations() -> Result<()> {
    let pair = Pair::open(37475).await?;
    let target = key(A);
    let (callback, received) = collector();
    let subscription = pair.a.subscribe_delivery(&target, callback)?;
    let publisher = pair.a.publisher(&target, &Qos::DEFAULT)?;
    timeout(WAIT, pair.left.close())
        .await?
        .map_err(anyhow::Error::msg)?;
    let value = Arc::new(Probe { seq: 91 });

    assert!(
        publisher
            .put_payload(Payload::shared(Arc::clone(&value)), None)
            .is_err()
    );
    let delivered = recv(&received).await?;
    assert!(Arc::ptr_eq(
        &delivered
            .payload
            .downcast::<Probe>()
            .context("native shared payload")?,
        &value
    ));
    assert!(pair.a.declare_alive(&target).is_err());
    assert!(pair.a.respond(&target, Box::new(drop)).is_err());
    assert_eq!(pair.a.alive(&target, WAIT).await?, Vec::<Key>::new());
    assert!(
        replies(pair.a.query(&target, params(None))?)
            .await?
            .is_empty()
    );
    drop((subscription, publisher, pair.a, pair.b));
    timeout(WAIT, pair.right.close())
        .await?
        .map_err(anyhow::Error::msg)?;
    Ok(())
}
