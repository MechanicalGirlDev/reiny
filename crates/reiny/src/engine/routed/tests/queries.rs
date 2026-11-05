use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn late_retained_queries_preserve_local_arc_and_remote_fingerprint() -> Result<()> {
    let pair = Pair::open(37469).await?;
    let target = key(A);
    let (signal, ready) = query_signal(&pair.right, &target)?;
    let value = Arc::new(Probe { seq: 73 });
    let retained = Payload::shared(Arc::clone(&value));
    let reply_key = target.clone();
    let responder = pair.a.respond(
        &target,
        Box::new(move |query| {
            assert!(query.payload().is_none());
            assert!(
                query
                    .reply_payload(&reply_key, retained.clone(), Some(vec![17; 8]))
                    .is_ok()
            );
        }),
    )?;
    let publisher = pair.a.publisher(&target, &Qos::DEFAULT)?;
    publisher.put_payload(Payload::shared(Arc::clone(&value)), Some(vec![17; 8]))?;
    matched(&ready, true).await?;

    for (engine, native) in [(&pair.a, true), (&pair.b, false)] {
        let received = replies(engine.query(&target, params(None))?).await?;
        assert_eq!(received.len(), 1);
        let sample = success(received.first().context("retained reply")?)?;
        assert_eq!(sample.key, target);
        assert_eq!(sample.attachment, Some(vec![17; 8]));
        assert_eq!(Probe::decode(sample.payload.bytes())?.seq, 73);
        match sample.payload.downcast::<Probe>() {
            Some(received) => {
                assert!(native);
                assert!(Arc::ptr_eq(&received, &value));
            }
            None => assert!(!native),
        }
    }
    drop((responder, publisher, signal));
    pair.close().await
}

#[tokio::test(flavor = "multi_thread")]
async fn wildcard_rpc_invokes_each_owner_once_and_keeps_error_replies() -> Result<()> {
    let pair = Pair::open(37470).await?;
    let (a_signal, a_ready) = query_signal(&pair.right, &key(A))?;
    let (b_signal, b_ready) = query_signal(&pair.left, &key(B))?;
    let (calls_tx, calls) = flume::unbounded();
    let mut guards = Vec::new();
    for (engine, target) in [(&pair.a, key(A)), (&pair.b, key(B))] {
        let reply_key = target.clone();
        let tx = calls_tx.clone();
        guards.push(engine.respond(
            &target,
            Box::new(move |query| {
                assert_eq!(query.payload(), Some([9].as_slice()));
                assert_eq!(query.attachment(), Some([17; 8].as_slice()));
                forward(&tx, reply_key.clone());
                if reply_key.source.as_deref() == Some(A) {
                    assert!(query.reply(&reply_key, vec![10], Some(vec![18; 8])).is_ok());
                } else {
                    assert!(query.reply_err(vec![11]).is_ok());
                }
            }),
        )?);
    }
    matched(&a_ready, true).await?;
    matched(&b_ready, true).await?;

    for engine in [&pair.a, &pair.b] {
        let received = replies(engine.query(&Key::all(DOMAIN), params(Some(vec![9])))?).await?;
        assert_eq!(received.len(), 2);
        let sample = received
            .iter()
            .find_map(|reply| reply.as_ref().ok())
            .context("successful RPC reply")?;
        assert_eq!(sample.key, key(A));
        assert_eq!(sample.payload.bytes(), [10]);
        assert_eq!(sample.attachment, Some(vec![18; 8]));
        assert!(
            received
                .iter()
                .any(|reply| matches!(reply, Err(bytes) if bytes == &[11]))
        );
        assert_eq!(
            HashSet::from([recv(&calls).await?, recv(&calls).await?]),
            HashSet::from([key(A), key(B)])
        );
        assert!(calls.try_recv().is_err());
    }
    drop((guards, a_signal, b_signal));
    pair.close().await
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelled_merged_receive_keeps_later_replies_and_drops_cleanly() -> Result<()> {
    let pair = Pair::open(37471).await?;
    let (signal, ready) = query_signal(&pair.left, &key(B))?;
    let (callback, held) = collector();
    let remote = pair.b.respond(&key(B), callback)?;
    let (callback, local_held) = collector();
    let local = pair.a.respond(&key(A), callback)?;
    matched(&ready, true).await?;
    let mut stream = pair.a.query(&Key::all(DOMAIN), params(None))?;
    let local_query = recv(&local_held).await?;
    let remote_query = recv(&held).await?;
    {
        let mut pending = stream.next_delivery();
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx).is_pending())).await
        );
    }
    remote_query.reply(&key(B), vec![2], None)?;
    // The held Local query remains pending: remote replies must not wait behind it.
    let reply = timeout(WAIT, stream.next_delivery())
        .await?
        .context("remote reply")?;
    let sample = success(&reply)?;
    assert_eq!(sample.key, key(B));
    assert_eq!(sample.payload.bytes(), [2]);
    local_query.reply(&key(A), vec![1], None)?;
    let remaining = replies(stream).await?;
    assert_eq!(remaining.len(), 1);
    assert_eq!(success(&remaining[0])?.key, key(A));
    drop((remote, local, signal));
    pair.close().await
}
