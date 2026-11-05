use super::*;
use crate::{MessageType, ModuleContext, shutdown::Shutdown};

#[tokio::test(flavor = "multi_thread")]
async fn sdk_late_subscribers_use_the_production_retained_cache_then_stop() -> Result<()> {
    let pair = Pair::open(37473).await?;
    let a = ModuleContext::new(
        pair.a.clone(),
        A.into(),
        DOMAIN.into(),
        Shutdown::new(),
        None,
        Vec::new(),
    )
    .await?;
    let b = ModuleContext::new(
        pair.b.clone(),
        B.into(),
        DOMAIN.into(),
        Shutdown::new(),
        None,
        Vec::new(),
    )
    .await?;
    let c = ModuleContext::new(
        pair.a.clone(),
        C.into(),
        DOMAIN.into(),
        Shutdown::new(),
        None,
        Vec::new(),
    )
    .await?;
    let target = Key::output(DOMAIN, A, "test", Probe::TYPE_NAME);
    let (signal, ready) = query_signal(&pair.right, &target)?;
    let publisher = a.publisher::<Probe>().latched().build()?;
    let original = Arc::new(Probe { seq: 81 });
    publisher.send_shared(Arc::clone(&original)).await?;
    matched(&ready, true).await?;

    let mut local = c.subscriber::<Probe>().latched().build()?;
    let mut remote = b.subscriber::<Probe>().latched().build()?;
    let native = timeout(WAIT, local.recv_shared_envelope())
        .await?
        .context("native replay")?;
    let wire = timeout(WAIT, remote.recv_shared_envelope())
        .await?
        .context("wire replay")?;
    assert!(Arc::ptr_eq(&native.value, &original));
    assert!(!Arc::ptr_eq(&wire.value, &original));
    for received in [&native, &wire] {
        assert_eq!(received.value.seq, 81);
        assert_eq!(
            (received.source.as_str(), received.source_port.as_str()),
            (A, "test")
        );
    }
    let raw = replies(pair.b.query(&target, params(None))?).await?;
    assert_eq!(raw.len(), 1);
    assert_eq!(
        success(&raw[0])?.attachment,
        Some(Probe::SCHEMA_FINGERPRINT.to_le_bytes().to_vec())
    );
    let (sub_signal, subscribed) = subscription_signal(&pair.left, &target)?;
    matched(&subscribed, true).await?;
    publisher.send_shared(Arc::new(Probe { seq: 82 })).await?;
    for input in [&mut local, &mut remote] {
        assert_eq!(
            timeout(WAIT, input.recv())
                .await?
                .context("live value")?
                .seq,
            82,
            "no duplicate replay"
        );
    }
    a.shutdown_now();
    b.shutdown_now();
    c.shutdown_now();
    assert!(timeout(WAIT, local.recv()).await?.is_none());
    assert!(timeout(WAIT, remote.recv()).await?.is_none());
    drop((local, remote, publisher, a, b, c, signal, sub_signal));
    pair.close().await
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_query_merges_input_and_output_facets_across_placements() -> Result<()> {
    let pair = Pair::open(37474).await?;
    let output = key(A).with_chunk("@schema/example.Probe");
    let input = Key::input(DOMAIN, B, "sink", "Probe").with_chunk("@schema/example.Probe");
    let (signal, ready) = query_signal(&pair.left, &input)?;
    let mut guards = Vec::new();
    for (engine, target) in [(&pair.a, &output), (&pair.b, &input)] {
        let reply_key = target.clone();
        guards.push(engine.respond(
            target,
            Box::new(move |query| {
                assert!(query.reply(&reply_key, vec![1], None).is_ok());
            }),
        )?);
    }
    matched(&ready, true).await?;

    let received = replies(
        pair.a
            .query(&Key::all(DOMAIN).with_chunk("@schema/*"), params(None))?,
    )
    .await?;
    assert_eq!(received.len(), 2);
    assert_eq!(
        received
            .into_iter()
            .map(|reply| reply
                .map(|sample| sample.key)
                .map_err(|bytes| anyhow::anyhow!("reply error {bytes:?}")))
            .collect::<Result<HashSet<_>>>()?,
        HashSet::from([output, input])
    );
    drop((guards, signal));
    pair.close().await
}

#[tokio::test(flavor = "multi_thread")]
async fn sdk_exact_routes_reject_other_sources_ports_and_fingerprints() -> Result<()> {
    let pair = Pair::open(37476).await?;
    let b = ModuleContext::new(
        pair.b.clone(),
        B.into(),
        DOMAIN.into(),
        Shutdown::new(),
        None,
        Vec::new(),
    )
    .await?;
    let c = ModuleContext::new(
        pair.a.clone(),
        C.into(),
        DOMAIN.into(),
        Shutdown::new(),
        None,
        Vec::new(),
    )
    .await?;
    let target = Key::output(DOMAIN, A, "test", Probe::TYPE_NAME);
    let (signal, ready) = subscription_signal(&pair.left, &target)?;
    let mut remote = b.subscriber::<Probe>().from(A).build()?;
    let mut local = c.subscriber::<Probe>().from(A).build()?;
    matched(&ready, true).await?;
    let fingerprint = Some(Probe::SCHEMA_FINGERPRINT.to_le_bytes().to_vec());
    let mut publishers = Vec::new();
    for wrong in [
        Key::output(DOMAIN, C, "test", Probe::TYPE_NAME),
        Key::output(DOMAIN, A, "other", Probe::TYPE_NAME),
    ] {
        let publisher = pair.a.publisher(&wrong, &Qos::DEFAULT)?;
        publisher.put(Probe { seq: 0 }.encode_to_vec(), fingerprint.clone())?;
        publishers.push(publisher);
    }
    let publisher = pair.a.publisher(&target, &Qos::DEFAULT)?;
    publisher.put(Probe { seq: 0 }.encode_to_vec(), Some(vec![0; 8]))?;
    let value = Arc::new(Probe { seq: 97 });
    publisher.put_payload(Payload::shared(Arc::clone(&value)), fingerprint)?;

    let native = timeout(WAIT, local.recv_shared_envelope())
        .await?
        .context("native sample")?;
    let wire = timeout(WAIT, remote.recv_shared_envelope())
        .await?
        .context("wire sample")?;
    assert!(Arc::ptr_eq(&native.value, &value));
    for sample in [native, wire] {
        assert_eq!(sample.value.seq, 97);
        assert_eq!(
            (sample.source.as_str(), sample.source_port.as_str()),
            (A, "test")
        );
    }
    b.shutdown_now();
    c.shutdown_now();
    drop((local, remote, publisher, publishers, b, c, signal));
    pair.close().await
}
