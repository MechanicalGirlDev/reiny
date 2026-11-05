use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn wildcard_presence_merges_owners_and_guard_drop_withdraws_both_transports() -> Result<()> {
    let pair = Pair::open(37472).await?;
    let pattern = Key::launch(DOMAIN, None);
    let a = Key::launch(DOMAIN, Some(A));
    let b = Key::launch(DOMAIN, Some(B));
    let (callback, at_a) = collector();
    let watch_a = pair.a.watch_alive(&pattern, callback)?;
    let (callback, at_b) = collector();
    let watch_b = pair.b.watch_alive(&pattern, callback)?;
    let token_a = pair.a.declare_alive(&a)?;
    let token_b = pair.b.declare_alive(&b)?;
    for receiver in [&at_a, &at_b] {
        let events = [recv(receiver).await?, recv(receiver).await?];
        assert!(events.contains(&Presence::Joined(a.clone())));
        assert!(events.contains(&Presence::Joined(b.clone())));
    }
    for engine in [&pair.a, &pair.b] {
        assert_eq!(
            engine
                .alive(&pattern, WAIT)
                .await?
                .into_iter()
                .collect::<HashSet<_>>(),
            HashSet::from([a.clone(), b.clone()])
        );
    }
    drop((token_a, token_b));
    for receiver in [&at_a, &at_b] {
        let events = [recv(receiver).await?, recv(receiver).await?];
        assert!(events.contains(&Presence::Left(a.clone())));
        assert!(events.contains(&Presence::Left(b.clone())));
    }
    drop((watch_a, watch_b));
    pair.close().await
}
