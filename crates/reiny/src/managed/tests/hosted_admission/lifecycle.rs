use super::*;

#[tokio::test]
async fn lost_ports_invalidate_prepared_and_committed_admission() {
    for committed in [false, true] {
        let bus = Arc::new(Local::new());
        let context = hosted(bus.clone(), SENDER).await;
        let output = context.output::<Probe>("outgoing").expect("output");
        let mut events = context.hosted_events();
        let mut prepared = Some(context.prepare_admission().expect("prepare"));
        let _ready = committed.then(|| {
            context
                .commit_admission(prepared.take().expect("ticket"))
                .expect("commit")
        });

        drop(output);

        timeout(PATIENCE, events.changed())
            .await
            .expect("loss deadline")
            .expect("loss event");
        assert_eq!(*events.borrow(), Some(HostedEvent::ReadinessLost));
        assert_eq!(
            *context.hosted_events().borrow(),
            Some(HostedEvent::ReadinessLost)
        );
        if let Some(prepared) = prepared {
            assert!(matches!(
                context.commit_admission(prepared),
                Err(AdmissionError::StaleTicket)
            ));
        }
        assert_eq!(
            bus.alive(&Key::module(DOMAIN, Some(SENDER), "@ready"), PATIENCE)
                .await
                .expect("presence"),
            Vec::<Key>::new()
        );
        timeout(PATIENCE, context.shutdown())
            .await
            .expect("loss requests shutdown");
    }
}

#[tokio::test]
async fn shutdown_rejects_preparation_and_commit_without_readiness() {
    for prepare_first in [false, true] {
        let bus = Arc::new(Local::new());
        let context = hosted(bus.clone(), SENDER).await;
        let _output = context.output::<Probe>("outgoing").expect("output");
        let prepared = prepare_first.then(|| context.prepare_admission().expect("prepare"));
        let mut events = context.hosted_events();

        context.shutdown_now();

        timeout(PATIENCE, events.changed())
            .await
            .expect("stop deadline")
            .expect("stop event");
        assert_eq!(*events.borrow(), Some(HostedEvent::StopRequested));
        assert!(matches!(
            context.prepare_admission(),
            Err(AdmissionError::ShuttingDown)
        ));
        if let Some(prepared) = prepared {
            assert!(matches!(
                context.commit_admission(prepared),
                Err(AdmissionError::ShuttingDown)
            ));
        }
        assert_eq!(
            bus.alive(&Key::module(DOMAIN, Some(SENDER), "@ready"), PATIENCE)
                .await
                .expect("presence"),
            Vec::<Key>::new()
        );
    }
}

#[tokio::test]
async fn first_stop_or_loss_cause_survives_the_other_event() {
    for stop_first in [false, true] {
        let context = hosted(Arc::new(Local::new()), SENDER).await;
        let output = context.output::<Probe>("outgoing").expect("output");
        let _ready = context
            .commit_admission(context.prepare_admission().expect("prepare"))
            .expect("commit");
        let mut events = context.hosted_events();
        let (release, released) = tokio::sync::oneshot::channel();
        let loss = tokio::spawn(async move {
            released.await.expect("release loss");
            drop(output);
        });

        if stop_first {
            context.shutdown_now();
        }
        release.send(()).expect("release");
        timeout(PATIENCE, events.changed())
            .await
            .expect("event deadline")
            .expect("first event");
        timeout(PATIENCE, loss)
            .await
            .expect("joined loss deadline")
            .expect("loss task");
        context.shutdown_now();

        let expected = if stop_first {
            HostedEvent::StopRequested
        } else {
            HostedEvent::ReadinessLost
        };
        assert_eq!(*events.borrow(), Some(expected.clone()));
        assert_eq!(*context.hosted_events().borrow(), Some(expected));
    }
}

#[tokio::test]
async fn context_drop_withdraws_admission_even_if_ticket_outlives_it() {
    let bus = Arc::new(Local::new());
    let context = hosted(bus.clone(), SENDER).await;
    let _output = context.output::<Probe>("outgoing").expect("output");
    let _ready = context
        .commit_admission(context.prepare_admission().expect("prepare"))
        .expect("commit");
    let key = Key::module(DOMAIN, Some(SENDER), "@ready");
    assert_eq!(
        bus.alive(&key, PATIENCE).await.expect("ready presence"),
        vec![key.clone()]
    );

    drop(context);

    assert_eq!(
        bus.alive(&key, PATIENCE).await.expect("withdrawn presence"),
        Vec::<Key>::new()
    );
}
