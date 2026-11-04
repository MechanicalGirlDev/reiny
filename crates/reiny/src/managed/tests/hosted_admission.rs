//! Admission tests observe the real Local engine and context lifecycle.

use super::*;
use crate::managed::{AdmissionError, HostedEvent, HostedOptions};

mod lifecycle;
mod startup;
mod transport;

fn hosted_options(bus: Arc<dyn Engine>, bindings: ModuleBindings) -> HostedOptions {
    HostedOptions {
        domain: DOMAIN.into(),
        bindings,
        config: None,
        engine: bus,
    }
}

async fn hosted(bus: Arc<dyn Engine>, namespace: &str) -> ModuleContext {
    let mut declared = bindings(namespace);
    declared.outputs.insert(
        "outgoing".into(),
        OutputBinding {
            contract: contract("test.Probe"),
        },
    );
    ModuleContext::open_hosted(hosted_options(bus, declared))
        .await
        .expect("hosted context")
}

#[tokio::test]
async fn preparation_freezes_registration_without_publishing_readiness() {
    let bus = Arc::new(Local::new());
    let context = hosted(bus.clone(), SENDER).await;
    let _output = context.output::<Probe>("outgoing").expect("output");
    let events = context.hosted_events();

    let prepared = context.prepare_admission().expect("prepare");

    assert_eq!(
        prepared.report().outputs["outgoing"].schema,
        Probe::SCHEMA_FINGERPRINT
    );
    assert_eq!(prepared.report().namespace, SENDER);
    assert!(matches!(
        context.prepare_admission(),
        Err(AdmissionError::AlreadyPrepared)
    ));
    assert!(context.ready().is_err());
    // The shared registry lock, rather than the duplicate-port check, rejects admission-time registration.
    assert!(context.lock_ports("new", false).is_err());
    assert_eq!(
        bus.alive(&Key::module(DOMAIN, Some(SENDER), "@ready"), PATIENCE)
            .await
            .expect("presence"),
        Vec::<Key>::new()
    );
    assert_eq!(*events.borrow(), None);
}

#[tokio::test]
async fn failed_preparation_leaves_registration_open() {
    let bus = Arc::new(Local::new());
    let context = hosted(bus, SENDER).await;

    assert!(matches!(context.prepare_admission(),
        Err(AdmissionError::PortUnavailable { input: false, name }) if name == "outgoing"));

    let _output = context
        .output::<Probe>("outgoing")
        .expect("registration remains open");
    context.prepare_admission().expect("retry preparation");
}

#[tokio::test]
async fn a_handle_lost_before_preparation_cannot_supply_a_report() {
    let context = hosted(Arc::new(Local::new()), SENDER).await;
    let output = context.output::<Probe>("outgoing").expect("output");

    drop(output);

    assert!(matches!(context.prepare_admission(),
        Err(AdmissionError::PortUnavailable { input: false, name }) if name == "outgoing"));
    assert_eq!(*context.hosted_events().borrow(), None);
}

#[tokio::test]
async fn same_namespace_does_not_allow_foreign_context_tickets() {
    let bus = Arc::new(Local::new());
    let first = hosted(bus.clone(), SENDER).await;
    let second = hosted(bus.clone(), SENDER).await;
    let _first_output = first.output::<Probe>("outgoing").expect("first output");
    let _second_output = second.output::<Probe>("outgoing").expect("second output");
    let prepared = first.prepare_admission().expect("prepare first");

    assert!(matches!(
        second.commit_admission(prepared),
        Err(AdmissionError::ForeignTicket)
    ));

    first
        .prepare_admission()
        .expect("foreign failure cancels original preparation");
    assert_eq!(
        bus.alive(&Key::module(DOMAIN, Some(SENDER), "@ready"), PATIENCE)
            .await
            .expect("presence"),
        Vec::<Key>::new()
    );
}

#[tokio::test]
async fn prepared_ticket_drop_thaws_registration_without_failure() {
    let context = hosted(Arc::new(Local::new()), SENDER).await;
    let _output = context.output::<Probe>("outgoing").expect("output");
    let prepared = context.prepare_admission().expect("prepare");

    drop(prepared);

    assert!(context.lock_ports("new", false).is_ok());
    context.prepare_admission().expect("fresh generation");
    assert_eq!(*context.hosted_events().borrow(), None);
}

#[tokio::test]
async fn ready_ticket_drop_withdraws_presence_without_failure() {
    let bus = Arc::new(Local::new());
    let context = hosted(bus.clone(), SENDER).await;
    let _output = context.output::<Probe>("outgoing").expect("output");
    let prepared = context.prepare_admission().expect("prepare");
    let ready = context.commit_admission(prepared).expect("commit");
    let key = Key::module(DOMAIN, Some(SENDER), "@ready");
    assert_eq!(
        bus.alive(&key, PATIENCE).await.expect("ready presence"),
        vec![key.clone()]
    );
    assert!(matches!(
        context.prepare_admission(),
        Err(AdmissionError::AlreadyReady)
    ));

    drop(ready);

    assert_eq!(
        bus.alive(&key, PATIENCE).await.expect("withdrawn presence"),
        Vec::<Key>::new()
    );
    assert_eq!(*context.hosted_events().borrow(), None);
    context
        .prepare_admission()
        .expect("explicit withdrawal allows another generation");
}
