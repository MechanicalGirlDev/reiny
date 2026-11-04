use super::*;

#[test]
fn duplicate_registration_preserves_the_first_factory() {
    let (records, _rx) = mpsc::channel(4);
    let (_gate, gate) = watch::channel(false);
    let factory = Arc::new(Factory {
        behavior: Behavior::Idle,
        records,
        gate,
    });
    let mut registry = ComponentRegistry::new();
    registry
        .register("factory".to_owned(), factory.clone())
        .expect("first");

    let error = registry
        .register("factory".to_owned(), factory.clone())
        .expect_err("duplicate");

    assert_eq!(error.code, HostErrorCode::DuplicateFactory);
    let original: Arc<dyn ComponentFactory> = factory;
    assert!(Arc::ptr_eq(&registry.factories["factory"], &original));
}

#[tokio::test]
async fn invalid_launches_never_construct_components() {
    let (registry, mut records, _gate) = fixture(Behavior::Idle);
    let mut invalid = instance("app/bad", "test");
    invalid.bindings.version = 0;
    let mut delegated = instance("app/parent/owned/child", "test");
    delegated.bindings.endpoint_namespace = "app/parent".to_owned();
    for (instances, code) in [
        (vec![], HostErrorCode::EmptyGraph),
        (
            vec![instance("app/a", "test"), instance("app/a", "test")],
            HostErrorCode::DuplicateInstance,
        ),
        (
            vec![instance("app/a", "absent")],
            HostErrorCode::UnknownFactory,
        ),
        (vec![invalid], HostErrorCode::InvalidBindings),
        (vec![delegated], HostErrorCode::InvalidBindings),
    ] {
        let result =
            ComponentHost::launch(registry.clone(), options(Arc::new(Local::new())), instances);

        assert_eq!(result.err().expect("launch rejected").code, code);
    }
    let mut invalid_options = options(Arc::new(Local::new()));
    invalid_options.stop_timeout = Duration::ZERO;
    let result = ComponentHost::launch(registry, invalid_options, vec![instance("app/a", "test")]);
    assert_eq!(
        result.err().expect("invalid options").code,
        HostErrorCode::InvalidOptions
    );
    assert!(records.try_recv().is_err());
}

#[test]
fn launch_without_tokio_runtime_is_rejected() {
    let (registry, _records, _gate) = fixture(Behavior::Idle);

    let result = ComponentHost::launch(
        registry,
        options(Arc::new(Local::new())),
        vec![instance("app/a", "test")],
    );

    assert_eq!(
        result.err().expect("runtime required").code,
        HostErrorCode::NoRuntime
    );
}
