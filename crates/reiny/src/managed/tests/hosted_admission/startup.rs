use super::*;
use crate::bindings::PortDelegation;

#[tokio::test]
async fn hosted_startup_rejects_invalid_identity_bindings_and_delegation() {
    let bus = Arc::new(Local::new());
    let mut invalid_domain = hosted_options(bus.clone(), bindings(SENDER));
    invalid_domain.domain = "wild/*".into();
    assert!(matches!(
        ModuleContext::open_hosted(invalid_domain).await,
        Err(AdmissionError::InvalidIdentity(_))
    ));
    let invalid_namespace = hosted_options(bus.clone(), bindings("bad/*"));
    assert!(matches!(
        ModuleContext::open_hosted(invalid_namespace).await,
        Err(AdmissionError::InvalidIdentity(_))
    ));
    let mut invalid_version = bindings(SENDER);
    invalid_version.version = 0;
    assert!(matches!(
        ModuleContext::open_hosted(hosted_options(bus.clone(), invalid_version)).await,
        Err(AdmissionError::InvalidBindings(_))
    ));
    for children in [false, true] {
        let mut declared = bindings(SENDER);
        if children {
            declared
                .children
                .insert("worker".into(), PortDelegation::default());
        } else {
            declared.executables.push("worker".into());
        }
        assert!(matches!(
            ModuleContext::open_hosted(hosted_options(bus.clone(), declared)).await,
            Err(AdmissionError::UnsupportedHostedBindings)
        ));
    }
    assert_eq!(
        bus.alive(&Key::launch(DOMAIN, Some(SENDER)), PATIENCE)
            .await
            .expect("no launch"),
        Vec::<Key>::new()
    );
}

#[tokio::test]
async fn preparation_never_writes_the_legacy_report_destination() {
    let mut context = hosted(Arc::new(Local::new()), SENDER).await;
    let _output = context.output::<Probe>("outgoing").expect("output");
    // An impossible parent proves preparation never invokes the legacy file-writing path.
    context.module.report_path = Some(
        std::env::current_exe()
            .expect("test executable")
            .join("report.json"),
    );

    context.prepare_admission().expect("in-memory report");
}

#[tokio::test]
async fn remote_stop_emits_stop_request_and_preserves_acknowledgement() {
    let bus = Arc::new(Local::new());
    let context = hosted(bus.clone(), SENDER).await;
    let mut events = context.hosted_events();

    let mut replies = bus
        .query(
            &Key::module(DOMAIN, Some(SENDER), "@stop"),
            QueryParams {
                payload: Some(Vec::new()),
                attachment: None,
                timeout: PATIENCE,
            },
        )
        .expect("stop query");

    let ack = timeout(PATIENCE, replies.next())
        .await
        .expect("ack deadline")
        .expect("ack exists")
        .expect("ack success");
    assert_eq!(ack.payload, b"ack");
    timeout(PATIENCE, events.changed())
        .await
        .expect("stop deadline")
        .expect("stop event");
    assert_eq!(*events.borrow(), Some(HostedEvent::StopRequested));
    timeout(PATIENCE, context.shutdown())
        .await
        .expect("shutdown signaled");
}

#[test]
fn hosted_context_uses_only_supplied_configuration_and_paths() -> crate::Result<()> {
    const CHILD: &str = "REINY_HOSTED_ISOLATION_TEST";
    if std::env::var_os(CHILD).is_none() {
        let executable = std::env::current_exe()?;
        let output = std::process::Command::new(&executable)
            .args(["--exact", "managed::tests::hosted_admission::startup::hosted_context_uses_only_supplied_configuration_and_paths", "--nocapture"])
            .env(CHILD, "1")
            .env(crate::runtime::DOMAIN_ENV, "foreign-domain")
            .env(crate::managed::MODULE_REPORT_ENV, executable.join("report.json"))
            .env(crate::managed::BUNDLE_DIR_ENV, executable.join("bundle"))
            .env(crate::managed::CONFIG_DIR_ENV, executable.join("config"))
            .output()?;
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return Ok(());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let bus = Arc::new(Local::new());
        let supplied = toml::Table::from_iter([(
            "identity".into(),
            toml::Value::String("instance-only".into()),
        )]);
        let mut options = hosted_options(bus.clone(), bindings(SENDER));
        options.config = Some(supplied.clone());
        let context = ModuleContext::open_hosted(options).await?;
        assert_eq!(context.config_table(), Some(&supplied));
        assert_eq!(context.domain(), DOMAIN);
        assert_eq!(context.extra_args(), Vec::<String>::new());
        assert!(context.module.report_path.is_none());
        assert!(context.module.bundle_dir.is_none());
        assert!(context.module.config_dir.is_none());
        assert!(context.module.child_zenoh_config.is_none());
        let other = ModuleContext::open_hosted(hosted_options(bus, bindings(RECEIVER))).await?;
        assert!(other.config_table().is_none());
        assert!(!other.shutdown.is_triggered());
        context.shutdown_now();
        assert!(!other.shutdown.is_triggered());
        assert_eq!(*other.hosted_events().borrow(), None);
        other.commit_admission(other.prepare_admission()?)?;
        Ok(())
    })
}
