use super::*;

#[tokio::test]
async fn binding_files_are_strict_and_never_downgrade_to_standalone() {
    // Given malformed, unknown-version, unknown-field, and mismatched-namespace contracts.
    let directory = Directory::new();
    let path = directory.0.join("bindings.json");
    for json in [
        "{",
        r#"{"version":2,"namespace":"deployment/consumer","inputs":{},"outputs":{}}"#,
        r#"{"version":1,"namespace":"deployment/consumer","inputs":{},"outputs":{},"typo":1}"#,
        r#"{"version":1,"namespace":"other/consumer","inputs":{},"outputs":{}}"#,
    ] {
        std::fs::write(&path, json).expect("binding fixture");
        let mut opts = RuntimeOptions::new(RECEIVER);
        opts.engine = Some(Arc::new(Local::new()));
        opts.module_bindings_path = Some(path.clone());
        opts.module_report_path = None;

        // When startup parses that contract, then it fails instead of widening subscriptions.
        assert!(Cloudy::open(opts).await.is_err());
    }
}

#[tokio::test]
async fn invalid_namespace_segments_are_rejected_at_startup() {
    // Given namespace paths that could change address semantics.
    for namespace in [
        "", "/module", "module/", "a//b", "a/*", "a/../b", "a/@ready",
    ] {
        let opts = options(Arc::new(Local::new()), bindings(namespace));

        // When startup validates the namespace, then invalid paths never reach the engine.
        assert!(Cloudy::open(opts).await.is_err());
    }
}

#[tokio::test]
async fn managed_config_parse_errors_are_fatal() {
    // Given a managed module with a malformed JSON application config.
    let directory = Directory::new();
    let path = directory.0.join("config.json");
    std::fs::write(&path, "{").expect("config fixture");
    let mut opts = options(Arc::new(Local::new()), bindings(RECEIVER));
    opts.config_path = Some(path);

    // When startup loads it, then defaults cannot conceal the invalid configuration.
    assert!(Cloudy::open(opts).await.is_err());
}

#[tokio::test]
async fn standalone_config_parse_errors_retain_the_existing_fallback() {
    // Given an old standalone entrypoint with a malformed config.
    let directory = Directory::new();
    let path = directory.0.join("config.json");
    std::fs::write(&path, "{").expect("config fixture");
    let mut opts = RuntimeOptions::new("standalone");
    opts.engine = Some(Arc::new(Local::new()));
    opts.module_report_path = None;
    opts.config_path = Some(path);

    // When it opens, then its existing default-config behavior is preserved.
    assert!(
        Cloudy::open(opts)
            .await
            .expect("standalone")
            .config_table()
            .is_none()
    );
}

#[tokio::test]
async fn relative_binding_and_report_paths_are_rejected() {
    // Given startup paths that would depend on the process working directory.
    let mut binding_path = RuntimeOptions::new(RECEIVER);
    binding_path.engine = Some(Arc::new(Local::new()));
    binding_path.module_bindings_path = Some(PathBuf::from("bindings.json"));
    binding_path.module_report_path = None;
    let mut report_path = options(Arc::new(Local::new()), bindings(RECEIVER));
    report_path.module_report_path = Some(PathBuf::from("report.json"));

    // When either boundary is opened, then the ambiguous path is rejected.
    assert!(Cloudy::open(binding_path).await.is_err());
    assert!(Cloudy::open(report_path).await.is_err());
}

#[test]
fn owner_report_marker_cannot_be_cleared_for_an_unbound_engine() -> crate::Result<()> {
    let report_env = crate::managed::MODULE_REPORT_ENV;
    if std::env::var_os(report_env).is_some() {
        // Given a real owner-marked process using a fresh, unrelated engine.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let bus = Arc::new(Local::new());
        let mut opts = RuntimeOptions::new("unbound");
        opts.domain = DOMAIN.into();
        opts.engine = Some(bus.clone());
        opts.module_report_path = None;
        // When application code clears its inherited option before opening a second bus.
        let opened = runtime.block_on(Cloudy::open(opts));
        // Then the owned process cannot downgrade and creates no launch token.
        assert!(opened.is_err());
        assert_eq!(
            runtime
                .block_on(bus.alive(&Key::launch(DOMAIN, Some("unbound")), PATIENCE))?
                .len(),
            0
        );
    } else {
        // Run the assertion in an isolated process, without mutating parallel tests' environment.
        let directory = Directory::new();
        let output = std::process::Command::new(std::env::current_exe()?)
            .args(["--exact", "managed::tests::startup::owner_report_marker_cannot_be_cleared_for_an_unbound_engine", "--nocapture"])
            .env(report_env, directory.0.join("owner-report.json"))
            .output()?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}
