use std::path::Path;

use super::startup::parse_config;
use super::*;

fn parse(args: &[&str]) -> RuntimeOptions {
    RuntimeOptions::from_arg_list("launch", args.iter().map(|s| (*s).to_string()))
}

/// YAML is the default (any extension but `.toml` / `.json`), and every format lands in the
/// same table. A file in the wrong format is an error (not an empty table).
#[test]
fn config_format_follows_the_extension() {
    let yaml = parse_config(Path::new("c.yaml"), "reply: PONG!\ndelay_ms: 250\n").expect("yaml");
    assert_eq!(yaml["reply"].as_str(), Some("PONG!"));
    assert_eq!(yaml["delay_ms"].as_integer(), Some(250));
    let bare = parse_config(Path::new("pong.config"), "delay_ms: 250\n").expect("no ext");
    assert_eq!(bare["delay_ms"].as_integer(), Some(250));
    let json = parse_config(
        Path::new("c.JSON"),
        r#"{"reply": "PONG!", "delay_ms": 250}"#,
    )
    .expect("json");
    assert_eq!(json["reply"].as_str(), Some("PONG!"));
    assert_eq!(json["delay_ms"].as_integer(), Some(250));
    let toml = parse_config(Path::new("c.toml"), "delay_ms = 250\n").expect("toml");
    assert_eq!(toml["delay_ms"].as_integer(), Some(250));
    assert!(parse_config(Path::new("c.yml"), "delay_ms = 250\n").is_err());
}

#[test]
fn known_flags_are_consumed() {
    let o = parse(&[
        "--name",
        "pong-2",
        "--domain",
        "lab",
        "--log-level",
        "debug",
        "--config",
        "g.toml",
    ]);
    assert_eq!(o.id, "pong-2");
    assert_eq!(o.domain, "lab");
    assert_eq!(o.log_level, Level::DEBUG);
    assert_eq!(o.config_path, Some(PathBuf::from("g.toml")));
    assert_eq!(o.extra_args, Vec::<String>::new());
}

#[test]
fn unknown_args_pass_through_in_order() {
    let o = parse(&["--port", "50051", "--name", "ctrl", "--fast"]);
    assert_eq!(o.id, "ctrl");
    assert_eq!(o.extra_args, ["--port", "50051", "--fast"]);
}

#[tokio::test]
async fn managed_flag_without_value_fails_before_engine_startup() {
    // Given a missing bindings value followed by another recognized argument.
    let mut opts = parse(&["--module-bindings", "--name", "deployment/module"]);
    opts.engine = Some(Arc::new(crate::engine::Local::new()));
    opts.module_report_path = None;
    assert_eq!(opts.id, "deployment/module");

    // When startup resolves managed options, then it cannot become standalone.
    assert!(Cloudy::open(opts).await.is_err());
}

#[test]
fn managed_binding_flag_is_consumed_without_flattening_namespace() {
    // Given the launcher's named-module arguments.
    let opts = parse(&[
        "--name",
        "deployment/nested/module",
        "--module-bindings",
        "/bindings.json",
    ]);

    // When options are parsed, then the full namespace and contract path survive verbatim.
    assert_eq!(opts.id, "deployment/nested/module");
    assert_eq!(
        opts.module_bindings_path,
        Some(PathBuf::from("/bindings.json"))
    );
    assert_eq!(opts.extra_args, Vec::<String>::new());
}

#[cfg(feature = "zenoh")]
#[test]
fn connect_endpoints_become_one_json5_override() {
    let o = parse(&[
        "--connect",
        "tcp/1.2.3.4:7447",
        "--connect",
        "tcp/5.6.7.8:7447",
        "--zenoh-mode",
        "client",
    ]);
    assert!(
        o.zenoh_overrides
            .contains(&("mode".to_string(), "\"client\"".to_string()))
    );
    let (key, value) = o
        .zenoh_overrides
        .iter()
        .find(|(k, _)| k == "connect/endpoints")
        .expect("connect override present");
    assert_eq!(key, "connect/endpoints");
    assert_eq!(value, "[\"tcp/1.2.3.4:7447\",\"tcp/5.6.7.8:7447\"]");
}

#[cfg(feature = "zenoh")]
#[test]
fn zenoh_config_flag_selects_file_source() {
    let o = parse(&["--zenoh-config", "z.json5"]);
    assert!(matches!(o.zenoh, ZenohSource::File(p) if p == *Path::new("z.json5")));
}

/// A zenoh argument is consumed together with its value (feature or no feature) and never leaks
#[test]
fn zenoh_args_never_leak_into_extra_args() {
    let o = parse(&[
        "--connect",
        "tcp/1.2.3.4:7447",
        "--zenoh-mode",
        "client",
        "--x",
    ]);
    assert_eq!(o.extra_args, ["--x"]);
}
