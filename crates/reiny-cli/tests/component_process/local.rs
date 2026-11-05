//! Public CLI proof of explicit isolation, not a private engine-only fixture.

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};

use super::fixture::{Events, Fixture, Hosts, success};

pub(super) async fn delivery_and_rejection(hosts: &Hosts, endpoint: &str) -> Result<()> {
    for local in [false, true] {
        let mut events = Events::listen().await?;
        let fixture = Fixture::new(hosts, endpoint, &events.address, false, "1")?;
        let path = fixture.path().join("main.yaml");
        let mut manifest: Value = serde_yaml::from_str(&std::fs::read_to_string(&path)?)?;
        manifest["providers"] = json!({
            "combined": {"type": "process", "bin_dir": hosts.binaries}
        });
        manifest["component_hosts"] = json!({
            "combined": {"provider": "combined", "bin": "placement-local"}
        });
        if local {
            manifest["component_hosts"]["combined"]["transport"] = "local".into();
        }
        manifest["placement"] = json!({
            "placement/sender": "combined", "placement/receiver": "combined"
        });
        std::fs::write(&path, serde_yaml::to_string(&manifest)?)?;
        fixture.prepare().await?;
        success(&fixture.apply().await?, "same-host shared delivery")?;
        let ready = fixture.status().await?;
        assert_eq!(ready["phase"], "ready");
        assert_eq!(
            ready["component_hosts"].as_object().context("hosts")?.len(),
            1
        );
        let pid = ready["component_hosts"]["combined"]["pid"]
            .as_u64()
            .context("host PID")?;
        for (role, source, port) in [
            ("receiver", "placement/sender", "data"),
            ("sender", "placement/receiver", "acknowledgement"),
        ] {
            let delivered = events.wait(role, "delivered").await?;
            assert_eq!(delivered["pid"], pid);
            assert_eq!(delivered["native"], true);
            assert_eq!(delivered["sequence"], 42);
            assert_eq!(delivered["source"], source);
            assert_eq!(delivered["source_port"], port);
            if local {
                assert_eq!(delivered["encodes"], 0);
            }
        }
        let stopped = fixture.stop().await?;
        assert_eq!(stopped["phase"], "stopped");
        assert!(stopped["component_hosts"]["combined"]["pid"].is_null());
        assert!(!stopped["component_hosts"]["combined"]["exit"].is_null());
        for role in ["sender", "receiver"] {
            let stopped = events.wait(role, "stop").await?;
            // Both sends are complete before stop. The default route is a
            // positive network-encoding control for the very same SDK fixture.
            assert_eq!(stopped["encodes"], if local { 0 } else { 2 });
        }
    }

    for isolated in ["producer", "consumer"] {
        let mut events = Events::listen().await?;
        let fixture = Fixture::new(hosts, endpoint, &events.address, false, "1")?;
        let path = fixture.path().join("main.yaml");
        let mut manifest: Value = serde_yaml::from_str(&std::fs::read_to_string(&path)?)?;
        manifest["component_hosts"][isolated]["transport"] = "local".into();
        manifest["providers"][isolated] = json!({
            "type": "process", "bin_dir": hosts.binaries
        });
        std::fs::write(&path, serde_yaml::to_string(&manifest)?)?;
        let root = fixture.path().to_str().context("root")?;
        let prepared = fixture.path().join("rejected.json");
        let output = fixture
            .cli(&[
                "prepare",
                root,
                "--out",
                prepared.to_str().context("prepared path")?,
            ])
            .await?;
        ensure!(!output.status.success(), "cross-host local graph accepted");
        ensure!(
            String::from_utf8_lossy(&output.stderr).contains("local-only host boundary"),
            "rejection did not reach transport validation: {output:?}"
        );
        assert!(reiny_launch::DeploymentClient::find(fixture.path())?.is_none());
        for role in ["sender", "receiver"] {
            assert!(!fixture.path().join(format!("{role}.ran")).exists());
        }
        events.assert_no_run()?;
    }
    Ok(())
}
