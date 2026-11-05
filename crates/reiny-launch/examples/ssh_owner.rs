//! Minimal explicit remote-owner embedding.
//! Arguments: verified prepared JSON and a JSON host-name-to-`SshTarget` map.
//! Local `DeploymentClient` controls stop. Unknown records require reconciliation.

use std::sync::atomic::AtomicBool;

use anyhow::{Context, Result};
use reiny_launch::{ModuleObserver, PreparedDeployment};

struct HostedOnly;

impl ModuleObserver for HostedOnly {
    fn ready(&self, _: &str) -> Result<bool> {
        anyhow::bail!("SSH owner example accepts hosted components only")
    }
    fn request_stop(&self, _: &str) -> Result<()> {
        anyhow::bail!("SSH owner example accepts hosted components only")
    }
}

fn main() -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    let prepared = PreparedDeployment::load_file(std::path::Path::new(
        &args.next().context("prepared JSON argument required")?,
    ))?;
    let targets = serde_json::from_slice(&std::fs::read(
        args.next()
            .context("explicit SSH target JSON argument required")?,
    )?)?;
    anyhow::ensure!(args.next().is_none(), "unexpected argument");
    reiny_launch::serve_remote(
        prepared,
        targets,
        &HostedOnly,
        &AtomicBool::new(false),
        &|| {
            use std::io::Write;
            println!("OWNER_LISTENING");
            std::io::stdout().flush()?;
            Ok(())
        },
    )
}
