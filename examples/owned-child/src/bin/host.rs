use reiny::{Cloudy, RuntimeOptions};

#[tokio::main]
async fn main() -> reiny::Result<()> {
    let cloudy = Cloudy::open(RuntimeOptions::from_args("host")).await?;
    let runtime = tempfile::tempdir()?;
    let mut command = tokio::process::Command::new(cloudy.artifact("driver")?);
    let mut child = cloudy.spawn_owned_child("driver", &mut command, runtime.path())?;
    child.wait_ready().await?;
    cloudy.ready()?;
    cloudy.shutdown().await;
    if !child.wait().await?.success() {
        return Err(std::io::Error::other("driver did not stop cooperatively").into());
    }
    println!("OWNED_CHILD_STOPPED pid={} success=true", child.id());
    Ok(())
}
