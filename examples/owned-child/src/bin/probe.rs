use std::time::Duration;

use owned_child_example::shared_types::{Add, State};
use reiny::{ModuleContext, RuntimeOptions};

#[tokio::main]
async fn main() -> reiny::Result<()> {
    let cloudy = ModuleContext::open(RuntimeOptions::from_args("probe")).await?;
    let endpoint = "owned-child-example/host";
    let mut states = cloudy.input::<State>("state")?;
    let caller = cloudy.rpc_client::<Add>("adder")?;
    cloudy.ready()?;
    let state = tokio::time::timeout(Duration::from_secs(10), states.recv_envelope())
        .await?
        .ok_or_else(|| std::io::Error::other("state subscription ended"))?;
    let response = tokio::time::timeout(
        Duration::from_secs(10),
        caller.call(Add {
            a: state.value.value,
            b: 1,
        }),
    )
    .await??;
    if state.source != endpoint
        || state.source_port != "state"
        || state.value.value != 42
        || response.sum != 43
    {
        return Err(std::io::Error::other("delegated endpoint result differs").into());
    }
    println!("OWNED_CHILD_QA_OK state=42 reply=43 source={endpoint}");
    Ok(())
}
