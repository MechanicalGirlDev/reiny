use owned_child_example::shared_types::{Add, State, Sum};
use reiny::{ModuleContext, RuntimeOptions};

#[tokio::main]
async fn main() -> reiny::Result<()> {
    let cloudy = ModuleContext::open(RuntimeOptions::from_args("driver")).await?;
    let states = cloudy.output::<State>("state")?;
    let mut requests = cloudy.rpc_server::<Add>("adder")?;
    states.send(State { value: 42 }).await?;
    cloudy.ready()?;
    while let Some(request) = requests.recv().await {
        match request.value.a.checked_add(request.value.b) {
            Some(sum) => request.reply(Sum { sum }).await?,
            None => request.reply_err("overflow").await?,
        }
    }
    Ok(())
}
