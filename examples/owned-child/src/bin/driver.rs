use owned_child_example::internals::{Add, State, Sum};
use reiny::{Cloudy, RuntimeOptions};

#[tokio::main]
async fn main() -> reiny::Result<()> {
    let cloudy = Cloudy::open(RuntimeOptions::from_args("driver")).await?;
    let states = cloudy.output::<State>("state")?;
    let mut requests = cloudy.provides::<Add>("adder")?;
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
