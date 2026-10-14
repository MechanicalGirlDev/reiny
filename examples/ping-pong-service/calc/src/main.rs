//! calc demonstrates explicit typed ports in a namespaced deployment.

use reiny::prelude::*;

use crate::shared_types::{Add, Sum};

#[reiny::main]
async fn main(cloudy: ModuleContext) -> reiny::Result<()> {
    let mut adds = cloudy.rpc_server::<Add>("adder")?;
    cloudy.ready()?;

    while let Some(req) = adds.recv().await {
        let Add { a, b } = req.value;
        tracing::info!(a, b, "← Add");
        match a.checked_add(b) {
            Some(sum) => req.reply(Sum { sum }).await?,
            None => req.reply_err("overflow").await?,
        }
    }

    Ok(())
}
