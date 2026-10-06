//! calc demonstrates explicit typed ports in a namespaced deployment.

use reiny::prelude::*;

use crate::internals::{Add, Sum};

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let mut adds = cloudy.serve::<Add>()?;
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
