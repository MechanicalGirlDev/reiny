//! asker demonstrates explicit typed ports in a namespaced deployment.

use std::time::Duration;

use reiny::prelude::*;

use crate::internals::Add;

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    // Select the sibling service within this composition, including when nested.
    let (parent, _) = cloudy.id().rsplit_once('/').ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "asker must run inside a namespaced composition",
        )
    })?;
    let adder = cloudy
        .caller::<Add>()
        .to(format!("{parent}/calc"))
        .timeout(Duration::from_secs(2))
        .build();
    cloudy.ready()?;
    cloudy.ready()?;
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut n = 0;

    loop {
        tokio::select! {
            () = cloudy.shutdown() => break,
            _ = tick.tick() => {
                n += 1;
                match adder.call(Add { a: n, b: n * 10 }).await {
                    Ok(sum) => tracing::info!(a = n, b = n * 10, sum = sum.sum, "← Sum"),
                    Err(e) => tracing::warn!(error = %e, "Add failed"),
                }
            }
        }
    }

    Ok(())
}
