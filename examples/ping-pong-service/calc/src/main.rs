//! calc — `Add` を serve し、`Sum` を返す。
//!
//! service は **request 型がサービスの住所**。`serve::<Add>()` は
//! `reiny/<domain>/<id>/Add` に queryable を置き、`recv()` で request を受け、
//! `reply()` / `reply_err()` のどちらかで消費する(返さずに drop すると呼び出し側は
//! `CallError::NoReply` を受ける —— ハングはしない)。

use reiny::prelude::*;

use crate::internals::{Add, Sum};

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let mut adds = cloudy.serve::<Add>()?;

    while let Some(req) = adds.recv().await {
        let Add { a, b } = req.value;
        tracing::info!(a, b, "← Add");
        match a.checked_add(b) {
            Some(sum) => req.reply(Sum { sum }).await?,
            // 断る側の理由は文字列で届く(呼び出し側は CallError::Remote)。
            None => req.reply_err("overflow").await?,
        }
    }

    Ok(())
}
