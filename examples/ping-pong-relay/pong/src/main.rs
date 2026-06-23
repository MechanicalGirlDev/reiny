//! pong — パイプラインの sink。`Relayed` を購読してログするだけ(何も公開しない)。
//!
//! 見どころ: publications が空のプロジェクト = 純粋な購読者。公開する型が無いので
//! 自分のトピックは持たず、上流 relay の Relayed を受け取って end-to-end の経過時間を出す。
//!
//! 注意: これは reiny の到達目標を示す設計サンプル。umbrella crate `reiny` と
//! `reiny-build`(Reiny.toml パーサ + codegen)は未実装なので、まだビルドは通らない。

use reiny::prelude::*;

use reiny::dependencies::relay::Relayed;

#[reiny::main]
async fn main(cloudy: Cloudy) -> reiny::Result<()> {
    let mut incoming = cloudy.subscribe::<Relayed>()?;

    while let Some(m) = incoming.recv().await {
        let elapsed = cloudy.now_unix() - m.origin_unix;
        tracing::info!(
            seq = m.seq,
            via = %m.via,
            hops = m.hops,
            elapsed_s = elapsed,
            "● sink received"
        );
    }

    Ok(())
}
