# ping-pong-relay

reiny の **到達目標** を示すサンプル。往復ではなく、データが **段を経て流れる**
パイプラインです。

```
ping ──Ping──▶ relay ──Relayed──▶ pong
(source)     (transform)        (sink)
reiny/ping     reiny/relay      (購読のみ)
```

- **ping**(source): `Ping` を一定間隔で流すだけ(購読しない)。
- **relay**(transform): `Ping` を受けて `Relayed` に変換し(経由 id・hop を付与)、下流へ流す。
- **pong**(sink): `Relayed` を購読して表示するだけ(何も公開しない)。

> ⚠️ これは *設計サンプル* です。現状の reiny クレートだけでは **まだビルドは通りません**
> (umbrella crate `reiny` と `reiny-build` は今後実装)。

## 見どころ: ノードは「購読する型」と「公開する型」を持つ

各ノードの Reiny.toml の `[publications]` / `[dependencies]` が、そのままパイプラインの
配線になります。

| プロジェクト | dependencies(購読) | publications(公開) | 役割 |
| --- | --- | --- | --- |
| ping | —(なし) | `Ping` | source(生産のみ) |
| relay | `Ping` | `Relayed` | transform(変換) |
| pong | `Relayed` | —(なし) | sink(消費のみ) |

- **source**(ping)は `[dependencies]` が空 → 何も購読しない純粋な生産者。
- **sink**(pong)は `[publications]` が空 → トピックを持たない純粋な購読者。
- relay は両方を持ち、`Ping` を受けて `Relayed` に変換する。これを並べれば多段になる。

relay の中身はシンプルに「受けて・変換して・流す」だけ:

```rust
let out = cloudy.publish::<Relayed>()?;
let mut incoming = cloudy.subscribe::<Ping>()?;
while let Some(ping) = incoming.recv().await {
    out.send(Relayed { seq: ping.seq, via: cloudy.id().to_string(), hops: 1, origin_unix: ping.sent_unix }).await?;
}
```

## レイアウト

```
ping-pong-relay/
├── Cargo.toml          # cargo ワークスペース(members = ping, relay, pong)
├── ping-pong.toml      # launch config(ping → relay → pong の起動順)
├── ping/               # source: publications = Ping
├── relay/              # transform: dependencies = Ping / publications = Relayed
│   ├── proto/relayed.proto
│   └── src/main.rs
└── pong/               # sink: dependencies = Relayed / publications = 空(proto なし)
    └── src/main.rs
```

## 動かす(将来像)

```sh
# 起動順込みでまとめて
reiny ping-pong.toml

# または別々の端末で(下流から上げると取りこぼしが少ない)
cargo run -p pong &
cargo run -p relay &
cargo run -p ping
```
