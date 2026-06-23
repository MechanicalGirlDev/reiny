# ping-pong-config

reiny の使い方を示すサンプル。`pong` の振る舞い(返答文・遅延)をコードに
埋め込まず、**設定(config)で外から与える** パターンです。

- **ping**: いつもどおり `Ping` を打って `Pong` を待つ。
- **pong**: `reply`(返答文)と `delay_ms`(返すまでの遅延)を **設定から読む**。

## 見どころ: 型付き設定 `[config]`

`Reiny.toml` の `[config]` に **設定スキーマと既定値** を宣言します。

```toml
# pong/Reiny.toml
[config]
reply = "pong"   # 返答メッセージ
delay_ms = 0     # 受信から返球までの遅延(ms)
```

`reiny-build` がこれを読んで型付きの設定構造体を生成し(`reiny::config::Config {
reply: String, delay_ms: u64 }`)、コードからは `cloudy.config()` で読みます:

```rust
let cfg = cloudy.config();
tokio::time::sleep(Duration::from_millis(cfg.delay_ms)).await;
pongs.send(Pong { message: cfg.reply.clone(), .. }).await?;
```

既定値は `[config]`、**上書き** は起動時の設定ファイルです。launch config の
`config = "pong.config.toml"` で渡すと、既定値の上に重なります。

```toml
# pong.config.toml(上書き)
reply = "PONG!"
delay_ms = 250
```

## レイアウト

```
ping-pong-config/
├── Cargo.toml          # cargo ワークスペース(members = ping, pong)
├── ping-pong.toml      # launch config(pong に config を渡す)
├── pong.config.toml    # ★ 設定の上書き値
├── ping/               # ふつうの ping
└── pong/
    ├── Reiny.toml      # ★ [config] に既定値スキーマ
    ├── proto/pong.proto
    └── src/main.rs     # cloudy.config() で読む
```

## 動かす

```sh
# 設定なしだと Reiny.toml [config] の既定値(reply="pong", delay=0)で動く
cargo run -p pong &
cargo run -p ping

# ランチャでまとめて起動(reply="PONG!", delay=250ms)
#   ※ あらかじめ cargo build してから、bin の置き場を --bin-dir で指す
reiny --config ping-pong.toml --bin-dir target/debug
```
