# ping-pong-schema

reiny の使い方を示すサンプル。通信の中身は [`../ping-pong-workspace`](../ping-pong-workspace)
と同じ ping ↔ pong の往復ですが、**共有スキーマクレート(`[schema]`)で型の生成を 1 度にまとめる**
点が違います。

> **配置パターン: workspace 共有 + `[schema]`。** workspace 版は ping と pong が **両方**
> `[internals]` の proto を prost コンパイルしていました(同じ型を 2 度生成)。こちらは
> `[schema] crate = "..."` を足し、`[internals]` を **そのクレートだけ** が 1 度だけ
> コンパイル + `impl Topic` し、grain はそれを Cargo 依存として共有します。grain ごとの
> 重複 proto コンパイルが無くなります。

## workspace 版との違い

| | workspace(共有カタログ) | workspace + schema |
| --- | --- | --- |
| `[internals]` の prost コンパイル | **各 grain** が自前で(ping と pong で 2 回) | **スキーマクレートが 1 度だけ** |
| grain の型の入手 | 各 grain が `crate::internals`(自前生成) | スキーマクレートを依存 → `crate::internals` に再エクスポート |
| grain の `prost` 依存 | 要る(自前で prost 型を生成) | **不要**(型を使うだけ。生成はスキーマ側) |
| 向く場面 | 少数 grain / 手軽さ重視 | grain が増えてビルド時間に効くとき |

## 仕組み

`reiny-build`(各 `build.rs` から呼ぶ)は上方の `Reiny.toml` を見つけ、**自分のパッケージ名**で
モードを決めます。

- パッケージ名が `[schema].crate`(= `pingpong-schema`)→ **スキーマ本体モード**。
  `[internals]` の proto をコンパイルし、型 + `impl Topic` + `internals` を生成。
  `schema/src/lib.rs` の `reiny::schema!()` がそれをライブラリとして取り込む。
- それ以外(`[projects.ping]` / `[projects.pong]`)→ **消費モード**。proto は再コンパイルせず、
  `pingpong-schema` の `internals` を `crate::internals` として見せる薄い再エクスポートだけ。

型 = トピックなので、消費側の書き味は workspace 版と同じです。

```rust
use crate::internals::{Ping, Pong}; // 実体は pingpong-schema::internals::*

let pings = cloudy.publish::<Ping>()?;
let mut pongs = cloudy.subscribe::<Pong>()?;
```

```toml
[internals]
Ping = { proto = "proto/ping.proto", message = "ping.Ping" }
Pong = { proto = "proto/pong.proto", message = "pong.Pong" }

# このクレートだけが [internals] を 1 度コンパイルする(名前は schema/ の package 名と一致)。
[schema]
crate = "pingpong-schema"

[projects.ping]
publications = ["Ping"]
dependencies = ["Pong"]
[projects.pong]
publications = ["Pong"]
dependencies = ["Ping"]
```

## レイアウト

```
ping-pong-schema/
├── Cargo.toml          # cargo ワークスペース(members = schema, ping, pong)
├── Reiny.toml          # ★ [internals] + [schema] + [projects.*]
├── ping-pong.toml      # launch config(schema は lib なので grain には出ない)
├── proto/              # 共有 proto
│   ├── ping.proto
│   └── pong.proto
├── schema/             # ★ 共有スキーマクレート([internals] を 1 度だけ生成)
│   ├── Cargo.toml      #   lib。deps: reiny + prost、build-dep: reiny-build
│   ├── build.rs        #   reiny_build::compile()(スキーマ本体モード)
│   └── src/lib.rs      #   reiny::schema!();
├── ping/
│   ├── Cargo.toml      #   deps: reiny + pingpong-schema(prost は不要)
│   ├── build.rs        #   reiny_build::compile()(消費モード)
│   └── src/main.rs     #   use crate::internals::{Ping, Pong}
└── pong/
    └── (ping と対称)
```

★ が workspace 版との差分(`schema/` クレートと `[schema]` 節)。

## 確認する

```sh
# モードと型 → トピックを表示(proto はコンパイルしない)
reiny check .
# → mode: workspace+schema (schema crate)
#   schema crate: pingpong-schema (compiles [internals] once; grains share it)
```

## 動かす

```sh
# 個別に、別々の端末で
cargo run -p pong   # pong を起動(Ping を待ち受け)
cargo run -p ping   # ping を起動(Ping を送信開始)

# または、ランチャでまとめて
reiny --config ping-pong.toml --bin-dir target/debug
```
