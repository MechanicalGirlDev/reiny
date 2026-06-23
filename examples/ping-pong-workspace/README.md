# ping-pong-workspace

reiny の **到達目標** を示すサンプル。通信の中身は [`../ping-pong-ring`](../ping-pong-ring)
と同じ ping ↔ pong の往復ですが、**proto と `Reiny.toml` の置き方** が違います。

> **配置パターン: ワークスペース共有。** proto と `Reiny.toml` を **ワークスペース直下に
> 1 つだけ** 置き、members(`ping/`・`pong/`)で共有します。型は共有カタログ `[internals]`
> に集約し、各プロジェクトは「カタログのどの型を公開/購読するか」を宣言するだけです。

> ⚠️ これは「こう書けるようにしたい」という *設計サンプル* です。現状の reiny クレート
> だけでは **まだビルドは通りません**(umbrella crate `reiny` と `reiny-build` は今後実装)。

## 2 つの配置パターンの違い

| | ring(per-project) | workspace(共有) |
| --- | --- | --- |
| `proto/` | 各プロジェクトに 1 つずつ | ワークスペースに 1 つ |
| `Reiny.toml` | 各プロジェクトに 1 つずつ | ワークスペースに 1 つ |
| 型の所有 | プロジェクトが所有 | 共有カタログ `[internals]` |
| `dependencies` が指すもの | **他プロジェクト**(その公開型を引く) | **カタログの型** |
| 向く場面 | 独立に配布/バージョン管理する部品 | 密結合な一群でメッセージ定義を共有 |

## 共有 Reiny.toml

ワークスペース直下の 1 ファイルに、型カタログと各プロジェクトの宣言をまとめます。

```toml
[workspace]
version = "0.1.0"
authors = ["nop <noplab90@gmail.com>"]
license = "MIT"

# 共有メッセージカタログ(proto/ で定義)。全プロジェクトが参照できる。
[internals]
Ping = { proto = "proto/ping.proto", message = "ping.Ping" }
Pong = { proto = "proto/pong.proto", message = "pong.Pong" }

# 各プロジェクトの name はプロセス名。トピックは型から決まる(型 = トピック、1:1)。
[projects.ping]
publications = ["Ping"]   # 型 Ping を公開(reiny/ping-1/Ping)
dependencies = ["Pong"]   # 型 Pong を購読(reiny/*/Pong)

[projects.pong]
publications = ["Pong"]
dependencies = ["Ping"]
```

アプリ側のコードは共有カタログの型を使うだけ。トピック名(文字列)は出てきません。

```rust
use reiny::internals::{Ping, Pong};

let pings = cloudy.publish::<Ping>()?;       // [projects.ping].publications → reiny/ping-1/Ping
let mut pongs = cloudy.subscribe::<Pong>()?; // [projects.ping].dependencies → reiny/*/Pong
```

`reiny-build`(各 member の `build.rs` から呼ぶ)が上方の `Reiny.toml` を見つけ、
`[internals]` の proto をコンパイルして共有カタログ `reiny::internals::*` を生成し、現在の
バイナリ名(`ping`/`pong`)に対応する `[projects.<name>]` で publish/subscribe の可否を
検証します。

## レイアウト

```
ping-pong-workspace/
├── Cargo.toml          # cargo ワークスペース(members = ping, pong)
├── Reiny.toml          # ★ 共有マニフェスト([internals] + [projects.*])
├── ping-pong.toml      # launch config(reiny ランチャ用 / [grain])
├── proto/              # ★ 共有 proto
│   ├── ping.proto
│   └── pong.proto
├── ping/
│   ├── Cargo.toml
│   ├── build.rs        # reiny-build が上方の Reiny.toml を読む
│   └── src/main.rs     # #[reiny::main] で ping プロセス(cloudy)を起動
└── pong/
    └── (ping と対称)
```

★ が ring 版との差分（proto と Reiny.toml がワークスペースに集約されている）。

## 動かす(将来像)

```sh
# ランチャでまとめて(launch config の [grain] を起動)
reiny ping-pong.toml

# または個別に、別々の端末で
cargo run -p pong   # pong を起動(Ping を待ち受け)
cargo run -p ping   # ping を起動(Ping を送信開始)
```
