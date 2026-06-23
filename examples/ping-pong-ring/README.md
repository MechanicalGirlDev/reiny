# ping-pong-ring

reiny の使い方を示す最小サンプル。2 つのプロジェクトが型を介して往復する、
いちばんシンプルな通信です。

> **配置パターン: per-project(リング型)。** proto と `Reiny.toml` を **各プロジェクト**
> (`ping/`・`pong/`)に置きます。型は各プロジェクトが所有し、相手の公開型に依存し合う
> ことで往復のリングを作ります。proto と `Reiny.toml` を **ワークスペースに 1 つ** 置く
> 共有カタログ版は [`../ping-pong-workspace`](../ping-pong-workspace) を参照。

```
ping  ──  Ping(reiny/ping-1/Ping) ──▶  pong
ping  ◀── Pong(reiny/pong-1/Pong) ──   pong
```

- **ping**: 最初の一球を打ち、`Pong` が返るたびに次の `Ping` を打ち返す。
- **pong**: `Ping` を受けるたびに、同じ `seq` の `Pong` を返す。

## ねらい: Reiny.toml

各プロジェクトは `Cargo.toml`(Rust のビルド)に加えて、**`Reiny.toml`** を持ちます。
これは reiny レベルの宣言ファイルで、「何という名前で動き、どんな型を公開し、どんな型に
依存するか」を表します。

```toml
[project]
name = "ping"        # プロセス/インスタンス名(トピックではない)
version = "0.1.0"    # プロジェクト/公開型スキーマのバージョン
description = "..."
authors = ["nop <noplab90@gmail.com>"]
license = "MIT"

[publications]       # 公開する型。publish は自分の reiny/<id>/Ping(購読側は reiny/*/Ping)。複数可
Ping = { proto = "proto/ping.proto", message = "ping.Ping" }

[dependencies]       # 依存する他プロジェクト。その公開型を購読できる
pong = { version = "0.1", path = "../pong" }
```

ポイント:

| 項目 | 意味 |
| --- | --- |
| `[project].name` | プロセス/インスタンス名(ランチャのキー・liveliness id)。**トピックではない**。 |
| `[project].version` | プロジェクトと公開型スキーマのバージョン。購読側との互換判定に使う。 |
| `[publications]` | 公開する型。publish 先は `reiny/<id>/<型>`、購読側は `reiny/*/<型>`。複数列挙してよい。 |
| `[dependencies]` | 依存先プロジェクト。その `[publications]` の型を購読できるようになる。 |

ping と pong は互いの公開型を使うので **相互依存** します(ping は pong の `Pong`、
pong は ping の `Ping`)。これは*型参照*レベルの循環で、reiny が型を解決する前提
なので問題ありません。

## 型 = トピック

reiny は **型でアドレスする**。型を publish すると自分の `reiny/<id>/<型>` トピックへ流れ、
型を subscribe すると `reiny/*/<型>`(全 publisher の同じ型のトピック)を束ねて受け取ります。
**型がトピックの鍵**で、`<id>` はインスタンス名前空間(例 `ping-1`)。publish/subscribe は型で行い、
コードに **トピック名(文字列)は出てきません**。1 プロセスは複数の型を pub/sub してよく、
`name` はプロセス/インスタンス名であってトピックそのものではありません。

```rust
let pings = cloudy.publish::<Ping>()?;    // 自分の reiny/ping-1/Ping へ送る
let mut pongs = cloudy.subscribe::<Pong>()?; // reiny/*/Pong(全 pong)を購読
```

`reiny-build`(`build.rs` から呼ぶ)が `Reiny.toml` を読み、proto をコンパイルして
次を生成します:

- `reiny::publications::*` — 自分が公開する型(例 `Ping`)
- `reiny::dependencies::<project>::*` — 依存先の公開型(例 `dependencies::pong::Pong`)
- 「型 → トピック」マッピング(自分の publish `Ping → reiny/<id>/Ping`、購読 `Pong → reiny/*/Pong`)

## ランチャとの関係(2 つの TOML)

reiny には役割の違う 2 種類の TOML があります。

| ファイル | 層 | 相当 | 役割 |
| --- | --- | --- | --- |
| `Reiny.toml`(各プロジェクト) | プロジェクトの身元 | Cargo.toml / package.xml | 名前・version・公開型・依存型を宣言 |
| `ping-pong.toml`(ルート) | デプロイ | roslaunch | どの grain を一緒に起動するか(`[grain]`) |

ランチャ(`reiny-launch` の `reiny` バイナリ)は launch config の `[grain]` を読み、
各 grain を子プロセスとして起動します。ping-pong の launch config:

```toml
[grain]
pong = { bin = "pong", on_exit = "respawn" }
ping = { bin = "ping", depends_on = ["pong"] }
```

## レイアウト

```
ping-pong/
├── Cargo.toml          # cargo ワークスペース(members = ping, pong)
├── ping-pong.toml      # launch config(reiny ランチャ用 / [grain])
├── ping/
│   ├── Cargo.toml      # Rust パッケージ定義
│   ├── Reiny.toml      # reiny プロジェクト宣言(name/version/publications/dependencies)
│   ├── build.rs        # reiny-build を呼んで型を生成
│   ├── proto/
│   │   └── ping.proto  # 公開型 Ping のスキーマ
│   └── src/
│       └── main.rs     # #[reiny::main] で ping プロセス(cloudy)を起動
└── pong/
    └── (ping と対称)
```

## 動かす

```sh
# 個別に、別々の端末で
cargo run -p pong   # pong を起動(Ping を待ち受け)
cargo run -p ping   # ping を起動(Ping を送信開始)

# または、ランチャでまとめて(launch config の [grain] を起動)
#   ※ あらかじめ cargo build してから、bin の置き場を --bin-dir で指す
reiny --config ping-pong.toml --bin-dir target/debug
```
