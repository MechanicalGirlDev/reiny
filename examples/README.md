# reiny examples

reiny の使い方を示すサンプル集。どれも題材は最小の ping-pong 通信に統一し、
1 つの軸だけを変えて見せています。

> 土台は umbrella crate `reiny`(SDK + `#[reiny::main]` + `Cloudy` ハンドル)と
> `reiny-build`(Reiny.toml パーサ + コード生成)、ランチャ `reiny-launch`。
> `ping-pong-cli` を除く各サンプルは `cargo build` でそのままビルドできます
> (`ping-pong-cli` は CLI 雛形生成の到達像を示す手順書で、その CLI は未実装)。

## 共通の約束

- **型 = トピック**: 型でアドレスする。型 `T` を publish すると自分の `reiny/<id>/T` へ流れ、
  型 `T` を subscribe すると `reiny/*/T`(全 publisher の同じ型)を束ねて受ける。型がトピックの鍵で、
  `<id>` はインスタンス名前空間。1 プロセスは複数の型を pub/sub してよい。`[project].name` は
  プロセス/インスタンス名であってトピックそのものではない。
- 各プロジェクトは `Cargo.toml`(Rust ビルド)に加えて **`Reiny.toml`**(名前・version・
  公開型・依存型)を持つ。
- `#[reiny::main] async fn main(cloudy: Cloudy)` で起動。`cloudy.publish::<T>()` /
  `cloudy.subscribe::<T>()` は **型を渡すだけ**(reiny が型 → トピックを解決、文字列は出てこない)。
- 各サンプルに、ランチャ `reiny`(reiny-launch)用の `ping-pong.toml`(launch config)を同梱。

## 一覧

### 配置パターン(proto/Reiny.toml をどこに置くか)

| サンプル | 内容 |
| --- | --- |
| [ping-pong-ring](ping-pong-ring) | proto と Reiny.toml を **各プロジェクト** に配置。型は各々が所有し相互依存(リング)。 |
| [ping-pong-workspace](ping-pong-workspace) | proto と Reiny.toml を **ワークスペースに 1 つ** 集約。型は共有カタログ `[internals]`。 |

### 通信トポロジ / 機能

| サンプル | トポロジ | 見せる機能 |
| --- | --- | --- |
| [ping-pong-fanout](ping-pong-fanout) | 1 ping → N pong | 全インスタンスが同じ型 = 同じトピックへ集約。型で購読するだけで broadcast |
| [ping-pong-config](ping-pong-config) | ping ↔ pong | `[config]` による型付き設定。返答文・遅延を外出し |
| [ping-pong-relay](ping-pong-relay) | ping → relay → pong | 多段パイプライン。source / transform / sink の配線 |

### ワークフロー(CLI で雛形から作る)

| サンプル | 内容 |
| --- | --- |
| [ping-pong-cli](ping-pong-cli) | 静的なプロジェクト木ではなく **シェルスクリプトで `reiny` CLI を順に叩く** 手順を見せる。`reiny new` / `init` / `add` / `build` / `run` の期待挙動 |

## どれから読むか

- まず [ping-pong-ring](ping-pong-ring) で `Reiny.toml` と publish/subscribe の基本を掴む。
- 型の置き方の選択肢として [ping-pong-workspace](ping-pong-workspace) を見る。
- あとは見たい機能(fanout / config / relay)へ。
- それらの `Reiny.toml` / proto / src を手書きせず **CLI で生成する** 道筋は
  [ping-pong-cli](ping-pong-cli) を見る。
