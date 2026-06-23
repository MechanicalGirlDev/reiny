# reiny examples

reiny の **到達目標** を示すサンプル集。どれも題材は最小の ping-pong 通信に統一し、
1 つの軸だけを変えて見せています。

> ⚠️ これらは「こう書けるようにしたい」という *設計サンプル* です。現状の reiny クレート
> (`reiny-proto` / `reiny-transport` / `reiny-grain` / `reiny-launch`)だけでは
> **まだビルドは通りません**。前提となる umbrella crate `reiny`(SDK + `#[reiny::main]` +
> `Cloudy` ハンドル)と `reiny-build`(Reiny.toml パーサ + コード生成)は今後の実装対象です。

## 共通の約束

- 各プロジェクトは `Cargo.toml`(Rust ビルド)に加えて **`Reiny.toml`**(名前・version・
  公開型・依存型)を持つ。プロジェクト名 = 実行時のトピック名(`reiny/<name>`)。
- `#[reiny::main] async fn main(cloudy: Cloudy)` で起動。`cloudy.publish::<T>()` /
  `cloudy.subscribe::<T>()` は **型を渡すだけ**(トピック名は出てこない)。
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
| [ping-pong-fanout](ping-pong-fanout) | 1 ping → N pong | 複数インスタンスの自動連番 + 型での購読(ワイルドカード broadcast) |
| [ping-pong-config](ping-pong-config) | ping ↔ pong | `[config]` による型付き設定。返答文・遅延を外出し |
| [ping-pong-relay](ping-pong-relay) | ping → relay → pong | 多段パイプライン。source / transform / sink の配線 |

## どれから読むか

- まず [ping-pong-ring](ping-pong-ring) で `Reiny.toml` と publish/subscribe の基本を掴む。
- 型の置き方の選択肢として [ping-pong-workspace](ping-pong-workspace) を見る。
- あとは見たい機能(fanout / config / relay)へ。
