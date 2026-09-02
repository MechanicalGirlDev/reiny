# ping-pong-schema-split

スキーマを **複数クレート**へ割った ping-pong。`../ping-pong-schema` が `[schema]` 1 つで
`[internals]` 全部を抱えるのに対し、こちらは `[schema.<name>]` を並べて proto を分ける。

```
proto/geometry/geometry.proto   →  pingpong-geometry   (Point)
proto/msg/pingpong.proto        →  pingpong-msg        (Ping / Pong、Point を参照)
ping/ pong/                     →  launch(両方を Cargo 依存にする)
```

## 何が難しくて、reiny が何をしているか

`msg/pingpong.proto` は `geometry.proto` を import する。protoc は import 先も descriptor に
含めるので、素直に prost へ流すと **`Point` が pingpong-msg 側にも生成される** —— 名前が同じ
だけの別の型が 2 つでき、`Ping.at` に geometry の `Point` を入れられなくなる。

reiny-build はこれを cargo の `links` メタで解く。

1. `pingpong-geometry` の build script が、自分の proto の include ディレクトリと、
   自分が定義した FQN 一覧(`pingpong.Point`)を `cargo:proto_include=` / `cargo:proto_types=`
   で出す。
2. cargo がそれを **直接依存** の build script へ
   `DEP_PINGPONG_GEOMETRY_PROTO_INCLUDE` / `_PROTO_TYPES` として渡す。
3. `pingpong-msg` の build script(中身は `reiny_build::compile()` の 1 行)がそれを読み、
   prost の `extern_path` に変換する。prost は extern された型を **生成せず**、参照だけを
   `::pingpong_geometry::__pb::pingpong::Point` に差し替える。

`ping/src/main.rs` が geometry の `Point` を `msg` の `Ping.at` に直接入れているのは、
これが効いていることのコンパイル時チェックを兼ねている。

## 守る約束は 2 つだけ

- **各スキーマクレートの Cargo.toml に `links = "<パッケージ名>"`**。無いと cargo がメタを
  誰にも渡さない。reiny は Cargo.toml を書き換えられないので、ここだけ手で書く
  (忘れるとビルド時に名指しで指摘される)。
- **`depends` は推移的に閉じ、同じ辺を Cargo の依存にも持つ**。cargo は直接依存にしか
  `DEP_*` を渡さないため。`a ← b ← c` なら `c` の `depends` に `a` も要る。

## 動かす

```sh
cargo build
reiny check          # 区画・所有・型 → トピック
reiny ping-pong.yaml
```
