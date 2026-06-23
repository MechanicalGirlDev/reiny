# ping-pong-fanout

reiny の使い方を示すサンプル。1 つの `ping` が打った 1 球を、複数の `pong`
インスタンスが受けてそれぞれ返す **broadcast / fan-out** です。

```
                   ┌──▶ pong-1 ──┐
ping ──Ping──▶ (reiny/ping-1/Ping)  pong-2  ──Pong──▶ ping
                   └──▶ pong-3 ──┘
```

- **ping**: `Ping` を 1 秒ごとに broadcast し、返ってきた `Pong` を「誰が返したか」付きでログ。
- **pong**: 同じバイナリを複数起動。各インスタンスが `Ping` を受けて、自分の id を載せた `Pong` を返す。

## 見どころ: 型で購読 → fan-out

各 pong インスタンスは自分の `reiny/<id>/Pong`(例 `reiny/pong-1/Pong`, `reiny/pong-2/Pong`)
へ publish します。ping 側はインスタンス数を意識せず、**型 `Pong` を subscribe するだけ**。
reiny はこれを `reiny/*/Pong` に展開するので、全インスタンスの `Pong` がまとまって届きます。

```rust
let mut pongs = cloudy.subscribe::<Pong>()?; // reiny/*/Pong — 全 pong インスタンス分が届く
```

同じ `pong` バイナリを 3 つ起動すると reiny が **連番 id**(`pong-1` / `pong-2` / `pong-3`)を
自動採番し、それが publish 先トピックの `<id>` になります。各 `Pong` の `from` にも載るので
誰が答えたか分かり、インスタンスが増減しても **型で購読する** ping 側は無変更です。

## レイアウト

```
ping-pong-fanout/
├── Cargo.toml          # cargo ワークスペース(members = ping, pong)
├── ping-pong.toml      # launch config(pong を 3 起動 + ping)
├── ping/               # Ping を broadcast、Pong を集約
│   ├── Reiny.toml      # publications = Ping / dependencies = pong
│   ├── proto/ping.proto
│   └── src/main.rs
└── pong/               # 複数起動される返球役(id を載せて返す)
    ├── Reiny.toml      # publications = Pong / dependencies = ping
    ├── proto/pong.proto
    └── src/main.rs
```

## 動かす

```sh
# 手で増やす: pong を好きなだけ起動してから ping
cargo run -p pong &   # pong-1
cargo run -p pong &   # pong-2
cargo run -p pong &   # pong-3
cargo run -p ping     # 3 つの返球がまとまって届く

# または、ランチャでまとめて(pong×3 + ping)
#   ※ あらかじめ cargo build してから、bin の置き場を --bin-dir で指す
reiny --config ping-pong.toml --bin-dir target/debug
```
