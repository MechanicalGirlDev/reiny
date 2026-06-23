# ping-pong-fanout

reiny の **到達目標** を示すサンプル。1 つの `ping` が打った 1 球を、複数の `pong`
インスタンスが受けてそれぞれ返す **broadcast / fan-out** です。

```
                   ┌──▶ pong-1 ──┐
ping ──Ping──▶ (reiny/ping)  pong-2  ──Pong──▶ ping
                   └──▶ pong-3 ──┘
```

- **ping**: `Ping` を 1 秒ごとに broadcast し、返ってきた `Pong` を「誰が返したか」付きでログ。
- **pong**: 同じバイナリを複数起動。各インスタンスが `Ping` を受けて、自分の id を載せた `Pong` を返す。

> ⚠️ これは *設計サンプル* です。現状の reiny クレートだけでは **まだビルドは通りません**
> (umbrella crate `reiny` と `reiny-build` は今後実装)。

## 見どころ: 複数インスタンス × 型での購読

同じ `pong` バイナリを 3 つ起動すると、reiny が **連番 id**(`pong-1` / `pong-2` /
`pong-3`)を自動採番します。ping 側はトピック名やインスタンス数を一切意識せず、
**型 `Pong` を購読するだけ**:

```rust
let mut pongs = cloudy.subscribe::<Pong>()?; // 全 pong インスタンスの Pong がまとまって届く
```

reiny は型(=プロジェクト)単位の購読をワイルドカードに展開するので、インスタンスが
増減しても購読側は無変更です。各 `Pong` には返した `from`(例 `"pong-2"`)が載るので、
誰が答えたか分かります。

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

## 動かす(将来像)

```sh
# ランチャでまとめて(pong×3 + ping)
reiny ping-pong.toml

# または手で増やす: pong を好きなだけ起動してから ping
cargo run -p pong &   # pong-1
cargo run -p pong &   # pong-2
cargo run -p pong &   # pong-3
cargo run -p ping     # 3 つの返球がまとまって届く
```
