# ping-pong-service

reiny 0.4.0 の **型付き request/response(service)** を示すサンプル。配置は
[`../ping-pong-workspace`](../ping-pong-workspace) と同じワークスペース共有で、
`Reiny.toml` に `[services]` を 1 節足しただけ。

> **service は request 型がサービスの住所。** ROS 2 の service が名前(`/add`)で
> アドレスするところを、reiny は型でアドレスする —— `Add` を serve する launch は
> `reiny/<domain>/<id>/Add` に queryable を置き、`call::<Add>(…)` は `reiny/<domain>/*/Add` へ
> 撃って `Sum` を受け取る。トピック文字列は相変わらず出てこない。

## Reiny.toml

```toml
[internals]
Add = { proto = "proto/calc.proto", message = "calc.Add" }
Sum = { proto = "proto/calc.proto", message = "calc.Sum" }

# request 型 → response 型。impl reiny::Service for Add { type Response = Sum; } を生成する。
[services]
Adder = { request = "Add", response = "Sum" }
```

`reiny check` が `services:` 表を出す。

## コード

```rust
// calc(server)
let mut adds = cloudy.serve::<Add>()?;
while let Some(req) = adds.recv().await {
    let Add { a, b } = req.value;
    req.reply(Sum { sum: a + b }).await?;      // または req.reply_err("…")
}

// asker(client)
let sum = cloudy.call::<Add>(Add { a: 1, b: 2 }).await?;   // Sum
// 宛先・タイムアウト固定: cloudy.caller::<Add>().to("calc").timeout(d).build()
```

- 応答が来ない(server 不在 / 返さずに drop)は `CallError::NoReply`、期限切れは `Timeout`、
  `reply_err` は `Remote(String)`。呼び出し側はこれで「居ない」と「断られた」を出し分ける。
- 誰が serve しているかは `cloudy.servers::<Add>()` / `watch_servers::<Add>()`
  (publisher の presence と同じ仕組み、キーは `…/Add/@service`)。

## 動かす

```sh
cargo build
reiny ping-pong-service.toml --bin-dir target/debug
# または別々の端末で
cargo run -p calc
cargo run -p asker
```
