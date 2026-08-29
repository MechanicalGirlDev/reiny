# reiny 運用の設計 —— `depends_on` の待ち / subscriber presence / バッファの計数 / `topic pub`

対象: `reiny` / `reiny-launch` / `reiny-cli` / `reiny-link`（`reiny-core` / `reiny-build` /
`reiny-macros` / `reiny-iceoryx2` / `reiny-ros2` は触らない）。
起点: 0.5.0 のエンジン抽象まで（`docs/design/0.5.0.md` §11）。**0.5.0 に同梱して出す。**
改訂: 2026-08-29 —— 4 項目に確定（§0）。

> **この文書の位置づけ** —— 0.3.0 / 0.4.0 / 0.5.0 と同じく、実装の手順書ではなく判断の記録。
> `docs/design/0.5.0.md` が「reiny の芯から zenoh の形を抜く」を扱うのに対し、こちらは
> **走らせ続けたときに壊れるところを塞ぐ**ことだけを扱う。同じ 0.5.0 で出るが判定基準
> （§0）が別なので、文書を分けた。新しい概念は 1 つも足さない —— 4 項目とも「reiny が
> 既に持っている道具を、使っていない場所で使う」だけになっている。

---

## 0. 方針

判定基準は積み上げる。0.3.0 の「type = topic が原因で書けないか」、0.4.0 の「手書きの代替を
3 か所以上で書いているか」、0.5.0 の「zenoh が走らない場所に launch を置きたいか」。
**この文書はもう 1 つ足す**:

> **reiny が既に持っている道具で塞げる穴か。** 塞げるのに塞いでいないなら、それは機能の
> 不足ではなく配線の不足で、足すべきは新しい概念ではない。

| #   | 要求                          | 判定             | 判定理由                                                                                                                                                             |
| --- | ----------------------------- | ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | `depends_on` を本当に待たせる | **足す**（§1）   | `runner.rs` は依存順に `spawn()` を呼ぶだけで待ちが**一切ない**。`depends_on` は事実上「数マイクロ秒の順序」で、API が嘘をついている。待つ材料（`@launch`）は 0.4 からある |
| 2   | respawn の backoff            | **足す**（§1.4） | 即死する launch が全速でスピンし、ログを埋める。上限 2 つと deadline 1 つで足りる                                                                                        |
| 3   | subscriber presence           | **足す**（§2）   | 0.4 で意識的に外した。だが「publish しているのに誰も反応しない。購読者は居るのか」はブリングアップの第一問で、いま**バスに聞く手段が無い**。`@service` と同じ形で書ける      |
| 4   | 詰まり・取りこぼしの可視化    | **足す**（§3）   | Fifo(256) が満杯なら engine の受信スレッドが止まり、その launch の**全購読が詰まる**。`AGENTS.md` にも `local.rs` にも書いてあるのに計数が 1 つも無い（黙って止まる）        |
| 5   | `reiny topic pub`             | **足す**（§4）   | `service call` があって `topic pub` が無いのは非対称。`@schema` + `prost-reflect` は `codec.rs` に既にある                                                                |

**破壊的変更**は `reiny_launch::run_launch_dirs` の引数 1 つだけ（§6）。**wire は互換** ——
足すのは verbatim チャンク `@sub` の liveliness トークンで、`*` にも `**` にもマッチしないので
0.4 の購読者・`bag record` の `*/*` 捕捉・`Key::all` のどれからも見えない（`@service` を足した
0.4.0 と同じ性質）。

---

## 1. `depends_on` を待たせる

### 1.1 いま起きていること

```rust
// runner.rs（0.4.0 まで）
for &i in &order {                       // topo_order で依存順に並べただけ
    let child = spawn_one(bin_dirs, spec, default_log_level)?;
    children.insert(spec.name.clone(), child);
}
```

`spawn()` は即座に返るので、`depends_on` が保証しているのは「`fork` の順序」でしかない。
依存先がデバイスを開いている 2 秒の間に、依存元は起動を終えて最初の publish を撃ち終わる。

0.5.0 で `reiny run` はトピックの流れ図まで描くようになったのに、その流れが**繋がるのを待って
いない**。これを最初の項目に置く理由は 1 つで、**塞ぐ道具が 0.4 から手元にある** ——
`reiny/<domain>/<id>/@launch` の liveliness トークン。

### 1.2 何を「起動した」とみなすか

`@launch` トークンが立ったこと。それは「プロセスが立ち上がり、バス上の身元ができた」を意味し、
それ以上は意味しない（`Cloudy::new` はユーザコードの手前でトークンを立てる）。

**それでよい。** ランチャの責務はプロセスの順序であって、購読が張れたかではない。型ごとの
readiness は SDK 側の語彙で表すべきもので、0.5.0 でそれが揃う:

| 聞きたいこと                           | 聞く場所                                                    |
| -------------------------------------- | ----------------------------------------------------------- |
| 依存先のプロセスは立ったか             | ランチャ（`@launch`。これが §1）                             |
| 相手はこの型を publish しているか       | `cloudy.publishers::<T>()` / `watch_publishers`（0.3）       |
| 相手はこの型を購読しているか           | `cloudy.subscribers::<T>()` / `watch_subscribers`（**§2**）  |
| 相手はこの service を serve しているか | `cloudy.servers::<S>()` / `watch_servers`（0.4）             |

「最初の 1 通が消える」を本当に消すのは latched（0.3）か §2 の `watch_subscribers` であって、
ランチャの待ちではない。ランチャの待ちは `depends_on` を**書いてあるとおりの意味にする**だけ。

### 1.3 待ちの置き場所 —— `reiny-launch` は zenoh を知らない

`reiny-launch` はバスに触らないライブラリで、そのままにする（zenoh 依存を足すと、ランチャを
組み込むだけのダウンストリームが zenoh をリンクすることになる）。分割はこう:

- **`reiny-launch`（待ちの方針）** —— いつ待つか、どれだけ待つか、Ctrl+C でどう降りるか。
  `run_launch_dirs` が `ready: Option<Ready<'_>>`（述語 + 期限）を受け取り、「この launch は
  いま生きているか」を **1 回聞く述語**として使う。poll 間隔は runner が持つ。
- **`reiny-cli`（バスの知識）** —— その述語を zenoh の liveliness で実装する。`reiny run` が
  セッションを 1 本開き、`reiny/<domain>/<name>/@launch` を撃つ。

`ResolvedLaunch` に `domain: Option<String>` を足す（既に `--domain` 引数として組み立てている
値を、引数列を解析し直さずに読めるように）。決まらなければ `REINY_DOMAIN` か `"default"` ——
子の解決規則と同じ。

**待つ対象は「誰かが `depends_on` に書いた launch」だけ。** 誰も依存していない launch を待って
も得るものが無く、reiny の launch でない bin を config に混ぜている構成を無駄に遅らせる。

**期限切れは警告して進む。** 落とすのは行儀が良すぎる —— 依存先が単に遅いだけかもしれず、
ランチャが起動を拒否したら人間にできるのは「もう一度実行する」だけ。既定 10 秒、
`reiny run --ready-timeout <秒>`（`0` で 0.4 と同じ「待たない」）。

`--ready-timeout 0` が逃げ道になる場面: 依存先が reiny の launch でない、あるいは launch config
の `zenoh_config` がランチャの既定セッションからは見えない fabric を指している。

### 1.4 respawn の backoff

`on_exit = "respawn"` は 0.4 まで**即座に**再起動していた。デバイスが無い / 設定が不正で即死する
launch は、`spawn` と `wait` の所要時間だけを周期にしてスピンする。

指数 backoff: 200 ms から倍々で 30 s 上限、**プロセスが 60 s 生き延びたら段をリセット**する
（一時的な不調と恒久的な不能を分けるのは、この 1 本で足りる）。

待ちは `sleep` ではなく**再起動の deadline** で表す。監視ループは 100 ms 周期なので、
`sleep(30s)` を挟むと Ctrl+C が最大 30 秒効かなくなる。

---

## 2. subscriber presence

### 2.1 キー

publisher のトークンが型のキーそのもの、server のトークンが `…/<Req>/@service` なのに合わせ、
**購読者のトークンは `reiny/<domain>/<id>/<T>/@sub`**。

- verbatim チャンク（先頭 `@`）なので `*` にも `**` にもマッチしない。`publishers::<T>()`
  （`reiny/<d>/*/<T>`）にも `bag record` の `reiny/<d>/*/*` にも `Key::all` にも混ざらない ——
  `@service` を足した 0.4.0 とまったく同じ性質で、**エンジン側の変更が要らない**（zenoh /
  `Local` / iceoryx2 のどれも、キーの形を知らずに liveliness を通すだけ）。
- トークンが立つのは**自分の id** のキー。`subscriber().from("ctrl")` で購読キーが
  `reiny/<d>/ctrl/<T>` になっても、名乗るのは `reiny/<d>/<自分>/<T>/@sub`。答えたいのは
  「誰が誰の話を聞いているか」ではなく「誰がこの型を聞いているか」だから。
- `Caps.liveliness` が無いエンジンでは黙って立てない（`@schema` と同じ扱い）。publisher は
  liveliness 無しをエラーにしているが、それは `publishers()` の答えが信用できなくなるため。
  購読者のトークンは購読の動作に影響しないので、エラーにする理由が無い。

### 2.2 API

```rust
cloudy.subscribers::<T>().await?      // -> Vec<String>（id 昇順、自分を含む）
cloudy.watch_subscribers::<T>()?      // -> Presence<T>（Joined / Left。宣言済みは history で流れる）
```

`publishers` / `watch_publishers`（0.3）、`servers` / `watch_servers`（0.4）の 3 組目。
実装は `alive_ids` / `watch_key` に `@sub` チャンクを付けて渡すだけ。

CLI:

| コマンド           | 変更                                   |
| ------------------ | -------------------------------------- |
| `reiny topic list` | `SUB` 列（`TYPE / PUB / SUB / SRV`）    |
| `reiny node info`  | `sub :` 行                             |

`reiny-link` の `LinkEngine` は、相手の Hello の `flags::SUB` を `@sub` トークンとして写す
（`peer_keys` に 1 行）。MCU が何を聞いているかが zenoh 側の `reiny topic list` に出る。

### 2.3 subscriber も `@schema` を名乗る

publisher は `T::DESCRIPTOR` があれば `…/<T>/@schema/<message>` で descriptor set に答える
（0.3）。**購読者も同じことをする。**

理由は §4 の `reiny topic pub` にある。ブリングアップで CLI から突きたいのは、たいてい
「コマンドを待っている launch」であって、その型の publisher は**まだ動いていない**。
publisher しか descriptor を名乗らないと、いちばん要る場面で encode できない。

コストは購読者 1 つにつき queryable 1 本（publisher と同じ条件 —— `DESCRIPTOR` があり、
エンジンに query がある場合だけ）。同じ launch が同じ型を publish かつ subscribe すると同じ
キーの queryable が 2 本立つが、`collect_schemas*` は fqn で重複を吸収する。

### 2.4 bridge の絞り込みは、やらない

エンジン抽象と一緒に入った `bridge.rs` には、この `ponytail:` が付いている:

> A で購読するのは「トークンが見えた型 × 全 source」。B が要る型だけに絞るのはエンジンに
> 「誰が欲しがっているか」を聞く口ができてから

`@sub` はその口そのものなので、絞り込みが書けるようになる。**書かない。**

`reiny bridge serial` の帯域はすでに守られている —— `Link::send_raw` は
`peer_subscribes_hash(hash)` が偽なら `Ok(false)` で捨てる（`link.rs:648`）。相手の Hello に
無い型はワイヤに 1 バイトも出ない。絞り込みで浮くのは bridge プロセス内の zenoh 購読 1 本と
チャネル往復だけで、対価は「両側の `@sub` を見張る」「エコー判定をもう 1 系統」「型ごとの
参照計数が publisher 側と購読側の 2 条件になる」という新しい状態機械。**割に合わない。**

`ponytail:` コメントは「口が無い」から「口はあるが、絞る価値が出ていない」に書き換える。
価値が出るのは、リンクではなく **zenoh ↔ zenoh の bridge**（WAN 越しで帯域が有限）が現れたとき。

---

## 3. 詰まり・取りこぼしの可視化

`AGENTS.md` にも `SubscriberBuilder::latest` の doc にも `engine/local.rs` にも同じことが
書いてある:

> 既定の Fifo(256) は**満杯になるとエンジンの受信スレッドをブロックし、その launch の全購読が
> 詰まる**。高レートの状態量は `latest(1)` にする。

規約はある。**間違えたときに気づく手段が無い。** 0.4 までの `pubsub.rs` には計数が 1 つも無く、
`latest(n)` のリングが最古を捨てるのも無言。ロボットが「たまに固まる」の原因がこれだったとき、
バスにもログにも痕跡が残らない。

足すのは 2 つだけ:

```rust
pub struct SubscriberStats {
    pub received: u64,   // engine がこの購読のバッファに渡した数（捨てた分を含む）
    pub dropped: u64,    // リングが満杯で捨てた数（`latest(n)` のときだけ）
    pub blocked: u64,    // Fifo が満杯で engine のスレッドを待たせた回数（既定バッファのとき）
}
cloudy.subscribe::<T>()?.stats()
```

と、**初回 1 回だけの `warn!`**（型名と、どちらの経路かと、次の一手を書く）。毎回鳴らすと
ログが埋まり、鳴らさないと気づけない —— 指紋の不一致（0.3）が既に採っている折り合いと同じ。

計数は callback（= engine のスレッド）で `AtomicU64` に足すだけ。既定経路の挙動は変えない:
`try_send` が満杯を返したら計数して**そのままブロッキングの `send` に落ちる**（0.4 と同じ）。

**バスには出さない。** 診断トピック（`/diagnostics` 相当）は別の設計判断で、購読者ごとの
カウンタと違って「誰が集めて誰が見るか」を決める必要がある。`reiny topic hz` にも出さない ——
CLI が数えられるのは **CLI 自身の**バッファであって、launch のそれではない。

---

## 4. `reiny topic pub`

`reiny service call Req '{json}'` は 0.4 で入り、校正やリセットをシェルから撃てるようになった。
publish 側は空いたまま —— `ros2 topic pub` に当たるものが無い。

```sh
reiny topic pub Command '{"stop":{}}'                  # 1 回
reiny topic pub Command '{"vel":1.0}' --rate 10        # Ctrl+C まで 10 Hz
reiny topic pub Command '{}' --count 3 --rate 2 --as sim
```

- 型の descriptor は**バスから**取る（`@schema`）。§2.3 で購読者も名乗るので、突きたい相手が
  聞くだけの launch でも動く。descriptor がどこにも無ければエラー（hex を打たせる口は作らない）。
- キーは `reiny/<domain>/<--as|reiny-cli>/<TYPE>`。publisher と同じく **liveliness トークンを
  同伴**する（reiny の不変条件。`topic list` / `node list` に出る）。指紋も attachment に載せる。
- 生きた publisher が居ても**拒否しない**。`bag play` が拒否するのは録画元の id に成り代わる
  からで、`topic pub` は自分の id で出す —— 同じ型を複数の source が publish するのは reiny の
  普通の構成。

`--latched` は入れない: latch は queryable を握っているプロセスが要るので、撃って終わる CLI
では表現できない（`--rate` で走らせている間だけ効く latch は嘘になる）。

---

## 5. 足さないもの（意識的に）

| 却下                                          | 理由                                                                                                       |
| --------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| bridge の購読絞り込み                          | リンクは `send_raw` の時点で捨てている（§2.4）。浮くのはプロセス内の 1 購読                                    |
| 診断トピック（`/diagnostics` 相当）            | 「誰が集めて誰が見るか」を決める設計。カウンタ + warn で穴は塞がる（§3）                                       |
| `reiny topic hz` にドロップ数                  | CLI が数えられるのは CLI 自身のバッファ。launch のものではない                                                 |
| ライフサイクル状態機械（configure / activate） | `@launch` の readiness（§1）+ 型ごとの presence（§2）で足りる。状態遷移を要求する消費者が出てから               |
| 動的パラメータ（ROS 2 params 相当）            | latched publish + service で表せる。規約であって機能ではない                                                  |
| `topic pub --latched`                          | 撃って終わるプロセスは latch を握れない（§4）                                                                 |
| readiness を launch config のキーに            | フラグ 1 本（`--ready-timeout`）で逃げられる。config スキーマを増やすのは per-launch で変えたい人が出てから      |
| MQTT / NATS エンジン                           | `0.5.0.md` §6 のまま —— 消費者待ち                                                                                |
| CLI の非 zenoh 対応                            | `0.5.0.md` §7 のまま —— `reiny bridge` 経由で見える                                                                |
| ゼロコピー loan API                            | `0.5.0.md` §5.4 のまま。**測ってから**。この 4 項目とは独立に判断する                                            |
| sim time / `/clock`                            | `bag play` の決定性を要求する消費者が出てから                                                                  |

---

## 6. 公開 API 差分

**破壊的**は 1 つ（`run_launch_dirs` の引数）。**wire は互換**（`@sub` は verbatim チャンク）。

| クレート       | 追加                                                                                                    | 変更 / 削除                                                                                                        |
| -------------- | --------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `reiny`        | `Cloudy::{subscribers, watch_subscribers}`、`engine::SUB_CHUNK`、`SubscriberStats`、`Subscriber::stats`      | —                                                                                                                  |
| `reiny-launch` | `ResolvedLaunch.domain`、`Ready`                                                                            | **破壊的**: `run_launch_dirs` が `ready: Option<Ready<'_>>` を取る（`run_launch` は `None` を渡す糖衣のまま）           |
| `reiny-cli`    | `reiny topic pub`、`reiny run --ready-timeout`、`topic list` の `SUB` 列、`node info` の `sub :` 行           | —                                                                                                                  |
| `reiny-link`   | —                                                                                                          | `LinkEngine` が相手の `flags::SUB` を `@sub` presence として写す                                                     |

`prelude` は変えない（`SubscriberStats` は `Subscriber::stats()` の戻りでしか出てこない）。

### ダウンストリームの移行

| 0.4                                  | 0.5                                                                          |
| ------------------------------------ | ---------------------------------------------------------------------------- |
| `run_launch_dirs(&plan, &dirs, log)` | `run_launch_dirs(&plan, &dirs, log, None)`                                    |
| `depends_on` = 起動順だけ             | `depends_on` = 依存先の `@launch` を待つ（`--ready-timeout 0` で 0.4 の挙動）   |
| `on_exit = "respawn"` が即再起動      | 200 ms → 30 s の指数 backoff（60 s 生存でリセット）                            |
| 購読者はバスに見えない                | `reiny/<d>/<id>/<T>/@sub`、`subscribers::<T>()`                                |
| 取りこぼしは無言                      | 初回 1 回の `warn!` + `Subscriber::stats()`                                    |

---

## 7. 導入順

各段が単独でリリース可能な順（エンジン抽象の後に積む）。段の間の依存は 1 本だけ
（段 3 → 段 4、§2.3 の `@schema`）。

1. **段 1: ランチャ**（§1）—— `reiny-launch` + `reiny-cli`。他に触らない。
2. **段 2: カウンタ**（§3）—— `pubsub.rs` だけ。
3. **段 3: subscriber presence**（§2）—— `reiny` + `reiny-link` + CLI の表示。
4. **段 4: `topic pub`**（§4）—— 段 3 の `@schema` の上。

---

## 8. 検証

| 何を                                  | どこで                                                                  |
| ------------------------------------- | ------------------------------------------------------------------------ |
| backoff の段と reset                  | `runner.rs` の単体（時計を渡す純関数にする）                              |
| readiness の待ちと期限切れ            | `runner.rs` の単体（`ready` 述語を偽装して、待った / 諦めたを観測する）    |
| `@sub` が `*` に混ざらない            | `engine/mod.rs` の `Key` 単体（`@service` の既存テストの隣）              |
| `subscribers` / `watch_subscribers`   | `engine/conformance.rs` の `exercise`（`Local` と `Zenoh` を 1 本で通す）  |
| カウンタ                              | `pubsub.rs` の単体（`Ring` の drop）+ conformance で `stats()`            |
| `@sub` を link が写す                 | `reiny-link/tests/engine.rs`（MCU 役の Hello の SUB が `subscribers()` に出る） |
| `topic list` の SUB 列 / `topic pub`  | `reiny-cli/tests/topic_e2e.rs`（ポート 37450 のまま）                     |

ポートは増やさない —— 0.5.0 の検証記録どおり、e2e の 5 秒 patience は負荷に対して余裕ではない。

---

## 9. 実装記録 —— 何が入り、設計とどこがズレたか

段 1 → 2 → 3 → 4 の順に、設計どおりに入れた。範囲を削った段は無い。

### 9.1 入ったもの

| 設計 | 実装 |
| --- | --- |
| §1.3 待ちの分割 | `reiny_launch::Ready { is_live: &dyn Fn(&ResolvedLaunch) -> bool, timeout }`。`run_launch_dirs(plan, dirs, log, Option<Ready>)`。poll は `READY_POLL` = 50 ms、`wait_for_deps` が `depends_on` を 1 つずつ。確認済みの launch は `live: HashSet` に覚えて再問い合わせしない |
| §1.3 domain | `ResolvedLaunch.domain: Option<String>`。CLI 側 `domain_of()` が `spec.domain` → `REINY_DOMAIN` → `"default"` |
| §1.3 CLI | `runcmd::ready_session()`（`depends_on` がどこにも無ければセッションを開かない / 開けなければ warn して待たない）、`reiny run --ready-timeout`（既定 10 s、`0` は `None` 相当） |
| §1.4 backoff | `next_backoff(previous: Option<Duration>, uptime) -> Duration`（純関数）。`BACKOFF_MIN` 200 ms / `BACKOFF_MAX` 30 s / `BACKOFF_RESET` 1 分。`pending: Vec<(name, Instant)>` を監視ループが毎周見る |
| §2.1 キー | `engine::SUB_CHUNK = "@sub"`。エンジンは無改造（`Key` の chunk として通るだけ） |
| §2.2 API | `Cloudy::{subscribers, watch_subscribers}`、`SubscriberBuilder::build` が `…/@sub` トークンを立てる。`LinkEngine::peer_keys` が `flags::SUB` を写す |
| §2.2 CLI | `topic list` は 3 本の liveliness query を `Roles { pubs, subs, srvs }` に畳む。`node info` は `pub :` / `sub :` / `srv :` |
| §2.3 `@schema` | `SubscriberBuilder::build` が publisher と同じ条件で `declare_schema` |
| §3 カウンタ | `SubscriberStats { received, dropped, blocked }` + `Subscriber::stats()`。`Counters`(`AtomicU64` ×3 + `AtomicBool`)を callback と共有し、`note_full` が初回だけ `warn!` |
| §4 `topic pub` | `topiccmd::publish`。`--as` / `--rate` / `--count`。指紋を attachment に、liveliness トークン同伴 |
| §8 検証 | `runner.rs` 単体 4（backoff の段と reset / 待って返る / 期限切れ / Ctrl+C）、`Key` 単体に `@sub` の非交差、`conformance::exercise` に subscriber presence と `ring.stats()`、`reiny-link/tests/engine.rs` に MCU の SUB、`topic_e2e.rs` に SUB 列・`node info gui`・`topic pub`(成功 / 不正な `--as` / 未記述の型) |

### 9.2 設計から変えた点

- **`ReadyCheck` 型エイリアスではなく `Ready` 構造体**（§1.3 / §6 は `ReadyCheck<'a>` と書いた）。
  述語と期限は必ずセットで、引数を 2 本に増やすと `None` の綴りが `(None, _)` になって汚い。
- **`--ready-timeout 0` は CLI で `None` に落とす**だけでなく、`run_launch_dirs` 側でも
  `ready.filter(|r| !r.timeout.is_zero())` で潰す。ゼロ期限のまま `wait_for_deps` に入ると、
  依存 1 つごとに「現れなかった」警告が出る（待ってすらいないのに）。
- **`Vec::extract_if` を使わなかった**。`retain` + 収集の 6 行で足り、安定化の版に縛られない。
- **`topic list` の空メッセージが変わった**（"no live publishers or servers" → subscribers も
  数えるので）。`topic_e2e` の行アサートは列で比較する形に書き換えた（`ends_with("ctrl")` は
  列が増えると意味が変わる）。
- **`topic pub` の前に settle を置かない。** 最初の 1 通が落ちる心配はあるが、descriptor を
  取る `@schema` の query が既に相手との往復を終えているので、リンクも相手の購読宣言も
  張れている。固定 sleep を足すのは「効いているかどうか誰も確かめられない待ち」を増やすだけ。
- **`--count` を 2 以上にしたときの既定レートは 1 Hz。** 設計は書いていなかった。n 通を
  詰めて撃つのは誰の意図でもない。
- **`bridge` の絞り込みは §2.4 のとおり書かなかった。** `ponytail:` コメントは「口が無い」から
  「口はあるが割に合わない」に書き換えた（`Link::send_raw` が既にワイヤ手前で捨てている）。
- **同じ型の購読が 1 launch に 2 つあると、片方の drop で `watch_subscribers` に `Left` が
  流れる**（トークンが別々なので）。`subscribers()`（alive query）は残っている方を数えるので
  答えは正しい。publisher / server のトークンでも 0.3 / 0.4 から同じ性質で、`@sub` が持ち込んだ
  ものではない —— conformance では publisher の居ない型（`ConfSum`）を使って回避した。
- `runner.rs` の監視ループの終了条件が `children.is_empty()` から
  **`children.is_empty() && pending.is_empty()`** に変わった。backoff 待ちの launch が居るのに
  抜けると、respawn する前にランチャが落ちる。

### 9.3 検証

- `cargo test`（ワークスペース）: 全通過。新規は `reiny-launch` 単体 +4、`reiny` の `Key` 単体
  拡張、`conformance`（`Local` / `Zenoh` の 2 回とも）、`reiny-link` の engine テスト、
  `topic_e2e`（1 本のまま、セッションもポートも増やしていない）。
- `cargo fmt --all --check` / `cargo clippy --all-targets -- -D warnings` / `cargo build
  --all-targets`。
- `@sub` が既存の問い合わせに混ざらないことは `Key::matches` の単体で固定した ——
  `publishers::<T>()`（`reiny/<d>/*/<T>`）にも `Key::all`（`bag record` の `*/*`）にも
  `@service` のパターンにもマッチしない。
