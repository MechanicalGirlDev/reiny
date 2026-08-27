# reiny bag 設計 —— `reiny bag record / play / info`

対象: `reiny-cli`（本体）、`reiny`（小さな口を 2 つ）、`reiny-build`（descriptor の同梱）。
起点: `d55838a`（0.3.0 段 4 まで）。行番号はすべてこのコミット時点のもの。
依存: zenoh 1.9/1.10（前設計と同じ）、`mcap` 0.25（Foxglove 公式クレート）。

> **この文書の位置づけ** —— ROS 2 の `ros2 bag` に相当する「バスの記録と再生」を reiny の
> CLI に足すにあたり、**何を持ってきて何を持ってこないか**を決めたもの。rosbag2 の機能一覧を
> 写す文書ではない。判断の記録である。

---

## 0. 方針

### 0.1 判定基準

reiny は type = topic の薄い層であり、bag もその薄さを保つ。判定基準を 1 つに固定する:

> **バスの外にある道具（MCAP のツール群・Foxglove）がすでにやることは、reiny に書かない。**
> reiny が書くのは、**zenoh のキーと reiny の約束事（domain / 送信元 / presence / latched /
> 指紋）を知らないと書けない部分**だけ。

この基準で rosbag2 のサブコマンドを仕分けた結果:

| `ros2 bag`  | reiny            | 判定理由                                                                     |
| ----------- | ---------------- | ---------------------------------------------------------------------------- |
| `record`    | **`bag record`** | キーの形・latched の初期値・指紋 attachment を知らないと正しく録れない       |
| `play`      | **`bag play`**   | 送信元セグメントの保持・presence トークン・latched 応答・指紋の復元が要る    |
| `info`      | **`bag info`**   | `mcap info` でも出るが、domain / 送信元 / 型 / latched は reiny の語彙。40 行 |
| `convert`   | 無し             | 形式は MCAP 一択（§0.2）。変換先が無い                                        |
| `reindex`   | 無し             | `mcap recover` がやる（Ctrl+C を取り損ねた bag の修復も同じ）               |
| filter/merge/split | 無し      | `mcap filter` / `mcap merge` がやる。時間・トピックで切るのに reiny の知識は要らない |
| `list`      | 無し             | サブコマンドが 3 つしか無い                                                  |
| echo（`ros2 topic echo`）| 無し | `mcap cat --json` が protobuf を decode して出す（§5 のスキーマ同梱が前提） |
| 一時停止 / 1 ステップ | 無し   | 実需要が出てから。`--rate` と `--start` で大半は足りる                       |

### 0.2 形式は MCAP

自前の長さ付きレコード列（依存ゼロ）と比べて選んだ理由:

- **rosbag2 の既定形式**で、Foxglove がそのまま開く。MCAP は `protobuf` を第一級のスキーマ符号化
  として持ち（schema data = `FileDescriptorSet`）、descriptor を同梱すれば **reiny 固有のコードを
  1 行も書かずに** GUI で中身が見える。
- `mcap` CLI（Go 製、単体バイナリ）が `info` / `cat` / `filter` / `merge` / `recover` を持つ。
  上表の「無し」はこれに委ねている。自前形式だとこれらを全部書くことになる。
- チャンク単位の zstd 圧縮が形式に組み込み。protobuf の状態ストリームは 3〜5 倍縮む。
- クレートは公式（`mcap` 0.25、Foxglove 保守）で、書く側は `Writer` + `add_schema` /
  `add_channel` / `write_to_known_channel` / `finish` の 5 呼び出しで済む。

代償は依存が 1 つ増えること（`mcap` + その `zstd-sys` の C ビルド）。`cargo install reiny-cli`
が C コンパイラを要求するようになる。それが刺さる環境が出たら `mcap` を `default-features =
false`（非圧縮）に落とす。今は刺さっていない。

### 0.3 ダウンストリームで何に使うか（動機）

HumanoidSystem で実際に困っている順:

1. **実機の歩行 1 本を hs-gui でオフライン再生する。** 今は実機の隣に座って見るしかない。
2. **不具合報告に bag を添える。** ログは各 grain の `logs/*.log` で揃ったが、バス上の値は残らない。
3. **policy の回帰比較。** 実機で録った `WorldState` / `ImuData` を別 domain の hs-control
   （policy モード）へ流し、policy のビルド差で setpoint がどう変わるかを見る。
4. **sysid の入力。** 今は専用 JSONL（`hs-plugin-sysid`）。bag から `Trajectory` を作れば録り直しが要らない。

4 は HumanoidSystem 側の作業（`mcap` クレートで読むだけ、reiny は関与しない）。

---

## 1. 使い方

```text
reiny bag record [--out <file>] [--type <T>]... [--from <id>]... [--exclude-type <T>]...
                 [--duration <sec>] [--no-snapshot]  [bus 引数]
reiny bag play   <file> [--as <id>] [--rate <x>] [--loop] [--start <sec>] [--duration <sec>]
                 [--type <T>]... [--from <id>]... [--force]  [bus 引数]
reiny bag info   <file>

bus 引数（3 つ共通、grain と同じ綴り）: --domain <d> / --zenoh-config <f> / --connect <ep>... / --zenoh-mode <m>
domain の既定は grain と同じ: --domain > REINY_DOMAIN > "default"
```

例:

```bash
reiny bag record --type RobotState --type ImuData --out walk-01.mcap   # Ctrl+C で終了
reiny bag info walk-01.mcap
reiny bag play walk-01.mcap --rate 0.5 --loop                            # 同じ domain へ、同じ送信元 id で
reiny bag play walk-01.mcap --domain replay --as bag                     # 別 domain へ、送信元を bag に書き換えて
```

`--type` の `T` はキーの型セグメント（`[internals]` の別名。`RobotState`）で、proto の FQN ではない。
何も指定しなければ domain 内の全型・全送信元。

---

## 2. 記録（`record`）

### 2.1 何を購読するか

`reiny/<domain>/*/*` を **生の zenoh subscriber** で 1 本。型は知らない、知る必要も無い ——
payload は prost の bytes、キーが `(domain, 送信元, 型)` を全部持っている。`SampleKind::Put`
だけ録る。

`*/*`（2 段固定）であって `**` ではない。`--id` と `--domain` は `validate_segment`
（`crates/reiny/src/lib.rs:295`）が `/` を弾くので 2 段で必ず尽きる。`**` にすると §5 の
`@schema` 段も…と思いきや、zenoh の `@` 始まりチャンク（verbatim）は `**` にも `*` にも
マッチしない（`zenoh-keyexpr/src/key_expr/borrowed.rs:300,357`）ので実害は無いが、
「録る対象は 2 段」を明示する方が読み手に優しい。

### 2.2 latched の初期値（snapshot）

購読を宣言した直後に `session.get("reiny/<domain>/*/*")` を 1 発撃つ。reiny の latched
publisher は自分の publish キーに queryable を立てている（`pubsub.rs:142` `declare_latch`）ので、
これで**記録開始前に publish された最後の値**が集まる。応答は log_time = 記録開始時刻で
先頭に書き、そのチャネルに `reiny.latched = "true"` を付ける（§3.3 で使う）。
`--no-snapshot` で切れる。

### 2.3 時刻

MCAP のメッセージは `log_time` と `publish_time` の 2 つを持つ。

- `log_time` = 受信時の `SystemTime`（ns since epoch）。rosbag2 と同じく**受信時刻が主**。
- `publish_time` = zenoh の `Sample::timestamp()` があればその HLC、無ければ `log_time`。

**grain の既定では無い**: zenoh の `timestamping.enabled` は peer で `false`
（`zenoh-config/src/defaults.rs:139-145`）。送信時刻が要る用途は grain 側の zenoh 設定
（`--zenoh-config` か `RuntimeOptions.zenoh_overrides` に `timestamping/enabled = true`）で
入れる。bag はあるものを写すだけで、無いものを捏造しない。

### 2.4 チャネル

MCAP のチャネル = zenoh のキー 1 本（`topic` にキーをそのまま入れる）。初見のキーで
`add_channel`。`message_encoding = "protobuf"`。metadata:

| キー                  | 値                                                                  |
| --------------------- | ------------------------------------------------------------------- |
| `reiny.domain`        | キーの 1 段目                                                        |
| `reiny.source`        | 2 段目（送信元 id）                                                  |
| `reiny.type`          | 3 段目（型セグメント）                                               |
| `reiny.schema`        | attachment が 8 バイトなら `Topic::SCHEMA` の 16 桁 hex。無ければ省略 |
| `reiny.latched`       | §2.2 の snapshot 由来なら `"true"`                                   |

キーを 3 段に割って metadata に写すのは、再生側が `--domain` / `--as` で段を差し替えるときに
文字列を再パースしないため。**metadata が権威、`topic` は表示用**。

`sequence` はチャネルごとの連番。

### 2.5 終了

Ctrl+C（`ctrlc`、reiny-launch がすでに使っている）でフラグを立て、購読ループは
`recv_timeout(100ms)` で回してフラグを見る。抜けたら **`Writer::finish()`** —— これが
summary / index を書く。finish 無しの bag も `MessageStream` の線形走査では読めるが
`Summary::read` は失敗し `info` が出ない。落とした bag は `mcap recover`。

`--duration` は同じ経路（時間が来たらフラグ）。ファイル分割（`--max-size`）は足さない
（`record` はディスクへ流すので長さの上限はディスク。分割が要るのは §3.1 の読み込み側）。

### 2.6 スキーマ

段 1 では **スキーマ無し**（`schema_id = 0`）。再生は bytes をそのまま返すので困らない。
Foxglove で中を見るには §5 が要る。

---

## 3. 再生（`play`）

### 3.1 読み込み

`std::fs::read` で丸ごと読んで `MessageStream::new(&bytes)`（`mcap/src/lib.rs:228`）。
記録は受信順に書いているので log_time 昇順に出てくる。

<!-- ponytail: 丸読み。RAM を超える bag が出たら mcap::sans_io の LinearReader へ -->

### 3.2 キーをどう戻すか

既定は**録ったキーそのまま** —— `reiny/<domain>/<source>/<TYPE>`。購読側の `Envelope.source`
と `subscriber().from(id)` が録画時と同じに見える。これが「再生」の意味。

- `--domain <d>`: 1 段目を差し替える。**flag > `REINY_DOMAIN` > bag の値**（grain と同じ優先順。
  bag の値が最後に来るのは、`REINY_DOMAIN` を張った環境で再生したら普通はそこへ出したいから）。
- `--as <id>`: 2 段目を差し替える。全チャネルが同じ id になる。「これは bag です」と
  presence で名乗らせたいときの口。

### 3.3 reiny の約束事を守る

`session.put` をキーへ投げるだけでは reiny の購読者から見て**本物と違う**。3 つ揃える:

1. **publisher + liveliness トークンを再生前に全チャネル分宣言する。** `publishers()` /
   `watch_publishers()` が最初のメッセージより先に Joined を見る。トークンは
   `Publisher` と同じくキーそのもの。
2. **`reiny.latched` のチャネルは queryable を立て、直近に再生した値を返す。**
   `declare_latch` の写し（`Arc<Mutex<Option<Vec<u8>>>>`）。これが無いと、再生中に起動した
   `subscriber().latched()` が初期値を取れず、録画時には成立していた挙動が再生で崩れる。
3. **`reiny.schema` があれば attachment に戻す。** 8 バイト LE。無いと購読側の照合は
   素通り（`schema_matches` は attachment 無しを通す）なので動きはするが、指紋の意味が
   再生で消える。

### 3.4 時間

`t0 = Instant::now()`、各メッセージの期限は `t0 + (log_time − first) / rate` の**絶対時刻**。
`sleep_until` して `put`。相対 sleep の累積ではないのでドリフトしない。`--start` は
`first + start` 未満を読み飛ばし、`--duration` はそこから数え、`--loop` は `t0` を取り直して
先頭へ戻る。

ジッタは OS のタイマ粒度で上限が決まる（Windows は既定 15.6 ms、何かが `timeBeginPeriod(1)`
を呼んでいれば 1 ms）。100 Hz の `RobotState` を GUI で見る分には足りる。**制御ループへ
食わせる用途には最初から使わない** —— それは bag をファイルとして読む側の仕事で、バスを
経由する意味が無い。

<!-- ponytail: 期限の 2ms 手前まで sleep して残りをスピン、が要るなら足す。今は要らない -->

### 3.5 生きている publisher への安全弁

再生先の domain に**同じ型の生きた publisher** が居るなら、既定で拒否する:

```text
error: live publisher(s) for JointTargets in domain "default": hs-gui
       replay elsewhere (--domain) or override with --force
```

判定は各チャネルの型について liveliness `get("reiny/<domain>/*/<TYPE>")`（`publishers()` と
同じ）。理由は HumanoidSystem に固有ではない —— **bag に `JointTargets` が入っていて、実機を
駆動中の hs-control と同じ domain へ流せば実機が動く。** rosbag には無い弁で、reiny には
presence があるから 15 行で付く。`--as` を付けても判定は変えない（送信元が違っても
購読者は両方受ける）。

---

## 4. 要約（`info`）

`Summary::read(&bytes)` の statistics + channels から:

```text
walk-01.mcap  12.3 s  (2026-08-27 10:14:02 → 10:14:14)  3,208 messages  zstd
domain: default
  hs-control   RobotState   1,230  100.0 Hz          schema hs.RobotState  9f4e…c1a0
  hs-control   ImuData      1,228   99.8 Hz          schema hs.ImuData     0b77…3e9d
  hs-physics   WorldState     750   61.0 Hz  latched schema —
```

列は 送信元 / 型 / 件数 / 平均 Hz / latched / スキーマ有無 + 指紋。行はチャネル（キー）単位。

---

## 5. スキーマの自己記述（段 2）

### 5.1 問題

record は型を知らない。Foxglove や `mcap cat --json` で中を見るには、チャネルに
`FileDescriptorSet` と FQN（`hs.RobotState`）を付ける必要がある。どこから取るか。

| 候補                                               | 落とした理由                                                                                           |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `reiny bag record --manifest Reiny.toml` で CLI が protoc を回す | CLI に `prost-build` + 同梱 protoc が入る（重い）。録る機械に**その版の**ソースが要る。配布 bundle（`dist/`）から録れない |
| `--descriptors <bin>` を手で渡す                   | 版ズレを人が管理する。指紋があるのに手作業に戻す                                                       |
| **grain がバスで名乗る**                           | **採用。** 走っている grain が持つ descriptor が唯一正しい。CLI に protoc が要らない                   |

### 5.2 仕組み

3 箇所、どれも小さい:

1. **`reiny-build`**: すでに `reiny_descriptors.bin` を `OUT_DIR` に書いている
   （`lib.rs:1028`）。生成コードに 1 行足す:
   ```rust
   pub const REINY_DESCRIPTORS: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/reiny_descriptors.bin"));
   ```
   `reiny_generated.rs` は `reiny::schema!()` / `#[reiny::main]` でクレート直下に `include!` される
   ので `OUT_DIR` はそのクレートのもの。各 `impl Topic` に
   `const DESCRIPTOR: Option<Descriptor> = Some(Descriptor { message: "hs.RobotState", file_set: REINY_DESCRIPTORS })`。
2. **`reiny`**: `Topic` に既定値付き const を 1 つ:
   ```rust
   pub struct Descriptor { pub message: &'static str, pub file_set: &'static [u8] }
   pub trait Topic { const TYPE: &'static str; const SCHEMA: Option<u64> = None; const DESCRIPTOR: Option<Descriptor> = None; }
   ```
   `PublisherBuilder::build` は `T::DESCRIPTOR` が `Some` なら queryable を 1 本足す:
   **`reiny/<domain>/<id>/<TYPE>/@schema/<fqn>`**、応答 payload = `file_set`。liveliness
   トークン・latched queryable と同じく `Publisher` が握って drop で消える。
3. **`reiny bag record`**: 起動時に `get("reiny/<domain>/*/*/@schema/*")` を 1 発。応答キーから
   `(送信元, 型, FQN)`、payload から set。チャネル作成時に `add_schema(fqn, "protobuf", pruned)`。

`@schema` が **verbatim チャンク**なのが効く: `reiny/<domain>/**` を購読している誰かにも
`*/*` の record にも**見えない**（§2.1）。バスの語彙を汚さずに脇へ置ける。
`validate_segment` が `@` を弾くので id / domain と衝突しない。

### 5.3 細部

- **file closure に刈る。** set はクレート全体（hs-proto なら数十 KB）で、MCAP のスキーマは
  型ごとに 1 レコードなので、素のまま入れると 30 型 × 数十 KB が毎 bag に乗る。
  `FileDescriptorProto.dependency` を BFS して、その型のファイルと推移 import だけ残す。15 行。
- **指紋で突き合わせる。** record は取得した descriptor から `fingerprint(fqn, msg)`
  （`reiny-build/src/lib.rs:1148`）を計算し、sample の attachment と比べる。同じビルドから
  出ているので一致するはず —— 不一致は reiny-build のバグか他人の `@schema` で、警告して
  **descriptor 側を信じる**（bytes の正体は publisher が知っている）。
  これに要る `fingerprint` / `prost-types` を CLI から使うため、reiny-build の feature を
  `descriptors = ["dep:prost", "dep:prost-types"]` に割り、`compile` はその上に載せる。
  CLI は `descriptors` だけ開ける（protoc は引かない）。
- **手書き `impl Topic` は `None`** → チャネルはスキーマ無しのまま録れて再生できる。
  「第三者は自分の型に `impl Topic` を書くだけで参加できる」は崩さない。
- **多クレート `[schema]`** の各区画の set は import 込み（prost は import 元も descriptor に
  含める）なので、区画単位で自己完結する。刈る処理も区画をまたがない。
- 応答は数十 KB × publisher 数、record 起動時に 1 回だけ。定常コストは無い。

---

## 6. 置き場所と依存

| クレート      | 変更                                                                                                          |
| ------------- | ------------------------------------------------------------------------------------------------------------- |
| `reiny-cli`   | `src/bagcmd.rs`（record / play / info + bus 引数 → セッション。`checkcmd.rs` と同じ流儀で 1 ファイル）。依存に `reiny`（zenoh 再エクスポート + `RuntimeOptions`）・`mcap`・`ctrlc` |
| `reiny`       | `RuntimeOptions::zenoh_config(self) -> Result<zenoh::Config>` を pub に切り出す（`runtime.rs:191-197` の overrides 適用部。`run_with` もこれを呼ぶ）。段 2: `Descriptor` + `Topic::DESCRIPTOR` + `@schema` queryable |
| `reiny-build` | 段 2: `REINY_DESCRIPTORS` const の生成、`impl Topic` への `DESCRIPTOR`、feature `descriptors` の分離、`fingerprint` を pub に |

- **tokio は入れない。** zenoh の同期 API（`.wait()`）と std スレッドで足りる。CLI は今も tokio 無し。
- **feature で括らない。** 括ると「録れない `reiny`」を配る手段が増えるだけ。zenoh のビルド時間は
  bag が無くても `reiny run` の利用者は grain 側で払っている。
- **`main.rs:171` の `SUBCOMMANDS` に `"bag"` を足す。** 忘れると `reiny bag …` が後方互換の
  「位置引数 = launch config」に食われて `bag` というファイルを探しに行く。テスト
  `subcommands_are_not_backward_compat` に足す。
- 既定の出力名は `<yyyymmdd-HHMMSS>.mcap`（UTC）。HumanoidSystem の `logs/<yyyymmdd-HHMMSS>-<bin>.log`
  と同じ並びになる。日付整形は std に無い —— `time` クレートか 15 行の civil-from-days、どちらでも。
- 版数: 0.3.0 は未公開なので**同乗**。全部追加のみ（`Topic` の const は既定値付き）。

---

## 7. 足さないもの（意識的に）

| 却下                                   | 理由                                                                                         |
| -------------------------------------- | -------------------------------------------------------------------------------------------- |
| filter / merge / split / convert       | `mcap` CLI がやる（§0.1）                                                                    |
| `bag echo` / 動的 decode               | `mcap cat --json` がやる。`prost-reflect` を CLI に入れない                                   |
| presence イベントの記録                | 「誰がいつ居たか」は液体で、再生して意味があるのは値だけ。要るなら MCAP の metadata レコードに後から足せる |
| `record` の分割（`--max-size`）        | 書く側はストリームなので上限はディスク。読む側は §3.1 の ponytail                            |
| 一時停止 / ステップ / キーボード操作   | 実需要が出てから                                                                             |
| GUI（hs-gui の「記録」ボタン）         | HumanoidSystem 側の話。やるなら `reiny bag record` を子プロセスで起動する 1 ボタン           |
| `Reiny.toml` / launch config への記録設定 | 記録は**運用**であって配備でもスキーマでもない。CLI 引数だけ                                 |
| 送信時刻の捏造                         | zenoh が付けなければ `publish_time = log_time`（§2.3）                                       |

---

## 8. 導入順

| 段 | 内容                                                                                   | 見える成果                                            |
| -- | -------------------------------------------------------------------------------------- | ----------------------------------------------------- |
| 1  | `record`（スキーマ無し、snapshot 込み）/ `info` / `play`（3.3 の 3 点 + 3.5 の安全弁） | 実機の 1 本を録って hs-gui で再生できる               |
| 2  | §5 `@schema` —— reiny-build / reiny / record の 3 箇所                                 | Foxglove と `mcap cat --json` で中が見える            |
| 3  | HumanoidSystem 側（別リポ）: `hs.py run --bag`、sysid が bag を読む                    | reiny の変更は無し                                    |

段 1 だけで動機 1〜3 は満たせる。段 2 は「見える」の追加であって、段 1 の bag は段 2 の
`play` でそのまま再生できる（スキーマ無しチャネルの扱いは同じ）。

---

## 9. 検証

- **`crates/reiny-cli/src/bagcmd.rs` の e2e**（`crates/reiny/src/e2e.rs` と同じ流儀: ループバック
  TCP 固定ポート、マルチキャスト off。ポートは **37448** —— reiny 側と同時に走る）。
  1 本で: 2 送信元 × 2 型を録る → snapshot が先頭に入る → 別 domain へ `--as bag` で再生 →
  `Envelope.source == "bag"`、順序と件数が一致、`reiny.latched` チャネルが `latched()` 購読者に
  初期値を返す、attachment の指紋が戻っている → 同じ型の生きた publisher が居る domain へは
  `--force` 無しで拒否される。
- **`play` の時間**: `--rate 2` で 1 秒分の bag が 0.5 秒 ± タイマ粒度で終わること。
- **`mcap` CLI**: `mcap info` / `mcap recover`（finish 無しの bag）で読めること。段 2 では
  `mcap cat --json` が `RobotState` を decode して出すこと、Foxglove で開けること（手動、
  記録に残す）。
- 段 2 の verbatim: `reiny/<domain>/**` の subscriber が `@schema` 応答を受け取らないことを
  e2e に 1 assert。

---

## 10. 実装記録 —— 何が入り、設計とどこがズレたか

段 1・段 2 を **1 度に**実装した(段 2 の `@schema` は段 1 の record を触るので、分けると
2 回書くことになる)。段 3 は HumanoidSystem 側なので未着手のまま。以下は実装後に書いた節。

### 10.1 入ったもの

| 設計 | 実装 |
| --- | --- |
| §2 record | `reiny bag record`。`reiny/<domain>/*/*` を生 subscriber 1 本、snapshot は `get` 1 発、channel metadata に domain/source/type/schema(指紋)/latched |
| §3 play | `reiny bag play`。キー復元(`--domain`/`--as`)、publisher+liveliness+latched queryable、指紋 attachment 復元、絶対期限 sleep、`--start`/`--duration`/`--rate`/`--loop`/`--type`/`--from` |
| §3.5 安全弁 | 宣言前に liveliness `get`、生きた publisher が居れば `--force` 無しで拒否 |
| §4 info | `reiny bag info`。source/type/count/Hz/latched/schema+指紋 |
| §5 `@schema` | `Topic::DESCRIPTOR` + publisher の queryable(reiny)、`REINY_DESCRIPTORS` + `impl` の `DESCRIPTOR`(reiny-build)、record の `collect_schemas`(CLI) |
| §5.3 刈り込み | `reiny_build::descriptor_subset`(ファイル閉包へ刈る)+ `message_fingerprint`。feature `descriptors` |

### 10.2 設計から変えた点

- **info の schema 列は「記述子名」と「指紋」を独立に出す**。§4 の例は
  `schema hs.RobotState 9f4e…` と 1 まとまりに見えるが、段 1 の bag は指紋しか無い
  (`@schema` を出す grain が居ないと descriptor が付かない)。両者を 1 つの match で
  組むと「指紋はあるが記述子は無い」行で指紋が落ちる。別々に組む。
- **e2e は「ビルド済みバイナリのサブプロセス」で回す**(§9 は「`bagcmd.rs` の e2e」)。
  reiny-cli は bin 専用クレート(lib ターゲットが無い)なので、`tests/` から `bagcmd::run`
  を呼べない。`CARGO_BIN_EXE_reiny` を `Command` で起動し、テスト側は grain 役の zenoh
  セッションを 1 本張って publisher / latched / presence を演じる。record は `--duration`
  で終わらせる。**ポートは 37448**(reiny 本体 e2e の 37447 と 1 つずらす)。
- **descriptor は「不透明バイト列」として扱う**。reiny(`declare_schema`)も play も、
  descriptor set の中身を一切解釈しない —— そのまま `@schema` で返し、そのまま MCAP へ
  入れるだけ。解釈(FQN 探索・ファイル刈り・指紋)は record が `reiny-build` に投げる。
  だから reiny 本体の e2e は「本物でない `b"not-a-real-descriptor-set"`」で通せる
  (バイト列がそのまま往復することだけを確かめる)。
- **`RuntimeOptions::zenoh_config()` を pub に切り出した**(設計では「bus 引数を写す」と
  だけ)。`--connect` の json5 化・`REINY_DOMAIN`・既定値の解決を `reiny bag` が写さずに
  済むよう、`run_with` がセッションを開く直前に通る経路を関数にして公開した。副産物として
  `ZenohSource::into_config`(消費)は `to_config`(借用 + `Config::clone`)になった。

### 10.3 まだ入っていないもの

段 3(HumanoidSystem 側: `hs.py run --bag`、sysid が bag を読む)は別リポの作業。reiny への
変更は無い。§7「足さないもの」は意図どおり据え置き。

### 10.4 検証

- reiny ワークスペース: `cargo fmt --all --check` / `cargo clippy --all-targets -D warnings`
  / `cargo test`(50 テスト、うち `reiny-build` に descriptor 刈り込み + 指紋一致の 2 本)。
- **`crates/reiny-cli/tests/bag_e2e.rs`** —— record→info→play を通しで 1 本。snapshot が
  先頭に入る、info に latched の cfg 行と Probe の指紋が出る、別 domain へ `--as bag` で
  再生して送信元が `bag`・値の順序と件数・指紋の復元が揃う、生きた publisher の居る domain へ
  `--force` 無しは拒否・`--force` で通る、までを確かめる。
- **`crates/reiny/src/e2e.rs`** に `@schema` を 1 ケース追加 —— `DESCRIPTOR` を持つ publisher
  が `reiny/<domain>/*/*/@schema/*` に応え、その応答が `reiny/<domain>/**` には混ざらない
  (verbatim)ことを実 zenoh で確かめる。
- 生成物確認: `examples/ping-pong-schema-split` の各区画に `REINY_DESCRIPTORS` と、その区画の
  FQN を指す `DESCRIPTOR` が出る(consumer 側は再エクスポートのみ)。

---

## 付録: 検証済み事実の一次ソース

| 事実                                                             | 場所                                                        |
| ---------------------------------------------------------------- | ----------------------------------------------------------- |
| zenoh の `timestamping.enabled` は peer / client で `false`      | `zenoh-config-1.9.0/src/defaults.rs:139-145`                |
| `@` 始まりチャンクは verbatim。`*` / `**` にマッチしない          | `zenoh-keyexpr-1.9.0/src/key_expr/borrowed.rs:300,357,368`  |
| `Sample` は `key_expr` / `kind` / `timestamp` / `attachment` を持つ | `zenoh-1.9.0/src/api/sample.rs:243-307`                   |
| `mcap` 0.25 の `add_schema` / `add_channel` / `write_to_known_channel` / `finish` | `mcap-0.25.0/src/write.rs:501,626,837,1176` |
| `MessageStream` / `Summary` / `LinearReader`                     | `mcap-0.25.0/src/lib.rs:228`, `src/read.rs:44`              |
| `Channel.metadata: BTreeMap<String,String>`、`Message.log_time / publish_time` | `mcap-0.25.0/src/lib.rs:196-216`              |
| reiny の latched は publish キー上の queryable                    | `crates/reiny/src/pubsub.rs:142`                            |
| 指紋 = attachment 8 バイト LE、無ければ照合を素通り               | `crates/reiny/src/pubsub.rs:185-197,364-375`                |
| `reiny_descriptors.bin` を `OUT_DIR` に書いている                | `crates/reiny-build/src/lib.rs:1028`                        |
| CLI の後方互換パスがサブコマンド名を列挙で判定している            | `crates/reiny-cli/src/main.rs:171`                          |
