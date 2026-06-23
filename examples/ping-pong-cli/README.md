# ping-pong-cli

他のサンプルが「完成したプロジェクト木」を見せるのに対し、これは **`reiny` の CLI で
雛形をゼロから組み立てる手順** をシェルスクリプトで見せます。`reiny new` / `reiny init` /
`reiny add` / `reiny build` / `reiny run` を順に叩くと、ping と pong が立ち上がります。

> CLI は `reiny-cli` crate(バイナリ名 `reiny`)として実装済みです。リポジトリ直下で
> `cargo build -p reiny-cli` してから `target/debug` を PATH に通すと、下のスクリプトが
> そのまま通ります:
>
> ```sh
> cargo build -p reiny-cli
> export PATH="$(git rev-parse --show-toplevel)/target/debug:$PATH"
> ```
>
> 生成される ping/pong は、上方探索で見つけた reiny crate への path 依存を埋めた
> 独立 cargo プロジェクトです(`.gitignore` 済み)。

## このサンプルがコミットしているもの

雛形(`ping/` `pong/`)は **スクリプトが生成する** ので、リポジトリには置きません
(`.gitignore` 済み)。コミットしているのはスクリプトと手書きの launch config だけ:

```
ping-pong-cli/
├── README.md
├── .gitignore        # ping/ pong/ target/ を無視(生成物)
├── ping-pong.toml    # 手書きの launch config(reiny run が読む)
├── 00-clean.sh       # 生成物を消してやり直す
├── 01-new.sh         # reiny new  : 新規ディレクトリに ping を作る
├── 02-init.sh        # reiny init : 既存ディレクトリに pong を作る
├── 03-add.sh         # reiny add  : ping ↔ pong の購読を配線
├── 04-build.sh       # reiny build: Reiny.toml 駆動 codegen + ビルド
├── 05-run.sh         # reiny run     : launch config からまとめて起動
├── 06-compress.sh    # reiny compress: 要る成果物 + reiny 本体を dist/ に束ねる(単体で完結)
└── run-all.sh        # 00→04 を一気に流す
```

## reiny コマンドと期待する挙動

| コマンド | 相当 | 期待する挙動 |
| --- | --- | --- |
| `reiny new <path> --publish <T>` | `cargo new` | `<path>` を**新規作成**し、grain 雛形一式(`Cargo.toml` / `Reiny.toml` / `build.rs` / `proto/<t>.proto` / `src/main.rs`)を書き出す。`--publish` で公開型 `T` の proto・`[publications]`・publish 行まで用意。省略で sink 雛形。 |
| `reiny init [path] --publish <T>` | `cargo init` | ディレクトリを作らず、**既存ディレクトリにその場で**雛形を足す。既存 `Cargo.toml` には reiny 依存と `build.rs` を**追記**し壊さない。`--name` 省略時はディレクトリ名を `[project].name` に。 |
| `reiny add <path>` | `cargo add --path` | カレント grain の `Reiny.toml` `[dependencies]` に相手を追記。相手の公開型が `reiny::dependencies::<name>::<T>` として subscribe 可能になる。src は変えない。 |
| `reiny build` | `cargo build` + codegen | `Reiny.toml` を読み、`[publications]` の proto → `reiny::publications::*`、`[dependencies]` → `reiny::dependencies::*` を生成して「型 → トピック」を埋め、bin をビルド。 |
| `reiny run <launch.toml>` | (ランチャ) | launch config の `[grain]` 節を読み、`depends_on` 順に各 grain を子プロセス起動。`reiny <launch.toml>`(位置引数)/ 現状の `reiny --config <launch.toml>` も同義。 |
| `reiny compress <launch.toml> --out <dir> [--launcher <name>]` | (配布) | launch config を辿り、**動かすのに要るものだけ**(到達可能な grain の bin + 実際にリンクしている `.so`/`.dll`/`.dylib` + launch config + **ランチャ reiny 本体**)を `<dir>/` に束ねる。`target/` 全体やシステムライブラリは入れない。**reiny ごと入るので `<dir>` 単体で完結**し、reiny 未インストールのマシンでもコピーするだけで動く。`--launcher <name>` で同梱する reiny を `<name>` にリネーム+`<name>.toml` に揃え、`./<name>` だけで起動できる(下記)。 |

## 手順(将来像)

```sh
cd examples/ping-pong-cli
chmod +x *.sh        # 初回のみ

./00-clean.sh        # まっさらに
./01-new.sh          # reiny new ping  --publish Ping
./02-init.sh         # reiny init pong --publish Pong
./03-add.sh          # 雙方向の購読を配線(reiny add)
./04-build.sh        # reiny build(codegen + cargo build)
./05-run.sh          # reiny run ping-pong.toml(その場で起動。Ctrl-C で停止)
./06-compress.sh     # reiny compress(要るものだけ dist/ に束ねて配布用に)

# まとめて(起動以外):
./run-all.sh && ./05-run.sh
```

`05-run.sh` は「その場で動かす」、`06-compress.sh` は「他所へ持っていく」。どちらも
`04-build.sh` のビルド成果物を前提にする並列の出口で、互いに依存しません。

`compress` は **ランチャ reiny ごと** 束ねるので、出力ディレクトリ単体で完結します。
`--launcher` でその reiny をアプリ名にリネームすると、**引数なしの単一実行ファイル** が
エントリポイントになります:

```sh
./06-compress.sh        # reiny compress ... --launcher ping-pong
# → dist/
#   ├── ping-pong        ← reiny をリネームして同梱(エントリポイント)
#   ├── ping-pong.toml   ← ↑が自分の名前から自動で読む launch config
#   ├── ping  pong       ← grain bin
#   └── lib/             ← 要る共有ライブラリだけ

cd dist && ./ping-pong  # reiny 未インストールのマシンでも、これだけで全 grain 起動
```

### リネームしたランチャが「自分の名前」から config を読む

同梱した `ping-pong`(中身は reiny)は、起動時に自分の実行パス(`argv[0]`)の
basename を見て、**同じディレクトリの `<basename>.toml`** を launch config として
自動で読みます。だから `./ping-pong` は `./ping-pong.toml` を、引数なしで拾って
`reiny run` 相当を行います。

```
./ping-pong              ≡ reiny run ./ping-pong.toml     (自分の名前から解決)
./reiny run x.toml       ← リネームしない(--launcher 省略)ときの明示形
```

- basename が `reiny` のまま(リネームなし)のときは自動読みせず、従来通り
  `reiny run <launch.toml>` / `reiny <launch.toml>` を要求する。
- grain bin は実行ファイルと同じディレクトリから探す(`--bin-dir` 既定)。

### `new` と `init` の違いだけ覚える

```
reiny new pong   → ディレクトリ pong/ を作って、その中に雛形を生成
reiny init pong  → 既にある pong/ の中に、その場で雛形を生成(作らない)
```

中身(`Reiny.toml` / `build.rs` / `proto/` / `src/`)は両者で同じ。既存の Cargo
プロジェクトを reiny 化したいなら `init`、新規なら `new`。このサンプルでは ping を
`new`、pong を `init` で作り、両方が同じ雛形になることを見せています。

## できあがる配線

`01`〜`03` を流すと、型 = トピックの約束に沿って往復が組まれます:

```
ping ──Ping──▶ pong
     ◀─Pong──
reiny/ping-1/Ping   reiny/pong-1/Pong
```

| プロジェクト | 作り方 | publications(公開) | dependencies(購読) |
| --- | --- | --- | --- |
| ping | `reiny new ping --publish Ping` | `Ping` | `pong`(= `Pong`) |
| pong | `reiny init pong --publish Pong` | `Pong` | `ping`(= `Ping`) |

## どこを読むか

- まず [ping-pong-ring](../ping-pong-ring) で `Reiny.toml` と publish/subscribe の中身を掴む。
- このサンプルは、その `Reiny.toml` / proto / src を **手書きせず CLI で生成する** 道筋を見せる。
