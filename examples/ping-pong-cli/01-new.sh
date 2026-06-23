#!/usr/bin/env bash
# 01-new.sh — `reiny new` で「新しいディレクトリ」に grain プロジェクトを作る。
#
# `reiny new <path> --publish <Type>` は cargo new 相当。<path> ディレクトリを
# 新規作成し、その中に reiny grain の雛形一式を書き出す。
#
#   reiny new ping --publish Ping
#
# 期待する挙動(生成物):
#   ping/
#   ├── Cargo.toml         # name=ping、deps=reiny、build-deps=reiny-build を記入済み
#   ├── Reiny.toml         # [project] name=ping / [publications] Ping=... / [dependencies](空)
#   ├── build.rs           # reiny_build::compile()(Reiny.toml 駆動の codegen)
#   ├── proto/
#   │   └── ping.proto     # message Ping { ... } のスタブ(package ping)
#   └── src/
#       └── main.rs        # #[reiny::main] と publish::<Ping>() のスタブ
#
#   - --publish <Type> で公開型を 1 つ用意する。proto・[publications]・src の
#     publish 行までまとめて生成される。
#   - --publish を省くと [publications] は空(純粋な購読者=sink の雛形)。
#   - <path> が既存だとエラー。中身のあるディレクトリに作りたいときは
#     02-init.sh の `reiny init` を使う。
set -euo pipefail
cd "$(dirname "$0")"

echo "==> reiny new ping --publish Ping   (新規ディレクトリ ping/ を作る)"
reiny new ping --publish Ping

echo
echo "    生成された ping/ の中身:"
echo "      ping/{Cargo.toml,Reiny.toml,build.rs,proto/ping.proto,src/main.rs}"
echo "    次は 02-init.sh(reiny init で pong を作る)。"
