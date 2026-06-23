#!/usr/bin/env bash
# 02-init.sh — `reiny init` で「既存ディレクトリ」を grain プロジェクトにする。
#
# `reiny init [path] --publish <Type>` は cargo init 相当。ディレクトリを新規作成
# せず、いまある(空でも、既存 Cargo プロジェクトでも)ディレクトリにその場で
# 雛形を足す。new との違いはディレクトリを作るかどうかだけ。
#
#   mkdir pong
#   cd pong && reiny init --publish Pong          # path 省略時はカレント
#   # ≡ reiny init pong --publish Pong            # path 明示でも可
#
# 期待する挙動:
#   - すでに Cargo.toml があれば reiny / reiny-build 依存と build.rs を「追記」し、
#     既存の [package] 等は壊さない。無ければ Cargo.toml も作る。
#   - --name 省略時はディレクトリ名(ここでは pong)を [project].name に使う。
#   - 生成物の構成は reiny new と同じ(Reiny.toml / build.rs / proto/ / src/)。
#   - pong は Pong を公開する。Ping の購読は次の 03-add.sh で配線する。
set -euo pipefail
cd "$(dirname "$0")"

echo "==> mkdir pong && reiny init --publish Pong   (既存ディレクトリにその場で雛形)"
mkdir -p pong
( cd pong && reiny init --publish Pong )

echo
echo "    pong/ が grain プロジェクトになった(publications = Pong)。"
echo "    次は 03-add.sh(ping ↔ pong の購読を配線)。"
