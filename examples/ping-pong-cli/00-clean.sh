#!/usr/bin/env bash
# 00-clean.sh — 生成物をすべて消して、雛形生成をやり直せる状態に戻す。
#
# 消すもの: reiny new / reiny init が作った ping/ pong/ と、ビルド成果物 target/。
# 残すもの: スクリプト・README・手書きの ping-pong.toml(コミット対象)。
#
# 期待する挙動:
#   - ping/ pong/ target/ が無ければ何もしない(冪等)。
#   - 実行後はディレクトリにスクリプト類と ping-pong.toml だけが残る。
set -euo pipefail
cd "$(dirname "$0")"

echo "==> clean: removing generated ping/ pong/ target/"
rm -rf ping pong target Cargo.lock
echo "    done. 01-new.sh から作り直せます。"
