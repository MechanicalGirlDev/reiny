#!/usr/bin/env bash
# run-all.sh — 00→04 を順に流して雛形生成〜ビルドまで一気に行う。
# 起動(05-run.sh)はフォアグラウンドで止まり続けるので、ここには含めない。
#
#   ./run-all.sh        # clean → new → init → add → build
#   ./05-run.sh         # 起動は別途
set -euo pipefail
cd "$(dirname "$0")"

for step in 00-clean 01-new 02-init 03-add 04-build; do
  echo
  echo "######## $step ########"
  "./$step.sh"
done

echo
echo "######## ready ########"
echo "雛形生成〜ビルド完了。起動は ./05-run.sh で。"
