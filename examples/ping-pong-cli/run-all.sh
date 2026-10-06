#!/usr/bin/env bash
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
