#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export CARGO_TARGET_DIR="$(pwd)/target"

echo "==> reiny build   (root main.yaml の宣言で両方の成果物を用意)"
reiny build

echo
echo "    ping / pong の bin がビルドされた。次は 05-run.sh(まとめて起動)。"
