#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "==> cd pong && reiny add ../ping   (pong → Ping を購読)"
( cd pong && reiny add ../ping )

echo "==> cd ping && reiny add ../pong   (ping → Pong を購読)"
( cd ping && reiny add ../pong )

for proj in ping pong; do
  cp "templates/$proj/main.yaml" "$proj/main.yaml"
  cp "templates/$proj/main.rs" "$proj/src/main.rs"
done

echo
echo "    雙方向の購読を配線した。次は 04-build.sh(codegen + build)。"
