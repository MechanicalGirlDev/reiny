#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "==> reiny new ping --publish Ping   (新規ディレクトリ ping/ を作る)"
reiny new ping --publish Ping

echo
echo "    生成された ping/ の中身:"
echo "      ping/{Cargo.toml,main.yaml,build.rs,proto/ping.proto,src/main.rs}"
echo "    次は 02-init.sh(reiny init で pong を作る)。"
