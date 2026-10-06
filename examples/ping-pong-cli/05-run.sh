#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "==> reiny run main.yaml   (宣言した接続で起動。Ctrl-C で停止)"
reiny run main.yaml
