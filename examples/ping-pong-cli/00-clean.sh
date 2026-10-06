#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "==> clean: removing generated ping/ pong/ target/"
rm -rf ping pong target Cargo.lock
echo "    done. 01-new.sh から作り直せます。"
