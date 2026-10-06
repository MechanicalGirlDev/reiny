#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "==> mkdir pong && reiny init --publish Pong   (既存ディレクトリにその場で雛形)"
mkdir -p pong
( cd pong && reiny init --publish Pong )

echo
echo "    pong/ が launch プロジェクトになった(publications = Pong)。"
echo "    次は 03-add.sh(ping ↔ pong の購読を配線)。"
