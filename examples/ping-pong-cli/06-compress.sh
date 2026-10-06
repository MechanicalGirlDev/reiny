#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "==> reiny compress main.yaml --out dist --launcher ping-pong"
echo "    (launch + 依存ライブラリ + launch config + リネームしたランチャを dist/ に束ねる)"
reiny compress main.yaml --out dist --launcher ping-pong

echo
echo "    dist/ 単体で完結。コピー先でも次だけで起動する:"
echo "      cd dist && ./ping-pong          # 横の main.yaml を自動で読む"
