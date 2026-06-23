#!/usr/bin/env bash
# 04-build.sh — `reiny build` で Reiny.toml 駆動の codegen を回してビルドする。
#
# `reiny build` は build.rs の `reiny_build::compile()` と同じ codegen を確実に
# 走らせてから cargo build をかけるラッパ。各 grain の Reiny.toml を読み、
#   - [publications] の proto → `reiny::publications::*`
#   - [dependencies] の公開型 → `reiny::dependencies::<name>::*`
# を生成して「型 → トピック」を埋め込む。
#
#   cd ping && reiny build
#   cd pong && reiny build
#
# 期待する挙動:
#   - proto から Rust 型が生成され、publish::<T>() / subscribe::<T>() が解決する。
#   - bin(ping / pong)がビルドされ、05-run.sh の reiny run から起動できる。
#   - Reiny.toml と proto に不整合があればここで型エラーとして出る。
set -euo pipefail
cd "$(dirname "$0")"

for proj in ping pong; do
  echo "==> cd $proj && reiny build"
  ( cd "$proj" && reiny build )
done

echo
echo "    ping / pong の bin がビルドされた。次は 05-run.sh(まとめて起動)。"
