#!/usr/bin/env bash
# 03-add.sh — `reiny add` で「他プロジェクトの公開型を購読する」依存を配線する。
#
# `reiny add <path>` は、カレント grain の Reiny.toml の [dependencies] に相手を
# 足す。これで相手の公開型が `reiny::dependencies::<name>::<Type>` として見え、
# subscribe できるようになる(型 = トピックなので、購読は型を渡すだけ)。
#
#   cd pong && reiny add ../ping      # pong が ping の Ping を購読できるように
#   cd ping && reiny add ../pong      # ping が pong の Pong を購読できるように
#
# 期待する挙動(pong/Reiny.toml への追記例):
#   [dependencies]
#   ping = { version = "0.1", path = "../ping" }
#
#   - キー名・version は相手の Reiny.toml の [project] から解決。
#   - 雙方向に add すると往復(ping→pong→ping)になる。片方向で足せば relay/sink。
#   - add しただけでは src は変わらない。subscribe::<T>() の呼び出しは自分で書く。
set -euo pipefail
cd "$(dirname "$0")"

echo "==> cd pong && reiny add ../ping   (pong → Ping を購読)"
( cd pong && reiny add ../ping )

echo "==> cd ping && reiny add ../pong   (ping → Pong を購読)"
( cd ping && reiny add ../pong )

echo
echo "    雙方向の購読を配線した。次は 04-build.sh(codegen + build)。"
