#!/usr/bin/env bash
# 06-compress.sh — `reiny compress` で「動かすのに要るものだけ」を 1 ディレクトリに束ねる。
#                  ランチャ reiny 自身も同梱するので、出力ディレクトリ単体で完結して動く。
#
# `reiny compress <launch.toml> --out <dir> [--launcher <name>]` は launch config の
# [grain] を辿り、各 grain の実行ファイル・実際にリンクしている動的ライブラリ・
# launch config に加えて、**ランチャ reiny 本体** を <dir>/ に集める。target/ 全体ではなく
# 「必要なものだけ」を抜き出すのが眼目。reiny がインストールされていない別マシンでも、
# <dir> をコピーするだけで動く。
#
# --launcher <name> を付けると、同梱する reiny を <name> にリネームし、launch config も
# <name>.toml に揃える。リネームされた実行ファイルは起動時に「自分の名前(argv[0] の
# basename)」から横の <name>.toml を見つけて自動で読み、引数なしで grain 群を起こす。
#
#   reiny compress ping-pong.toml --out dist --launcher ping-pong
#
# 期待する挙動(生成物):
#   dist/
#   ├── ping-pong        # ランチャ(reiny を ping-pong にリネームして同梱)= エントリポイント
#   ├── ping-pong.toml   # ↑が自分の名前から見つけて自動で読む launch config
#   ├── ping             # grain 実行ファイル(ELF / .exe / Mach-O)
#   ├── pong             # 同上
#   └── lib/             # 実際に要る共有ライブラリだけ(.so / .dll / .dylib)
#
#   - これで「ディレクトリだけで完結」: コピー先で `./ping-pong` と打つだけ。
#     ./ping-pong は横の ping-pong.toml を読み、pong → ping の順に子プロセス起動する。
#     (= `reiny run ping-pong.toml` を引数なしでやってくれる)
#   - --launcher 省略時は reiny という名前のまま同梱(`./reiny run ping-pong.toml` で起動)。
#   - 集めるのは「到達可能な grain の bin + 依存ライブラリ + launch config + ランチャ」のみ。
#     ビルド中間物・他 target・未使用 crate・OS 同梱のシステムライブラリは入れない
#     (--include-system でシステムライブラリも同梱)。
#   - 先に 04-build.sh でビルド済みであること。--out 省略時は ./dist。
set -euo pipefail
cd "$(dirname "$0")"

echo "==> reiny compress ping-pong.toml --out dist --launcher ping-pong"
echo "    (grain + 依存ライブラリ + launch config + リネームしたランチャを dist/ に束ねる)"
reiny compress ping-pong.toml --out dist --launcher ping-pong

echo
echo "    dist/ 単体で完結。コピー先でも次だけで起動する:"
echo "      cd dist && ./ping-pong          # 横の ping-pong.toml を自動で読む"
