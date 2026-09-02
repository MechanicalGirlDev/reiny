#!/usr/bin/env bash
# 05-run.sh — `reiny run` で launch config から launch 群をまとめて起動する。
#
# `reiny run <launch.yaml>` は launch config の [launch] 節を読み、各 launch を
# 同一ワークスペースの子プロセスとして起動する。depends_on の順序を守る。
#
#   reiny run ping-pong.yaml          # 推奨(将来像)
#   reiny ping-pong.yaml              # 位置引数ショートハンド(同じ)
#   reiny --config ping-pong.yaml     # 現状のランチャ(reiny-launch)の形
#
# 期待する挙動:
#   - ping-pong.yaml の通り pong → ping の順に起動する。
#   - ping が Ping を流し、pong が受けて Pong を返し、ping が Pong を受ける。
#   - 各 launch のログが seq 付きで流れる。Ctrl-C で全 launch を停止。
#
# 別々の端末で動かしたいときは(下流から上げると取りこぼしが少ない):
#   cargo run -p pong &
#   cargo run -p ping
set -euo pipefail
cd "$(dirname "$0")"

echo "==> reiny run ping-pong.yaml   (pong → ping の順に起動。Ctrl-C で停止)"
reiny run ping-pong.yaml
