#!/usr/bin/env bash
#
# reiny 開発環境のセットアップ (Linux / macOS)
#
# - Rust ツールチェイン (rust-toolchain.toml に固定) を用意する
# - protoc はビルド時にクレートとして同梱されるため、別途インストール不要
# - 依存をフェッチし、ビルドが通ることを確認する
#
set -euo pipefail

# リポジトリのルート (このスクリプトの 1 つ上の階層) へ移動する
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

echo "==> reiny セットアップを開始します (${REPO_ROOT})"

# --- Rust ツールチェイン ---------------------------------------------------
if ! command -v rustup >/dev/null 2>&1; then
    echo "!! rustup が見つかりません。先に Rust をインストールしてください:" >&2
    echo "     curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh" >&2
    exit 1
fi

# rust-toolchain.toml に固定したチャンネルを明示的にインストールしておく
echo "==> Rust ツールチェインを確認しています"
rustup show active-toolchain >/dev/null 2>&1 || true
rustup toolchain install >/dev/null 2>&1 || true

# --- 依存の取得とビルド確認 ------------------------------------------------
echo "==> 依存クレートを取得しています (cargo fetch)"
cargo fetch

echo "==> ワークスペースをビルドしています (cargo build)"
cargo build

echo "==> 完了しました。protoc は同梱バイナリを使うため追加インストールは不要です。"
