#!/usr/bin/env fish
#
# reiny 開発環境のセットアップ (fish shell)
#
# - Rust ツールチェイン (rust-toolchain.toml に固定) を用意する
# - protoc はビルド時にクレートとして同梱されるため、別途インストール不要
# - 依存をフェッチし、ビルドが通ることを確認する
#

# 何か失敗したら即終了する
function __reiny_die
    echo $argv >&2
    exit 1
end

# リポジトリのルート (このスクリプトの 1 つ上の階層) へ移動する
set -l script_dir (cd (dirname (status --current-filename)); and pwd)
set -l repo_root (cd "$script_dir/.."; and pwd)
cd "$repo_root"; or __reiny_die "リポジトリのルートへ移動できませんでした"

echo "==> reiny セットアップを開始します ($repo_root)"

# --- Rust ツールチェイン ---------------------------------------------------
if not command -q rustup
    echo "!! rustup が見つかりません。先に Rust をインストールしてください:" >&2
    echo "     curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh" >&2
    exit 1
end

# rust-toolchain.toml に固定したチャンネルを明示的にインストールしておく
echo "==> Rust ツールチェインを確認しています"
rustup show active-toolchain >/dev/null 2>&1; or true
rustup toolchain install >/dev/null 2>&1; or true

# --- 依存の取得とビルド確認 ------------------------------------------------
echo "==> 依存クレートを取得しています (cargo fetch)"
cargo fetch; or __reiny_die "cargo fetch に失敗しました"

echo "==> ワークスペースをビルドしています (cargo build)"
cargo build; or __reiny_die "cargo build に失敗しました"

echo "==> 完了しました。protoc は同梱バイナリを使うため追加インストールは不要です。"
