#!/usr/bin/env pwsh
#
# reiny 開発環境のセットアップ (Windows / PowerShell)
#
# - Rust ツールチェイン (rust-toolchain.toml に固定) を用意する
# - protoc はビルド時にクレートとして同梱されるため、別途インストール不要
# - 依存をフェッチし、ビルドが通ることを確認する
#
$ErrorActionPreference = 'Stop'

# リポジトリのルート (このスクリプトの 1 つ上の階層) へ移動する
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

Write-Host "==> reiny セットアップを開始します ($RepoRoot)"

# --- Rust ツールチェイン ---------------------------------------------------
if (-not (Get-Command rustup -ErrorAction SilentlyContinue)) {
    Write-Error @"
rustup が見つかりません。先に Rust をインストールしてください:
    https://rustup.rs/ から rustup-init.exe を実行
"@
    exit 1
}

# rust-toolchain.toml に固定したチャンネルを明示的にインストールしておく
Write-Host "==> Rust ツールチェインを確認しています"
rustup show active-toolchain *> $null
rustup toolchain install *> $null

# --- 依存の取得とビルド確認 ------------------------------------------------
Write-Host "==> 依存クレートを取得しています (cargo fetch)"
cargo fetch
if ($LASTEXITCODE -ne 0) { throw "cargo fetch に失敗しました" }

Write-Host "==> ワークスペースをビルドしています (cargo build)"
cargo build
if ($LASTEXITCODE -ne 0) { throw "cargo build に失敗しました" }

Write-Host "==> 完了しました。protoc は同梱バイナリを使うため追加インストールは不要です。"
