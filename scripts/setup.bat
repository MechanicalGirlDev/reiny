@echo off
rem
rem reiny 開発環境のセットアップ (Windows / cmd.exe)
rem
rem - Rust ツールチェイン (rust-toolchain.toml に固定) を用意する
rem - protoc はビルド時にクレートとして同梱されるため、別途インストール不要
rem - 依存をフェッチし、ビルドが通ることを確認する
rem
setlocal

rem リポジトリのルート (このスクリプトの 1 つ上の階層) へ移動する
set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%.." || exit /b 1
set "REPO_ROOT=%CD%"

echo ==^> reiny セットアップを開始します (%REPO_ROOT%)

rem --- Rust ツールチェイン ---------------------------------------------------
where rustup >nul 2>&1
if errorlevel 1 (
    echo !! rustup が見つかりません。先に Rust をインストールしてください: 1>&2
    echo      https://rustup.rs/ から rustup-init.exe を実行 1>&2
    popd
    exit /b 1
)

rem rust-toolchain.toml に固定したチャンネルを明示的にインストールしておく
echo ==^> Rust ツールチェインを確認しています
rustup show active-toolchain >nul 2>&1
rustup toolchain install >nul 2>&1

rem --- 依存の取得とビルド確認 ------------------------------------------------
echo ==^> 依存クレートを取得しています (cargo fetch)
cargo fetch
if errorlevel 1 (
    echo cargo fetch に失敗しました 1>&2
    popd
    exit /b 1
)

echo ==^> ワークスペースをビルドしています (cargo build)
cargo build
if errorlevel 1 (
    echo cargo build に失敗しました 1>&2
    popd
    exit /b 1
)

echo ==^> 完了しました。protoc は同梱バイナリを使うため追加インストールは不要です。
popd
endlocal
