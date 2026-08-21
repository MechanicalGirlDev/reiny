//! launch config の `[grain]` テーブル — reiny ランチャの唯一のエントリポイント。
//!
//! Cargo の `[dependencies]` と同じ書式で、各値は
//!
//! - **文字列** = grain config ファイルパスのショートハンド
//!   (`gui = "configs/gui.toml"` ≡ `gui = { config = "configs/gui.toml" }`)、または
//! - **インラインテーブル** = launch override 付きの詳細形
//!   (`monitor = { bin = "...", on_exit = "respawn" }`)。
//!
//! `HumanoidSystem` の `[component]` と違い、**既知種別(control/gui/policy/physics)も
//! プラグインという区別も無い**。すべてのキーは対等な「grain」で、キー名 = インスタンス名 =
//! 既定 bin 名。ランチャは各 grain を同一ワークスペースの子プロセスとして起動する。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// grain がプロセス終了したときの振る舞い(ランチャが解釈)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnExit {
    /// 落ちても記録のみで他は継続する(既定。grain は対等なので privileged な
    /// `control` のような全体停止既定は持たない)。全 grain が終了したらランチャも終わる。
    #[default]
    Ignore,
    /// 落ちたら同じ grain を再起動する。
    Respawn,
    /// 1つでも落ちたら全体を停止する。
    ShutdownAll,
}

/// launch config のルート。`[grain]` テーブルと、その全体に効く既定値。
#[derive(Debug, Clone, Default, Deserialize)]
pub struct LaunchConfig {
    /// この launch 全体の論理名前空間(`--domain`)。同じ LAN / マシン上の別の launch と
    /// 混ざらなくなる —— 実機とログ再生、ロボット 2 体、CI の並列ジョブが同じ理由で救われる。
    /// grain 側の `domain` が指定されていればそちらが勝つ。
    pub domain: Option<String>,
    /// 起動する grain 群。キー = インスタンス名 = 既定 bin 名。`BTreeMap` でキー順を
    /// 決定的にし、起動順(依存が無いとき)を安定させる。
    #[serde(default)]
    pub grain: BTreeMap<String, GrainSpec>,
}

/// 1 grain の宣言。Cargo 依存と同じく、文字列(config パスのショートハンド)
/// またはインラインテーブル(launch override 付き)。
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum GrainSpec {
    /// ショートハンド: config ファイルパスのみ(= `{ config = "..." }`)。
    Config(PathBuf),
    /// 詳細形: launch override を伴う。
    Detailed(GrainEntry),
}

/// `GrainSpec` の詳細形フィールド(全て任意)。
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct GrainEntry {
    /// grain 固有 config ファイルへの、launch config ディレクトリ基準のパス。
    /// 指定すると起動引数に `--config <abs>` を付与する。
    pub config: Option<PathBuf>,
    /// 起動 bin 名の override(未指定はキー名)。
    pub bin: Option<String>,
    /// 追加の起動引数(`--config <...>` の後に付与)。
    pub args: Vec<String>,
    /// 依存(これより前に起動)。
    pub depends_on: Vec<String>,
    /// プロセス終了時の振る舞い(未指定は `Ignore`)。
    pub on_exit: Option<OnExit>,
    /// ログレベルの override。
    pub log_level: Option<String>,
    /// 論理名前空間の override(未指定は launch config の `domain`)。
    /// 別 domain の grain 同士は通信しないので、通常は launch 全体で揃える。
    pub domain: Option<String>,
    /// zenoh セッション設定ファイル(JSON5)への、launch config ディレクトリ基準のパス。
    /// 指定すると起動引数に `--zenoh-config <abs>` を付与する。
    pub zenoh_config: Option<PathBuf>,
    /// false で当該 grain を起動対象から外す(既定 true)。
    pub enabled: Option<bool>,
}

impl GrainSpec {
    /// grain config ファイルへのパス(launch config dir 基準)。
    #[must_use]
    pub fn config(&self) -> Option<&Path> {
        match self {
            Self::Config(p) => Some(p),
            Self::Detailed(e) => e.config.as_deref(),
        }
    }

    /// bin 名の override(未指定はキー名)。
    #[must_use]
    pub fn bin(&self) -> Option<&str> {
        match self {
            Self::Config(_) => None,
            Self::Detailed(e) => e.bin.as_deref(),
        }
    }

    /// 追加起動引数。
    #[must_use]
    pub fn args(&self) -> &[String] {
        match self {
            Self::Config(_) => &[],
            Self::Detailed(e) => &e.args,
        }
    }

    /// `depends_on`。
    #[must_use]
    pub fn depends_on(&self) -> &[String] {
        match self {
            Self::Config(_) => &[],
            Self::Detailed(e) => &e.depends_on,
        }
    }

    /// `on_exit`(未指定は `Ignore`)。
    #[must_use]
    pub fn on_exit(&self) -> OnExit {
        match self {
            Self::Config(_) => OnExit::default(),
            Self::Detailed(e) => e.on_exit.unwrap_or_default(),
        }
    }

    /// `log_level` の override。
    #[must_use]
    pub fn log_level(&self) -> Option<&str> {
        match self {
            Self::Config(_) => None,
            Self::Detailed(e) => e.log_level.as_deref(),
        }
    }

    /// `domain` の override。
    #[must_use]
    pub fn domain(&self) -> Option<&str> {
        match self {
            Self::Config(_) => None,
            Self::Detailed(e) => e.domain.as_deref(),
        }
    }

    /// zenoh セッション設定ファイルへのパス(launch config dir 基準)。
    #[must_use]
    pub fn zenoh_config(&self) -> Option<&Path> {
        match self {
            Self::Config(_) => None,
            Self::Detailed(e) => e.zenoh_config.as_deref(),
        }
    }

    /// 起動対象か(既定 true)。`enabled = false` で外す。
    #[must_use]
    pub fn enabled(&self) -> bool {
        match self {
            Self::Config(_) => true,
            Self::Detailed(e) => e.enabled.unwrap_or(true),
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)] // テストは panic で失敗を表現してよい
mod tests {
    use super::*;

    fn config(s: &str) -> LaunchConfig {
        toml::from_str::<LaunchConfig>(s).expect("parse launch config")
    }

    #[test]
    fn shorthand_string_is_config_path() {
        let c = config(
            r#"
            [grain]
            gui = "configs/gui.toml"
        "#,
        );
        let g = &c.grain["gui"];
        assert_eq!(g.config(), Some(Path::new("configs/gui.toml")));
        assert_eq!(g.bin(), None);
        assert_eq!(g.on_exit(), OnExit::Ignore);
        assert!(g.enabled());
        assert!(g.depends_on().is_empty());
    }

    #[test]
    fn detailed_form_overrides() {
        let c = config(
            r#"
            [grain]
            monitor = { bin = "reiny-monitor", on_exit = "respawn", depends_on = ["gui"], args = ["--fast"] }
        "#,
        );
        let g = &c.grain["monitor"];
        assert_eq!(g.bin(), Some("reiny-monitor"));
        assert_eq!(g.on_exit(), OnExit::Respawn);
        assert_eq!(g.depends_on(), ["gui".to_string()]);
        assert_eq!(g.args(), ["--fast".to_string()]);
    }

    #[test]
    fn domain_and_zenoh_config_parse() {
        let c = config(
            r#"
            domain = "lab"

            [grain]
            gui = { config = "configs/gui.toml", zenoh_config = "z.json5" }
            solo = { bin = "solo", domain = "other" }
        "#,
        );
        assert_eq!(c.domain.as_deref(), Some("lab"));
        assert_eq!(c.grain["gui"].zenoh_config(), Some(Path::new("z.json5")));
        assert_eq!(
            c.grain["gui"].domain(),
            None,
            "grain 未指定なら launch 既定に委ねる"
        );
        assert_eq!(c.grain["solo"].domain(), Some("other"));
    }

    #[test]
    fn disabled_flag_parses() {
        let c = config(
            r#"
            [grain]
            gui = { config = "configs/gui.toml", enabled = false }
        "#,
        );
        assert!(!c.grain["gui"].enabled());
    }
}
