//! launch config の `[grain]` 節から launch plan(起動する grain 群と順序)を導出する。
//! `reiny --config <launch>.toml` を唯一のエントリポイントとする。
//!
//! HumanoidSystem の hs-launch と違い、**既知種別/プラグインの区別は無い**。すべてのキーが
//! 対等な grain で、キー名 = インスタンス名 = 既定 bin 名。bin は同一ワークスペースの
//! target ディレクトリから起動する(別ワークスペースの plugin 探索は持たない)。
//!
//! 既定の規約(override は `[grain]` のインラインテーブルで可能):
//! - bin = キー名、depends_on = []、on_exit = ignore。
//! - `config = "..."` を与えると起動引数に `--config <abs>` を付与する。

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

pub use crate::config::OnExit;
use crate::config::{GrainSpec, LaunchConfig};

/// 解決済みの1 grain 起動仕様(規約 + override 適用後)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedGrain {
    /// インスタンス名(`--name` で子に渡す。plan 内で一意)。launch config のキー名。
    pub name: String,
    /// 起動する実行ファイル名(未指定はキー名)。
    pub bin: String,
    /// 起動引数(`--config <abs path>` 等)。
    pub args: Vec<String>,
    /// 起動順序を規定する依存(これより前に起動)。
    pub depends_on: Vec<String>,
    /// プロセス終了時の振る舞い。
    pub on_exit: OnExit,
    /// この子に渡すログレベルの override(未指定はランチャ既定)。
    pub log_level: Option<String>,
}

/// launch config から導出した launch plan。
#[derive(Debug, Clone, Default)]
pub struct LaunchPlan {
    pub grains: Vec<ResolvedGrain>,
}

/// launch plan の検証エラー。
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LaunchError {
    #[error("duplicate grain name '{0}'")]
    DuplicateName(String),
    #[error("grain '{grain}' depends_on undefined grain '{dep}'")]
    UndefinedDependency { grain: String, dep: String },
    #[error("dependency cycle detected involving '{0}'")]
    Cycle(String),
}

impl LaunchPlan {
    /// launch config(TOML)ファイルから launch plan を導出する。
    pub fn from_launch_config(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read launch config {}", path.display()))?;
        let config: LaunchConfig = toml::from_str(&text)
            .with_context(|| format!("failed to parse [grain] from {}", path.display()))?;
        Ok(Self::from_config(&config, path))
    }

    /// パース済み `LaunchConfig` から plan を組む。grain config パスは launch config の
    /// 場所(dir)基準で絶対解決し、子の作業ディレクトリに依らないようにする。
    fn from_config(config: &LaunchConfig, launch_config_path: &Path) -> Self {
        // launch config を絶対化(存在前提。失敗時は与えられたパスをそのまま使う)。
        let abs = std::fs::canonicalize(launch_config_path)
            .unwrap_or_else(|_| launch_config_path.to_path_buf());
        let cfg_dir = abs.parent().map(Path::to_path_buf).unwrap_or_default();

        // BTreeMap なのでキー順は決定的 → 起動順(依存が無いとき)も安定。
        let grains = config
            .grain
            .iter()
            .filter(|(_, spec)| spec.enabled())
            .map(|(name, spec)| resolve(name, spec, &cfg_dir))
            .collect();

        Self { grains }
    }

    /// 一意名・依存参照・循環を検証する。
    pub fn validate(&self) -> Result<(), LaunchError> {
        let mut seen = std::collections::HashSet::new();
        for g in &self.grains {
            if !seen.insert(g.name.as_str()) {
                return Err(LaunchError::DuplicateName(g.name.clone()));
            }
        }
        for g in &self.grains {
            for dep in &g.depends_on {
                if !seen.contains(dep.as_str()) {
                    return Err(LaunchError::UndefinedDependency {
                        grain: g.name.clone(),
                        dep: dep.clone(),
                    });
                }
            }
        }
        self.topo_order().map(|_| ())
    }

    /// depends_on を満たす起動順(インデックス列)を返す。循環時は `Cycle`。
    pub fn topo_order(&self) -> Result<Vec<usize>, LaunchError> {
        use std::collections::HashMap;

        let index: HashMap<&str, usize> = self
            .grains
            .iter()
            .enumerate()
            .map(|(i, g)| (g.name.as_str(), i))
            .collect();

        // 0=未訪問, 1=訪問中, 2=完了
        let mut state = vec![0u8; self.grains.len()];
        let mut order = Vec::with_capacity(self.grains.len());

        fn visit(
            i: usize,
            grains: &[ResolvedGrain],
            index: &std::collections::HashMap<&str, usize>,
            state: &mut [u8],
            order: &mut Vec<usize>,
        ) -> Result<(), LaunchError> {
            match state[i] {
                2 => return Ok(()),
                1 => return Err(LaunchError::Cycle(grains[i].name.clone())),
                _ => {}
            }
            state[i] = 1;
            for dep in &grains[i].depends_on {
                if let Some(&j) = index.get(dep.as_str()) {
                    visit(j, grains, index, state, order)?;
                }
            }
            state[i] = 2;
            order.push(i);
            Ok(())
        }

        for i in 0..self.grains.len() {
            visit(i, &self.grains, &index, &mut state, &mut order)?;
        }
        Ok(order)
    }
}

/// grain を規約既定 + override から組む。config があれば `--config <abs>` を付与。
fn resolve(name: &str, spec: &GrainSpec, cfg_dir: &Path) -> ResolvedGrain {
    let mut args = Vec::new();
    if let Some(cfg) = spec.config() {
        args.push("--config".to_string());
        args.push(
            resolve_relative(cfg_dir, cfg)
                .to_string_lossy()
                .into_owned(),
        );
    }
    args.extend(spec.args().iter().cloned());
    ResolvedGrain {
        name: name.to_string(),
        bin: spec.bin().unwrap_or(name).to_string(),
        args,
        depends_on: spec.depends_on().to_vec(),
        on_exit: spec.on_exit(),
        log_level: spec.log_level().map(str::to_string),
    }
}

/// 相対パスを base 基準で解決する(絶対パスはそのまま)。
fn resolve_relative(base: &Path, p: &Path) -> PathBuf {
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        base.join(p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// テスト用 plan。launch config パスは存在しない名前でよい(canonicalize は失敗して
    /// 与えたパスにフォールバックする — 構造の検証には十分)。
    fn plan(s: &str) -> LaunchPlan {
        let config: LaunchConfig = toml::from_str(s).expect("parse launch config");
        LaunchPlan::from_config(&config, Path::new("reiny.toml"))
    }

    fn get<'a>(p: &'a LaunchPlan, name: &str) -> Option<&'a ResolvedGrain> {
        p.grains.iter().find(|g| g.name == name)
    }

    #[test]
    fn bin_defaults_to_key_name() {
        let p = plan(
            r#"
            [grain]
            gui = "configs/gui.toml"
        "#,
        );
        let gui = get(&p, "gui").expect("gui present");
        assert_eq!(gui.bin, "gui", "bin defaults to the grain key");
        assert_eq!(gui.on_exit, OnExit::Ignore);
        assert!(gui.depends_on.is_empty());
        assert!(gui.args.iter().any(|a| a == "--config"));
        assert!(gui.args.iter().any(|a| a.ends_with("configs/gui.toml")));
    }

    #[test]
    fn detailed_overrides_apply() {
        let p = plan(
            r#"
            [grain]
            monitor = { config = "configs/m.toml", bin = "reiny-monitor", depends_on = ["gui"], on_exit = "respawn", args = ["--fast"] }
            gui = "configs/gui.toml"
        "#,
        );
        let m = get(&p, "monitor").expect("monitor present");
        assert_eq!(m.bin, "reiny-monitor");
        assert_eq!(m.depends_on, vec!["gui".to_string()]);
        assert_eq!(m.on_exit, OnExit::Respawn);
        // --config <abs> の後に追加 args が並ぶ。
        assert_eq!(m.args.last().map(String::as_str), Some("--fast"));
    }

    #[test]
    fn grain_without_config_has_no_config_arg() {
        let p = plan(
            r#"
            [grain]
            monitor = { bin = "reiny-monitor" }
        "#,
        );
        let m = get(&p, "monitor").expect("monitor present");
        assert!(m.args.iter().all(|a| a != "--config"));
    }

    #[test]
    fn disabled_grain_is_skipped() {
        let p = plan(
            r#"
            [grain]
            gui = { config = "configs/gui.toml", enabled = false }
        "#,
        );
        assert!(get(&p, "gui").is_none());
    }

    #[test]
    fn empty_config_yields_empty_plan() {
        let p = plan("");
        assert!(p.grains.is_empty());
        p.validate().unwrap();
    }

    #[test]
    fn topo_orders_dependencies_first() {
        let p = plan(
            r#"
            [grain]
            gui = "configs/gui.toml"
            monitor = { bin = "reiny-monitor", depends_on = ["gui"] }
        "#,
        );
        p.validate().unwrap();
        let order = p.topo_order().unwrap();
        let pos = |name: &str| {
            order
                .iter()
                .position(|&i| p.grains[i].name == name)
                .unwrap()
        };
        assert!(pos("gui") < pos("monitor"));
    }

    // validate/topo の構造検証は ResolvedGrain を直接組んで行う。
    fn rg(name: &str, deps: &[&str]) -> ResolvedGrain {
        ResolvedGrain {
            name: name.to_string(),
            bin: name.to_string(),
            args: vec![],
            depends_on: deps.iter().map(|s| (*s).to_string()).collect(),
            on_exit: OnExit::Ignore,
            log_level: None,
        }
    }

    #[test]
    fn rejects_undefined_dependency() {
        let p = LaunchPlan {
            grains: vec![rg("a", &["ghost"])],
        };
        assert_eq!(
            p.validate(),
            Err(LaunchError::UndefinedDependency {
                grain: "a".into(),
                dep: "ghost".into(),
            })
        );
    }

    #[test]
    fn detects_cycle() {
        let p = LaunchPlan {
            grains: vec![rg("a", &["b"]), rg("b", &["a"])],
        };
        assert!(matches!(p.validate(), Err(LaunchError::Cycle(_))));
    }

    #[test]
    fn rejects_duplicate_name() {
        let p = LaunchPlan {
            grains: vec![rg("a", &[]), rg("a", &[])],
        };
        assert_eq!(p.validate(), Err(LaunchError::DuplicateName("a".into())));
    }
}
