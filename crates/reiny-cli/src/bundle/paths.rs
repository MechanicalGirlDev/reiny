use super::{BTreeSet, Component, Context, DeploymentPlan, Path, PathBuf, Result, ensure};

pub(super) fn instance_path(namespace: &str, deployment: &str) -> Result<PathBuf> {
    if namespace == deployment {
        return Ok(PathBuf::new());
    }
    let suffix = namespace
        .strip_prefix(&format!("{deployment}/"))
        .context("instance namespace outside deployment")?;
    Ok(PathBuf::from("modules").join(suffix))
}

/// Stable origin IDs follow sorted canonical directories, not filesystem iteration.
pub(super) fn source_origins(plan: &DeploymentPlan) -> Result<Vec<PathBuf>> {
    let checkouts = plan.root.join(".reiny/cache/git/checkouts");
    let mut roots = BTreeSet::from([plan.root.clone()]);
    for dir in plan.module_sources.values() {
        if let Ok(relative) = dir.strip_prefix(&checkouts) {
            let hash = relative
                .components()
                .next()
                .context("Git checkout has no origin")?;
            roots.insert(checkouts.join(hash.as_os_str()).canonicalize()?);
        } else if !dir.starts_with(&plan.root) {
            roots.insert(dir.clone());
        }
    }
    let roots: Vec<_> = roots.into_iter().collect();
    Ok(roots
        .iter()
        .filter(|root| {
            root.starts_with(&checkouts)
                || !roots.iter().any(|other| {
                    other != *root && root.starts_with(other) && !root.starts_with(&checkouts)
                })
        })
        .cloned()
        .collect())
}

pub(super) fn relocate(path: &Path, origins: &[PathBuf]) -> Result<PathBuf> {
    let path = path
        .canonicalize()
        .with_context(|| format!("resolving runtime file {}", path.display()))?;
    ensure!(
        path.is_file(),
        "runtime path is not a file: {}",
        path.display()
    );
    // Git checkout roots are more specific than the enclosing deployment root.
    let (index, root) = origins.iter().enumerate()
        .filter(|(_, root)| path.starts_with(root))
        .max_by_key(|(_, root)| root.components().count())
        .with_context(|| format!("runtime file {} is outside owned module origins; move it into a module source tree", path.display()))?;
    let relative = path.strip_prefix(root)?;
    ensure!(!relative.components().any(|part| {
        matches!(part, Component::Normal(name) if name == ".git" || name == ".reiny" || name == "target")
    }), "runtime file {} is in an excluded build/cache directory", path.display());
    Ok(PathBuf::from("sources")
        .join(index.to_string())
        .join(relative))
}

pub(super) fn relative_path(from: &Path, to: &Path) -> PathBuf {
    let common = from
        .components()
        .zip(to.components())
        .take_while(|(a, b)| a == b)
        .count();
    let mut result = PathBuf::new();
    for _ in from.components().skip(common) {
        result.push("..");
    }
    for component in to.components().skip(common) {
        result.push(component.as_os_str());
    }
    if result.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        result
    }
}
