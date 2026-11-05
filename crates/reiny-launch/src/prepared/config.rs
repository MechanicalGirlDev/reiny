use super::{BTreeMap, Component, Context, Path, PathBuf, Result, artifacts, ensure};

pub(crate) fn config_files(
    config: &Path,
    assets: &[PathBuf],
) -> Result<(BTreeMap<PathBuf, PathBuf>, PathBuf)> {
    let config = lexical_absolute(config)?;
    let parent = config.parent().context("config has no parent directory")?;
    let mut sources = vec![config.clone()];
    let mut root = parent.to_owned();
    for asset in assets {
        ensure!(
            !asset.as_os_str().is_empty()
                && !asset.has_root()
                && asset
                    .components()
                    .all(|part| !matches!(part, Component::Prefix(_))),
            "config asset must be relative to the selected config: {}",
            asset.display()
        );
        let source = lexical_absolute(&parent.join(asset))?;
        ensure!(
            !sources.contains(&source),
            "conflicting config bundle path {}",
            asset.display()
        );
        while !source.starts_with(&root) {
            ensure!(root.pop(), "config files have no common filesystem root");
        }
        sources.push(source);
    }
    let entry = config.strip_prefix(&root)?.to_owned();
    let mut files = BTreeMap::new();
    for source in sources {
        ensure!(
            source.is_file(),
            "config file not found: {}",
            source.display()
        );
        files.insert(source.strip_prefix(&root)?.to_owned(), source);
    }
    Ok((files, entry))
}

pub(crate) fn freeze_config(
    files: &BTreeMap<PathBuf, PathBuf>,
    entry: &Path,
    cache: &Path,
    cancel: &crate::CancelToken,
) -> Result<(PathBuf, PathBuf)> {
    let staged = artifacts::stage_files_with(files, cache, entry, cancel)?;
    let bundle = staged
        .ancestors()
        .nth(entry.components().count())
        .context("staged config has no bundle directory")?
        .to_owned();
    Ok((staged, bundle))
}

pub(crate) fn lexical_absolute(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => ensure!(
                normalized.pop(),
                "config path traverses beyond filesystem root: {}",
                path.display()
            ),
        }
    }
    ensure!(normalized.is_absolute(), "config source is not absolute");
    Ok(normalized)
}
