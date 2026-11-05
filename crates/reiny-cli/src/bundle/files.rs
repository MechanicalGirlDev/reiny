use super::{Context, Path, PathBuf, Result, ensure};

pub(super) fn copy_config_tree(source: &Path, destination: &Path) -> Result<()> {
    std::fs::create_dir_all(destination)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let target = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_config_tree(&entry.path(), &target)?;
        } else {
            copy_file(&entry.path(), &target)?;
        }
    }
    Ok(())
}

pub(super) fn copy_tree(
    source: &Path,
    destination: &Path,
    output: &Path,
    omit_manifest: bool,
) -> Result<()> {
    std::fs::create_dir_all(destination)?;
    let mut entries = std::fs::read_dir(source)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let name = entry.file_name();
        if name == ".git"
            || name == ".reiny"
            || name == "target"
            || (omit_manifest && name == "main.yaml")
        {
            continue;
        }
        let path = entry.path();
        if path.canonicalize()? == output {
            continue;
        }
        ensure!(
            !entry.file_type()?.is_symlink(),
            "runtime source tree contains a symlink; materialize it before bundling: {}",
            path.display()
        );
        let target = destination.join(name);
        if path.is_dir() {
            copy_tree(&path, &target, output, false)?;
        } else {
            copy_file(&path, &target)?;
        }
    }
    Ok(())
}

pub(super) fn copy_file(src: &Path, dst: &Path) -> Result<()> {
    if dst.exists() {
        ensure!(
            std::fs::read(src)? == std::fs::read(dst)?,
            "bundle file collision: {} -> {}",
            src.display(),
            dst.display()
        );
        return Ok(());
    }
    std::fs::create_dir_all(dst.parent().context("copied file has no parent")?)?;
    std::fs::copy(src, dst)
        .with_context(|| format!("copying {} -> {}", src.display(), dst.display()))?;
    Ok(())
}

/// Unix loaders do not search an executable's directory by default. A tiny
/// exec wrapper supplies that directory even after immutable artifact staging.
/// The native executable uses a runtime-library suffix so staging retains it.
pub(super) fn copy_executable(src: &Path, dst: &Path) -> Result<()> {
    if !cfg!(unix) {
        return copy_file(src, dst);
    }
    let name = dst
        .file_name()
        .context("executable has no filename")?
        .to_str()
        .context("executable filename is not UTF-8")?;
    let native_name = format!("{name}.runtime.so");
    let native = dst.with_file_name(&native_name);
    copy_file(src, &native)?;
    ensure!(
        !dst.exists(),
        "executable wrapper collision: {}",
        dst.display()
    );
    let wrapper = format!(
        "#!/bin/sh\nHERE=$(CDPATH= cd -- \"$(dirname -- \"$0\")\" && pwd) || exit 1\nLD_LIBRARY_PATH=\"$HERE${{LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}}\" DYLD_LIBRARY_PATH=\"$HERE${{DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}}\" exec \"$HERE/{native_name}\" \"$@\"\n"
    );
    std::fs::write(dst, wrapper)?;
    std::fs::set_permissions(dst, std::fs::metadata(src)?.permissions())?;
    Ok(())
}

pub(super) fn is_runtime_library(path: &Path) -> bool {
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_default();
    name.ends_with(".dll")
        || name.ends_with(".dylib")
        || name.ends_with(".so")
        || name.contains(".so.")
}

/// Linux linker-resolved dependencies supplement the immutable staged libraries.
pub(super) fn collect_libs(bin: &Path, include_system: bool, out: &mut Vec<PathBuf>) -> Result<()> {
    if !cfg!(target_os = "linux") {
        return Ok(());
    }
    let output = std::process::Command::new("ldd")
        .arg(bin)
        .output()
        .context("ldd is required to discover Linux runtime libraries")?;
    if !output.status.success() {
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        ensure!(
            text.contains("not a dynamic executable") || text.contains("statically linked"),
            "ldd failed for {}: {text}",
            bin.display()
        );
        return Ok(());
    }
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        if let Some((_, rest)) = line.split_once("=>") {
            let path = rest.split_whitespace().next().unwrap_or("");
            ensure!(
                path != "not",
                "unresolved runtime library for {}: {line}",
                bin.display()
            );
            if !path.is_empty() && (include_system || !is_system_lib(path)) {
                ensure!(
                    Path::new(path).is_file(),
                    "resolved runtime library missing: {path}"
                );
                out.push(PathBuf::from(path));
            }
        }
    }
    Ok(())
}

pub(super) fn is_system_lib(path: &str) -> bool {
    path.contains("ld-linux")
        || path.contains("linux-vdso")
        || path.starts_with("/lib/")
        || path.starts_with("/lib64/")
        || path.starts_with("/usr/lib/")
        || path.starts_with("/usr/lib64/")
}
