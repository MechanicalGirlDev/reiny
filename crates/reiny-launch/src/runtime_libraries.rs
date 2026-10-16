//! Local PE runtime closure, separate from Cargo's compiler artifact inventory.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use object::read::pe::{PeFile32, PeFile64};
use object::read::{FileKind, Object, ReadCache};

/// Non-PE inputs keep the existing platform discovery contract. Once a file
/// declares the DOS/PE magic, malformed or unsupported headers are errors.
pub(super) fn pe_imports(path: &Path) -> Result<Option<BTreeSet<String>>> {
    let mut file =
        fs::File::open(path).with_context(|| format!("open runtime image {}", path.display()))?;
    let mut magic = Vec::with_capacity(2);
    file.by_ref().take(2).read_to_end(&mut magic)?;
    if magic != b"MZ" {
        return Ok(None);
    }
    // Read only headers and import sections, not the entire executable (which
    // can contain hundreds of megabytes of unrelated code and debug data).
    let cache = ReadCache::new(file);
    let imports = (|| match FileKind::parse(&cache)? {
        FileKind::Pe32 => import_names(&PeFile32::parse(&cache)?),
        FileKind::Pe64 => import_names(&PeFile64::parse(&cache)?),
        _ => bail!("DOS image is not a PE executable"),
    })()
    .with_context(|| format!("read PE runtime imports from {}", path.display()))?;
    Ok(Some(imports))
}

fn import_names<'data>(image: &impl Object<'data>) -> Result<BTreeSet<String>> {
    image
        .import_libraries()?
        .map(|library| {
            let library = library?;
            let name = std::str::from_utf8(library.name())?;
            // Imports are loader names, never a license to traverse the host
            // filesystem or search PATH/system installation directories.
            ensure!(
                !name.is_empty() && !name.contains(['/', '\\', ':']) && name != "." && name != "..",
                "PE import must be a local library name: {name}"
            );
            Ok(name.to_ascii_lowercase())
        })
        .collect()
}

pub(super) fn collect_pe_dependencies(
    executable: &Path,
    imports: BTreeSet<String>,
    parent: &Path,
    files: &mut BTreeMap<PathBuf, PathBuf>,
) -> Result<()> {
    // Roots already include all sibling/native-linked runtime libraries,
    // including plugins loaded without static imports. Index deps by name only:
    // unrelated compiler DLLs must never be read, parsed, copied, or hashed.
    let mut candidates: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();
    for (name, source) in files.iter() {
        candidates
            .entry(name.to_string_lossy().to_ascii_lowercase())
            .or_default()
            .push(source.clone());
    }
    let deps = parent.join("deps");
    if deps.is_dir() {
        for entry in fs::read_dir(&deps)? {
            let entry = entry?;
            if entry.path().is_file() {
                candidates
                    .entry(entry.file_name().to_string_lossy().to_ascii_lowercase())
                    .or_default()
                    .push(entry.path());
            }
        }
    }
    let mut visited = BTreeSet::from([executable.to_owned()]);
    let mut pending: Vec<_> = files.values().cloned().collect();
    let mut needed = imports;
    loop {
        for name in std::mem::take(&mut needed) {
            let Some(sources) = candidates.get(&name) else {
                // Not locally supplied: the target environment provides it.
                continue;
            };
            let source = &sources[0];
            for other in &sources[1..] {
                ensure!(
                    source == other || fs::read(source)? == fs::read(other)?,
                    "conflicting runtime libraries named {name}"
                );
            }
            let filename = source.file_name().context("runtime library filename")?;
            files.insert(PathBuf::from(filename), source.clone());
            pending.push(source.clone());
        }
        let Some(source) = pending.pop() else {
            break;
        };
        if visited.insert(source.clone())
            && let Some(imports) = pe_imports(&source)?
        {
            needed.extend(imports);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifacts::{collect_libraries, stage_prebuilt};

    // A single-section PE fixture with ordinary and RVA-based delay import
    // descriptors. No compiler, external DLL installation, or host PATH needed.
    fn image(is_64: bool, imports: &[&str], delayed: &[&str]) -> Result<Vec<u8>> {
        fn word(bytes: &mut [u8], offset: usize, value: u16) {
            bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
        }
        fn dword(bytes: &mut [u8], offset: usize, value: u32) {
            bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
        }
        let mut bytes = vec![0; 0x1200];
        bytes[..2].copy_from_slice(b"MZ");
        dword(&mut bytes, 0x3c, 0x80);
        bytes[0x80..0x84].copy_from_slice(b"PE\0\0");
        word(&mut bytes, 0x84, if is_64 { 0x8664 } else { 0x14c });
        word(&mut bytes, 0x86, 1);
        let optional_size = if is_64 { 240 } else { 224 };
        word(&mut bytes, 0x94, optional_size);
        word(&mut bytes, 0x96, 2);
        let optional = 0x98;
        word(&mut bytes, optional, if is_64 { 0x20b } else { 0x10b });
        dword(&mut bytes, optional + 32, 0x1000);
        dword(&mut bytes, optional + 36, 0x200);
        dword(&mut bytes, optional + 56, 0x2000);
        dword(&mut bytes, optional + 60, 0x200);
        word(&mut bytes, optional + 68, 3);
        let directories = optional + if is_64 { 112 } else { 96 };
        dword(&mut bytes, directories - 4, 16);
        let section = optional + usize::from(optional_size);
        bytes[section..section + 8].copy_from_slice(b".idata\0\0");
        dword(&mut bytes, section + 8, 0x1000);
        dword(&mut bytes, section + 12, 0x1000);
        dword(&mut bytes, section + 16, 0x1000);
        dword(&mut bytes, section + 20, 0x200);
        dword(&mut bytes, section + 36, 0x4000_0040);
        let mut descriptor = 0x200;
        let mut name_offset = 0x800;
        for (names, directory, stride, name_field) in [(imports, 1, 20, 12), (delayed, 13, 32, 4)] {
            if names.is_empty() {
                continue;
            }
            dword(
                &mut bytes,
                directories + directory * 8,
                u32::try_from(descriptor - 0x200 + 0x1000)?,
            );
            dword(
                &mut bytes,
                directories + directory * 8 + 4,
                u32::try_from((names.len() + 1) * stride)?,
            );
            for name in names {
                if directory == 13 {
                    dword(&mut bytes, descriptor, 1);
                }
                dword(
                    &mut bytes,
                    descriptor + name_field,
                    u32::try_from(name_offset - 0x200 + 0x1000)?,
                );
                bytes[name_offset..name_offset + name.len()].copy_from_slice(name.as_bytes());
                name_offset += name.len() + 1;
                descriptor += stride;
            }
            descriptor += stride; // Null descriptor terminates each table.
        }
        Ok(bytes)
    }

    #[test]
    fn pe_closure_keeps_direct_transitive_delay_and_dynamic_roots() -> Result<()> {
        for is_64 in [false, true] {
            let fixture = tempfile::tempdir()?;
            let target = fixture.path().join("target");
            let deps = target.join("deps");
            let native = fixture.path().join("native/lib");
            let native_bin = fixture.path().join("native/bin");
            for directory in [&deps, &native, &native_bin] {
                fs::create_dir_all(directory)?;
            }
            let executable = target.join("app.exe");
            fs::write(
                &executable,
                image(is_64, &["DIRECT.dll", "KERNEL32.dll"], &["delayed.dll"])?,
            )?;
            for (name, imports, delayed) in [
                ("direct.dll", vec!["transitive.dll"], vec![]),
                ("transitive.dll", vec!["direct.dll"], vec!["late-leaf.dll"]),
                ("delayed.dll", vec![], vec![]),
                ("late-leaf.dll", vec![], vec![]),
                ("plugin-leaf.dll", vec![], vec![]),
                ("native-leaf.dll", vec![], vec![]),
            ] {
                fs::write(deps.join(name), image(is_64, &imports, &delayed)?)?;
            }
            // Deliberately malformed PE: parsing this unrelated compiler DLL
            // would fail the test, as would including it in the final inventory.
            fs::write(deps.join("unrelated_proc_macro.dll"), b"MZbad")?;
            fs::write(
                target.join("plugin.DLL"),
                image(is_64, &["plugin-leaf.dll"], &[])?,
            )?;
            fs::write(
                native_bin.join("native.dll"),
                image(is_64, &["native-leaf.dll"], &[])?,
            )?;
            let mut files = BTreeMap::from([(PathBuf::from("app.exe"), executable.clone())]);
            collect_libraries(&executable, &[native], &mut files)?;
            assert_eq!(
                files
                    .keys()
                    .map(|name| name.to_string_lossy())
                    .collect::<Vec<_>>(),
                [
                    "app.exe",
                    "delayed.dll",
                    "direct.dll",
                    "late-leaf.dll",
                    "native-leaf.dll",
                    "native.dll",
                    "plugin-leaf.dll",
                    "plugin.DLL",
                    "transitive.dll",
                ]
            );
            // The actual staging path must also prune the compiler DLL.
            let staged = stage_prebuilt(&executable, &fixture.path().join("cache"))?;
            let directory = staged.parent().context("staged directory")?;
            assert!(directory.join("transitive.dll").is_file());
            assert!(directory.join("late-leaf.dll").is_file());
            assert!(directory.join("plugin-leaf.dll").is_file());
            assert!(!directory.join("unrelated_proc_macro.dll").exists());
            assert!(!directory.join("KERNEL32.dll").exists());
        }
        Ok(())
    }

    #[test]
    fn pe_inventory_remains_authoritative_and_import_errors_do_not_fall_back() -> Result<()> {
        let fixture = tempfile::tempdir()?;
        let executable = fixture.path().join("app.exe");
        let deps = fixture.path().join("deps");
        fs::create_dir(&deps)?;
        fs::write(&executable, image(true, &["runtime.dll"], &[])?)?;
        fs::write(deps.join("runtime.dll"), image(true, &[], &[])?)?;
        let cache = fixture.path().join("cache");
        fs::write(
            fixture.path().join(".reiny-bundle.json"),
            br#"{"v":1,"files":["app.exe"]}"#,
        )?;
        let staged = stage_prebuilt(&executable, &cache)?;
        assert_eq!(
            fs::read_dir(staged.parent().context("snapshot")?)?.count(),
            1
        );
        fs::remove_file(fixture.path().join(".reiny-bundle.json"))?;
        fs::write(&executable, b"MZbad")?;
        assert!(stage_prebuilt(&executable, &cache).is_err());
        fs::write(&executable, image(true, &["../runtime.dll"], &[])?)?;
        assert!(stage_prebuilt(&executable, &cache).is_err());
        fs::write(&executable, image(true, &["runtime.dll"], &[])?)?;
        fs::write(deps.join("runtime.dll"), b"MZbad")?;
        assert!(stage_prebuilt(&executable, &cache).is_err());
        Ok(())
    }

    #[test]
    fn pe_selected_dependency_conflicts_remain_errors() -> Result<()> {
        let fixture = tempfile::tempdir()?;
        let executable = fixture.path().join("app.exe");
        fs::create_dir(fixture.path().join("deps"))?;
        fs::write(&executable, image(true, &["runtime.dll"], &[])?)?;
        fs::write(fixture.path().join("runtime.dll"), image(true, &[], &[])?)?;
        fs::write(fixture.path().join("deps/runtime.dll"), b"different")?;
        assert!(stage_prebuilt(&executable, &fixture.path().join("cache")).is_err());
        Ok(())
    }
}
