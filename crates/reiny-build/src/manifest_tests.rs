//! Filesystem-backed schema resolution fixtures.

use super::*;

mod config;
mod discovery;
mod errors;
mod layouts;

/// A throwaway directory tree for manifest fixtures. `describe` reads real files — it rejects a
/// `proto` that is not there — so these tests need a filesystem. The directory removes itself.
struct Fixture(PathBuf);

impl Fixture {
    fn new(name: &str) -> Self {
        use std::sync::atomic::{AtomicU32, Ordering};
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "reiny-build-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn root(&self) -> &Path {
        &self.0
    }

    fn path(&self, rel: &str) -> PathBuf {
        self.0.join(rel)
    }

    /// Write `contents` at `rel`, creating the parent directories.
    fn write(&self, rel: &str, contents: &str) {
        let path = self.path(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, contents).unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// `Resolution` is not `Debug`, so `unwrap_err` is unavailable; this says the same thing, and
/// renders the whole anyhow chain so a context line can be asserted on.
fn describe_err(dir: &Path) -> String {
    match describe(dir) {
        Err(e) => format!("{e:#}"),
        Ok(_) => panic!("expected {} to fail resolution", dir.display()),
    }
}

#[cfg(all(feature = "compile", windows))]
#[test]
fn canonical_module_paths_compile_with_native_protoc() -> Result<()> {
    let fixture = Fixture::new("verbatim-protoc");
    fixture.write(
        "proto/pulse.proto",
        "syntax = \"proto3\"; package fixture; message Pulse { uint64 value = 1; }\n",
    );
    let proto_dir = fixture.path("proto");
    let out_dir = fixture.path("out");
    std::fs::create_dir_all(&out_dir)?;
    let plan = CompilePlan {
        entries: Vec::new(),
        protos: vec![std::fs::canonicalize(fixture.path("proto/pulse.proto"))?],
        includes: vec![std::fs::canonicalize(&proto_dir)?],
        externs: Vec::new(),
        own_names: Vec::new(),
        emit_meta: None,
    };
    let descriptor = compile_protos(&plan, &out_dir, |_| {})?;
    assert!(
        descriptor
            .file
            .iter()
            .any(|file| file.package.as_deref() == Some("fixture"))
    );
    Ok(())
}
