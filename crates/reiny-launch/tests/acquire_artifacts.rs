//! Regression fixtures for the internal acquisition and artifact units.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

#[path = "../src/acquire.rs"]
pub mod acquire;
#[path = "../src/artifacts.rs"]
pub mod artifacts;
#[path = "../src/file_lock.rs"]
mod file_lock;

use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use acquire::{GitSource, SourceResolver};
use artifacts::{BuildKind, BuildSpec, prepare};
use tempfile::TempDir;

fn git(repo: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(["-c", "core.hooksPath=/dev/null"])
        .args(args)
        .current_dir(repo)
        .env("GIT_TERMINAL_PROMPT", "0")
        .output()
        .expect("start fixture Git");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn repository() -> TempDir {
    let repo = tempfile::tempdir().unwrap();
    git(
        repo.path(),
        &["init", "--initial-branch=main", "--template="],
    );
    git(repo.path(), &["config", "user.name", "Fixture"]);
    git(
        repo.path(),
        &["config", "user.email", "fixture@example.invalid"],
    );
    git(repo.path(), &["config", "commit.gpgsign", "false"]);
    fs::create_dir(repo.path().join("nested")).unwrap();
    commit(repo.path(), "first");
    repo
}

fn commit(repo: &Path, contents: &str) -> String {
    fs::write(repo.join("nested/value"), contents).unwrap();
    git(repo, &["add", "--", "nested/value"]);
    git(repo, &["commit", "-m", "fixture revision"]);
    git(repo, &["rev-parse", "HEAD"])
}

fn source(repo: &Path) -> GitSource {
    GitSource {
        git: repo.to_str().unwrap().to_owned(),
        reference: "main".to_owned(),
        path: PathBuf::from("nested"),
    }
}

fn resolve(root: &Path, source: &GitSource, update: bool) -> PathBuf {
    let mut resolver = SourceResolver::open(root, update).unwrap();
    let path = resolver.resolve("module", root, source).unwrap();
    resolver.finish().unwrap();
    path
}

#[test]
fn refs_stay_pinned_until_explicit_update_and_aliases_share_checkout() {
    let repo = repository();
    let root = tempfile::tempdir().unwrap();
    let source = source(repo.path());
    let first = resolve(root.path(), &source, false);
    let old_lock = fs::read(root.path().join("lock.yaml")).unwrap();
    commit(repo.path(), "second");
    assert_eq!(resolve(root.path(), &source, false), first);
    assert_eq!(fs::read(first.join("value")).unwrap(), b"first");
    assert_eq!(fs::read(root.path().join("lock.yaml")).unwrap(), old_lock);

    let mut resolver = SourceResolver::open(root.path(), true).unwrap();
    let updated = resolver.resolve("module", root.path(), &source).unwrap();
    let alias = resolver
        .resolve("nested/alias", root.path(), &source)
        .unwrap();
    assert_eq!(updated, alias);
    assert_ne!(updated, first);
    assert_eq!(fs::read(updated.join("value")).unwrap(), b"second");
    assert_eq!(fs::read(first.join("value")).unwrap(), b"first");
    resolver.finish().unwrap();
    let lock: serde_yaml::Value =
        serde_yaml::from_slice(&fs::read(root.path().join("lock.yaml")).unwrap()).unwrap();
    assert_eq!(lock["version"].as_u64(), Some(1));
    assert_eq!(
        lock["modules"]["module"]["commit"],
        lock["modules"]["nested/alias"]["commit"]
    );
}

#[test]
fn pinned_cache_rehydrates_exact_revision_after_ref_moves() {
    let repo = repository();
    let root = tempfile::tempdir().unwrap();
    let source = source(repo.path());
    resolve(root.path(), &source, false);
    let old_lock = fs::read(root.path().join("lock.yaml")).unwrap();
    commit(repo.path(), "second");
    fs::remove_dir_all(root.path().join(".reiny/cache/git")).unwrap();
    let hydrated = resolve(root.path(), &source, false);
    assert_eq!(fs::read(hydrated.join("value")).unwrap(), b"first");
    assert_eq!(fs::read(root.path().join("lock.yaml")).unwrap(), old_lock);
}

#[test]
fn missing_pin_does_not_fall_back_to_newest_ref_or_overwrite_lock() {
    let repo = repository();
    let root = tempfile::tempdir().unwrap();
    let source = source(repo.path());
    resolve(root.path(), &source, false);
    let lock_path = root.path().join("lock.yaml");
    let mut lock: serde_yaml::Value =
        serde_yaml::from_slice(&fs::read(&lock_path).unwrap()).unwrap();
    lock["modules"]["module"]["commit"] = serde_yaml::Value::String("0".repeat(40));
    fs::write(&lock_path, serde_yaml::to_string(&lock).unwrap()).unwrap();
    let before = fs::read(&lock_path).unwrap();
    fs::remove_dir_all(root.path().join(".reiny/cache/git")).unwrap();
    let mut resolver = SourceResolver::open(root.path(), false).unwrap();
    assert!(resolver.resolve("module", root.path(), &source).is_err());
    assert!(resolver.finish().is_err());
    assert_eq!(fs::read(lock_path).unwrap(), before);
}

#[test]
fn failed_resolution_leaves_good_lock_and_new_transactions_unlocked() {
    let repo = repository();
    let root = tempfile::tempdir().unwrap();
    let mut source = source(repo.path());
    resolve(root.path(), &source, false);
    let before = fs::read(root.path().join("lock.yaml")).unwrap();
    source.reference = "refs/heads/does-not-exist".to_owned();
    {
        let mut resolver = SourceResolver::open(root.path(), false).unwrap();
        assert!(SourceResolver::open(root.path(), false).is_err());
        assert!(resolver.resolve("bad", root.path(), &source).is_err());
        assert!(resolver.finish().is_err());
        assert_eq!(fs::read(root.path().join("lock.yaml")).unwrap(), before);
    }
    let mut resolver = SourceResolver::open(root.path(), false).unwrap();
    resolver.finish().unwrap();
    assert!(SourceResolver::open(root.path(), false).is_ok());
}

#[test]
fn local_relative_and_file_urls_resolve_and_unsafe_inputs_fail() {
    let repo = repository();
    let root = tempfile::tempdir().unwrap();
    let source = source(repo.path());
    let file_url = format!(
        "file:///{}",
        repo.path()
            .to_str()
            .unwrap()
            .replace('\\', "/")
            .trim_start_matches('/')
    );
    let source = GitSource {
        git: file_url,
        ..source
    };
    let path = resolve(root.path(), &source, false);
    assert_eq!(fs::read(path.join("value")).unwrap(), b"first");
    let relative = GitSource {
        git: repo
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned(),
        ..source.clone()
    };
    let mut resolver = SourceResolver::open(root.path(), false).unwrap();
    let path = resolver
        .resolve("relative", repo.path().parent().unwrap(), &relative)
        .unwrap();
    assert_eq!(fs::read(path.join("value")).unwrap(), b"first");
    resolver.finish().unwrap();
    for unsafe_source in [
        GitSource {
            path: PathBuf::from("../outside"),
            ..source.clone()
        },
        GitSource {
            path: root.path().to_owned(),
            ..source.clone()
        },
        GitSource {
            git: "https://user:secret@example.invalid/repo".to_owned(),
            ..source.clone()
        },
        GitSource {
            git: "https://token@example.invalid/repo".to_owned(),
            ..source.clone()
        },
        GitSource {
            git: "https://example.invalid/repo?token=secret".to_owned(),
            ..source.clone()
        },
        GitSource {
            reference: "--upload-pack=evil".to_owned(),
            ..source.clone()
        },
        GitSource {
            reference: "refs/heads/*".to_owned(),
            ..source.clone()
        },
        GitSource {
            path: PathBuf::from("missing"),
            ..source.clone()
        },
    ] {
        let before = fs::read(root.path().join("lock.yaml")).unwrap();
        let mut resolver = SourceResolver::open(root.path(), false).unwrap();
        assert!(
            resolver
                .resolve("unsafe", root.path(), &unsafe_source)
                .is_err()
        );
        assert!(resolver.finish().is_err());
        assert_eq!(fs::read(root.path().join("lock.yaml")).unwrap(), before);
    }
}

#[cfg(unix)]
#[test]
fn symlinked_module_cannot_escape_checkout() {
    let repo = repository();
    let root = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(root.path(), repo.path().join("escape")).unwrap();
    git(repo.path(), &["add", "--", "escape"]);
    git(repo.path(), &["commit", "-m", "fixture symlink"]);
    let source = GitSource {
        path: PathBuf::from("escape"),
        ..source(repo.path())
    };
    let mut resolver = SourceResolver::open(root.path(), false).unwrap();
    assert!(resolver.resolve("escape", root.path(), &source).is_err());
    assert!(resolver.finish().is_err());
    assert!(!root.path().join("lock.yaml").exists());
}

#[test]
fn schemas_have_defaults_and_reject_unknown_fields() {
    let source: GitSource =
        serde_yaml::from_str("git: ssh://git@example.invalid/repo\nref: main\n").unwrap();
    assert_eq!(source.path, PathBuf::from("."));
    assert!(serde_yaml::from_str::<GitSource>("git: a\nref: b\nunknown: true\n").is_err());
    let build: BuildSpec = serde_yaml::from_str("type: cargo\n").unwrap();
    assert!(matches!(build.kind, BuildKind::Cargo));
    assert_eq!(build.manifest, PathBuf::from("Cargo.toml"));
    assert_eq!(build.profile, "release");
    assert!(build.package.is_none() && build.features.is_empty());
    assert!(build.default_features && build.locked);
    assert!(serde_yaml::from_str::<BuildSpec>("type: shell\n").is_err());
    assert!(serde_yaml::from_str::<BuildSpec>("type: cargo\ncommand: evil\n").is_err());
}

fn output(executable: &Path) -> String {
    let output = Command::new(executable).output().unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn cargo_fixture(module: &Path) -> (BuildSpec, String) {
    let project = module.join("nested");
    fs::create_dir_all(project.join("src")).unwrap();
    fs::write(
        project.join("Cargo.toml"),
        r#"
[workspace]
[package]
name = "artifact-fixture"
version = "0.0.0"
edition = "2021"
[features]
default = ["forbidden"]
forbidden = []
chosen = []
[[bin]]
name = "artifact-fixture-bin"
path = "src/main.rs"
required-features = ["chosen"]
[profile.launch]
inherits = "dev"
"#,
    )
    .unwrap();
    fs::write(
        project.join("Cargo.lock"),
        "version = 4\n[[package]]\nname = \"artifact-fixture\"\nversion = \"0.0.0\"\n",
    )
    .unwrap();
    let code = r#"
#[cfg(feature = "forbidden")]
compile_error!("default features must be disabled");
fn main() {
    if std::env::args().nth(1).as_deref() == Some("--hold") {
        use std::io::Write;
        println!("ready");
        std::io::stdout().flush().unwrap();
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).unwrap();
    } else {
        println!("first");
    }
}
"#
    .to_owned();
    fs::write(project.join("src/main.rs"), &code).unwrap();
    let spec = BuildSpec {
        kind: BuildKind::Cargo,
        manifest: PathBuf::from("nested/Cargo.toml"),
        package: Some("artifact-fixture".to_owned()),
        profile: "launch".to_owned(),
        features: vec!["chosen".to_owned()],
        default_features: false,
        locked: true,
    };
    (spec, code)
}

#[test]
fn cargo_selects_configured_bin_stages_immutable_bytes_and_rejects_failed_build() {
    // Keep every build in one test: builds are deliberately sequential.
    let fixture = tempfile::tempdir().unwrap();
    // Git checkouts can be deep enough to exceed MSVC's object-path limit.
    let module = fixture
        .path()
        .join(".reiny/cache/git/checkouts")
        .join("a".repeat(64))
        .join("nested-module-definition".repeat(3));
    let project = module.join("nested");
    let (spec, code) = cargo_fixture(&module);
    let cache = fixture.path().join("cache");
    let first = prepare(&spec, &module, "artifact-fixture-bin", &cache).unwrap();
    assert!(first.starts_with(&cache));
    assert_eq!(output(&first), "first");
    let bytes = fs::read(&first).unwrap();
    assert_eq!(
        prepare(&spec, &module, "artifact-fixture-bin", &cache).unwrap(),
        first
    );
    let mut running = Running(
        Command::new(&first)
            .arg("--hold")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stdout = running.0.stdout.take().unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut ready = String::new();
        let result = BufReader::new(stdout).read_line(&mut ready);
        let _ = ready_tx.send(result.map(|_| ready));
    });
    assert_eq!(
        ready_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap()
            .trim(),
        "ready"
    );
    reader.join().unwrap();
    fs::write(
        project.join("src/main.rs"),
        code.replace("\"first\"", "\"second\""),
    )
    .unwrap();
    let second = prepare(&spec, &module, "artifact-fixture-bin", &cache).unwrap();
    assert_ne!(first, second);
    assert_eq!(output(&second), "second");
    assert_eq!(fs::read(&first).unwrap(), bytes);
    drop(running);
    assert_eq!(output(&first), "first");
    fs::write(
        project.join("src/main.rs"),
        "compile_error!(\"fixture build failure\"); fn main() {}",
    )
    .unwrap();
    assert!(prepare(&spec, &module, "artifact-fixture-bin", &cache).is_err());
    assert_eq!(output(&second), "second");
    assert_eq!(fs::read(&first).unwrap(), bytes);
}
