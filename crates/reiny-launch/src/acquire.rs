//! Pinned Git module acquisition. Fetching never runs a module's build commands.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::file_lock::FileLock;

/// A Git repository and an exact reference containing a module.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GitSource {
    /// HTTPS, SSH, or local Git repository location.
    pub git: String,
    /// Branch, tag, or commit to pin on first resolution.
    #[serde(rename = "ref")]
    pub reference: String,
    /// Module directory inside the repository; defaults to the repository root.
    #[serde(default = "default_path")]
    pub path: PathBuf,
}

fn default_path() -> PathBuf {
    PathBuf::from(".")
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Record {
    url: String,
    #[serde(rename = "ref")]
    reference: String,
    path: PathBuf,
    commit: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Lock {
    version: u32,
    modules: BTreeMap<String, Record>,
}

/// A transaction holding the root lock until resolution is finished or dropped.
pub(crate) struct SourceResolver {
    root: PathBuf,
    cache: PathBuf,
    update: bool,
    lock: Option<FileLock>,
    records: BTreeMap<String, Record>,
    resolved: BTreeMap<(String, String), String>,
    failed: bool,
}

impl SourceResolver {
    /// Open a root lock transaction. An existing writer is reported as an error.
    ///
    /// # Errors
    /// Returns an error for an inaccessible root, busy lock, or invalid lockfile.
    pub(crate) fn open(root: &Path, update: bool) -> Result<Self> {
        let root = root.canonicalize().context("canonicalize module root")?;
        let cache = root.join(".reiny/cache/git");
        fs::create_dir_all(&cache)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join(".reiny/source.lock"))?;
        let lock = FileLock::new(lock).context("another source resolver holds the root lock")?;
        let lock_path = root.join("lock.yaml");
        let records = if lock_path.exists() {
            let parsed: Lock = serde_yaml::from_slice(&fs::read(&lock_path)?)
                .context("parse Git module lockfile")?;
            ensure!(
                parsed.version == 1,
                "unsupported Git lock version {}",
                parsed.version
            );
            for record in parsed.modules.values() {
                validate_url(&record.url)?;
                validate_path(&record.path)?;
                validate_commit(&record.commit)?;
            }
            parsed.modules
        } else {
            BTreeMap::new()
        };
        Ok(Self {
            root,
            cache,
            update,
            lock: Some(lock),
            records,
            resolved: BTreeMap::new(),
            failed: false,
        })
    }

    /// Resolve a source under an invocation namespace without moving a locked pin.
    ///
    /// Relative repository locations are interpreted against `base`.
    ///
    /// # Errors
    /// Returns an error for an invalid source, failed Git operation, or path escape.
    /// Any failure prevents this transaction from writing its lockfile.
    pub(crate) fn resolve(
        &mut self,
        namespace: &str,
        base: &Path,
        source: &GitSource,
    ) -> Result<PathBuf> {
        let result = self.resolve_inner(namespace, base, source);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn resolve_inner(
        &mut self,
        namespace: &str,
        base: &Path,
        source: &GitSource,
    ) -> Result<PathBuf> {
        ensure!(
            self.lock.is_some(),
            "source transaction has already finished"
        );
        ensure!(
            !namespace.is_empty(),
            "Git module namespace must not be empty"
        );
        validate_url(&source.git)?;
        validate_path(&source.path)?;
        ensure!(
            !source.reference.is_empty() && !source.reference.starts_with('-'),
            "Git ref must be nonempty and must not begin with '-'"
        );
        git(
            &self.root,
            &["check-ref-format", "--allow-onelevel", &source.reference],
        )
        .context("Git source must name one exact ref or commit")?;
        let url = repository_url(&source.git, base)?;
        let previous = self.records.get(namespace).filter(|record| {
            record.url == url && record.reference == source.reference && record.path == source.path
        });
        let pinned = if self.update {
            None
        } else {
            previous.map(|record| record.commit.clone())
        };
        let repo = self.cache.join("repos").join(hash(url.as_bytes()));
        fs::create_dir_all(repo.parent().context("repository cache parent")?)?;
        if !repo.exists() {
            fs::create_dir(&repo)?;
            if let Err(error) = git(&repo, &["init", "--bare", "--template="]) {
                fs::remove_dir_all(&repo)?;
                return Err(error);
            }
        }
        let commit = if let Some(commit) = pinned {
            validate_commit(&commit)?;
            if git(&repo, &["cat-file", "-e", &format!("{commit}^{{commit}}")]).is_err() {
                // Never fetch the mutable ref as a substitute for a missing pin.
                git(&repo, &["fetch", "--no-tags", "--", &url, &commit])?;
                git(&repo, &["cat-file", "-e", &format!("{commit}^{{commit}}")])?;
            }
            commit
        } else if let Some(commit) = self.resolved.get(&(url.clone(), source.reference.clone())) {
            commit.clone()
        } else {
            git(
                &repo,
                &["fetch", "--no-tags", "--", &url, &source.reference],
            )?;
            let commit = git(&repo, &["rev-parse", "--verify", "FETCH_HEAD^{commit}"])?;
            let commit = commit.trim().to_owned();
            validate_commit(&commit)?;
            self.resolved
                .insert((url.clone(), source.reference.clone()), commit.clone());
            commit
        };
        let checkout = materialize(&self.cache, &repo, &url, &commit)?;
        let module = checkout
            .join(&source.path)
            .canonicalize()
            .context("Git module path does not exist")?;
        ensure!(
            module.starts_with(&checkout),
            "Git module path escapes its checkout"
        );
        ensure!(module.is_dir(), "Git module path is not a directory");
        self.records.insert(
            namespace.to_owned(),
            Record {
                url,
                reference: source.reference.clone(),
                path: source.path.clone(),
                commit,
            },
        );
        Ok(module)
    }

    /// Atomically publish all successful pins to `root/lock.yaml`.
    ///
    /// # Errors
    /// Refuses a failed transaction and reports serialization or filesystem errors.
    pub(crate) fn finish(&mut self) -> Result<()> {
        ensure!(
            self.lock.is_some(),
            "source transaction has already finished"
        );
        ensure!(
            !self.failed,
            "cannot write lockfile after failed Git resolution"
        );
        if self.records.is_empty() {
            self.lock.take();
            return Ok(());
        }
        let serialized = serde_yaml::to_string(&Lock {
            version: 1,
            modules: self.records.clone(),
        })?;
        let staging = self.root.join(".reiny/lock.yaml.partial");
        let mut file = File::create(&staging)?;
        file.write_all(serialized.as_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(staging, self.root.join("lock.yaml")).context("publish Git module lockfile")?;
        self.lock.take();
        Ok(())
    }
}

fn materialize(cache: &Path, repo: &Path, url: &str, commit: &str) -> Result<PathBuf> {
    let checkout = cache
        .join("checkouts")
        .join(hash(format!("{url}\0{commit}").as_bytes()));
    if !checkout.exists() {
        fs::create_dir_all(checkout.parent().context("checkout cache parent")?)?;
        let staging = checkout.with_extension("partial");
        if staging.exists() {
            fs::remove_dir_all(&staging)?;
        }
        let repo_arg = git_path(repo)?;
        let stage_arg = git_path(&staging)?;
        let result = (|| {
            git(
                cache,
                &[
                    "clone",
                    "--no-checkout",
                    "--shared",
                    "--template=",
                    "--",
                    &repo_arg,
                    &stage_arg,
                ],
            )?;
            git(&staging, &["checkout", "--detach", "--force", commit, "--"])?;
            fs::rename(&staging, &checkout)?;
            Ok::<(), anyhow::Error>(())
        })();
        if result.is_err() && staging.exists() {
            fs::remove_dir_all(&staging)?;
        }
        result?;
    }
    checkout.canonicalize().context("canonicalize Git checkout")
}

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn validate_commit(commit: &str) -> Result<()> {
    ensure!(
        (commit.len() == 40 || commit.len() == 64)
            && commit.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "invalid locked Git commit"
    );
    Ok(())
}

fn validate_path(path: &Path) -> Result<()> {
    ensure!(
        path.components()
            .all(|part| matches!(part, Component::Normal(_) | Component::CurDir)),
        "Git module path must stay inside the checkout"
    );
    Ok(())
}

fn validate_url(url: &str) -> Result<()> {
    ensure!(
        !url.is_empty() && !url.starts_with('-'),
        "invalid Git repository location"
    );
    if let Some((scheme, rest)) = url.split_once("://") {
        ensure!(
            matches!(scheme, "https" | "http" | "ssh" | "git" | "file"),
            "unsupported Git URL scheme"
        );
        ensure!(
            !rest.contains(['?', '#']),
            "Git URL must not contain query credentials or fragments"
        );
        let authority = rest.split('/').next().unwrap_or_default();
        if let Some((user, _)) = authority.rsplit_once('@') {
            ensure!(
                scheme == "ssh" && !user.contains([':', '%']),
                "Git URL must not contain inline credentials"
            );
        }
    }
    Ok(())
}

fn repository_url(url: &str, base: &Path) -> Result<String> {
    // SCP-like SSH syntax, including user@host:path, is not a filesystem path.
    let scp = url
        .split_once(':')
        .is_some_and(|(host, _)| !host.contains(['/', '\\']) && host.len() > 1);
    if url.contains("://") || scp {
        return Ok(url.to_owned());
    }
    let path = Path::new(url);
    let path = if path.is_absolute() {
        path.to_owned()
    } else {
        base.join(path)
    };
    git_path(
        &path
            .canonicalize()
            .context("resolve local Git repository")?,
    )
}

fn git_path(path: &Path) -> Result<String> {
    let text = path.to_str().context("non-UTF-8 Git path")?;
    #[cfg(windows)]
    {
        // Git interprets Rust's canonical \\?\C:\ prefix as SCP/SSH syntax.
        let text = if let Some(unc) = text.strip_prefix(r"\\?\UNC\") {
            format!("//{unc}")
        } else {
            text.strip_prefix(r"\\?\").unwrap_or(text).to_owned()
        };
        Ok(text.replace('\\', "/"))
    }
    #[cfg(not(windows))]
    {
        Ok(text.to_owned())
    }
}

fn git(cwd: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "protocol.ext.allow=never",
        ])
        .args(args)
        .current_dir(cwd)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GCM_INTERACTIVE", "never")
        .env("GIT_SSH_COMMAND", "ssh -oBatchMode=yes")
        .stdin(Stdio::null())
        .output()
        .context("start Git")?;
    if !output.status.success() {
        bail!(
            "Git {} failed: {}",
            args.first().copied().unwrap_or_default(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    String::from_utf8(output.stdout).context("Git returned non-UTF-8 output")
}
