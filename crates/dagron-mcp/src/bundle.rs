//! `dagron_ci_run`'s `bundle_from`: a patch bundle of a git working tree, as
//! dagron-ci's run API takes one (`POST /ci/v1/bundles`), for a run of a
//! change the agent has not pushed.
//!
//! The same bundle `dagron-ci run --remote --bundle-from` makes: a tar of the
//! files the tree changes or adds since the base commit, as they are in the
//! tree, and at its top a `.dagron-deleted` naming those it deletes. The
//! change is everything the tree has that the base has not: commits since
//! it, what is staged and what is not, and untracked files git does not
//! ignore. Its entries are sorted, with no times or owners, so the same
//! change is the same bytes.
//!
//! **Only under the worktree.** This server reads no file of the machine
//! otherwise, so a bundle is made only of a working tree whose top is under
//! `DAGRON_MCP_WORKTREE` where that is set, and otherwise under the
//! directory it was started in.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{bail, Context, Result};

/// The file at a bundle's top that names the files it deletes.
const DELETED: &str = ".dagron-deleted";
/// The largest bundle dagron-ci takes as a request's body.
pub(crate) const MAX_BUNDLE: usize = 32 * 1024 * 1024;

/// A bundle of a working tree's change.
#[derive(Debug)]
pub(crate) struct Made {
    /// The base commit's full id.
    pub base: String,
    /// The tar.
    pub bytes: Vec<u8>,
    /// How many files it carries, and how many it deletes.
    pub files: usize,
    pub deleted: usize,
}

/// `git ARGS` at `top`: its standard output.
fn git(top: &Path, args: &[&str]) -> Result<Vec<u8>> {
    let out = Command::new("git")
        .args(args)
        .current_dir(top)
        .stdin(Stdio::null())
        .output()
        .context("git could not be run")?;
    if !out.status.success() {
        bail!(
            "git {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(out.stdout)
}

/// The `-z` list git printed, each entry as UTF-8.
fn entries(out: &[u8]) -> Result<Vec<String>> {
    out.split(|b| *b == 0)
        .filter(|e| !e.is_empty())
        .map(|e| {
            String::from_utf8(e.to_vec()).map_err(|_| {
                anyhow::anyhow!(
                    "{:?} is not a UTF-8 path, which a bundle cannot name",
                    String::from_utf8_lossy(e)
                )
            })
        })
        .collect()
}

/// Make the bundle of the change in the working tree at `dir`, which must be
/// under `root`, since `base` (`HEAD` when none is given).
pub(crate) fn make(root: &Path, dir: &str, base: Option<&str>) -> Result<Made> {
    let root = root
        .canonicalize()
        .with_context(|| format!("the worktree {} cannot be read", root.display()))?;
    let at = root.join(dir);
    let at = at
        .canonicalize()
        .with_context(|| format!("`bundle_from` {dir:?} is no directory"))?;
    if !at.starts_with(&root) {
        bail!(
            "`bundle_from` {dir:?} is not under {}, the one directory a bundle is made from \
             (DAGRON_MCP_WORKTREE)",
            root.display()
        );
    }
    let top = git(&at, &["rev-parse", "--show-toplevel"])
        .with_context(|| format!("`bundle_from` {dir:?} is not in a git working tree"))?;
    let top = PathBuf::from(String::from_utf8_lossy(&top).trim())
        .canonicalize()
        .context("the working tree's top cannot be read")?;
    // Every file the bundle could carry is under the tree's top.
    if !top.starts_with(&root) {
        bail!(
            "`bundle_from` {dir:?} is in a working tree whose top, {}, is not under {} \
             (DAGRON_MCP_WORKTREE)",
            top.display(),
            root.display()
        );
    }
    let named = base.unwrap_or("HEAD");
    let base = git(
        &top,
        &[
            "rev-parse",
            "--verify",
            "--end-of-options",
            &format!("{named}^{{commit}}"),
        ],
    )
    .with_context(|| format!("the base {named:?} is no commit"))?;
    let base = String::from_utf8_lossy(&base).trim().to_string();

    let listed = entries(&git(
        &top,
        &["diff", "--name-status", "--no-renames", "-z", &base, "--"],
    )?)?;
    let mut changed = BTreeSet::new();
    let mut deleted = BTreeSet::new();
    for pair in listed.chunks(2) {
        let [status, path] = pair else {
            bail!("git diff listed a status with no path");
        };
        match status.as_str() {
            "A" | "M" | "T" => changed.insert(path.clone()),
            "D" => deleted.insert(path.clone()),
            "U" => bail!("{path} has a merge conflict: resolve it first"),
            other => bail!("{path}: git says {other:?} of it, which a bundle cannot carry"),
        };
    }
    for path in entries(&git(
        &top,
        &["ls-files", "--others", "--exclude-standard", "-z"],
    )?)? {
        changed.insert(path);
    }
    // A file the index no longer tracks but the tree still has is the tree's.
    deleted.retain(|path| !changed.contains(path));
    if changed.contains(DELETED) || deleted.contains(DELETED) {
        bail!("{DELETED} is the bundle's own: the change cannot touch a file of that name");
    }
    // DELETED lists a path a line: one with a newline in it would be read as
    // two, and the second deleted.
    if let Some(path) = changed.iter().chain(&deleted).find(|p| p.contains('\n')) {
        bail!("{path:?} has a newline in its name, which {DELETED} cannot list");
    }
    if changed.is_empty() && deleted.is_empty() {
        bail!("{dir:?} has no change since {base}: there is nothing to run");
    }

    let mut files = BTreeMap::new();
    // The tar is not compressed, so it is at least the files it carries: one
    // past MAX_BUNDLE is said so before they are read.
    let mut size: u64 = 0;
    for path in &changed {
        let file = top.join(path);
        let meta = std::fs::symlink_metadata(&file).with_context(|| path.clone())?;
        if meta.file_type().is_symlink() {
            bail!("{path} is a link: a bundle carries files, not links");
        }
        if !meta.is_file() {
            bail!("{path} is not a file (a submodule?): a bundle carries files");
        }
        size = size.saturating_add(meta.len());
        if size > MAX_BUNDLE as u64 {
            bail!(
                "the change comes to more than the {MAX_BUNDLE} bytes a bundle may be: push it to \
                 a branch and run that"
            );
        }
        let bytes = std::fs::read(&file).with_context(|| path.clone())?;
        files.insert(path.clone(), (bytes, executable(&meta)));
    }

    let mut tar = tar::Builder::new(Vec::new());
    let mut add = |path: &str, bytes: &[u8], mode: u32| -> Result<()> {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_size(bytes.len() as u64);
        header.set_mode(mode);
        header.set_mtime(0);
        header.set_uid(0);
        header.set_gid(0);
        tar.append_data(&mut header, path, bytes)
            .with_context(|| format!("{path} cannot be put in the bundle"))
    };
    if !deleted.is_empty() {
        let list: String = deleted.iter().map(|p| format!("{p}\n")).collect();
        add(DELETED, list.as_bytes(), 0o644)?;
    }
    for (path, (bytes, exec)) in &files {
        add(path, bytes, if *exec { 0o755 } else { 0o644 })?;
    }
    let bytes = tar.into_inner().context("the bundle cannot be made")?;
    if bytes.len() > MAX_BUNDLE {
        bail!(
            "the change comes to {} bytes, more than the {MAX_BUNDLE} a bundle may be: push it to a \
             branch and run that",
            bytes.len()
        );
    }
    Ok(Made {
        base,
        bytes,
        files: files.len(),
        deleted: deleted.len(),
    })
}

#[cfg(unix)]
fn executable(meta: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    meta.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn executable(_: &std::fs::Metadata) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn git_in(dir: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .args([
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@t",
                "-c",
                "commit.gpgsign=false",
            ])
            .args(args)
            .current_dir(dir)
            .output()
            .unwrap();
        assert!(out.status.success(), "git {args:?}");
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    }

    fn names(bytes: &[u8]) -> Vec<(String, String)> {
        let mut archive = tar::Archive::new(bytes);
        archive
            .entries()
            .unwrap()
            .map(|e| {
                let mut e = e.unwrap();
                let path = e.path().unwrap().to_string_lossy().to_string();
                let mut text = String::new();
                e.read_to_string(&mut text).unwrap();
                (path, text)
            })
            .collect()
    }

    #[test]
    fn a_tree_under_the_worktree_is_bundled_and_no_other() {
        let root = tempfile::tempdir().unwrap();
        let repo = root.path().join("repo");
        std::fs::create_dir_all(repo.join("src")).unwrap();
        git_in(&repo, &["init", "-q", "-b", "main"]);
        std::fs::write(repo.join("src/a.rs"), "a\n").unwrap();
        std::fs::write(repo.join("b.rs"), "b\n").unwrap();
        git_in(&repo, &["add", "-A"]);
        git_in(&repo, &["commit", "-qm", "base"]);
        let head = git_in(&repo, &["rev-parse", "HEAD"]);
        std::fs::write(repo.join("src/a.rs"), "a2\n").unwrap();
        std::fs::remove_file(repo.join("b.rs")).unwrap();
        std::fs::write(repo.join("new.rs"), "n\n").unwrap();

        let made = make(root.path(), "repo/src", None).unwrap();
        assert_eq!(made.base, head);
        assert_eq!((made.files, made.deleted), (2, 1));
        assert_eq!(
            names(&made.bytes),
            [
                (DELETED.to_string(), "b.rs\n".to_string()),
                ("new.rs".to_string(), "n\n".to_string()),
                ("src/a.rs".to_string(), "a2\n".to_string()),
            ]
        );
        assert_eq!(
            make(root.path(), "repo", Some(&head)).unwrap().bytes,
            made.bytes
        );

        // Outside the worktree, by `..` or from the top, is refused.
        let inner = repo.join("src");
        for dir in ["..", "../.."] {
            let err = make(&inner, dir, None).unwrap_err().to_string();
            assert!(err.contains("is not under"), "{dir}: {err}");
        }
        let err = make(&inner, "/", None).unwrap_err().to_string();
        assert!(err.contains("is not under"), "{err}");
        // Inside the worktree, but in a tree whose top is above it.
        let err = make(&inner, ".", None).unwrap_err().to_string();
        assert!(err.contains("whose top"), "{err}");
        let err = make(root.path(), ".", None).unwrap_err().to_string();
        assert!(err.contains("not in a git working tree"), "{err:#}");
        git_in(&repo, &["checkout", "-q", "--", "."]);
        git_in(&repo, &["clean", "-qfd"]);
        let err = make(root.path(), "repo", None).unwrap_err().to_string();
        assert!(err.contains("no change since"), "{err}");
    }
}
