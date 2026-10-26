//! `run --remote --bundle-from DIR`: a patch bundle (`crate::bundles`) made
//! of a git working tree, for a run of a change nobody pushed.
//!
//! The base is a commit of the repository's, `--sha` (any revision git
//! names) or the tree's `HEAD`; the forge must have it on `--ref`. The
//! change is everything the tree has that the base has not: commits since
//! it, what is staged and what is not, and untracked files git does not
//! ignore. The bundle carries each file it changes or adds, as it is in the
//! tree, and names those it deletes in `.dagron-deleted`.
//!
//! The tar is made the same way for the same change: its entries sorted,
//! with no times or owners. Its SHA-256, which the server answers with and a
//! run on it shows, names the change.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::bundles::{DELETED, MAX_INLINE};

/// A bundle of a working tree's change.
#[derive(Debug)]
pub(super) struct Made {
    /// The base commit's full id.
    pub base: String,
    /// The bundle: a tar, or the tar compressed with zstd where the tar is
    /// more than a request's body may be.
    pub bytes: Vec<u8>,
    /// How many files it carries, and how many it deletes.
    pub files: usize,
    pub deleted: usize,
}

/// `git ARGS` at `top`: its standard output, or what it said went wrong.
fn git(top: &Path, args: &[&str]) -> Result<Vec<u8>, String> {
    let out = Command::new("git")
        .args(args)
        .current_dir(top)
        .stdin(Stdio::null())
        .output()
        .map_err(|e| format!("git could not be run: {e}"))?;
    if !out.status.success() {
        let said = String::from_utf8_lossy(&out.stderr);
        return Err(format!("git {}: {}", args.join(" "), said.trim()));
    }
    Ok(out.stdout)
}

/// The `-z` list git printed, each entry as UTF-8.
fn entries(out: &[u8]) -> Result<Vec<String>, String> {
    out.split(|b| *b == 0)
        .filter(|e| !e.is_empty())
        .map(|e| {
            String::from_utf8(e.to_vec()).map_err(|_| {
                format!(
                    "{:?} is not a UTF-8 path, which a bundle cannot name",
                    String::from_utf8_lossy(e)
                )
            })
        })
        .collect()
}

/// Make the bundle of the change in the working tree at `dir` since `base`
/// (`HEAD` when none is given).
pub(super) fn make(dir: &str, base: Option<&str>) -> Result<Made, String> {
    let top = git(Path::new(dir), &["rev-parse", "--show-toplevel"])
        .map_err(|e| format!("{dir} is not in a git working tree: {e}"))?;
    let top = PathBuf::from(String::from_utf8_lossy(&top).trim());
    let rev = format!("{}^{{commit}}", base.unwrap_or("HEAD"));
    let base = git(&top, &["rev-parse", "--verify", "--end-of-options", &rev])
        .map_err(|e| format!("the base {:?} is no commit: {e}", base.unwrap_or("HEAD")))?;
    let base = String::from_utf8_lossy(&base).trim().to_string();

    // The tree against the base, file by file: what it changes or adds, and
    // what it deletes. A path is relative to the tree's top.
    let listed = entries(&git(
        &top,
        &["diff", "--name-status", "--no-renames", "-z", &base, "--"],
    )?)?;
    let mut changed = BTreeSet::new();
    let mut deleted = BTreeSet::new();
    for pair in listed.chunks(2) {
        let [status, path] = pair else {
            return Err("git diff listed a status with no path".into());
        };
        match status.as_str() {
            "A" | "M" | "T" => changed.insert(path.clone()),
            "D" => deleted.insert(path.clone()),
            "U" => return Err(format!("{path} has a merge conflict: resolve it first")),
            other => {
                return Err(format!(
                    "{path}: git says {other:?} of it, which a bundle cannot carry"
                ))
            }
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
        return Err(format!(
            "{DELETED} is the bundle's own: the change cannot touch a file of that name"
        ));
    }
    // DELETED lists a path a line: one with a newline in it would be read as
    // two, and the second deleted.
    if let Some(path) = changed.iter().chain(&deleted).find(|p| p.contains('\n')) {
        return Err(format!(
            "{path:?} has a newline in its name, which {DELETED} cannot list"
        ));
    }
    if changed.is_empty() && deleted.is_empty() {
        return Err(format!(
            "{dir} has no change since {base}: there is nothing to run"
        ));
    }

    let mut files = BTreeMap::new();
    for path in &changed {
        let at = top.join(path);
        let meta = std::fs::symlink_metadata(&at).map_err(|e| format!("{path}: {e}"))?;
        if meta.file_type().is_symlink() {
            return Err(format!(
                "{path} is a link: a bundle carries files, not links"
            ));
        }
        if !meta.is_file() {
            return Err(format!(
                "{path} is not a file (a submodule?): a bundle carries files"
            ));
        }
        let bytes = std::fs::read(&at).map_err(|e| format!("{path}: {e}"))?;
        files.insert(path.clone(), (bytes, executable(&meta)));
    }

    let mut tar = tar::Builder::new(Vec::new());
    let mut add = |path: &str, bytes: &[u8], mode: u32| {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_size(bytes.len() as u64);
        header.set_mode(mode);
        header.set_mtime(0);
        header.set_uid(0);
        header.set_gid(0);
        tar.append_data(&mut header, path, bytes)
            .map_err(|e| format!("{path} cannot be put in the bundle: {e}"))
    };
    if !deleted.is_empty() {
        let list: String = deleted.iter().map(|p| format!("{p}\n")).collect();
        add(DELETED, list.as_bytes(), 0o644)?;
    }
    for (path, (bytes, exec)) in &files {
        add(path, bytes, if *exec { 0o755 } else { 0o644 })?;
    }
    let tar = tar
        .into_inner()
        .map_err(|e| format!("the bundle cannot be made: {e}"))?;
    let bytes = if tar.len() > MAX_INLINE {
        zstd::stream::encode_all(tar.as_slice(), 3)
            .map_err(|e| format!("the bundle cannot be compressed: {e}"))?
    } else {
        tar
    };
    if bytes.len() > MAX_INLINE {
        return Err(format!(
            "the change comes to {} bytes compressed, more than the {MAX_INLINE} a bundle sent here may be",
            bytes.len()
        ));
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

    fn sh(dir: &Path, args: &[&str]) {
        let ok = Command::new("git")
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
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success();
        assert!(ok, "git {args:?}");
    }

    fn read(bytes: &[u8]) -> BTreeMap<String, (String, u32)> {
        let mut out = BTreeMap::new();
        let mut archive = tar::Archive::new(bytes);
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let path = entry.path().unwrap().to_string_lossy().to_string();
            let mode = entry.header().mode().unwrap();
            let mut text = String::new();
            entry.read_to_string(&mut text).unwrap();
            out.insert(path, (text, mode));
        }
        out
    }

    /// The change since the base is committed, staged, left in the tree or
    /// untracked, alike; an ignored file is not carried; a deleted one is
    /// named; and the server reads the bundle as what it is.
    #[test]
    fn a_working_tree_s_change_since_its_base_is_bundled() {
        let dir = tempfile::tempdir().unwrap();
        let top = dir.path();
        sh(top, &["init", "-q", "-b", "main"]);
        std::fs::create_dir_all(top.join("src")).unwrap();
        for (path, text) in [
            ("src/lib.rs", "a\n"),
            ("src/gone.rs", "g\n"),
            ("README.md", "r\n"),
            (".gitignore", "target/\n"),
        ] {
            std::fs::write(top.join(path), text).unwrap();
        }
        sh(top, &["add", "-A"]);
        sh(top, &["commit", "-qm", "base"]);
        let base = String::from_utf8(git(top, &["rev-parse", "HEAD"]).unwrap()).unwrap();

        // Committed since, staged, changed in the tree, and new.
        std::fs::write(top.join("README.md"), "r2\n").unwrap();
        sh(top, &["commit", "-qam", "after"]);
        std::fs::write(top.join("src/lib.rs"), "b\n").unwrap();
        sh(top, &["rm", "-q", "src/gone.rs"]);
        std::fs::write(top.join("run.sh"), "#!/bin/sh\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(top.join("run.sh"), std::fs::Permissions::from_mode(0o755))
                .unwrap();
        }
        std::fs::create_dir_all(top.join("target")).unwrap();
        std::fs::write(top.join("target/out"), "x").unwrap();

        // From a directory inside the tree, with the base named as git would.
        let sub = top.join("src");
        let made = make(sub.to_str().unwrap(), Some(base.trim())).unwrap();
        assert_eq!(made.base, base.trim());
        assert_eq!((made.files, made.deleted), (3, 1));
        let got = read(&made.bytes);
        assert_eq!(
            got.keys().map(String::as_str).collect::<Vec<_>>(),
            [DELETED, "README.md", "run.sh", "src/lib.rs"],
            "the deleted list first, then the files in order, and no ignored file"
        );
        assert_eq!(got[DELETED].0, "src/gone.rs\n");
        assert_eq!(got["src/lib.rs"], ("b\n".to_string(), 0o644));
        if cfg!(unix) {
            assert_eq!(got["run.sh"].1, 0o755);
        }
        crate::bundles::read(&made.bytes).expect("a bundle the server takes");

        // The same change is the same bytes.
        let again = make(top.to_str().unwrap(), Some(base.trim())).unwrap();
        assert_eq!(again.bytes, made.bytes);

        // HEAD is the base by default: what was committed since is not carried.
        let from_head = read(&make(top.to_str().unwrap(), None).unwrap().bytes);
        assert!(!from_head.contains_key("README.md"), "{from_head:?}");
        assert!(from_head.contains_key("src/lib.rs"));
    }

    #[test]
    fn what_no_bundle_carries_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let top = dir.path();
        assert!(make(top.to_str().unwrap(), None)
            .unwrap_err()
            .contains("not in a git working tree"));
        sh(top, &["init", "-q", "-b", "main"]);
        std::fs::write(top.join("a"), "a\n").unwrap();
        sh(top, &["add", "-A"]);
        sh(top, &["commit", "-qm", "base"]);
        let err = make(top.to_str().unwrap(), None).unwrap_err();
        assert!(err.contains("no change since"), "{err}");
        let err = make(top.to_str().unwrap(), Some("nope")).unwrap_err();
        assert!(err.contains("is no commit"), "{err}");
        std::fs::write(top.join(DELETED), "x\n").unwrap();
        let err = make(top.to_str().unwrap(), None).unwrap_err();
        assert!(err.contains("the bundle's own"), "{err}");
        std::fs::remove_file(top.join(DELETED)).unwrap();
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("a", top.join("link")).unwrap();
            let err = make(top.to_str().unwrap(), None).unwrap_err();
            assert!(err.contains("is a link"), "{err}");
        }
    }
}
