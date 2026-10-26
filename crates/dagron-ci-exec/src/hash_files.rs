//! `hashFiles(…)`, as a workflow's expressions call it: one hash of the
//! job's files that its patterns match, which a cache's `key` is most often
//! made of (`hashFiles('**/Cargo.lock')`). Read as GitHub's runner reads it
//! (its `hashFiles` script, on `@actions/glob` with its defaults):
//!
//! - **The patterns** are globs ([`dagron_ci_expr::glob`]), read against
//!   each file's path under the job's workspace (`GITHUB_WORKSPACE`), `/`
//!   between its parts. An argument may hold several, a line each: each line
//!   is trimmed, and an empty one or one starting `#` is passed over. An odd
//!   number of `!` before a pattern leaves out what it matches, and the last
//!   pattern that matches a path decides. A pattern written under the
//!   workspace's own path (`${{ github.workspace }}/src`), or after `./`, is
//!   read as the rest; the workspace's own path is all of it.
//! - **A pattern names what is under it too**: `src` is `src` and
//!   `src/**`, and `!vendor` leaves out all of `vendor/`, as GitHub's
//!   implicit descendants have it. A list with nothing to include matches
//!   nothing.
//! - **The hash** is SHA-256's, in hex, of each matched file's own SHA-256
//!   one after another, the files in their paths' order. GitHub's runner
//!   makes it the same way, in the order its search finds them, so the two
//!   agree where that order is the paths': a key made here is not one
//!   GitHub's cache knows, and need not be.
//! - **No file matched**: the empty string, as on GitHub.
//! - **A symbolic link to a file** is hashed as that file, wherever it
//!   points; one to a directory is looked into only where the first
//!   argument is `--follow-symbolic-links`, and a link back up into a
//!   directory it is under is not followed round. A broken link is passed
//!   over. As on GitHub.
//! - **Only regular files** are hashed: a directory is looked into, and a
//!   pipe, a socket or a device is passed over (where GitHub's runner would
//!   wait on a pipe for ever).
//! - **Only what a pattern could reach is looked into**: a pattern's
//!   leading parts that hold no wildcard are where its search starts, as on
//!   GitHub, so `hashFiles('Cargo.lock')` reads no directory but the top,
//!   and one no pattern could reach is neither read nor an error. The step
//!   runner's own directory is passed over, where it is in the workspace.

use std::collections::BTreeSet;
use std::fs;
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};

use dagron_ci_expr::glob::Patterns;
use dagron_ci_expr::Value;
use sha2::{Digest, Sha256};

/// The argument that has links followed, first if it is there.
pub const FOLLOW_LINKS: &str = "--follow-symbolic-links";

/// `hashFiles(args…)` in `workspace`, passing over `skip` (the step
/// runner's own directory) where it is in it.
pub fn hash_files(workspace: &Path, skip: Option<&Path>, args: &[Value]) -> Result<Value, String> {
    let mut args: Vec<String> = args.iter().map(Value::text).collect();
    let follow = args.first().is_some_and(|a| a.trim() == FOLLOW_LINKS);
    if follow {
        args.remove(0);
    }
    if args.is_empty() {
        return Err("names no files: give it a pattern, as hashFiles('**/Cargo.lock')".into());
    }
    let patterns = patterns(workspace, &args);
    let includes: Vec<&String> = patterns.iter().filter(|p| !p.starts_with('!')).collect();
    if includes.is_empty() {
        return Ok(Value::from(String::new()));
    }
    let starts: Vec<Vec<String>> = includes.iter().map(|p| literal_start(p)).collect();
    let patterns = Patterns::include(&patterns)?;

    // The search goes from the workspace's real path, and passes over the
    // runner's directory by its real path: a workspace known as `.`, where
    // the working directory could not be asked, would otherwise never be the
    // directory the runner made. Patterns are read against the path as it
    // is written, which is `github.workspace`'s.
    let root = workspace
        .canonicalize()
        .unwrap_or_else(|_| workspace.to_path_buf());
    let skip = skip.map(|dir| dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf()));
    let mut walk = Walk {
        follow,
        patterns: &patterns,
        starts: &starts,
        skip: skip.as_deref(),
        files: BTreeSet::new(),
        above: Vec::new(),
    };
    walk.dir(&root, &[])?;
    if walk.files.is_empty() {
        return Ok(Value::from(String::new()));
    }
    let mut outer = Sha256::new();
    for (_, path) in &walk.files {
        outer.update(file_hash(path)?);
    }
    Ok(Value::from(hex(&outer.finalize())))
}

/// The patterns `args` hold, as the glob matcher reads them: a line each,
/// trimmed, without blanks and `#` comments, `!` once where an odd number
/// of them led, relative to the workspace, and each with its descendants.
fn patterns(workspace: &Path, args: &[String]) -> Vec<String> {
    let own = workspace.display().to_string();
    let own = own.trim_end_matches('/');
    let mut out = Vec::new();
    for line in args.iter().flat_map(|arg| arg.split('\n')) {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let bangs = line.chars().take_while(|c| *c == '!').count();
        let bang = if bangs % 2 == 1 { "!" } else { "" };
        let mut rest = &line[bangs..];
        if rest == own {
            rest = "";
        } else if let Some(under) = rest.strip_prefix(own).and_then(|r| r.strip_prefix('/')) {
            rest = under;
        }
        while let Some(under) = rest.strip_prefix("./") {
            rest = under;
        }
        let rest = rest.trim_end_matches('/');
        let rest = if rest == "." { "" } else { rest };
        if rest.is_empty() {
            out.push(format!("{bang}**"));
            continue;
        }
        out.push(format!("{bang}{rest}"));
        if !rest.ends_with("**") {
            out.push(format!("{bang}{rest}/**"));
        }
    }
    out
}

/// The parts of `pattern` before its first that holds a wildcard: where its
/// search starts. All of them, for a pattern with none.
fn literal_start(pattern: &str) -> Vec<String> {
    pattern
        .split('/')
        .take_while(|part| !part.contains(['*', '?', '[', '\\']))
        .map(str::to_string)
        .collect()
}

/// A search of the workspace: what it found, and how it goes.
struct Walk<'a> {
    follow: bool,
    patterns: &'a Patterns,
    /// Where each include pattern's search starts ([`literal_start`]).
    starts: &'a [Vec<String>],
    skip: Option<&'a Path>,
    /// Each regular file matched, by its path under the workspace.
    files: BTreeSet<(String, PathBuf)>,
    /// The real path of each directory being looked into, where links are
    /// followed: a link back up one of them is not followed round.
    above: Vec<PathBuf>,
}

impl Walk<'_> {
    /// Whether a directory at `parts` under the workspace could hold what
    /// a pattern matches: one of them starts its search there, under it, or
    /// above it.
    fn reaches(&self, parts: &[String]) -> bool {
        self.starts.iter().any(|start| {
            let n = start.len().min(parts.len());
            start[..n] == parts[..n]
        })
    }

    fn dir(&mut self, dir: &Path, parts: &[String]) -> Result<(), String> {
        if self.follow {
            let real = dir
                .canonicalize()
                .map_err(|e| format!("{}: {e}", dir.display()))?;
            if self.above.contains(&real) {
                return Ok(());
            }
            self.above.push(real);
        }
        let entries = fs::read_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        for entry in entries {
            let entry = entry.map_err(|e| format!("{}: {e}", dir.display()))?;
            let path = entry.path();
            if self.skip == Some(path.as_path()) {
                continue;
            }
            let mut at = parts.to_vec();
            at.push(entry.file_name().to_string_lossy().into_owned());
            let kind = match entry.file_type() {
                Ok(kind) => kind,
                // Gone since it was listed.
                Err(e) if e.kind() == ErrorKind::NotFound => continue,
                Err(e) => return Err(format!("{}: {e}", path.display())),
            };
            let (is_dir, is_file) = if kind.is_symlink() {
                match fs::metadata(&path) {
                    // A link to a directory is looked into where links are
                    // followed; one to a file is that file.
                    Ok(meta) => (meta.is_dir() && self.follow, meta.is_file()),
                    // A broken link, or one gone since it was listed.
                    Err(_) => continue,
                }
            } else {
                (kind.is_dir(), kind.is_file())
            };
            if is_dir {
                if self.reaches(&at) {
                    self.dir(&path, &at)?;
                }
            } else if is_file {
                let name = at.join("/");
                if self.patterns.matches(&name) {
                    self.files.insert((name, path));
                }
            }
        }
        if self.follow {
            self.above.pop();
        }
        Ok(())
    }
}

/// The SHA-256 of the file at `path`.
fn file_hash(path: &Path) -> Result<[u8; 32], String> {
    let mut file = fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file
            .read(&mut buf)
            .map_err(|e| format!("{}: {e}", path.display()))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().into())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A workspace whose files are made in the reverse of their paths'
    /// order, so that a search that kept the order it found them in would
    /// not happen to agree with their paths'. `a-b/` and `a/` are where the
    /// order of whole paths (`a-b/…` before `a/…`) and of each directory's
    /// names sorted one level at a time (`a/` before `a-b/`) part.
    fn tree() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let at = |p: &str, text: &str| {
            let path = dir.path().join(p);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, text).unwrap();
        };
        at("crates/b/src/lib.rs", "fn b() {}");
        at("crates/b/Cargo.lock", "b lock");
        at("crates/a/Cargo.lock", "a lock");
        at("crates/a-b/Cargo.lock", "a-b lock");
        at("Cargo.lock", "root lock");
        at(".hidden/Cargo.lock", "hidden lock");
        dir
    }

    fn hash(dir: &Path, args: &[&str]) -> Result<String, String> {
        hash_skipping(dir, None, args)
    }

    fn hash_skipping(dir: &Path, skip: Option<&Path>, args: &[&str]) -> Result<String, String> {
        let args: Vec<Value> = args.iter().map(|a| Value::from(a.to_string())).collect();
        hash_files(dir, skip, &args).map(|v| v.text())
    }

    /// What the hash is made of, by hand: each file's SHA-256, one after
    /// another in the order given, hashed again.
    fn expected(texts: &[&str]) -> String {
        let mut outer = Sha256::new();
        for text in texts {
            outer.update(Sha256::digest(text.as_bytes()));
        }
        outer
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    #[test]
    fn the_hash_is_of_each_matched_file_s_hash_in_their_paths_order() {
        let dir = tree();
        // `.hidden/…`, `Cargo.lock`, `crates/a-b/…`, `crates/a/…`,
        // `crates/b/…`: whole paths' order, `-` before `/`.
        assert_eq!(
            hash(dir.path(), &["**/Cargo.lock"]).unwrap(),
            expected(&["hidden lock", "root lock", "a-b lock", "a lock", "b lock"])
        );
        assert_eq!(
            hash(dir.path(), &["Cargo.lock"]).unwrap(),
            expected(&["root lock"]),
            "a pattern without ** names the top's alone"
        );
        // Two patterns that both match a file hash it once.
        assert_eq!(
            hash(dir.path(), &["crates/a/Cargo.lock", "crates/a/*"]).unwrap(),
            expected(&["a lock"])
        );
    }

    /// A pattern names what is under it too, as GitHub's implicit
    /// descendants have it: a directory's name is all of it, and `!` before
    /// one leaves all of it out.
    #[test]
    fn a_directory_s_name_is_all_of_it() {
        let dir = tree();
        assert_eq!(
            hash(dir.path(), &["crates/b"]).unwrap(),
            expected(&["b lock", "fn b() {}"])
        );
        assert_eq!(
            hash(dir.path(), &["crates/b/"]).unwrap(),
            expected(&["b lock", "fn b() {}"])
        );
        assert_eq!(
            hash(dir.path(), &["**/Cargo.lock", "!crates", "!.hidden"]).unwrap(),
            expected(&["root lock"])
        );
        // The workspace's own path, `.` and `./` are all of it.
        let ws = dir.path().display().to_string();
        let all = hash(dir.path(), &["**"]).unwrap();
        assert_eq!(hash(dir.path(), &[&ws]).unwrap(), all);
        assert_eq!(hash(dir.path(), &["."]).unwrap(), all);
        assert_eq!(hash(dir.path(), &["./"]).unwrap(), all);
    }

    #[test]
    fn a_bang_leaves_out_and_nothing_matched_is_empty() {
        let dir = tree();
        assert_eq!(
            hash(dir.path(), &["**/Cargo.lock", "!crates/**", "!.hidden/**"]).unwrap(),
            expected(&["root lock"])
        );
        assert_eq!(hash(dir.path(), &["**/*.toml"]).unwrap(), "");
        // Nothing to include: nothing matched, as on GitHub.
        assert_eq!(hash(dir.path(), &["!x"]).unwrap(), "");
        assert_eq!(hash(dir.path(), &["# only a comment"]).unwrap(), "");
        let err = hash(dir.path(), &[]).unwrap_err();
        assert!(err.contains("names no files"), "{err}");
    }

    /// An argument may hold several patterns, a line each, trimmed, with
    /// blanks and `#` comments passed over; an even number of `!` is none.
    #[test]
    fn patterns_are_read_a_line_each_as_github_reads_them() {
        let dir = tree();
        assert_eq!(
            hash(
                dir.path(),
                &["  Cargo.lock \n\n# the crates'\n crates/a/Cargo.lock\n!!crates/b/Cargo.lock"]
            )
            .unwrap(),
            expected(&["root lock", "a lock", "b lock"])
        );
        assert_eq!(
            hash(dir.path(), &["**/Cargo.lock\n!!!crates\n!.hidden"]).unwrap(),
            expected(&["root lock"]),
            "three ! leave out"
        );
    }

    #[test]
    fn a_pattern_under_the_workspace_s_own_path_reads_as_the_rest() {
        let dir = tree();
        let ws = dir.path().display().to_string();
        assert_eq!(
            hash(dir.path(), &[&format!("{ws}/crates/a/Cargo.lock")]).unwrap(),
            expected(&["a lock"])
        );
        assert_eq!(
            hash(dir.path(), &["./crates/b/src/*.rs"]).unwrap(),
            expected(&["fn b() {}"])
        );
    }

    #[test]
    fn a_change_to_a_matched_file_changes_the_hash_and_another_does_not() {
        let dir = tree();
        let before = hash(dir.path(), &["**/Cargo.lock"]).unwrap();
        fs::write(dir.path().join("crates/b/src/lib.rs"), "fn b() { 1 }").unwrap();
        assert_eq!(hash(dir.path(), &["**/Cargo.lock"]).unwrap(), before);
        fs::write(dir.path().join("crates/a/Cargo.lock"), "a lock, changed").unwrap();
        assert_ne!(hash(dir.path(), &["**/Cargo.lock"]).unwrap(), before);
    }

    /// A link to a file is that file, wherever it points; a link to a
    /// directory is looked into only where links are followed; a broken
    /// one is passed over. As GitHub's runner has it.
    #[cfg(unix)]
    #[test]
    fn links_are_read_as_github_reads_them() {
        use std::os::unix::fs::symlink;
        let dir = tree();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("Cargo.lock"), "outside lock").unwrap();
        fs::create_dir_all(dir.path().join("shared")).unwrap();
        let shared = dir.path().join("shared");
        symlink(dir.path().join("Cargo.lock"), shared.join("one.lock")).unwrap();
        symlink(outside.path().join("Cargo.lock"), shared.join("two.lock")).unwrap();
        symlink(dir.path().join("no-such-file"), shared.join("broken.lock")).unwrap();
        assert_eq!(
            hash(dir.path(), &["shared/*.lock"]).unwrap(),
            expected(&["root lock", "outside lock"]),
            "links to files are hashed as those files; a broken one is passed over"
        );

        symlink(dir.path().join("crates/a"), dir.path().join("linked")).unwrap();
        symlink(outside.path(), dir.path().join("away")).unwrap();
        assert_eq!(
            hash(dir.path(), &["linked/**", "away/**"]).unwrap(),
            "",
            "links to directories are not looked into"
        );
        assert_eq!(
            hash(dir.path(), &[FOLLOW_LINKS, "linked/**", "away/**"]).unwrap(),
            expected(&["outside lock", "a lock"]),
            "followed, and named by the path they are reached at"
        );
    }

    /// A link back up to a directory it is under is not followed round:
    /// every file is found once under each path that does not go round.
    /// Without the guard, the search would go down `crates/up/crates/up/…`
    /// until the system stops it, and find each lock again at each level.
    #[cfg(unix)]
    #[test]
    fn a_link_back_up_is_not_followed_round() {
        let dir = tree();
        std::os::unix::fs::symlink(dir.path(), dir.path().join("crates/up")).unwrap();
        assert_eq!(
            hash(dir.path(), &[FOLLOW_LINKS, "**/Cargo.lock"]).unwrap(),
            expected(&["hidden lock", "root lock", "a-b lock", "a lock", "b lock"])
        );
    }

    /// A pipe in the workspace is passed over: opened, it would wait for a
    /// writer for ever. Only regular files are hashed.
    #[cfg(unix)]
    #[test]
    fn a_pipe_is_passed_over() {
        let dir = tree();
        let fifo = dir.path().join("crates/a/pipe.lock");
        let c = std::ffi::CString::new(fifo.display().to_string()).unwrap();
        // SAFETY: a valid C string and mode.
        assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o644) }, 0);
        assert_eq!(
            hash(dir.path(), &["crates/a/*.lock"]).unwrap(),
            expected(&["a lock"])
        );
    }

    /// A workspace known by a path that is not its real one (`.`, where the
    /// working directory could not be asked, or one through a link) is
    /// searched from its real path, and the runner's directory is passed
    /// over all the same.
    #[cfg(unix)]
    #[test]
    fn a_workspace_by_another_path_is_searched_from_its_real_one() {
        let dir = tree();
        let own = dir.path().join("dagron-ci-1");
        fs::create_dir_all(&own).unwrap();
        fs::write(own.join("Cargo.lock"), "the runner's").unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        let alias = elsewhere.path().join("ws");
        std::os::unix::fs::symlink(dir.path(), &alias).unwrap();
        assert_eq!(
            hash_skipping(
                &alias,
                Some(&own.canonicalize().unwrap()),
                &["*/Cargo.lock"]
            )
            .unwrap(),
            expected(&["hidden lock"])
        );
    }

    /// The step runner's own directory, where it is in the workspace, is
    /// passed over: what it holds changes from one step to the next.
    #[test]
    fn the_runner_s_own_directory_is_passed_over() {
        let dir = tree();
        let own = dir.path().join("dagron-ci-1");
        fs::create_dir_all(&own).unwrap();
        fs::write(own.join("Cargo.lock"), "the runner's").unwrap();
        assert_eq!(
            hash_skipping(dir.path(), Some(&own), &["*/Cargo.lock", "Cargo.lock"]).unwrap(),
            expected(&["hidden lock", "root lock"])
        );
        assert_eq!(
            hash(dir.path(), &["*/Cargo.lock", "Cargo.lock"]).unwrap(),
            expected(&["hidden lock", "root lock", "the runner's"]),
            "not passed over where it is not named"
        );
    }

    /// A directory no pattern could reach is not read: one that cannot be
    /// read is no error where no pattern needs it.
    #[cfg(unix)]
    #[test]
    fn what_no_pattern_reaches_is_not_read() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tree();
        let shut = dir.path().join("shut");
        fs::create_dir_all(shut.join("inside")).unwrap();
        fs::set_permissions(&shut, fs::Permissions::from_mode(0o000)).unwrap();
        let readable = fs::read_dir(&shut).is_ok();
        let got = hash(dir.path(), &["crates/a/Cargo.lock"]);
        fs::set_permissions(&shut, fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(got.unwrap(), expected(&["a lock"]));
        // Root reads it all the same; elsewhere, one that must be read is
        // an error that names it.
        if !readable {
            fs::set_permissions(&shut, fs::Permissions::from_mode(0o000)).unwrap();
            let err = hash(dir.path(), &["**/Cargo.lock"]).unwrap_err();
            fs::set_permissions(&shut, fs::Permissions::from_mode(0o755)).unwrap();
            assert!(err.contains("shut"), "{err}");
        }
    }
}
