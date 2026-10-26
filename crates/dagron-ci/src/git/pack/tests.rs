//! The pack reader held to git itself: a repository made with the system's
//! git, packs its own server and `git pack-objects` write, and what `git
//! diff` lists for the same two commits. Each test is skipped where there is
//! no git. The image has none; the oracle is for tests alone.

use std::io::Write as _;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::time::Duration;

use super::*;
use crate::git::{self, Advertisement};

/// Limits no test's pack comes near.
pub(crate) const ROOMY: Limits = Limits {
    objects: 100_000,
    inflated: 64 << 20,
    delta_depth: 100,
    tree_depth: 64,
    walked: 64 << 20,
    paths: 100_000,
    path_len: 4096,
    listed: 16 << 20,
    until: None,
};

/// `git` with no configuration but the repository's own.
fn git_command(dir: &Path) -> Command {
    let mut git = Command::new("git");
    git.current_dir(dir)
        .args(["-c", "user.email=t@example.com", "-c", "user.name=t"])
        .args([
            "-c",
            "commit.gpgsign=false",
            "-c",
            "init.defaultBranch=main",
        ])
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null");
    git
}

/// Run `git args` in `dir` with `input` on its standard input, however it
/// ends.
fn run_git(dir: &Path, args: &[&str], input: &[u8], env: &[(&str, &str)]) -> Output {
    let mut child = git_command(dir)
        .args(args)
        .envs(env.iter().copied())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("git");
    // Written whole before anything is read: each command here reads all
    // of what it is given before it answers.
    child.stdin.take().unwrap().write_all(input).unwrap();
    child.wait_with_output().unwrap()
}

/// Run `git args` in `dir` with `input` on its standard input: what it
/// writes, once it has done so.
pub(crate) fn git_with(dir: &Path, args: &[&str], input: &[u8], env: &[(&str, &str)]) -> Vec<u8> {
    let out = run_git(dir, args, input, env);
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}

/// What `git upload-pack` answers in protocol v2, as a smart HTTP server
/// runs it: the advertisement, or the answer to `request`, whatever that
/// is (a server's error is an answer too).
pub(crate) fn upload_pack(dir: &Path, request: Option<&[u8]>) -> Vec<u8> {
    let env = [("GIT_PROTOCOL", "version=2")];
    let out = match request {
        None => run_git(
            dir,
            &["upload-pack", "--stateless-rpc", "--advertise-refs", "."],
            b"",
            &env,
        ),
        Some(request) => run_git(dir, &["upload-pack", "--stateless-rpc", "."], request, &env),
    };
    out.stdout
}

/// A repository of the system's git, in a directory of its own.
pub(crate) struct Scratch {
    pub(crate) dir: tempfile::TempDir,
    pub(crate) format: ObjectFormat,
}

impl Scratch {
    /// An empty repository, or `None` where there is no git.
    pub(crate) fn new(format: ObjectFormat) -> Option<Scratch> {
        if Command::new("git").arg("--version").output().is_err() {
            eprintln!("skipped: no git to hold the pack reader to");
            return None;
        }
        let scratch = Scratch {
            dir: tempfile::tempdir().unwrap(),
            format,
        };
        let name = match format {
            ObjectFormat::Sha1 => "--object-format=sha1",
            ObjectFormat::Sha256 => "--object-format=sha256",
        };
        scratch.git(&["init", "-q", name, "."]);
        Some(scratch)
    }

    pub(crate) fn path(&self) -> &Path {
        self.dir.path()
    }

    pub(crate) fn git(&self, args: &[&str]) -> String {
        let out = git_with(self.path(), args, b"", &[]);
        String::from_utf8(out).unwrap().trim().to_string()
    }

    fn write(&self, path: &str, text: &str) {
        let path = self.path().join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }

    fn remove(&self, path: &str) {
        let path = self.path().join(path);
        match path.is_dir() {
            true => std::fs::remove_dir_all(path).unwrap(),
            false => std::fs::remove_file(path).unwrap(),
        }
    }

    /// Commit the working tree, with `index`'s entries `(mode, id, path)`
    /// set beside it (a link, a submodule): the commit's id.
    fn commit(&self, message: &str, index: &[(&str, &str, &str)]) -> String {
        self.git(&["add", "-A"]);
        for (mode, id, path) in index {
            self.git(&[
                "update-index",
                "--add",
                "--cacheinfo",
                &format!("{mode},{id},{path}"),
            ]);
        }
        self.git(&["commit", "-q", "--allow-empty", "-m", message]);
        self.git(&["rev-parse", "HEAD"])
    }

    /// A blob of `text`, written to the repository: its id.
    fn blob(&self, text: &str) -> String {
        let out = git_with(
            self.path(),
            &["hash-object", "-w", "--stdin"],
            text.as_bytes(),
            &[],
        );
        String::from_utf8(out).unwrap().trim().to_string()
    }

    /// A commit's id of this repository's format that is no commit of it,
    /// for a submodule.
    fn elsewhere(&self, digit: char) -> String {
        digit.to_string().repeat(self.format.len() * 2)
    }

    /// What `git diff --name-only --no-renames from to` lists, sorted.
    pub(crate) fn diff(&self, from: &str, to: &str) -> Vec<String> {
        let out = git_with(
            self.path(),
            &["diff", "-z", "--name-only", "--no-renames", from, to],
            b"",
            &[],
        );
        let mut paths: Vec<String> = out
            .split(|b| *b == 0)
            .filter(|p| !p.is_empty())
            .map(|p| String::from_utf8(p.to_vec()).unwrap())
            .collect();
        paths.sort();
        paths
    }

    /// What dagron-ci's own fetch of `wants` is answered with by this
    /// repository's `git upload-pack`: the pack.
    pub(crate) fn fetched(&self, wants: &[&str]) -> Vec<u8> {
        let advertised = upload_pack(self.path(), None);
        let Advertisement::V2 {
            object_format,
            fetch: Some(features),
        } = git::read_advertisement(&advertised, &[]).unwrap()
        else {
            panic!("git's own server speaks v2 and fetches");
        };
        assert!(features.iter().any(|f| f == "shallow"), "{features:?}");
        let filter = features.iter().any(|f| f == "filter");
        let request = git::fetch_request(wants, object_format.as_deref(), filter);
        git::read_fetch(upload_pack(self.path(), Some(&request))).unwrap()
    }
}

/// Two commits of `repo` with a change too large for the compare API, and
/// every kind of change in it: a file changed, added, removed, renamed, made
/// executable; a directory removed, one that became a file and a file that
/// became one; a link and a submodule moved on; a change deep down; a name
/// that is not ASCII. `docs/` is left as it is. The two commits' ids.
pub(crate) fn a_large_change(repo: &Scratch) -> (String, String) {
    repo.write("src/keep.rs", "fn keep() {}\n");
    repo.write("src/lib.rs", "pub fn one() {}\n");
    repo.write("src/deep/a/b/c/d.rs", "// deep\n");
    repo.write("old/name.md", "a name\n");
    repo.write("bin/tool.sh", "#!/bin/sh\necho tool\n");
    repo.write("gone/x.txt", "x\n");
    repo.write("gone/y/z.txt", "z\n");
    repo.write("was_file", "a file\n");
    repo.write("was_dir/f.txt", "in a directory\n");
    repo.write("docs/guide.md", "# Guide\n");
    repo.write("ünïcode.txt", "1\n");
    let link = repo.blob("src/keep.rs");
    let sub = repo.elsewhere('1');
    let base = repo.commit(
        "base",
        &[("120000", &link, "link"), ("160000", &sub, "sub")],
    );

    repo.write("src/lib.rs", "pub fn one() {}\npub fn two() {}\n");
    repo.write("src/deep/a/b/c/d.rs", "// deeper\n");
    repo.remove("old");
    repo.write("new/name.md", "a name\n");
    repo.remove("gone");
    repo.remove("was_file");
    repo.write("was_file/inner.txt", "now a directory\n");
    repo.remove("was_dir");
    repo.write("was_dir", "now a file\n");
    repo.write("ünïcode.txt", "2\n");
    for n in 0..400 {
        repo.write(&format!("gen/{:02}/{n}.txt", n % 7), &format!("{n}\n"));
    }
    let link = repo.blob("src/lib.rs");
    let sub = repo.elsewhere('2');
    executable(&repo.path().join("bin/tool.sh"));
    let head = repo.commit(
        "head",
        &[("120000", &link, "link"), ("160000", &sub, "sub")],
    );
    (base, head)
}

/// A branch rebased and pushed again: its own change (`docs/a.md`) made on
/// the main it left, and made again on main moved on (400 files under
/// `src/`). The old tip, where the branch left main (the merge base of the
/// two tips), and the new tip. The old tip is not in the new one's history,
/// and `docs/a.md` is the same in both.
pub(crate) fn a_rebased_branch(repo: &Scratch) -> (String, String, String) {
    repo.write("docs/a.md", "a\n");
    repo.write("src/lib.rs", "pub fn one() {}\n");
    let left = repo.commit("main", &[]);
    repo.write("docs/a.md", "a, changed\n");
    let old = repo.commit("the branch", &[]);
    repo.git(&["reset", "-q", "--hard", &left]);
    for n in 0..400 {
        repo.write(&format!("src/{n}.rs"), &format!("// {n}\n"));
    }
    repo.commit("main moves on", &[]);
    repo.write("docs/a.md", "a, changed\n");
    let new = repo.commit("the branch, rebased", &[]);
    assert_eq!(repo.git(&["merge-base", &old, &new]), left);
    (old, left, new)
}

/// Make `path` executable, as `chmod +x` does.
fn executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

fn changed(pack: &[u8], format: ObjectFormat, from: &str, to: &str) -> Result<Vec<String>, String> {
    read(pack, format, &ROOMY)?.changed(from, to, &ROOMY)
}

#[test]
fn a_shallow_fetch_lists_what_git_diff_lists() {
    let Some(repo) = Scratch::new(ObjectFormat::Sha1) else {
        return;
    };
    let (base, head) = a_large_change(&repo);
    let expected = repo.diff(&base, &head);
    assert!(expected.len() > 400, "{}", expected.len());
    for path in [
        "bin/tool.sh",
        "gone/y/z.txt",
        "link",
        "new/name.md",
        "old/name.md",
        "src/deep/a/b/c/d.rs",
        "sub",
        "was_dir",
        "was_dir/f.txt",
        "was_file",
        "was_file/inner.txt",
        "ünïcode.txt",
    ] {
        assert!(expected.iter().any(|p| p == path), "git lists {path}");
    }
    assert!(!expected
        .iter()
        .any(|p| p.starts_with("docs/") || p == "src/keep.rs"));

    // Without the files' contents, where the server takes a filter.
    repo.git(&["config", "uploadpack.allowFilter", "true"]);
    let pack = repo.fetched(&[&base, &head]);
    assert_eq!(changed(&pack, repo.format, &base, &head).unwrap(), expected);
    assert_eq!(
        changed(&pack, repo.format, &head, &base).unwrap(),
        expected,
        "the same paths, the other way about"
    );
    let filtered = pack.len();

    // With them, where it does not: the same, from a larger pack.
    repo.git(&["config", "uploadpack.allowFilter", "false"]);
    let pack = repo.fetched(&[&base, &head]);
    assert!(pack.len() > filtered, "{} > {filtered}", pack.len());
    assert_eq!(changed(&pack, repo.format, &base, &head).unwrap(), expected);

    // A commit to itself changes nothing.
    assert_eq!(
        changed(&pack, repo.format, &head, &head).unwrap(),
        Vec::<String>::new()
    );
}

#[test]
fn a_sha256_repository_is_read_in_its_own_format() {
    let Some(repo) = Scratch::new(ObjectFormat::Sha256) else {
        return;
    };
    let (base, head) = a_large_change(&repo);
    assert_eq!(head.len(), 64);
    repo.git(&["config", "uploadpack.allowFilter", "true"]);
    let pack = repo.fetched(&[&base, &head]);
    assert_eq!(
        changed(&pack, ObjectFormat::Sha256, &base, &head).unwrap(),
        repo.diff(&base, &head)
    );
    // Read as SHA-1, its checksum is not the pack's.
    assert!(read(&pack, ObjectFormat::Sha1, &ROOMY).is_err());
}

/// A history whose trees are much alike, packed by `git pack-objects`
/// whole (every commit, tree and blob of it), with deltas many deep: by
/// offset, or (as a server without `ofs-delta` sends them) by their base's
/// id.
#[test]
fn deltas_by_offset_and_by_id_are_made_whole() {
    let Some(repo) = Scratch::new(ObjectFormat::Sha1) else {
        return;
    };
    let mut commits = Vec::new();
    for round in 0..12 {
        for n in 0..60 {
            let text = match n % 12 == round {
                true => format!("{n} {round}\n"),
                false => format!("{n}\n"),
            };
            repo.write(&format!("t/{:02}/{n}.txt", n % 5), &text);
        }
        repo.write(&format!("t/round{round}.txt"), "a file of this round\n");
        commits.push(repo.commit(&format!("round {round}"), &[]));
    }
    let revs = format!("{}\n", commits.last().unwrap());
    for offsets in [true, false] {
        let mut args = vec![
            "pack-objects",
            "--revs",
            "--stdout",
            "--window=50",
            "--depth=50",
        ];
        if offsets {
            args.push("--delta-base-offset");
        }
        let pack = git_with(repo.path(), &args, revs.as_bytes(), &[]);
        let objects = read(&pack, repo.format, &ROOMY).unwrap();
        assert_eq!(objects.commits().len(), commits.len());
        for pair in [(0, 11), (3, 4), (11, 5)] {
            let (from, to) = (&commits[pair.0], &commits[pair.1]);
            assert_eq!(
                objects.changed(from, to, &ROOMY).unwrap(),
                repo.diff(from, to),
                "{pair:?}, by offset: {offsets}"
            );
        }
        // There are deltas among them, so none may be a delta at all.
        let whole = Limits {
            delta_depth: 0,
            ..ROOMY
        };
        let err = read(&pack, repo.format, &whole).err().unwrap();
        assert_eq!(err, "an object is more than 0 deltas deep");
    }
}

#[test]
fn a_pack_past_its_limits_is_refused_with_why() {
    let Some(repo) = Scratch::new(ObjectFormat::Sha1) else {
        return;
    };
    let (base, head) = a_large_change(&repo);
    let pack = repo.fetched(&[&base, &head]);
    let objects = |limits: &Limits| read(&pack, repo.format, limits).err();
    let walk = |limits: &Limits| {
        read(&pack, repo.format, &ROOMY)
            .unwrap()
            .changed(&base, &head, limits)
            .err()
    };
    let err = objects(&Limits {
        objects: 3,
        ..ROOMY
    })
    .unwrap();
    assert!(
        err.starts_with("the pack holds ") && err.ends_with("objects, more than 3"),
        "{err}"
    );
    assert_eq!(
        objects(&Limits {
            inflated: 1000,
            ..ROOMY
        }),
        Some("the pack inflates to more than 1000 bytes".into())
    );
    let gone = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
    let late = Limits {
        until: Some(gone),
        ..ROOMY
    };
    assert_eq!(objects(&late), Some("out of time".into()));
    assert_eq!(walk(&late), Some("out of time".into()));
    // src/deep/a/b/c/d.rs is five trees down.
    assert_eq!(
        walk(&Limits {
            tree_depth: 4,
            ..ROOMY
        }),
        Some("a tree is more than 4 deep".into())
    );
    assert_eq!(
        walk(&Limits {
            tree_depth: 5,
            ..ROOMY
        }),
        None
    );
    assert_eq!(
        walk(&Limits { paths: 10, ..ROOMY }),
        Some("the change touches more than 10 paths".into())
    );
    assert_eq!(
        walk(&Limits {
            walked: 100,
            ..ROOMY
        }),
        Some("the trees walked come to more than 100 bytes".into())
    );
    assert_eq!(
        walk(&Limits {
            path_len: 18,
            ..ROOMY
        }),
        Some("a path is longer than 18 bytes".into()),
        "src/deep/a/b/c/d.rs is 19"
    );
    assert_eq!(
        walk(&Limits {
            listed: 1000,
            ..ROOMY
        }),
        Some("the paths listed come to more than 1000 bytes".into())
    );
}

#[test]
fn a_broken_pack_is_an_error_and_not_a_panic() {
    let Some(repo) = Scratch::new(ObjectFormat::Sha1) else {
        return;
    };
    let (base, head) = a_large_change(&repo);
    let pack = repo.fetched(&[&base, &head]);
    let read = |pack: &[u8]| read(pack, ObjectFormat::Sha1, &ROOMY).err();
    // Its checksum, rewritten for what it holds now.
    let summed = |body: Vec<u8>| sealed(&body, ObjectFormat::Sha1);
    let body = &pack[..pack.len() - 20];

    assert_eq!(read(b""), Some("the answer is not a pack".into()));
    assert_eq!(
        read(&pack[..pack.len() / 2]),
        Some("the pack's checksum does not match it".into())
    );
    let mut flipped = pack.clone();
    flipped[100] ^= 1;
    assert_eq!(
        read(&flipped),
        Some("the pack's checksum does not match it".into())
    );
    // One object more than it holds, and one fewer.
    let count = u32::from_be_bytes(body[8..12].try_into().unwrap());
    let recounted = |count: u32| {
        let mut body = body.to_vec();
        body[8..12].copy_from_slice(&count.to_be_bytes());
        summed(body)
    };
    assert_eq!(
        read(&recounted(count + 1)),
        Some("the pack is cut short".into())
    );
    assert_eq!(
        read(&recounted(count - 1)),
        Some("the pack goes on past its last object".into())
    );
    let mut version = body.to_vec();
    version[7] = 4;
    assert_eq!(
        read(&summed(version)),
        Some("pack version 4 is not one git writes".into())
    );
    // Every cut of it, its checksum made good, fails and does not panic.
    for cut in (12..body.len()).step_by(97) {
        assert!(
            read(&summed(body[..cut].to_vec())).is_some(),
            "cut at {cut}"
        );
    }

    // A pack without a tree the walk needs: a commit alone.
    let objects = super::read(&pack, ObjectFormat::Sha1, &ROOMY).unwrap();
    let mut alone = Objects {
        format: ObjectFormat::Sha1,
        by_id: HashMap::new(),
    };
    for id in [&base, &head] {
        let id = ObjectFormat::Sha1.parse(id).unwrap();
        alone.by_id.insert(id, objects.by_id[&id].clone());
    }
    let err = alone.changed(&base, &head, &ROOMY).unwrap_err();
    assert!(
        err.starts_with("tree ") && err.ends_with(" is not in the pack"),
        "{err}"
    );
    assert_eq!(
        objects.changed(&"0".repeat(40), &head, &ROOMY),
        Err("commit 000000000000 is not in the pack".into())
    );
    assert_eq!(
        objects.changed("not hex", &head, &ROOMY),
        Err("commit not hex is not in the pack".into())
    );
}

/// A tree is read as git writes one, and anything else is refused: a name
/// that is empty, a dot, has a slash or comes twice; a mode that is not
/// octal; an entry cut short; entries out of git's order. A name that is not
/// UTF-8 is refused only where it is listed.
#[test]
fn a_tree_git_would_not_write_is_refused() {
    let format = ObjectFormat::Sha1;
    let id = |n: u8| [n; 20];
    let entry =
        |mode: &str, name: &[u8], to: [u8; 20]| [mode.as_bytes(), b" ", name, b"\0", &to].concat();
    let commit = |tree: &[u8]| {
        format!(
            "tree {}\nauthor a <a> 0 +0000\n\nm\n",
            format.hex(format.object_id(Kind::Tree, tree))
        )
    };
    let objects = |a: Vec<u8>, b: Vec<u8>| {
        let mut by_id = HashMap::new();
        let (ca, cb) = (commit(&a), commit(&b));
        let ids =
            [ca.as_bytes(), cb.as_bytes()].map(|c| format.hex(format.object_id(Kind::Commit, c)));
        by_id.insert(format.object_id(Kind::Tree, &a), (Kind::Tree, a));
        by_id.insert(format.object_id(Kind::Tree, &b), (Kind::Tree, b));
        by_id.insert(
            format.object_id(Kind::Commit, ca.as_bytes()),
            (Kind::Commit, ca.into_bytes()),
        );
        by_id.insert(
            format.object_id(Kind::Commit, cb.as_bytes()),
            (Kind::Commit, cb.into_bytes()),
        );
        let objects = Objects { format, by_id };
        objects.changed(&ids[0], &ids[1], &ROOMY)
    };
    let base = entry("100644", b"a.txt", id(1));
    assert_eq!(
        objects(base.clone(), entry("100644", b"a.txt", id(2))),
        Ok(vec!["a.txt".into()])
    );
    // A mode git writes another way is the same mode.
    assert_eq!(
        objects(base.clone(), entry("100664", b"a.txt", id(1))),
        Ok(vec![])
    );
    assert_eq!(
        objects(base.clone(), entry("100755", b"a.txt", id(1))),
        Ok(vec!["a.txt".into()])
    );
    for bad in [
        entry("100644", b"", id(2)),
        entry("100644", b".", id(2)),
        entry("100644", b"..", id(2)),
        entry("100644", b"a/b", id(2)),
        entry("100648", b"b", id(2)),
        entry("", b"b", id(2)),
        [entry("100644", b"b", id(2)), entry("100644", b"b", id(3))].concat(),
        entry("100644", b"b", id(2))[..20].to_vec(),
        [entry("100644", b"b", id(2)), entry("100644", b"a", id(3))].concat(),
        // A directory sorts as if its name ended in a slash: after `b.c`.
        [entry("40000", b"b", id(2)), entry("100644", b"b.c", id(3))].concat(),
    ] {
        let err = objects(base.clone(), bad.clone()).unwrap_err();
        assert!(err.ends_with("is not one git writes"), "{bad:?}: {err}");
    }
    // Not UTF-8: refused where it changed, and passed over where it did not.
    let latin = entry("100644", b"caf\xe9", id(4));
    assert_eq!(
        objects(base.clone(), [base.clone(), latin.clone()].concat()),
        Err("a path at the top is not UTF-8".into())
    );
    assert_eq!(
        objects(
            [base.clone(), latin.clone()].concat(),
            [entry("100644", b"a.txt", id(5)), latin].concat()
        ),
        Ok(vec!["a.txt".into()])
    );
    // A file and a directory of one name are two entries, as git sorts them.
    let file = entry("100644", b"b", id(6));
    let dir = entry(
        "40000",
        b"b",
        format.object_id(Kind::Tree, b"").0[..20]
            .try_into()
            .unwrap(),
    );
    assert_eq!(
        objects(
            [file.clone(), entry("100644", b"b.c", id(7))].concat(),
            [entry("100644", b"b.c", id(7)), dir].concat()
        ),
        Ok(vec!["b".into()])
    );
}

/// Objects made by hand, of SHA-1: trees no git would be asked to make.
struct ByHand(Objects);

impl ByHand {
    fn new() -> ByHand {
        ByHand(Objects {
            format: ObjectFormat::Sha1,
            by_id: HashMap::new(),
        })
    }

    fn put(&mut self, kind: Kind, data: Vec<u8>) -> Oid {
        let id = self.0.format.object_id(kind, &data);
        self.0.by_id.insert(id, (kind, data));
        id
    }

    /// A tree of `entries` (mode, name, object), put in git's order: its id.
    fn tree(&mut self, mut entries: Vec<(&str, Vec<u8>, Oid)>) -> Oid {
        fn read<'a>((mode, name, id): &'a (&str, Vec<u8>, Oid)) -> TreeEntry<'a> {
            let mode = canonical(u32::from_str_radix(mode, 8).unwrap());
            TreeEntry {
                name,
                mode,
                id: *id,
            }
        }
        entries.sort_by(|x, y| git_order(read(x), read(y)));
        let data = entries
            .iter()
            .flat_map(|(mode, name, id)| [mode.as_bytes(), b" ", name, b"\0", &id.0[..20]].concat())
            .collect();
        self.put(Kind::Tree, data)
    }

    /// A commit of `tree`: its id in hex.
    fn commit(&mut self, tree: Oid) -> String {
        let text = format!(
            "tree {}\nauthor a <a> 0 +0000\n\nm\n",
            self.0.format.hex(tree)
        );
        let id = self.put(Kind::Commit, text.into_bytes());
        self.0.format.hex(id)
    }
}

/// A big tree reached by many paths is read at each, and counts at each:
/// here 1,001 directories, each a tree of 4,000 files on one side and the
/// same with its modes spelled another way on the other. The two read as
/// one, so the walk touches nothing under them, and is held to its bytes
/// read and its time all the same.
#[test]
fn a_tree_reached_by_many_paths_is_walked_within_bounds() {
    let mut hand = ByHand::new();
    let blob = ObjectFormat::Sha1.object_id(Kind::Blob, b"");
    let mut files = |mode| {
        let files = (0..4000)
            .map(|n| (mode, format!("f{n:04}").into_bytes(), blob))
            .collect();
        hand.tree(files)
    };
    // 100664 is 100644 as git reads it.
    let (one, other) = (files("100644"), files("100664"));
    let mut change = |dirs: usize| {
        let mut root = |sub| {
            let dirs = (0..dirs)
                .map(|n| ("40000", format!("d{n:04}").into_bytes(), sub))
                .collect();
            hand.tree(dirs)
        };
        let (from, to) = (root(one), root(other));
        (hand.commit(from), hand.commit(to))
    };
    let (from, to) = change(3);
    let (many_from, many_to) = change(1001);
    let objects = hand.0;
    assert_eq!(objects.changed(&from, &to, &ROOMY), Ok(vec![]));

    assert_eq!(
        objects.changed(&many_from, &many_to, &ROOMY),
        Err("the trees walked come to more than 64 MiB".into())
    );
    // Past the time it was given, part way through: no count of what it
    // touched decides when the time is looked at.
    let late = Limits {
        walked: usize::MAX,
        until: Some(Instant::now() + Duration::from_millis(50)),
        ..ROOMY
    };
    assert_eq!(
        objects.changed(&many_from, &many_to, &late),
        Err("out of time".into())
    );
}

/// A path longer than git checks out is refused, and so is a list of paths
/// that comes to too much: here a chain of 20 directories with names of 255
/// bytes, and 1,000 files at its foot, each path over 5,000 bytes.
#[test]
fn a_path_too_long_and_paths_too_many_bytes_are_refused() {
    let mut hand = ByHand::new();
    let blob = ObjectFormat::Sha1.object_id(Kind::Blob, b"");
    let files = (0..1000)
        .map(|n| ("100644", format!("f{n}").into_bytes(), blob))
        .collect();
    let mut tree = hand.tree(files);
    for n in 0..20 {
        let mut name = format!("{n:02}").into_bytes();
        name.resize(255, b'x');
        tree = hand.tree(vec![("40000", name, tree)]);
    }
    let empty = hand.tree(vec![]);
    let (from, to) = (hand.commit(empty), hand.commit(tree));
    let objects = hand.0;
    assert_eq!(
        objects.changed(&from, &to, &ROOMY),
        Err("a path is longer than 4096 bytes".into())
    );
    let long = Limits {
        path_len: usize::MAX,
        ..ROOMY
    };
    assert_eq!(objects.changed(&from, &to, &long).unwrap().len(), 1000);
    assert_eq!(
        objects.changed(
            &from,
            &to,
            &Limits {
                listed: 1 << 20,
                ..long
            }
        ),
        Err("the paths listed come to more than 1 MiB".into())
    );
}

#[test]
fn a_delta_is_applied_as_git_applies_one() {
    let mut budget = Budget {
        left: 1 << 20,
        of: 1 << 20,
    };
    let base = b"0123456789";
    // Base 10, result 7: copy 4 from offset 2, insert "ab", copy 1 from 9.
    let delta = [10, 7, 0x91, 2, 4, 2, b'a', b'b', 0x91, 9, 1];
    assert_eq!(apply(base, &delta, &mut budget).unwrap(), b"2345ab9");
    let refused = [
        (
            vec![9, 1, 1, b'x'],
            "a delta's base is not the size the delta says",
        ),
        (
            vec![10, 2, 0x91, 9, 2],
            "a delta copies from past its base's end",
        ),
        (
            vec![10, 1, 0],
            "a delta has an instruction 0, which git keeps for later",
        ),
        (
            vec![10, 1, 2, b'a', b'b'],
            "a delta makes more than it says",
        ),
        (vec![10, 3, 1, b'a'], "a delta makes less than it says"),
        (vec![10, 3, 3, b'a'], "a delta is cut short"),
        (vec![10, 3, 0x91, 2], "a delta is cut short"),
        (
            vec![10, 0xff, 0xff, 0xff, 0xff, 0x7f],
            "the pack inflates to more than 1 MiB",
        ),
    ];
    for (delta, why) in refused {
        assert_eq!(
            apply(base, &delta, &mut budget),
            Err(why.to_string()),
            "{delta:?}"
        );
    }
    // A copy of length 0 is one of 64 KiB.
    let big = vec![7; 0x10000];
    let delta = [0x80, 0x80, 0x04, 0x80, 0x80, 0x04, 0x80];
    assert_eq!(apply(&big, &delta, &mut budget).unwrap(), big);
}

#[test]
fn a_size_or_an_offset_past_what_git_writes_is_refused() {
    let mut at = 0;
    assert!(object_header(&[0xff; 12], &mut at).is_err());
    let mut at = 0;
    assert_eq!(object_header(&[0x95, 0x01], &mut at), Ok((1, 0x15)));
    let mut at = 0;
    assert!(base_offset(&[0xff; 12], &mut at).is_err());
    // 0x81 0x00: (1 + 1) * 128 + 0, git's own spelling of 256.
    let mut at = 0;
    assert_eq!(base_offset(&[0x81, 0x00], &mut at), Ok(256));
    assert!(delta_size(&mut &[0xff; 12][..]).is_err());
}
