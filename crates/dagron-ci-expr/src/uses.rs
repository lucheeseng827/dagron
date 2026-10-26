//! What a step's `uses:` names, as GitHub writes it: an action of the
//! workflow's own repository (`./dir`), or one of another
//! (`owner/repo@ref`, `owner/repo/dir@ref`). Read where a workflow is
//! compiled and again where its steps run, and so read here, once.

/// An action of another repository, as `uses:` names one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Remote<'a> {
    pub owner: &'a str,
    pub repo: &'a str,
    /// The directory of the repository the action is in; empty for its top.
    pub path: &'a str,
    /// A tag, a branch, or a commit's full id.
    pub git_ref: &'a str,
}

impl Remote<'_> {
    /// `owner/repo`.
    pub fn repository(&self) -> String {
        format!("{}/{}", self.owner, self.repo)
    }

    /// The directory the repository is fetched into, under the actions'
    /// directory: its owner, its name and the ref, each `/` of the ref
    /// written `%2F`, so that two refs are always two directories.
    pub fn fetched_dir(&self) -> String {
        format!(
            "{}/{}/{}",
            self.owner,
            self.repo,
            self.git_ref.replace('/', "%2F")
        )
    }

    /// Whether its ref is a commit's full id (40 or 64 lowercase hex
    /// digits), which no one can move.
    pub fn is_pinned(&self) -> bool {
        (self.git_ref.len() == 40 || self.git_ref.len() == 64)
            && self
                .git_ref
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    }
}

/// `uses` as an action of another repository, where it is one: an owner
/// and a repository's name, a directory of it or none, and a ref.
pub fn remote(uses: &str) -> Option<Remote<'_>> {
    let (what, git_ref) = uses.trim().split_once('@')?;
    let mut parts = what.splitn(3, '/');
    let owner = parts.next()?;
    let repo = parts.next()?;
    let path = parts.next().unwrap_or("");
    let owner_ok = !owner.is_empty()
        && owner.len() <= 100
        && owner
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        && !owner.starts_with('.');
    let named = |part: &str| {
        !part.is_empty()
            && part.len() <= 100
            && part != "."
            && part != ".."
            && part
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    };
    let path_ok = path.is_empty() || path.split('/').all(named);
    (owner_ok && named(repo) && path_ok && is_ref(git_ref)).then_some(Remote {
        owner,
        repo,
        path,
        git_ref,
    })
}

/// A tag's or a branch's name as git takes one, of letters, digits, `.`,
/// `_`, `-` and `/`; or a commit's full id.
pub fn is_ref(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '/'))
        && !name.starts_with(['-', '/', '.'])
        && !name.ends_with(['/', '.'])
        && !name.ends_with(".lock")
        && !name.contains("..")
        && !name.contains("//")
        && !name.contains("/.")
}

/// The directory a local action is in, within the workspace: what follows
/// `./`, where no part of it climbs out with `..`.
pub fn local(uses: &str) -> Option<&str> {
    let dir = uses.trim().strip_prefix("./")?;
    let climbs = dir.split('/').any(|part| part == "..");
    (!climbs && !dir.contains('\0') && !dir.starts_with('/')).then_some(dir)
}

/// The image a step runs in, where `uses` names one: `docker://` and an
/// image as a registry has it, with no space or control character in it.
pub fn docker(uses: &str) -> Option<&str> {
    let image = uses.trim().strip_prefix("docker://")?;
    (!image.is_empty()
        && image.len() <= 512
        && !image.chars().any(|c| c.is_whitespace() || c.is_control()))
    .then_some(image)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_docker_image_is_read_as_github_writes_one() {
        assert_eq!(docker("docker://alpine:3.20"), Some("alpine:3.20"));
        assert_eq!(
            docker("docker://ghcr.io/acme/tool@sha256:abc"),
            Some("ghcr.io/acme/tool@sha256:abc")
        );
        assert_eq!(docker("docker://"), None);
        assert_eq!(docker("docker://a b"), None);
        assert_eq!(docker("alpine:3.20"), None);
        assert_eq!(remote("docker://alpine@sha256:abc"), None);
    }

    #[test]
    fn an_action_of_another_repository_is_read_as_github_writes_one() {
        let setup = remote("actions/setup-node@v4").unwrap();
        assert_eq!(
            (setup.owner, setup.repo, setup.path, setup.git_ref),
            ("actions", "setup-node", "", "v4")
        );
        assert_eq!(setup.repository(), "actions/setup-node");
        assert_eq!(setup.fetched_dir(), "actions/setup-node/v4");
        assert!(!setup.is_pinned());

        let deep = remote("octo/tools/lint/rust@releases/v1").unwrap();
        assert_eq!((deep.path, deep.git_ref), ("lint/rust", "releases/v1"));
        assert_eq!(deep.fetched_dir(), "octo/tools/releases%2Fv1");

        let sha = "4b4708e725f29891d22b4c96af9f0a403ff51c7c";
        assert!(remote(&format!("o/r@{sha}")).unwrap().is_pinned());
        assert!(!remote(&format!("o/r@{}", sha.to_uppercase()))
            .unwrap()
            .is_pinned());

        for not in [
            "actions/setup-node",
            "setup-node@v4",
            "./local",
            "docker://alpine:3",
            "o/r@",
            "o/r@-v1",
            "o/r@v1 x",
            "o/r@a..b",
            "o/r@v1.lock",
            "o/r/../x@v1",
            "o/r//x@v1",
            "o/r/x/@v1",
            "/r@v1",
            "o/@v1",
            "o/r$x@v1",
            ".o/r@v1",
        ] {
            assert_eq!(remote(not), None, "{not}");
        }
    }

    #[test]
    fn a_local_action_is_a_directory_of_the_workspace() {
        assert_eq!(
            local("./.github/actions/greet"),
            Some(".github/actions/greet")
        );
        assert_eq!(local("./"), Some(""));
        assert_eq!(local("./a/b..c"), Some("a/b..c"));
        for not in [
            "./../outside",
            "./a/../../outside",
            ".//etc",
            "a/b",
            "o/r@v1",
        ] {
            assert_eq!(local(not), None, "{not}");
        }
    }
}
