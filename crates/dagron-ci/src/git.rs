//! git's ref listing over smart HTTP: the part of `git ls-remote` the poll
//! sweep needs. And a shallow fetch of two commits, for a paths filter whose
//! change the forge's compare API cannot list: the request, and the pack out
//! of its answer, which [`pack`] reads. This file builds the requests and
//! reads the answers; the forge (`forge.rs`) sends them, so everything here is
//! pure and tested on bytes a server sent.
//!
//! dagron-ci speaks the protocol itself, so its image needs no git binary and
//! a credential never reaches a command line.
//!
//! Protocol v2 first. Its `ls-refs` command takes ref prefixes, so a
//! repository's pull request refs (GitHub keeps two per pull request, for
//! good) never cross the wire. A server that only speaks v0 answers the first
//! request with every ref it has, and the list is filtered here instead.
//!
//! Servers frame a v2 answer to the first request in two ways, and both are
//! read: git's own (`git http-backend`) opens it with `version 2`, as git's
//! protocol document has it; github.com names the service and flushes first,
//! as every smart server does for v0.

use std::collections::BTreeMap;

use crate::event::{is_commit_id, is_storable_ref};

pub mod pack;

/// Where the first request goes, after the repository's URL: the ref
/// advertisement of upload-pack, the service `git fetch` uses.
pub const ADVERTISEMENT_PATH: &str = "info/refs?service=git-upload-pack";
/// Where a v2 command goes, after the repository's URL.
pub const COMMAND_PATH: &str = "git-upload-pack";
pub const COMMAND_CONTENT_TYPE: &str = "application/x-git-upload-pack-request";
pub const COMMAND_ACCEPT: &str = "application/x-git-upload-pack-result";
/// Sent with both requests. A server that does not know it answers in v0.
pub const PROTOCOL_HEADER: &str = "git-protocol";
pub const PROTOCOL_VERSION_2: &str = "version=2";

/// A ref listing.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Refs {
    /// Each ref, and the id of the object it points at. For an annotated tag
    /// that is the tag's own id, as a webhook reports it.
    pub ids: BTreeMap<String, String>,
    /// For each annotated tag listed, the commit it names: git's peeled id.
    pub peeled: BTreeMap<String, String>,
    /// The branch `HEAD` names, `refs/heads/main`: the repository's default
    /// branch, where the server said.
    pub head: Option<String>,
    /// The commit `HEAD` points at, where it was listed.
    pub head_id: Option<String>,
}

/// What the advertisement said.
#[derive(Debug, PartialEq, Eq)]
pub enum Advertisement {
    /// A v2 server: send [`ls_refs_request`] next, echoing the object format
    /// the server named, if it named one. `fetch` is what its `fetch` command
    /// takes (`shallow`, `filter`, …), and `None` where it offers none.
    V2 {
        object_format: Option<String>,
        fetch: Option<Vec<String>>,
    },
    /// A v0 server, which listed every ref already: those under the prefixes
    /// asked for.
    V0(Refs),
}

/// An answer that is not what git sends.
#[derive(Debug, PartialEq, Eq)]
pub struct ProtocolError(pub String);

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Read the answer to `GET {repo}/info/refs?service=git-upload-pack`. Refs are
/// kept when they start with one of `prefixes` (each ending in `/`).
pub fn read_advertisement(body: &[u8], prefixes: &[&str]) -> Result<Advertisement, ProtocolError> {
    let mut pkts = Pkts(body);
    let is = |pkt: &Option<Pkt>, text: &[u8]| matches!(pkt, Some(Pkt::Data(line)) if line.trim_ascii_end() == text);
    // A smart server answering in v0 names the service, then flushes, and so
    // does github.com ahead of a v2 answer. git's own server opens a v2 answer
    // with `version 2` and no service line, as git's protocol document has
    // it. A dumb server (a plain file listing) does neither, and is not read.
    let mut first = pkts.next()?;
    if is(&first, b"# service=git-upload-pack") {
        if pkts.next()? != Some(Pkt::Flush) {
            return Err(ProtocolError(
                "the service line is not followed by a flush".into(),
            ));
        }
        first = pkts.next()?;
    } else if !is(&first, b"version 2") {
        return Err(ProtocolError(
            "not a git smart HTTP server: the answer names neither git-upload-pack nor \
             protocol version 2"
                .into(),
        ));
    }
    match first {
        Some(Pkt::Data(line)) if line.trim_ascii_end() == b"version 2" => {
            let mut ls_refs = false;
            let mut object_format = None;
            let mut fetch = None;
            loop {
                match pkts.next()? {
                    Some(Pkt::Flush) => break,
                    Some(Pkt::Data(line)) => {
                        let line = text(line)?;
                        if line == "ls-refs" || line.starts_with("ls-refs=") {
                            ls_refs = true;
                        } else if let Some(format) = line.strip_prefix("object-format=") {
                            object_format = Some(format.to_string());
                        } else if line == "fetch" {
                            fetch = Some(Vec::new());
                        } else if let Some(features) = line.strip_prefix("fetch=") {
                            fetch = Some(features.split_whitespace().map(str::to_string).collect());
                        }
                    }
                    _ => return Err(truncated()),
                }
            }
            if !ls_refs {
                return Err(ProtocolError(
                    "the server speaks protocol v2 but offers no ls-refs".into(),
                ));
            }
            Ok(Advertisement::V2 {
                object_format,
                fetch,
            })
        }
        // v0: the first ref carries the capabilities after a NUL.
        first => {
            let mut refs = Refs::default();
            let mut next = first;
            loop {
                match next {
                    Some(Pkt::Flush) => break,
                    Some(Pkt::Data(line)) => {
                        let mut parts = line.split(|b| *b == 0);
                        let line = parts.next().unwrap_or_default();
                        // The first line's capabilities name what HEAD is.
                        // Ones that are not text say nothing of it.
                        if let Some(capabilities) = parts.next() {
                            refs.head = std::str::from_utf8(capabilities)
                                .ok()
                                .and_then(|c| {
                                    c.split([' ', '\n'])
                                        .find_map(|c| c.strip_prefix("symref=HEAD:"))
                                })
                                .filter(|r| r.starts_with("refs/heads/") && is_storable_ref(r))
                                .map(str::to_string);
                        }
                        let line = text(line)?;
                        if let Some(id) = line.strip_suffix(" HEAD").filter(|id| is_commit_id(id)) {
                            refs.head_id = Some(id.to_string());
                        }
                        keep(&mut refs, line, prefixes);
                    }
                    _ => return Err(truncated()),
                }
                next = pkts.next()?;
            }
            Ok(Advertisement::V0(refs))
        }
    }
}

/// The body of `POST {repo}/git-upload-pack` that lists the refs under
/// `prefixes`, and with each annotated tag the commit it names (`peel`).
pub fn ls_refs_request(prefixes: &[&str], object_format: Option<&str>) -> Vec<u8> {
    let mut out = Vec::new();
    pkt(&mut out, "command=ls-refs\n");
    if let Some(format) = object_format {
        pkt(&mut out, &format!("object-format={format}\n"));
    }
    out.extend_from_slice(b"0001");
    pkt(&mut out, "peel\n");
    // And what HEAD names: the default branch.
    pkt(&mut out, "symrefs\n");
    pkt(&mut out, "ref-prefix HEAD\n");
    for prefix in prefixes {
        pkt(&mut out, &format!("ref-prefix {prefix}\n"));
    }
    out.extend_from_slice(b"0000");
    out
}

/// Read the answer to [`ls_refs_request`]: each ref and the object it points
/// at, filtered by `prefixes` as well, since a server may treat them as hints.
pub fn read_ls_refs(body: &[u8], prefixes: &[&str]) -> Result<Refs, ProtocolError> {
    let mut pkts = Pkts(body);
    let mut refs = Refs::default();
    loop {
        match pkts.next()? {
            Some(Pkt::Flush) => return Ok(refs),
            Some(Pkt::Data(line)) => {
                let line = text(line)?;
                if let Some(message) = line.strip_prefix("ERR ") {
                    return Err(ProtocolError(format!("the server refused: {message}")));
                }
                // `<id> HEAD symref-target:refs/heads/main`. An empty
                // repository's (`unborn HEAD …`) names no commit, and no
                // default branch to run anything at.
                let mut fields = line.split(' ');
                if let (Some(id), Some("HEAD")) = (fields.next(), fields.next()) {
                    if is_commit_id(id) {
                        refs.head_id = Some(id.to_string());
                        refs.head = fields
                            .find_map(|f| f.strip_prefix("symref-target:"))
                            .filter(|r| r.starts_with("refs/heads/") && is_storable_ref(r))
                            .map(str::to_string);
                    }
                    continue;
                }
                keep(&mut refs, line, prefixes);
            }
            _ => return Err(truncated()),
        }
    }
}

/// The body of `POST {repo}/git-upload-pack` that fetches the commits
/// `wants` at depth 1: each commit and every tree of it, and, with `filter`
/// (where the server's `fetch` takes it), none of its files' contents. Not a
/// thin pack, so every delta's base is in it; deltas by offset, as git's
/// own client asks; and no progress.
pub fn fetch_request(wants: &[&str], object_format: Option<&str>, filter: bool) -> Vec<u8> {
    let mut out = Vec::new();
    pkt(&mut out, "command=fetch\n");
    if let Some(format) = object_format {
        pkt(&mut out, &format!("object-format={format}\n"));
    }
    out.extend_from_slice(b"0001");
    pkt(&mut out, "no-progress\n");
    pkt(&mut out, "ofs-delta\n");
    pkt(&mut out, "deepen 1\n");
    if filter {
        pkt(&mut out, "filter blob:none\n");
    }
    for want in wants {
        pkt(&mut out, &format!("want {want}\n"));
    }
    // Nothing is had, so there is nothing to negotiate: the pack comes now.
    pkt(&mut out, "done\n");
    out.extend_from_slice(b"0000");
    out
}

/// Read the answer to [`fetch_request`]: the pack, as its `packfile`
/// section's first band carries it, drawn together in the answer's own
/// bytes, so the two are not held at once. The sections before that one
/// (`shallow-info`, `wanted-refs` and the like) are passed over, and so is
/// the second band, progress; the third is the server's error.
pub fn read_fetch(mut body: Vec<u8>) -> Result<Vec<u8>, ProtocolError> {
    let mut pkts = Pkts(&body);
    let no_pack = || ProtocolError("the answer holds no pack".into());
    loop {
        let name = match pkts.next()? {
            Some(Pkt::Data(line)) => text(line)?,
            Some(Pkt::Flush) => return Err(no_pack()),
            _ => return Err(truncated()),
        };
        if let Some(message) = name.strip_prefix("ERR ") {
            return Err(ProtocolError(format!("the server refused: {message}")));
        }
        if name == "packfile" {
            break;
        }
        loop {
            match pkts.next()? {
                Some(Pkt::Delim) => break,
                Some(Pkt::Data(_)) => {}
                Some(Pkt::Flush) => return Err(no_pack()),
                _ => return Err(truncated()),
            }
        }
    }
    // Where the next line starts, and where the pack read so far ends: each
    // line's data is moved back over the framing before it.
    let (mut at, mut end) = (body.len() - pkts.0.len(), 0);
    loop {
        let mut pkts = Pkts(&body[at..]);
        let pkt = pkts.next()?;
        let next = body.len() - pkts.0.len();
        let data = match pkt {
            Some(Pkt::Flush) => {
                body.truncate(end);
                return Ok(body);
            }
            Some(Pkt::Data(line)) => match line.split_first() {
                // The band's byte, after the line's four of length.
                Some((&1, _)) => at + 5..next,
                Some((&2, _)) => at..at,
                Some((&3, message)) => {
                    let message = String::from_utf8_lossy(message);
                    let message: String = message.trim().chars().take(200).collect();
                    return Err(ProtocolError(format!("the server failed: {message}")));
                }
                _ => {
                    return Err(ProtocolError(
                        "a line of the pack is on no band git has".into(),
                    ))
                }
            },
            _ => return Err(truncated()),
        };
        let len = data.len();
        body.copy_within(data, end);
        (at, end) = (next, end + len);
    }
}

/// Keep `<id> <ref>[ <attributes>]` when the ref is under a prefix and both
/// are fit for the queue.
///
/// An annotated tag is listed under its own id, which is what a webhook
/// reports and so what the two must share. The commit it names is kept beside
/// it: a v0 server lists it as a ref of its own (`refs/tags/v1^{}`), a v2
/// server as an attribute of the tag's line (`peeled:<id>`).
fn keep(refs: &mut Refs, line: &str, prefixes: &[&str]) {
    let mut fields = line.split(' ');
    let (Some(id), Some(name)) = (fields.next(), fields.next()) else {
        return;
    };
    let wanted = |name: &str| prefixes.iter().any(|p| name.starts_with(p)) && is_storable_ref(name);
    if let Some(tag) = name.strip_suffix("^{}") {
        if wanted(tag) && is_commit_id(id) {
            refs.peeled.insert(tag.to_string(), id.to_string());
        }
        return;
    }
    if !wanted(name) || !is_commit_id(id) {
        return;
    }
    refs.ids.insert(name.to_string(), id.to_string());
    if let Some(commit) = fields.find_map(|f| f.strip_prefix("peeled:")) {
        if is_commit_id(commit) {
            refs.peeled.insert(name.to_string(), commit.to_string());
        }
    }
}

fn text(line: &[u8]) -> Result<&str, ProtocolError> {
    std::str::from_utf8(line)
        .map(|l| l.strip_suffix('\n').unwrap_or(l))
        .map_err(|_| ProtocolError("a pkt-line is not UTF-8".into()))
}

fn truncated() -> ProtocolError {
    ProtocolError("the answer ends before its flush".into())
}

fn pkt(out: &mut Vec<u8>, line: &str) {
    out.extend_from_slice(format!("{:04x}", line.len() + 4).as_bytes());
    out.extend_from_slice(line.as_bytes());
}

#[derive(Debug, PartialEq, Eq)]
enum Pkt<'a> {
    Flush,
    Delim,
    ResponseEnd,
    Data(&'a [u8]),
}

/// pkt-lines: four hex digits of length (counting themselves), then the
/// line. `0000` flushes, `0001` delimits, `0002` ends a response.
struct Pkts<'a>(&'a [u8]);

impl<'a> Pkts<'a> {
    fn next(&mut self) -> Result<Option<Pkt<'a>>, ProtocolError> {
        if self.0.is_empty() {
            return Ok(None);
        }
        let len = match self.0.get(..4) {
            Some(digits) if digits.iter().all(u8::is_ascii_hexdigit) => {
                usize::from_str_radix(std::str::from_utf8(digits).unwrap_or_default(), 16)
                    .unwrap_or_default()
            }
            _ => {
                return Err(ProtocolError(
                    "a pkt-line does not start with four hex digits".into(),
                ))
            }
        };
        let (pkt, used) = match len {
            0 => (Pkt::Flush, 4),
            1 => (Pkt::Delim, 4),
            2 => (Pkt::ResponseEnd, 4),
            3 => return Err(ProtocolError("a pkt-line has length 3".into())),
            n if n > self.0.len() => return Err(truncated()),
            n => (Pkt::Data(&self.0[4..n]), n),
        };
        self.0 = &self.0[used..];
        Ok(Some(pkt))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    const PREFIXES: &[&str] = &["refs/heads/", "refs/tags/"];
    const MAIN: &str = "7fd1a60b01f91b314f59955a4e4d4e80d8edf11d";
    const TOPIC: &str = "b1b3f9723831141a31a1a7252a213e216ea76e56";
    const TAG: &str = "b3cbd5bbd7e81436d2eee04537ea2b4c0cad4cdf";
    const COMMIT: &str = "553c2077f0edc3d5dc5d17262f6aa498e69d6f8e";

    /// Frame `lines` as pkt-lines; `"0000"`, `"0001"` and `"0002"` pass as is.
    pub(crate) fn pkts(lines: &[&str]) -> Vec<u8> {
        let mut out = Vec::new();
        for line in lines {
            match *line {
                "0000" | "0001" | "0002" => out.extend_from_slice(line.as_bytes()),
                l => pkt(&mut out, l),
            }
        }
        out
    }

    /// What github.com answered for a public repository, verbatim.
    const GITHUB_V2: &[u8] = b"001e# service=git-upload-pack\n0000000eversion 2\n0028agent=git/github-3404ba3431c7-Linux\n0013ls-refs=unborn\n0027fetch=shallow wait-for-done filter\n0012server-option\n0017object-format=sha1\n0000";
    const GITHUB_LS_REFS: &[u8] = b"003f7fd1a60b01f91b314f59955a4e4d4e80d8edf11d refs/heads/master\n0048b1b3f9723831141a31a1a7252a213e216ea76e56 refs/heads/octocat-patch-1\n003db3cbd5bbd7e81436d2eee04537ea2b4c0cad4cdf refs/heads/test\n0000";

    #[test]
    fn github_speaks_v2_and_lists_what_was_asked() {
        assert_eq!(
            read_advertisement(GITHUB_V2, PREFIXES).unwrap(),
            Advertisement::V2 {
                object_format: Some("sha1".into()),
                // It fetches shallow history, and leaves out what is asked.
                fetch: Some(vec![
                    "shallow".into(),
                    "wait-for-done".into(),
                    "filter".into()
                ]),
            }
        );
        let refs = read_ls_refs(GITHUB_LS_REFS, PREFIXES).unwrap();
        assert!(refs.peeled.is_empty(), "branches have nothing to peel");
        assert_eq!(
            refs.ids.into_iter().collect::<Vec<_>>(),
            [
                ("refs/heads/master".to_string(), MAIN.to_string()),
                ("refs/heads/octocat-patch-1".into(), TOPIC.into()),
                ("refs/heads/test".into(), TAG.into()),
            ]
        );
    }

    /// What stock `git http-backend` (git 2.54.0) answered, verbatim, for a
    /// repository with a branch, a lightweight tag and an annotated one: no
    /// service line, and `version 2` first.
    const GIT_V2: &[u8] = b"000eversion 2\n001bagent=git/2.54.0-Linux\n0013ls-refs=unborn\n0020fetch=shallow wait-for-done\n0012server-option\n0017object-format=sha1\n0000";
    const GIT_LS_REFS: &[u8] = b"003d3081088b3c2972b40f67321ebd9923c3fedcb487 refs/heads/main\n003d3081088b3c2972b40f67321ebd9923c3fedcb487 refs/tags/light\n006a0d7fdafb51bae627a19a5dbc416be395f64bb2cf refs/tags/v1 peeled:3081088b3c2972b40f67321ebd9923c3fedcb487\n0000";

    /// git's own server frames a v2 answer as git's protocol document does:
    /// it is read like github.com's, which names the service first.
    #[test]
    fn git_s_own_server_opens_with_version_2_and_is_read() {
        assert_eq!(
            read_advertisement(GIT_V2, PREFIXES).unwrap(),
            Advertisement::V2 {
                object_format: Some("sha1".into()),
                // Without uploadpack.allowFilter, no filter.
                fetch: Some(vec!["shallow".into(), "wait-for-done".into()]),
            }
        );
        let refs = read_ls_refs(GIT_LS_REFS, PREFIXES).unwrap();
        let commit = "3081088b3c2972b40f67321ebd9923c3fedcb487";
        assert_eq!(
            refs.ids.into_iter().collect::<Vec<_>>(),
            [
                ("refs/heads/main".to_string(), commit.to_string()),
                ("refs/tags/light".into(), commit.into()),
                (
                    "refs/tags/v1".into(),
                    "0d7fdafb51bae627a19a5dbc416be395f64bb2cf".into()
                ),
            ]
        );
        assert_eq!(
            refs.peeled.into_iter().collect::<Vec<_>>(),
            [("refs/tags/v1".to_string(), commit.to_string())]
        );
    }

    #[test]
    fn the_request_names_the_command_its_format_and_its_prefixes() {
        // What `git ls-remote --symref --heads --tags` sends, less its agent
        // string: HEAD as well, and what it names.
        assert_eq!(
            ls_refs_request(PREFIXES, Some("sha1")),
            pkts(&[
                "command=ls-refs\n",
                "object-format=sha1\n",
                "0001",
                "peel\n",
                "symrefs\n",
                "ref-prefix HEAD\n",
                "ref-prefix refs/heads/\n",
                "ref-prefix refs/tags/\n",
                "0000",
            ])
        );
        assert_eq!(
            ls_refs_request(&["refs/heads/"], None),
            b"0014command=ls-refs\n00010009peel\n000csymrefs\n0014ref-prefix HEAD\n001bref-prefix refs/heads/\n0000"
        );
    }

    #[test]
    fn a_v0_server_s_listing_is_filtered_here() {
        let body = pkts(&[
            "# service=git-upload-pack\n",
            "0000",
            &format!("{MAIN} HEAD\0multi_ack side-band-64k symref=HEAD:refs/heads/main\n"),
            &format!("{MAIN} refs/heads/main\n"),
            &format!("{TOPIC} refs/pull/7/head\n"),
            &format!("{TAG} refs/tags/v1\n"),
            &format!("{COMMIT} refs/tags/v1^{{}}\n"),
            "0000",
        ]);
        let Advertisement::V0(refs) = read_advertisement(&body, PREFIXES).unwrap() else {
            panic!("a v0 advertisement");
        };
        assert_eq!(
            refs.ids.into_iter().collect::<Vec<_>>(),
            [
                ("refs/heads/main".to_string(), MAIN.to_string()),
                // The tag's own id, as a webhook reports it; not its commit.
                ("refs/tags/v1".into(), TAG.into()),
            ]
        );
        // Its commit is kept beside it, under the tag's name.
        assert_eq!(
            refs.peeled.into_iter().collect::<Vec<_>>(),
            [("refs/tags/v1".to_string(), COMMIT.to_string())]
        );
    }

    /// A v2 server names an annotated tag's commit on the tag's own line. A
    /// tag that is a commit's own name (a lightweight one) has none.
    #[test]
    fn a_v2_listing_names_each_annotated_tag_s_commit() {
        let body = pkts(&[
            &format!("{MAIN} refs/heads/main symref-target:refs/heads/x\n"),
            &format!("{TAG} refs/tags/v1 peeled:{COMMIT}\n"),
            &format!("{TOPIC} refs/tags/light\n"),
            // Not an id, and a ref that was not asked for: neither is kept.
            &format!("{TAG} refs/tags/odd peeled:not-an-id\n"),
            &format!("{TAG} refs/notes/x peeled:{COMMIT}\n"),
            "0000",
        ]);
        let refs = read_ls_refs(&body, PREFIXES).unwrap();
        assert_eq!(
            refs.ids.keys().map(String::as_str).collect::<Vec<_>>(),
            [
                "refs/heads/main",
                "refs/tags/light",
                "refs/tags/odd",
                "refs/tags/v1"
            ]
        );
        assert_eq!(refs.ids["refs/tags/v1"], TAG, "the tag's own id");
        assert_eq!(
            refs.peeled.into_iter().collect::<Vec<_>>(),
            [("refs/tags/v1".to_string(), COMMIT.to_string())]
        );
    }

    #[test]
    fn an_empty_repository_lists_nothing() {
        let v0 = pkts(&[
            "# service=git-upload-pack\n",
            "0000",
            &format!("{} capabilities^{{}}\0side-band-64k\n", "0".repeat(40)),
            "0000",
        ]);
        assert_eq!(
            read_advertisement(&v0, PREFIXES).unwrap(),
            Advertisement::V0(Refs::default())
        );
        assert_eq!(read_ls_refs(b"0000", PREFIXES).unwrap(), Refs::default());
    }

    #[test]
    fn the_default_branch_is_what_head_names() {
        // v0: a capability of the first line.
        let v0 = pkts(&[
            "# service=git-upload-pack\n",
            "0000",
            &format!("{MAIN} HEAD\0multi_ack symref=HEAD:refs/heads/main side-band-64k\n"),
            &format!("{MAIN} refs/heads/main\n"),
            "0000",
        ]);
        let Advertisement::V0(refs) = read_advertisement(&v0, PREFIXES).unwrap() else {
            panic!("a v0 answer");
        };
        assert_eq!(refs.head.as_deref(), Some("refs/heads/main"));
        assert_eq!(refs.head_id.as_deref(), Some(MAIN));
        // Capabilities that are not text say nothing of HEAD, and fail
        // nothing.
        let mut odd = pkts(&["# service=git-upload-pack\n", "0000"]);
        let line = [format!("{MAIN} HEAD\0").into_bytes(), vec![0xff, b'\n']].concat();
        odd.extend(format!("{:04x}", line.len() + 4).into_bytes());
        odd.extend(line);
        odd.extend(b"0000");
        let Advertisement::V0(refs) = read_advertisement(&odd, PREFIXES).unwrap() else {
            panic!("a v0 answer");
        };
        assert_eq!(refs.head, None);
        // v2: HEAD's own line, asked for with `symrefs`.
        let v2 = pkts(&[
            &format!("{MAIN} HEAD symref-target:refs/heads/trunk\n"),
            &format!("{MAIN} refs/heads/trunk\n"),
            "0000",
        ]);
        let refs = read_ls_refs(&v2, PREFIXES).unwrap();
        assert_eq!(refs.head.as_deref(), Some("refs/heads/trunk"));
        assert_eq!(refs.head_id.as_deref(), Some(MAIN));
        assert!(
            !refs.ids.contains_key("HEAD"),
            "HEAD is no ref of the listing's"
        );
        // An empty repository's names none.
        let unborn = pkts(&["unborn HEAD symref-target:refs/heads/main\n", "0000"]);
        assert_eq!(read_ls_refs(&unborn, PREFIXES).unwrap().head, None);
    }

    #[test]
    fn what_the_queue_cannot_store_is_left_out() {
        let body = pkts(&[
            &format!("{MAIN} refs/heads/main symref-target:refs/heads/x\n"),
            &format!("{} refs/heads/upper\n", MAIN.to_uppercase()),
            &format!("{} refs/heads/short\n", &MAIN[..12]),
            &format!("{MAIN} refs/heads/\u{7}bell\n"),
            "unborn HEAD symref-target:refs/heads/main\n",
            "0000",
        ]);
        assert_eq!(
            read_ls_refs(&body, PREFIXES)
                .unwrap()
                .ids
                .into_keys()
                .collect::<Vec<_>>(),
            ["refs/heads/main"]
        );
    }

    #[test]
    fn what_is_not_git_is_refused() {
        // A dumb server's plain listing, an HTML page, a cut-off answer, an
        // error line, and v2 without ls-refs.
        let dumb = format!("{MAIN}\trefs/heads/main\n");
        for body in [dumb.as_bytes(), b"<!DOCTYPE html>", b""] {
            assert!(read_advertisement(body, PREFIXES).is_err(), "{body:?}");
        }
        assert!(read_advertisement(&GITHUB_V2[..40], PREFIXES).is_err());
        assert!(read_ls_refs(&GITHUB_LS_REFS[..80], PREFIXES).is_err());
        let unflushed = &GITHUB_LS_REFS[..GITHUB_LS_REFS.len() - 4];
        assert_eq!(read_ls_refs(unflushed, PREFIXES), Err(truncated()));
        let err = read_ls_refs(&pkts(&["ERR access denied\n", "0000"]), PREFIXES).unwrap_err();
        assert_eq!(err.0, "the server refused: access denied");
        let v2 = pkts(&[
            "# service=git-upload-pack\n",
            "0000",
            "version 2\n",
            "fetch\n",
            "0000",
        ]);
        assert!(read_advertisement(&v2, PREFIXES).is_err());
        // The same without the service line, as git's own server frames it:
        // no ls-refs, a cut-off answer, and a first line that is neither.
        let bare = pkts(&["version 2\n", "fetch\n", "0000"]);
        assert!(read_advertisement(&bare, PREFIXES).is_err());
        assert!(read_advertisement(&GIT_V2[..GIT_V2.len() - 4], PREFIXES).is_err());
        let refs_first = pkts(&[&format!("{MAIN} refs/heads/main\n"), "0000"]);
        let err = read_advertisement(&refs_first, PREFIXES).unwrap_err();
        assert!(
            err.0.starts_with("not a git smart HTTP server"),
            "{}",
            err.0
        );
        let no_flush = pkts(&["# service=git-upload-pack\n", "version 2\n", "0000"]);
        assert!(read_advertisement(&no_flush, PREFIXES).is_err());
        assert!(
            read_ls_refs(b"0003", PREFIXES).is_err(),
            "length 3 is reserved"
        );
        assert!(read_ls_refs(b"+fff", PREFIXES).is_err(), "not hex");
    }

    /// A fetch asks for each commit at depth 1, without the files' contents
    /// where it may, as one pack that is not thin; and nothing it has.
    #[test]
    fn a_fetch_asks_for_two_commits_and_their_trees_alone() {
        assert_eq!(
            fetch_request(&[MAIN, TOPIC], Some("sha1"), true),
            pkts(&[
                "command=fetch\n",
                "object-format=sha1\n",
                "0001",
                "no-progress\n",
                "ofs-delta\n",
                "deepen 1\n",
                "filter blob:none\n",
                &format!("want {MAIN}\n"),
                &format!("want {TOPIC}\n"),
                "done\n",
                "0000",
            ])
        );
        let plain = fetch_request(&[MAIN], None, false);
        assert_eq!(
            plain,
            pkts(&[
                "command=fetch\n",
                "0001",
                "no-progress\n",
                "ofs-delta\n",
                "deepen 1\n",
                &format!("want {MAIN}\n"),
                "done\n",
                "0000",
            ])
        );
        let text = String::from_utf8(plain).unwrap();
        assert!(
            !text.contains("thin-pack") && !text.contains("have "),
            "{text}"
        );
    }

    /// A band-1 line of the pack: its band, then its bytes.
    fn band(n: u8, bytes: &[u8]) -> Vec<u8> {
        let mut out = format!("{:04x}", bytes.len() + 5).into_bytes();
        out.push(n);
        out.extend_from_slice(bytes);
        out
    }

    #[test]
    fn the_pack_is_read_out_of_its_band_past_the_sections_before_it() {
        let mut body = pkts(&[
            "shallow-info\n",
            &format!("shallow {MAIN}\n"),
            &format!("shallow {TOPIC}\n"),
            "0001",
            "packfile\n",
        ]);
        body.extend(band(2, b"Enumerating objects: 3, done.\n"));
        body.extend(band(1, b"PACK\0\0\0\x02"));
        body.extend(band(2, b"Total 3\n"));
        body.extend(band(1, b"rest"));
        body.extend(b"0000");
        assert_eq!(read_fetch(body.clone()).unwrap(), b"PACK\0\0\0\x02rest");

        // The server's error, on the third band or as the answer itself.
        let mut failed = pkts(&["packfile\n"]);
        failed.extend(band(1, b"PACK"));
        failed.extend(band(3, b"upload-pack: not our ref\n"));
        assert_eq!(
            read_fetch(failed).unwrap_err().0,
            "the server failed: upload-pack: not our ref"
        );
        let refused = pkts(&["ERR upload-pack: not our ref 7fd1a60b\n", "0000"]);
        assert_eq!(
            read_fetch(refused).unwrap_err().0,
            "the server refused: upload-pack: not our ref 7fd1a60b"
        );

        // No pack, a pack cut short, and a band git does not have.
        let none = pkts(&["shallow-info\n", &format!("shallow {MAIN}\n"), "0000"]);
        assert_eq!(read_fetch(none).unwrap_err().0, "the answer holds no pack");
        assert_eq!(
            read_fetch(b"0000".to_vec()).unwrap_err().0,
            "the answer holds no pack"
        );
        assert_eq!(
            read_fetch(body[..body.len() - 4].to_vec()),
            Err(truncated())
        );
        assert_eq!(read_fetch(vec![]), Err(truncated()));
        let mut odd = pkts(&["packfile\n"]);
        odd.extend(band(4, b"?"));
        odd.extend(b"0000");
        assert!(read_fetch(odd).is_err());
    }

    #[test]
    fn a_server_that_offers_no_fetch_says_none() {
        let v2 = pkts(&["version 2\n", "ls-refs\n", "0000"]);
        assert_eq!(
            read_advertisement(&v2, PREFIXES).unwrap(),
            Advertisement::V2 {
                object_format: None,
                fetch: None
            }
        );
        let bare = pkts(&["version 2\n", "ls-refs\n", "fetch\n", "0000"]);
        assert_eq!(
            read_advertisement(&bare, PREFIXES).unwrap(),
            Advertisement::V2 {
                object_format: None,
                fetch: Some(vec![])
            }
        );
    }
}
