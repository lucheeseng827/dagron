//! What dagron-ci reads from GitLab, and writes to it: through GitLab's REST
//! API (`/api/v4`), at the repository's own host, with the access token the
//! operator keeps for that host (`CI_GITLAB_TOKENS_DIR`), which goes to that
//! host and no other.
//!
//! - A commit's pipelines (`.dagron/ci`'s files): the repository tree, and
//!   each file raw.
//! - A file of a commit, as the GitHub front-end reads its settings and
//!   workflow files.
//! - The files a change touched: the compare API, and where it cannot list
//!   them, a shallow fetch of the change's two commits, as for GitHub.
//! - The open merge requests, for the poll sweep: each as a pull request's
//!   event, at `refs/merge-requests/{iid}/head`.
//! - A commit's statuses, as GitLab's own: `pending`, `success`, `failed`.
//! - Fleet mode's runners ([`crate::fleet`]): a project's jobs that wait, one
//!   job as it stands, a runner made for the project or its group and removed
//!   again, a merge request's projects, and a job run again.
//!
//! A project is named by its full path, URL-encoded, as GitLab takes it in
//! place of its number: `group%2Fsub%2Fproject`.
//!
//! The token goes as `Authorization: Bearer`, which GitLab takes for an
//! access token, and not as `PRIVATE-TOKEN`: a redirect to another host
//! drops the one and would carry the other.

use std::collections::BTreeSet;

use reqwest::header::ACCEPT;
use reqwest::Url;
use serde::Deserialize;

use super::{
    api_error, is_url_safe, read_capped, read_up_to, short, size, Commented, CommitStatus, Forge,
    ForgeError, Issued, Jit, PipelineFile, Pulls, Retried, StatusError, MAX_API_PAGE,
    MAX_COMPARISON, MAX_PIPELINES, MAX_PIPELINE_BYTES, PIPELINE_DIR,
};
use crate::db::Repo;
use crate::event::{Kind, NewEvent, Trust};
use crate::pipeline::compile::Changes;

/// Where GitLab's REST API is, under a host.
pub(crate) const API_PATH: &str = "/api/v4";

/// The user name git is given with a GitLab access token: any will do, and
/// this is GitLab's own for one.
pub(crate) const GIT_USER: &str = "oauth2";

/// The most files a comparison may list and still be taken as all of them.
const COMPARE_FILE_LIMIT: usize = 1000;

/// The most merge requests read a sweep: a page of 100, thirty times.
const MERGE_REQUEST_PAGES: u32 = 30;

/// The most pages of `.dagron/ci`'s listing read, of 100 entries each.
const TREE_PAGES: u32 = 10;

/// Whether GitLab says there is a page after this one.
fn has_next(response: &reqwest::Response) -> bool {
    response
        .headers()
        .get("x-next-page")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| !v.trim().is_empty())
}

/// The ref GitLab is told a commit status is of, from the event's ref: a
/// branch's name or a tag's, and a merge request's source branch, whose
/// pipeline is the one the merge request shows. Only where that branch is
/// the project's own, which a GitLab merge request's `trust` says
/// ([`merge_request_trust`]): a fork's is in the fork, and the project the
/// status goes to may have none of that name, or another's, so a fork's
/// merge request names none. GitLab gathers a commit's
/// statuses of one ref into one pipeline of its own, an external one where
/// GitLab ran none, so a run's jobs read as one pipeline there. A merge
/// train's commit is the train's ref's, as GitLab names its pipeline: the
/// statuses join that pipeline, which the train merges on
/// ([`crate::gitlab::train_of`]). `None` for any other ref: GitLab then picks
/// one.
pub(crate) fn status_ref(git_ref: &str, head_ref: Option<&str>, trust: &str) -> Option<String> {
    let name = if let Some(name) = git_ref
        .strip_prefix("refs/heads/")
        .or_else(|| git_ref.strip_prefix("refs/tags/"))
    {
        Some(name)
    } else if crate::gitlab::train_of(git_ref).is_some() {
        Some(git_ref)
    } else if git_ref.starts_with("refs/merge-requests/") && trust == "trusted" {
        head_ref.map(|r| r.strip_prefix("refs/heads/").unwrap_or(r))
    } else {
        None
    };
    // GitLab takes a ref of 255 characters at most.
    name.filter(|n| !n.is_empty() && n.len() <= 255)
        .map(str::to_string)
}

/// Whether `repo` is a GitLab project.
pub(crate) fn is_gitlab(repo: &Repo) -> bool {
    repo.forge == "gitlab"
}

/// GitLab's state for one of dagron-ci's statuses, which are GitHub's
/// words: `error` has no word of its own there, and is a failure.
pub(crate) fn state_of(github: &str) -> &'static str {
    match github {
        "success" => "success",
        "failure" | "error" => "failed",
        _ => "pending",
    }
}

#[derive(Deserialize)]
struct TreeEntry {
    #[serde(rename = "type")]
    kind: String,
    name: String,
    path: String,
}

#[derive(Deserialize)]
struct Comparison {
    #[serde(default)]
    diffs: Vec<Diff>,
    #[serde(default)]
    compare_timeout: bool,
}

#[derive(Deserialize)]
struct Diff {
    old_path: String,
    new_path: String,
}

/// The commit `repository/merge_base` names.
#[derive(Deserialize)]
struct MergeBase {
    id: String,
}

#[derive(Deserialize)]
struct MergeRequest {
    iid: i64,
    sha: Option<String>,
    source_branch: Option<String>,
    target_branch: Option<String>,
    source_project_id: Option<i64>,
    target_project_id: Option<i64>,
    author: Option<Author>,
    diff_refs: Option<DiffRefs>,
}

#[derive(Deserialize)]
struct Author {
    username: String,
}

#[derive(Deserialize)]
struct DiffRefs {
    base_sha: Option<String>,
}

/// A merge request's trust: one whose source branch is the project's own
/// is trusted, since only someone who may push to the project can push
/// one; one from a fork is not. The webhook and the poll sweep both decide
/// it here. GitLab says nothing of what the author is to the project, so
/// no event of it carries an association.
pub(crate) fn merge_request_trust(source: Option<i64>, target: Option<i64>) -> Trust {
    match (source, target) {
        (Some(s), Some(t)) if s == t => Trust::Trusted,
        _ => Trust::Untrusted,
    }
}

impl Forge {
    /// GitLab's API URL for `tail` of `repo`'s project.
    fn gitlab_url(&self, repo: &Repo, tail: &[&str]) -> Result<Url, String> {
        let api = self.api_for(repo);
        let mut url = Url::parse(&api).map_err(|e| format!("{api}: {e}"))?;
        url.path_segments_mut()
            .map_err(|()| format!("{api} takes no path"))?
            .pop_if_empty()
            .push("projects")
            .push(&repo.full_name)
            .extend(tail);
        Ok(url)
    }

    /// `GET` of `tail` of `repo`'s project, with the host's token where the
    /// operator keeps one: the answer, and whether a token went with it.
    async fn gitlab_get(
        &self,
        repo: &Repo,
        tail: &[&str],
        query: &[(&str, &str)],
        what: &str,
    ) -> Result<(reqwest::Response, bool), ForgeError> {
        if !is_url_safe(repo) {
            return Err(ForgeError::Failed(format!(
                "{}/{} cannot be put in a URL as it is; reconnect it",
                repo.forge_host, repo.full_name
            )));
        }
        let mut url = self
            .gitlab_url(repo, tail)
            .map_err(|e| ForgeError::Failed(format!("{what}: {e}")))?;
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query);
        }
        let api = self.api_for(repo);
        let mut request = self
            .http
            .to(&api)
            .get(url)
            .header(ACCEPT, "application/json");
        let token = self.gitlab_token(repo);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .map_err(|e| ForgeError::Failed(format!("{what}: {e}")))?;
        Ok((response, token.is_some()))
    }

    /// The token the operator keeps for `repo`'s host, where it keeps one.
    pub(crate) fn gitlab_token(&self, repo: &Repo) -> Option<&str> {
        self.gitlab_tokens
            .get(&repo.forge_host.to_ascii_lowercase())
            .map(String::as_str)
    }

    /// [`Forge::pipelines`], of a GitLab project.
    pub(super) async fn gitlab_pipelines(
        &self,
        repo: &Repo,
        sha: &str,
    ) -> Result<Option<Vec<PipelineFile>>, ForgeError> {
        const WHAT: &str = "listing .dagron/ci";
        let mut entries: Vec<TreeEntry> = Vec::new();
        let mut page = 1;
        loop {
            let at = page.to_string();
            let (response, credentialed) = self
                .gitlab_get(
                    repo,
                    &["repository", "tree"],
                    &[
                        ("path", PIPELINE_DIR),
                        ("ref", sha),
                        ("per_page", "100"),
                        ("page", &at),
                    ],
                    WHAT,
                )
                .await?;
            match response.status().as_u16() {
                200 => {}
                // No such directory at this commit (and a private project an
                // anonymous caller may not read).
                404 => return Ok(None),
                _ => return Err(api_error(&response, repo, credentialed, WHAT)),
            }
            let next = has_next(&response);
            let body = read_capped(response, MAX_API_PAGE, WHAT).await?;
            let listed: Vec<TreeEntry> = serde_json::from_slice(&body)
                .map_err(|e| ForgeError::Failed(format!("{WHAT}: {e}")))?;
            entries.extend(listed.into_iter().filter(|e| {
                e.kind == "blob" && (e.name.ends_with(".yaml") || e.name.ends_with(".yml"))
            }));
            if !next {
                break;
            }
            page += 1;
            if page > TREE_PAGES {
                return Err(ForgeError::Failed(format!(
                    "{WHAT}: it holds more than {} entries, which is more than dagron-ci reads",
                    TREE_PAGES * 100
                )));
            }
        }
        if entries.is_empty() {
            return Ok(None);
        }
        entries.sort_by(|a, b| a.path.cmp(&b.path));
        let mut files = Vec::with_capacity(entries.len());
        for (i, entry) in entries.into_iter().enumerate() {
            let direct = entry
                .path
                .strip_prefix(PIPELINE_DIR)
                .and_then(|p| p.strip_prefix('/'));
            if direct != Some(entry.name.as_str())
                || entry.name.contains(|c: char| c.is_control() || c == '/')
            {
                files.push(PipelineFile {
                    path: format!("{:?}", entry.path),
                    text: Err("is not a file name dagron-ci takes: it must be printable".into()),
                });
                continue;
            }
            let text = if i >= MAX_PIPELINES {
                Err(format!(
                    "not read: a commit may hold at most {MAX_PIPELINES} pipelines"
                ))
            } else {
                match self.gitlab_file(repo, &entry.path, sha).await? {
                    Some(text) => text,
                    None => Err("was listed, and is not there to read".into()),
                }
            };
            files.push(PipelineFile {
                path: entry.path,
                text,
            });
        }
        Ok(Some(files))
    }

    /// [`Forge::file`], of a GitLab project: `None` where the commit has no
    /// such file.
    pub(super) async fn gitlab_file(
        &self,
        repo: &Repo,
        path: &str,
        sha: &str,
    ) -> Result<Option<Result<String, String>>, ForgeError> {
        let what = format!("reading {path}");
        let (response, credentialed) = self
            .gitlab_get(
                repo,
                &["repository", "files", path, "raw"],
                &[("ref", sha)],
                &what,
            )
            .await?;
        match response.status().as_u16() {
            200 => {}
            404 => return Ok(None),
            _ => return Err(api_error(&response, repo, credentialed, &what)),
        }
        Ok(Some(
            match read_up_to(response, MAX_PIPELINE_BYTES, &what).await? {
                Some(bytes) => {
                    String::from_utf8(bytes).map_err(|_| "is not UTF-8 text".to_string())
                }
                None => Err(format!(
                    "is over the {} a pipeline may be",
                    size(MAX_PIPELINE_BYTES)
                )),
            },
        ))
    }

    /// [`Forge::changed_files`], of a GitLab project. GitLab compares from
    /// the two commits' merge base (`straight` is false), and so does a
    /// shallow fetch, from the merge base GitLab names
    /// ([`Forge::gitlab_merge_base`]): a merge request's base commit is that
    /// already (`diff_refs.base_sha`), but a push's, the branch's tip before,
    /// is not where the push was forced.
    pub(super) async fn gitlab_changed_files(
        &self,
        repo: &Repo,
        base: &str,
        head: &str,
    ) -> Result<Changes, ForgeError> {
        const WHAT: &str = "comparing commits";
        let (response, credentialed) = self
            .gitlab_get(
                repo,
                &["repository", "compare"],
                &[("from", base), ("to", head)],
                WHAT,
            )
            .await?;
        match response.status().as_u16() {
            200 => {}
            404 | 422 => {
                return Ok(Changes::Unlisted(format!(
                    "the forge cannot compare {} with {}",
                    short(base),
                    short(head)
                )))
            }
            _ => return Err(api_error(&response, repo, credentialed, WHAT)),
        }
        let Some(body) = read_up_to(response, MAX_COMPARISON, WHAT).await? else {
            let why = format!("the comparison is over {}", size(MAX_COMPARISON));
            return Ok(self.fetched_or(repo, base, head, None, why).await);
        };
        let comparison: Comparison = serde_json::from_slice(&body)
            .map_err(|e| ForgeError::Failed(format!("{WHAT}: {e}")))?;
        if comparison.compare_timeout {
            let why = "the forge took too long to compare the commits".to_string();
            return Ok(self.fetched_or(repo, base, head, None, why).await);
        }
        if comparison.diffs.len() >= COMPARE_FILE_LIMIT {
            let why = format!(
                "the change touches {COMPARE_FILE_LIMIT} files or more, more than the forge lists"
            );
            return Ok(self.fetched_or(repo, base, head, None, why).await);
        }
        let mut names = BTreeSet::new();
        for diff in comparison.diffs {
            names.insert(diff.old_path);
            names.insert(diff.new_path);
        }
        Ok(Changes::Listed(names.into_iter().collect()))
    }

    /// The merge base of `base` and `head` (`repository/merge_base`), which
    /// GitLab's compare compares from. The error says why GitLab named none.
    pub(super) async fn gitlab_merge_base(
        &self,
        repo: &Repo,
        base: &str,
        head: &str,
    ) -> Result<String, String> {
        const WHAT: &str = "finding the merge base";
        let said = |e: ForgeError| match e {
            ForgeError::Failed(why) | ForgeError::RateLimited { why, .. } => why,
        };
        let (response, credentialed) = self
            .gitlab_get(
                repo,
                &["repository", "merge_base"],
                &[("refs[]", base), ("refs[]", head)],
                WHAT,
            )
            .await
            .map_err(said)?;
        if response.status().as_u16() != 200 {
            return Err(said(api_error(&response, repo, credentialed, WHAT)));
        }
        let body = read_capped(response, MAX_API_PAGE, WHAT)
            .await
            .map_err(said)?;
        let commit: MergeBase =
            serde_json::from_slice(&body).map_err(|e| format!("{WHAT}: {e}"))?;
        Some(commit.id)
            .filter(|id| crate::event::is_commit_id(id))
            .ok_or_else(|| format!("{WHAT}: the forge names none"))
    }

    /// The project's open merge requests, each as a pull request's event,
    /// every page: what the poll sweep reads where no webhook says.
    pub(super) async fn gitlab_merge_requests(&self, repo: &Repo) -> Result<Pulls, ForgeError> {
        const WHAT: &str = "listing merge requests";
        let mut events = Vec::new();
        for page in 1..=MERGE_REQUEST_PAGES {
            let page = page.to_string();
            let (response, credentialed) = self
                .gitlab_get(
                    repo,
                    &["merge_requests"],
                    &[("state", "opened"), ("per_page", "100"), ("page", &page)],
                    WHAT,
                )
                .await?;
            if response.status().as_u16() != 200 {
                return Err(api_error(&response, repo, credentialed, WHAT));
            }
            let next = has_next(&response);
            let body = read_capped(response, MAX_API_PAGE, WHAT).await?;
            let listed: Vec<MergeRequest> = serde_json::from_slice(&body)
                .map_err(|e| ForgeError::Failed(format!("{WHAT}: {e}")))?;
            for mr in listed {
                let Some(sha) = mr.sha.filter(|s| !s.is_empty()) else {
                    continue;
                };
                let trust = merge_request_trust(mr.source_project_id, mr.target_project_id);
                events.push(NewEvent {
                    kind: Kind::PullRequest,
                    git_ref: format!("refs/merge-requests/{}/head", mr.iid),
                    sha,
                    commit: None,
                    // Seldom there: GitLab's listing leaves it out, and the
                    // sweep asks each merge request it queues for it.
                    base_sha: mr
                        .diff_refs
                        .and_then(|d| d.base_sha)
                        .filter(|s| crate::event::is_commit_id(s)),
                    base_ref: mr.target_branch,
                    pr: Some(mr.iid),
                    actor: mr.author.map(|a| a.username),
                    trust,
                    association: None,
                    head_ref: mr.source_branch,
                });
            }
            if !next {
                return Ok(Pulls::Listed {
                    events,
                    complete: true,
                    etag: None,
                });
            }
        }
        tracing::warn!(
            repo = %repo.full_name,
            "more than {} open merge requests; the rest are left to webhooks",
            MERGE_REQUEST_PAGES * 100
        );
        Ok(Pulls::Listed {
            events,
            complete: false,
            etag: None,
        })
    }

    /// [`Forge::merge_request_base`]: `GET /projects/{path}/merge_requests/{iid}`.
    pub(super) async fn gitlab_merge_request_base(
        &self,
        repo: &Repo,
        iid: i64,
    ) -> Result<Option<String>, ForgeError> {
        const WHAT: &str = "reading a merge request";
        let iid = iid.to_string();
        let (response, credentialed) = self
            .gitlab_get(repo, &["merge_requests", &iid], &[], WHAT)
            .await?;
        match response.status().as_u16() {
            200 => {}
            404 => return Ok(None),
            _ => return Err(api_error(&response, repo, credentialed, WHAT)),
        }
        let body = read_capped(response, MAX_API_PAGE, WHAT).await?;
        let mr: MergeRequest = serde_json::from_slice(&body)
            .map_err(|e| ForgeError::Failed(format!("{WHAT}: {e}")))?;
        Ok(mr
            .diff_refs
            .and_then(|d| d.base_sha)
            .filter(|s| crate::event::is_commit_id(s)))
    }

    /// [`Forge::post_comment`], on a GitLab project: the merge request
    /// `iid`'s note, a new one (`POST
    /// /projects/{path}/merge_requests/{iid}/notes`) or the one `id` names
    /// with `body` in place of what it said (`PUT …/notes/{id}`), with the
    /// host's token. A note that is not there to be written again is
    /// [`Commented::Gone`].
    pub(super) async fn gitlab_post_note(
        &self,
        repo: &Repo,
        iid: i64,
        body: &str,
        id: Option<&str>,
    ) -> Result<Commented, StatusError> {
        let what = format!("writing the note on merge request !{iid}");
        let token = match self.comments(repo) {
            true => self.gitlab_token(repo),
            false => None,
        };
        let Some(token) = token else {
            return Err(StatusError::Refused(format!(
                "{what}: a note goes to a GitLab project with the token CI_GITLAB_TOKENS_DIR keeps \
                 for its host, {}, where CI_GITLAB_MR_NOTES is on",
                repo.forge_host
            )));
        };
        if id.is_some_and(|id| id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit())) {
            return Err(StatusError::Failed(format!(
                "{what}: {id:?} is not a note's id"
            )));
        }
        let iid = iid.to_string();
        let mut tail = vec!["merge_requests", iid.as_str(), "notes"];
        tail.extend(id);
        let url = self
            .gitlab_url(repo, &tail)
            .map_err(|e| StatusError::Failed(format!("{what}: {e}")))?;
        let api = self.api_for(repo);
        let client = self.http.to(&api);
        let request = match id {
            Some(_) => client.put(url),
            None => client.post(url),
        };
        let response = request
            .header(ACCEPT, "application/json")
            .bearer_auth(token)
            .json(&serde_json::json!({ "body": super::comment_text(body) }))
            .send()
            .await
            .map_err(|e| StatusError::Failed(format!("{what}: {e}")))?;
        let code = response.status().as_u16();
        if id.is_some() && code == 404 {
            return Ok(Commented::Gone);
        }
        if !response.status().is_success() {
            if let ForgeError::RateLimited { wait, why } = api_error(&response, repo, true, &what) {
                return Err(StatusError::RateLimited { wait, why });
            }
            return Err(match code {
                401 | 403 | 404 => StatusError::Refused(format!(
                    "{what}: HTTP {code}: the token CI_GITLAB_TOKENS_DIR keeps for {} may not \
                     write this project's merge request notes (it needs the api scope, and \
                     Reporter or more)",
                    repo.forge_host
                )),
                _ => StatusError::Failed(format!("{what}: HTTP {code}")),
            });
        }
        #[derive(Deserialize)]
        struct Made {
            id: u64,
        }
        let body = read_capped(response, MAX_API_PAGE, &what)
            .await
            .map_err(|e| match e {
                ForgeError::Failed(why) | ForgeError::RateLimited { why, .. } => {
                    StatusError::Failed(why)
                }
            })?;
        serde_json::from_slice::<Made>(&body)
            .map(|made| Commented::Written(made.id))
            .map_err(|e| StatusError::Failed(format!("{what}: GitLab's answer names no note: {e}")))
    }

    /// [`Forge::write_issue`], on a GitLab project: `POST
    /// /projects/{path}/issues`, or `PUT …/issues/{iid}` with the issue's
    /// new description. One a person closed is left closed, and said to be.
    pub(super) async fn gitlab_write_issue(
        &self,
        repo: &Repo,
        title: &str,
        body: &str,
        iid: Option<&str>,
    ) -> Result<Issued, StatusError> {
        let what = match iid {
            Some(iid) => format!("writing issue #{iid}"),
            None => "opening an issue".to_string(),
        };
        let token = match self.issues(repo) {
            true => self.gitlab_token(repo),
            false => None,
        };
        let Some(token) = token else {
            return Err(StatusError::Refused(format!(
                "{what}: an issue goes to a GitLab project with the token CI_GITLAB_TOKENS_DIR \
                 keeps for its host, {}, where CI_FLAKE_ISSUES is set",
                repo.forge_host
            )));
        };
        if iid.is_some_and(|iid| iid.is_empty() || !iid.bytes().all(|b| b.is_ascii_digit())) {
            return Err(StatusError::Failed(format!(
                "{what}: not an issue's number"
            )));
        }
        let mut tail = vec!["issues"];
        tail.extend(iid);
        let url = self
            .gitlab_url(repo, &tail)
            .map_err(|e| StatusError::Failed(format!("{what}: {e}")))?;
        let api = self.api_for(repo);
        let client = self.http.to(&api);
        let description = super::comment_text(body);
        let (request, sent) = match iid {
            Some(_) => (
                client.put(url),
                serde_json::json!({ "description": description }),
            ),
            None => (
                client.post(url),
                serde_json::json!({ "title": title, "description": description }),
            ),
        };
        let response = request
            .header(ACCEPT, "application/json")
            .bearer_auth(token)
            .json(&sent)
            .send()
            .await
            .map_err(|e| StatusError::Failed(format!("{what}: {e}")))?;
        let code = response.status().as_u16();
        if iid.is_some() && code == 404 {
            return Ok(Issued::Gone);
        }
        if !response.status().is_success() {
            if let ForgeError::RateLimited { wait, why } = api_error(&response, repo, true, &what) {
                return Err(StatusError::RateLimited { wait, why });
            }
            return Err(match code {
                401 | 403 | 404 => StatusError::Refused(format!(
                    "{what}: HTTP {code}: the token CI_GITLAB_TOKENS_DIR keeps for {} may not \
                     write this project's issues (it needs the api scope, Reporter or more, \
                     and the project's issues on)",
                    repo.forge_host
                )),
                _ => StatusError::Failed(format!("{what}: HTTP {code}")),
            });
        }
        #[derive(Deserialize)]
        struct Made {
            iid: u64,
            state: String,
            closed_at: Option<String>,
        }
        let body = read_capped(response, MAX_API_PAGE, &what)
            .await
            .map_err(|e| match e {
                ForgeError::Failed(why) | ForgeError::RateLimited { why, .. } => {
                    StatusError::Failed(why)
                }
            })?;
        let made = serde_json::from_slice::<Made>(&body).map_err(|e| {
            StatusError::Failed(format!("{what}: GitLab's answer names no issue: {e}"))
        })?;
        Ok(match made.state.as_str() {
            "closed" => Issued::Closed(made.closed_at),
            _ => Issued::Written(made.iid.to_string()),
        })
    }

    /// [`Forge::post_status`], on a GitLab project: `POST
    /// /projects/{path}/statuses/{sha}`, its state in GitLab's words. A
    /// status GitLab already has in that state is posted already.
    pub(super) async fn gitlab_post_status(
        &self,
        repo: &Repo,
        sha: &str,
        status: &CommitStatus,
    ) -> Result<(), StatusError> {
        let what = format!("posting {:?} on {}", status.context, short(sha));
        let Some(token) = self.gitlab_token(repo) else {
            return Err(StatusError::Refused(format!(
                "{what}: statuses go to a GitLab project with the token CI_GITLAB_TOKENS_DIR keeps \
                 for its host, {}, and it keeps none",
                repo.forge_host
            )));
        };
        let url = self
            .gitlab_url(repo, &["statuses", sha])
            .map_err(|e| StatusError::Failed(format!("{what}: {e}")))?;
        let mut body = serde_json::json!({
            "state": state_of(status.state),
            "name": status.context,
            "description": status.description,
        });
        if let Some(target) = &status.target_url {
            body["target_url"] = serde_json::json!(target);
        }
        if let Some(pipeline_ref) = &status.pipeline_ref {
            body["ref"] = serde_json::json!(pipeline_ref);
        }
        let api = self.api_for(repo);
        let response = self
            .http
            .to(&api)
            .post(url)
            .header(ACCEPT, "application/json")
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .map_err(|e| StatusError::Failed(format!("{what}: {e}")))?;
        let code = response.status().as_u16();
        if response.status().is_success() {
            return Ok(());
        }
        if let ForgeError::RateLimited { wait, why } = api_error(&response, repo, true, &what) {
            return Err(StatusError::RateLimited { wait, why });
        }
        let said = read_up_to(response, 4096, &what)
            .await
            .ok()
            .flatten()
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .unwrap_or_default();
        // GitLab will not move a status to the state it is in.
        if code == 400 && said.contains("Cannot transition status") {
            return Ok(());
        }
        Err(match code {
            401 | 403 | 404 => StatusError::Refused(format!(
                "{what}: HTTP {code}: the token CI_GITLAB_TOKENS_DIR keeps for {} may not post \
                 this project's statuses (it needs the api scope, and Developer)",
                repo.forge_host
            )),
            _ => StatusError::Failed(format!("{what}: HTTP {code}")),
        })
    }
}

/// How many pages of a project's waiting jobs a look reads, of 100 each.
const PENDING_PAGES: u32 = 5;

/// The shortest time GitLab lets a runner give a job.
const MIN_RUNNER_TIMEOUT: u64 = 600;

/// What fleet mode is told of a GitLab project: its number, and its
/// namespace's where that is a group, which a group's runner is made for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GitlabProject {
    pub id: u64,
    pub group: Option<u64>,
}

/// A GitLab pipeline as it stands: the ref it ran at (a branch's, a tag's,
/// or a merge request's `refs/merge-requests/{iid}/{head,merge,train}`) and
/// its status, each where GitLab gives one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GitlabPipeline {
    pub git_ref: Option<String>,
    pub status: Option<String>,
}

#[derive(Deserialize)]
struct ProjectAnswer {
    id: u64,
    namespace: Option<Namespace>,
}

#[derive(Deserialize)]
struct Namespace {
    id: u64,
    kind: Option<String>,
}

/// A job, as GitLab's API answers for one.
#[derive(Deserialize)]
struct JobAnswer {
    id: u64,
    status: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    stage: Option<String>,
    #[serde(default, rename = "ref")]
    git_ref: Option<String>,
    #[serde(default)]
    tag_list: Vec<String>,
    #[serde(default)]
    pipeline: Option<PipelineOf>,
    #[serde(default)]
    commit: Option<CommitOf>,
    #[serde(default)]
    runner: Option<RunnerOf>,
}

#[derive(Deserialize)]
struct PipelineOf {
    id: u64,
    #[serde(default)]
    sha: Option<String>,
}

#[derive(Deserialize)]
struct CommitOf {
    id: String,
}

#[derive(Deserialize)]
struct RunnerOf {
    id: Option<u64>,
    description: Option<String>,
}

/// What a GitLab job's status is to fleet mode: one that waits for a
/// runner (or will), one a runner has, and one that is over, with how it
/// ended in GitHub's words, which the fleet's rows keep. `None` for one that
/// is not asking for a runner and may never (`manual`, `scheduled`).
pub(crate) fn job_action(status: &str) -> Option<(crate::github::JobAction, Option<&'static str>)> {
    use crate::github::JobAction;
    Some(match status {
        "created" | "pending" | "waiting_for_resource" => (JobAction::Queued, None),
        "preparing" | "running" => (JobAction::InProgress, None),
        "success" => (JobAction::Completed, Some("success")),
        "failed" => (JobAction::Completed, Some("failure")),
        "canceled" | "canceling" => (JobAction::Completed, Some("cancelled")),
        "skipped" => (JobAction::Completed, Some("skipped")),
        _ => return None,
    })
}

/// The most labels a job is kept with, and the longest a text, as a
/// workflow job's are ([`crate::github::Job`]).
const MAX_JOB_TAGS: usize = 32;
const MAX_JOB_TEXT: usize = 255;

pub(crate) fn kept(text: String) -> String {
    text.chars()
        .filter(|c| !c.is_control())
        .take(MAX_JOB_TEXT)
        .collect()
}

impl JobAnswer {
    /// The fleet's job, which is a workflow job's shape, with its ref: a
    /// pipeline is a workflow run, and a job's tags its labels. `None` for
    /// one the fleet has nothing to do with, or that is no job's. A ref
    /// GitLab did not give is `None` too, and told apart as a fork's.
    fn job(self, repo: &Repo) -> Option<(crate::github::Job, Option<String>)> {
        let (action, conclusion) = job_action(&self.status)?;
        let pipeline = self.pipeline?;
        let sha = self
            .commit
            .map(|c| c.id)
            .or(pipeline.sha)
            .filter(|sha| crate::event::is_commit_id(sha))?;
        if self.tag_list.len() > MAX_JOB_TAGS {
            return None;
        }
        let (runner_id, runner_name) = match self.runner {
            Some(runner) => (runner.id, runner.description.map(kept)),
            None => (None, None),
        };
        let job = crate::github::Job {
            forge_host: repo.forge_host.to_ascii_lowercase(),
            full_name: repo.full_name.to_ascii_lowercase(),
            action,
            id: self.id,
            run_id: pipeline.id,
            run_attempt: 1,
            name: kept(self.name.unwrap_or_default()),
            workflow: self.stage.map(kept).filter(|s| !s.is_empty()),
            labels: self.tag_list.into_iter().map(kept).collect(),
            head_sha: sha,
            runner_id,
            runner_name: runner_name.filter(|n| !n.is_empty()),
            conclusion: conclusion.map(str::to_string),
        };
        Some((job, self.git_ref.filter(|git_ref| !git_ref.is_empty())))
    }
}

impl Forge {
    /// A request of `path` (from the API's root) at `repo`'s host, with the
    /// host's token: fleet mode's calls, none of which is made without one.
    fn gitlab_call(
        &self,
        repo: &Repo,
        method: reqwest::Method,
        path: &[&str],
        what: &str,
    ) -> Result<reqwest::RequestBuilder, ForgeError> {
        let Some(token) = self.gitlab_token(repo) else {
            return Err(ForgeError::Failed(format!(
                "{what}: there is no token for {} (CI_GITLAB_TOKENS_DIR), and runners are \
                 made and removed with one",
                repo.forge_host
            )));
        };
        if !is_url_safe(repo) {
            return Err(ForgeError::Failed(format!(
                "{what}: {}/{} cannot be put in a URL as it is; reconnect it",
                repo.forge_host, repo.full_name
            )));
        }
        let api = self.api_for(repo);
        let mut url =
            Url::parse(&api).map_err(|e| ForgeError::Failed(format!("{what}: {api}: {e}")))?;
        url.path_segments_mut()
            .map_err(|()| ForgeError::Failed(format!("{what}: {api} takes no path")))?
            .pop_if_empty()
            .extend(path);
        Ok(self
            .http
            .to(&api)
            .request(method, url)
            .header(ACCEPT, "application/json")
            .bearer_auth(token))
    }

    /// `repo`'s project's number, and its group's: `GET /projects/{path}`.
    pub(crate) async fn gitlab_project(&self, repo: &Repo) -> Result<GitlabProject, ForgeError> {
        const WHAT: &str = "reading the project";
        let (response, credentialed) = self.gitlab_get(repo, &[], &[], WHAT).await?;
        if response.status().as_u16() != 200 {
            return Err(api_error(&response, repo, credentialed, WHAT));
        }
        let body = read_capped(response, MAX_API_PAGE, WHAT).await?;
        let project: ProjectAnswer = serde_json::from_slice(&body)
            .map_err(|e| ForgeError::Failed(format!("{WHAT}: {e}")))?;
        Ok(GitlabProject {
            id: project.id,
            group: project
                .namespace
                .filter(|n| n.kind.as_deref() == Some("group"))
                .map(|n| n.id),
        })
    }

    /// Make a runner named `name` for `repo`'s project, or for its group
    /// where `group` says, taking the jobs whose tags are among `tags` and
    /// no untagged one: `POST /user/runners`. What comes back is the
    /// runner's token, which is a secret: whoever holds it is the runner.
    pub(crate) async fn gitlab_create_runner(
        &self,
        repo: &Repo,
        name: &str,
        tags: &[String],
        group: bool,
        timeout_secs: u64,
    ) -> Result<Jit, ForgeError> {
        let what = format!("making the runner {name:?}");
        let project = self.gitlab_project(repo).await?;
        let mut body = serde_json::json!({
            "description": name,
            "tag_list": tags,
            "run_untagged": false,
            "maximum_timeout": timeout_secs.max(MIN_RUNNER_TIMEOUT),
        });
        if group {
            let Some(id) = project.group else {
                return Err(ForgeError::Failed(format!(
                    "{what}: the project's namespace is no group, and a group's runner is made \
                     for one"
                )));
            };
            body["runner_type"] = serde_json::json!("group_type");
            body["group_id"] = serde_json::json!(id);
        } else {
            body["runner_type"] = serde_json::json!("project_type");
            body["project_id"] = serde_json::json!(project.id);
            body["locked"] = serde_json::json!(true);
        }
        let response = self
            .gitlab_call(repo, reqwest::Method::POST, &["user", "runners"], &what)?
            .json(&body)
            .send()
            .await
            .map_err(|e| ForgeError::Failed(format!("{what}: {e}")))?;
        if !response.status().is_success() {
            return Err(api_error(&response, repo, true, &what));
        }
        #[derive(Deserialize)]
        struct Made {
            id: u64,
            token: String,
        }
        let made: Made = response.json().await.map_err(|e| {
            ForgeError::Failed(format!("{what}: GitLab's answer is no runner's: {e}"))
        })?;
        if made.token.is_empty() {
            return Err(ForgeError::Failed(format!(
                "{what}: GitLab's answer has no token"
            )));
        }
        Ok(Jit {
            runner_id: made.id,
            config: made.token,
        })
    }

    /// Remove the runner `runner_id`: `DELETE /runners/{id}`. `true` once
    /// GitLab has none by that id. `false` where it is running a job, as
    /// `GET /runners/{id}/jobs?status=running` says: GitLab would remove a
    /// runner with a job, and the job would fail with it.
    pub(crate) async fn gitlab_remove_runner(
        &self,
        repo: &Repo,
        runner_id: &str,
    ) -> Result<bool, ForgeError> {
        let what = format!("removing the runner {runner_id}");
        if runner_id.is_empty() || !runner_id.bytes().all(|b| b.is_ascii_digit()) {
            return Ok(true);
        }
        let response = self
            .gitlab_call(
                repo,
                reqwest::Method::GET,
                &["runners", runner_id, "jobs"],
                &what,
            )?
            .query(&[("status", "running")])
            .send()
            .await
            .map_err(|e| ForgeError::Failed(format!("{what}: {e}")))?;
        match response.status().as_u16() {
            200 => {
                let body = read_capped(response, MAX_API_PAGE, &what).await?;
                let running: Vec<serde_json::Value> = serde_json::from_slice(&body)
                    .map_err(|e| ForgeError::Failed(format!("{what}: {e}")))?;
                if !running.is_empty() {
                    return Ok(false);
                }
            }
            404 => return Ok(true),
            _ => return Err(api_error(&response, repo, true, &what)),
        }
        let response = self
            .gitlab_call(
                repo,
                reqwest::Method::DELETE,
                &["runners", runner_id],
                &what,
            )?
            .send()
            .await
            .map_err(|e| ForgeError::Failed(format!("{what}: {e}")))?;
        match response.status().as_u16() {
            200..=299 | 404 => Ok(true),
            _ => Err(api_error(&response, repo, true, &what)),
        }
    }

    /// The jobs of `repo`'s project that wait for a runner, as GitLab lists
    /// them (`GET /projects/{path}/jobs?scope[]=pending`), each with its ref,
    /// at most [`PENDING_PAGES`] pages of them, and whether that was all.
    pub(crate) async fn gitlab_pending_jobs(
        &self,
        repo: &Repo,
    ) -> Result<(Vec<(crate::github::Job, Option<String>)>, bool), ForgeError> {
        const WHAT: &str = "listing the jobs that wait";
        let mut jobs = Vec::new();
        for page in 1..=PENDING_PAGES {
            let page = page.to_string();
            let query = [("scope[]", "pending"), ("per_page", "100"), ("page", &page)];
            let (response, credentialed) = self.gitlab_get(repo, &["jobs"], &query, WHAT).await?;
            if response.status().as_u16() != 200 {
                return Err(api_error(&response, repo, credentialed, WHAT));
            }
            let more = has_next(&response);
            let body = read_capped(response, MAX_API_PAGE, WHAT).await?;
            let listed: Vec<JobAnswer> = serde_json::from_slice(&body)
                .map_err(|e| ForgeError::Failed(format!("{WHAT}: {e}")))?;
            jobs.extend(listed.into_iter().filter_map(|job| job.job(repo)));
            if !more {
                return Ok((jobs, true));
            }
        }
        Ok((jobs, false))
    }

    /// How the job `job_id` of `repo`'s project stands (`GET
    /// /projects/{path}/jobs/{id}`): `None` where GitLab has no such job, or
    /// it is one the fleet has nothing to do with.
    pub(crate) async fn gitlab_job(
        &self,
        repo: &Repo,
        job_id: &str,
    ) -> Result<Option<crate::github::Job>, ForgeError> {
        const WHAT: &str = "reading a job";
        if job_id.is_empty() || !job_id.bytes().all(|b| b.is_ascii_digit()) {
            return Ok(None);
        }
        let (response, credentialed) = self.gitlab_get(repo, &["jobs", job_id], &[], WHAT).await?;
        match response.status().as_u16() {
            200 => {}
            404 => return Ok(None),
            _ => return Err(api_error(&response, repo, credentialed, WHAT)),
        }
        let body = read_capped(response, MAX_API_PAGE, WHAT).await?;
        let job: JobAnswer = serde_json::from_slice(&body)
            .map_err(|e| ForgeError::Failed(format!("{WHAT}: {e}")))?;
        Ok(job.job(repo).map(|(job, _)| job))
    }

    /// The ref the pipeline `pipeline` of `repo`'s project ran at (`GET
    /// /projects/{path}/pipelines/{id}`): a branch's, a tag's, or a merge
    /// request's (`refs/merge-requests/{iid}/head`). `None` where GitLab has
    /// no such pipeline, or names no ref of it.
    pub(crate) async fn gitlab_pipeline_ref(
        &self,
        repo: &Repo,
        pipeline: &str,
    ) -> Result<Option<String>, ForgeError> {
        Ok(self
            .gitlab_pipeline(repo, pipeline)
            .await?
            .and_then(|pipeline| pipeline.git_ref))
    }

    /// The pipeline `pipeline` of `repo`'s project as it stands (`GET
    /// /projects/{path}/pipelines/{id}`): `None` where GitLab has no such
    /// pipeline.
    pub(crate) async fn gitlab_pipeline(
        &self,
        repo: &Repo,
        pipeline: &str,
    ) -> Result<Option<GitlabPipeline>, ForgeError> {
        const WHAT: &str = "reading a pipeline";
        if pipeline.is_empty() || !pipeline.bytes().all(|b| b.is_ascii_digit()) {
            return Ok(None);
        }
        let (response, credentialed) = self
            .gitlab_get(repo, &["pipelines", pipeline], &[], WHAT)
            .await?;
        match response.status().as_u16() {
            200 => {}
            404 => return Ok(None),
            _ => return Err(api_error(&response, repo, credentialed, WHAT)),
        }
        #[derive(Deserialize)]
        struct Pipeline {
            #[serde(rename = "ref")]
            git_ref: Option<String>,
            status: Option<String>,
        }
        let body = read_capped(response, MAX_API_PAGE, WHAT).await?;
        let pipeline: Pipeline = serde_json::from_slice(&body)
            .map_err(|e| ForgeError::Failed(format!("{WHAT}: {e}")))?;
        let given = |s: Option<String>| s.filter(|s| !s.is_empty());
        Ok(Some(GitlabPipeline {
            git_ref: given(pipeline.git_ref),
            status: given(pipeline.status),
        }))
    }

    /// Whether the merge request `iid` of `repo` is the project's own, its
    /// source branch in the project ([`merge_request_trust`]): `None` where
    /// GitLab has no such merge request.
    pub(crate) async fn gitlab_merge_request_is_own(
        &self,
        repo: &Repo,
        iid: i64,
    ) -> Result<Option<bool>, ForgeError> {
        const WHAT: &str = "reading a merge request";
        let iid = iid.to_string();
        let (response, credentialed) = self
            .gitlab_get(repo, &["merge_requests", &iid], &[], WHAT)
            .await?;
        match response.status().as_u16() {
            200 => {}
            404 => return Ok(None),
            _ => return Err(api_error(&response, repo, credentialed, WHAT)),
        }
        let body = read_capped(response, MAX_API_PAGE, WHAT).await?;
        let mr: MergeRequest = serde_json::from_slice(&body)
            .map_err(|e| ForgeError::Failed(format!("{WHAT}: {e}")))?;
        Ok(Some(
            merge_request_trust(mr.source_project_id, mr.target_project_id) == Trust::Trusted,
        ))
    }

    /// Run the job `job_id` of `repo`'s project again: `POST
    /// /projects/{path}/jobs/{id}/retry`, which GitLab does at once, as a
    /// new job of the same pipeline.
    pub(crate) async fn gitlab_retry_job(
        &self,
        repo: &Repo,
        job_id: &str,
    ) -> Result<Retried, ForgeError> {
        let what = format!("running the job {job_id} again");
        if job_id.is_empty() || !job_id.bytes().all(|b| b.is_ascii_digit()) {
            return Ok(Retried::Refused(format!(
                "{what}: {job_id:?} is no job's id"
            )));
        }
        let request = match self.gitlab_call(
            repo,
            reqwest::Method::POST,
            &["projects", &repo.full_name, "jobs", job_id, "retry"],
            &what,
        ) {
            Ok(request) => request,
            Err(ForgeError::Failed(why) | ForgeError::RateLimited { why, .. }) => {
                return Ok(Retried::NotYet(why))
            }
        };
        let response = request
            .send()
            .await
            .map_err(|e| ForgeError::Failed(format!("{what}: {e}")))?;
        let code = response.status().as_u16();
        match code {
            200..=299 => Ok(Retried::Asked),
            404 => Ok(Retried::Refused(format!("{what}: GitLab has no such job"))),
            _ => {
                let said: String = response
                    .text()
                    .await
                    .unwrap_or_default()
                    .trim()
                    .chars()
                    .take(300)
                    .collect();
                Ok(Retried::NotYet(format!("{what}: HTTP {code}: {said}")))
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode, Uri};
    use axum::response::{IntoResponse, Response};
    use axum::Router;
    use serde_json::json;

    use super::*;

    /// What the stub answers a path with: a status, headers, and a body.
    type Answer = (u16, Vec<(&'static str, String)>, String);

    /// A GitLab: each path (as sent, its escapes and all) and query answered
    /// as set, and each request seen.
    #[derive(Default)]
    pub(crate) struct GitlabState {
        /// By the path and query a request asks for.
        pub(crate) answers: BTreeMap<String, Answer>,
        /// Each request: its method, path and query, Authorization, body.
        pub(crate) seen: Vec<(String, String, String, String)>,
        /// Each runner made (`POST /user/runners`): its id and what it was
        /// made with. Its token is `glrt-{id}`.
        pub(crate) runners: Vec<(u64, serde_json::Value)>,
        /// The runners removed (`DELETE /runners/{id}`), in order.
        pub(crate) removed: Vec<u64>,
        /// The runners that are running a job, as `GET
        /// /runners/{id}/jobs?status=running` says.
        pub(crate) busy: std::collections::BTreeSet<u64>,
        /// The repository whose git `group/app.git` serves, through the
        /// system's git ([`crate::forge::tests::serve_git`]).
        pub(crate) git: Option<std::path::PathBuf>,
    }

    pub(crate) type StubGitlab = Arc<Mutex<GitlabState>>;

    pub(crate) async fn stub_gitlab() -> (String, StubGitlab) {
        async fn handle(
            State(state): State<StubGitlab>,
            method: axum::http::Method,
            uri: Uri,
            headers: HeaderMap,
            body: axum::body::Bytes,
        ) -> Response {
            let asked = uri
                .path_and_query()
                .map(|p| p.to_string())
                .unwrap_or_default();
            let mut gl = state.lock().unwrap();
            gl.seen.push((
                method.to_string(),
                asked.clone(),
                headers
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string(),
                String::from_utf8_lossy(&body).into_owned(),
            ));
            // A runner's own endpoints, which answer as runners are made.
            if method == axum::http::Method::POST && asked == "/user/runners" {
                let made: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let id = gl.runners.len() as u64 + 900;
                gl.runners.push((id, made));
                return (
                    StatusCode::CREATED,
                    axum::Json(json!({ "id": id, "token": format!("glrt-{id}") })),
                )
                    .into_response();
            }
            if let Some(rest) = asked.strip_prefix("/runners/") {
                let id: u64 = rest
                    .split(['/', '?'])
                    .next()
                    .and_then(|id| id.parse().ok())
                    .unwrap_or(0);
                if method == axum::http::Method::DELETE {
                    gl.removed.push(id);
                    return StatusCode::NO_CONTENT.into_response();
                }
                if rest.ends_with("/jobs?status=running") {
                    let running = match gl.busy.contains(&id) {
                        true => json!([{ "id": 1 }]),
                        false => json!([]),
                    };
                    return axum::Json(running).into_response();
                }
            }
            if let Some(dir) = gl.git.as_deref() {
                if let Some(answer) =
                    crate::forge::tests::serve_git(dir, &method, &uri, &headers, &body)
                {
                    return answer;
                }
            }
            match gl.answers.get(&asked) {
                Some((status, headers, body)) => {
                    let mut response =
                        (StatusCode::from_u16(*status).unwrap(), body.clone()).into_response();
                    for (name, value) in headers {
                        response.headers_mut().insert(*name, value.parse().unwrap());
                    }
                    response
                }
                None => StatusCode::NOT_FOUND.into_response(),
            }
        }
        let gl: StubGitlab = Arc::default();
        let app = Router::new().fallback(handle).with_state(gl.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (origin, gl)
    }

    const SHA: &str = "4b4708e725f29891d22b4c96af9f0a403ff51c7c";
    const BASE: &str = "1111111111111111111111111111111111111111";
    const PROJECT: &str = "/projects/group%2Fapp";

    pub(crate) fn project(host: &str) -> Repo {
        Repo {
            forge: "gitlab".into(),
            ..crate::forge::tests::repo(host, "group/app")
        }
    }

    fn forge(origin: &str) -> Forge {
        let tokens = BTreeMap::from([("GitLab.Example".to_string(), "glpat-x".to_string())]);
        Forge::new(Some("ghp_never".into()))
            .with_origin(origin)
            .with_gitlab_tokens(&tokens)
    }

    fn answer(gl: &StubGitlab, at: &str, status: u16, body: serde_json::Value) {
        gl.lock()
            .unwrap()
            .answers
            .insert(at.to_string(), (status, Vec::new(), body.to_string()));
    }

    fn text(gl: &StubGitlab, at: &str, body: &str) {
        gl.lock()
            .unwrap()
            .answers
            .insert(at.to_string(), (200, Vec::new(), body.to_string()));
    }

    /// A job reads as a workflow job would, its tags its labels and its
    /// pipeline its workflow run; one asking for no runner is none of the
    /// fleet's, and one GitLab has not is none at all. Each read with the
    /// host's token.
    #[tokio::test]
    async fn a_job_is_read_for_the_fleet_as_a_workflow_job_is() {
        let (origin, gl) = stub_gitlab().await;
        let forge = forge(&origin);
        let repo = project("gitlab.example");
        answer(
            &gl,
            &format!("{PROJECT}/jobs/501"),
            200,
            json!({
                "id": 501, "status": "failed", "name": "test", "stage": "test", "ref": "main",
                "tag_list": ["linux"], "pipeline": { "id": 77, "sha": SHA },
                "commit": { "id": SHA },
                "runner": { "id": 900, "description": "dagron-0a1b2c3d4e5f" },
            }),
        );
        answer(
            &gl,
            &format!("{PROJECT}/jobs/502"),
            200,
            json!({ "id": 502, "status": "manual", "pipeline": { "id": 77, "sha": SHA } }),
        );
        let job = forge.gitlab_job(&repo, "501").await.unwrap().unwrap();
        assert_eq!(
            (
                job.action,
                job.conclusion.as_deref(),
                job.run_id,
                job.labels.as_slice(),
                job.runner_name.as_deref()
            ),
            (
                crate::github::JobAction::Completed,
                Some("failure"),
                77,
                &["linux".to_string()][..],
                Some("dagron-0a1b2c3d4e5f")
            )
        );
        assert_eq!(forge.gitlab_job(&repo, "502").await, Ok(None), "manual");
        assert_eq!(forge.gitlab_job(&repo, "503").await, Ok(None), "not there");
        assert!(gl
            .lock()
            .unwrap()
            .seen
            .iter()
            .all(|s| s.2 == "Bearer glpat-x"));
        for (status, action) in [
            ("pending", Some(crate::github::JobAction::Queued)),
            ("preparing", Some(crate::github::JobAction::InProgress)),
            ("canceled", Some(crate::github::JobAction::Completed)),
            ("scheduled", None),
        ] {
            assert_eq!(job_action(status).map(|a| a.0), action, "{status}");
        }
    }

    #[test]
    fn a_project_s_api_is_its_host_s_own() {
        let forge = Forge::new(None);
        assert_eq!(
            forge.api_for(&project("gitlab.example")),
            "https://gitlab.example/api/v4"
        );
        assert!(Forge::reads_pipelines(&project("gitlab.example")));
        assert_eq!(state_of("failure"), "failed");
        assert_eq!(state_of("error"), "failed");
        assert_eq!(state_of("success"), "success");
        assert_eq!(state_of("pending"), "pending");
    }

    #[tokio::test]
    async fn the_pipelines_are_read_from_the_tree_with_the_host_s_token() {
        let (origin, gl) = stub_gitlab().await;
        let tree = |n: u32| {
            format!("{PROJECT}/repository/tree?path=.dagron%2Fci&ref={SHA}&per_page=100&page={n}")
        };
        gl.lock().unwrap().answers.insert(
            tree(1),
            (
                200,
                vec![("x-next-page", "2".into())],
                json!([
                    {"type": "blob", "name": "b.yml", "path": ".dagron/ci/b.yml"},
                    {"type": "blob", "name": "notes.md", "path": ".dagron/ci/notes.md"},
                ])
                .to_string(),
            ),
        );
        answer(
            &gl,
            &tree(2),
            200,
            json!([
                {"type": "blob", "name": "a.yaml", "path": ".dagron/ci/a.yaml"},
                {"type": "tree", "name": "more.yaml", "path": ".dagron/ci/more.yaml"},
            ]),
        );
        let raw =
            |name: &str| format!("{PROJECT}/repository/files/.dagron%2Fci%2F{name}/raw?ref={SHA}");
        text(&gl, &raw("a.yaml"), "name: a\n");
        text(&gl, &raw("b.yml"), "name: b\n");
        let forge = forge(&origin);
        let files = forge
            .pipelines(&project("gitlab.example"), SHA)
            .await
            .unwrap()
            .unwrap();
        let read: Vec<(&str, &str)> = files
            .iter()
            .map(|f| (f.path.as_str(), f.text.as_deref().unwrap()))
            .collect();
        assert_eq!(
            read,
            [
                (".dagron/ci/a.yaml", "name: a\n"),
                (".dagron/ci/b.yml", "name: b\n")
            ]
        );
        let seen = gl.lock().unwrap().seen.clone();
        assert_eq!(seen.len(), 4, "two pages and two files: {seen:?}");
        assert!(
            seen.iter()
                .all(|(_, _, token, _)| token == "Bearer glpat-x"),
            "{seen:?}"
        );

        // No such directory: no pipelines.
        assert_eq!(
            forge
                .pipelines(&project("gitlab.example"), BASE)
                .await
                .unwrap(),
            None
        );
        // A file the commit lacks is none.
        assert_eq!(
            forge
                .file(&project("gitlab.example"), ".dagron/ci.yaml", SHA)
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn a_token_goes_to_its_own_host_and_to_no_other() {
        let (origin, gl) = stub_gitlab().await;
        let forge = forge(&origin);
        let other = project("gitlab.other");
        assert!(!forge.has_token(&other));
        assert!(!forge.posts_statuses(&other));
        assert!(forge.has_token(&project("gitlab.example")));
        assert!(forge.posts_statuses(&project("gitlab.example")));
        assert_eq!(forge.pipelines(&other, SHA).await, Ok(None));
        let seen = gl.lock().unwrap().seen.clone();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].2, "", "nothing sent to another host: {seen:?}");

        let refused = forge
            .post_status(&other, SHA, &crate::forge::tests::a_status())
            .await;
        assert!(
            matches!(&refused, Err(StatusError::Refused(why)) if why.contains("CI_GITLAB_TOKENS_DIR")),
            "{refused:?}"
        );
        assert_eq!(gl.lock().unwrap().seen.len(), 1, "not posted");
    }

    #[tokio::test]
    async fn a_change_lists_its_files_under_both_names_or_says_why_it_cannot() {
        let (origin, gl) = stub_gitlab().await;
        let compare = format!("{PROJECT}/repository/compare?from={BASE}&to={SHA}");
        answer(
            &gl,
            &compare,
            200,
            json!({"diffs": [
                {"old_path": "src/a.rs", "new_path": "src/a.rs"},
                {"old_path": "old.md", "new_path": "docs/new.md"},
            ]}),
        );
        let forge = forge(&origin);
        let repo = project("gitlab.example");
        assert_eq!(
            forge.changed_files(&repo, BASE, SHA).await.unwrap(),
            Changes::Listed(vec![
                "docs/new.md".into(),
                "old.md".into(),
                "src/a.rs".into()
            ])
        );
        answer(
            &gl,
            &compare,
            200,
            json!({"diffs": [], "compare_timeout": true}),
        );
        assert!(matches!(
            forge.changed_files(&repo, BASE, SHA).await.unwrap(),
            Changes::Unlisted(_)
        ));
        answer(&gl, &compare, 404, json!({"message": "404 Not found"}));
        assert!(matches!(
            forge.changed_files(&repo, BASE, SHA).await.unwrap(),
            Changes::Unlisted(_)
        ));
    }

    /// The merge base GitLab names for `base` and `head`.
    fn merge_base(base: &str, head: &str) -> String {
        format!("{PROJECT}/repository/merge_base?refs%5B%5D={base}&refs%5B%5D={head}")
    }

    /// Where GitLab's compare cannot list a change (past what it lists, or
    /// out of time), a shallow fetch does, from the merge base GitLab names,
    /// with the host's token.
    #[tokio::test]
    async fn a_change_the_compare_api_cannot_list_is_fetched_and_listed() {
        use crate::git::pack::tests::{a_large_change, Scratch};
        let Some(scratch) = Scratch::new(crate::git::pack::ObjectFormat::Sha1) else {
            return;
        };
        let (base, head) = a_large_change(&scratch);
        scratch.git(&["config", "uploadpack.allowFilter", "true"]);
        let (origin, gl) = stub_gitlab().await;
        gl.lock().unwrap().git = Some(scratch.path().to_path_buf());
        let compare = format!("{PROJECT}/repository/compare?from={base}&to={head}");
        let diffs: Vec<serde_json::Value> = (0..COMPARE_FILE_LIMIT)
            .map(|n| json!({"old_path": format!("f{n}"), "new_path": format!("f{n}")}))
            .collect();
        answer(&gl, &compare, 200, json!({ "diffs": diffs }));
        answer(&gl, &merge_base(&base, &head), 200, json!({ "id": base }));
        let forge = forge(&origin);
        let repo = project("gitlab.example");
        let expected = Changes::Listed(scratch.diff(&base, &head));
        assert_eq!(
            forge.changed_files(&repo, &base, &head).await.unwrap(),
            expected
        );
        let git: Vec<(String, String)> = gl
            .lock()
            .unwrap()
            .seen
            .iter()
            .filter(|s| s.1.starts_with("/group/app.git/"))
            .map(|s| (s.0.clone(), s.2.clone()))
            .collect();
        let token = "Basic b2F1dGgyOmdscGF0LXg=".to_string();
        assert_eq!(
            git,
            [("GET".to_string(), token.clone()), ("POST".into(), token)],
            "oauth2 and the host's token, to the project's git"
        );

        answer(
            &gl,
            &compare,
            200,
            json!({"diffs": [], "compare_timeout": true}),
        );
        assert_eq!(
            forge.changed_files(&repo, &base, &head).await.unwrap(),
            expected
        );

        // Neither: the reason says why of both.
        gl.lock().unwrap().git = None;
        let Changes::Unlisted(why) = forge.changed_files(&repo, &base, &head).await.unwrap() else {
            panic!("no git to fetch from");
        };
        assert!(
            why.starts_with(
                "the forge took too long to compare the commits; nor could a shallow fetch: \
                 fetching the commits: HTTP 404"
            ),
            "{why}"
        );
    }

    /// A push forced after a rebase: its base, the branch's old tip, is not
    /// where the branch left main. GitLab compares from where it did, and so
    /// does the fetch, from the merge base GitLab names; from the old tip,
    /// the branch's own file, the same in both tips, would be left out.
    /// Where GitLab names none, nothing is fetched.
    #[tokio::test]
    async fn a_forced_push_is_fetched_from_its_merge_base() {
        use crate::git::pack::tests::{a_rebased_branch, Scratch};
        let Some(scratch) = Scratch::new(crate::git::pack::ObjectFormat::Sha1) else {
            return;
        };
        let (old, left, new) = a_rebased_branch(&scratch);
        let own = "docs/a.md".to_string();
        assert!(!scratch.diff(&old, &new).contains(&own));
        let expected = scratch.diff(&left, &new);
        assert!(expected.contains(&own));
        scratch.git(&["config", "uploadpack.allowFilter", "true"]);
        let (origin, gl) = stub_gitlab().await;
        gl.lock().unwrap().git = Some(scratch.path().to_path_buf());
        let compare = format!("{PROJECT}/repository/compare?from={old}&to={new}");
        answer(
            &gl,
            &compare,
            200,
            json!({"diffs": [], "compare_timeout": true}),
        );
        answer(&gl, &merge_base(&old, &new), 200, json!({ "id": left }));
        let forge = forge(&origin);
        let repo = project("gitlab.example");
        assert_eq!(
            forge.changed_files(&repo, &old, &new).await.unwrap(),
            Changes::Listed(expected)
        );

        answer(
            &gl,
            &merge_base(&old, &new),
            404,
            json!({"message": "404 Merge Base Not Found"}),
        );
        let Changes::Unlisted(why) = forge.changed_files(&repo, &old, &new).await.unwrap() else {
            panic!("no merge base to fetch from");
        };
        assert!(
            why.starts_with(
                "the forge took too long to compare the commits; nor could a shallow fetch: \
                 finding the merge base: HTTP 404"
            ),
            "{why}"
        );
    }

    #[tokio::test]
    async fn the_open_merge_requests_are_read_page_by_page_as_pull_requests() {
        let (origin, gl) = stub_gitlab().await;
        let page = |n: u32| format!("{PROJECT}/merge_requests?state=opened&per_page=100&page={n}");
        let mr = |iid: i64, source: i64| {
            json!({
                "iid": iid, "sha": SHA, "source_branch": format!("topic-{iid}"),
                "target_branch": "main", "source_project_id": source,
                "target_project_id": 7, "author": {"username": "dev"},
                "diff_refs": null,
            })
        };
        gl.lock().unwrap().answers.insert(
            page(1),
            (
                200,
                vec![("x-next-page", "2".into())],
                json!([mr(1, 7)]).to_string(),
            ),
        );
        gl.lock().unwrap().answers.insert(
            page(2),
            (
                200,
                vec![("x-next-page", "".into())],
                json!([mr(2, 99)]).to_string(),
            ),
        );
        let forge = forge(&origin);
        let Pulls::Listed {
            events, complete, ..
        } = forge
            .list_pulls(&project("gitlab.example"), None)
            .await
            .unwrap()
        else {
            panic!("not listed");
        };
        assert!(complete);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].git_ref, "refs/merge-requests/1/head");
        assert_eq!(events[0].base_sha, None, "as GitLab lists it");
        // Asked for alone, a merge request says its base.
        answer(
            &gl,
            &format!("{PROJECT}/merge_requests/1"),
            200,
            json!({"iid": 1, "sha": SHA, "diff_refs": {"base_sha": BASE}}),
        );
        assert_eq!(
            forge
                .merge_request_base(&project("gitlab.example"), 1)
                .await
                .unwrap()
                .as_deref(),
            Some(BASE)
        );
        assert_eq!(
            forge
                .merge_request_base(&project("gitlab.example"), 2)
                .await
                .unwrap(),
            None
        );
        assert_eq!(events[0].head_ref.as_deref(), Some("topic-1"));
        assert_eq!(events[0].trust, Trust::Trusted);
        assert_eq!(events[1].trust, Trust::Untrusted, "a fork's");

        gl.lock().unwrap().answers.insert(
            page(1),
            (429, vec![("retry-after", "7".into())], "{}".into()),
        );
        assert!(matches!(
            forge.list_pulls(&project("gitlab.example"), None).await,
            Err(ForgeError::RateLimited { wait, .. }) if wait.as_secs() == 7
        ));
    }

    #[tokio::test]
    async fn a_status_goes_up_in_gitlab_s_words() {
        let (origin, gl) = stub_gitlab().await;
        let at = format!("{PROJECT}/statuses/{SHA}");
        answer(&gl, &at, 201, json!({"id": 1}));
        let forge = forge(&origin);
        let repo = project("gitlab.example");
        let mut status = crate::forge::tests::a_status();
        status.state = "error";
        forge.post_status(&repo, SHA, &status).await.unwrap();
        let (method, _, token, body) = gl.lock().unwrap().seen[0].clone();
        assert_eq!(
            (method.as_str(), token.as_str()),
            ("POST", "Bearer glpat-x")
        );
        let body: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(body["state"], "failed");
        assert_eq!(body["name"], status.context.as_str());
        assert!(
            body.get("ref").is_none(),
            "a status of no ref names none: {body}"
        );

        // One of a ref names it: GitLab keeps it in that ref's pipeline.
        status.pipeline_ref = Some("feature/x".into());
        forge.post_status(&repo, SHA, &status).await.unwrap();
        let body = gl.lock().unwrap().seen[1].3.clone();
        let body: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(body["ref"], "feature/x");

        // A status already in that state is posted already.
        answer(
            &gl,
            &at,
            400,
            json!({"message": "Cannot transition status via :run from :running"}),
        );
        forge.post_status(&repo, SHA, &status).await.unwrap();

        answer(&gl, &at, 403, json!({"message": "403 Forbidden"}));
        assert!(matches!(
            forge.post_status(&repo, SHA, &status).await,
            Err(StatusError::Refused(_))
        ));
        answer(&gl, &at, 502, json!({}));
        assert!(matches!(
            forge.post_status(&repo, SHA, &status).await,
            Err(StatusError::Failed(_))
        ));
    }

    /// The ref a status is of: a branch's or a tag's name, and a merge
    /// request's source branch, whose pipeline its page shows.
    #[test]
    fn a_status_is_of_its_branch_its_tag_or_its_merge_request_s_source() {
        let of = |git_ref: &str, head_ref: Option<&str>| status_ref(git_ref, head_ref, "trusted");
        assert_eq!(of("refs/heads/main", None).as_deref(), Some("main"));
        assert_eq!(
            of("refs/heads/feature/x", Some("ignored")).as_deref(),
            Some("feature/x")
        );
        assert_eq!(of("refs/tags/v1.2", None).as_deref(), Some("v1.2"));
        assert_eq!(
            of("refs/merge-requests/7/head", Some("topic")).as_deref(),
            Some("topic")
        );
        assert_eq!(
            of("refs/merge-requests/7/head", Some("refs/heads/topic")).as_deref(),
            Some("topic")
        );
        // A fork's merge request: its source branch is the fork's, and the
        // project its statuses go to may have none of that name, or
        // another's. GitLab picks.
        assert_eq!(
            status_ref("refs/merge-requests/7/head", Some("topic"), "untrusted"),
            None
        );
        // A merge request whose source branch is not known, and any other
        // ref: GitLab picks.
        assert_eq!(of("refs/merge-requests/7/head", None), None);
        assert_eq!(of("refs/pull/7/head", Some("topic")), None);
        // A merge train's: the train's pipeline, which GitLab merges on.
        assert_eq!(
            of("refs/merge-requests/7/train", None).as_deref(),
            Some("refs/merge-requests/7/train")
        );
        assert_eq!(of("refs/heads/", None), None);
        assert_eq!(of(&format!("refs/heads/{}", "b".repeat(256)), None), None);
    }

    #[tokio::test]
    async fn a_merge_request_s_note_is_made_then_written_again_in_place() {
        let (origin, gl) = stub_gitlab().await;
        let notes = format!("{PROJECT}/merge_requests/7/notes");
        answer(&gl, &notes, 201, json!({"id": 3001, "body": "made"}));
        answer(&gl, &format!("{notes}/3001"), 200, json!({"id": 3001}));
        let repo = project("gitlab.example");

        // Off unless asked for, and then for the host the token is for.
        let off = forge(&origin);
        assert!(!off.comments(&repo) && !off.pr_comments());
        assert!(matches!(
            off.post_comment(&repo, 7, "said", None).await,
            Err(StatusError::Refused(_))
        ));
        let forge = forge(&origin).with_mr_notes(true);
        assert!(forge.pr_comments() && forge.comments(&repo));
        assert!(!forge.comments(&project("elsewhere.example")));

        assert_eq!(
            forge.post_comment(&repo, 7, "first", None).await.unwrap(),
            Commented::Written(3001)
        );
        assert_eq!(
            forge
                .post_comment(&repo, 7, "second", Some("3001"))
                .await
                .unwrap(),
            Commented::Written(3001)
        );
        let seen = gl.lock().unwrap().seen.clone();
        let asked: Vec<(&str, &str, &str)> = seen
            .iter()
            .map(|(m, at, token, _)| (m.as_str(), at.as_str(), token.as_str()))
            .collect();
        assert_eq!(
            asked,
            [
                ("POST", notes.as_str(), "Bearer glpat-x"),
                ("PUT", format!("{notes}/3001").as_str(), "Bearer glpat-x"),
            ]
        );
        let body: serde_json::Value = serde_json::from_str(&seen[1].3).unwrap();
        assert_eq!(body, json!({"body": "second"}));

        // Deleted since: gone, for a new one to be made.
        assert_eq!(
            forge
                .post_comment(&repo, 7, "third", Some("3002"))
                .await
                .unwrap(),
            Commented::Gone
        );
        // A token that may not write notes is refused, and would be again;
        // a rate limit waits; GitLab having a bad moment is tried again.
        answer(&gl, &notes, 403, json!({"message": "403 Forbidden"}));
        assert!(matches!(
            forge.post_comment(&repo, 7, "said", None).await,
            Err(StatusError::Refused(_))
        ));
        gl.lock().unwrap().answers.insert(
            notes.clone(),
            (429, vec![("retry-after", "9".into())], "{}".into()),
        );
        assert!(matches!(
            forge.post_comment(&repo, 7, "said", None).await,
            Err(StatusError::RateLimited { wait, .. }) if wait.as_secs() == 9
        ));
        answer(&gl, &notes, 502, json!({}));
        assert!(matches!(
            forge.post_comment(&repo, 7, "said", None).await,
            Err(StatusError::Failed(_))
        ));
        // An id that is no note's goes nowhere.
        assert!(matches!(
            forge.post_comment(&repo, 7, "said", Some("../9")).await,
            Err(StatusError::Failed(_))
        ));
    }

    #[tokio::test]
    async fn a_flaky_job_s_issue_is_opened_then_written_again_and_found_closed() {
        let (origin, gl) = stub_gitlab().await;
        let issues = format!("{PROJECT}/issues");
        answer(
            &gl,
            &issues,
            201,
            json!({"iid": 12, "state": "opened", "closed_at": null}),
        );
        answer(
            &gl,
            &format!("{issues}/12"),
            200,
            json!({"iid": 12, "state": "opened"}),
        );
        answer(
            &gl,
            &format!("{issues}/13"),
            200,
            json!({"iid": 13, "state": "closed", "closed_at": "2026-10-20T12:00:00.000Z"}),
        );
        let repo = project("gitlab.example");

        let off = forge(&origin);
        assert!(!off.issues(&repo));
        assert!(matches!(
            off.write_issue(&repo, "t", "said", None).await,
            Err(StatusError::Refused(_))
        ));
        let forge = forge(&origin).with_flake_issues(true);
        assert!(forge.issues(&repo) && !forge.issues(&project("elsewhere.example")));

        assert_eq!(
            forge
                .write_issue(&repo, "Flaky job: test of ci", "first", None)
                .await
                .unwrap(),
            Issued::Written("12".into())
        );
        assert_eq!(
            forge
                .write_issue(&repo, "Flaky job: test of ci", "second", Some("12"))
                .await
                .unwrap(),
            Issued::Written("12".into())
        );
        assert_eq!(
            forge
                .write_issue(&repo, "t", "third", Some("13"))
                .await
                .unwrap(),
            Issued::Closed(Some("2026-10-20T12:00:00.000Z".into()))
        );
        assert_eq!(
            forge
                .write_issue(&repo, "t", "fourth", Some("14"))
                .await
                .unwrap(),
            Issued::Gone
        );
        let seen = gl.lock().unwrap().seen.clone();
        assert_eq!(
            (seen[0].0.as_str(), seen[0].1.as_str(), seen[0].2.as_str()),
            ("POST", issues.as_str(), "Bearer glpat-x")
        );
        let made: serde_json::Value = serde_json::from_str(&seen[0].3).unwrap();
        assert_eq!(
            made,
            json!({"title": "Flaky job: test of ci", "description": "first"})
        );
        let again: serde_json::Value = serde_json::from_str(&seen[1].3).unwrap();
        assert_eq!(
            (seen[1].0.as_str(), again),
            ("PUT", json!({"description": "second"}))
        );

        answer(&gl, &issues, 403, json!({"message": "403 Forbidden"}));
        assert!(matches!(
            forge.write_issue(&repo, "t", "said", None).await,
            Err(StatusError::Refused(_))
        ));
        assert!(matches!(
            forge.write_issue(&repo, "t", "said", Some("../9")).await,
            Err(StatusError::Failed(_))
        ));
    }
}
