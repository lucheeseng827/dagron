//! The poll sweep: what catches a commit no webhook announced. A dropped
//! delivery, a forge that cannot reach dagron-ci, an air gap: each repository
//! is swept every `poll_secs` (CI_POLL_SECS unless it sets its own), and what
//! changed since its last sweep is queued, as a webhook would have queued it.
//!
//! A sweep reads:
//! - the branches and tags, with git's ref listing ([`crate::git`]), from any
//!   forge;
//! - on GitHub, the open pull requests, from its API. While they fit on one
//!   page the request is conditional: an unchanged list answers 304, which
//!   GitHub does not count against the rate limit.
//!
//! It compares them with what the last sweep saw (`ci_poll_refs`):
//! - a new or moved branch queues a `push`, a new or moved tag a `tag`, and a
//!   branch or tag gone a `delete`;
//! - a pull request opened or pushed to queues a `pull_request`, its trust
//!   decided as for a webhook's. One that closed is forgotten and queues
//!   nothing: closing a pull request runs nothing.
//!
//! **Once.** A polled event carries the commit key a webhook's would, so a
//! commit both doors see runs once: the second event a worker takes finds the
//! first one's run.
//!
//! **The first sweep runs nothing.** It records what it finds, so connecting
//! a repository does not run every branch it has.
//!
//! **Replicas share the work.** A sweeper claims the most overdue repository
//! (`FOR UPDATE SKIP LOCKED` on Postgres) and holds it for a lease. A sweep's
//! writes commit in one transaction, fenced on its claim, so a sweeper that
//! outlived its lease writes nothing. Each next sweep is jittered by a tenth
//! either way, so repositories connected together drift apart.

use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{watch, Notify};
use tokio::task::JoinSet;

use crate::db::{Db, DueRepo, Repo, SweepWrite};
use crate::event::{Kind, NewEvent, Trust};
use crate::forge::{is_url_safe, Forge, ForgeError, Pulls};
use crate::metrics::Metrics;

/// Sweepers per replica. A sweep is a few requests, so four keep a thousand
/// repositories on a five-minute interval.
const SWEEPERS: usize = 4;

/// How long a sweeper holds a repository it claimed: well past
/// [`SWEEP_TIMEOUT`], so a live sweep never loses its claim.
const LEASE: Duration = Duration::from_secs(300);

/// The longest a sweep may take before it is given up as failed.
const SWEEP_TIMEOUT: Duration = Duration::from_secs(120);

/// The most GitLab merge requests one sweep asks for their bases.
const MAX_BASE_LOOKUPS: usize = 20;

/// How long an idle sweeper waits before looking for a due repository again.
const IDLE: Duration = Duration::from_secs(5);

/// How long a sweeper waits after the database fails it.
const DB_RETRY: Duration = Duration::from_secs(5);

/// The furthest a forge's rate limit pushes a repository's next sweep out.
const MAX_RATE_LIMIT_WAIT: Duration = Duration::from_secs(3600);

/// Branches and tags. Pull requests come from the forge's API, which says
/// which are open and who opened them.
const REF_PREFIXES: &[&str] = &["refs/heads/", "refs/tags/"];

/// What one sweep did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Swept {
    /// The repository's first sweep: this many refs recorded, nothing queued.
    Baseline(usize),
    /// Events queued, by id.
    Queued(Vec<String>),
    /// Nothing changed since the last sweep.
    Unchanged,
    /// The sweep failed, for this reason; it is tried again next interval.
    Failed(String),
    /// The forge's rate limit is spent; the next sweep waits for it.
    RateLimited(String),
    /// The claim lapsed and another sweeper took the repository over; this
    /// sweep wrote nothing.
    Lost,
}

/// What a sweep saw of the repository.
struct Observed {
    /// Branches and tags, and the id each points at: a commit's, or an
    /// annotated tag's own.
    refs: BTreeMap<String, String>,
    /// The commit each annotated tag names.
    peeled: BTreeMap<String, String>,
    pulls: Pulls,
}

pub struct Poller {
    db: Db,
    forge: Arc<Forge>,
    /// CI_POLL_SECS: the interval of a repository that sets none. 0 sweeps
    /// only the repositories that set their own.
    default_secs: u64,
    metrics: Arc<Metrics>,
}

impl Poller {
    pub fn new(db: Db, forge: Arc<Forge>, default_secs: u64, metrics: Arc<Metrics>) -> Poller {
        Poller {
            db,
            forge,
            default_secs,
            metrics,
        }
    }

    /// Claim the most overdue repository and sweep it, as `claim`. `None`
    /// when no repository is due.
    pub async fn sweep_next(&self, claim: &str) -> sqlx::Result<Option<Swept>> {
        let Some(due) = self.db.claim_sweep(claim, LEASE, self.default_secs).await? else {
            return Ok(None);
        };
        let repo = &due.repo;
        let interval = Duration::from_secs(due.poll_secs);
        // One deadline for all of the sweep's calls to the forge, well
        // inside the lease.
        let deadline = tokio::time::Instant::now() + SWEEP_TIMEOUT;
        let observed = match tokio::time::timeout_at(deadline, self.observe(&due)).await {
            Ok(observed) => observed,
            Err(_) => Err(ForgeError::Failed(format!(
                "the sweep took longer than {} s",
                SWEEP_TIMEOUT.as_secs()
            ))),
        };
        let swept = match observed {
            Ok(observed) => {
                let last = self.db.poll_refs(&repo.id).await?;
                let mut write = diff(&last, &observed, due.baseline, due.etag.clone());
                self.merge_request_bases(repo, &mut write, deadline).await;
                let recorded = write.set.len();
                match self
                    .db
                    .finish_sweep(&repo.id, claim, &write, jitter(interval))
                    .await?
                {
                    None => Swept::Lost,
                    Some(_) if due.baseline => Swept::Baseline(recorded),
                    Some(ids) if ids.is_empty() => Swept::Unchanged,
                    Some(ids) => Swept::Queued(ids),
                }
            }
            Err(ForgeError::RateLimited { wait, why }) => {
                let next = jitter(interval).max(wait.min(MAX_RATE_LIMIT_WAIT));
                match self.db.fail_sweep(&repo.id, claim, &why, next).await? {
                    true => Swept::RateLimited(why),
                    false => Swept::Lost,
                }
            }
            Err(ForgeError::Failed(why)) => {
                match self
                    .db
                    .fail_sweep(&repo.id, claim, &why, jitter(interval))
                    .await?
                {
                    true => Swept::Failed(why),
                    false => Swept::Lost,
                }
            }
        };
        self.metrics.poll_swept(&swept);
        log(repo, &swept);
        Ok(Some(swept))
    }

    /// GitLab lists a merge request without the commit it is compared with:
    /// each one queued is asked for it alone, so that its paths filter has a
    /// base, as a webhook's event has. At most [`MAX_BASE_LOOKUPS`] a sweep,
    /// and none past the sweep's `deadline`. One GitLab answers with a
    /// failure is queued without, and its paths are not checked; one that
    /// is not asked, or that meets the rate limit or the deadline, is left
    /// for the next sweep ([`defer`]), which finds it again.
    async fn merge_request_bases(
        &self,
        repo: &Repo,
        write: &mut SweepWrite,
        deadline: tokio::time::Instant,
    ) {
        if repo.forge != "gitlab" {
            return;
        }
        let mut later = Vec::new();
        let mut asked = 0;
        let mut waiting = false;
        for event in write
            .events
            .iter_mut()
            .filter(|e| e.kind == Kind::PullRequest && e.base_sha.is_none())
        {
            let Some(iid) = event.pr else { continue };
            if waiting || asked >= MAX_BASE_LOOKUPS {
                later.push(event.git_ref.clone());
                continue;
            }
            asked += 1;
            let asking = self.forge.merge_request_base(repo, iid);
            match tokio::time::timeout_at(deadline, asking).await {
                Ok(Ok(base)) => event.base_sha = base,
                Ok(Err(ForgeError::Failed(why))) => {
                    tracing::warn!(repo = %repo.full_name, iid, %why, "a merge request's base could not be read: it is queued without");
                }
                Ok(Err(ForgeError::RateLimited { why, .. })) => {
                    tracing::info!(repo = %repo.full_name, iid, %why, "GitLab's rate limit is spent: the merge requests left are read next sweep");
                    waiting = true;
                    later.push(event.git_ref.clone());
                }
                Err(_) => {
                    waiting = true;
                    later.push(event.git_ref.clone());
                }
            }
        }
        if !later.is_empty() {
            tracing::info!(repo = %repo.full_name, left = later.len(), "merge requests left for the next sweep, to read their bases");
            defer(write, &later);
        }
    }

    async fn observe(&self, due: &DueRepo) -> Result<Observed, ForgeError> {
        let repo = &due.repo;
        if !is_url_safe(repo) {
            return Err(ForgeError::Failed(format!(
                "{}/{} cannot be put in a URL as it is; reconnect it",
                repo.forge_host, repo.full_name
            )));
        }
        let listed = self.forge.list_refs(repo, REF_PREFIXES).await?;
        let pulls = match repo.forge.as_str() {
            "github" | "gitlab" => self.forge.list_pulls(repo, due.etag.as_deref()).await?,
            _ => Pulls::NotListed,
        };
        Ok(Observed {
            refs: listed.ids,
            peeled: listed.peeled,
            pulls,
        })
    }
}

/// What a sweep writes: the events for what changed since `last`, and the
/// refs to record. A baseline records and queues nothing. `kept_etag` is the
/// ETag the last sweep kept, still good when the pull requests answered 304.
fn diff(
    last: &BTreeMap<String, String>,
    observed: &Observed,
    baseline: bool,
    kept_etag: Option<String>,
) -> SweepWrite {
    let mut write = SweepWrite::default();
    let changed = |write: &mut SweepWrite, name: &str, sha: &str, event: NewEvent| {
        if last.get(name).map(String::as_str) != Some(sha) {
            write.set.push((name.to_string(), sha.to_string()));
            if !baseline {
                write.events.push(event);
            }
        }
    };

    for (name, sha) in &observed.refs {
        let event = if name.starts_with("refs/tags/") {
            // An annotated tag is queued under its own id, as a webhook queues
            // it, with the commit it names beside it.
            NewEvent {
                commit: observed.peeled.get(name).cloned(),
                ..trusted(Kind::Tag, name, sha)
            }
        } else {
            // Measured against what the last sweep saw: every commit pushed
            // since, whether or not a webhook announced it.
            NewEvent {
                base_sha: last.get(name).cloned(),
                ..trusted(Kind::Push, name, sha)
            }
        };
        changed(&mut write, name, sha, event);
    }
    for (name, sha) in last {
        let branch_or_tag = REF_PREFIXES.iter().any(|p| name.starts_with(p));
        if branch_or_tag && !observed.refs.contains_key(name) {
            write.drop.push(name.clone());
            if !baseline {
                write.events.push(trusted(Kind::Delete, name, sha));
            }
        }
    }

    match &observed.pulls {
        Pulls::NotListed => {}
        Pulls::Unchanged => write.etag = kept_etag,
        Pulls::Listed {
            events,
            complete,
            etag,
        } => {
            write.etag = etag.clone();
            // A page boundary that moved mid-listing can list one twice.
            let open: BTreeMap<&str, &NewEvent> =
                events.iter().map(|e| (e.git_ref.as_str(), e)).collect();
            for (name, event) in &open {
                changed(&mut write, name, &event.sha, (*event).clone());
            }
            if *complete {
                for name in last.keys() {
                    let pull =
                        name.starts_with("refs/pull/") || name.starts_with("refs/merge-requests/");
                    if pull && !open.contains_key(name.as_str()) {
                        write.drop.push(name.clone());
                    }
                }
            }
        }
    }
    write
}

/// Leave the refs `names` out of what the sweep writes, their events and
/// the commits they point at: the next sweep finds them changed again.
fn defer(write: &mut SweepWrite, names: &[String]) {
    write.events.retain(|e| !names.contains(&e.git_ref));
    write.set.retain(|(name, _)| !names.contains(name));
}

/// A push, tag or delete: only a writer can move a branch or a tag.
fn trusted(kind: Kind, name: &str, sha: &str) -> NewEvent {
    NewEvent {
        kind,
        git_ref: name.to_string(),
        sha: sha.to_string(),
        commit: None,
        base_sha: None,
        base_ref: None,
        pr: None,
        actor: None,
        trust: Trust::Trusted,
        association: None,
        head_ref: None,
    }
}

/// `interval`, give or take a tenth.
fn jitter(interval: Duration) -> Duration {
    // Each RandomState is freshly keyed, so this is a cheap random u64.
    let random = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    let unit = (random >> 11) as f64 / (1_u64 << 53) as f64;
    interval.mul_f64(0.9 + 0.2 * unit)
}

fn log(repo: &Repo, swept: &Swept) {
    let repo = format!("{}/{}", repo.forge_host, repo.full_name);
    match swept {
        Swept::Baseline(refs) => {
            tracing::info!(repo, refs, "first poll sweep recorded what it found")
        }
        Swept::Queued(ids) => tracing::info!(repo, events = ids.len(), "poll sweep queued events"),
        Swept::Unchanged => tracing::debug!(repo, "poll sweep found nothing new"),
        Swept::Failed(why) => tracing::warn!(repo, reason = %why, "poll sweep failed"),
        Swept::RateLimited(why) => {
            tracing::warn!(repo, reason = %why, "poll sweep held back by the forge")
        }
        Swept::Lost => tracing::warn!(
            repo,
            "a poll sweep outlived its lease; another sweeper has the repository"
        ),
    }
}

/// Start this replica's sweepers. They run until `stop` turns true, and drop
/// a sweep in hand: nothing of it is written, and its repository is due
/// again once the claim lapses.
pub fn spawn(poller: Arc<Poller>, wake: Arc<Notify>, stop: watch::Receiver<bool>) -> JoinSet<()> {
    let mut set = JoinSet::new();
    let replica = uuid::Uuid::now_v7().simple().to_string();
    for n in 0..SWEEPERS {
        let claim = format!("{}-poll-{n}", &replica[replica.len() - 12..]);
        set.spawn(sweep(claim, poller.clone(), wake.clone(), stop.clone()));
    }
    set
}

async fn sweep(
    claim: String,
    poller: Arc<Poller>,
    wake: Arc<Notify>,
    mut stop: watch::Receiver<bool>,
) {
    while !*stop.borrow() {
        let swept = tokio::select! {
            swept = poller.sweep_next(&claim) => swept,
            _ = stop.changed() => break,
        };
        let pause = match swept {
            Ok(Some(Swept::Queued(ids))) => {
                // One wake per idle worker that has an event to take.
                for _ in 0..ids.len().min(64) {
                    wake.notify_one();
                }
                continue;
            }
            Ok(Some(_)) => continue,
            Ok(None) => IDLE,
            Err(e) => {
                tracing::warn!(sweeper = %claim, error = %e, "poll sweep: the database failed");
                DB_RETRY
            }
        };
        tokio::select! {
            _ = tokio::time::sleep(pause) => {}
            _ = stop.changed() => {}
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::{HeaderMap, Method, StatusCode, Uri};
    use axum::response::{IntoResponse, Response};
    use axum::Router;
    use serde_json::json;
    use sha2::{Digest, Sha256};

    use super::*;
    use crate::db::tests::{drop_schema, postgres, sqlite};
    use crate::git::tests::pkts;
    use crate::github::{self, tests::listed_pull};

    const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const C: &str = "cccccccccccccccccccccccccccccccccccccccc";
    const D: &str = "dddddddddddddddddddddddddddddddddddddddd";
    const T1: &str = "1111111111111111111111111111111111111111";
    const T2: &str = "2222222222222222222222222222222222222222";

    /// `Basic base64("x-access-token:ghp_test")`.
    const BASIC: &str = "Basic eC1hY2Nlc3MtdG9rZW46Z2hwX3Rlc3Q=";

    /// One request the stub forge answered.
    #[derive(Clone, Debug)]
    struct Request {
        line: String,
        authorization: String,
        if_none_match: String,
        git_protocol: String,
    }

    /// A forge serving `acme/widgets`: git's smart HTTP, and GitHub's pulls
    /// API with ETags and pages.
    #[derive(Default)]
    pub(crate) struct ForgeState {
        /// What a ref listing holds, pull request refs included, so the
        /// poller's own filter is tested too.
        pub(crate) refs: BTreeMap<String, String>,
        /// The commit each annotated tag in `refs` names.
        pub(crate) peeled: BTreeMap<String, String>,
        /// The commits each commit has behind it, for the compare API.
        pub(crate) behind: BTreeMap<String, Vec<String>>,
        pulls: Vec<serde_json::Value>,
        /// Pull requests per page (0: GitHub's 100).
        page_size: usize,
        /// Answer as a server that only speaks protocol v0.
        v0: bool,
        /// Frame a v2 answer as git's own server does: `version 2` first, and
        /// no service line before it. Otherwise as github.com frames it.
        stock: bool,
        /// A status git requests answer instead.
        pub(crate) git_answer: Option<u16>,
        /// The branch HEAD names, said as a symref where it is set.
        pub(crate) head: Option<String>,
        /// Files at any commit, by path: GitHub's contents API, raw.
        pub(crate) files: BTreeMap<String, String>,
        /// A status and headers the pulls API answers instead.
        pulls_answer: Option<(u16, Vec<(&'static str, String)>)>,
        seen: Vec<Request>,
    }

    pub(crate) type StubForge = Arc<Mutex<ForgeState>>;

    fn framed(lines: &[String], content_type: &'static str) -> Response {
        let lines: Vec<&str> = lines.iter().map(String::as_str).collect();
        ([("content-type", content_type)], pkts(&lines)).into_response()
    }

    pub(crate) async fn stub_forge() -> (String, StubForge) {
        async fn handle(
            State(forge): State<StubForge>,
            method: Method,
            uri: Uri,
            headers: HeaderMap,
            body: Bytes,
        ) -> Response {
            let h = |n: &str| {
                headers
                    .get(n)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string()
            };
            let mut f = forge.lock().unwrap();
            f.seen.push(Request {
                line: format!("{method} {uri}"),
                authorization: h("authorization"),
                if_none_match: h("if-none-match"),
                git_protocol: h("git-protocol"),
            });
            match uri.path() {
                "/acme/widgets.git/info/refs" => {
                    if let Some(code) = f.git_answer {
                        return StatusCode::from_u16(code).unwrap().into_response();
                    }
                    let mut lines = if f.stock && !f.v0 {
                        Vec::new()
                    } else {
                        vec!["# service=git-upload-pack\n".to_string(), "0000".into()]
                    };
                    if f.v0 {
                        let symref = f
                            .head
                            .as_ref()
                            .map(|h| format!(" symref=HEAD:{h}"))
                            .unwrap_or_default();
                        for (i, (name, sha)) in f.refs.iter().enumerate() {
                            let caps = if i == 0 {
                                format!("\0side-band-64k{symref}")
                            } else {
                                String::new()
                            };
                            lines.push(format!("{sha} {name}{caps}\n"));
                            // A v0 server lists a tag's commit as a ref of its own.
                            if let Some(commit) = f.peeled.get(name) {
                                lines.push(format!("{commit} {name}^{{}}\n"));
                            }
                        }
                    } else {
                        lines.push("version 2\n".into());
                        lines.push("ls-refs=unborn\n".into());
                        lines.push("object-format=sha1\n".into());
                    }
                    lines.push("0000".into());
                    framed(&lines, "application/x-git-upload-pack-advertisement")
                }
                "/acme/widgets.git/git-upload-pack" if method == Method::POST && !f.v0 => {
                    if !body.starts_with(b"0014command=ls-refs\n") {
                        return StatusCode::BAD_REQUEST.into_response();
                    }
                    // A v2 server names a tag's commit only when asked to peel.
                    let peel = body.windows(9).any(|w| w == b"0009peel\n");
                    let symrefs = body.windows(8).any(|w| w == b"symrefs\n");
                    let mut lines: Vec<String> = f
                        .head
                        .as_ref()
                        .filter(|_| symrefs)
                        .and_then(|h| {
                            f.refs
                                .get(h)
                                .map(|sha| format!("{sha} HEAD symref-target:{h}\n"))
                        })
                        .into_iter()
                        .collect();
                    lines.extend(f.refs.iter().map(|(name, sha)| match f.peeled.get(name) {
                        Some(commit) if peel => format!("{sha} {name} peeled:{commit}\n"),
                        _ => format!("{sha} {name}\n"),
                    }));
                    lines.push("0000".into());
                    framed(&lines, "application/x-git-upload-pack-result")
                }
                "/repos/acme/widgets/pulls" => {
                    if let Some((code, extra)) = &f.pulls_answer {
                        let mut res = StatusCode::from_u16(*code).unwrap().into_response();
                        for (name, value) in extra {
                            res.headers_mut().insert(*name, value.parse().unwrap());
                        }
                        return res;
                    }
                    let page: usize = uri
                        .query()
                        .unwrap_or("")
                        .split('&')
                        .find_map(|kv| kv.strip_prefix("page="))
                        .and_then(|p| p.parse().ok())
                        .unwrap_or(1);
                    let size = if f.page_size == 0 { 100 } else { f.page_size };
                    let pages: Vec<&[serde_json::Value]> = f.pulls.chunks(size).collect();
                    let items = pages.get(page - 1).copied().unwrap_or_default();
                    let body = serde_json::to_string(items).unwrap();
                    let etag = format!(
                        "W/\"{}\"",
                        &hex::encode(Sha256::digest(body.as_bytes()))[..16]
                    );
                    if page == 1 && h("if-none-match") == etag {
                        return StatusCode::NOT_MODIFIED.into_response();
                    }
                    let mut res = (
                        [
                            ("content-type", "application/json"),
                            ("etag", etag.as_str()),
                        ],
                        body,
                    )
                        .into_response();
                    if page < pages.len() {
                        let link = format!(
                            "<https://api.example/repos/acme/widgets/pulls?page={}>; rel=\"next\"",
                            page + 1
                        );
                        res.headers_mut().insert("link", link.parse().unwrap());
                    }
                    res
                }
                path if path.starts_with("/repos/acme/widgets/contents/") => {
                    match f
                        .files
                        .get(path.trim_start_matches("/repos/acme/widgets/contents/"))
                    {
                        Some(text) => text.clone().into_response(),
                        None => StatusCode::NOT_FOUND.into_response(),
                    }
                }
                // The commit a ref names, as its id alone: a tag's own commit.
                path if path.starts_with("/repos/acme/widgets/commits/") => {
                    let name = format!(
                        "refs/{}",
                        path.trim_start_matches("/repos/acme/widgets/commits/")
                    );
                    match f.peeled.get(&name).or_else(|| f.refs.get(&name)) {
                        Some(sha) => sha.clone().into_response(),
                        None => StatusCode::NOT_FOUND.into_response(),
                    }
                }
                // How the second commit stands to the first, as GitHub says
                // it. A commit nothing here knows is not found.
                path if path.starts_with("/repos/acme/widgets/compare/") => {
                    let range = path.trim_start_matches("/repos/acme/widgets/compare/");
                    let Some((base, head)) = range.split_once("...") else {
                        return StatusCode::NOT_FOUND.into_response();
                    };
                    let known = |sha: &str| {
                        f.refs.values().any(|v| v == sha)
                            || f.behind.contains_key(sha)
                            || f.behind
                                .values()
                                .any(|behind| behind.iter().any(|b| b == sha))
                    };
                    if !known(base) || !known(head) {
                        return StatusCode::NOT_FOUND.into_response();
                    }
                    let has = |of: &str, sha: &str| {
                        f.behind
                            .get(of)
                            .is_some_and(|behind| behind.iter().any(|b| b == sha))
                    };
                    let status = if base == head {
                        "identical"
                    } else if has(head, base) {
                        "ahead"
                    } else if has(base, head) {
                        "behind"
                    } else {
                        "diverged"
                    };
                    axum::Json(serde_json::json!({ "status": status, "commits": [] }))
                        .into_response()
                }
                _ => StatusCode::NOT_FOUND.into_response(),
            }
        }
        let forge: StubForge = Arc::default();
        let app = Router::new().fallback(handle).with_state(forge.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (origin, forge)
    }

    pub(crate) fn refs(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(name, sha)| (name.to_string(), sha.to_string()))
            .collect()
    }

    fn same_repo() -> serde_json::Value {
        json!({ "id": 7, "full_name": "acme/widgets" })
    }

    fn poller(db: &Db, origin: &str, token: Option<&str>) -> Poller {
        Poller::new(
            db.clone(),
            Arc::new(Forge::new(token.map(str::to_string)).with_origin(origin)),
            300,
            Arc::new(Metrics::default()),
        )
    }

    /// Run `sql` (Postgres-style) on either driver.
    async fn exec(db: &Db, sql: &str) {
        match db {
            Db::Postgres(p) => sqlx::query(sql).execute(p).await.map(drop),
            Db::Sqlite(p) => sqlx::query(sql).execute(p).await.map(drop),
        }
        .unwrap();
    }

    /// Every repository due now, as if its interval had passed.
    async fn due_now(db: &Db) {
        exec(db, "UPDATE ci_repos SET poll_due_at = NULL").await;
    }

    /// `(kind, ref, sha, trust, source)` of every queued event, sorted.
    async fn queued(db: &Db) -> Vec<(String, String, String, String, String)> {
        let sql = "SELECT kind, ref, sha, trust, source FROM ci_events ORDER BY kind, ref, sha";
        match db {
            Db::Postgres(p) => sqlx::query_as(sql).fetch_all(p).await,
            Db::Sqlite(p) => sqlx::query_as(sql).fetch_all(p).await,
        }
        .unwrap()
    }

    fn row(
        kind: &str,
        git_ref: &str,
        sha: &str,
        trust: &str,
    ) -> (String, String, String, String, String) {
        (
            kind.into(),
            git_ref.into(),
            sha.into(),
            trust.into(),
            "poll".into(),
        )
    }

    fn observed(refs: BTreeMap<String, String>, pulls: Pulls) -> Observed {
        Observed {
            refs,
            peeled: BTreeMap::new(),
            pulls,
        }
    }

    fn pr(number: i64, sha: &str) -> NewEvent {
        NewEvent {
            kind: Kind::PullRequest,
            git_ref: format!("refs/pull/{number}/head"),
            sha: sha.into(),
            commit: None,
            base_sha: Some(T1.into()),
            base_ref: Some("main".into()),
            pr: Some(number),
            actor: Some("contributor".into()),
            trust: Trust::Trusted,
            association: None,
            head_ref: None,
        }
    }

    #[test]
    fn a_baseline_records_everything_and_queues_nothing() {
        let seen = observed(
            refs(&[("refs/heads/main", A), ("refs/tags/v1", T1)]),
            Pulls::Listed {
                events: vec![pr(7, B)],
                complete: true,
                etag: Some("W/\"e\"".into()),
            },
        );
        let write = diff(&BTreeMap::new(), &seen, true, None);
        assert!(write.events.is_empty());
        assert_eq!(write.set.len(), 3);
        assert_eq!(write.etag.as_deref(), Some("W/\"e\""));
    }

    /// A merge request whose base was not read is left out of the sweep's
    /// write, its event and its commit, so that the next sweep queues it.
    #[test]
    fn a_deferred_merge_request_is_found_again_next_sweep() {
        let mr = |iid: i64, sha: &str| NewEvent {
            git_ref: format!("refs/merge-requests/{iid}/head"),
            base_sha: None,
            ..pr(iid, sha)
        };
        let seen = observed(
            refs(&[("refs/heads/main", B)]),
            Pulls::Listed {
                events: vec![mr(1, C), mr(2, D)],
                complete: true,
                etag: None,
            },
        );
        let last = refs(&[("refs/heads/main", A)]);
        let mut write = diff(&last, &seen, false, None);
        assert_eq!(write.events.len(), 3);
        defer(&mut write, &["refs/merge-requests/2/head".to_string()]);
        let names: Vec<&str> = write.events.iter().map(|e| e.git_ref.as_str()).collect();
        assert_eq!(names, ["refs/heads/main", "refs/merge-requests/1/head"]);
        assert!(write
            .set
            .iter()
            .all(|(name, _)| name != "refs/merge-requests/2/head"));
        // Not recorded, it is new to the next sweep, which queues it.
        let again = diff(&last, &seen, false, None);
        assert!(again
            .events
            .iter()
            .any(|e| e.git_ref == "refs/merge-requests/2/head"));
    }

    /// A GitLab merge request that is no longer open is forgotten, as a
    /// pull request is, once the listing is whole.
    #[test]
    fn a_merge_request_no_longer_listed_is_forgotten() {
        let last = refs(&[
            ("refs/heads/main", A),
            ("refs/merge-requests/3/head", B),
            ("refs/merge-requests/4/head", C),
        ]);
        let open = NewEvent {
            git_ref: "refs/merge-requests/4/head".into(),
            ..pr(4, C)
        };
        let listed = |complete| {
            observed(
                refs(&[("refs/heads/main", A)]),
                Pulls::Listed {
                    events: vec![open.clone()],
                    complete,
                    etag: None,
                },
            )
        };
        let write = diff(&last, &listed(true), false, None);
        assert_eq!(write.drop, ["refs/merge-requests/3/head"]);
        assert!(write.events.is_empty());
        let write = diff(&last, &listed(false), false, None);
        assert!(write.drop.is_empty(), "not from a listing cut short");
    }

    #[test]
    fn what_changed_is_queued_as_a_webhook_would_queue_it() {
        let last = refs(&[
            ("refs/heads/main", A),
            ("refs/heads/gone", A),
            ("refs/tags/v1", T1),
            ("refs/pull/7/head", B),
            ("refs/pull/8/head", C),
        ]);
        let seen = observed(
            refs(&[
                ("refs/heads/main", B),
                ("refs/heads/new", C),
                ("refs/tags/v1", T2),
            ]),
            Pulls::Listed {
                events: vec![pr(7, C), pr(7, C), pr(9, D)],
                complete: true,
                etag: None,
            },
        );
        let write = diff(&last, &seen, false, Some("W/\"old\"".into()));
        let events: Vec<(Kind, &str, &str)> = write
            .events
            .iter()
            .map(|e| (e.kind, e.git_ref.as_str(), e.sha.as_str()))
            .collect();
        assert_eq!(
            events,
            [
                (Kind::Push, "refs/heads/main", B),
                (Kind::Push, "refs/heads/new", C),
                (Kind::Tag, "refs/tags/v1", T2),
                // A branch gone: the commit it last pointed at.
                (Kind::Delete, "refs/heads/gone", A),
                // Listed twice across a moving page boundary, queued once.
                (Kind::PullRequest, "refs/pull/7/head", C),
                (Kind::PullRequest, "refs/pull/9/head", D),
            ]
        );
        assert!(write.events[..4]
            .iter()
            .all(|e| e.trust == Trust::Trusted && e.actor.is_none()));
        // A moved branch is measured against what the last sweep saw; a new
        // branch, a tag and a deletion against nothing.
        let bases: Vec<Option<&str>> = write.events[..4]
            .iter()
            .map(|e| e.base_sha.as_deref())
            .collect();
        assert_eq!(bases, [Some(A), None, None, None]);
        // Pull request 8 closed: forgotten, and no event.
        assert_eq!(write.drop, ["refs/heads/gone", "refs/pull/8/head"]);
        assert_eq!(
            write.etag, None,
            "a listing that kept no ETag drops the old one"
        );
    }

    #[test]
    fn an_unread_or_partial_pull_request_list_forgets_none() {
        let last = refs(&[("refs/heads/main", A), ("refs/pull/7/head", B)]);
        let unchanged = diff(
            &last,
            &observed(refs(&[("refs/heads/main", A)]), Pulls::Unchanged),
            false,
            Some("W/\"kept\"".into()),
        );
        assert_eq!(
            unchanged,
            SweepWrite {
                etag: Some("W/\"kept\"".into()),
                ..SweepWrite::default()
            }
        );
        let partial = diff(
            &last,
            &observed(
                refs(&[("refs/heads/main", A)]),
                Pulls::Listed {
                    events: vec![pr(9, C)],
                    complete: false,
                    etag: None,
                },
            ),
            false,
            None,
        );
        assert!(
            partial.drop.is_empty(),
            "pull request 7 may be on a page not read"
        );
        assert_eq!(partial.events.len(), 1);
        let unlisted = diff(
            &last,
            &observed(refs(&[("refs/heads/main", A)]), Pulls::NotListed),
            false,
            None,
        );
        assert_eq!(unlisted, SweepWrite::default());
    }

    /// An annotated tag is queued under its own id, the key a webhook derives
    /// too, with the commit it names beside it. A lightweight tag has none.
    #[test]
    fn an_annotated_tag_is_queued_under_its_own_id_with_its_commit() {
        let seen = Observed {
            refs: refs(&[("refs/tags/light", A), ("refs/tags/v1", T1)]),
            peeled: refs(&[("refs/tags/v1", B)]),
            pulls: Pulls::NotListed,
        };
        let write = diff(&BTreeMap::new(), &seen, false, None);
        let tags: Vec<(Kind, &str, &str, Option<&str>)> = write
            .events
            .iter()
            .map(|e| (e.kind, e.git_ref.as_str(), e.sha.as_str(), e.commit.as_deref()))
            .collect();
        assert_eq!(
            tags,
            [
                (Kind::Tag, "refs/tags/light", A, None),
                (Kind::Tag, "refs/tags/v1", T1, Some(B)),
            ]
        );
        // What the next sweep compares with is the tag's own id.
        assert!(write
            .set
            .contains(&("refs/tags/v1".to_string(), T1.to_string())));
    }

    /// A sweep that finds an annotated tag queues it under the tag's own id
    /// and hands a worker the commit, from a v2 server (which is asked to
    /// peel) and from a v0 one (which lists the commit as a ref of its own).
    #[tokio::test]
    async fn a_swept_annotated_tag_runs_at_the_commit_it_names() {
        for v0 in [false, true] {
            let (db, _dir) = sqlite().await;
            let (origin, forge) = stub_forge().await;
            db.add_repo("generic", "git.example", "acme/widgets", Some("wf-1"), "default")
                .await
                .unwrap()
                .unwrap();
            {
                let mut f = forge.lock().unwrap();
                f.v0 = v0;
                f.refs = refs(&[("refs/heads/main", A)]);
            }
            let poller = poller(&db, &origin, None);
            assert_eq!(
                poller.sweep_next("s1").await.unwrap(),
                Some(Swept::Baseline(1))
            );
            {
                let mut f = forge.lock().unwrap();
                f.refs = refs(&[("refs/heads/main", A), ("refs/tags/v1", T1)]);
                f.peeled = refs(&[("refs/tags/v1", A)]);
            }
            due_now(&db).await;
            let Some(Swept::Queued(ids)) = poller.sweep_next("s1").await.unwrap() else {
                panic!("the tag is queued (v0: {v0})");
            };
            assert_eq!(ids.len(), 1);
            assert_eq!(
                queued(&db).await,
                [(
                    "tag".to_string(),
                    "refs/tags/v1".to_string(),
                    T1.to_string(),
                    "trusted".to_string(),
                    "poll".to_string()
                )],
                "stored under the tag's own id (v0: {v0})"
            );
            let ev = db
                .claim_event("w", Duration::from_secs(60))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(ev.sha, A, "the worker is handed the commit (v0: {v0})");
            assert!(
                ev.commit_key.ends_with(&format!(":tag:refs/tags/v1:{T1}")),
                "{}",
                ev.commit_key
            );
        }
    }

    /// A dispatch that names a ref and no commit runs at the commit the ref
    /// points at: for an annotated tag the one it names, not the tag's own id.
    #[tokio::test]
    async fn a_ref_resolves_to_the_commit_an_annotated_tag_names() {
        for v0 in [false, true] {
            let (origin, forge) = stub_forge().await;
            {
                let mut f = forge.lock().unwrap();
                f.v0 = v0;
                f.refs = refs(&[
                    ("refs/heads/main", A),
                    ("refs/tags/light", B),
                    ("refs/tags/v1", T1),
                ]);
                f.peeled = refs(&[("refs/tags/v1", C)]);
            }
            let reader = Forge::new(None).with_origin(&origin);
            let repo = crate::db::Repo {
                forge: "generic".into(),
                forge_host: "git.example".into(),
                full_name: "acme/widgets".into(),
                ..Default::default()
            };
            for (git_ref, want) in [
                ("refs/tags/v1", Some(C)),
                ("refs/tags/light", Some(B)),
                ("refs/heads/main", Some(A)),
                ("refs/tags/none", None),
            ] {
                assert_eq!(
                    reader.resolve(&repo, git_ref).await.unwrap().as_deref(),
                    want,
                    "{git_ref} (v0: {v0})"
                );
            }
        }
    }

    #[test]
    fn the_next_sweep_is_jittered_by_a_tenth() {
        let interval = Duration::from_secs(300);
        let draws: Vec<Duration> = (0..200).map(|_| jitter(interval)).collect();
        assert!(draws
            .iter()
            .all(|d| (270.0..=330.0).contains(&d.as_secs_f64())));
        assert!(draws.iter().any(|d| *d != draws[0]), "it varies");
    }

    async fn the_poll_contract(db: &Db) {
        let (origin, forge) = stub_forge().await;
        let repo = db
            .add_repo(
                "github",
                "github.com",
                "acme/widgets",
                Some("wf-1"),
                "default",
            )
            .await
            .unwrap()
            .unwrap();
        {
            let mut f = forge.lock().unwrap();
            f.refs = refs(&[
                ("refs/heads/main", A),
                ("refs/tags/v1", T1),
                ("refs/pull/7/head", B),
                ("refs/pull/7/merge", C),
            ]);
            f.pulls = vec![listed_pull(7, B, same_repo(), "MEMBER")];
        }
        let poller = poller(db, &origin, Some("ghp_test"));

        // The first sweep records what it finds and queues nothing.
        assert_eq!(
            poller.sweep_next("s1").await.unwrap(),
            Some(Swept::Baseline(3))
        );
        assert_eq!(
            poller.sweep_next("s1").await.unwrap(),
            None,
            "not due again for an interval"
        );
        assert!(queued(db).await.is_empty());
        {
            let f = forge.lock().unwrap();
            let lines: Vec<&str> = f.seen.iter().map(|r| r.line.as_str()).collect();
            assert_eq!(
                lines,
                [
                    "GET /acme/widgets.git/info/refs?service=git-upload-pack",
                    "POST /acme/widgets.git/git-upload-pack",
                    "GET /repos/acme/widgets/pulls?state=open&per_page=100&page=1",
                ]
            );
            assert!(f.seen[..2]
                .iter()
                .all(|r| r.authorization == BASIC && r.git_protocol == "version=2"));
            assert_eq!(f.seen[2].authorization, "Bearer ghp_test");
            assert_eq!(f.seen[2].if_none_match, "", "nothing to match yet");
        }

        // Then main moves, a branch is pushed, v1 is deleted and v2 tagged,
        // pull request 7 is pushed to, and a fork opens 8.
        {
            let mut f = forge.lock().unwrap();
            f.refs = refs(&[
                ("refs/heads/main", B),
                ("refs/heads/topic", C),
                ("refs/tags/v2", T2),
                ("refs/pull/7/head", C),
            ]);
            f.pulls = vec![
                listed_pull(7, C, same_repo(), "MEMBER"),
                listed_pull(
                    8,
                    D,
                    json!({ "id": 99, "full_name": "contributor/widgets" }),
                    "NONE",
                ),
            ];
        }
        due_now(db).await;
        let Some(Swept::Queued(ids)) = poller.sweep_next("s1").await.unwrap() else {
            panic!("a sweep that saw changes queues them");
        };
        assert_eq!(ids.len(), 6);
        assert_eq!(
            queued(db).await,
            [
                row("delete", "refs/tags/v1", T1, "trusted"),
                row("pull_request", "refs/pull/7/head", C, "trusted"),
                row("pull_request", "refs/pull/8/head", D, "untrusted"),
                row("push", "refs/heads/main", B, "trusted"),
                row("push", "refs/heads/topic", C, "trusted"),
                row("tag", "refs/tags/v2", T2, "trusted"),
            ]
        );
        let commit_key = |e: &NewEvent| crate::event::commit_key(&repo.id, e);
        let keys: Vec<String> = {
            let sql = "SELECT commit_key FROM ci_events WHERE kind = 'push' ORDER BY ref";
            match db {
                Db::Postgres(p) => sqlx::query_scalar(sql).fetch_all(p).await,
                Db::Sqlite(p) => sqlx::query_scalar(sql).fetch_all(p).await,
            }
            .unwrap()
        };
        assert_eq!(
            keys[0],
            commit_key(&trusted(Kind::Push, "refs/heads/main", B)),
            "the key a webhook's push would carry"
        );

        // Nothing changed: the pull requests answer 304 to the kept ETag.
        due_now(db).await;
        assert_eq!(
            poller.sweep_next("s1").await.unwrap(),
            Some(Swept::Unchanged)
        );
        {
            let f = forge.lock().unwrap();
            let last = f.seen.last().unwrap();
            assert!(last.line.contains("/pulls?") && last.if_none_match.starts_with("W/\""));
        }

        // A closed pull request is forgotten without an event.
        forge
            .lock()
            .unwrap()
            .pulls
            .retain(|p| p["number"] != json!(8));
        due_now(db).await;
        assert_eq!(
            poller.sweep_next("s1").await.unwrap(),
            Some(Swept::Unchanged)
        );
        let seen = db.poll_refs(&repo.id).await.unwrap();
        assert!(!seen.contains_key("refs/pull/8/head"));
        assert_eq!(seen["refs/pull/7/head"], C);

        // Over one page: every page is read, and no ETag is kept, since a
        // change on a later page leaves the first one's alone. GitHub lists
        // the newest first, so a new pull request always changes page 1.
        {
            let mut f = forge.lock().unwrap();
            f.page_size = 1;
            f.pulls.insert(0, listed_pull(9, D, same_repo(), "OWNER"));
        }
        due_now(db).await;
        assert!(matches!(
            poller.sweep_next("s1").await.unwrap(),
            Some(Swept::Queued(ids)) if ids.len() == 1
        ));
        due_now(db).await;
        assert_eq!(
            poller.sweep_next("s1").await.unwrap(),
            Some(Swept::Unchanged)
        );
        {
            let f = forge.lock().unwrap();
            let pages: Vec<&Request> = f
                .seen
                .iter()
                .filter(|r| r.line.contains("/pulls?"))
                .collect();
            let last_two = &pages[pages.len() - 2..];
            assert!(last_two[0].line.ends_with("page=1") && last_two[1].line.ends_with("page=2"));
            assert_eq!(last_two[0].if_none_match, "");
        }

        // A spent rate limit holds the next sweep until it resets.
        let reset = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 1200;
        forge.lock().unwrap().pulls_answer = Some((
            403,
            vec![
                ("x-ratelimit-remaining", "0".into()),
                ("x-ratelimit-reset", reset.to_string()),
            ],
        ));
        due_now(db).await;
        let Some(Swept::RateLimited(why)) = poller.sweep_next("s1").await.unwrap() else {
            panic!("a spent rate limit is its own outcome");
        };
        assert!(why.contains("rate limit is spent"), "{why}");
        let held: bool = {
            let (pg, lite) = (
                "SELECT poll_due_at > now() + make_interval(secs => 1000) FROM ci_repos",
                "SELECT poll_due_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+1000 seconds') FROM ci_repos",
            );
            match db {
                Db::Postgres(p) => sqlx::query_scalar(pg).fetch_one(p).await,
                Db::Sqlite(p) => sqlx::query_scalar(lite).fetch_one(p).await,
            }
            .unwrap()
        };
        assert!(held, "next due when the limit resets, not in 300 s");
        let listed = db.list_repos().await.unwrap();
        assert_eq!(listed[0].poll_error.as_deref(), Some(why.as_str()));
        assert!(listed[0].polled_at.is_some());

        // A repository that sets 0 is never polled.
        assert!(db
            .set_poll_secs("github.com", "acme/widgets", Some(0))
            .await
            .unwrap());
        assert_eq!(poller.sweep_next("s1").await.unwrap(), None);
    }

    #[tokio::test]
    async fn sqlite_keeps_the_poll_contract() {
        let (db, _dir) = sqlite().await;
        the_poll_contract(&db).await;
    }

    #[tokio::test]
    async fn postgres_keeps_the_poll_contract() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        the_poll_contract(&db).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    async fn a_sweep_that_lost_its_claim_writes_nothing(db: &Db) {
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let first = db.claim_sweep("a", LEASE, 300).await.unwrap().unwrap();
        assert!(first.baseline);
        assert_eq!(first.poll_secs, 300);
        assert!(
            db.claim_sweep("b", LEASE, 300).await.unwrap().is_none(),
            "held"
        );
        // Sweeper a outlives its lease, and b takes the repository over.
        due_now(db).await;
        assert!(db.claim_sweep("b", LEASE, 300).await.unwrap().is_some());
        let write = SweepWrite {
            events: vec![trusted(Kind::Push, "refs/heads/main", A)],
            set: vec![("refs/heads/main".into(), A.into())],
            ..SweepWrite::default()
        };
        let next = Duration::from_secs(300);
        assert_eq!(
            db.finish_sweep(&repo.id, "a", &write, next).await.unwrap(),
            None
        );
        assert!(!db.fail_sweep(&repo.id, "a", "late", next).await.unwrap());
        assert!(queued(db).await.is_empty() && db.poll_refs(&repo.id).await.unwrap().is_empty());
        let ids = db
            .finish_sweep(&repo.id, "b", &write, next)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(db.poll_refs(&repo.id).await.unwrap().len(), 1);
        // Finished: no longer a baseline, and not due.
        assert!(db.claim_sweep("c", LEASE, 300).await.unwrap().is_none());
        due_now(db).await;
        assert!(
            !db.claim_sweep("c", LEASE, 300)
                .await
                .unwrap()
                .unwrap()
                .baseline
        );
        // CI_POLL_SECS=0 leaves a repository that sets nothing alone.
        due_now(db).await;
        assert!(db.claim_sweep("d", LEASE, 0).await.unwrap().is_none());
        assert!(db
            .set_poll_secs("github.com", "acme/widgets", Some(60))
            .await
            .unwrap());
        assert_eq!(
            db.claim_sweep("d", LEASE, 0)
                .await
                .unwrap()
                .unwrap()
                .poll_secs,
            60
        );

        // Disconnected, it is swept no more, and the sweep that held it
        // writes nothing. Connected again, it starts over: the interval it
        // had is gone, and its first sweep is a baseline, so that what moved
        // while it was away is not run.
        let gone = db
            .disconnect_repo("github.com", "acme/widgets", "alice")
            .await
            .unwrap();
        assert_eq!(gone.as_deref(), Some(repo.id.as_str()));
        assert_eq!(
            db.finish_sweep(&repo.id, "d", &write, next).await.unwrap(),
            None
        );
        due_now(db).await;
        assert!(db.claim_sweep("e", LEASE, 300).await.unwrap().is_none());
        db.connect_repo(
            "github",
            "github.com",
            "acme/widgets",
            None,
            "default",
            None,
            crate::db::Mode::Compat,
            "alice",
        )
        .await
        .unwrap();
        let again = db.claim_sweep("e", LEASE, 300).await.unwrap().unwrap();
        assert!(again.baseline);
        assert_eq!(again.poll_secs, 300);
        assert_eq!(
            db.poll_refs(&repo.id).await.unwrap().len(),
            1,
            "what it had seen is what the baseline is taken over"
        );
    }

    #[tokio::test]
    async fn sqlite_fences_a_lost_sweep() {
        let (db, _dir) = sqlite().await;
        a_sweep_that_lost_its_claim_writes_nothing(&db).await;
    }

    #[tokio::test]
    async fn postgres_fences_a_lost_sweep() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        a_sweep_that_lost_its_claim_writes_nothing(&db).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    #[tokio::test]
    async fn the_token_goes_to_github_com_only() {
        let (db, _dir) = sqlite().await;
        let (origin, forge) = stub_forge().await;
        forge.lock().unwrap().refs = refs(&[("refs/heads/main", A)]);
        db.add_repo(
            "github",
            "git.corp.example",
            "acme/widgets",
            None,
            "default",
        )
        .await
        .unwrap()
        .unwrap();
        let poller = poller(&db, &origin, Some("ghp_test"));
        assert_eq!(
            poller.sweep_next("s1").await.unwrap(),
            Some(Swept::Baseline(1))
        );
        let f = forge.lock().unwrap();
        assert_eq!(f.seen.len(), 3);
        assert!(
            f.seen.iter().all(|r| r.authorization.is_empty()),
            "{:?}",
            f.seen
        );
    }

    /// What must hold for any git server: the sweep baselines and then
    /// queues a push against a server that opens its v2 answer with
    /// `version 2`, as git's own does; against one that opens with the service
    /// line, as github.com does; and against a v0 server.
    #[tokio::test]
    async fn a_sweep_reads_each_way_a_server_frames_its_answer() {
        for (framing, stock, v0) in [
            ("git's own v2", true, false),
            ("github.com's v2", false, false),
            ("v0", false, true),
        ] {
            let (db, _dir) = sqlite().await;
            let (origin, forge) = stub_forge().await;
            {
                let mut f = forge.lock().unwrap();
                f.stock = stock;
                f.v0 = v0;
                f.refs = refs(&[("refs/heads/main", A)]);
            }
            db.add_repo("generic", "git.example", "acme/widgets", Some("wf-1"), "default")
                .await
                .unwrap()
                .unwrap();
            let poller = poller(&db, &origin, None);
            assert_eq!(
                poller.sweep_next("s1").await.unwrap(),
                Some(Swept::Baseline(1)),
                "{framing}"
            );
            forge.lock().unwrap().refs = refs(&[("refs/heads/main", B)]);
            due_now(&db).await;
            let Some(Swept::Queued(ids)) = poller.sweep_next("s1").await.unwrap() else {
                panic!("the push is queued ({framing})");
            };
            assert_eq!(ids.len(), 1, "{framing}");
            assert_eq!(
                queued(&db).await,
                [row("push", "refs/heads/main", B, "trusted")],
                "{framing}"
            );
            // A dispatch that names no commit reads the ref the same way.
            let reader = Forge::new(None).with_origin(&origin);
            let repo = crate::db::Repo {
                forge: "generic".into(),
                forge_host: "git.example".into(),
                full_name: "acme/widgets".into(),
                ..Default::default()
            };
            assert_eq!(
                reader.resolve(&repo, "refs/heads/main").await.unwrap().as_deref(),
                Some(B),
                "{framing}"
            );
        }
    }

    #[tokio::test]
    async fn a_v0_server_and_a_forge_without_pulls_still_poll_branches_and_tags() {
        let (db, _dir) = sqlite().await;
        let (origin, forge) = stub_forge().await;
        {
            let mut f = forge.lock().unwrap();
            f.v0 = true;
            f.refs = refs(&[
                ("refs/heads/main", A),
                ("refs/merge-requests/1/head", B),
                ("refs/tags/v1", T1),
            ]);
        }
        db.add_repo("gitea", "gitea.example", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let poller = poller(&db, &origin, None);
        assert_eq!(
            poller.sweep_next("s1").await.unwrap(),
            Some(Swept::Baseline(2))
        );
        forge
            .lock()
            .unwrap()
            .refs
            .insert("refs/heads/topic".into(), C.into());
        due_now(&db).await;
        assert!(matches!(
            poller.sweep_next("s1").await.unwrap(),
            Some(Swept::Queued(ids)) if ids.len() == 1
        ));
        let f = forge.lock().unwrap();
        assert!(
            f.seen
                .iter()
                .all(|r| r.line.starts_with("GET /acme/widgets.git/info/refs")),
            "one request per sweep, and no pull request API: {:?}",
            f.seen
        );
    }

    /// The plan's acceptance: a webhook and a poll that see one commit produce
    /// one engine run, whichever arrives first.
    async fn a_webhook_and_a_poll_share_one_run(db: Db) {
        use crate::dispatch::tests::stub_api;
        use crate::dispatch::Dispatcher;
        use axum::body::Body;
        use tower::ServiceExt;

        let dir = tempfile::tempdir().unwrap();
        let secret = dir.path().join("secret");
        std::fs::write(&secret, "s3cret").unwrap();
        let config = crate::Config::from_lookup(|name| match name {
            "CI_DATABASE_URL" => Some("sqlite://unused.db".into()),
            "CI_GITHUB_WEBHOOK_SECRET_FILE" => Some(secret.to_str().unwrap().into()),
            _ => None,
        })
        .unwrap();
        let app = crate::router(crate::AppState::new(
            db.clone(),
            &config,
            crate::Seams::default(),
        ));
        let deliver = |id: &'static str, sha: &'static str| {
            let body = json!({
                "ref": "refs/heads/main",
                "before": A,
                "after": sha,
                "repository": { "id": 7, "full_name": "acme/widgets", "html_url": "https://github.com/acme/widgets" },
                "sender": { "login": "octocat" },
            })
            .to_string();
            let request = axum::http::Request::post("/hooks/github")
                .header("content-type", "application/json")
                .header(github::EVENT_HEADER, "push")
                .header(github::DELIVERY_HEADER, id)
                .header(
                    github::SIGNATURE_HEADER,
                    github::sign(b"s3cret", body.as_bytes()),
                )
                .body(Body::from(body))
                .unwrap();
            let app = app.clone();
            async move { app.oneshot(request).await.unwrap().status() }
        };

        let (api, runs) = stub_api(vec![(201, "")]).await;
        let (origin, forge) = stub_forge().await;
        db.add_repo(
            "github",
            "github.com",
            "acme/widgets",
            Some("wf-1"),
            "default",
        )
        .await
        .unwrap()
        .unwrap();
        let wake = Arc::new(Notify::new());
        let (stop_tx, stop) = watch::channel(false);
        let cells = [("default".to_string(), api)].into_iter().collect();
        // The repository holds no pipelines (the stub has none to serve), so
        // its bound workflow runs.
        let dispatcher = Dispatcher::new(
            db.clone(),
            cells,
            "dgr_t".into(),
            Arc::new(Forge::new(None).with_origin(&origin)),
            crate::pipeline::compile::Settings::default(),
        );
        let mut workers = crate::worker::spawn(
            db.clone(),
            Arc::new(dispatcher),
            crate::worker::Settings {
                workers: 2,
                lease: Duration::from_secs(60),
                max_attempts: 3,
            },
            Arc::new(Metrics::default()),
            wake.clone(),
            stop,
        );
        let poller = poller(&db, &origin, None);
        let settled = |want: usize| {
            let db = db.clone();
            async move {
                for _ in 0..400 {
                    let events = db.list_events("", 50).await.unwrap();
                    if events.len() == want && events.iter().all(|e| e.state == "done") {
                        return events;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                panic!("{want} events never all finished");
            }
        };

        forge.lock().unwrap().refs = refs(&[("refs/heads/main", A)]);
        assert_eq!(
            poller.sweep_next("s1").await.unwrap(),
            Some(Swept::Baseline(1))
        );

        // The webhook first: main moves to B, and the poll sees it later.
        assert_eq!(deliver("d-1", B).await, StatusCode::ACCEPTED);
        forge.lock().unwrap().refs = refs(&[("refs/heads/main", B)]);
        settled(1).await;
        due_now(&db).await;
        assert!(matches!(
            poller.sweep_next("s1").await.unwrap(),
            Some(Swept::Queued(ids)) if ids.len() == 1
        ));
        wake.notify_one();
        settled(2).await;

        // The poll first, as when the delivery was dropped: main moves to C,
        // and the delivery comes late.
        forge.lock().unwrap().refs = refs(&[("refs/heads/main", C)]);
        due_now(&db).await;
        assert!(matches!(
            poller.sweep_next("s1").await.unwrap(),
            Some(Swept::Queued(ids)) if ids.len() == 1
        ));
        wake.notify_one();
        settled(3).await;
        assert_eq!(deliver("d-2", C).await, StatusCode::ACCEPTED);
        let events = settled(4).await;

        let runs = runs.lock().unwrap().clone();
        assert_eq!(
            runs.iter()
                .map(|r| r.params["ci_sha"].as_str())
                .collect::<Vec<_>>(),
            [B, C],
            "one run per commit, though each was seen twice"
        );
        let already = events
            .iter()
            .filter(|e| {
                e.reason
                    .as_deref()
                    .is_some_and(|r| r.contains("was submitted for this commit already"))
            })
            .count();
        assert_eq!(already, 2, "{events:?}");
        let _ = stop_tx.send(true);
        while workers.join_next().await.is_some() {}
    }

    #[tokio::test]
    async fn sqlite_runs_a_commit_seen_by_both_doors_once() {
        let (db, _dir) = sqlite().await;
        a_webhook_and_a_poll_share_one_run(db).await;
    }

    #[tokio::test]
    async fn postgres_runs_a_commit_seen_by_both_doors_once() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        a_webhook_and_a_poll_share_one_run(db.clone()).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    #[tokio::test]
    async fn a_refusal_is_recorded_with_what_to_do() {
        let (db, _dir) = sqlite().await;
        let (origin, forge) = stub_forge().await;
        forge.lock().unwrap().git_answer = Some(401);
        db.add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let Some(Swept::Failed(why)) = poller(&db, &origin, None).sweep_next("s1").await.unwrap()
        else {
            panic!("a refused listing fails the sweep");
        };
        assert_eq!(
            why,
            "listing refs: HTTP 401; a private repository needs the App installed on it \
             (CI_GITHUB_APP_ID), or CI_GITHUB_TOKEN_FILE, a token that can read it"
        );
        let listed = db.list_repos().await.unwrap();
        assert_eq!(listed[0].poll_error.as_deref(), Some(why.as_str()));
        assert_eq!(listed[0].polled_at, None, "still to take its baseline");
    }
}
