//! The run API. `POST /ci/v1/runs` queues a dispatch: a run of a repository's
//! pipeline at a ref, asked for by a caller rather than announced by a forge.
//! `GET /ci/v1/runs/{id}` says what became of it, or of any other event.
//!
//! **Who may call it.** dagron-ci keeps no users. A caller presents a
//! dagron-api credential, a session or a personal access token, as
//! `Authorization: Bearer`, and dagron-ci asks the dagron-api of the
//! repository's cell who that is (`GET /api/me`). That dagron-api is the one
//! the run is submitted to, so the rule is its own: a viewer may read, and run
//! nothing; anyone else may run. The credential goes to that cell's dagron-api
//! and nowhere else, and is never stored.
//!
//! **Trust.** A user's dispatch is `trusted`, as a writer's push is. A caller
//! whose subject is `agent:<name>`, as an agent's token presents in
//! dagron-api, is recorded `agent`. Its runs are refused, unless this
//! dagron-ci runs them (`CI_AGENT_RUNS=untrusted`): then they run under the
//! floor of a run that is not trusted, and the agent reruns and cancels the
//! events it asked for, and no other.
//!
//! **Once.** A repeat of a request under the same `Idempotency-Key` returns
//! the event the first one queued, and the same key with another request is
//! refused (409). Keys are the caller's own: two callers' keys never meet.
//! Without a key, every request is a new run.
//!
//! **A ref, and maybe a commit.** `ref` is a full branch or tag ref; `sha`
//! pins the commit. Without `sha`, the forge is asked where the ref points
//! now, as the poll sweep asks it, and the run pins that commit.
//!
//! **Again.** `POST /ci/v1/runs/{id}/rerun` runs an event's failed or
//! cancelled runs again, each from its failed jobs; with `?failed=false`,
//! each of its runs that is over, one that succeeded too, whole: every job.
//! The same engine runs, so the same checks on the same commit, and no new
//! count. dagron-ci asks the cell's dagron-api for each rerun with the
//! caller's own credential, so who may rerun is that dagron-api's rule and
//! its record names the caller, and it re-opens its own record of the run in
//! the same step (`rerun.rs`). A rerun passes the build's limit on runs in
//! flight like a new run.
//!
//! **Stop.** `POST /ci/v1/runs/{id}/cancel` stops an event's runs, asked of
//! the cell's dagron-api with the caller's own credential too, and withdraws
//! an event that is still waiting to be dispatched (`api/cancel.rs`).
//!
//! **Read.** `GET /ci/v1/runs/{id}/logs` is what an event's runs wrote, read
//! from the cell's dagron-api as the caller (`api/logs.rs`).
//!
//! **Which run.** The `{id}` of each of those, and of `GET`, is an event's:
//! what `POST /ci/v1/runs` answered with. It may also be an engine run's, the
//! id a commit status links to (CI_RUN_URL), and then it means that one run
//! of its event and no other.
//!
//! **Who.** `GET /ci/v1/me` is who presents the credential, as every door
//! here asks it: what `dagron-ci login` checks a token with before it keeps
//! it.

pub(crate) mod cancel;
pub mod fleet;
mod logs;
pub mod repos;

use std::collections::BTreeMap;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::db::{Dispatched, EventView, RequestKey};
use crate::event::{is_commit_id, is_storable_ref, Kind, NewEvent, Trust};
use crate::metrics::{ApiOutcome, Reopened};
use crate::problem::problem;
use crate::rerun::{Asked, RerunFrom};
use crate::server::AppState;

pub use cancel::cancel_run;
pub use logs::run_logs;

/// The largest request body `POST /ci/v1/runs` reads.
pub const MAX_BODY: usize = 64 * 1024;

/// Inputs a dispatch may carry, and the size of each value.
const MAX_INPUTS: usize = 64;
const MAX_INPUT_BYTES: usize = 4096;

/// dagron-api's own limit on an `Idempotency-Key`, mirrored.
const MAX_IDEMPOTENCY_KEY: usize = 255;

/// The longest credential passed on to dagron-api.
const MAX_CREDENTIAL: usize = 4096;

/// `POST /ci/v1/runs`'s body.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunRequest {
    /// `HOST/OWNER/NAME`, as `dagron-ci repo add` takes it.
    repo: String,
    #[serde(rename = "ref")]
    git_ref: String,
    sha: Option<String>,
    #[serde(default)]
    inputs: BTreeMap<String, String>,
    /// The one pipeline to run: a file in `.dagron/ci`, `deploy` or
    /// `deploy.yaml`. Without it, every pipeline whose `on:` takes dispatch.
    pipeline: Option<String>,
    /// A patch bundle the caller uploaded (`POST /ci/v1/bundles`):
    /// the run is of its base, with it laid over, and untrusted.
    bundle: Option<String>,
}

/// A caller, as the dagron-api of a cell knows it (`GET /api/me`).
#[derive(Debug, Deserialize)]
struct Caller {
    sub: String,
    #[serde(default)]
    email: String,
    #[serde(default)]
    groups: Vec<String>,
}

impl Caller {
    fn viewer(&self) -> bool {
        self.groups.iter().any(|g| g == "viewer")
    }

    /// Whether the caller is an admin, as dagron-api knows one: the group
    /// its own admin routes ask for.
    fn admin(&self) -> bool {
        self.groups.iter().any(|g| g == "admin")
    }

    fn trust(&self) -> Trust {
        if self.sub.starts_with("agent:") {
            Trust::Agent
        } else {
            Trust::Trusted
        }
    }

    /// Who the event records as its actor: the caller's email, or its
    /// subject when it has none.
    fn actor(&self) -> String {
        if self.email.is_empty() {
            self.sub.clone()
        } else {
            self.email.clone()
        }
    }

    /// Whether the caller may change `event`, as one who may run: anyone
    /// may, but an agent only an event it asked for.
    fn owns(&self, event: &crate::db::EventView) -> bool {
        self.trust() != Trust::Agent || event.actor.as_deref() == Some(self.actor().as_str())
    }
}

/// Why a caller was not identified.
enum AuthError {
    /// dagron-api refused the credential: 401 for the caller.
    Refused,
    /// dagron-api could not be asked: 503, worth retrying.
    Unavailable(String),
}

/// `POST /ci/v1/runs`: 202 with the queued event, or 200 with the event an
/// earlier request under the same `Idempotency-Key` queued.
pub async fn create_run(State(st): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let refuse = |outcome: ApiOutcome, status: StatusCode, title: &str, detail: String| {
        st.metrics.api_request(outcome);
        problem(status, title, detail)
    };
    let Some(credential) = bearer(&headers) else {
        return refuse(
            ApiOutcome::Unauthorized,
            StatusCode::UNAUTHORIZED,
            "not authenticated",
            "send a dagron-api session or API token as Authorization: Bearer".into(),
        );
    };
    let parsed: Result<RunRequest, _> = serde_json::from_slice(&body);
    // The repository decides which dagron-api is asked; nothing about it is
    // told to a caller who has not been identified.
    let spec = parsed
        .as_ref()
        .ok()
        .and_then(|r| crate::cli::split_spec(&r.repo).ok());
    let fail = |(outcome, response): (ApiOutcome, Response)| {
        st.metrics.api_request(outcome);
        response
    };
    let repo = match spec {
        Some((host, name)) => match st.db.find_repo(host, name).await {
            Ok(repo) => repo,
            Err(e) => return fail(database_error(e)),
        },
        None => None,
    };
    let caller = match identify(&st, repo.as_ref().map(|r| r.cell.as_str()), credential).await {
        Ok(caller) => caller,
        Err(failure) => return fail(failure),
    };
    if caller.viewer() {
        return refuse(
            ApiOutcome::Forbidden,
            StatusCode::FORBIDDEN,
            "read-only caller",
            "a viewer may read runs, and run nothing".into(),
        );
    }

    let request = match parsed {
        Ok(request) => request,
        Err(e) => {
            return refuse(
                ApiOutcome::BadRequest,
                StatusCode::BAD_REQUEST,
                "unreadable request",
                format!("the body is not a run request: {e}"),
            )
        }
    };
    if let Err(why) = validate(&request) {
        return refuse(
            ApiOutcome::BadRequest,
            StatusCode::BAD_REQUEST,
            "invalid run request",
            why,
        );
    }
    let Some(repo) = repo else {
        return refuse(
            ApiOutcome::NotFound,
            StatusCode::NOT_FOUND,
            "repository not connected",
            format!(
                "{} is not served here; connect it with `dagron-ci repo add`",
                request.repo
            ),
        );
    };
    if !repo.mode.runs_pipelines() {
        return refuse(
            ApiOutcome::Conflict,
            StatusCode::CONFLICT,
            "repository in fleet mode",
            format!(
                "{}: {} (`dagron-ci repo set … --mode native` runs its pipelines here)",
                request.repo,
                crate::db::FLEET_MODE
            ),
        );
    }
    let key = match idempotency_key(&headers) {
        Ok(key) => key,
        Err(why) => {
            return refuse(
                ApiOutcome::BadRequest,
                StatusCode::BAD_REQUEST,
                "invalid Idempotency-Key",
                why,
            )
        }
    };
    let fingerprint = fingerprint(&repo.forge_host, &repo.full_name, &request);

    // A repeat is answered before the forge is asked anything, and
    // before its bundle is looked for: one past its time answers the same.
    if let Some(key) = &key {
        match st.db.find_api_request(&caller.sub, key).await {
            Ok(Some((event_id, earlier))) => {
                return repeat(&st, &event_id, earlier == fingerprint, key).await
            }
            Ok(None) => {}
            Err(e) => return fail(database_error(e)),
        }
    }

    // A bundle is its uploader's, of its repository, and its base is the
    // commit the run is of. Another's, or one past its time, is none here.
    let bundle = match &request.bundle {
        None => None,
        Some(id) => {
            let found = match st.db.bundle(id).await {
                Ok(found) => found,
                Err(e) => return fail(database_error(e)),
            };
            let Some(bundle) = found.filter(|b| b.principal == caller.sub) else {
                return refuse(
                    ApiOutcome::NotFound,
                    StatusCode::NOT_FOUND,
                    "no such bundle",
                    format!("{id} is no bundle of yours that is still kept: upload one with POST /ci/v1/bundles"),
                );
            };
            if bundle.repo_id != repo.id {
                return refuse(
                    ApiOutcome::BadRequest,
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "the bundle is of another repository",
                    format!(
                        "{id} was uploaded for another repository than {}",
                        request.repo
                    ),
                );
            }
            if request
                .sha
                .as_deref()
                .is_some_and(|sha| sha != bundle.base_sha)
            {
                return refuse(
                    ApiOutcome::BadRequest,
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "the commit is not the bundle's base",
                    format!(
                        "{id} is laid over {}; name that commit, or none",
                        bundle.base_sha
                    ),
                );
            }
            if !bundle.ready {
                if let Err(why) = crate::bundles::made_ready(&st, &bundle).await {
                    return refuse(
                        ApiOutcome::BadRequest,
                        StatusCode::UNPROCESSABLE_ENTITY,
                        "invalid bundle",
                        why,
                    );
                }
            }
            Some(bundle)
        }
    };

    let sha = match bundle
        .as_ref()
        .map(|b| b.base_sha.clone())
        .or_else(|| request.sha.clone())
    {
        // A commit the caller names is one that is on the ref they name: a
        // run for a ref is of a commit of that ref, and never of whatever
        // commit a caller could reach.
        Some(sha) => match st.forge.on_ref(&repo, &request.git_ref, &sha).await {
            Ok(crate::forge::OnRef::Yes) => sha,
            Ok(crate::forge::OnRef::NoRef) => {
                return refuse(
                    ApiOutcome::BadRequest,
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "no such ref",
                    format!(
                        "{}/{} has no {}",
                        repo.forge_host, repo.full_name, request.git_ref
                    ),
                )
            }
            Ok(crate::forge::OnRef::No(why)) => {
                return refuse(
                    ApiOutcome::BadRequest,
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "the commit is not on the ref",
                    why,
                )
            }
            Err(why) => {
                return refuse(
                    ApiOutcome::Unavailable,
                    StatusCode::BAD_GATEWAY,
                    "the forge could not be read",
                    format!("asking whether the commit is on {}: {why}", request.git_ref),
                )
            }
        },
        None => match st.forge.resolve(&repo, &request.git_ref).await {
            Ok(Some(sha)) => sha,
            Ok(None) => {
                return refuse(
                    ApiOutcome::BadRequest,
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "no such ref",
                    format!(
                        "{}/{} has no {}",
                        repo.forge_host, repo.full_name, request.git_ref
                    ),
                )
            }
            Err(why) => {
                return refuse(
                    ApiOutcome::Unavailable,
                    StatusCode::BAD_GATEWAY,
                    "the forge could not be read",
                    format!("resolving {}: {why}", request.git_ref),
                )
            }
        },
    };
    let event = NewEvent {
        kind: Kind::Dispatch,
        git_ref: request.git_ref.clone(),
        sha,
        commit: None,
        base_sha: None,
        base_ref: None,
        pr: None,
        actor: Some(caller.actor()),
        // A change nobody reviewed is untrusted whoever asks; an agent's is
        // an agent's.
        trust: match (bundle.is_some(), caller.trust()) {
            (true, Trust::Agent) => Trust::Agent,
            (true, _) => Trust::Untrusted,
            (false, trust) => trust,
        },
        association: None,
        head_ref: None,
    };
    let request_key = key.as_deref().map(|key| RequestKey {
        principal: &caller.sub,
        key,
        fingerprint: &fingerprint,
    });
    match st
        .db
        .record_dispatch(
            &repo,
            &event,
            &request.inputs,
            request.pipeline.as_deref(),
            bundle.as_ref().map(|b| b.id.as_str()),
            request_key.as_ref(),
        )
        .await
    {
        Ok(Dispatched::Event(id)) => {
            st.metrics.api_request(ApiOutcome::Accepted);
            st.wake.notify_one();
            tracing::info!(
                event_id = %id,
                repo = %repo.full_name,
                git_ref = %event.git_ref,
                actor = %caller.actor(),
                trust = event.trust.as_str(),
                "dispatch queued"
            );
            view(&st, &id, StatusCode::ACCEPTED).await
        }
        // Another request with this key won the race to record it.
        Ok(Dispatched::Repeat { event_id, same }) => {
            repeat(&st, &event_id, same, key.as_deref().unwrap_or_default()).await
        }
        Err(e) => fail(database_error(e)),
    }
}

/// What a path's `{id}` names.
pub(crate) struct Named {
    /// The event, with each of its runs; or, when the id is an engine run's,
    /// with that run alone.
    pub(crate) event: EventView,
    /// Whether the id was an engine run's: the request is about that run.
    one_run: bool,
}

/// The event a path's `{id}` names, if it names one. An event's own id means
/// the event and each of its runs. An engine run's id means that run of its
/// event and no other: it is the id a commit status links to (CI_RUN_URL),
/// and so the one a person looking at a check has to hand.
pub(crate) async fn named(st: &AppState, id: &str) -> sqlx::Result<Option<Named>> {
    if let Some(event) = st.db.event_view(id).await? {
        return Ok(Some(Named {
            event,
            one_run: false,
        }));
    }
    let Some(event_id) = st.db.event_of_run(id).await? else {
        return Ok(None);
    };
    Ok(st.db.event_view(&event_id).await?.map(|mut event| {
        event.runs.retain(|run| run.id == id);
        Named {
            event,
            one_run: true,
        }
    }))
}

/// `GET /ci/v1/runs/{id}`: an event, its state, and the engine runs it
/// submitted. Any identified caller may read, viewers included.
pub async fn get_run(
    State(st): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let Some(credential) = bearer(&headers) else {
        return problem(
            StatusCode::UNAUTHORIZED,
            "not authenticated",
            "send a dagron-api session or API token as Authorization: Bearer",
        );
    };
    let found = match named(&st, &id).await {
        Ok(found) => found.map(|named| named.event),
        Err(e) => return database_error(e).1,
    };
    if let Err((_, response)) =
        identify(&st, found.as_ref().map(|v| v.cell.as_str()), credential).await
    {
        return response;
    }
    match found {
        Some(found) => (StatusCode::OK, Json(view_json(&found))).into_response(),
        None => problem(
            StatusCode::NOT_FOUND,
            "no such run",
            format!("no event {id:?}"),
        ),
    }
}

/// `GET /ci/v1/runs/{id}/statuses`: what dagron-ci posted on the commit of
/// the event `id`, oldest first, and what came of each: from the
/// database while the event's day is there (`"from": "database"`), and from
/// its day's segments in CI_OBJECT_STORE_URL after (`"from": "segments"`).
/// The caller is anyone dagron-api knows, as for the event itself.
pub async fn run_statuses(
    State(st): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let Some(credential) = bearer(&headers) else {
        return problem(
            StatusCode::UNAUTHORIZED,
            "not authenticated",
            "send a dagron-api session or API token as Authorization: Bearer",
        );
    };
    let found = match named(&st, &id).await {
        Ok(found) => found,
        Err(e) => return database_error(e).1,
    };
    // An id whose rows have gone is found in its day's index, with its
    // repository's cell, whose dagron-api says who may read it: never by
    // another cell's.
    let archived = match &found {
        Some(_) => None,
        None => match st.db.archived(&id).await {
            Ok(archived) => archived,
            Err(e) => return database_error(e).1,
        },
    };
    let cell = found
        .as_ref()
        .map(|n| n.event.cell.as_str())
        .or(archived.as_ref().map(|(_, _, cell)| cell.as_str()));
    if let Err((_, response)) = identify(&st, cell, credential).await {
        return response;
    }
    let parse = |rows: Vec<String>| -> Vec<serde_json::Value> {
        rows.iter()
            .filter_map(|r| serde_json::from_str(r).ok())
            .collect()
    };
    // An engine run's id names its event, and that run's posts alone.
    if let Some(found) = &found {
        let event_id = &found.event.id;
        return match st.db.report_log_of(event_id).await {
            Ok(rows) => {
                let mut statuses = parse(rows);
                if found.one_run {
                    statuses
                        .retain(|s| s.get("run_id").and_then(|r| r.as_str()) == Some(id.as_str()));
                }
                (
                    StatusCode::OK,
                    Json(json!({ "event_id": event_id, "from": "database", "statuses": statuses })),
                )
                    .into_response()
            }
            Err(e) => database_error(e).1,
        };
    }
    let (Some((event_id, day, _)), Some(store)) = (archived, &st.payloads) else {
        return problem(
            StatusCode::NOT_FOUND,
            "no such run",
            format!("no event or run {id:?}, in the database or in a day written out"),
        );
    };
    match crate::records::report_log_from_segments(&st.db, store, &event_id, day).await {
        Ok(mut statuses) => {
            // An engine run's id: that run's posts alone.
            if event_id != id {
                statuses.retain(|s| s.get("run_id").and_then(|r| r.as_str()) == Some(id.as_str()));
            }
            (
                StatusCode::OK,
                Json(json!({ "event_id": event_id, "from": "segments", "statuses": statuses })),
            )
                .into_response()
        }
        Err(e) => {
            tracing::warn!(event = %event_id, error = %format!("{e:#}"), "reading a run's statuses from the segments of its day");
            problem(
                StatusCode::SERVICE_UNAVAILABLE,
                "not read",
                "the segments of the event's day could not be read; try again",
            )
        }
    }
}

/// `GET /ci/v1/me`: who the `default` cell's dagron-api (else the first
/// cell's) says presents the credential, as the repositories' door asks it,
/// and what a run they ask for is recorded as: `{sub, email, groups,
/// trust}`. 401 for a credential it refuses, 503 when it cannot be asked.
pub async fn me(State(st): State<AppState>, headers: HeaderMap) -> Response {
    let Some(credential) = bearer(&headers) else {
        return problem(
            StatusCode::UNAUTHORIZED,
            "not authenticated",
            "send a dagron-api session or API token as Authorization: Bearer",
        );
    };
    match identify(&st, None, credential).await {
        Ok(caller) => (
            StatusCode::OK,
            Json(json!({
                "sub": caller.sub,
                "email": caller.email,
                "groups": caller.groups,
                "trust": caller.trust().as_str(),
            })),
        )
            .into_response(),
        Err((_, response)) => response,
    }
}

/// Who presents the request's credential, for the repository `spec`
/// (`HOST/OWNER/NAME`) that must be connected: the caller's subject, and the
/// repository. A viewer may run nothing. The error is the response to send.
pub(crate) async fn caller_of(
    st: &AppState,
    headers: &HeaderMap,
    spec: &str,
) -> Result<(String, crate::db::Repo), Response> {
    let Some(credential) = bearer(headers) else {
        return Err(problem(
            StatusCode::UNAUTHORIZED,
            "not authenticated",
            "send a dagron-api session or API token as Authorization: Bearer".to_string(),
        ));
    };
    let repo = match crate::cli::split_spec(spec) {
        Ok((host, name)) => match st.db.find_repo(host, name).await {
            Ok(repo) => repo,
            Err(e) => return Err(database_error(e).1),
        },
        Err(_) => None,
    };
    let caller = identify(st, repo.as_ref().map(|r| r.cell.as_str()), credential)
        .await
        .map_err(|(_, response)| response)?;
    if caller.viewer() {
        return Err(problem(
            StatusCode::FORBIDDEN,
            "read-only caller",
            "a viewer may read runs, and run nothing".to_string(),
        ));
    }
    match repo {
        Some(repo) => Ok((caller.sub, repo)),
        None => Err(problem(
            StatusCode::NOT_FOUND,
            "repository not connected",
            format!("{spec} is not served here; connect it with `dagron-ci repo add`"),
        )),
    }
}

/// Ask the dagron-api of `cell` (or, for a repository not found, of the
/// `default` cell, else the first) who presents `credential`. The error is
/// how the request ends, and the response to send.
async fn identify(
    st: &AppState,
    cell: Option<&str>,
    credential: &str,
) -> Result<Caller, (ApiOutcome, Response)> {
    let base = cell
        .and_then(|c| st.cells.get(c))
        .or_else(|| st.cells.get("default"))
        .or_else(|| st.cells.values().next());
    let Some(base) = base else {
        return Err((
            ApiOutcome::NotConfigured,
            problem(
                StatusCode::NOT_FOUND,
                "run API not configured",
                "this dagron-ci names no cell (CI_CELLS) whose dagron-api could identify the caller",
            ),
        ));
    };
    match whoami(st.http.to(base), base, credential).await {
        Ok(caller) => Ok(caller),
        Err(AuthError::Refused) => Err((
            ApiOutcome::Unauthorized,
            problem(
                StatusCode::UNAUTHORIZED,
                "not authenticated",
                "dagron-api did not accept the credential",
            ),
        )),
        Err(AuthError::Unavailable(why)) => {
            tracing::warn!(dagron_api = %base, reason = %why, "could not identify a run API caller");
            Err((
                ApiOutcome::Unavailable,
                problem(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "caller not identified",
                    "dagron-api could not be asked who the caller is; try again",
                ),
            ))
        }
    }
}

async fn whoami(http: &reqwest::Client, base: &str, credential: &str) -> Result<Caller, AuthError> {
    let response = http
        .get(format!("{base}/api/me"))
        .bearer_auth(credential)
        .send()
        .await
        .map_err(|e| AuthError::Unavailable(e.to_string()))?;
    match response.status().as_u16() {
        200 => response
            .json::<Caller>()
            .await
            .map_err(|e| AuthError::Unavailable(format!("/api/me answered something else: {e}"))),
        401 | 403 => Err(AuthError::Refused),
        status => Err(AuthError::Unavailable(format!("/api/me answered {status}"))),
    }
}

/// `Authorization: Bearer <credential>`, when it is one dagron-api could take.
pub(crate) fn bearer(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?.trim();
    let (scheme, credential) = value.split_once(' ')?;
    let credential = credential.trim();
    (scheme.eq_ignore_ascii_case("bearer")
        && !credential.is_empty()
        && credential.len() <= MAX_CREDENTIAL
        && credential.bytes().all(|b| b.is_ascii_graphic()))
    .then_some(credential)
}

/// What a run request must hold, besides a repository this dagron-ci serves.
fn validate(r: &RunRequest) -> Result<(), String> {
    if let Some(pipeline) = &r.pipeline {
        let file = !pipeline.is_empty()
            && pipeline.len() <= 128
            && !pipeline.starts_with('.')
            && pipeline
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
        if !file {
            return Err(format!(
                "pipeline must name a file in .dagron/ci, such as deploy or deploy.yaml, \
                 not {pipeline:?}"
            ));
        }
    }
    let short = ["refs/heads/", "refs/tags/"]
        .iter()
        .find_map(|p| r.git_ref.strip_prefix(p));
    if short.is_none_or(str::is_empty) {
        return Err(format!(
            "ref must be a full branch or tag ref, such as refs/heads/main, not {:?}",
            r.git_ref
        ));
    }
    if !is_storable_ref(&r.git_ref) || r.git_ref.contains(char::is_whitespace) {
        return Err("ref holds a space or a control character".into());
    }
    if let Some(sha) = &r.sha {
        if !is_commit_id(sha) {
            return Err(format!(
                "sha must be a full commit id, 40 or 64 lowercase hex digits, not {sha:?}"
            ));
        }
    }
    if r.inputs.len() > MAX_INPUTS {
        return Err(format!("at most {MAX_INPUTS} inputs"));
    }
    for (name, value) in &r.inputs {
        let mut chars = name.chars();
        let identifier = chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
            && name.len() <= 64;
        if !identifier {
            return Err(format!(
                "input {name:?} must be a parameter name: a letter or _, then letters, digits or _, at most 64"
            ));
        }
        if name.starts_with("ci_") {
            return Err(format!(
                "input {name:?}: names starting ci_ are dagron-ci's, set from the event"
            ));
        }
        if value.len() > MAX_INPUT_BYTES {
            return Err(format!("input {name:?} is over {MAX_INPUT_BYTES} bytes"));
        }
    }
    Ok(())
}

/// An `Idempotency-Key` as dagron-api takes one: ASCII, trimmed, 1 to 255
/// printable characters without spaces.
fn idempotency_key(headers: &HeaderMap) -> Result<Option<String>, String> {
    let Some(raw) = headers.get("idempotency-key") else {
        return Ok(None);
    };
    let key = raw
        .to_str()
        .map_err(|_| "Idempotency-Key must be ASCII".to_string())?
        .trim();
    if key.is_empty() || key.len() > MAX_IDEMPOTENCY_KEY {
        return Err(format!(
            "Idempotency-Key must be 1 to {MAX_IDEMPOTENCY_KEY} characters"
        ));
    }
    if !key.bytes().all(|b| b.is_ascii_graphic()) {
        return Err("Idempotency-Key must be printable ASCII without spaces".into());
    }
    Ok(Some(key.to_string()))
}

/// The request as it was sent, hashed: a repeat must match it. The commit a
/// ref resolved to is not part of it, so a repeat after the branch moved
/// still finds the first run, at the first commit.
fn fingerprint(host: &str, name: &str, r: &RunRequest) -> String {
    let mut canonical = json!({
        "repo": format!("{host}/{name}"),
        "ref": r.git_ref,
        "sha": r.sha,
        "inputs": r.inputs,
    });
    // Only when named, so a request that names none hashes as it always has.
    if let Some(pipeline) = &r.pipeline {
        canonical["pipeline"] = json!(pipeline);
    }
    if let Some(bundle) = &r.bundle {
        canonical["bundle"] = json!(bundle);
    }
    hex::encode(Sha256::digest(canonical.to_string().as_bytes()))
}

/// The answer to a repeat of an earlier request's key.
async fn repeat(st: &AppState, event_id: &str, same: bool, key: &str) -> Response {
    if same {
        st.metrics.api_request(ApiOutcome::Replayed);
        return view(st, event_id, StatusCode::OK).await;
    }
    st.metrics.api_request(ApiOutcome::Conflict);
    problem(
        StatusCode::CONFLICT,
        "Idempotency-Key reused",
        format!("Idempotency-Key {key:?} was sent before with another request (event {event_id})"),
    )
}

/// The event `id` as `status`, or a 500 when it cannot be read back.
async fn view(st: &AppState, id: &str, status: StatusCode) -> Response {
    match st.db.event_view(id).await {
        Ok(Some(found)) => (status, Json(view_json(&found))).into_response(),
        Ok(None) => problem(
            StatusCode::INTERNAL_SERVER_ERROR,
            "event gone",
            format!("event {id} was queued, and is gone: its repository was disconnected"),
        ),
        Err(e) => database_error(e).1,
    }
}

fn view_json(v: &EventView) -> serde_json::Value {
    json!({
        "id": v.id,
        "repo": v.repo,
        "kind": v.kind,
        "ref": v.git_ref,
        "sha": v.sha,
        "source": v.source,
        "trust": v.trust,
        "actor": v.actor,
        "state": v.state,
        "reason": v.reason,
        "attempts": v.attempts,
        "pipeline": v.pipeline,
        // The patch a bundle's run ran, by the SHA-256 of what was uploaded:
        // what an approval that binds the patch is checked against.
        "bundle": v.bundle.as_ref().map(|id| json!({ "id": id, "sha256": v.bundle_sha256 })),
        "engine_runs": v.runs.iter().map(|run| json!({
            "id": run.id,
            "cell": run.cell,
            "pipeline": run.pipeline,
            // As dagron-ci last saw it: `submitted` while it is followed,
            // then how it ended, and `submitted` again if it runs again.
            "status": run.status,
        })).collect::<Vec<_>>(),
    })
}

/// What became of a rerun asked for an event's runs.
pub(crate) struct Rerun {
    /// The runs that are running again, as the route's answer lists them.
    pub(crate) again: Vec<serde_json::Value>,
    /// The runs left as they were, each with why.
    pub(crate) left: Vec<serde_json::Value>,
    /// Why dagron-api could not be asked, where it could not.
    pub(crate) unavailable: Option<String>,
    /// The status dagron-api refused the credential with, and the run it
    /// refused it for, where none had been rerun before it.
    pub(crate) denied: Option<(u16, String)>,
}

/// Run `found`'s failed and cancelled runs again, each from its failed
/// jobs, as `credential`, and re-open the record of each that is. With
/// [`RerunFrom::All`], each of its runs that is over, one that succeeded too,
/// is run again whole. The rerun route asks with its caller's credential, so
/// that the rule on who may is dagron-api's; a check's "Re-run" (`server.rs`)
/// asks with dagron-ci's own, from the failed jobs.
pub(crate) async fn rerun_runs(
    st: &AppState,
    reruns: &crate::rerun::Reruns,
    found: &EventView,
    credential: &str,
    from: RerunFrom,
) -> sqlx::Result<Rerun> {
    let mut again = Vec::new();
    let mut left = Vec::new();
    let mut unavailable = None;
    let mut denied = None;
    for run in &found.runs {
        let mut leave = |why: String| {
            left.push(json!({ "id": run.id, "pipeline": run.pipeline, "why": why }));
        };
        match run.status.as_str() {
            "failed" | "cancelled" => {}
            "succeeded" if from == RerunFrom::All => {}
            "submitted" => {
                leave("it is running".into());
                continue;
            }
            "succeeded" => {
                leave("it succeeded: none of its jobs failed".into());
                continue;
            }
            other => {
                leave(format!("it is {other}"));
                continue;
            }
        }
        // Like a new run: under the build's limit, in a slot of its own key.
        if let Some(admission) = &st.admission {
            let caller = found.actor.as_deref();
            match st
                .db
                .admit_for(
                    &run.key,
                    Some(&found.repo),
                    caller,
                    crate::admission::HOLD,
                    admission.by_repository(),
                    admission.by_caller(),
                    |load| admission.admit_caller(&found.repo, caller, &found.trust, load),
                )
                .await
            {
                Ok(Ok(())) => {}
                Ok(Err(why)) => {
                    leave(format!("waiting: {why}"));
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        let slot = st.admission.is_some().then_some(run.key.as_str());
        // The slot goes when dagron-api surely did not run it again. After
        // an answer that leaves that open it is kept until it lapses.
        let release = || async {
            if let Some(key) = slot {
                if let Err(e) = st.db.release_admission(key).await {
                    tracing::warn!(error = %e, "an admission slot was not let go of; it lapses on its own");
                }
            }
        };
        match reruns.rerun(&run.cell, &run.id, credential, from).await {
            Ok(Asked::Rerun(jobs)) => {
                if let Err(why) = reruns
                    .reopen(
                        &st.db,
                        &run.id,
                        std::time::Duration::ZERO,
                        slot,
                        Reopened::Door,
                    )
                    .await
                {
                    // Running again, and not followed yet: the sweep, or its
                    // first checkout, re-opens the record.
                    tracing::warn!(run_id = %run.id, %why, "a rerun's record was not re-opened");
                }
                again.push(json!({
                    "id": run.id,
                    "cell": run.cell,
                    "pipeline": run.pipeline,
                    "jobs": jobs,
                }));
            }
            Ok(Asked::Refused(said)) => {
                release().await;
                // Not one dagron-api reruns now. It may be running already,
                // rerun through dagron-api itself: then it is followed.
                match reruns
                    .follow(&st.db, &run.cell, &run.id, Reopened::Door)
                    .await
                {
                    Ok(true) => leave("it is running again already, and is followed".into()),
                    Ok(false) => leave(format!("dagron-api did not rerun it: {said}")),
                    Err(why) => {
                        leave("dagron-api could not be asked about it".into());
                        unavailable = Some(why);
                    }
                }
            }
            Ok(Asked::Waiting(said)) => {
                // Its workflow's own limit on runs in flight, held by another
                // run of its concurrency key: not rerun, and not an error.
                release().await;
                leave(format!("waiting: {said}"));
            }
            Ok(Asked::Unsupported(said)) => {
                // A cell whose dagron-api is older than the rerun of every
                // job: not rerun, and nothing to try again.
                release().await;
                leave(format!(
                    "its cell's dagron-api runs no rerun of every job: {said}"
                ));
            }
            Ok(Asked::Gone) => {
                release().await;
                leave("its cell's dagron-api has no such run".into());
            }
            Ok(Asked::Denied(code)) => {
                release().await;
                // With a run of the event running again already, what was
                // done is what is said, and this one is left with why.
                if !again.is_empty() {
                    leave("dagron-api did not let this caller rerun it".into());
                    continue;
                }
                denied = Some((code, run.id.clone()));
                break;
            }
            Err(why) => {
                leave("dagron-api could not be asked for it".into());
                unavailable = Some(why);
            }
        }
    }
    Ok(Rerun {
        again,
        left,
        unavailable,
        denied,
    })
}

/// `POST /ci/v1/runs/{id}/rerun`'s query.
#[derive(Debug, Deserialize)]
pub struct RerunQuery {
    /// `true`, the default: a run is run again from its failed jobs, and
    /// what succeeded is kept. `false`: every job of it, a run that
    /// succeeded too. Taken as text, so that anything else is refused as
    /// every refusal here is, with a problem document, once the caller is
    /// identified.
    failed: Option<String>,
}

/// `POST /ci/v1/runs/{id}/rerun`: run an event's failed or cancelled runs
/// again, each from its failed jobs; with `?failed=false`, each of its runs
/// that is over, whole. 200 with the runs that are running again (`rerun`)
/// and the ones left as they are, each with why (`left`).
///
/// A run is rerun by its cell's dagron-api, asked with the caller's own
/// credential, and dagron-ci's record of it re-opens with that answer: the
/// run is followed again, its jobs' checks read `pending` and then how they
/// end, and a job's checkout is given its credential. It passes the build's
/// limit on runs in flight like a new run. Where a build counts runs, a
/// rerun of the failed jobs is not counted again, and a rerun of every job
/// is, as the run run again.
///
/// 409 when none of the event's runs can be rerun (each says why, a cell
/// whose dagron-api runs no rerun of every job among them), 503 when
/// dagron-api could not be asked, the caller's own 401 or 403 when
/// dagron-api does not let them, and 400 for a `failed` that is neither
/// `true` nor `false`.
pub async fn rerun_run(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<RerunQuery>,
    headers: HeaderMap,
) -> Response {
    let refuse = |outcome: ApiOutcome, status: StatusCode, title: &str, detail: String| {
        st.metrics.api_request(outcome);
        problem(status, title, detail)
    };
    let fail = |(outcome, response): (ApiOutcome, Response)| {
        st.metrics.api_request(outcome);
        response
    };
    let Some(credential) = bearer(&headers) else {
        return refuse(
            ApiOutcome::Unauthorized,
            StatusCode::UNAUTHORIZED,
            "not authenticated",
            "send a dagron-api session or API token as Authorization: Bearer".into(),
        );
    };
    let found = match named(&st, &id).await {
        Ok(found) => found.map(|named| named.event),
        Err(e) => return fail(database_error(e)),
    };
    // Nothing of the event is told to a caller who has not been identified.
    let caller = match identify(&st, found.as_ref().map(|v| v.cell.as_str()), credential).await {
        Ok(caller) => caller,
        Err(failure) => return fail(failure),
    };
    if caller.viewer() {
        return refuse(
            ApiOutcome::Forbidden,
            StatusCode::FORBIDDEN,
            "read-only caller",
            "a viewer may read runs, and run nothing".into(),
        );
    }
    if caller.trust() == Trust::Agent && !st.agent_runs {
        return refuse(
            ApiOutcome::Forbidden,
            StatusCode::FORBIDDEN,
            "agent caller",
            "this dagron-ci runs no agent's work (CI_AGENT_RUNS=refuse), and so reruns none".into(),
        );
    }
    let from = match query.failed.as_deref() {
        None | Some("true") => RerunFrom::Failed,
        Some("false") => RerunFrom::All,
        Some(other) => {
            return refuse(
                ApiOutcome::BadRequest,
                StatusCode::BAD_REQUEST,
                "invalid rerun",
                format!("failed is true or false, not {other:?}"),
            )
        }
    };
    let Some(found) = found else {
        return refuse(
            ApiOutcome::NotFound,
            StatusCode::NOT_FOUND,
            "no such run",
            format!("no event {id:?}"),
        );
    };
    if !caller.owns(&found) {
        return refuse(
            ApiOutcome::Forbidden,
            StatusCode::FORBIDDEN,
            "not the agent's",
            format!(
                "an agent reruns the events it asked for, and event {} is not one",
                found.id
            ),
        );
    }
    // A fleet-mode repository's runs are its forge's, as a new one's are.
    if let Some((host, name)) = found.repo.split_once('/') {
        match st.db.find_repo(host, name).await {
            Ok(Some(repo)) if !repo.mode.runs_pipelines() => {
                return refuse(
                    ApiOutcome::Conflict,
                    StatusCode::CONFLICT,
                    "repository in fleet mode",
                    format!(
                        "{}: {} (`dagron-ci repo set … --mode native` runs its pipelines here)",
                        found.repo,
                        crate::db::FLEET_MODE
                    ),
                );
            }
            Ok(_) => {}
            Err(e) => return fail(database_error(e)),
        }
    }
    let Some(reruns) = &st.reruns else {
        return refuse(
            ApiOutcome::NotConfigured,
            StatusCode::NOT_FOUND,
            "reruns not configured",
            "this dagron-ci has no token for its cells' dagron-api (CI_API_TOKEN_FILE): it \
             could not follow a run it had rerun"
                .into(),
        );
    };

    let Rerun {
        again,
        left,
        unavailable,
        denied,
    } = match rerun_runs(&st, reruns, &found, credential, from).await {
        Ok(rerun) => rerun,
        Err(e) => return fail(database_error(e)),
    };
    if let Some((code, run_id)) = denied {
        let (outcome, status, title) = match code {
            401 => (
                ApiOutcome::Unauthorized,
                StatusCode::UNAUTHORIZED,
                "not authenticated",
            ),
            _ => (ApiOutcome::Forbidden, StatusCode::FORBIDDEN, "not allowed"),
        };
        return refuse(
            outcome,
            status,
            title,
            format!("dagron-api did not let this caller rerun run {run_id}"),
        );
    }

    if !again.is_empty() {
        st.metrics.api_request(ApiOutcome::Accepted);
        tracing::info!(
            event_id = %found.id, actor = %caller.actor(), runs = again.len(),
            every_job = from == RerunFrom::All,
            "runs rerun through dagron-ci"
        );
        crate::audit::record(st.audit.as_ref(), || {
            let mut entry = crate::audit::Entry::new(crate::audit::kind::RUN_RERUN)
                .actor(Some(&caller.actor()))
                .with("event_id", found.id.as_str())
                .with("rerun", serde_json::to_value(&again).unwrap_or_default())
                .with("left", serde_json::to_value(&left).unwrap_or_default());
            entry.repo = Some(found.repo.clone());
            entry
        });
        return (
            StatusCode::OK,
            Json(json!({ "id": found.id, "rerun": again, "left": left })),
        )
            .into_response();
    }
    let why = left
        .iter()
        .map(|run| {
            format!(
                "run {} ({}): {}",
                run["id"].as_str().unwrap_or_default(),
                run["pipeline"].as_str().unwrap_or_default(),
                run["why"].as_str().unwrap_or_default()
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    match unavailable {
        Some(reason) => {
            tracing::warn!(event_id = %found.id, %reason, "a rerun could not be asked for");
            refuse(
                ApiOutcome::Unavailable,
                StatusCode::SERVICE_UNAVAILABLE,
                "dagron-api could not be asked",
                format!("nothing was rerun; try again. {why}"),
            )
        }
        None => refuse(
            ApiOutcome::Conflict,
            StatusCode::CONFLICT,
            "nothing to rerun",
            if why.is_empty() {
                format!("event {} submitted no run", found.id)
            } else {
                why
            },
        ),
    }
}

fn database_error(e: sqlx::Error) -> (ApiOutcome, Response) {
    tracing::error!(error = %e, "the run API could not reach its database");
    (
        ApiOutcome::Error,
        problem(
            StatusCode::INTERNAL_SERVER_ERROR,
            "database unavailable",
            "the event queue is unavailable; the request can be retried",
        ),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::{Method, Request};
    use axum::routing::{get, post};
    use axum::Router;
    use tokio::sync::{watch, Notify};
    use tower::ServiceExt;

    use super::*;
    use crate::db::tests::{drop_schema, postgres, sqlite};
    use crate::db::Db;
    use crate::dispatch::Dispatcher;
    use crate::metrics::Metrics;
    use crate::poll::tests::{refs, stub_forge};
    use crate::{Config, Seams};

    const MAIN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const PINNED: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    /// The head of a pull request from a fork: it has main behind it, and
    /// main does not have it.
    const FORKS: &str = "cccccccccccccccccccccccccccccccccccccccc";

    /// What the stub dagron-api saw and made.
    #[derive(Default)]
    struct ApiState {
        /// `Authorization` of each `/api/me` call.
        identified: Vec<String>,
        /// `(workflow, Idempotency-Key, parameters)` of each run request.
        runs: Vec<(String, String, BTreeMap<String, String>)>,
        by_key: BTreeMap<String, String>,
    }

    type StubApi = Arc<Mutex<ApiState>>;

    /// A dagron-api that knows four callers and runs any workflow, replaying a
    /// key it has seen, as dagron-api does.
    async fn stub_api() -> (String, StubApi) {
        async fn me(State(api): State<StubApi>, headers: HeaderMap) -> Response {
            let auth = headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            api.lock().unwrap().identified.push(auth.clone());
            let (sub, email, groups) = match auth.as_str() {
                "Bearer dgr_operator" => ("u-1", "dev@example.com", vec![]),
                "Bearer dgr_other" => ("u-2", "", vec![]),
                "Bearer dgr_viewer" => ("u-3", "viewer@example.com", vec!["viewer"]),
                "Bearer dgr_agent" => ("agent:fixer", "", vec![]),
                "Bearer dgr_admin" => ("u-7", "admin@example.com", vec!["admin"]),
                "Bearer dgr_admin_two" => ("u-8", "", vec!["admin"]),
                "Bearer dgr_agent_admin" => ("agent:root", "", vec!["admin"]),
                "Bearer dgr_viewer_admin" => ("u-9", "", vec!["viewer", "admin"]),
                _ => return StatusCode::UNAUTHORIZED.into_response(),
            };
            Json(json!({ "sub": sub, "email": email, "name": sub, "groups": groups, "exp": 0 }))
                .into_response()
        }
        async fn run(
            State(api): State<StubApi>,
            Path(workflow): Path<String>,
            headers: HeaderMap,
            Json(body): Json<serde_json::Value>,
        ) -> (StatusCode, String) {
            let key = headers
                .get("idempotency-key")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            let params = serde_json::from_value(body["parameters"].clone()).unwrap();
            let mut api = api.lock().unwrap();
            api.runs.push((workflow, key.clone(), params));
            if let Some(run) = api.by_key.get(&key) {
                return (StatusCode::OK, format!(r#"{{"run_id":"{run}"}}"#));
            }
            let run = format!("run-{}", api.by_key.len() + 1);
            api.by_key.insert(key, run.clone());
            (StatusCode::CREATED, format!(r#"{{"run_id":"{run}"}}"#))
        }
        let api: StubApi = Arc::default();
        let app = Router::new()
            .route("/api/me", get(me))
            .route("/api/workflows/{workflow}/run", post(run))
            .with_state(api.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (base, api)
    }

    struct Rig {
        app: Router,
        api: StubApi,
        db: Db,
        cells: BTreeMap<String, String>,
        /// The stub forge's origin.
        origin: String,
        _dir: tempfile::TempDir,
    }

    async fn rig(db: Db) -> Rig {
        let (api_base, api) = stub_api().await;
        let (origin, forge) = stub_forge().await;
        forge.lock().unwrap().refs = refs(&[
            ("refs/heads/main", MAIN),
            ("refs/heads/main-old", PINNED),
            ("refs/pull/8/head", FORKS),
        ]);
        // Main has the pinned commit behind it, and the fork's head has main.
        forge.lock().unwrap().behind = BTreeMap::from([
            (MAIN.to_string(), vec![PINNED.to_string()]),
            (
                FORKS.to_string(),
                vec![MAIN.to_string(), PINNED.to_string()],
            ),
        ]);
        let dir = tempfile::tempdir().unwrap();
        let token = dir.path().join("token");
        std::fs::write(&token, "dgr_ci").unwrap();
        let config = Config::from_lookup(|name| match name {
            "CI_DATABASE_URL" => Some("sqlite://unused.db".into()),
            "CI_CELLS" => Some(format!("default={api_base}")),
            "CI_API_TOKEN_FILE" => Some(token.to_str().unwrap().into()),
            _ => None,
        })
        .unwrap();
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
        let state = AppState::new(db.clone(), &config, Seams::default()).with_forge_origin(&origin);
        Rig {
            app: crate::router(state),
            api,
            db,
            cells: config.cells.clone(),
            origin,
            _dir: dir,
        }
    }

    impl Rig {
        async fn call(
            &self,
            method: Method,
            path: &str,
            token: Option<&str>,
            key: Option<&str>,
            body: Option<serde_json::Value>,
        ) -> (StatusCode, serde_json::Value) {
            let mut req = Request::builder().method(method).uri(path);
            if let Some(token) = token {
                req = req.header("authorization", format!("Bearer {token}"));
            }
            if let Some(key) = key {
                req = req.header("idempotency-key", key);
            }
            let req = match body {
                Some(body) => req
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string())),
                None => req.body(Body::empty()),
            }
            .unwrap();
            let res = self.app.clone().oneshot(req).await.unwrap();
            let status = res.status();
            let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
                .await
                .unwrap();
            (
                status,
                serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
            )
        }

        async fn post(
            &self,
            token: Option<&str>,
            key: Option<&str>,
            body: serde_json::Value,
        ) -> (StatusCode, serde_json::Value) {
            self.call(Method::POST, "/ci/v1/runs", token, key, Some(body))
                .await
        }

        async fn get(&self, token: &str, id: &str) -> (StatusCode, serde_json::Value) {
            self.call(
                Method::GET,
                &format!("/ci/v1/runs/{id}"),
                Some(token),
                None,
                None,
            )
            .await
        }
    }

    fn main_at(sha: &str) -> serde_json::Value {
        json!({ "repo": "github.com/acme/widgets", "ref": "refs/heads/main", "sha": sha })
    }

    /// A rig whose dagron-ci keeps bundles: a directory for its store, and
    /// links signed for a job's checkout.
    async fn bundle_rig(db: Db) -> (Rig, tempfile::TempDir, Arc<crate::bundles::Links>) {
        let (api_base, api) = stub_api().await;
        let (origin, forge) = stub_forge().await;
        forge.lock().unwrap().refs = refs(&[("refs/heads/main", MAIN)]);
        forge.lock().unwrap().behind =
            BTreeMap::from([(MAIN.to_string(), vec![PINNED.to_string()])]);
        let dir = tempfile::tempdir().unwrap();
        let token = dir.path().join("token");
        std::fs::write(&token, "dgr_ci").unwrap();
        let config = Config::from_lookup(|name| match name {
            "CI_DATABASE_URL" => Some("sqlite://unused.db".into()),
            "CI_CELLS" => Some(format!("default={api_base}")),
            "CI_API_TOKEN_FILE" => Some(token.to_str().unwrap().into()),
            _ => None,
        })
        .unwrap();
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
        let store = tempfile::tempdir().unwrap();
        let links = Arc::new(crate::bundles::Links::new(
            "http://ci.test:8090",
            b"engines' secret",
        ));
        let mut state =
            AppState::new(db.clone(), &config, Seams::default()).with_forge_origin(&origin);
        state.payloads = Some(
            crate::payloads::Payloads::from_url(&format!("file://{}", store.path().display()))
                .unwrap(),
        );
        state.bundle_links = Some(links.clone());
        let rig = Rig {
            app: crate::router(state),
            api,
            db,
            cells: config.cells.clone(),
            origin,
            _dir: dir,
        };
        (rig, store, links)
    }

    fn bundle_tar(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut b = tar::Builder::new(Vec::new());
        for (path, bytes) in entries {
            let mut h = tar::Header::new_gnu();
            h.set_size(bytes.len() as u64);
            h.set_mode(0o644);
            h.set_cksum();
            b.append_data(&mut h, path, *bytes).unwrap();
        }
        b.into_inner().unwrap()
    }

    impl Rig {
        async fn upload(
            &self,
            token: &str,
            query: &str,
            body: Vec<u8>,
        ) -> (StatusCode, serde_json::Value) {
            let req = Request::builder()
                .method(Method::POST)
                .uri(format!("/ci/v1/bundles?{query}"))
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/x-tar")
                .body(Body::from(body))
                .unwrap();
            let res = self.app.clone().oneshot(req).await.unwrap();
            let status = res.status();
            let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
                .await
                .unwrap();
            (
                status,
                serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
            )
        }

        async fn content(&self, path_and_query: &str) -> (StatusCode, Vec<u8>) {
            let req = Request::builder()
                .uri(path_and_query)
                .body(Body::empty())
                .unwrap();
            let res = self.app.clone().oneshot(req).await.unwrap();
            let status = res.status();
            (
                status,
                axum::body::to_bytes(res.into_body(), 1 << 22)
                    .await
                    .unwrap()
                    .to_vec(),
            )
        }
    }

    /// 1.6: a bundle is uploaded, checked and kept, and a run on it is of its
    /// base, untrusted, its uploader's alone; a link a checkout is given
    /// reads it back as a plain tar, and no other link does.
    #[tokio::test]
    async fn a_bundle_is_kept_and_run_on_its_base_untrusted_by_its_uploader_alone() {
        let (db, _dir) = sqlite().await;
        let (rig, _store, links) = bundle_rig(db).await;
        let tar = bundle_tar(&[
            (".dagron/ci/ci.yaml", b"name: ci\n"),
            ("src/lib.rs", b"x\n"),
        ]);
        let query = format!("repo=github.com/acme/widgets&base={MAIN}");

        let (status, body) = rig.upload("dgr_viewer", &query, tar.clone()).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        let (status, body) = rig
            .upload("dgr_operator", &query, b"not a tar".to_vec())
            .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        let evil = bundle_tar(&[(crate::bundles::DELETED, b"../outside\n")]);
        let (status, _) = rig.upload("dgr_operator", &query, evil).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        let (status, body) = rig
            .upload(
                "dgr_operator",
                "repo=github.com/acme/widgets&base=main",
                tar.clone(),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

        let (status, body) = rig
            .upload(
                "dgr_operator",
                &query,
                zstd::stream::encode_all(tar.as_slice(), 3).unwrap(),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let id = body["id"].as_str().unwrap().to_string();
        let uploaded = body["sha256"].clone();
        assert!(uploaded.as_str().is_some_and(|s| s.len() == 64), "{body}");
        assert_eq!(body["base"], MAIN);
        assert!(
            body["expires_at"].as_str().is_some_and(|e| !e.is_empty()),
            "{body}"
        );

        let on = |sha: Option<&str>| {
            let mut r = json!({ "repo": "github.com/acme/widgets", "ref": "refs/heads/main", "bundle": id });
            if let Some(sha) = sha {
                r["sha"] = json!(sha);
            }
            r
        };
        // Another's bundle is none of theirs.
        let (status, _) = rig.post(Some("dgr_other"), None, on(None)).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, body) = rig.post(Some("dgr_operator"), None, on(Some(PINNED))).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");

        let (status, body) = rig.post(Some("dgr_operator"), None, on(None)).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        let event_id = body["id"].as_str().unwrap().to_string();
        let ev = rig
            .db
            .claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ev.id, event_id);
        assert_eq!(
            (ev.sha.as_str(), ev.trust.as_str(), ev.bundle_id.as_deref()),
            (MAIN, "untrusted", Some(id.as_str())),
            "of its base, untrusted, naming the bundle"
        );
        // The run says which patch it is of, by what was uploaded.
        let (status, view) = rig.get("dgr_viewer", &event_id).await;
        assert_eq!(status, StatusCode::OK, "{view}");
        assert_eq!(
            view["bundle"],
            json!({ "id": id, "sha256": uploaded }),
            "{view}"
        );

        // The link a checkout is given reads it as a plain tar.
        let link = links.link(&id);
        let path = link.strip_prefix("http://ci.test:8090").unwrap();
        let (status, bytes) = rig.content(path).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(bytes, tar, "the tar as it was, decompressed");
        let forged = path.replace("sig=", "sig=00");
        assert_eq!(rig.content(&forged).await.0, StatusCode::FORBIDDEN);
        let other = Arc::new(crate::bundles::Links::new(
            "http://ci.test:8090",
            b"another secret",
        ));
        let wrong = other.link(&id);
        assert_eq!(
            rig.content(wrong.strip_prefix("http://ci.test:8090").unwrap())
                .await
                .0,
            StatusCode::FORBIDDEN
        );
    }

    async fn the_run_api_contract(db: Db) {
        let rig = rig(db).await;

        // Nobody is told anything before dagron-api says who they are.
        let (status, _) = rig.post(None, None, main_at(PINNED)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, _) = rig.post(Some("dgr_nobody"), None, main_at(PINNED)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let unknown = json!({ "repo": "github.com/acme/secret", "ref": "refs/heads/main" });
        let (status, _) = rig.post(Some("dgr_nobody"), None, unknown.clone()).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "not 404: existence is hidden"
        );
        let (status, _) = rig.post(Some("dgr_operator"), None, unknown).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, body) = rig.post(Some("dgr_viewer"), None, main_at(PINNED)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

        // A repository is named as a forge names it, without regard to case:
        // someone who copies `Acme/Widgets` from its page means the one that
        // is connected, and the event is of the repository as it is kept.
        let mut as_its_page_has_it = main_at(PINNED);
        as_its_page_has_it["repo"] = json!("GitHub.com/Acme/Widgets");
        let (status, run) = rig
            .post(Some("dgr_operator"), None, as_its_page_has_it)
            .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{run}");
        assert_eq!(run["repo"].as_str(), Some("github.com/acme/widgets"));

        // A dispatch at a pinned commit, with inputs.
        let mut request = main_at(PINNED);
        request["inputs"] = json!({ "suite": "smoke", "dry_run": "true" });
        let (status, run) = rig.post(Some("dgr_operator"), None, request.clone()).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{run}");
        assert_eq!(
            (
                run["kind"].as_str(),
                run["source"].as_str(),
                run["trust"].as_str(),
                run["actor"].as_str(),
                run["state"].as_str(),
                run["sha"].as_str(),
            ),
            (
                Some("dispatch"),
                Some("api"),
                Some("trusted"),
                Some("dev@example.com"),
                Some("new"),
                Some(PINNED),
            )
        );
        assert_eq!(run["engine_runs"], json!([]));
        let id = run["id"].as_str().unwrap().to_string();
        // The credential went to the repository's cell, as it was sent.
        assert!(rig
            .api
            .lock()
            .unwrap()
            .identified
            .iter()
            .all(|a| a.starts_with("Bearer dgr_")));

        // Without a key every request is a run; under one, a repeat is the
        // first request's event, and another request is refused.
        let (_, again) = rig.post(Some("dgr_operator"), None, request.clone()).await;
        assert_ne!(again["id"], run["id"]);
        let (status, first) = rig
            .post(Some("dgr_operator"), Some("k-1"), request.clone())
            .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let (status, repeat) = rig
            .post(Some("dgr_operator"), Some("k-1"), request.clone())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(repeat["id"], first["id"]);
        let (status, body) = rig
            .post(Some("dgr_operator"), Some("k-1"), main_at(MAIN))
            .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        let mut named = request.clone();
        named["pipeline"] = json!("deploy");
        let (status, body) = rig
            .post(Some("dgr_operator"), Some("k-1"), named.clone())
            .await;
        assert_eq!(status, StatusCode::CONFLICT, "another pipeline: {body}");
        // A dispatch may name the one pipeline it runs.
        let (status, deploy) = rig.post(Some("dgr_operator"), None, named).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{deploy}");
        assert_eq!(deploy["pipeline"].as_str(), Some("deploy"));
        assert_eq!(first["pipeline"], serde_json::Value::Null);
        let (status, other) = rig
            .post(Some("dgr_other"), Some("k-1"), request.clone())
            .await;
        assert_eq!(
            status,
            StatusCode::ACCEPTED,
            "another caller's key is its own"
        );
        assert_ne!(other["id"], first["id"]);
        assert_eq!(
            other["actor"].as_str(),
            Some("u-2"),
            "no email: the subject"
        );

        // No sha: the forge says where the ref points, exactly.
        let by_ref = json!({ "repo": "github.com/acme/widgets", "ref": "refs/heads/main" });
        let (status, resolved) = rig.post(Some("dgr_operator"), None, by_ref).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{resolved}");
        assert_eq!(resolved["sha"].as_str(), Some(MAIN));
        let gone = json!({ "repo": "github.com/acme/widgets", "ref": "refs/heads/gone" });
        let (status, _) = rig.post(Some("dgr_operator"), None, gone).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);

        // What a request must not be.
        for bad in [
            json!({ "repo": "github.com/acme/widgets", "ref": "main" }),
            json!({ "repo": "github.com/acme/widgets", "ref": "refs/heads/" }),
            json!({ "repo": "github.com/acme/widgets", "ref": "refs/heads/a b" }),
            json!({ "repo": "github.com/acme/widgets", "ref": "refs/heads/main", "sha": PINNED.to_uppercase() }),
            json!({ "repo": "github.com/acme/widgets", "ref": "refs/heads/main", "inputs": { "ci_trust": "trusted" } }),
            json!({ "repo": "github.com/acme/widgets", "ref": "refs/heads/main", "inputs": { "bad-name": "x" } }),
            json!({ "repo": "github.com/acme/widgets", "ref": "refs/heads/main", "pipeline": "a/b" }),
            json!({ "repo": "github.com/acme/widgets", "ref": "refs/heads/main", "pipeline": "../deploy" }),
            json!({ "repo": "github.com/acme/widgets", "ref": "refs/heads/main", "pipeline": "" }),
            json!({ "repo": "github.com/acme/widgets", "ref": "refs/heads/main", "pipeline": ".dagron" }),
            json!({ "repo": "github.com/acme/widgets", "ref": "refs/heads/main", "inptus": {} }),
        ] {
            let (status, body) = rig.post(Some("dgr_operator"), None, bad.clone()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{bad} → {body}");
        }
        let (status, _) = rig
            .post(Some("dgr_operator"), Some("has space"), main_at(PINNED))
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // Any identified caller may read; an unknown id is 404 only to one.
        let (status, read) = rig.get("dgr_viewer", &id).await;
        assert_eq!(
            (status, read["id"].as_str()),
            (StatusCode::OK, Some(id.as_str()))
        );
        assert_eq!(rig.get("dgr_viewer", "nope").await.0, StatusCode::NOT_FOUND);
        assert_eq!(rig.get("dgr_nobody", &id).await.0, StatusCode::UNAUTHORIZED);
        assert_eq!(
            rig.get("dgr_nobody", "nope").await.0,
            StatusCode::UNAUTHORIZED
        );

        // A commit is one that is on the ref the caller names, or the run is
        // not made. A fork's head is reachable in the repository it forks,
        // and was never on main.
        let (status, body) = rig.post(Some("dgr_operator"), None, main_at(FORKS)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(
            body["title"].as_str(),
            Some("the commit is not on the ref"),
            "{body}"
        );
        // Main is at an earlier commit than the fork's head: behind it.
        assert_eq!(
            body["detail"].as_str(),
            Some(
                "cccccccccccc is not on refs/heads/main: the ref is at aaaaaaaaaaaa, which is \
                 behind it"
            )
        );
        assert!(
            body["detail"]
                .as_str()
                .is_some_and(|why| why.contains("is not on refs/heads/main")),
            "{body}"
        );
        // Nor is a commit the forge does not know.
        let unknown = "d".repeat(40);
        let (status, body) = rig
            .post(Some("dgr_operator"), None, main_at(&unknown))
            .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert!(
            body["detail"]
                .as_str()
                .is_some_and(|why| why.contains("the forge knows no commit dddddddddddd")),
            "{body}"
        );
        // The ref's own commit is, and asks the forge no more than that.
        let (status, at_head) = rig.post(Some("dgr_operator"), None, main_at(MAIN)).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{at_head}");
        // And a ref the repository does not have is said to be that.
        let (status, body) = rig
            .post(
                Some("dgr_operator"),
                None,
                json!({ "repo": "github.com/acme/widgets", "ref": "refs/heads/gone", "sha": PINNED }),
            )
            .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["title"].as_str(), Some("no such ref"), "{body}");

        // An agent's dispatch is queued as `agent`.
        let (status, agent) = rig.post(Some("dgr_agent"), None, main_at(PINNED)).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(agent["trust"].as_str(), Some("agent"));

        // The workers run what was queued: the inputs reach the workflow
        // beside the event, and the agent's dispatch is refused.
        let wake = Arc::new(Notify::new());
        let (stop_tx, stop) = watch::channel(false);
        let mut workers = crate::worker::spawn(
            rig.db.clone(),
            Arc::new(Dispatcher::new(
                rig.db.clone(),
                rig.cells.clone(),
                "dgr_ci".into(),
                Arc::new(crate::forge::Forge::new(None).with_origin(&rig.origin)),
                crate::pipeline::compile::Settings::default(),
            )),
            crate::worker::Settings {
                workers: 2,
                lease: Duration::from_secs(60),
                max_attempts: 3,
            },
            Arc::new(Metrics::default()),
            wake.clone(),
            stop,
        );
        wake.notify_waiters();
        let mut finished = None;
        for _ in 0..400 {
            let (_, read) = rig.get("dgr_viewer", &id).await;
            let (_, agent_read) = rig.get("dgr_viewer", agent["id"].as_str().unwrap()).await;
            if read["state"] == "done" && agent_read["state"] == "skipped" {
                finished = Some((read, agent_read));
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let (read, agent_read) = finished.expect("the workers finish both dispatches");
        assert_eq!(read["engine_runs"][0]["cell"].as_str(), Some("default"));
        assert_eq!(
            read["engine_runs"][0]["pipeline"].as_str(),
            Some("workflow:wf-1")
        );
        assert!(read["engine_runs"][0]["id"]
            .as_str()
            .unwrap()
            .starts_with("run-"));
        assert!(
            agent_read["reason"]
                .as_str()
                .unwrap()
                .contains("agent trust class"),
            "{agent_read}"
        );
        {
            let api = rig.api.lock().unwrap();
            let params = api
                .runs
                .iter()
                .map(|r| &r.2)
                .find(|p| p.get("suite").is_some())
                .expect("the dispatch with inputs ran");
            assert_eq!(params["suite"], "smoke");
            assert_eq!(params["ci_event"], "dispatch");
            assert_eq!(params["ci_sha"], PINNED);
            assert_eq!(params["ci_ref"], "refs/heads/main");
            assert_eq!(params["ci_trust"], "trusted");
            assert!(api.runs.iter().all(|r| r.2["ci_trust"] != "agent"));
        }
        let _ = stop_tx.send(true);
        while workers.join_next().await.is_some() {}
    }

    /// The repositories' door, on either back end: who may use it, what a
    /// `PUT` and a `DELETE` do, and what is on record after.
    async fn the_repositories_door(db: Db) {
        let rig = rig(db).await;
        let path = "/ci/v1/repos/github.com/acme/gadgets";
        let wants = Some(json!({ "workflow": "wf-1", "poll_secs": 0 }));

        // Who: an admin, as dagron-api knows one, and no agent.
        for token in [None, Some("dgr_nobody")] {
            let (code, _) = rig
                .call(Method::PUT, path, token, None, wants.clone())
                .await;
            assert_eq!(code, StatusCode::UNAUTHORIZED, "{token:?}");
        }
        for token in ["dgr_operator", "dgr_viewer", "dgr_agent", "dgr_agent_admin"] {
            for (method, at) in [
                (Method::PUT, path),
                (Method::DELETE, path),
                (Method::GET, path),
                (Method::GET, "/ci/v1/repos"),
            ] {
                let (code, body) = rig.call(method.clone(), at, Some(token), None, None).await;
                assert_eq!(code, StatusCode::FORBIDDEN, "{token} {method} {at}: {body}");
            }
        }
        // A viewer changes nothing, admin or not, and such an admin reads.
        for method in [Method::PUT, Method::DELETE] {
            let (code, body) = rig
                .call(method.clone(), path, Some("dgr_viewer_admin"), None, None)
                .await;
            assert_eq!(code, StatusCode::FORBIDDEN, "{method}: {body}");
        }
        let (code, _) = rig
            .call(
                Method::GET,
                "/ci/v1/repos",
                Some("dgr_viewer_admin"),
                None,
                None,
            )
            .await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(
            rig.db
                .find_repo("github.com", "acme/gadgets")
                .await
                .unwrap(),
            None
        );

        // What is no repository's name, or no settings of one, is refused.
        let admin = Some("dgr_admin");
        for bad in [
            "/ci/v1/repos/github.com/acme",
            "/ci/v1/repos/github.com/acme/..",
        ] {
            let (code, _) = rig.call(Method::PUT, bad, admin, None, None).await;
            assert_eq!(code, StatusCode::BAD_REQUEST, "{bad}");
        }
        for bad in [
            json!({ "mode": "push" }),
            json!({ "forge": "sourcehut" }),
            json!({ "cell": "nowhere" }),
            json!({ "poll_secs": -1 }),
            json!({ "poll_secs": 5 }),
            json!({ "poll_secs": 86_401 }),
        ] {
            let (code, body) = rig
                .call(Method::PUT, path, admin, None, Some(bad.clone()))
                .await;
            assert_eq!(code, StatusCode::BAD_REQUEST, "{bad}: {body}");
        }
        let (code, _) = rig.call(Method::GET, path, admin, None, None).await;
        assert_eq!(code, StatusCode::NOT_FOUND, "it was never served");
        let (code, _) = rig.call(Method::DELETE, path, admin, None, None).await;
        assert_eq!(code, StatusCode::NOT_FOUND);

        // A PUT connects it, and a second changes what it says and no more.
        let (code, body) = rig.call(Method::PUT, path, admin, None, wants).await;
        assert_eq!(code, StatusCode::CREATED, "{body}");
        assert_eq!(
            (
                &body["action"],
                &body["repo"],
                &body["cell"],
                &body["workflow"]
            ),
            (
                &json!("connected"),
                &json!("github.com/acme/gadgets"),
                &json!("default"),
                &json!("wf-1")
            )
        );
        let id = body["id"].as_str().unwrap().to_string();
        let served = rig
            .db
            .find_repo("github.com", "acme/gadgets")
            .await
            .unwrap();
        assert_eq!(served.map(|r| r.id), Some(id.clone()));
        let (code, body) = rig.call(Method::PUT, path, admin, None, None).await;
        assert_eq!(
            (code, &body["action"]),
            (StatusCode::OK, &json!("unchanged"))
        );
        let (code, body) = rig
            .call(
                Method::PUT,
                path,
                admin,
                None,
                Some(json!({ "workflow": null })),
            )
            .await;
        assert_eq!(
            (code, &body["action"], &body["workflow"]),
            (StatusCode::OK, &json!("changed"), &json!(null))
        );
        // Said again, it is so already: nothing changes, and nothing is
        // recorded (the changes below are as many as before).
        let same = json!({ "workflow": null, "poll_secs": 0, "cell": "default" });
        let (code, body) = rig.call(Method::PUT, path, admin, None, Some(same)).await;
        assert_eq!(
            (code, &body["action"]),
            (StatusCode::OK, &json!("unchanged"))
        );
        let (code, _) = rig
            .call(
                Method::PUT,
                path,
                admin,
                None,
                Some(json!({ "forge": "gitlab" })),
            )
            .await;
        assert_eq!(code, StatusCode::BAD_REQUEST, "its forge is said once");
        let (code, body) = rig
            .call(Method::GET, "/ci/v1/repos", admin, None, None)
            .await;
        assert_eq!(code, StatusCode::OK);
        assert!(
            body["repos"]
                .as_array()
                .unwrap()
                .iter()
                .any(|r| r["id"] == json!(id) && r["poll_secs"] == json!(0)),
            "{body}"
        );

        // A DELETE disconnects it, and keeps what it had: its row, and who
        // did what to it.
        let (code, body) = rig.call(Method::DELETE, path, admin, None, None).await;
        assert_eq!(
            (code, &body["action"]),
            (StatusCode::OK, &json!("disconnected"))
        );
        let (code, _) = rig.call(Method::DELETE, path, admin, None, None).await;
        assert_eq!(code, StatusCode::NOT_FOUND, "it is not connected now");
        assert_eq!(
            rig.db
                .find_repo("github.com", "acme/gadgets")
                .await
                .unwrap(),
            None
        );
        let (code, body) = rig.call(Method::GET, path, admin, None, None).await;
        assert_eq!(code, StatusCode::OK, "{body}");
        assert_eq!(
            (&body["id"], &body["connected"]),
            (&json!(id), &json!(false))
        );
        assert!(body["disconnected_at"].is_string(), "{body}");
        let changes: Vec<(&str, &str)> = body["changes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| (c["actor"].as_str().unwrap(), c["action"].as_str().unwrap()))
            .collect();
        assert_eq!(
            changes,
            [
                ("admin@example.com", "connected"),
                ("admin@example.com", "changed"),
                ("admin@example.com", "disconnected"),
            ]
        );
        assert_eq!(body["changes"][0]["set"]["poll_secs"], json!(0));
        assert_eq!(body["changes"][1]["set"], json!({ "workflow": null }));

        // Connected again, by another admin, it is the repository it was.
        let (code, body) = rig
            .call(Method::PUT, path, Some("dgr_admin_two"), None, None)
            .await;
        assert_eq!(
            (code, &body["action"], &body["id"]),
            (StatusCode::CREATED, &json!("reconnected"), &json!(id))
        );
        let (_, body) = rig.call(Method::GET, path, admin, None, None).await;
        assert_eq!(body["connected"], json!(true));
        assert_eq!(
            body["changes"][3]["actor"], "u-8",
            "one with no email is their subject"
        );
    }

    /// The fleet's report is an admin's to read, and refuses what is no
    /// report's.
    #[tokio::test]
    async fn the_fleet_s_usage_is_an_admin_s_to_read() {
        let (db, _dir) = sqlite().await;
        let rig = rig(db).await;
        let at = "/ci/v1/fleet/usage?by=label&since=2026-10-01&until=2026-10-02";
        let (code, _) = rig.call(Method::GET, at, None, None, None).await;
        assert_eq!(code, StatusCode::UNAUTHORIZED);
        for token in ["dgr_operator", "dgr_viewer", "dgr_agent_admin"] {
            let (code, body) = rig.call(Method::GET, at, Some(token), None, None).await;
            assert_eq!(code, StatusCode::FORBIDDEN, "{token}: {body}");
        }
        let (code, body) = rig
            .call(Method::GET, at, Some("dgr_admin"), None, None)
            .await;
        assert_eq!(code, StatusCode::OK, "{body}");
        assert_eq!(
            body,
            json!({
                "since": "2026-10-01", "until": "2026-10-02", "by": "label", "rows": [],
                "total": { "key": "total", "jobs": 0, "job_seconds": 0, "runners": 0, "runner_seconds": 0 },
            })
        );
        let (code, body) = rig
            .call(
                Method::GET,
                "/ci/v1/fleet/usage",
                Some("dgr_admin"),
                None,
                None,
            )
            .await;
        assert_eq!((code, &body["by"]), (StatusCode::OK, &json!("repo")));
        for bad in [
            "/ci/v1/fleet/usage?by=cost",
            "/ci/v1/fleet/usage?since=last-week",
            "/ci/v1/fleet/usage?since=2026-10-02&until=2026-10-01",
            "/ci/v1/fleet/usage?per=day",
        ] {
            let (code, _) = rig
                .call(Method::GET, bad, Some("dgr_admin"), None, None)
                .await;
            assert_eq!(code, StatusCode::BAD_REQUEST, "{bad}");
        }
    }

    /// `GET /ci/v1/me` is the caller as the `default` cell's dagron-api
    /// says, and what a run they ask for is recorded as; a credential it
    /// refuses is a 401.
    async fn who_the_caller_is(db: Db) {
        let rig = rig(db).await;
        let me = |token| rig.call(Method::GET, "/ci/v1/me", token, None, None);
        let (code, _) = me(None).await;
        assert_eq!(code, StatusCode::UNAUTHORIZED);
        let (code, body) = me(Some("dgr_operator")).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(
            body,
            json!({ "sub": "u-1", "email": "dev@example.com", "groups": [], "trust": "trusted" })
        );
        let (_, body) = me(Some("dgr_viewer")).await;
        assert_eq!(body["groups"], json!(["viewer"]));
        let (_, body) = me(Some("dgr_agent")).await;
        assert_eq!(
            (&body["sub"], &body["trust"]),
            (&json!("agent:fixer"), &json!("agent"))
        );
        let (code, body) = me(Some("dgr_unknown")).await;
        assert_eq!(
            (code, &body["title"]),
            (StatusCode::UNAUTHORIZED, &json!("not authenticated"))
        );
        assert_eq!(
            rig.api.lock().unwrap().identified,
            [
                "Bearer dgr_operator",
                "Bearer dgr_viewer",
                "Bearer dgr_agent",
                "Bearer dgr_unknown"
            ]
        );
    }

    #[tokio::test]
    async fn sqlite_says_who_the_caller_is() {
        let (db, _dir) = sqlite().await;
        who_the_caller_is(db).await;
    }

    #[tokio::test]
    async fn postgres_says_who_the_caller_is() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        who_the_caller_is(db.clone()).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    /// A repository is named by its id as well as by its name, and served
    /// as its mode says: a fleet-mode repository's runs are its forge's.
    async fn a_repository_by_its_id_and_its_mode(db: Db) {
        let rig = rig(db).await;
        let admin = Some("dgr_admin");
        let named = "/ci/v1/repos/gitlab.example/group/app";
        let (code, body) = rig
            .call(
                Method::PUT,
                named,
                admin,
                None,
                Some(json!({ "forge": "gitlab" })),
            )
            .await;
        assert_eq!(code, StatusCode::CREATED, "{body}");
        assert_eq!(body["mode"], "native", "unless it is said otherwise");
        let id = body["id"].as_str().unwrap().to_string();
        let by_id = format!("/ci/v1/repos/{id}");
        let (code, body) = rig.call(Method::GET, &by_id, admin, None, None).await;
        assert_eq!(
            (code, &body["repo"], &body["mode"]),
            (
                StatusCode::OK,
                &json!("gitlab.example/group/app"),
                &json!("native")
            )
        );
        let (code, body) = rig
            .call(
                Method::PUT,
                &by_id,
                admin,
                None,
                Some(json!({ "mode": "fleet" })),
            )
            .await;
        assert_eq!(
            (code, &body["action"], &body["mode"]),
            (StatusCode::OK, &json!("changed"), &json!("fleet"))
        );
        let (code, body) = rig.call(Method::DELETE, &by_id, admin, None, None).await;
        assert_eq!(
            (code, &body["repo"], &body["id"]),
            (
                StatusCode::OK,
                &json!("gitlab.example/group/app"),
                &json!(id)
            )
        );
        // An id names a repository that was connected: a PUT of it connects
        // it again, on the forge it was on.
        let (code, body) = rig.call(Method::PUT, &by_id, admin, None, None).await;
        assert_eq!(
            (code, &body["action"], &body["forge"], &body["id"]),
            (
                StatusCode::CREATED,
                &json!("reconnected"),
                &json!("gitlab"),
                &json!(id)
            )
        );
        let (_, body) = rig.call(Method::GET, named, admin, None, None).await;
        let actions: Vec<(&str, &serde_json::Value)> = body["changes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| (c["action"].as_str().unwrap(), &c["set"]))
            .collect();
        assert_eq!(actions[1], ("changed", &json!({ "mode": "fleet" })));
        assert_eq!(actions[3].0, "reconnected");
        // An id nobody was given names nothing, and what is neither an id
        // nor a name is refused.
        let nobody_s = format!("/ci/v1/repos/{}", uuid::Uuid::now_v7());
        for method in [Method::GET, Method::PUT, Method::DELETE] {
            let (code, _) = rig.call(method.clone(), &nobody_s, admin, None, None).await;
            assert_eq!(code, StatusCode::NOT_FOUND, "{method}");
        }
        let (code, _) = rig
            .call(Method::GET, "/ci/v1/repos/not-an-id", admin, None, None)
            .await;
        assert_eq!(code, StatusCode::BAD_REQUEST);

        // A fleet-mode repository's runs are its forge's: none is asked of
        // dagron-ci, none is run again, and nothing is queued.
        let (status, body) = rig
            .post(
                Some("dgr_operator"),
                None,
                json!({ "repo": "github.com/acme/widgets", "ref": "refs/heads/main" }),
            )
            .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        let earlier = body["id"].as_str().unwrap().to_string();
        assert!(rig
            .db
            .claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .is_some());
        let (code, _) = rig
            .call(
                Method::PUT,
                "/ci/v1/repos/github.com/acme/widgets",
                admin,
                None,
                Some(json!({ "mode": "fleet" })),
            )
            .await;
        assert_eq!(code, StatusCode::OK);
        let (status, body) = rig
            .post(
                Some("dgr_operator"),
                None,
                json!({ "repo": "github.com/acme/widgets", "ref": "refs/heads/main" }),
            )
            .await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        let (status, body) = rig
            .call(
                Method::POST,
                &format!("/ci/v1/runs/{earlier}/rerun"),
                Some("dgr_operator"),
                None,
                None,
            )
            .await;
        assert_eq!(
            (status, &body["title"]),
            (StatusCode::CONFLICT, &json!("repository in fleet mode")),
            "{body}"
        );
        assert!(rig
            .db
            .claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn sqlite_names_a_repository_by_its_id_and_serves_it_as_its_mode_says() {
        let (db, _dir) = sqlite().await;
        a_repository_by_its_id_and_its_mode(db).await;
    }

    #[tokio::test]
    async fn postgres_names_a_repository_by_its_id_and_serves_it_as_its_mode_says() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        a_repository_by_its_id_and_its_mode(db.clone()).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    #[tokio::test]
    async fn sqlite_keeps_the_repositories_door() {
        let (db, _dir) = sqlite().await;
        the_repositories_door(db).await;
    }

    #[tokio::test]
    async fn postgres_keeps_the_repositories_door() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        the_repositories_door(db.clone()).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    #[tokio::test]
    async fn sqlite_keeps_the_run_api_contract() {
        let (db, _dir) = sqlite().await;
        the_run_api_contract(db).await;
    }

    #[tokio::test]
    async fn postgres_keeps_the_run_api_contract() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        the_run_api_contract(db.clone()).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    #[tokio::test]
    async fn without_a_cell_the_run_api_is_not_there() {
        let (db, _dir) = sqlite().await;
        let config =
            Config::from_lookup(|name| (name == "CI_DATABASE_URL").then(|| "sqlite://x.db".into()))
                .unwrap();
        let app = crate::router(AppState::new(db, &config, Seams::default()));
        let req = Request::post("/ci/v1/runs")
            .header("authorization", "Bearer dgr_operator")
            .body(Body::from(main_at(PINNED).to_string()))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(req).await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
        let req = Request::get("/ci/v1/me")
            .header("authorization", "Bearer dgr_operator")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(req).await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn only_a_bearer_credential_is_passed_on() {
        let headers = |v: &str| {
            let mut h = HeaderMap::new();
            h.insert(header::AUTHORIZATION, v.parse().unwrap());
            h
        };
        assert_eq!(bearer(&headers("Bearer dgr_x")), Some("dgr_x"));
        assert_eq!(bearer(&headers("bearer  dgr_x ")), Some("dgr_x"));
        for refused in ["Basic dXNlcjpwYXNz", "Bearer", "Bearer a b", "dgr_x"] {
            assert_eq!(bearer(&headers(refused)), None, "{refused}");
        }
        assert_eq!(bearer(&HeaderMap::new()), None);
    }

    #[test]
    fn the_fingerprint_is_the_request_as_sent() {
        let request = |sha: Option<&str>, input: &str| RunRequest {
            repo: "github.com/acme/widgets".into(),
            git_ref: "refs/heads/main".into(),
            sha: sha.map(str::to_string),
            inputs: BTreeMap::from([("suite".to_string(), input.to_string())]),
            pipeline: None,
            bundle: None,
        };
        let f = |r: &RunRequest| fingerprint("github.com", "acme/widgets", r);
        assert_eq!(f(&request(None, "a")), f(&request(None, "a")));
        assert_ne!(f(&request(None, "a")), f(&request(None, "b")));
        assert_ne!(f(&request(None, "a")), f(&request(Some(MAIN), "a")));
    }
}
