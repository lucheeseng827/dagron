//! The repositories' door. `GET /ci/v1/repos` lists the repositories this
//! dagron-ci serves, and `GET`, `PUT` and `DELETE /ci/v1/repos/{host}/{owner}/{name}`
//! read one, connect or change it, and disconnect it. Each takes the
//! repository's id in its name's place too (`/ci/v1/repos/{id}`), the id
//! dagron-ci gave it when it was first connected, which stays the same
//! whatever its forge calls it: a path names one or the other, as an id has
//! no `/` in it and a name has two. An id names a repository that was
//! connected once, so a `PUT` of one changes it or connects it again.
//!
//! **How it is served** (`mode`): `native` runs its `.dagron/ci`
//! pipelines; `compat` those and the forge's own files its `.dagron/ci.yaml`
//! names; `fleet` none, as its forge runs them ([`crate::db::Mode`]). A
//! repository is connected `native` unless the `PUT` says otherwise.
//!
//! **Who.** An admin, as the dagron-api of the `default` cell knows the
//! caller (`GET /api/me`, the group `admin`): the same credential the run
//! API takes, and the same answer to who presents it. An agent's token
//! connects nothing, whatever groups it carries. A viewer changes nothing,
//! as dagron-api holds one: an admin who is also in `viewer` reads, and
//! their `PUT` and `DELETE` are refused.
//!
//! **Each change is recorded with who made it**, in the transaction that
//! makes it: `GET` shows a repository's changes, oldest first. Before this
//! door a repository was connected by whoever held the database's
//! credentials, and the database recorded nobody.
//!
//! **A disconnect keeps what the repository had.** Its events and runs are
//! read as before; it takes no more webhooks, run requests or sweeps, and
//! the events of it that were waiting are skipped. Connected again, it is
//! the repository it was.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use super::{bearer, database_error, identify, Caller};
use crate::config::{MAX_POLL_SECS, MIN_POLL_SECS};
use crate::db::{Connected, Mode, Repo};
use crate::problem::problem;
use crate::server::AppState;

/// The largest body a `PUT` may have.
pub const MAX_BODY: usize = 4 * 1024;

/// The forges a repository may be on, as the table holds them.
const FORGES: [&str; 6] = [
    "github",
    "gitlab",
    "gitea",
    "forgejo",
    "bitbucket",
    "generic",
];

/// What a `PUT` may say of a repository. A key that is absent leaves what
/// it names as it is; `workflow` and `poll_secs` may be `null`, for none and
/// for the default interval.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Wanted {
    forge: Option<String>,
    cell: Option<String>,
    #[serde(default, deserialize_with = "present")]
    workflow: Option<Option<String>>,
    #[serde(default, deserialize_with = "present")]
    poll_secs: Option<Option<u64>>,
    mode: Option<Mode>,
}

/// A key that is there, whatever it holds: `null` is a value, and not the
/// key's absence.
fn present<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

/// `host/owner/name`, as a path names a repository: a host, and a name of
/// at least two parts, each of what a forge's URL takes. The host may carry
/// a port, as `dagron-ci repo` takes one ([`crate::cli::split_spec`]):
/// what is connected at a terminal is read and disconnected here.
fn split(spec: &str) -> Option<(&str, &str)> {
    let (host, name) = spec.split_once('/')?;
    let part = |s: &str| {
        !s.is_empty()
            && s.len() <= 128
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
            && s != "."
            && s != ".."
    };
    let host_ok = match host.split_once(':') {
        Some((host, port)) => {
            part(host) && !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit())
        }
        None => part(host),
    };
    (host_ok && name.contains('/') && name.split('/').all(part) && spec.len() <= 255)
        .then_some((host, name))
}

/// Whether `spec` is a repository's id as dagron-ci writes one: a UUID,
/// hyphenated and in lower case ([`crate::db::Db::connect_repo`]).
fn is_id(spec: &str) -> bool {
    uuid::Uuid::parse_str(spec).is_ok_and(|id| id.hyphenated().to_string() == spec)
}

/// The repository a path names, as its host and its name: as it says them,
/// or as the row of the id it says has them. Else how the request ends.
async fn named(st: &AppState, spec: &str) -> Result<(String, String), Response> {
    if let Some((host, name)) = split(spec) {
        return Ok((host.to_string(), name.to_string()));
    }
    if !is_id(spec) {
        return Err(misnamed(spec));
    }
    match st.db.repo_any_by_id(spec).await {
        Ok(Some((repo, _))) => Ok((repo.forge_host, repo.full_name)),
        Ok(None) => Err(unknown(spec)),
        Err(e) => Err(database_error(e).1),
    }
}

/// The caller, where they may read what this dagron-ci serves, or change
/// it (`changes`): an admin who is no agent, and to change it no viewer
/// either. Else how the request ends.
pub(super) async fn admin(
    st: &AppState,
    headers: &HeaderMap,
    changes: bool,
) -> Result<Caller, Response> {
    let Some(credential) = bearer(headers) else {
        return Err(problem(
            StatusCode::UNAUTHORIZED,
            "not authenticated",
            "send a dagron-api session or API token as Authorization: Bearer",
        ));
    };
    let caller = identify(st, None, credential)
        .await
        .map_err(|(_, response)| response)?;
    if caller.sub.starts_with("agent:") || !caller.admin() {
        return Err(problem(
            StatusCode::FORBIDDEN,
            "not allowed",
            "which repositories a dagron-ci serves is an admin's to say",
        ));
    }
    if changes && caller.viewer() {
        return Err(problem(
            StatusCode::FORBIDDEN,
            "read-only caller",
            "a viewer may read the repositories, and change none",
        ));
    }
    Ok(caller)
}

fn shown(repo: &Repo) -> Value {
    json!({
        "id": repo.id,
        "repo": format!("{}/{}", repo.forge_host, repo.full_name),
        "forge": repo.forge,
        "mode": repo.mode.as_str(),
        "cell": repo.cell,
        "workflow": repo.workflow,
    })
}

fn unknown(spec: &str) -> Response {
    problem(
        StatusCode::NOT_FOUND,
        "no such repository",
        format!("{spec:?} is not a repository this dagron-ci has served"),
    )
}

fn misnamed(spec: &str) -> Response {
    problem(
        StatusCode::BAD_REQUEST,
        "not a repository's name",
        format!("{spec:?} is neither host/owner/name nor a repository's id"),
    )
}

/// `GET /ci/v1/repos`: the repositories that are connected.
pub async fn list(State(st): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(response) = admin(&st, &headers, false).await {
        return response;
    }
    match st.db.list_repos().await {
        Ok(listed) => {
            let repos: Vec<Value> = listed
                .iter()
                .map(|l| {
                    let mut repo = shown(&l.repo);
                    repo["poll_secs"] = json!(l.poll_secs);
                    repo["polled_at"] = json!(l.polled_at);
                    repo["poll_error"] = json!(l.poll_error);
                    repo
                })
                .collect();
            (StatusCode::OK, Json(json!({ "repos": repos }))).into_response()
        }
        Err(e) => database_error(e).1,
    }
}

/// `GET /ci/v1/repos/{host}/{owner}/{name}` (or `{id}`): a repository this
/// dagron-ci serves or served, and what was changed of it, by whom.
pub async fn get(
    State(st): State<AppState>,
    Path(spec): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = admin(&st, &headers, false).await {
        return response;
    }
    let (host, name) = match named(&st, &spec).await {
        Ok(named) => named,
        Err(response) => return response,
    };
    let (repo, disconnected_at) = match st.db.repo_any(&host, &name).await {
        Ok(Some(found)) => found,
        Ok(None) => return unknown(&spec),
        Err(e) => return database_error(e).1,
    };
    let changes = match st.db.repo_changes(&repo.id).await {
        Ok(changes) => changes,
        Err(e) => return database_error(e).1,
    };
    let mut body = shown(&repo);
    body["connected"] = json!(disconnected_at.is_none());
    body["disconnected_at"] = json!(disconnected_at);
    body["changes"] = changes
        .iter()
        .map(|c| {
            json!({
                "at": c.at,
                "actor": c.actor,
                "action": c.action,
                "set": serde_json::from_str::<Value>(&c.detail).unwrap_or(Value::Null),
            })
        })
        .collect();
    (StatusCode::OK, Json(body)).into_response()
}

/// `PUT /ci/v1/repos/{host}/{owner}/{name}` (or `{id}`): connect the
/// repository, or change what a connected one's events run, where, how often
/// it is polled, and how it is served. 201 where it was not connected, 200
/// where it was.
pub async fn put(
    State(st): State<AppState>,
    Path(spec): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let caller = match admin(&st, &headers, true).await {
        Ok(caller) => caller,
        Err(response) => return response,
    };
    let (host, name) = match named(&st, &spec).await {
        Ok(named) => named,
        Err(response) => return response,
    };
    let (host, name) = (host.as_str(), name.as_str());
    let wanted: Wanted = if body.iter().all(u8::is_ascii_whitespace) {
        Wanted::default()
    } else {
        match serde_json::from_slice(&body) {
            Ok(wanted) => wanted,
            Err(e) => {
                return problem(
                    StatusCode::BAD_REQUEST,
                    "not a repository's settings",
                    format!("the body takes forge, cell, workflow, poll_secs and mode: {e}"),
                )
            }
        }
    };
    let refused = |detail: String| {
        problem(
            StatusCode::BAD_REQUEST,
            "not a repository's settings",
            detail,
        )
    };
    if let Some(forge) = wanted.forge.as_deref().filter(|f| !FORGES.contains(f)) {
        return refused(format!("forge {forge:?} is none of {}", FORGES.join(", ")));
    }
    if let Some(cell) = wanted
        .cell
        .as_deref()
        .filter(|c| !st.cells.contains_key(*c))
    {
        return refused(format!(
            "cell {cell:?} is not one this dagron-ci names (CI_CELLS)"
        ));
    }
    if let Some(Some(workflow)) = &wanted.workflow {
        if workflow.is_empty() || workflow.len() > 128 || !workflow.is_ascii() {
            return refused("workflow is a workflow's id, or null for none".to_string());
        }
    }
    // The bounds `dagron-ci repo` holds an interval to: a repository read
    // every second spends its forge's rate limit for every other.
    if let Some(Some(secs)) = wanted.poll_secs {
        if secs != 0 && !(MIN_POLL_SECS..=MAX_POLL_SECS).contains(&secs) {
            return refused(format!(
                "poll_secs is 0 for never, {MIN_POLL_SECS} to {MAX_POLL_SECS}, or null for \
                 the default interval"
            ));
        }
    }
    let actor = caller.actor();
    let connected = match st.db.find_repo(host, name).await {
        Ok(found) => found,
        Err(e) => return database_error(e).1,
    };
    if connected.is_some() {
        if wanted.forge.is_some() {
            return refused(
                "forge is said when a repository is connected, and not changed after".to_string(),
            );
        }
        let changed = st
            .db
            .change_repo(
                host,
                name,
                wanted.workflow.as_ref().map(|w| w.as_deref()),
                wanted.cell.as_deref(),
                wanted.poll_secs,
                wanted.mode,
                &actor,
            )
            .await;
        let changes = match changed {
            Ok(Some(changes)) => changes,
            // Disconnected between the look and the write.
            Ok(None) => return unknown(&spec),
            Err(e) => return database_error(e).1,
        };
        return match st.db.find_repo(host, name).await {
            Ok(Some(repo)) => {
                if changes {
                    tracing::info!(repo = %spec, actor = %actor, "a repository was changed");
                    crate::audit::record(st.audit.as_ref(), || {
                        crate::audit::Entry::new(crate::audit::kind::REPO_CHANGED)
                            .actor(Some(&actor))
                            .repo(host, name)
                            .with("action", "changed")
                            .with("mode", repo.mode.as_str())
                            .with("cell", repo.cell.as_str())
                            .with("workflow", repo.workflow.clone())
                    });
                }
                let mut body = shown(&repo);
                body["action"] = json!(if changes { "changed" } else { "unchanged" });
                (StatusCode::OK, Json(body)).into_response()
            }
            Ok(None) => unknown(&spec),
            Err(e) => database_error(e).1,
        };
    }
    // Connected again, a repository is on the forge it was on, unless this
    // says otherwise.
    let forge = match wanted.forge {
        Some(forge) => forge,
        None => match st.db.repo_any(host, name).await {
            Ok(was) => was.map_or_else(|| "github".to_string(), |(repo, _)| repo.forge),
            Err(e) => return database_error(e).1,
        },
    };
    let cell = wanted.cell.as_deref().unwrap_or("default");
    let workflow = wanted.workflow.as_ref().and_then(|w| w.as_deref());
    let poll = wanted.poll_secs.flatten();
    let mode = wanted.mode.unwrap_or_default();
    let made = match st
        .db
        .connect_repo(&forge, host, name, workflow, cell, poll, mode, &actor)
        .await
    {
        Ok(made) => made,
        Err(e) => return database_error(e).1,
    };
    let (repo, action) = match made {
        Connected::New(repo) => (repo, "connected"),
        Connected::Again(repo) => (repo, "reconnected"),
        // Connected by another caller between the look and the write.
        Connected::Already => {
            return problem(
                StatusCode::CONFLICT,
                "connected meanwhile",
                "the repository was connected by another request; read it and try again",
            )
        }
    };
    tracing::info!(repo = %spec, actor = %actor, action, "a repository was connected");
    crate::audit::record(st.audit.as_ref(), || {
        crate::audit::Entry::new(crate::audit::kind::REPO_CHANGED)
            .actor(Some(&actor))
            .repo(host, name)
            .with("action", action)
            .with("mode", mode.as_str())
            .with("cell", cell)
            .with("workflow", workflow)
    });
    let mut body = shown(&repo);
    body["action"] = json!(action);
    (StatusCode::CREATED, Json(body)).into_response()
}

/// `DELETE /ci/v1/repos/{host}/{owner}/{name}` (or `{id}`): disconnect the
/// repository. What it had stays.
pub async fn delete(
    State(st): State<AppState>,
    Path(spec): Path<String>,
    headers: HeaderMap,
) -> Response {
    let caller = match admin(&st, &headers, true).await {
        Ok(caller) => caller,
        Err(response) => return response,
    };
    let (host, name) = match named(&st, &spec).await {
        Ok(named) => named,
        Err(response) => return response,
    };
    let (host, name) = (host.as_str(), name.as_str());
    let actor = caller.actor();
    match st.db.disconnect_repo(host, name, &actor).await {
        Ok(Some(id)) => {
            tracing::info!(repo = %spec, actor = %actor, "a repository was disconnected");
            crate::audit::record(st.audit.as_ref(), || {
                crate::audit::Entry::new(crate::audit::kind::REPO_CHANGED)
                    .actor(Some(&actor))
                    .repo(host, name)
                    .with("action", "disconnected")
            });
            (
                StatusCode::OK,
                Json(json!({
                    "id": id,
                    "repo": format!("{host}/{name}").to_ascii_lowercase(),
                    "action": "disconnected",
                })),
            )
                .into_response()
        }
        Ok(None) => problem(
            StatusCode::NOT_FOUND,
            "not connected",
            format!("{spec:?} is not a repository this dagron-ci serves"),
        ),
        Err(e) => database_error(e).1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_path_names_a_repository_by_its_host_and_its_name() {
        assert_eq!(
            split("github.com/acme/widgets"),
            Some(("github.com", "acme/widgets"))
        );
        assert_eq!(
            split("gitlab.example/group/sub/project"),
            Some(("gitlab.example", "group/sub/project"))
        );
        // A host with a port, as `dagron-ci repo add` takes one.
        let ported = "ghe.example:8443/acme/widgets.js";
        assert_eq!(split(ported), Some(("ghe.example:8443", "acme/widgets.js")));
        assert_eq!(
            split(ported),
            crate::cli::split_spec(ported).ok(),
            "as the command reads it"
        );
        for bad in [
            "github.com/acme",
            "github.com",
            "",
            "github.com//widgets",
            "github.com/acme/..",
            "github.com/acme/wid gets",
            "github.com/acme/widgets?x=1",
            "/acme/widgets",
            "ghe.example:/acme/widgets",
            "ghe.example:84a3/acme/widgets",
            ":8443/acme/widgets",
        ] {
            assert_eq!(split(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn an_id_is_one_as_dagron_ci_writes_it() {
        let id = uuid::Uuid::now_v7().to_string();
        assert!(is_id(&id));
        assert_eq!(split(&id), None, "and no name");
        for bad in [
            id.to_ascii_uppercase(),
            id.replace('-', ""),
            format!("{{{id}}}"),
            format!("urn:uuid:{id}"),
            "github.com".to_string(),
            String::new(),
        ] {
            assert!(!is_id(&bad), "{bad:?}");
        }
    }

    #[test]
    fn a_key_that_is_null_is_a_value_and_one_that_is_absent_is_left() {
        let wanted: Wanted = serde_json::from_str(r#"{"workflow": null, "poll_secs": 0}"#).unwrap();
        assert_eq!(wanted.workflow, Some(None));
        assert_eq!(wanted.poll_secs, Some(Some(0)));
        let wanted: Wanted = serde_json::from_str(r#"{"cell": "eu"}"#).unwrap();
        assert_eq!((wanted.workflow, wanted.poll_secs), (None, None));
        assert!(serde_json::from_str::<Wanted>(r#"{"mode": "push"}"#).is_err());
        let wanted: Wanted = serde_json::from_str(r#"{"mode": "fleet"}"#).unwrap();
        assert_eq!(wanted.mode, Some(Mode::Fleet));
        assert!(serde_json::from_str::<Wanted>(r#"{"poll_secs": -1}"#).is_err());
    }
}
