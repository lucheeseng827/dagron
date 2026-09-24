//! First-class workflows: named, reusable DAG definitions managed via the UI.
//!
//! Distinct from the engine's per-run `workflow_definitions`. A `workflow` row is
//! authored/edited here; "running" it submits its `spec` through the same
//! create_run path the submit endpoint uses, producing an ordinary run. The
//! engine never reads this table.

use std::collections::{BTreeMap, HashMap};

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::AuthUser;
use crate::routes::control;
use crate::state::AppState;

#[derive(Serialize, sqlx::FromRow)]
pub struct Workflow {
    pub id: String,
    pub name: String,
    pub spec: String,
    pub description: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    /// Lifecycle: active / paused / retired. Distinct from a *schedule* being
    /// disabled — this stops the workflow however many schedules it has, and
    /// leaves them all intact.
    #[sqlx(default)]
    pub state: String,
    /// Current definition version; `workflow_versions` holds the history.
    #[sqlx(default)]
    pub version: i64,
    /// Organizational labels parsed from the spec (#26). `#[sqlx(default)]` so the
    /// row query (which doesn't select a `tags` column) maps; set from the spec.
    #[sqlx(default)]
    pub tags: Vec<String>,
}

#[derive(Deserialize)]
pub struct UpsertBody {
    pub name: Option<String>,
    pub spec: String,
    #[serde(default)]
    pub description: Option<String>,
}

/// Enriched list row for the Workflows table/board view: definition + its
/// schedule + a digest of its recent runs (last/history/success), all derived
/// from real data. Runs are matched to a workflow by definition name.
#[derive(Serialize)]
pub struct WorkflowRow {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    /// "git" (namespaced name from the operator/GitOps) or "manual".
    pub source: String,
    pub created_at: String,
    pub updated_at: String,
    pub schedule_id: Option<String>,
    pub cron_expr: Option<String>,
    pub next_fire_at: Option<String>,
    /// The *schedule* is disabled. Not the same as the workflow being paused:
    /// this one is per-schedule, `state` below is the whole workflow.
    pub paused: bool,
    pub has_schedule: bool,
    /// Workflow lifecycle: active / paused / retired.
    pub state: String,
    pub last_status: Option<String>,
    pub last_at: Option<String>,
    /// Up to 14 recent run statuses, oldest → newest (for the sparkline).
    pub history: Vec<String>,
    pub success_rate: Option<i64>,
    pub run_count: i64,
    /// Organizational labels declared in the spec (#26).
    pub tags: Vec<String>,
}

/// Query params for `GET /api/workflows`. `?tag=<t>` returns only workflows
/// carrying that tag (#26).
#[derive(Deserialize)]
pub struct ListQuery {
    pub tag: Option<String>,
}

#[derive(sqlx::FromRow)]
struct WfBase {
    id: String,
    name: String,
    spec: String,
    description: Option<String>,
    created_at: String,
    updated_at: String,
    state: String,
}
#[derive(sqlx::FromRow)]
struct SchedRow {
    workflow_id: String,
    schedule_id: String,
    cron_expr: String,
    enabled: i64,
    next_fire_at: Option<String>,
}
#[derive(sqlx::FromRow)]
struct RunStat {
    name: String,
    status: String,
    created_at: String,
}

/// Extract a workflow's `tags` from its stored spec YAML (#26) — a lenient
/// partial parse (empty on error or when none are declared), so no denormalized
/// column has to be kept in sync and tags always reflect the current definition.
fn parse_tags(yaml: &str) -> Vec<String> {
    #[derive(Deserialize)]
    struct TagsOnly {
        #[serde(default)]
        tags: Vec<String>,
    }
    serde_yaml::from_str::<TagsOnly>(yaml).map(|t| t.tags).unwrap_or_default()
}

/// `GET /api/workflows` — enriched rows (definition + schedule + run digest).
pub async fn list_workflows(
    _auth: AuthUser,
    State(state): State<AppState>,
    Query(q): Query<ListQuery>,
) -> Result<Json<Vec<WorkflowRow>>, StatusCode> {
    let wfs = sqlx::query_as::<_, WfBase>(
        "SELECT id, name, spec, description, created_at, updated_at, state
         FROM workflows ORDER BY name",
    )
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;

    // Ordered by created_at so that, when a workflow has more than one schedule,
    // selection below is deterministic (the oldest wins) rather than relying on
    // arbitrary row order.
    let scheds = sqlx::query_as::<_, SchedRow>(
        "SELECT workflow_id, id AS schedule_id, cron_expr, enabled, next_fire_at
         FROM schedules ORDER BY created_at",
    )
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;

    // Last 14 runs per workflow name (window function), newest first.
    let runs = sqlx::query_as::<_, RunStat>(
        "SELECT name, status, created_at FROM (
            SELECT d.name AS name, r.status AS status, r.created_at AS created_at,
                   row_number() OVER (PARTITION BY d.name ORDER BY r.created_at DESC) AS rn
            FROM workflow_runs r JOIN workflow_definitions d ON d.id = r.definition_id
         ) t WHERE rn <= 14
         ORDER BY name, created_at DESC",
    )
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;

    // Pre-index so the per-workflow loop is O(1) lookups instead of rescanning
    // the full vectors. `or_insert` keeps the first (oldest) schedule per
    // workflow, matching the ORDER BY above.
    let mut sched_by_workflow: HashMap<&str, &SchedRow> = HashMap::new();
    for s in &scheds {
        sched_by_workflow.entry(s.workflow_id.as_str()).or_insert(s);
    }
    let mut runs_by_name: HashMap<&str, Vec<&RunStat>> = HashMap::new();
    for r in &runs {
        runs_by_name.entry(r.name.as_str()).or_default().push(r);
    }

    let mut rows = Vec::with_capacity(wfs.len());
    for w in wfs {
        // Tags are parsed from the stored spec (no denormalized column to keep in
        // sync), so they always reflect the current definition (#26).
        let tags = parse_tags(&w.spec);
        let sched = sched_by_workflow.get(w.id.as_str()).copied();
        // runs for this workflow name, newest first
        let mine: Vec<&RunStat> = runs_by_name.get(w.name.as_str()).cloned().unwrap_or_default();
        let total = mine.len() as i64;
        let succeeded = mine.iter().filter(|r| r.status == "succeeded").count() as i64;
        let success_rate = if total > 0 { Some((succeeded * 100) / total) } else { None };
        // oldest → newest for the left-to-right sparkline
        let history: Vec<String> = mine.iter().rev().map(|r| r.status.clone()).collect();
        let last = mine.first();

        rows.push(WorkflowRow {
            source: if w.name.contains('/') { "git".into() } else { "manual".into() },
            schedule_id: sched.map(|s| s.schedule_id.clone()),
            cron_expr: sched.map(|s| s.cron_expr.clone()),
            next_fire_at: sched.and_then(|s| s.next_fire_at.clone()),
            paused: sched.map(|s| s.enabled == 0).unwrap_or(false),
            has_schedule: sched.is_some(),
            last_status: last.map(|r| r.status.clone()),
            last_at: last.map(|r| r.created_at.clone()),
            history,
            success_rate,
            run_count: total,
            tags,
            id: w.id,
            name: w.name,
            description: w.description,
            created_at: w.created_at,
            updated_at: w.updated_at,
            state: w.state,
        });
    }
    // Optional tag filter (#26): keep only workflows carrying the requested tag.
    if let Some(tag) = q.tag.as_deref().map(str::trim).filter(|t| !t.is_empty()) {
        rows.retain(|r| r.tags.iter().any(|t| t == tag));
    }
    Ok(Json(rows))
}

/// `GET /api/workflows/:id` — full workflow incl. spec.
pub async fn get_workflow(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Workflow>, StatusCode> {
    let mut wf = sqlx::query_as::<_, Workflow>(
        "SELECT id, name, spec, description, created_at, updated_at, state, version
         FROM workflows WHERE id = $1",
    )
    .bind(&id)
    .fetch_optional(&state.read_pool)
    .await
    .map_err(internal)?
    .ok_or(StatusCode::NOT_FOUND)?;
    wf.tags = parse_tags(&wf.spec);
    Ok(Json(wf))
}

/// `POST /api/workflows` — create. Validates the DAG (cycle/dup/unknown-dep) and
/// derives the name from the spec unless one is given. 409 on duplicate name.
pub async fn create_workflow(
    _auth: AuthUser,
    State(state): State<AppState>,
    Json(body): Json<UpsertBody>,
) -> Result<(StatusCode, Json<Workflow>), (StatusCode, String)> {
    let spec = control::parse_and_validate(&body.spec)?;
    let name = body.name.unwrap_or(spec.name);
    let description = body.description.filter(|d| !d.trim().is_empty());
    let now = chrono::Utc::now().to_rfc3339();
    let id = Uuid::new_v4().to_string();

    // One transaction: a workflow row must never exist without its version-1
    // history row backing it, so a client is never told "version 1" when
    // `workflow_versions` has nothing to show for it.
    let mut tx = state
        .write_pool
        .begin()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    sqlx::query(
        "INSERT INTO workflows (id, name, spec, description, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$5)",
    )
    .bind(&id)
    .bind(&name)
    .bind(&body.spec)
    .bind(&description)
    .bind(&now)
    .execute(&mut *tx)
    .await
    .map_err(|e| dup_or_internal(e, &name))?;

    // Version 1, so history starts at creation rather than at the first edit —
    // otherwise the original definition is the one version nobody can recover.
    let version = crate::routes::lifecycle::record_version(
        &mut tx,
        &id,
        &name,
        &body.spec,
        Some(&_auth.0.email),
    )
    .await
    .map_err(|e| {
        tracing::error!(workflow_id = %id, error = %e, "failed to record initial workflow version");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to record initial workflow version".to_string(),
        )
    })?;

    tx.commit()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    Ok((
        StatusCode::CREATED,
        Json(Workflow {
            tags: parse_tags(&body.spec),
            id,
            name,
            spec: body.spec,
            description,
            created_at: now.clone(),
            updated_at: now,
            state: "active".to_string(),
            version,
        }),
    ))
}

/// Error type for the handlers that can also refuse with a JSON body (`409` on a git-managed
/// workflow). The existing `(StatusCode, String)` / `StatusCode` errors convert, so their `?`s are untouched.
pub struct ApiErr(Response);

impl IntoResponse for ApiErr {
    fn into_response(self) -> Response {
        self.0
    }
}

impl From<(StatusCode, String)> for ApiErr {
    fn from(e: (StatusCode, String)) -> Self {
        ApiErr(e.into_response())
    }
}

impl From<StatusCode> for ApiErr {
    fn from(e: StatusCode) -> Self {
        ApiErr(e.into_response())
    }
}

/// Deleting a workflow is admin-only. It is the one irreversible control action here: the row
/// goes, and the delete cascades to the workflow's schedules, so an operator who wanted to stop
/// a workflow and reached for this loses the schedules too. The reversible path they actually
/// want, `POST /api/workflows/{id}/state {"state":"retired"}`, stays open to any session — it
/// stops the workflow running and *keeps* the schedules, which is what `docs/API.md` has
/// recommended over delete all along.
///
/// Creating and updating stay open as well: those are an operator's job, and a bad edit is
/// recoverable from `/versions`. This gates the one action that is not.
fn require_admin(claims: &crate::auth::SessionClaims) -> Result<(), ApiErr> {
    if crate::auth::is_admin(claims) {
        return Ok(());
    }
    // JSON, not `(StatusCode, String)`: that renders a plain-text body, and `docs/API.md` states
    // the error envelope is `{"error": "<message>"}` — which the `409` this handler can also
    // return already honours. One route answering two refusals in two shapes is a client bug
    // waiting to happen.
    let body = serde_json::json!({
        "error": "admin group required to delete a workflow; \
                  POST /api/workflows/{id}/state {\"state\":\"retired\"} stops it without losing its schedules",
    });
    Err(ApiErr((StatusCode::FORBIDDEN, Json(body)).into_response()))
}

/// `?force=true` on `PUT` / `DELETE /api/workflows/:id`.
#[derive(Deserialize, Default)]
pub struct ForceQuery {
    #[serde(default)]
    pub force: bool,
}

/// A workflow synced from a git repo is edited in git: refuse with `409` (the body names the repo and
/// the `sync-to-git` route that turns a UI edit into a PR). An admin may override with `?force=true`;
/// the forced edit shows up as `drift` until the next sync puts git's definition back.
async fn refuse_if_managed(
    pool: &sqlx::PgPool,
    id: &str,
    claims: &crate::auth::SessionClaims,
    force: bool,
) -> Result<(), ApiErr> {
    let repo: Option<String> = sqlx::query_scalar(
        "SELECT r.url FROM workflows w JOIN git_repos r ON r.id = w.managed_by WHERE w.id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(internal)?;
    let Some(url) = repo else { return Ok(()) };
    if force && claims.groups.iter().any(|g| g == "admin") {
        return Ok(());
    }
    let body = serde_json::json!({
        "error": format!(
            "this workflow is managed by the git repository {url}: change it there. \
             POST /api/workflows/{id}/sync-to-git opens a pull request from the console; \
             an admin can override with ?force=true (git wins again at the next sync)"
        ),
        "repo": url,
        "sync_to_git": format!("/api/workflows/{id}/sync-to-git"),
    });
    Err(ApiErr((StatusCode::CONFLICT, Json(body)).into_response()))
}

/// `PUT /api/workflows/:id` - update spec (+ optional rename). Re-validates. `409` on a git-managed
/// workflow (see [`refuse_if_managed`]).
pub async fn update_workflow(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<ForceQuery>,
    Json(body): Json<UpsertBody>,
) -> Result<Json<Workflow>, ApiErr> {
    refuse_if_managed(&state.write_pool, &id, &auth.0, q.force).await?;
    let spec = control::parse_and_validate(&body.spec)?;
    let name = body.name.unwrap_or(spec.name);
    let description = body.description.filter(|d| !d.trim().is_empty());
    let now = chrono::Utc::now().to_rfc3339();

    let mut tx = state
        .write_pool
        .begin()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    // Lock the row before touching it. Without this, two concurrent edits to
    // the *same* workflow can both read the same `MAX(version)` inside
    // record_version and race: one history insert then loses to the other's
    // UNIQUE (workflow_id, version) and silently drops (best-effort below), and
    // `workflows.version` can end up pointing at a history row that isn't the
    // spec this request just wrote. Locking here makes the second request wait
    // for the first to commit rather than interleave with it.
    let locked: Option<i64> = sqlx::query_scalar("SELECT version FROM workflows WHERE id = $1 FOR UPDATE")
        .bind(&id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
    if locked.is_none() {
        return Err((StatusCode::NOT_FOUND, format!("workflow '{id}' not found")).into());
    }

    sqlx::query(
        "UPDATE workflows SET name = $1, spec = $2, description = $3, updated_at = $4 WHERE id = $5",
    )
    .bind(&name)
    .bind(&body.spec)
    .bind(&description)
    .bind(&now)
    .bind(&id)
    .execute(&mut *tx)
    .await
    .map_err(|e| dup_or_internal(e, &name))?;

    // History, written after the update succeeds, in a savepoint rather than
    // the outer transaction directly: best-effort still applies here — losing
    // a history row must not fail an edit the user already made and can see —
    // but a plain swallowed error would otherwise leave the outer transaction
    // aborted (Postgres poisons a transaction on any failed statement), taking
    // the update above down with it. The savepoint contains the damage to just
    // this step. Sharing the row lock from above is what makes the write here
    // race-free, not the savepoint.
    let mut sp = sqlx::Acquire::begin(&mut tx)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
    match crate::routes::lifecycle::record_version(&mut sp, &id, &name, &body.spec, Some(&auth.0.email)).await {
        Ok(_) => {
            sp.commit()
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
        }
        Err(e) => {
            tracing::error!(workflow_id = %id, error = %e, "failed to record workflow version");
            sp.rollback()
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
        }
    }

    // Read back state and version rather than assuming: record_version above
    // has just bumped version, and an edit must not silently reactivate a
    // workflow someone paused.
    let (created_at, wf_state, version): (String, String, i64) =
        sqlx::query_as("SELECT created_at, state, version FROM workflows WHERE id = $1")
            .bind(&id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    tx.commit()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    Ok(Json(Workflow {
        tags: parse_tags(&body.spec),
        id,
        name,
        spec: body.spec,
        description,
        created_at,
        updated_at: now,
        state: wf_state,
        version,
    }))
}

/// `DELETE /api/workflows/:id`. 404 if absent; `409` on a git-managed workflow (see
/// [`refuse_if_managed`]).
pub async fn delete_workflow(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<ForceQuery>,
) -> Result<StatusCode, ApiErr> {
    require_admin(&auth.0)?;
    refuse_if_managed(&state.write_pool, &id, &auth.0, q.force).await?;
    let n = sqlx::query("DELETE FROM workflows WHERE id = $1")
        .bind(&id)
        .execute(&state.write_pool)
        .await
        .map_err(internal)?
        .rows_affected();
    if n == 0 {
        Err(StatusCode::NOT_FOUND.into())
    } else {
        Ok(StatusCode::NO_CONTENT)
    }
}

/// Optional body for [`run_workflow`].
#[derive(Deserialize, Default)]
pub struct RunWorkflowBody {
    /// Arguments for the stored spec's declared `parameters:`.
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
}

#[cfg(test)]
mod delete_authz_tests {
    use super::*;

    fn claims(groups: &[&str]) -> crate::auth::SessionClaims {
        crate::auth::SessionClaims {
            sub: "u1".to_string(),
            email: "u1@example.com".to_string(),
            name: "U1".to_string(),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            exp: 0,
        }
    }

    /// `delete_workflow` gates on this. The console's "operator" is a user with *no* groups, so
    /// the empty case is the one that matters: it is what used to delete a workflow, schedules
    /// and all. The body names `state: retired` because refusing without pointing at the
    /// reversible path would just read as the API being broken.
    #[tokio::test]
    async fn deleting_a_workflow_requires_the_admin_group() {
        for denied in [&[][..], &["viewer"][..], &["operator"][..], &["admins"][..]] {
            let err = require_admin(&claims(denied)).expect_err("must be refused");
            let res = err.into_response();
            assert_eq!(res.status(), StatusCode::FORBIDDEN, "groups {denied:?}");
            let body = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
            // The documented envelope, not a plain-text body: `{"error": "<message>"}`.
            let body: serde_json::Value = serde_json::from_slice(&body).expect("a JSON envelope");
            let msg = body["error"].as_str().expect("an `error` string");
            assert!(msg.contains("retired"), "the refusal must name the reversible path: {msg}");
            assert!(!msg.contains("  "), "no run of spaces from a continued literal: {msg}");
        }
        // `ApiErr` wraps a `Response` and has no `Debug`, so the ok side is asserted not unwrapped.
        assert!(require_admin(&claims(&["admin"])).is_ok(), "the admin group must pass");
        assert!(
            require_admin(&claims(&["viewer", "admin"])).is_ok(),
            "admin among other groups must pass"
        );
    }
}

#[cfg(test)]
mod run_workflow_body_tests {
    use super::RunWorkflowBody;

    /// The route took no body at all until parameters were added, so an absent
    /// body must stay valid. `Option<Json<_>>` covers the no-`Content-Type`
    /// case; this covers a client that sends `{}` instead.
    #[test]
    fn empty_object_body_means_no_parameters() {
        let b: RunWorkflowBody = serde_json::from_str("{}").unwrap();
        assert!(b.parameters.is_empty());
    }

    #[test]
    fn parameters_are_read_when_present() {
        let b: RunWorkflowBody =
            serde_json::from_str(r#"{"parameters":{"region":"ap-southeast-1"}}"#).unwrap();
        assert_eq!(b.parameters.get("region").map(String::as_str), Some("ap-southeast-1"));
    }
}

/// `POST /api/workflows/:id/run` — submit the stored spec as a new run, with
/// optional `{ "parameters": { … } }`.
///
/// The body is optional (`Option<Json<_>>` yields `None` when the request
/// carries no `Content-Type`), so the pre-existing bodyless call is unchanged.
/// That matters: this route took no body at all until now, and every existing
/// caller sends none.
///
/// Being able to pass arguments here is what makes a *stored* workflow callable
/// as a function. Without it a caller that needs to vary anything has to read
/// the spec, splice values in itself, and submit the result as new YAML —
/// which is a different and much broader operation than "run this workflow",
/// both to reason about and to authorize.
pub async fn run_workflow(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Option<Json<RunWorkflowBody>>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, String)> {
    // A paused or retired workflow refuses to run here as well as in the
    // scheduler. Enforcing it in only one of the two would mean "paused" stops
    // cron but not a button, which is not what anyone reads it to mean.
    let row: Option<(String, String)> =
        sqlx::query_as("SELECT spec, state FROM workflows WHERE id = $1")
            .bind(&id)
            .fetch_optional(&state.read_pool)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
    if let Some((_, wf_state)) = &row {
        if !crate::routes::lifecycle::is_runnable(wf_state) {
            return Err((
                StatusCode::CONFLICT,
                format!("workflow '{id}' is {wf_state} — set it active before running it"),
            ));
        }
    }
    let spec_yaml = row
        .map(|(spec, _)| spec)
        .ok_or((StatusCode::NOT_FOUND, format!("workflow '{id}' not found")))?;

    control::parse_and_validate(&spec_yaml)?;
    // Same pipeline as `POST /api/runs`: `workflow_ref` chains inlined, then the
    // engine's own parser and run writer — now with the caller's arguments fed
    // in as parameter overrides, so substitution happens once, in the engine.
    let params = body.map(|Json(b)| b.parameters).unwrap_or_default();
    let run_id =
        control::submit_yaml_with_params(&state, &spec_yaml, &spec_yaml, &params).await?;
    // `201`, not `200`. This route creates a run, and every other route that
    // creates one already says so — `POST /api/runs`, `/resubmit`. The odd one
    // out was not merely inconsistent: a layer in front that counts created
    // runs by status (a proxy, a quota, a meter) saw a creation it could not
    // distinguish from a read, so runs submitted by name went uncounted.
    // Clients check 2xx, so nothing that works today stops working.
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "run_id": run_id, "workflow_id": id })),
    ))
}

#[derive(Deserialize)]
pub struct WorkflowRunsParams {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// `GET /api/workflows/:id/runs?limit=&offset=` — this workflow's run history,
/// newest first. Runs are matched by definition **name** — the only linkage
/// that exists: each run snapshots its own `workflow_definitions` row (a fresh
/// id per run), so there is no FK from runs to the `workflows` table, and the
/// list digest in `list_workflows` uses the same name rule. Consequence: a
/// renamed workflow starts a fresh history (documented in API.md).
/// Backs the read-oriented workflow detail page.
pub async fn workflow_runs(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
    axum::extract::Query(params): axum::extract::Query<WorkflowRunsParams>,
) -> Result<Json<Vec<crate::routes::runs::RunSummary>>, (StatusCode, String)> {
    let name: Option<String> = sqlx::query_scalar("SELECT name FROM workflows WHERE id = $1")
        .bind(&id)
        .fetch_optional(&state.read_pool)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
    let name = name.ok_or((StatusCode::NOT_FOUND, format!("workflow '{id}' not found")))?;

    let limit = params.limit.unwrap_or(50).clamp(1, 500);
    let offset = params.offset.unwrap_or(0).max(0);
    let rows = sqlx::query_as::<_, crate::routes::runs::RunSummary>(&format!(
        "{}
         WHERE d.name = $1
         ORDER BY wr.created_at DESC
         LIMIT $2 OFFSET $3",
        crate::routes::runs::SUMMARY_SELECT
    ))
    .bind(&name)
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.read_pool)
    .await
    .map_err(|e| {
        tracing::error!(error = ?e, "workflow runs query failed");
        (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string())
    })?;
    Ok(Json(rows))
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn internal(err: sqlx::Error) -> StatusCode {
    tracing::error!(error = ?err, "db query failed");
    StatusCode::INTERNAL_SERVER_ERROR
}

/// Map a UNIQUE-violation to 409, anything else to 500.
fn dup_or_internal(err: sqlx::Error, name: &str) -> (StatusCode, String) {
    if let sqlx::Error::Database(db) = &err {
        if db.is_unique_violation() {
            return (StatusCode::CONFLICT, format!("workflow name '{name}' already exists"));
        }
    }
    tracing::error!(error = ?err, "db query failed");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string())
}

#[cfg(test)]
mod managed_tests {
    use super::*;
    use crate::routes::gitrepos::managed_tests::{add_repo, add_workflow, test_pool};

    fn who(groups: &[&str]) -> crate::auth::SessionClaims {
        crate::auth::SessionClaims {
            sub: "u".into(),
            email: "u@example.com".into(),
            name: "U".into(),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            exp: usize::MAX,
        }
    }

    /// A git-managed workflow refuses `PUT`/`DELETE` with a 409 naming the repo and `sync-to-git`;
    /// only an admin passing `force` gets through. Unmanaged workflows, and ones whose repo was
    /// disconnected, are untouched by the guard.
    #[tokio::test]
    async fn a_managed_workflow_refuses_edits_unless_an_admin_forces_it() {
        let Some(pool) = test_pool().await else { return };
        let tag = Uuid::new_v4().simple().to_string();
        let repo = add_repo(&pool, &tag).await;
        let managed = add_workflow(&pool, &format!("m-{tag}"), "s", Some(&repo), "s").await;
        let loose = add_workflow(&pool, &format!("l-{tag}"), "s", None, "s").await;
        let orphan = add_workflow(&pool, &format!("o-{tag}"), "s", Some("no-such-repo"), "s").await;

        let refused = refuse_if_managed(&pool, &managed, &who(&["viewer"]), false).await.expect_err("409");
        assert_eq!(refused.0.status(), StatusCode::CONFLICT);
        let body = axum::body::to_bytes(refused.0.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["repo"], format!("https://example.com/{tag}.git"));
        assert_eq!(body["sync_to_git"], format!("/api/workflows/{managed}/sync-to-git"));

        assert!(refuse_if_managed(&pool, &managed, &who(&["viewer"]), true).await.is_err(), "force needs admin");
        assert!(refuse_if_managed(&pool, &managed, &who(&["admin"]), false).await.is_err(), "admin needs force");
        assert!(refuse_if_managed(&pool, &managed, &who(&["admin"]), true).await.is_ok());
        assert!(refuse_if_managed(&pool, &loose, &who(&[]), false).await.is_ok());
        assert!(refuse_if_managed(&pool, &orphan, &who(&[]), false).await.is_ok());
    }
}
