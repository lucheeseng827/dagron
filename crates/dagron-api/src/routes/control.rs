//! Control mutations: cancel a run, retry a dead task, submit a new DAG.
//!
//! These write via `write_pool` and fire `pg_notify('task_events', run_id)` in
//! the same transaction so any open SSE stream (this replica or another) reflects
//! the change immediately.
//!
//! Cancel and submit go through `dagron_core::db` (`cancel_run`, `create_run*`),
//! so a run created or cancelled here is written exactly as the engine writes
//! it: a cancel from either API ends the run the same way, outbox event and
//! all. The retry SQL is still inlined, mirroring engine
//! `db::postgres::retry_task_from_ui`; keep it in sync if the engine schema
//! changes. The submitted task `input` JSON must match the engine's
//! `dag::TaskSpec` shape (see `TaskSpecInput`).

use std::collections::{BTreeMap, HashMap, HashSet};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use petgraph::algo::is_cyclic_directed;
use petgraph::graph::DiGraph;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use dagron_artifact::ArtifactKey;

use crate::auth::AuthUser;
#[cfg(feature = "postgres")]
use crate::routes::artifacts;
use crate::state::AppState;

// ── Cancel ────────────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct CancelResponse {
    /// How many tasks the cancel called off.
    pub cancelled: u64,
    /// How many of them a worker still holds: asked to stop, and `running`
    /// until their work is gone. While this is not zero the run reads
    /// `running` too, and ends `cancelled` once they have stopped.
    pub stopping: u64,
}

/// `POST /api/runs/:id/cancel` — cancel a running run and every task of it
/// still to finish. `cancelled` is how many tasks it called off: 0 for a run
/// already terminal, which this leaves as it is.
///
/// **A cancel asks.** A task that had not started, or was waiting (on an
/// approval, a sensor, a deferred job), is `cancelled` at once. One a worker
/// holds is asked to stop: its engine tells the executor at its next sweep,
/// and the task is `cancelled` when its container, pod or process is gone.
/// `stopping` counts those. The run ends with the last of them, and until
/// then reads `running`, with `stopping_as: "cancelled"` in its detail, and
/// holds its concurrency key. A second cancel in that time changes nothing.
///
/// Through `dagron_core::db::cancel_run`, the engine's own cancel: the run's
/// `run.completed` event (status `cancelled`) is written when the run ends,
/// and the engine's next tick runs the rest of its end (commit status,
/// webhooks, run sink).
pub async fn cancel_run(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<CancelResponse>, StatusCode> {
    // 404 if the run doesn't exist.
    let exists: Option<String> =
        sqlx::query_scalar("SELECT id FROM workflow_runs WHERE id = $1")
            .bind(&id)
            .fetch_optional(&state.write_pool)
            .await
            .map_err(internal)?;
    if exists.is_none() {
        return Err(StatusCode::NOT_FOUND);
    }

    let did = dagron_core::db::cancel_run(&state.write_pool, &id)
        .await
        .map_err(|e| {
            tracing::error!(error = ?e, run_id = %id, "cancel failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .unwrap_or_default();
    Ok(Json(CancelResponse {
        // Called off, whether or not their work has stopped yet: what this
        // field has always counted.
        cancelled: did.cancelled + did.stopping,
        stopping: did.stopping,
    }))
}

// ── Retry ─────────────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct RetryResponse {
    pub retried: bool,
}

/// The answer for a rerun, a retry or a clear that the datastore refused.
///
/// A run that had ended asks its concurrency key before it runs again, as a
/// new run does, and a full key is a `429` that says so, with nothing
/// written. A run rerun whole while a task of it still owes the teardown of a
/// remote job is a `409`, with nothing written: it can be rerun once the
/// engine has torn the job down. Anything else is logged and is the `500` it
/// always was.
fn run_again_refusal(e: anyhow::Error) -> (StatusCode, String) {
    if let Some(t) = e.downcast_ref::<dagron_core::models::TeardownOwed>() {
        return (StatusCode::CONFLICT, t.to_string());
    }
    if let Some(m) = e.downcast_ref::<dagron_core::models::MaxActiveRunsReached>() {
        let scope = m.key.as_deref().map(|k| format!(" and concurrency_key '{k}'")).unwrap_or_default();
        return (
            StatusCode::TOO_MANY_REQUESTS,
            format!(
                "max_active_runs reached for workflow '{}'{scope} ({} active, cap {}): \
                 the run was not set running again",
                m.name, m.active, m.max
            ),
        );
    }
    tracing::error!(error = ?e, "running a run again failed");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal server error".to_string())
}

// A rerun, a retry and a clear are the engine's: `dagron_core::db` has the one
// implementation of each, and its own API calls the same three functions.
// This file carried a copy of each, "kept in sync" by a comment, and the
// copies had drifted: the rerun here left rule-skipped tasks skipped, and none
// of the three cleared a task's fault verdict, its log or its remote job's
// handle. Each now keeps the attempts it replaces, asks the concurrency key
// when it sets an ended run running again, and writes an outbox event.

/// `POST /api/runs/:id/tasks/:tid/retry` — resurrect a failed/cancelled task,
/// and set its run running again if it had ended. 409 if the task isn't in a
/// retryable state; 404 if it doesn't belong to the run; 429 if the run had
/// ended and its concurrency key is full. The caller is recorded on the task
/// as who put it back (`reset_by`): a gate it puts back to ask again takes no
/// approval from them where it is `not_triggerer`.
pub async fn retry_task(
    auth: AuthUser,
    State(state): State<AppState>,
    Path((id, tid)): Path<(String, String)>,
) -> Result<Json<RetryResponse>, (StatusCode, String)> {
    // Scope-check: the task must belong to this run.
    let status: Option<String> = sqlx::query_scalar(
        "SELECT status FROM task_runs WHERE id = $1 AND run_id = $2",
    )
    .bind(&tid)
    .bind(&id)
    .fetch_optional(&state.write_pool)
    .await
    .map_err(internal_msg)?;
    let not_retryable = || {
        (
            StatusCode::CONFLICT,
            format!("task '{tid}' is not in a retryable (failed/cancelled) state"),
        )
    };
    let Some(status) = status else {
        return Err((StatusCode::NOT_FOUND, format!("task '{tid}' not found in run '{id}'")));
    };
    if status != "failed" && status != "cancelled" {
        return Err(not_retryable());
    }

    // The check above is for the answer; the engine's statement is the gate.
    let who = caller_identity(&auth.0);
    let retried = dagron_core::db::retry_task_from_ui_by(&state.write_pool, &tid, Some(&who))
        .await
        .map_err(run_again_refusal)?;
    if !retried {
        return Err(not_retryable());
    }
    tracing::info!(run_id = %id, task_id = %tid, "task retried");
    Ok(Json(RetryResponse { retried: true }))
}

#[derive(Serialize)]
pub struct ClearResponse {
    pub run_id: String,
    pub task_id: String,
    pub cleared: u64,
}

/// `POST /api/runs/:id/tasks/:tid/clear` — clear + downstream: reset a
/// completed task and every terminal task that transitively depends on it to
/// `pending`, recompute `remaining_deps`, and re-arm the run so the reconcile
/// loop re-runs just that sub-DAG. Unlike `retry_task` (a single failed task),
/// this re-runs a still-`succeeded` node on demand and cascades to its
/// downstream. 404 unknown run/task, 409 task not in a completed state, 429 if
/// the run had ended and its concurrency key is full. The caller is recorded
/// on each task put back (`reset_by`), as a retry records them.
pub async fn clear_task(
    auth: AuthUser,
    State(state): State<AppState>,
    Path((id, tid)): Path<(String, String)>,
) -> Result<Json<ClearResponse>, (StatusCode, String)> {
    let who = caller_identity(&auth.0);
    let cleared =
        dagron_core::db::clear_task_with_downstream_by(&state.write_pool, &id, &tid, Some(&who))
            .await
            .map_err(run_again_refusal)?;
    let Some(cleared) = cleared else {
        // The engine guards both under one answer: a task that is not in the
        // run, and one that is not completed. Tell them apart for the caller.
        let known = dagron_core::db::task_exists(&state.write_pool, &id, &tid)
            .await
            .map_err(run_again_refusal)?;
        return Err(if known {
            (
                StatusCode::CONFLICT,
                format!("task '{tid}' is not in a clearable (completed) state"),
            )
        } else {
            (StatusCode::NOT_FOUND, format!("task '{tid}' not found in run '{id}'"))
        });
    };

    tracing::info!(run_id = %id, task_id = %tid, cleared, "task cleared with downstream");
    Ok(Json(ClearResponse { run_id: id, task_id: tid, cleared }))
}

#[derive(Serialize)]
pub struct ApprovalResponse {
    pub run_id: String,
    pub task_id: String,
    pub resolution: String,
    pub decided_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Optional JSON body of approve/reject: why the approver decided as they did.
#[derive(Debug, Default, Deserialize)]
pub struct DecisionBody {
    #[serde(default)]
    pub comment: Option<String>,
    /// `<task>/<name>` → sha256 (hex) of each of the gate's `binds` as the
    /// approver reviewed it; required to approve a gate that has `binds`.
    #[serde(default)]
    pub digests: BTreeMap<String, String>,
}

/// `POST /api/runs/:id/tasks/:tid/approve` — approve a `type: approval` gate (#19):
/// the task succeeds and its dependents advance. Mirrors engine
/// `db::resolve_approval`. 404 unknown run/task, 409 not awaiting approval.
/// The authenticated user is recorded as the decider.
pub async fn approve_task(
    auth: AuthUser,
    State(state): State<AppState>,
    Path((id, tid)): Path<(String, String)>,
    body: Option<Json<DecisionBody>>,
) -> Result<Json<ApprovalResponse>, (StatusCode, String)> {
    resolve_approval(&state, &id, &tid, true, &auth, body.map(|b| b.0)).await
}

/// `POST /api/runs/:id/tasks/:tid/reject` — reject a gate: the task fails and its
/// `all_success` dependents skip. Same status codes as approve.
pub async fn reject_task(
    auth: AuthUser,
    State(state): State<AppState>,
    Path((id, tid)): Path<(String, String)>,
    body: Option<Json<DecisionBody>>,
) -> Result<Json<ApprovalResponse>, (StatusCode, String)> {
    resolve_approval(&state, &id, &tid, false, &auth, body.map(|b| b.0)).await
}

/// The slice of a stored TaskSpec that decides who may resolve its gate.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct GateAuthz {
    #[serde(default)]
    pub(crate) approvers: Vec<String>,
    #[serde(default)]
    pub(crate) not_triggerer: bool,
    #[serde(default)]
    pub(crate) binds: Vec<String>,
}

/// Check the digests an approver sent against the bound artifacts as they are
/// now, and return the `approved.sha256` document to publish (in `sha256sum -c`
/// format, paths relative to `$DAGRON_ARTIFACTS`). Nothing is approved on a
/// missing digest, a missing artifact, or a mismatch — the plan the approver
/// read is not necessarily the plan that is there now.
async fn check_bound_digests(
    state: &AppState,
    run_id: &str,
    gate: &str,
    binds: &[String],
    sent: &BTreeMap<String, String>,
) -> Result<(ArtifactKey, String), (StatusCode, String)> {
    let missing: Vec<&str> = binds
        .iter()
        .filter(|b| !sent.contains_key(b.as_str()))
        .map(String::as_str)
        .collect();
    if !missing.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "this gate binds its approval to artifacts, so approve it from the Approvals page, or send \"digests\" with the sha256 you reviewed for: {} (GET /api/approvals lists them)",
                missing.join(", ")
            ),
        ));
    }
    // A one-node install (SQLite) keeps no artifact index, so an approval bound
    // to artifacts cannot be checked there. The return
    // is the function's end only in this build.
    #[cfg(feature = "sqlite")]
    #[allow(clippy::needless_return)]
    {
        let _ = (state, run_id, gate, sent);
        return Err((
            StatusCode::NOT_IMPLEMENTED,
            "this gate binds its approval to artifacts, which dagron-api's Postgres build checks; a one-node (SQLite) install cannot"
                .to_string(),
        ));
    }
    #[cfg(feature = "postgres")]
    let mut lines = String::new();
    #[cfg(feature = "postgres")]
    for bind in binds {
        let Some((task, name)) = bind.split_once('/') else {
            tracing::error!(%bind, "stored bind is not <task>/<name>");
            return Err((StatusCode::INTERNAL_SERVER_ERROR, "internal server error".to_string()));
        };
        let key = ArtifactKey::new(run_id, task, name);
        let now = match artifacts::artifact_sha256(state, &key).await {
            Ok(Some(d)) => d,
            Ok(None) => {
                return Err((
                    StatusCode::CONFLICT,
                    format!("bound artifact '{bind}' does not exist in this run, so there is nothing to approve"),
                ))
            }
            Err(artifacts::DigestError::NoStore) => {
                return Err((
                    StatusCode::SERVICE_UNAVAILABLE,
                    "this gate binds its approval to artifacts but no artifact store is configured (DAGRON_ARTIFACT_DIR)".to_string(),
                ))
            }
            Err(artifacts::DigestError::Failed) => {
                return Err((StatusCode::INTERNAL_SERVER_ERROR, "internal server error".to_string()))
            }
        };
        let reviewed = sent[bind].trim().to_ascii_lowercase();
        if reviewed != now {
            return Err((
                StatusCode::CONFLICT,
                format!("'{bind}' changed since you reviewed it (sha256 is now {now}); review it again before approving"),
            ));
        }
        lines.push_str(&format!(
            "{now}  {}/{}\n",
            dagron_artifact::sanitize_component(task),
            dagron_artifact::sanitize_component(name)
        ));
    }
    #[cfg(feature = "postgres")]
    Ok((ArtifactKey::new(run_id, gate, APPROVED_DIGESTS_FILE), lines))
}

/// The artifact a gate with `binds` publishes on approval, under the gate's own
/// task name: `$DAGRON_ARTIFACTS/<gate>/approved.sha256`.
pub(crate) const APPROVED_DIGESTS_FILE: &str = "approved.sha256";

/// Who started a run, for a rule that reads it (`not_triggerer`), over
/// `workflow_runs` under the alias given, left-joined to `run_triggers rt`.
///
/// The actor of the run's trigger record, where it has one: the person who
/// asked, a pusher, a pull request's author, and for a run a parent's task
/// started, the parent's. A record with no actor names nobody, and bars
/// nobody. A run with no record predates it, and is its submitter's
/// (`triggered_by`), as it was.
///
/// It was `triggered_by` alone, which for a run a CI service submitted is the
/// service: the rule compared an approver with the service's own token and
/// never with whoever pushed. And a sub-workflow's run had no submitter at
/// all, so a gate in one barred nobody, whoever had started its parent.
macro_rules! triggerer_sql {
    ($run:literal) => {
        concat!(
            "CASE WHEN rt.run_id IS NOT NULL THEN rt.actor ELSE ",
            $run,
            ".triggered_by END"
        )
    };
}
pub(crate) use triggerer_sql;

async fn resolve_approval(
    state: &AppState,
    id: &str,
    tid: &str,
    approve: bool,
    auth: &AuthUser,
    body: Option<DecisionBody>,
) -> Result<Json<ApprovalResponse>, (StatusCode, String)> {
    let digests = body.as_ref().map(|b| b.digests.clone()).unwrap_or_default();
    let comment = body
        .and_then(|b| b.comment)
        .map(|c| c.trim().to_string())
        .filter(|c| !c.is_empty());
    if let Some(c) = &comment {
        if c.chars().count() > dagron_core::dag::APPROVAL_TEXT_MAX {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("comment exceeds {} characters", dagron_core::dag::APPROVAL_TEXT_MAX),
            ));
        }
    }
    let claims = &auth.0;
    let decided_by = caller_identity(claims);

    // Who may decide. Read from the primary (a replica can lag a fresh run) and
    // from the stored TaskSpec, which no request can edit.
    // `(input, triggered_by, name, reset_by)`: `reset_by` is who put the gate
    // back to ask again, if anyone did.
    type Gate = (Option<String>, Option<String>, String, Option<String>);
    let gate: Option<Gate> = sqlx::query_as(concat!(
        "SELECT t.input, ",
        triggerer_sql!("r"),
        ", t.name, t.reset_by FROM task_runs t
           JOIN workflow_runs r ON r.id = t.run_id
           LEFT JOIN run_triggers rt ON rt.run_id = r.id
          WHERE t.id = $1 AND t.run_id = $2",
    ))
    .bind(tid)
    .bind(id)
    .fetch_optional(&state.write_pool)
    .await
    .map_err(internal_msg)?;
    // sha256 lines to publish for downstream tasks once the approval lands.
    let mut approved_digests: Option<(ArtifactKey, String)> = None;
    if let Some((input, triggered_by, gate_name, reset_by)) = gate {
        let authz: GateAuthz = match input.as_deref() {
            Some(s) => serde_json::from_str(s).map_err(|e| {
                tracing::error!(error = ?e, task_id = %tid, "unreadable stored task spec");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal server error".to_string())
            })?,
            None => GateAuthz::default(),
        };
        if !dagron_core::dag::approver_permits(&authz.approvers, &claims.email, &claims.sub, &claims.groups) {
            return Err((
                StatusCode::FORBIDDEN,
                format!("you are not among this gate's approvers ({})", authz.approvers.join(", ")),
            ));
        }
        if approve && authz.not_triggerer {
            let is_me = |who: Option<&str>| {
                who.is_some_and(|t| {
                    [claims.email.as_str(), claims.sub.as_str()]
                        .iter()
                        .any(|me| !me.is_empty() && me.eq_ignore_ascii_case(t))
                })
            };
            if is_me(triggered_by.as_deref()) {
                return Err((
                    StatusCode::FORBIDDEN,
                    "this gate is not_triggerer: the person who started the run may not approve it"
                        .to_string(),
                ));
            }
            // Nor may whoever put the gate back to ask again, by a rerun, a
            // retry or a clear: else the person who re-armed a gate could
            // approve it, the only person involved.
            if is_me(reset_by.as_deref()) {
                return Err((
                    StatusCode::FORBIDDEN,
                    "this gate is not_triggerer: the person who put it back to ask again may not approve it"
                        .to_string(),
                ));
            }
        }
        if approve && !authz.binds.is_empty() {
            approved_digests =
                Some(check_bound_digests(state, id, &gate_name, &authz.binds, &digests).await?);
        }
    }

    let now = chrono::Utc::now().to_rfc3339();
    let (status, output) =
        if approve { ("succeeded", "approved") } else { ("failed", "rejected") };
    let mut tx = state.write_pool.begin().await.map_err(internal_msg)?;
    let rows = sqlx::query(
        "UPDATE task_runs SET status = $1, finished_at = $2, output = $3,
                decided_by = $4, decision_comment = $5, log = $6
         WHERE id = $7 AND run_id = $8 AND status = 'awaiting_approval'",
    )
    .bind(status)
    .bind(&now)
    .bind(output)
    .bind(&decided_by)
    .bind(comment.as_deref())
    .bind(dagron_core::dag::approval_log(approve, Some(&decided_by), comment.as_deref(), &now))
    .bind(tid)
    .bind(id)
    .execute(&mut *tx)
    .await
    .map_err(internal_msg)?
    .rows_affected();

    if rows == 0 {
        tx.rollback().await.map_err(internal_msg)?;
        // Disambiguate: unknown task → 404, existing-but-not-awaiting → 409.
        // `CAST(1 AS BIGINT)` — a bare literal is INT4 and will not decode into i64, so
        // this disambiguation would 500 exactly when the task DOES exist.
        let known: Option<i64> =
            sqlx::query_scalar("SELECT CAST(1 AS BIGINT) FROM task_runs WHERE id = $1 AND run_id = $2")
                .bind(tid)
                .bind(id)
                .fetch_optional(&state.read_pool)
                .await
                .map_err(internal_msg)?;
        return if known.is_some() {
            Err((StatusCode::CONFLICT, format!("task '{tid}' is not awaiting approval")))
        } else {
            Err((StatusCode::NOT_FOUND, format!("task '{tid}' not found in run '{id}'")))
        };
    }

    // Terminal transition → decrement dependents so their trigger_rule evaluates.
    sqlx::query(
        "UPDATE task_runs SET remaining_deps = remaining_deps - 1
         WHERE id IN (
             SELECT dependent_id FROM task_dependencies WHERE dependency_id = $1
         ) AND status = 'pending'",
    )
    .bind(tid)
    .execute(&mut *tx)
    .await
    .map_err(internal_msg)?;

    // Publish what was approved while the gate row is still locked: nothing
    // downstream can start before the commit, and a failed write drops the
    // transaction (rolling the approval back) rather than leaving an approval
    // that downstream tasks cannot verify. The put precedes the commit, so a
    // failed commit can leave this file behind while the gate stays pending: its
    // presence alone does not prove an approval (the next approval overwrites it).
    // Never set on SQLite, whose check above refuses an artifact-bound gate.
    #[cfg(feature = "sqlite")]
    let _ = &approved_digests;
    #[cfg(feature = "postgres")]
    if let Some((key, lines)) = &approved_digests {
        // Recorded as any artifact is, so that it is listed with the run's
        // and expires when they do.
        let put = crate::routes::artifact_index::put_recorded(state, key, lines.as_bytes()).await;
        if let Err(e) = put {
            tracing::error!(error = %e, "could not publish approved digests");
            return Err((StatusCode::INTERNAL_SERVER_ERROR, "internal server error".to_string()));
        }
    }

    notify(&mut tx, id).await.map_err(|s| (s, "internal server error".to_string()))?;
    tx.commit().await.map_err(internal_msg)?;

    let resolution = if approve { "approved" } else { "rejected" };
    tracing::info!(run_id = %id, task_id = %tid, resolution, "approval gate resolved");
    Ok(Json(ApprovalResponse {
        run_id: id.to_string(),
        task_id: tid.to_string(),
        resolution: resolution.to_string(),
        decided_by,
        comment,
    }))
}

// ── Rerun (cascade resume from failure) ────────────────────────────────────────

#[derive(Debug, Default, Deserialize)]
pub struct RerunBody {
    /// Rerun mode: `failed` (the default), from the failed tasks; or `all`,
    /// the run whole, what succeeded included. `task:<id>` is reserved.
    #[serde(default)]
    from: Option<String>,
    /// Deep-merged into each reset task's stored TaskSpec `input` (behind the
    /// `enterprise` feature). Accepted by all builds so clients get an explicit
    /// 400 rather than a silent no-op when `params` is supplied against a build
    /// without the feature.
    #[serde(default)]
    params: Option<serde_json::Value>,
}

#[derive(Serialize)]
pub struct RerunResponse {
    pub run_id: String,
    pub rerun: u64,
}

/// `POST /api/runs/:id/rerun` — cascade rerun a failed/cancelled run from its
/// failure frontier: every failed, cancelled and rule-skipped task resets to
/// `pending` and the run re-arms, while succeeded tasks stay intact.
/// `remaining_deps` is recomputed so the frontier becomes ready and tasks
/// behind a reset dependency wait for it to re-succeed. It is the engine's
/// rerun (`db::rerun_from_failed_in`), with the optional `params` input
/// override (QW4) written in the same transaction. With `{"from": "all"}`
/// the run is rerun whole (`db::rerun_all_in`): an ended run, one that
/// succeeded too, runs again from its first tasks. 404 unknown / 409 not
/// rerunnable, or rerun whole while a task still owes the teardown of a
/// remote job / 400 bad mode or non-object params / 429 the run's concurrency
/// key is full. The caller is recorded on each task put back (`reset_by`), as
/// a retry records them.
pub async fn rerun_run(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Option<Json<RerunBody>>,
) -> Result<Json<RerunResponse>, (StatusCode, String)> {
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let whole = match body.from.as_deref() {
        None | Some("failed") => false,
        Some("all") => true,
        Some(from) => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("unsupported rerun mode '{from}'; 'failed' and 'all' are supported"),
            ));
        }
    };
    // Non-enterprise builds accept the field (so serde doesn't silently discard
    // it) but must reject it explicitly so callers know params had no effect.
    #[cfg(not(feature = "enterprise"))]
    if body.params.is_some() {
        return Err((
            StatusCode::BAD_REQUEST,
            "`params` rerun override requires the `enterprise` feature".to_string(),
        ));
    }
    #[cfg(feature = "enterprise")]
    if let Some(p) = &body.params {
        if !p.is_object() {
            return Err((StatusCode::BAD_REQUEST, "params must be a JSON object".to_string()));
        }
    }

    // For the answer: which of "no such run" and "not one to rerun" it is.
    // No lock is taken here. The engine's rerun takes the admission lock
    // before any row of the run, and its guarded UPDATE of the run is the
    // gate two reruns serialize on: the loser reads `running` and gets a 409.
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM workflow_runs WHERE id = $1")
            .bind(&id)
            .fetch_optional(&state.write_pool)
            .await
            .map_err(internal_msg)?;
    let not_rerunnable = || {
        let states = if whole {
            "succeeded/failed/cancelled"
        } else {
            "failed/cancelled"
        };
        (
            StatusCode::CONFLICT,
            format!("run '{id}' is not in a rerunnable state ({states})"),
        )
    };
    let Some(status) = status else {
        return Err((StatusCode::NOT_FOUND, format!("run '{id}' not found")));
    };
    if status != "failed" && status != "cancelled" && !(whole && status == "succeeded") {
        return Err(not_rerunnable());
    }

    let mut tx = state.write_pool.begin().await.map_err(internal_msg)?;

    // Behind the `enterprise` feature: capture the broken cone's specs before
    // reset and deep-merge `params` into each task's `input`. The reset never
    // touches `input` (it carries the command), so these per-row overrides are
    // written beside it, once the engine's rerun has set the run running: a
    // run that has ended has no task a worker could move in between.
    //
    // The cone is the one the engine's rerun resets: the failed, the cancelled
    // and the rule-skipped; and for a whole rerun, every task but a runtime
    // fan-out's instances, which it drops. A task left out here runs again on
    // the input it had, beside siblings that were given the new one.
    #[cfg(feature = "enterprise")]
    let overrides: Vec<(String, String)> = if let Some(params) = body.params.as_ref() {
        let cone = if whole {
            "SELECT id, input FROM task_runs
              WHERE run_id = $1 AND fanout_parent IS NULL
                AND status IN ('succeeded', 'failed', 'cancelled', 'skipped')"
        } else {
            "SELECT id, input FROM task_runs
              WHERE run_id = $1 AND status IN ('failed', 'cancelled', 'skipped')"
        };
        let rows: Vec<(String, Option<String>)> = sqlx::query_as(cone)
            .bind(&id)
            .fetch_all(&mut *tx)
            .await
            .map_err(internal_msg)?;
        let mut out = Vec::new();
        for (task_id, input) in rows {
            let Some(input) = input else { continue };
            // Merge into the stored TaskSpec's `input` field; skip rows whose
            // persisted spec this service can't parse rather than failing the rerun.
            if let Ok(mut spec) = serde_json::from_str::<serde_json::Value>(&input) {
                if let Some(obj) = spec.as_object_mut() {
                    let slot = obj.entry("input").or_insert(serde_json::Value::Null);
                    deep_merge(slot, params.clone());
                    out.push((task_id, spec.to_string()));
                }
            }
        }
        out
    } else {
        Vec::new()
    };
    #[cfg(not(feature = "enterprise"))]
    let overrides: Vec<(String, String)> = Vec::new();

    let who = caller_identity(&auth.0);
    let reset = if whole {
        dagron_core::db::rerun_all_in(&mut tx, &id, Some(&who)).await
    } else {
        dagron_core::db::rerun_from_failed_in(
            &mut tx,
            &id,
            dagron_core::models::RunAgain::Rerun,
            Some(&who),
        )
        .await
    }
    .map_err(run_again_refusal)?;
    let Some(reset) = reset else {
        // Lost to another rerun between the read above and the gate.
        return Err(not_rerunnable());
    };

    for (task_id, new_input) in &overrides {
        sqlx::query("UPDATE task_runs SET input = $1 WHERE id = $2")
            .bind(new_input)
            .bind(task_id)
            .execute(&mut *tx)
            .await
            .map_err(internal_msg)?;
    }

    tx.commit().await.map_err(internal_msg)?;

    if whole {
        tracing::info!(run_id = %id, reset, overrides = overrides.len(), "run reran whole");
    } else {
        tracing::info!(run_id = %id, reset, overrides = overrides.len(), "run reran from failure");
    }
    Ok(Json(RerunResponse { run_id: id, rerun: reset }))
}

/// Recursively merge `overlay` into `base`: matching object keys merge; every
/// other shape (scalars, arrays, type mismatches) is replaced by `overlay`.
#[cfg(feature = "enterprise")]
fn deep_merge(base: &mut serde_json::Value, overlay: serde_json::Value) {
    match (base, overlay) {
        (serde_json::Value::Object(b), serde_json::Value::Object(o)) => {
            for (k, v) in o {
                deep_merge(b.entry(k).or_insert(serde_json::Value::Null), v);
            }
        }
        (b, o) => *b = o,
    }
}

// ── Submit ────────────────────────────────────────────────────────────────────

/// Mirror of engine `dag::TaskSpec` — the submitted `input` JSON must match this
/// shape so the engine's worker can deserialize it.
///
/// Fields are `pub(crate)` so the `workflow_ref` expander (`crate::expand`) can
/// read a call task and synthesize the inlined leaf tasks.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct TaskSpecInput {
    pub(crate) name: String,
    // A leaf task runs a `command`; a call task sets `workflow_ref` instead. The
    // field defaults to empty so a `workflow_ref` task (no command) still parses,
    // and is skipped when empty so an expanded leaf serializes cleanly for the
    // engine. Expansion guarantees every surviving task carries a command.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) command: Vec<String>,
    #[serde(default)]
    pub(crate) depends_on: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) input: Option<serde_json::Value>,
    /// Trigger rule (engine `trigger_rule`): when this task runs relative to its
    /// deps' outcomes. Round-tripped into the stored task JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) trigger_rule: Option<String>,
    #[serde(default = "default_max_attempts")]
    pub(crate) max_attempts: u32,
    #[serde(default)]
    pub(crate) retry_delay_secs: u64,
    /// Optional clamp on the exponential retry backoff (engine
    /// `retry_max_delay_secs`); round-tripped into the stored task JSON.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) retry_max_delay_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) timeout_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) docker_image: Option<String>,
    /// Runner-pool routing (engine `runner_class`): stamped on the task row so
    /// a segmented scheduler (`RUNNER_CLASSES`) claims it. Falls back to the
    /// spec-level class, then `"default"` — mirroring engine `create_run`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) runner_class: Option<String>,
    /// Chain another **saved** workflow as this step. At run-creation dagron-api
    /// loads that workflow's spec and inlines its tasks in place of this one
    /// (see [`crate::expand`]). Resolved away before the run is created, so it
    /// never reaches the engine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) workflow_ref: Option<String>,
    /// Call a **template** — a reusable sub-DAG declared in this spec's
    /// [`DagSpecInput::templates`] — instead of running a container (engine
    /// `template`, the DAG-of-DAGs pattern). The engine's expander inlines the
    /// template's tasks in place of this one at run creation, wiring this task's
    /// upstreams to the sub-DAG's roots and its downstreams to the sub-DAG's exits.
    ///
    /// A task is exactly one of: a leaf (`command`), a call (`template`), or a
    /// chain (`workflow_ref`) — see [`validate_graph`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) template: Option<String>,
    /// Arguments for this task's callee (engine `arguments`). Values may
    /// reference the caller's scope via `{{ name }}`. Meaningful in exactly two
    /// places: alongside `template` (fills the called template's declared
    /// `parameters`, consumed inline at expansion) and on a `type: workflow`
    /// trigger (becomes the **child run's parameters**, consumed by the engine
    /// at dispatch). On any other task kind it has nothing to pass to and is
    /// rejected.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub(crate) arguments: std::collections::BTreeMap<String, String>,
    /// Task kind (engine `type`). `type: approval` makes this a human approval
    /// gate (#19) — a command-less leaf that parks in `awaiting_approval`.
    /// Round-tripped into the stored task JSON so the engine sees it.
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub(crate) task_type: Option<String>,
    /// For a `type: workflow` trigger (#23): the name of the **registered
    /// workflow** to run as a child. Mirrors engine `dag::TaskSpec::workflow` so
    /// the field round-trips into the stored task JSON. Required for (and only
    /// valid on) a `type: workflow` task — see [`validate_graph`], which mirrors
    /// `dag::from_spec`. Without this field the API accepted a trigger whose
    /// target was silently dropped and never validated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) workflow: Option<String>,
    /// Approval timeout knobs (engine `approval_timeout_secs`/`approval_on_timeout`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) approval_timeout_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) approval_on_timeout: Option<String>,
    /// Context shown to the approver (engine `approval_message`/`approval_show`);
    /// only valid on a `type: approval` task. See [`validate_graph`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) approval_message: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) approval_show: Vec<String>,
    /// Artifacts whose sha256 an approval must carry (engine `binds`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) binds: Vec<String>,
    /// Who may decide the gate (engine `approvers`/`not_triggerer`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) approvers: Vec<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) not_triggerer: bool,
    /// Environment variables (engine `env`): literal `value` or
    /// `value_from: {secret: NAME}` resolved at dispatch (environment secret
    /// store first, then process env / secrets dir).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) env: Vec<EnvVarInput>,
    /// The environment this task's secrets are resolved from, in place of
    /// the workflow's own (engine `environment` on a task).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) environment: Option<String>,
    /// Conditional gate (engine `when`). On this path two forms are accepted:
    /// a parameter condition (evaluated at submit — false drops the task, its
    /// dependents proceed) or a runtime condition referencing
    /// `{{ tasks.<dep>.output }}` (persisted; the engine evaluates it at
    /// readiness so an upstream's result can branch the DAG).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) when: Option<String>,
    /// Loop operator (engine `repeat`): re-run until `until` holds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) repeat: Option<RepeatInput>,
}

/// Mirror of engine `dag::EnvVar` (same field names → same stored JSON).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct EnvVarInput {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) value_from: Option<SecretRefInput>,
}

/// Mirror of engine `dag::SecretRef`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct SecretRefInput {
    pub(crate) secret: String,
}

/// Mirror of engine `dag::RepeatSpec`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct RepeatInput {
    pub(crate) until: String,
    pub(crate) max_iterations: u32,
    #[serde(default)]
    pub(crate) delay_secs: u64,
}

/// Mirror of engine `dag::TemplateSpec` — a named, reusable sub-DAG that a task
/// calls with `template:`. Its tasks live in their own namespace: `depends_on`
/// inside a template refers to the template's own tasks, and the expander
/// prefixes every produced task with the calling task's name (`run-etl.build`).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct TemplateSpecInput {
    pub(crate) name: String,
    /// Default parameter values, overridable per call via the task's `arguments`.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub(crate) parameters: std::collections::BTreeMap<String, String>,
    pub(crate) tasks: Vec<TaskSpecInput>,
}

/// Mirror of engine `dag::TaskDefaults` (the DRY block): fields set here fill
/// every task that doesn't override them, exactly like the engine-side merge.
#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct TaskDefaultsInput {
    #[serde(default)]
    pub(crate) max_attempts: Option<u32>,
    #[serde(default)]
    pub(crate) retry_delay_secs: Option<u64>,
    #[serde(default)]
    pub(crate) retry_max_delay_secs: Option<u64>,
    #[serde(default)]
    pub(crate) timeout_secs: Option<u64>,
    #[serde(default)]
    pub(crate) docker_image: Option<String>,
    #[serde(default)]
    pub(crate) runner_class: Option<String>,
    #[serde(default)]
    pub(crate) env: Vec<EnvVarInput>,
}

impl TaskSpecInput {
    /// Whether this task is a `type: approval` human gate (#19).
    pub(crate) fn is_approval(&self) -> bool {
        self.task_type.as_deref() == Some("approval")
    }
    /// Whether this task is a `type: workflow` sub-workflow trigger (#23).
    pub(crate) fn is_workflow(&self) -> bool {
        self.task_type.as_deref() == Some("workflow")
    }
    /// Whether this task is a `type: wait` sensor (#27) — time, HTTP, or dataset.
    pub(crate) fn is_wait(&self) -> bool {
        self.task_type.as_deref() == Some("wait")
    }
}

fn default_max_attempts() -> u32 {
    1
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct DagSpecInput {
    pub(crate) name: String,
    /// Run-level wall-clock budget in seconds (engine `run_timeout_secs`);
    /// stamped as `workflow_runs.deadline_at` at run creation so the engine's
    /// deadline sweep enforces it.
    #[serde(default)]
    pub(crate) run_timeout_secs: Option<u64>,
    /// Name of the task whose output becomes the run's result (engine
    /// `result_from`, fast-win #15); stamped as `workflow_runs.result_from` so a
    /// waiter (`GET /api/runs/:id/wait`) gets a single return value. Must name a
    /// real task.
    #[serde(default)]
    pub(crate) result_from: Option<String>,
    /// Template parameters: `{{ name }}` references in task commands /
    /// docker_image / env values / when conditions are substituted at submit.
    #[serde(default)]
    pub(crate) parameters: std::collections::BTreeMap<String, String>,
    /// Reusable sub-DAGs (engine `templates`), each callable from a task's
    /// `template:` — the DAG-of-DAGs pattern. Validated here (names unique, each
    /// sub-graph well-formed, every call resolves) and expanded inline by the
    /// engine's parser at run creation, so nothing downstream sees a call task.
    #[serde(default)]
    pub(crate) templates: Vec<TemplateSpecInput>,
    /// Template libraries to import (`use: [library, library/template]`): other
    /// saved workflows whose `templates:` this spec may call. Resolved by
    /// [`crate::expand::resolve_template_uses`] before the engine parses the
    /// spec, so a call to an imported template is checked after resolution, not
    /// here (see [`validate_graph`]).
    #[serde(default, rename = "use")]
    pub(crate) uses: Vec<String>,
    /// Named environment (variable set + secrets): its variables join the
    /// substitution scope as `{{ env.NAME }}`, its name is stamped on the run
    /// so the engine resolves its secrets at dispatch. Unknown = 400.
    #[serde(default)]
    pub(crate) environment: Option<String>,
    /// Workflow-wide task defaults (the DRY block), merged into every task at
    /// submit — a task wins by setting its own value.
    #[serde(default)]
    pub(crate) task_defaults: Option<TaskDefaultsInput>,
    /// Spec-level runner class (engine `DagSpec::runner_class`): the fallback
    /// for tasks that don't set their own, applied at run creation.
    #[serde(default)]
    pub(crate) runner_class: Option<String>,
    pub(crate) tasks: Vec<TaskSpecInput>,
}

/// Parse + validate a DAG YAML (duplicate-name, unknown-dep, cycle). Shared by
/// submit and dead-letter redrive. Returns the spec or a (status, message) error.
/// This validates the *authored* spec — `workflow_ref` chains and `template:`
/// calls are checked for structure here and expanded later ([`crate::expand`] and
/// the engine's parser respectively), so the graph seen here is the one a human
/// wrote and the one the visual editor draws.
pub(crate) fn parse_and_validate(yaml: &str) -> Result<DagSpecInput, (StatusCode, String)> {
    let spec: DagSpecInput = serde_yaml::from_str(yaml)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid YAML: {e}")))?;
    if spec.run_timeout_secs == Some(0) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid run_timeout_secs=0 in DAG '{}'; expected >= 1 (or omit)", spec.name),
        ));
    }
    if let Some(class) = &spec.runner_class {
        validate_runner_class(class).map_err(|e| {
            (StatusCode::BAD_REQUEST, format!("invalid runner_class in DAG '{}': {e}", spec.name))
        })?;
    }
    if let Some(rf) = &spec.result_from {
        if !spec.tasks.iter().any(|t| &t.name == rf) {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("result_from names unknown task '{rf}' in DAG '{}'", spec.name),
            ));
        }
    }
    validate_templates(&spec)?;
    validate_graph(&spec.name, &spec.tasks, &spec.templates, !spec.uses.is_empty())?;
    Ok(spec)
}

/// Validate the `templates:` block: names are unique and non-empty, no template
/// is empty, and each template's task list is itself a well-formed sub-graph
/// (templates may call other templates, so the same declaration set is in scope).
///
/// Mirrors the checks in `dagron_core::expand`, which is what actually inlines
/// these at run creation. Doing them here means a bad sub-DAG is a 400 on save —
/// the console's visual editor never stores a workflow it can draw but not run.
fn validate_templates(spec: &DagSpecInput) -> Result<(), (StatusCode, String)> {
    let mut seen: HashSet<&str> = HashSet::new();
    for t in &spec.templates {
        if t.name.trim().is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("a template in DAG '{}' has an empty name", spec.name),
            ));
        }
        if !seen.insert(t.name.as_str()) {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("duplicate template name '{}' in DAG '{}'", t.name, spec.name),
            ));
        }
        if t.tasks.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("template '{}' in DAG '{}' has no tasks", t.name, spec.name),
            ));
        }
    }
    for t in &spec.templates {
        // A `workflow_ref` inside a template would be dropped silently: the chain
        // expander ([`crate::expand`]) only rewrites top-level tasks, and the
        // engine's spec model has no such field. Refuse instead of losing it.
        for task in &t.tasks {
            if task.workflow_ref.is_some() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!(
                        "task '{}' in template '{}' sets `workflow_ref` — chaining a saved \
                         workflow is only supported on top-level tasks; call a `template:` instead",
                        task.name, t.name
                    ),
                ));
            }
        }
        validate_graph(
            &format!("{} (template '{}')", spec.name, t.name),
            &t.tasks,
            &spec.templates,
            !spec.uses.is_empty(),
        )?;
    }
    Ok(())
}

/// Runner-class validation, mirroring core `dag::validate_runner_class`
/// (dagron-api cannot depend on dagron-core — see `src/tmpl.rs` for why):
/// lowercase `[a-z0-9_-]`, 1–64 chars, and not the reserved `"other"` (the
/// metrics tail bucket).
fn validate_runner_class(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > 64 {
        return Err(format!("runner_class must be 1-64 characters, got {} ('{name}')", name.len()));
    }
    if !name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '_' | '-'))
    {
        return Err(format!("runner_class '{name}' may only contain [a-z0-9_-]"));
    }
    if name == "other" {
        return Err("runner_class 'other' is reserved (it is the metrics tail bucket)".to_string());
    }
    Ok(())
}

/// Structural DAG validation: every task is exactly one kind (leaf / call /
/// chain), task names are unique, and the (resolvable) graph is acyclic. Runs on
/// the *authored*, unexpanded spec — before `workflow_ref` inlining, templating,
/// and `with_items` fan-out (`submit_yaml`) — so any check here must not assume
/// those have happened. A `depends_on`/`runner_class` that only resolves after
/// expansion (e.g. it targets a name inside a referenced sub-workflow, or is a
/// `{{ }}` template) is left for the engine's own post-expansion validation
/// (`dagron_core::dag::DagGraph::from_yaml_with_params`) rather than rejected
/// here, so a spec the engine would accept is never 400'd early.
///
/// `templates` is the set of declarations a `template:` call may name. It is the
/// same set for the top-level graph and for each template's own sub-graph, since
/// a template may call another template.
///
/// `imports` is true when the spec has a `use:` block: a call may then name a
/// template that a library supplies, which only exists after
/// [`crate::expand::resolve_template_uses`], so the unknown-template check waits
/// for the resolved spec (the save handlers and `build_dag` both re-validate it).
pub(crate) fn validate_graph(
    name: &str,
    tasks: &[TaskSpecInput],
    templates: &[TemplateSpecInput],
    imports: bool,
) -> Result<(), (StatusCode, String)> {
    let mut graph = DiGraph::<(), ()>::new();
    let mut idx = HashMap::new();
    let mut names = HashSet::new();
    const TRIGGER_RULES: &[&str] =
        &["all_success", "all_done", "one_failed", "all_failed", "none_failed"];
    for t in tasks {
        if !names.insert(t.name.clone()) {
            return Err((StatusCode::BAD_REQUEST, format!("duplicate task name '{}'", t.name)));
        }
        if let Some(rule) = &t.trigger_rule {
            if !TRIGGER_RULES.contains(&rule.as_str()) {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("task '{}' has invalid trigger_rule '{rule}' (expected one of {TRIGGER_RULES:?})", t.name),
                ));
            }
        }
        // `repeat:` loop-operator validation (mirrors core from_spec).
        if let Some(rep) = &t.repeat {
            if rep.until.trim().is_empty() {
                return Err((StatusCode::BAD_REQUEST, format!("task '{}' repeat.until is empty", t.name)));
            }
            if rep.max_iterations == 0 {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("task '{}' repeat.max_iterations must be >= 1", t.name),
                ));
            }
            // Mirrors core: `repeat` is evaluated after a success, by the
            // executor-result path for a command task and by the sub-workflow
            // sweep for a trigger. An approval and a wait sensor have no
            // iteration, so a loop on either would never run. Rejecting here
            // keeps the API's error identical to the engine's rather than
            // deferring to a later, less specific one.
            if !matches!(t.task_type.as_deref(), None | Some("task") | Some("workflow")) {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!(
                        "task '{}' cannot combine `repeat` with `type: {}` — `repeat` applies to command tasks and sub-workflow triggers",
                        t.name,
                        t.task_type.as_deref().unwrap_or("task")
                    ),
                ));
            }
        }
        // A templated runner_class (`{{ param }}`) only becomes a real class
        // name after submit_yaml's substitution pass — checking the raw string's
        // charset here would 400 a spec the engine would accept. The engine
        // re-validates the substituted value itself (dag::from_spec).
        if let Some(class) = &t.runner_class {
            if !class.contains("{{") {
                validate_runner_class(class).map_err(|e| {
                    (
                        StatusCode::BAD_REQUEST,
                        format!("invalid runner_class for task '{}': {e}", t.name),
                    )
                })?;
            }
        }
        // A runtime `when` may only reference tasks it depends on — an output
        // the gate is guaranteed to have when the engine evaluates readiness.
        if let Some(cond) = &t.when {
            for referenced in crate::tmpl::when_output_refs(cond) {
                if !t.depends_on.contains(&referenced) {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        format!(
                            "task '{}' when references '{{{{ tasks.{referenced}.output }}}}' but does \
                             not depend on '{referenced}' — add it to depends_on",
                            t.name
                        ),
                    ));
                }
            }
        }
        // Approver context mirrors `dag::validate`: approval-only, bounded, and
        // each `approval_show` entry a `<task>/<name>` artifact key.
        if !t.is_approval() && (t.approval_message.is_some() || !t.approval_show.is_empty()) {
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "task '{}' sets approval_message/approval_show but is not `type: approval`",
                    t.name
                ),
            ));
        }
        if let Some(m) = &t.approval_message {
            if m.chars().count() > dagron_core::dag::APPROVAL_TEXT_MAX {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!(
                        "task '{}' approval_message exceeds {} characters",
                        t.name,
                        dagron_core::dag::APPROVAL_TEXT_MAX
                    ),
                ));
            }
        }
        if t.approval_show.len() > dagron_core::dag::APPROVAL_SHOW_MAX {
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "task '{}' approval_show has more than {} entries",
                    t.name,
                    dagron_core::dag::APPROVAL_SHOW_MAX
                ),
            ));
        }
        for entry in &t.approval_show {
            if entry.contains("{{") {
                continue;
            }
            dagron_core::dag::validate_approval_show(entry).map_err(|e| {
                (StatusCode::BAD_REQUEST, format!("task '{}': {e}", t.name))
            })?;
        }
        if !t.is_approval() && !t.binds.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("task '{}' sets binds but is not `type: approval`", t.name),
            ));
        }
        if t.binds.len() > dagron_core::dag::APPROVAL_SHOW_MAX {
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "task '{}' binds has more than {} entries",
                    t.name,
                    dagron_core::dag::APPROVAL_SHOW_MAX
                ),
            ));
        }
        for entry in t.binds.iter().filter(|e| !e.contains("{{")) {
            dagron_core::dag::validate_approval_show(entry).map_err(|e| {
                (StatusCode::BAD_REQUEST, format!("task '{}': {}", t.name, e.replace("approval_show", "binds")))
            })?;
        }
        if !t.is_approval() && (!t.approvers.is_empty() || t.not_triggerer) {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("task '{}' sets approvers/not_triggerer but is not `type: approval`", t.name),
            ));
        }
        if t.approvers.len() > dagron_core::dag::APPROVERS_MAX {
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "task '{}' approvers has more than {} entries",
                    t.name,
                    dagron_core::dag::APPROVERS_MAX
                ),
            ));
        }
        for entry in &t.approvers {
            if entry.contains("{{") {
                continue;
            }
            dagron_core::dag::validate_approver(entry)
                .map_err(|e| (StatusCode::BAD_REQUEST, format!("task '{}': {e}", t.name)))?;
        }
        // Command-less task kinds are exempt from the leaf/chain rule (but must
        // not carry either): an approval gate waits for a human (#19), a
        // sub-workflow trigger runs a child workflow (#23), and a wait sensor
        // defers on a timer / endpoint / dataset (#27). This list must track
        // `dag::validate`'s — it is the same rule stated twice, and when #716
        // added the last two kinds only the engine's copy learned about them, so
        // saving a workflow containing a sensor or a trigger failed here while
        // submitting the identical spec as a run succeeded.
        if t.is_approval() || t.is_workflow() || t.is_wait() {
            if !t.command.is_empty() || t.workflow_ref.is_some() || t.template.is_some() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!(
                        "task '{}' is a command-less kind (approval / workflow / wait) and cannot set `command`, `template` or `workflow_ref`",
                        t.name
                    ),
                ));
            }
        } else {
            // A task is exactly one of: a leaf (`command`), a call into a declared
            // template (`template`), or a chain into a saved workflow
            // (`workflow_ref`). Catches a command-less task on the no-reference
            // path too (where the expander, which also checks this, is skipped).
            let kinds = [!t.command.is_empty(), t.template.is_some(), t.workflow_ref.is_some()];
            match kinds.iter().filter(|set| **set).count() {
                0 => {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        format!(
                            "task '{}' needs a `command` (leaf), a `template` (sub-DAG call) or a `workflow_ref` (chain)",
                            t.name
                        ),
                    ));
                }
                1 => {}
                _ => {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        format!(
                            "task '{}' sets more than one of `command` / `template` / `workflow_ref` — use exactly one",
                            t.name
                        ),
                    ));
                }
            }
        }
        // Mirror `dag::from_spec`'s workflow-target rule (same rule stated
        // twice, like the command-less-kind check above): a `type: workflow`
        // trigger must name a nonblank `workflow:` to run, and no other kind may
        // set one. Before `TaskSpecInput` carried the field, the API accepted a
        // trigger whose target was dropped on the floor — the engine then
        // rejected it at run creation, so a spec that saved failed to run.
        if t.is_workflow() {
            match t.workflow.as_deref() {
                Some(w) if !w.trim().is_empty() => {}
                _ => {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        format!("task '{}' is type: workflow but names no `workflow:` to trigger", t.name),
                    ));
                }
            }
        } else if t.workflow.is_some() {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("task '{}' sets `workflow:` but is not `type: workflow`", t.name),
            ));
        }
        // A call must name a template this spec declares. Unlike `workflow_ref`
        // (which resolves against the saved-workflow table at run creation), a
        // template is local to the spec, so this is fully checkable at save time.
        if let Some(called) = &t.template {
            if !imports && !templates.iter().any(|tpl| &tpl.name == called) {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!(
                        "task '{}' calls unknown template '{called}' in DAG '{name}' — declare it under `templates:`",
                        t.name
                    ),
                ));
            }
        } else if !t.arguments.is_empty() && t.task_type.as_deref() != Some("workflow") {
            // A `type: workflow` trigger is the second legitimate callee: its
            // `arguments` become the child run's parameters (#23). Everything
            // else has nothing to pass them to, and accepting them there would
            // make a parameter that looks configured and goes nowhere.
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "task '{}' sets `arguments` with no `template` or `type: workflow` to pass them to",
                    t.name
                ),
            ));
        }
        idx.insert(t.name.clone(), graph.add_node(()));
    }
    // Names a dependency may legitimately forward-reference: a task inside a
    // workflow_ref-referenced sub-workflow, namespaced only after expansion
    // (e.g. `call.inner` for a task named `call` with `workflow_ref: child`).
    let chains: HashSet<&str> =
        tasks.iter().filter(|t| t.workflow_ref.is_some()).map(|t| t.name.as_str()).collect();
    for t in tasks {
        let to = idx[&t.name];
        for dep in &t.depends_on {
            if let Some(&from) = idx.get(dep) {
                graph.add_edge(from, to, ());
                continue;
            }
            // Not a top-level authored name. If it's namespaced under a
            // workflow_ref task in this spec, defer to the engine's
            // post-expansion validation — this pre-check can't see inside the
            // chain. Anything else is a genuinely unknown dependency.
            if !chains.iter().any(|c| dep == c || dep.starts_with(&format!("{c}."))) {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("task '{}' depends on unknown task '{}'", t.name, dep),
                ));
            }
        }
    }
    if is_cyclic_directed(&graph) {
        return Err((StatusCode::BAD_REQUEST, format!("DAG '{}' contains a cycle", name)));
    }
    Ok(())
}

#[cfg(test)]
mod validate_graph_tests {
    use super::parse_and_validate;

    /// `parse_and_validate` runs on the authored, unexpanded YAML — it must not
    /// reject a `depends_on` that only resolves after `workflow_ref` inlining
    /// (the engine's own post-expansion validation is the authority on that).
    #[test]
    fn allows_a_dep_on_a_workflow_ref_internal_task() {
        let spec = "
name: parent
tasks:
  - name: prepare
    command: [\"true\"]
  - name: call
    workflow_ref: child
    depends_on: [prepare]
  - name: notify
    command: [\"echo\", \"done\"]
    depends_on: [call.inner]
";
        parse_and_validate(spec).expect("a forward-reference into a workflow_ref chain must not 400");
    }

    /// A `depends_on` on a name that isn't a sibling and isn't namespace-shaped
    /// like a workflow_ref reference (no workflow_ref task present at all) is
    /// still caught eagerly here — this is what lets gitrepos.rs's sync-time
    /// scan (`fetch_and_validate`, which also calls this) flag a broken synced
    /// workflow file immediately rather than only at submit time.
    #[test]
    fn still_rejects_a_genuinely_unknown_dep() {
        let spec = "
name: p
tasks:
  - name: a
    command: [\"true\"]
    depends_on: [ghost]
";
        let err = parse_and_validate(spec).unwrap_err();
        assert!(err.1.contains("unknown task 'ghost'"), "got: {}", err.1);
    }

    /// A templated `runner_class` only becomes a real class name after
    /// submit_yaml's substitution — the raw charset check must not fire on it.
    #[test]
    fn allows_a_templated_runner_class() {
        let spec = "
name: p
tasks:
  - name: a
    command: [\"true\"]
    runner_class: \"{{ params.class }}\"
";
        parse_and_validate(spec).expect("a templated runner_class must not 400 before substitution");
    }

    /// A non-templated, invalid runner_class is still caught early (fast
    /// feedback on the common typo case, no expansion needed to know it's bad).
    #[test]
    fn still_rejects_a_plain_invalid_runner_class() {
        let spec = "
name: p
tasks:
  - name: a
    command: [\"true\"]
    runner_class: \"Not Valid!\"
";
        let err = parse_and_validate(spec).unwrap_err();
        assert!(err.1.contains("runner_class"), "got: {}", err.1);
    }

    // ── `templates:` / `template:` — the DAG-of-DAGs pattern ─────────────────
    // The engine has expanded template calls since day one, but this validator
    // (which every save/submit path goes through) knew only `command` and
    // `workflow_ref`, so `examples/templates/01_dag_of_dags.yaml` was a 400 —
    // unsaveable through the API and therefore unopenable in the console editor.

    /// `examples/templates/01_dag_of_dags.yaml`, verbatim.
    const DAG_OF_DAGS: &str = "
name: dag-of-dags
templates:
  - name: etl
    tasks:
      - { name: build,   command: [\"sh\", \"-c\", \"echo build\"] }
      - { name: process, command: [\"sh\", \"-c\", \"echo process\"], depends_on: [build] }
      - { name: publish, command: [\"sh\", \"-c\", \"echo publish\"], depends_on: [process] }
tasks:
  - { name: prepare, command: [\"sh\", \"-c\", \"echo prepare\"] }
  - { name: run-etl, template: etl, depends_on: [prepare] }
  - { name: notify,  command: [\"sh\", \"-c\", \"echo done\"], depends_on: [run-etl] }
";

    #[test]
    fn accepts_a_template_call() {
        parse_and_validate(DAG_OF_DAGS).expect("the DAG-of-DAGs example must validate");
    }

    /// The same spec must survive the engine's own parser + expander, which is
    /// what actually runs it — this is the end-to-end guard against the two
    /// validators drifting apart again.
    #[test]
    fn the_engine_expands_the_same_spec() {
        let dag = dagron_core::dag::DagGraph::from_yaml(DAG_OF_DAGS)
            .expect("the engine must expand what this validator accepts");
        let names: Vec<&str> = dag.spec.tasks.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"run-etl.build"), "got: {names:?}");
        assert!(names.contains(&"run-etl.publish"), "got: {names:?}");
    }

    #[test]
    fn rejects_a_call_to_an_undeclared_template() {
        let spec = "
name: p
tasks:
  - { name: a, template: nope }
";
        let err = parse_and_validate(spec).unwrap_err();
        assert!(err.1.contains("unknown template 'nope'"), "got: {}", err.1);
    }

    #[test]
    fn a_call_may_name_a_template_a_use_block_supplies() {
        // With `use:` the template exists only after the library is merged in, so
        // the save-time check defers to the resolved spec rather than 400-ing.
        let spec = "
name: p
use: [iac-library]
tasks:
  - { name: a, template: tf_stage }
";
        parse_and_validate(spec).expect("a call into an imported template must not 400");
        // …and the same call without `use:` is still refused.
        let err = parse_and_validate(&spec.replace("use: [iac-library]\n", "")).unwrap_err();
        assert!(err.1.contains("unknown template 'tf_stage'"), "got: {}", err.1);
    }

    #[test]
    fn rejects_a_task_that_is_two_kinds_at_once() {
        let spec = "
name: p
templates:
  - name: t
    tasks:
      - { name: inner, command: [\"true\"] }
tasks:
  - { name: a, template: t, command: [\"true\"] }
";
        let err = parse_and_validate(spec).unwrap_err();
        assert!(err.1.contains("more than one of"), "got: {}", err.1);
    }

    /// A template's own task list is a graph too — a broken edge inside it is a
    /// save-time 400, not a run-time surprise.
    #[test]
    fn validates_inside_a_template() {
        let spec = "
name: p
templates:
  - name: t
    tasks:
      - { name: inner, command: [\"true\"], depends_on: [ghost] }
tasks:
  - { name: a, template: t }
";
        let err = parse_and_validate(spec).unwrap_err();
        assert!(err.1.contains("unknown task 'ghost'"), "got: {}", err.1);
    }

    /// `workflow_ref` is resolved by the API's chain expander, which only walks
    /// top-level tasks — inside a template it would be silently dropped, so it is
    /// refused instead.
    #[test]
    fn rejects_a_workflow_ref_inside_a_template() {
        let spec = "
name: p
templates:
  - name: t
    tasks:
      - { name: inner, workflow_ref: other }
tasks:
  - { name: a, template: t }
";
        let err = parse_and_validate(spec).unwrap_err();
        assert!(err.1.contains("only supported on top-level tasks"), "got: {}", err.1);
    }

    #[test]
    fn rejects_duplicate_template_names() {
        let spec = "
name: p
templates:
  - { name: t, tasks: [{ name: x, command: [\"true\"] }] }
  - { name: t, tasks: [{ name: y, command: [\"true\"] }] }
tasks:
  - { name: a, template: t }
";
        let err = parse_and_validate(spec).unwrap_err();
        assert!(err.1.contains("duplicate template name 't'"), "got: {}", err.1);
    }

    /// The console and the engine must agree about multi-dataset composition.
    ///
    /// `parse_and_validate` is dagron-api's own mirror of the spec rules and
    /// does not know about datasets at all; the authority is `dagron_core`'s
    /// parser, which `submit_yaml` routes through. When composition was gated,
    /// acceptance turned on which features dagron-api forwarded to dagron-core
    /// rather than on dagron-api's own `enterprise` flag, and a feature-on
    /// console refused specs the feature-on engine beside it accepted. The gate
    /// is gone, so both halves must now accept it in every build — no `cfg`
    /// split here, deliberately: that is the property under test.
    #[test]
    fn the_dataset_composition_line_matches_the_engine() {
        let multi = "
name: p
on_datasets: [\"a://b\", \"a://c\"]
datasets_mode: all
tasks:
  - { name: t, command: [\"true\"] }
";
        let dag = dagron_core::dag::DagGraph::from_yaml(multi)
            .expect("the engine accepts multi-dataset composition in every build");
        assert_eq!(dag.spec.on_datasets.len(), 2);
        assert_eq!(dag.spec.datasets_mode.as_deref(), Some("all"));
        // The mirror carries no dataset fields, so it must simply not object.
        assert!(parse_and_validate(multi).is_ok(), "and so does the console's mirror");
    }

    #[test]
    fn rejects_arguments_without_a_template() {
        let spec = "
name: p
tasks:
  - { name: a, command: [\"true\"], arguments: { x: \"1\" } }
";
        let err = parse_and_validate(spec).unwrap_err();
        assert!(err.1.contains("`arguments`"), "got: {}", err.1);
    }

    /// The API mirror must gate a `type: workflow` trigger's target the same way
    /// `dag::from_spec` does — otherwise a spec that saves here fails at run
    /// creation. A trigger names a nonblank `workflow:`; no other kind may.
    #[test]
    fn workflow_target_validation_mirrors_the_engine() {
        // A trigger with a target (and its child arguments) is accepted.
        parse_and_validate(
            "
name: p
tasks:
  - { name: t, type: workflow, workflow: child, arguments: { x: \"1\" } }
",
        )
        .expect("a type: workflow trigger with a target must be accepted");

        // A trigger with no target is rejected.
        let err = parse_and_validate(
            "
name: p
tasks:
  - { name: t, type: workflow, arguments: { x: \"1\" } }
",
        )
        .unwrap_err();
        assert!(err.1.contains("names no `workflow:`"), "got: {}", err.1);

        // `workflow:` on a non-trigger task is rejected.
        let err = parse_and_validate(
            "
name: p
tasks:
  - { name: t, command: [\"true\"], workflow: child }
",
        )
        .unwrap_err();
        assert!(err.1.contains("not `type: workflow`"), "got: {}", err.1);
    }
}

#[derive(Deserialize)]
pub struct SubmitBody {
    yaml: String,
    /// Arguments for the spec's declared `parameters:`. Optional, so the
    /// pre-existing `{"yaml": "..."}` body stays valid unchanged.
    #[serde(default)]
    parameters: BTreeMap<String, String>,
    /// What started the run, from a caller that may say ([`run_trigger`]).
    #[serde(default)]
    trigger: Option<TriggerBody>,
}

/// What a caller says started the run it is submitting: the event a CI service
/// is running a pipeline for. It becomes the run's `run_triggers` row.
#[derive(Deserialize, Default, Debug, Clone)]
pub struct TriggerBody {
    /// `ci`, the one kind a caller may set.
    pub kind: String,
    /// Who caused it at the source: the pusher, a pull request's author.
    #[serde(default)]
    pub actor: Option<String>,
    /// `trusted`, `untrusted` or `agent`.
    #[serde(default)]
    pub trust: Option<String>,
    #[serde(default)]
    pub forge_host: Option<String>,
    /// `owner/name` on that forge.
    #[serde(default)]
    pub repo: Option<String>,
    #[serde(default, rename = "ref")]
    pub git_ref: Option<String>,
    #[serde(default)]
    pub sha: Option<String>,
    #[serde(default)]
    pub base_sha: Option<String>,
    #[serde(default)]
    pub pr: Option<i64>,
    /// The caller's own id for the event.
    #[serde(default)]
    pub event_id: Option<String>,
}

/// The longest a trigger's text field may be. They are names, refs and ids.
const TRIGGER_FIELD_MAX: usize = 512;

/// The trigger record a submit carries, checked, with its principal set from
/// the caller the gateway authenticated. `None` when the submit names none.
///
/// A trigger is a claim about where a run came from, and what reads it (a
/// list narrowed to CI's runs) believes it. So it is taken only from a caller
/// in the trigger group, and anyone else's is refused outright: dropping it
/// silently would run a submit its sender believes is marked.
pub(crate) fn run_trigger(
    claims: &crate::auth::SessionClaims,
    body: Option<&TriggerBody>,
) -> Result<Option<dagron_core::models::RunTrigger>, (StatusCode, String)> {
    let Some(t) = body else { return Ok(None) };
    if !crate::auth::may_set_trigger(claims) {
        return Err((
            StatusCode::FORBIDDEN,
            format!(
                "this caller may not say what started a run: `trigger` is taken from a member of the `{}` group",
                crate::auth::TRIGGER_GROUP
            ),
        ));
    }
    let refuse = |why: String| Err((StatusCode::UNPROCESSABLE_ENTITY, why));
    if t.kind != "ci" {
        return refuse(format!("trigger.kind {:?} is not one a caller sets: `ci` is", t.kind));
    }
    if let Some(trust) = t.trust.as_deref() {
        if !matches!(trust, "trusted" | "untrusted" | "agent") {
            return refuse(format!("trigger.trust {trust:?} is not trusted, untrusted or agent"));
        }
    }
    for (name, value) in [
        ("actor", &t.actor),
        ("forge_host", &t.forge_host),
        ("repo", &t.repo),
        ("ref", &t.git_ref),
        ("sha", &t.sha),
        ("base_sha", &t.base_sha),
        ("event_id", &t.event_id),
    ] {
        if value.as_deref().is_some_and(|v| v.len() > TRIGGER_FIELD_MAX || v.chars().any(char::is_control)) {
            return refuse(format!(
                "trigger.{name} is over {TRIGGER_FIELD_MAX} bytes, or holds a control character"
            ));
        }
    }
    Ok(Some(dagron_core::models::RunTrigger {
        kind: t.kind.clone(),
        principal: format!("ci:{}", caller_identity(claims)),
        actor: t.actor.clone(),
        trust: t.trust.clone(),
        // A CI service's run comes from a repository, which it names below.
        source: None,
        forge_host: t.forge_host.clone(),
        repo: t.repo.clone(),
        git_ref: t.git_ref.clone(),
        sha: t.sha.clone(),
        base_sha: t.base_sha.clone(),
        pr: t.pr,
        event_id: t.event_id.clone(),
    }))
}

#[derive(Serialize)]
pub struct SubmitResponse {
    pub run_id: String,
}

#[cfg(test)]
mod submit_body_tests {
    use super::SubmitBody;

    /// The pre-existing body shape must keep parsing unchanged. `parameters`
    /// was added to a request type that every client already sends, so a
    /// missing field has to mean "none", not a 422.
    #[test]
    fn body_without_parameters_still_parses() {
        let b: SubmitBody = serde_json::from_str(r#"{"yaml":"name: x"}"#).unwrap();
        assert_eq!(b.yaml, "name: x");
        assert!(b.parameters.is_empty());
    }

    #[test]
    fn parameters_are_read_when_present() {
        let b: SubmitBody =
            serde_json::from_str(r#"{"yaml":"name: x","parameters":{"date":"2026-08-18"}}"#)
                .unwrap();
        assert_eq!(b.parameters.get("date").map(String::as_str), Some("2026-08-18"));
    }

    /// An explicit empty object is the same as omitting the field — no caller
    /// should have to care which of the two its HTTP library produces.
    #[test]
    fn empty_parameters_object_is_the_same_as_none() {
        let a: SubmitBody = serde_json::from_str(r#"{"yaml":"y"}"#).unwrap();
        let b: SubmitBody = serde_json::from_str(r#"{"yaml":"y","parameters":{}}"#).unwrap();
        assert_eq!(a.parameters, b.parameters);
    }
}

#[cfg(test)]
mod run_trigger_tests {
    use super::{run_trigger, SubmitBody, TriggerBody};
    use crate::auth::SessionClaims;
    use axum::http::StatusCode;

    fn claims(groups: &[&str]) -> SessionClaims {
        SessionClaims {
            sub: "u-1".into(),
            email: "ci@example.com".into(),
            name: "ci".into(),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            exp: 0,
        }
    }

    fn ci() -> TriggerBody {
        TriggerBody { kind: "ci".into(), ..TriggerBody::default() }
    }

    /// Every body sent before this field existed has none, and gets no record.
    #[test]
    fn a_submit_that_names_no_trigger_gets_none() {
        let b: SubmitBody = serde_json::from_str(r#"{"yaml":"name: x"}"#).unwrap();
        assert!(b.trigger.is_none());
        assert_eq!(run_trigger(&claims(&[]), None).unwrap(), None);
        // A member of the group that names none gets none either.
        assert_eq!(run_trigger(&claims(&["ci:trigger"]), None).unwrap(), None);
    }

    /// The whole point: only the group's members are believed. An operator, an
    /// admin and a viewer are each refused, not silently run unmarked.
    #[test]
    fn a_trigger_from_outside_the_group_is_refused() {
        for groups in [&[][..], &["admin"], &["viewer"], &["ci"], &["CI:TRIGGER"]] {
            let (status, why) = run_trigger(&claims(groups), Some(&ci())).unwrap_err();
            assert_eq!(status, StatusCode::FORBIDDEN, "{groups:?}");
            assert!(why.contains("ci:trigger"), "{why}");
        }
    }

    /// The principal is who the gateway authenticated, never a field of the
    /// body: there is none to set.
    #[test]
    fn the_principal_is_the_authenticated_caller() {
        let body: SubmitBody = serde_json::from_str(
            r#"{"yaml":"name: x","trigger":{"kind":"ci","principal":"ci:someone-else","actor":"octo",
                "trust":"trusted","forge_host":"github.com","repo":"acme/widgets","ref":"refs/heads/main",
                "sha":"abc","base_sha":"def","pr":7,"event_id":"e-1"}}"#,
        )
        .unwrap();
        let t = run_trigger(&claims(&["ci:trigger"]), body.trigger.as_ref()).unwrap().unwrap();
        assert_eq!(t.principal, "ci:ci@example.com");
        assert_eq!(
            (t.kind.as_str(), t.actor.as_deref(), t.trust.as_deref(), t.pr),
            ("ci", Some("octo"), Some("trusted"), Some(7))
        );
        assert_eq!(
            (t.forge_host.as_deref(), t.repo.as_deref(), t.git_ref.as_deref()),
            (Some("github.com"), Some("acme/widgets"), Some("refs/heads/main"))
        );
        assert_eq!(
            (t.sha.as_deref(), t.base_sha.as_deref(), t.event_id.as_deref()),
            (Some("abc"), Some("def"), Some("e-1"))
        );
    }

    /// A member cannot mark a run as a schedule's or a person's: those kinds
    /// are derived, and `ci` is the one a caller sets.
    #[test]
    fn only_the_ci_kind_and_a_known_trust_are_taken() {
        let member = claims(&["ci:trigger"]);
        for kind in ["manual", "schedule", "backfill", "", "CI"] {
            let t = TriggerBody { kind: kind.into(), ..TriggerBody::default() };
            let (status, why) = run_trigger(&member, Some(&t)).unwrap_err();
            assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{kind:?}");
            assert!(why.contains("trigger.kind"), "{why}");
        }
        let t = TriggerBody { trust: Some("owner".into()), ..ci() };
        assert_eq!(run_trigger(&member, Some(&t)).unwrap_err().0, StatusCode::UNPROCESSABLE_ENTITY);
        for trust in ["trusted", "untrusted", "agent"] {
            let t = TriggerBody { trust: Some(trust.into()), ..ci() };
            assert!(run_trigger(&member, Some(&t)).is_ok(), "{trust}");
        }
    }

    /// The fields are stored and shown, so they are bounded and printable.
    #[test]
    fn an_oversized_or_unprintable_field_is_refused() {
        let member = claims(&["ci:trigger"]);
        let long = TriggerBody { repo: Some("r".repeat(513)), ..ci() };
        let (status, why) = run_trigger(&member, Some(&long)).unwrap_err();
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(why.contains("trigger.repo"), "{why}");
        let at_the_limit = TriggerBody { repo: Some("r".repeat(512)), ..ci() };
        assert!(run_trigger(&member, Some(&at_the_limit)).is_ok());
        let odd = TriggerBody { actor: Some("octo\ncat".into()), ..ci() };
        assert!(run_trigger(&member, Some(&odd)).unwrap_err().1.contains("trigger.actor"));
    }
}

#[cfg(test)]
mod idempotency_tests {
    use super::*;
    use axum::http::HeaderMap;

    fn headers(value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("idempotency-key", value.parse().unwrap());
        h
    }

    fn body(yaml: &str, params: &[(&str, &str)]) -> SubmitBody {
        SubmitBody {
            yaml: yaml.to_string(),
            parameters: params.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            trigger: None,
        }
    }

    /// The header is opt-in. Every caller that exists today sends no key, and
    /// must keep getting the old behaviour rather than a validation error.
    #[test]
    fn no_header_means_not_idempotent() {
        assert_eq!(idempotency_key(&HeaderMap::new()).unwrap(), None);
    }

    /// An empty key is rejected rather than treated as absent. A client that
    /// sends one believes it has retry safety; silently giving it none is the
    /// exact failure the header exists to remove.
    #[test]
    fn an_empty_key_is_rejected_not_ignored() {
        let err = idempotency_key(&headers("   ")).unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.contains("must not be empty"), "got: {}", err.1);
    }

    #[test]
    fn an_oversized_key_is_rejected() {
        let err = idempotency_key(&headers(&"k".repeat(MAX_IDEMPOTENCY_KEY + 1))).unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn a_normal_key_is_accepted_and_trimmed() {
        let k = idempotency_key(&headers(" 6f1e2b9c-run-42 ")).unwrap();
        assert_eq!(k.as_deref(), Some("6f1e2b9c-run-42"));
    }

    /// The same request fingerprints the same, or a legitimate retry would be
    /// rejected as a key reuse — the failure that would make the feature worse
    /// than not having it.
    #[test]
    fn an_identical_request_fingerprints_identically() {
        let a = request_fingerprint(&body("name: x", &[("date", "2026-08-18")]));
        let b = request_fingerprint(&body("name: x", &[("date", "2026-08-18")]));
        assert_eq!(a, b);
    }

    #[test]
    fn a_different_spec_fingerprints_differently() {
        let a = request_fingerprint(&body("name: x", &[]));
        let b = request_fingerprint(&body("name: y", &[]));
        assert_ne!(a, b);
    }

    /// Same spec, different arguments, is a *different* request. Replaying the
    /// first run for it would hand the caller a run id for work that never ran.
    #[test]
    fn different_parameters_fingerprint_differently() {
        let a = request_fingerprint(&body("name: x", &[("date", "2026-08-18")]));
        let b = request_fingerprint(&body("name: x", &[("date", "2026-08-19")]));
        assert_ne!(a, b);
    }

    /// Fields are length-prefixed for this: without it, splitting the same
    /// bytes at a different boundary would collide, and two genuinely different
    /// requests would share a fingerprint.
    #[test]
    fn a_field_boundary_cannot_be_moved_without_changing_the_fingerprint() {
        let a = request_fingerprint(&body("y", &[("a", "bc")]));
        let b = request_fingerprint(&body("y", &[("ab", "c")]));
        assert_ne!(a, b);
    }

    /// `parameters` is a BTreeMap, so insertion order cannot leak into the
    /// digest — an agent whose map iterates differently between retries must
    /// still hit the replay.
    #[test]
    fn parameter_order_does_not_change_the_fingerprint() {
        let a = request_fingerprint(&body("y", &[("a", "1"), ("b", "2")]));
        let b = request_fingerprint(&body("y", &[("b", "2"), ("a", "1")]));
        assert_eq!(a, b);
    }
}

// ── idempotent submit ───────────────────────────────────────────────────────

/// Cap on an `Idempotency-Key`. Comfortably past a UUID or a hex digest, and
/// short enough that a hostile client cannot use a primary-key index as free
/// storage.
const MAX_IDEMPOTENCY_KEY: usize = 255;

/// How long a claimed-but-unresolved key is honoured before another submit may
/// take it. The window only matters when a submitter died between claiming the
/// key and creating the run; a live submit resolves its claim in the time one
/// `create_run` takes, so this is far longer than any healthy path needs and
/// short enough that a crash does not strand a key until its run's retention.
///
/// Taking a stale claim over is safe whatever became of the submit that made
/// it. The claim is resolved in the transaction that creates the run
/// (`dagron_core::db::create_run_claimed`): a submit that died left no run,
/// and one that is only slow finds its claim gone when it comes to commit,
/// and creates none.
const IN_FLIGHT_CLAIM_SECS: i64 = 300;

/// What the key lookup decided.
pub(crate) enum Claim {
    /// This request owns the key and must create the run, resolving the
    /// claim to its id in the same transaction. Carries the claim's
    /// `created_at` as this request wrote it: what tells its claim from one
    /// that takes the key over later.
    Held(String),
    /// The key already names a completed submission. Return *that* run rather
    /// than making a second one — the whole point of the header.
    Replay(String),
}

/// The `Idempotency-Key` header, validated.
///
/// An absent header means "not idempotent", which is every caller today. An
/// **empty** one is a 400 rather than the same thing: a client sending it
/// believes it has retry safety, and quietly giving it none is the failure this
/// endpoint exists to remove.
pub(crate) fn idempotency_key(
    headers: &axum::http::HeaderMap,
) -> Result<Option<String>, (StatusCode, String)> {
    let Some(raw) = headers.get("idempotency-key") else {
        return Ok(None);
    };
    let key = raw.to_str().map_err(|_| bad_key("must be ASCII"))?.trim();
    if key.is_empty() {
        return Err(bad_key("must not be empty"));
    }
    if key.len() > MAX_IDEMPOTENCY_KEY {
        return Err(bad_key(&format!("must be at most {MAX_IDEMPOTENCY_KEY} characters")));
    }
    if !key.chars().all(|c| c.is_ascii_graphic()) {
        return Err(bad_key("must be printable ASCII without spaces"));
    }
    Ok(Some(key.to_string()))
}

/// A rejected `Idempotency-Key`, as a 400 that names what was wrong with it.
fn bad_key(msg: &str) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, format!("Idempotency-Key {msg}"))
}

/// A key that cannot be honoured as asked: 409, naming the key so a retrying
/// caller can tell which of its in-flight submits this is about.
fn key_conflict(key: &str, msg: &str) -> (StatusCode, String) {
    (StatusCode::CONFLICT, format!("Idempotency-Key '{key}': {msg}"))
}

/// SHA-256 over the request this key is claimed for.
///
/// Stored so a key replayed with a *different* body can be rejected. Serving
/// the first run for a second, different request is the worst available
/// answer: the caller gets a 200 and a run id for work that never ran.
///
/// Every field is length-prefixed, so `{"a": "bc"}` and `{"ab": "c"}` cannot
/// hash alike. `parameters` is a `BTreeMap`, so iteration order is the key
/// order and two equal maps always fingerprint the same.
fn request_fingerprint(body: &SubmitBody) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    hash_field(&mut h, &body.yaml);
    for (k, v) in &body.parameters {
        hash_field(&mut h, k);
        hash_field(&mut h, v);
    }
    // Lowercase hex, the same way `tokens::hash_token` writes a digest.
    let mut out = String::with_capacity(64);
    for b in h.finalize() {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// [`request_fingerprint`] for `POST /api/workflows/{id}/run`: the stored
/// workflow and the arguments it is run with. The tag keeps it from ever
/// matching a `POST /api/runs` body, so one key reused across the two routes
/// is a 409, not a replay of the other route's run.
pub(crate) fn workflow_run_fingerprint(
    workflow_id: &str,
    parameters: &BTreeMap<String, String>,
) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    hash_field(&mut h, "workflow-run");
    hash_field(&mut h, workflow_id);
    for (k, v) in parameters {
        hash_field(&mut h, k);
        hash_field(&mut h, v);
    }
    let mut out = String::with_capacity(64);
    for b in h.finalize() {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Feed one length-prefixed field into the digest.
fn hash_field(h: &mut sha2::Sha256, s: &str) {
    use sha2::Digest;
    h.update((s.len() as u64).to_le_bytes());
    h.update(s.as_bytes());
}

/// Claim `key` for this submit, or discover what already holds it.
///
/// The claim is inserted **before** the run exists, which is what makes two
/// concurrent identical submits resolve to one run. A lookup-then-insert would
/// let both miss and both create; here exactly one `INSERT` wins and the loser
/// reads the winner's row.
///
/// The claim is scoped to `principal` (the authenticated caller): the key is
/// that caller's private handle on its own retry, so two principals sharing one
/// engine never collide on a key string one of them never issued.
pub(crate) async fn claim_idempotency_key(
    state: &AppState,
    principal: &str,
    key: &str,
    fingerprint: &str,
) -> Result<Claim, (StatusCode, String)> {
    let now = chrono::Utc::now();
    let stale = (now - chrono::Duration::seconds(IN_FLIGHT_CLAIM_SECS)).to_rfc3339();
    let now = now.to_rfc3339();

    // `DO UPDATE ... WHERE` makes stale-claim recovery part of the same atomic
    // statement: the row is taken over only if it is still unresolved *and*
    // old. `RETURNING` yields a row only when the insert or the update actually
    // happened, so "did I win" needs no second read.
    let won: Option<String> = sqlx::query_scalar(
        "INSERT INTO run_idempotency (principal, idempotency_key, run_id, fingerprint, created_at)
         VALUES ($1, $2, NULL, $3, $4)
         ON CONFLICT (principal, idempotency_key) DO UPDATE
            SET fingerprint = EXCLUDED.fingerprint, created_at = EXCLUDED.created_at
          WHERE run_idempotency.run_id IS NULL
            AND run_idempotency.created_at < $5
         RETURNING idempotency_key",
    )
    .bind(principal)
    .bind(key)
    .bind(fingerprint)
    .bind(&now)
    .bind(&stale)
    .fetch_optional(&state.write_pool)
    .await
    .map_err(internal_msg)?;

    if won.is_some() {
        return Ok(Claim::Held(now));
    }
    key_holder(state, principal, key, fingerprint).await.map(Claim::Replay)
}

/// The run `key` already names for `principal`, for a request that does not
/// hold the key: a repeat of the request the key was claimed for is answered
/// with that request's run. Anything else is a 409: the key was used for a
/// different request, or the submit that holds it has not finished.
async fn key_holder(
    state: &AppState,
    principal: &str,
    key: &str,
    fingerprint: &str,
) -> Result<String, (StatusCode, String)> {
    let existing: Option<(Option<String>, String)> = sqlx::query_as(
        "SELECT run_id, fingerprint FROM run_idempotency WHERE principal = $1 AND idempotency_key = $2",
    )
    .bind(principal)
    .bind(key)
    // The primary, not a replica: this read resolves the outcome of the write
    // above in the same logical operation. Reading a replica could observe a
    // stale snapshot — a resolved claim still showing run_id NULL, or a row not
    // yet arrived — and answer 409 to a caller whose run already exists, which
    // is exactly the failure the header is meant to remove.
    .fetch_optional(&state.write_pool)
    .await
    .map_err(internal_msg)?;

    match existing {
        // Fingerprint first: a key reused for a *different* body is a client
        // bug, and it must be reported as one whether or not the first
        // submission has finished.
        Some((_, fp)) if fp != fingerprint => {
            Err(key_conflict(key, "already used for a different request body"))
        }
        Some((Some(run_id), _)) => Ok(run_id),
        // Claimed, not yet resolved, and not yet stale: an identical submit is
        // in flight. 409 rather than a wait — the caller retries and gets the
        // run id, which is cheaper than holding a connection open here.
        Some((None, _)) => {
            Err(key_conflict(key, "a submission with this key is in flight; retry"))
        }
        // The row vanished between the two statements — only reachable if the
        // named run was purged concurrently. Treat it as contention.
        None => Err(key_conflict(key, "raced with a concurrent submission; retry")),
    }
}

/// Drop a held claim whose submit failed, so the key is usable again.
///
/// Guarded on `run_id IS NULL` and on `claimed_at`, the `created_at` this
/// request's claim wrote: a resolved claim can never be deleted by this, and
/// neither can the claim of a later submit that took the key over.
pub(crate) async fn release_idempotency_claim(
    state: &AppState,
    principal: &str,
    key: &str,
    claimed_at: &str,
) {
    if let Err(e) = sqlx::query(
        "DELETE FROM run_idempotency
          WHERE principal = $1 AND idempotency_key = $2 AND run_id IS NULL AND created_at = $3",
    )
    .bind(principal)
    .bind(key)
    .bind(claimed_at)
    .execute(&state.write_pool)
    .await
    {
        // Left behind, the claim ages out via IN_FLIGHT_CLAIM_SECS rather than
        // poisoning the key, so this is a log line and not a failed request.
        tracing::error!(error = ?e, %key, "failed to release idempotency claim");
    }
}

/// What a submit made under an `Idempotency-Key` came to.
pub(crate) enum Keyed {
    /// This request created the run: 201.
    Created(String),
    /// The key already named a run, which is the answer: 200, and nothing
    /// was created.
    Replayed(String),
}

/// [`submit_yaml_triggered`] under an `Idempotency-Key`, for both routes that
/// take one. The key is claimed for the caller, and the claim is resolved to
/// the run by the write that creates it, in one transaction
/// (`dagron_core::db::create_run_claimed`).
///
/// It was two steps: the run was created, and the claim was pointed at it by a
/// statement of its own afterwards, whose failure was only logged. A
/// submitter that died in between, or whose second statement failed, left a
/// run with an unresolved claim; once that went stale the caller's retry took
/// the key over and made a second run. A submit slower than the stale window
/// did the same without any failure at all.
///
/// - A refused submit gives the key back, as before.
/// - A submit whose claim was taken over while it was in flight creates
///   nothing, and is answered from what holds the key now: the other submit's
///   run, or a 409 while that one is still in flight.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn submit_yaml_keyed(
    state: &AppState,
    yaml: &str,
    authored_yaml: &str,
    caller_params: &BTreeMap<String, String>,
    triggered_by: &str,
    trigger: Option<&dagron_core::models::RunTrigger>,
    principal: &str,
    key: &str,
    fingerprint: &str,
) -> Result<Keyed, (StatusCode, String)> {
    let claimed_at = match claim_idempotency_key(state, principal, key, fingerprint).await? {
        // 200, not 201: nothing was created. The status is the whole signal
        // here — anything counting created runs in front of this endpoint
        // (a proxy, a quota, a meter) reads 201 and must not be told a
        // replay is a new run.
        Claim::Replay(run_id) => return Ok(Keyed::Replayed(run_id)),
        Claim::Held(claimed_at) => claimed_at,
    };
    let claim = dagron_core::db::IdempotencyClaim { principal, key, claimed_at: &claimed_at };
    // A submit that names no trigger is its caller's own.
    let manual;
    let trigger = match trigger {
        Some(t) => t,
        None => {
            manual = manual_trigger(triggered_by);
            &manual
        }
    };
    let created = match build_dag(state, yaml, caller_params, Some(trigger)).await {
        Ok(dag) => dagron_core::db::create_run_claimed(
            &state.write_pool,
            &dag,
            authored_yaml,
            Some(triggered_by),
            Some(trigger),
            &claim,
        )
        .await
        .map_err(create_run_refusal),
        Err(refusal) => Err(refusal),
    };
    match created {
        Ok(Some(run_id)) => Ok(Keyed::Created(run_id)),
        // The claim was not this request's any more by the time the run came
        // to be written, so no run was. Whoever holds the key has the answer.
        Ok(None) => key_holder(state, principal, key, fingerprint).await.map(Keyed::Replayed),
        Err(e) => {
            // A rejected submit must not burn the key. The caller fixed
            // nothing by retrying if the key it chose is now spent.
            release_idempotency_claim(state, principal, key, &claimed_at).await;
            Err(e)
        }
    }
}

/// The record of a run `who` asked this gateway for with no trigger of
/// their own to give: a person's, or a token of theirs.
pub(crate) fn manual_trigger(who: &str) -> dagron_core::models::RunTrigger {
    dagron_core::models::RunTrigger {
        kind: dagron_core::models::trigger_kind::MANUAL.to_string(),
        principal: format!("user:{who}"),
        actor: Some(who.to_string()),
        ..Default::default()
    }
}

/// `POST /api/runs` — submit a DAG
/// (`{ "yaml": "...", "parameters": { … } }`; `parameters` optional). Parses
/// and cycle-checks server-side (the authoritative validation), resolves any
/// `workflow_ref` chains into a flat DAG, then creates the run. The original
/// (un-expanded) YAML is stored as the run's definition.
///
/// Optionally idempotent. With an `Idempotency-Key` header, a repeat of
/// the same submit returns the **same** `run_id` with `200 OK` instead of
/// creating a second run; a first submit still answers `201 Created`. Without
/// the header the endpoint behaves exactly as before, so no existing caller
/// changes.
pub async fn submit_run(
    auth: AuthUser,
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(body): Json<SubmitBody>,
) -> Result<(StatusCode, Json<SubmitResponse>), (StatusCode, String)> {
    parse_and_validate(&body.yaml)?;

    // Idempotency claims are scoped to the authenticated caller: the key names
    // *this* principal's retry, never a run some other caller submitted under
    // the same string.
    let principal = auth.0.sub.as_str();
    let triggerer = caller_identity(&auth.0);
    // Refused before anything is written, as a malformed key is. The trigger is
    // not part of the request's fingerprint: two events for one commit carry
    // one key and two event ids, and the second is a replay of the first run,
    // whose record is the first event's.
    let trigger = run_trigger(&auth.0, body.trigger.as_ref())?;

    // Validated before anything is written, so a malformed key never leaves a
    // run behind that its caller cannot find again by retrying.
    let Some(key) = idempotency_key(&headers)? else {
        let run_id = submit_yaml_triggered(
            &state, &body.yaml, &body.yaml, &body.parameters, &triggerer, trigger.as_ref(),
        )
        .await?;
        return Ok((StatusCode::CREATED, Json(SubmitResponse { run_id })));
    };

    let fingerprint = request_fingerprint(&body);
    let submitted = submit_yaml_keyed(
        &state, &body.yaml, &body.yaml, &body.parameters, &triggerer, trigger.as_ref(),
        principal, &key, &fingerprint,
    )
    .await?;
    Ok(match submitted {
        Keyed::Created(run_id) => (StatusCode::CREATED, Json(SubmitResponse { run_id })),
        Keyed::Replayed(run_id) => (StatusCode::OK, Json(SubmitResponse { run_id })),
    })
}

/// Create a run from spec YAML: inline `workflow_ref` chains, fold the declared
/// environment's variables into the substitution scope, then hand the result to
/// the **engine's** parser and run writer.
///
/// `authored_yaml` is what gets stored as the run's definition (the spec a human
/// wrote, chains unresolved), while `yaml` is what actually runs.
///
/// This exists because dagron-api used to parse tasks into its own mirror of
/// `dag::TaskSpec` and write `task_runs` itself. Both mirrors drifted: the spec
/// mirror carried 18 of 35 fields, so submitting through the console silently
/// dropped `with_items`, `resources`, `wait`, `cache`, `pool`, `priority`,
/// `produces`, `workflow` and more — a `with_items` task ran once with `{{ item }}`
/// unsubstituted, and a wait sensor was dispatched to a worker where it failed
/// instead of parking. The row writer drifted separately (#657's `is_approval`).
/// Routing both through `dagron_core` makes that class of bug impossible: one
/// parser, one writer, shared with the engine.
#[cfg(test)]
pub(crate) async fn submit_yaml(
    state: &AppState,
    yaml: &str,
    authored_yaml: &str,
) -> Result<String, (StatusCode, String)> {
    submit_yaml_with_params(state, yaml, authored_yaml, &BTreeMap::new()).await
}

/// [`submit_yaml`], plus caller-supplied `parameters` — arguments for the
/// spec's declared `parameters:` block.
///
/// This is what makes a stored workflow callable as a *function*: the caller
/// names the workflow and passes arguments, instead of fetching its spec,
/// splicing values in on the client, and submitting the result as new YAML.
/// Every caller — the CLI, an SDK, curl, the console — gets the same path, and
/// the substitution stays in one place (the engine's), not two.
///
/// **Precedence: the declared environment wins.** Caller parameters go in
/// first, then `environment_params` on top, so a caller cannot shadow an
/// `env.NAME` key with a value of its own. The environment is a property of the
/// spec and the deployment; letting a request override it would make a declared
/// environment advisory.
///
/// Overrides beat the spec's own `parameters:` defaults (that is
/// `from_yaml_with_params`' documented behaviour), and keys the spec never
/// references are harmless.
#[cfg(test)]
pub(crate) async fn submit_yaml_with_params(
    state: &AppState,
    yaml: &str,
    authored_yaml: &str,
    caller_params: &BTreeMap<String, String>,
) -> Result<String, (StatusCode, String)> {
    submit_yaml_as(state, yaml, authored_yaml, caller_params, None).await
}

/// [`submit_yaml_with_params`] for a run `triggered_by` asked for, recorded as
/// theirs ([`manual_trigger`]). `None` writes no record, which no route does:
/// it is what a run from before the record looks like, for the tests of one.
#[cfg(test)]
pub(crate) async fn submit_yaml_as(
    state: &AppState,
    yaml: &str,
    authored_yaml: &str,
    caller_params: &BTreeMap<String, String>,
    triggered_by: Option<&str>,
) -> Result<String, (StatusCode, String)> {
    match triggered_by {
        Some(who) => {
            submit_yaml_triggered(state, yaml, authored_yaml, caller_params, who, None).await
        }
        None => {
            let dag = build_dag(state, yaml, caller_params, None).await?;
            dagron_core::db::create_run(&state.write_pool, &dag, authored_yaml)
                .await
                .map_err(create_run_refusal)
        }
    }
}

/// [`submit_yaml_recorded`] for a run `triggered_by` submitted, with the
/// trigger record the submit carried ([`run_trigger`]), or as their own
/// where it carried none ([`manual_trigger`]).
pub(crate) async fn submit_yaml_triggered(
    state: &AppState,
    yaml: &str,
    authored_yaml: &str,
    caller_params: &BTreeMap<String, String>,
    triggered_by: &str,
    trigger: Option<&dagron_core::models::RunTrigger>,
) -> Result<String, (StatusCode, String)> {
    let manual;
    let trigger = match trigger {
        Some(t) => t,
        None => {
            manual = manual_trigger(triggered_by);
            &manual
        }
    };
    submit_yaml_recorded(
        state,
        yaml,
        authored_yaml,
        caller_params,
        Some(triggered_by),
        trigger,
    )
    .await
}

/// Create a run from spec YAML as [`submit_yaml`] describes, with what
/// started it (`trigger`), written with the run or not at all. Every run this
/// gateway makes is made here or under an `Idempotency-Key`
/// ([`submit_yaml_keyed`]), and each has a record.
///
/// `triggered_by` is the caller, where the gateway knows one by name; it is
/// kept on the run as well, as it was before a run had a record.
pub(crate) async fn submit_yaml_recorded(
    state: &AppState,
    yaml: &str,
    authored_yaml: &str,
    caller_params: &BTreeMap<String, String>,
    triggered_by: Option<&str>,
    trigger: &dagron_core::models::RunTrigger,
) -> Result<String, (StatusCode, String)> {
    let dag = build_dag(state, yaml, caller_params, Some(trigger)).await?;
    let pool = &state.write_pool;
    let created = match triggered_by {
        Some(who) => {
            dagron_core::db::create_run_triggered_by(pool, &dag, authored_yaml, who, trigger).await
        }
        None => dagron_core::db::create_run_triggered(pool, &dag, authored_yaml, trigger).await,
    };
    created.map_err(create_run_refusal)
}

/// The identity a session is recorded and matched under: its email, else its
/// subject.
pub(crate) fn caller_identity(claims: &crate::auth::SessionClaims) -> String {
    if claims.email.is_empty() { claims.sub.clone() } else { claims.email.clone() }
}

/// Everything [`submit_yaml_with_params`] does before it writes: parse, fold in
/// the caller's parameters and the declared environment, inline `workflow_ref`
/// chains, and run the engine's parser. Reads only, so a caller that writes the
/// run some other way (dead-letter redrive) builds it with the same rules and
/// before opening its own transaction.
///
/// `trigger` is what the run will be recorded as started by. Each
/// environment the spec names is asked its policy with it
/// (`dagron_core::envpolicy`): a run that may not use one is refused here
/// (403), before its variables are put into anything that is stored. `None`
/// is a run with no record, which is this installation's own.
pub(crate) async fn build_dag(
    state: &AppState,
    yaml: &str,
    caller_params: &BTreeMap<String, String>,
    trigger: Option<&dagron_core::models::RunTrigger>,
) -> Result<dagron_core::dag::DagGraph, (StatusCode, String)> {
    // One Value parse serves both the environment-key read and the
    // `workflow_ref` check (LOW_LATENCY R-3) — previously each step re-parsed
    // the full document. Reading `environment` from the PRE-expansion document
    // is deliberate and equivalent: expansion only rewrites `tasks:` (inlined
    // children's top-level keys are never merged), so the key cannot change.
    let root: serde_yaml::Value = serde_yaml::from_str(yaml)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid YAML: {e}")))?;
    // The run's facts: its record's class and ref, which are the gateway's
    // and the CI service's word, and the workflow's own name.
    let pipeline = root
        .get("name")
        .and_then(serde_yaml::Value::as_str)
        .unwrap_or_default();
    let facts = dagron_core::envpolicy::RunFacts::of(trigger, pipeline);
    let mut params = caller_params.clone();
    params.extend(environment_params(state, &root, trigger, &facts).await?);
    // Library imports first: a library's templates join this spec's own before
    // the engine parses it, and a chained workflow is inlined from the merged text.
    let (root, yaml) = crate::expand::resolve_template_uses(state, root, yaml).await?;
    let expanded = crate::expand::expand_workflow_refs(state, root, &yaml).await?;
    // The engine's own parse → expand (task_defaults, parameters, templates,
    // fan-out, submit-time `when:`) → validate pipeline.
    let dag = dagron_core::dag::DagGraph::from_yaml_with_params(&expanded, &params).map_err(|e| {
        // A budget refusal is not a malformed spec, and must not read as one.
        // "invalid DAG: workflow 'x' would create 900 tasks…" tells an author to
        // go looking for a syntax error that isn't there.
        if let Some(b) = e.downcast_ref::<dagron_core::models::TaskBudgetExceeded>() {
            return (StatusCode::BAD_REQUEST, b.to_string());
        }
        // Same reasoning for the external ceiling: a declared budget the spec
        // exceeds is a deliberate, informative refusal, not a parse failure.
        if let Some(b) = e.downcast_ref::<dagron_core::models::ExternalBudgetExceeded>() {
            return (StatusCode::BAD_REQUEST, b.to_string());
        }
        // A parameter value the workflow's `param_schema` refuses is the
        // caller's input, not a broken spec: say which parameter and why.
        if let Some(p) = e.downcast_ref::<dagron_core::models::ParameterInvalid>() {
            return (StatusCode::BAD_REQUEST, p.to_string());
        }
        (StatusCode::BAD_REQUEST, format!("invalid DAG: {e}"))
    })?;
    // A run that is not trusted is confined (`dagron_core::confine`): one whose
    // workflow names what such a run may not use is refused, in the words its
    // making would refuse it with, and before anything is claimed for it.
    if dagron_core::confine::confined(&facts.trust) {
        if let Some((what, why)) = dagron_core::confine::refusal(&dag.spec, &facts.trust) {
            let refused = dagron_core::confine::ConfinedRefused {
                class: facts.trust.clone(),
                what,
                why,
            };
            return Err((StatusCode::FORBIDDEN, refused.to_string()));
        }
    }
    // A task may name an environment of its own for its secrets. Each is
    // asked here as well, and has to be there: the engine would refuse the
    // task its secrets at dispatch, and a run that is going to fail so is
    // better not made.
    for name in dagron_core::envpolicy::named_by(&dag) {
        if dag.spec.environment.as_deref() == Some(name.as_str()) {
            continue;
        }
        admit_environment(state, &name, trigger, &facts).await?;
        // A repository's need not be made: its secrets read as empty where
        // there is none, as GitHub's undefined secret does.
        if dagron_core::envpolicy::is_repository_environment(&name) {
            continue;
        }
        let there: Option<i32> = sqlx::query_scalar("SELECT 1 FROM environments WHERE name = $1")
            .bind(&name)
            .fetch_optional(&state.read_pool)
            .await
            .map_err(internal_msg)?;
        if there.is_none() {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("environment '{name}' not found"),
            ));
        }
    }
    Ok(dag)
}

/// Whether `environment` admits a run of these facts, by the policy stored
/// for it: 403 with the reason where it does not. A policy that cannot be
/// read is a 500, and never an admission. A repository's environment is
/// that repository's runs' alone, as the engine holds it
/// (`envpolicy::repository_admits`), whatever its policy.
async fn admit_environment(
    state: &AppState,
    environment: &str,
    trigger: Option<&dagron_core::models::RunTrigger>,
    facts: &dagron_core::envpolicy::RunFacts,
) -> Result<(), (StatusCode, String)> {
    dagron_core::envpolicy::repository_admits(environment, trigger)
        .map_err(|refused| (StatusCode::FORBIDDEN, refused.to_string()))?;
    // Read from the database that is written to. A policy is a gate: a
    // replica that had not caught up with one made stricter would admit the
    // run on the old one.
    let policy = dagron_core::db::environment_policy(&state.write_pool, environment)
        .await
        .map_err(|e| {
            tracing::error!(environment, error = ?e, "an environment's policy could not be read");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal server error".to_string(),
            )
        })?;
    dagron_core::envpolicy::admits(environment, policy.as_ref(), facts)
        .map_err(|refused| (StatusCode::FORBIDDEN, refused.to_string()))
}

/// The answer for a failed `dagron_core::db::create_run*` write.
pub(crate) fn create_run_refusal(e: anyhow::Error) -> (StatusCode, String) {
    // A per-workflow concurrency cap is a capacity condition, not a failure:
    // answer 429 like the engine's ops API, never 500. (No Retry-After here —
    // every handler on this path shares the plain (StatusCode, String) error
    // type, which doesn't carry headers; the engine's equivalent is a single
    // handler free to return a header-bearing tuple directly.)
    if let Some(m) = e.downcast_ref::<dagron_core::models::MaxActiveRunsReached>() {
        let scope = m.key.as_deref().map(|k| format!(" and concurrency_key '{k}'")).unwrap_or_default();
        // A workflow whose runs wait refuses one where as many wait as may.
        let waiting = match m.waiting {
            0 => String::new(),
            waiting => format!(", {waiting} of them waiting: no more may wait"),
        };
        return (
            StatusCode::TOO_MANY_REQUESTS,
            format!(
                "max_active_runs reached for workflow '{}'{scope} ({} active{waiting}, cap {})",
                m.name, m.active, m.max
            ),
        );
    }
    // A run that is not trusted named what such a run may not use: the
    // caller's to know, and no fault of this service.
    if let Some(refused) = e.downcast_ref::<dagron_core::confine::ConfinedRefused>() {
        return (StatusCode::FORBIDDEN, refused.to_string());
    }
    tracing::error!(error = ?e, "create_run failed");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal server error".to_string())
}

/// The declared environment's variables as `env.NAME` substitution keys.
///
/// The engine resolves an environment's *secrets* at dispatch; its plain
/// variables are substituted at submit, which is this gateway's job. Feeding
/// them in as parameter overrides means the engine's substitution does the work
/// — no second implementation of `{{ }}`.
///
/// The environment's policy is asked first, with the run's `facts`: a run
/// it does not admit is given none of its variables, and is not told
/// whether there is such an environment.
async fn environment_params(
    state: &AppState,
    doc: &serde_yaml::Value,
    trigger: Option<&dagron_core::models::RunTrigger>,
    facts: &dagron_core::envpolicy::RunFacts,
) -> Result<std::collections::BTreeMap<String, String>, (StatusCode, String)> {
    let mut out = std::collections::BTreeMap::new();
    let Some(env_name) = doc.get("environment").and_then(serde_yaml::Value::as_str) else {
        return Ok(out);
    };
    admit_environment(state, env_name, trigger, facts).await?;
    let vars: Option<String> =
        sqlx::query_scalar("SELECT variables FROM environments WHERE name = $1")
            .bind(env_name)
            .fetch_optional(&state.read_pool)
            .await
            .map_err(internal_msg)?;
    // A declared but unknown environment is a 400 — a spec pinned to prod must
    // not silently run without prod's variables.
    let Some(vars) = vars else {
        // A repository's that was never made has no variables.
        if dagron_core::envpolicy::is_repository_environment(env_name) {
            return Ok(out);
        }
        return Err((StatusCode::BAD_REQUEST, format!("environment '{env_name}' not found")));
    };
    let vars: std::collections::BTreeMap<String, String> =
        serde_json::from_str(&vars).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("environment '{env_name}' has malformed variables: {e}"),
            )
        })?;
    for (k, v) in vars {
        out.insert(format!("env.{k}"), v);
    }
    Ok(out)
}

/// `POST /api/runs/:id/resubmit` — start a **fresh** run from this run's stored
/// definition (re-trigger). Unlike `rerun` (which resumes a failed/cancelled run
/// in place), this works for any run — including a succeeded one — and produces a
/// brand-new run_id.
///
/// The new run is created with the parameters this one was created with
/// ([`resubmit_parameters`]). The stored definition holds only the spec's
/// defaults, so a resubmit used to run as if nobody had passed any: a run
/// submitted with `commit_sha` resubmitted with it empty, and one whose
/// parameters fill in a field the spec cannot run without was refused.
pub async fn resubmit_run(
    auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<SubmitResponse>), (StatusCode, String)> {
    // The definition and the parameters in one read, so that both are of the
    // same run on a replica that is behind.
    let row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT d.spec, r.parameters FROM workflow_runs r
         JOIN workflow_definitions d ON d.id = r.definition_id
         WHERE r.id = $1",
    )
    .bind(&id)
    .fetch_optional(&state.read_pool)
    .await
    .map_err(internal_msg)?;
    let (yaml, stored) = row.ok_or((StatusCode::NOT_FOUND, format!("run '{id}' not found")))?;

    parse_and_validate(&yaml)?;
    let params = resubmit_parameters(&id, &yaml, stored.as_deref())?;
    // A new run, and its caller's: what the run it copies was for is that
    // run's to say, and this one says which run that is. But for its class,
    // where that run was not trusted: what it ran was not this caller's to
    // write, and running it again does not make it so. Read from the
    // database that is written to, as a gate is.
    let triggerer = caller_identity(&auth.0);
    let original = dagron_core::db::run_trigger(&state.write_pool, &id)
        .await
        .map_err(|e| {
            tracing::error!(run = %id, error = ?e, "a run's record could not be read");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal server error".to_string(),
            )
        })?;
    let trigger = manual_trigger(&triggerer)
        .from_source(format!("resubmit:{id}"))
        .copying(original.as_ref());
    let run_id =
        submit_yaml_recorded(&state, &yaml, &yaml, &params, Some(&triggerer), &trigger).await?;
    Ok((StatusCode::CREATED, Json(SubmitResponse { run_id })))
}

/// The parameters a resubmit of run `id` passes: the ones the run was created
/// with (`workflow_runs.parameters`, its spec's defaults with its caller's
/// values over them), from the spec `yaml` and what is `stored`.
///
/// - **None stored:** a run from before the column existed. It resubmits with
///   its spec's defaults, which is all such a run ever had.
/// - **A declared environment is read again.** Its variables were folded into
///   what was stored, as `env.NAME`. They are not the caller's: a resubmit
///   takes the environment as it is now, like any new run, so a variable since
///   changed is the new value and one since removed is gone. Without a
///   declared environment an `env.` key can only be the caller's own, and is
///   carried like the rest.
/// - **Unreadable:** refused. A run must not go out without the parameters it
///   is said to be a resubmit with.
fn resubmit_parameters(
    id: &str,
    yaml: &str,
    stored: Option<&str>,
) -> Result<BTreeMap<String, String>, (StatusCode, String)> {
    let Some(stored) = stored else {
        return Ok(BTreeMap::new());
    };
    let mut params: BTreeMap<String, String> = serde_json::from_str(stored).map_err(|e| {
        tracing::error!(run_id = %id, error = %e, "a run's stored parameters cannot be read");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("run '{id}' cannot be resubmitted: its stored parameters cannot be read"),
        )
    })?;
    let declares_environment = serde_yaml::from_str::<serde_yaml::Value>(yaml)
        .ok()
        .and_then(|doc| doc.get("environment").and_then(|e| e.as_str().map(str::to_owned)))
        .is_some();
    if declares_environment {
        params.retain(|name, _| !name.starts_with("env."));
    }
    Ok(params)
}

/// Submit-time preparation, run after `workflow_ref` expansion and before
/// [`create_run`]: merge `task_defaults` into every task, build the `{{ }}`
/// substitution scope (spec `parameters` + the declared environment's
/// variables as `env.*` keys), evaluate parameter-time `when:` conditions
/// (false ⇒ the task is dropped and scrubbed from dependents' `depends_on`,
/// so they proceed on their remaining deps — skip semantics), and substitute
/// the scope into task fields. Runtime `when:` conditions (referencing
/// `tasks.*.output`) survive verbatim for the engine.
pub(crate) async fn prepare_spec(
    state: &AppState,
    mut spec: DagSpecInput,
) -> Result<DagSpecInput, (StatusCode, String)> {
    // task_defaults merge (same rules as engine expand::apply_task_defaults).
    if let Some(d) = spec.task_defaults.take() {
        for t in spec.tasks.iter_mut() {
            if t.max_attempts == 1 {
                if let Some(v) = d.max_attempts {
                    t.max_attempts = v;
                }
            }
            if t.retry_delay_secs == 0 {
                if let Some(v) = d.retry_delay_secs {
                    t.retry_delay_secs = v;
                }
            }
            if t.retry_max_delay_secs.is_none() {
                t.retry_max_delay_secs = d.retry_max_delay_secs;
            }
            if t.timeout_secs.is_none() {
                t.timeout_secs = d.timeout_secs;
            }
            if t.docker_image.is_none() {
                t.docker_image = d.docker_image.clone();
            }
            if t.runner_class.is_none() {
                t.runner_class = d.runner_class.clone();
            }
            if !d.env.is_empty() {
                let mut env = d.env.clone();
                env.append(&mut t.env);
                t.env = env;
            }
        }
    }

    // Substitution scope: parameters + `env.NAME` variables. A declared but
    // unknown environment is a 400 — a spec pinned to prod must not silently
    // run without prod's variables.
    let mut ctx = spec.parameters.clone();
    if let Some(env_name) = &spec.environment {
        let vars: Option<String> =
            sqlx::query_scalar("SELECT variables FROM environments WHERE name = $1")
                .bind(env_name)
                .fetch_optional(&state.read_pool)
                .await
                .map_err(internal_msg)?;
        let Some(vars) = vars else {
            return Err((StatusCode::BAD_REQUEST, format!("environment '{env_name}' not found")));
        };
        // Malformed variables fail as loudly as an unknown environment — a spec
        // pinned to prod must not silently run without prod's variables.
        let vars: std::collections::BTreeMap<String, String> =
            serde_json::from_str(&vars).map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("environment '{env_name}' has malformed variables: {e}"),
                )
            })?;
        for (k, v) in vars {
            ctx.insert(format!("env.{k}"), v);
        }
    }

    // Parameter-time `when:`: decide task fate now; keep runtime gates.
    let mut dropped: HashSet<String> = HashSet::new();
    for t in spec.tasks.iter_mut() {
        let Some(cond) = &t.when else { continue };
        let resolved = crate::tmpl::substitute(cond, &ctx);
        if crate::tmpl::when_output_refs(&resolved).is_empty() {
            let keep = crate::tmpl::eval_when(&resolved).map_err(|e| {
                (StatusCode::BAD_REQUEST, format!("task '{}' when: {e}", t.name))
            })?;
            if !keep {
                dropped.insert(t.name.clone());
            }
            t.when = None; // consumed at submit
        } else {
            t.when = Some(resolved); // runtime gate, engine evaluates
        }
    }
    if !dropped.is_empty() {
        spec.tasks.retain(|t| !dropped.contains(&t.name));
        for t in spec.tasks.iter_mut() {
            t.depends_on.retain(|d| !dropped.contains(d));
        }
        // Re-validate: a surviving runtime `when` may reference a task that was
        // just dropped (its dep got scrubbed above) — that gate could never be
        // evaluated, so reject it at submit rather than strand the run.
        validate_graph(&spec.name, &spec.tasks, &spec.templates, !spec.uses.is_empty())?;
    }

    // Substitute the scope into task string fields (mirror of core build_leaf
    // coverage for the fields this path models).
    if !ctx.is_empty() {
        for t in spec.tasks.iter_mut() {
            t.command = t.command.iter().map(|a| crate::tmpl::substitute(a, &ctx)).collect();
            t.docker_image = t.docker_image.as_ref().map(|s| crate::tmpl::substitute(s, &ctx));
            t.runner_class = t.runner_class.as_ref().map(|s| crate::tmpl::substitute(s, &ctx));
            for e in t.env.iter_mut() {
                e.value = crate::tmpl::substitute(&e.value, &ctx);
            }
            if let Some(rep) = t.repeat.as_mut() {
                rep.until = crate::tmpl::substitute(&rep.until, &ctx);
            }
        }
    }

    Ok(spec)
}

/// Insert definition + run + tasks + edges, mirroring engine `db::postgres::create_run`.
pub(crate) async fn create_run(
    state: &AppState,
    spec: &DagSpecInput,
    yaml: &str,
) -> anyhow::Result<String> {
    let def_id = Uuid::new_v4().to_string();
    let run_id = Uuid::new_v4().to_string();
    let created = chrono::Utc::now();
    let now = created.to_rfc3339();
    // Mirrors engine create_run: persist the absolute run deadline so the
    // engine's sweep enforces `run_timeout_secs` on UI-submitted runs too.
    let deadline_at = spec
        .run_timeout_secs
        .map(|secs| (created + chrono::TimeDelta::seconds(secs.min(i64::MAX as u64) as i64)).to_rfc3339());

    // in-degree per task = remaining_deps seed
    let mut indeg: HashMap<&str, i64> = spec.tasks.iter().map(|t| (t.name.as_str(), 0)).collect();
    for t in &spec.tasks {
        for _dep in &t.depends_on {
            *indeg.get_mut(t.name.as_str()).unwrap() += 1;
        }
    }

    let mut tx = state.write_pool.begin().await?;
    sqlx::query("INSERT INTO workflow_definitions (id, name, spec, created_at) VALUES ($1,$2,$3,$4)")
        .bind(&def_id).bind(&spec.name).bind(yaml).bind(&now)
        .execute(&mut *tx).await?;
    sqlx::query("INSERT INTO workflow_runs (id, definition_id, status, created_at, deadline_at, result_from, environment) VALUES ($1,$2,'running',$3,$4,$5,$6)")
        .bind(&run_id).bind(&def_id).bind(&now).bind(&deadline_at).bind(&spec.result_from).bind(&spec.environment)
        .execute(&mut *tx).await?;

    let mut ids: HashMap<String, String> = HashMap::new();
    for t in &spec.tasks {
        let task_id = Uuid::new_v4().to_string();
        let input_json = serde_json::to_string(t)?; // matches dag::TaskSpec shape
        let trigger_rule = t.trigger_rule.as_deref().unwrap_or("all_success");
        // Approval gate (#19): the engine's advance_ready_tasks keys off the
        // `is_approval` COLUMN (not the input JSON) to park a ready task in
        // `awaiting_approval` instead of dispatching it. Engine create_run sets
        // it; this API mirror must too, or a `type: approval` task submitted via
        // POST /api/runs lands with is_approval=0 and the executor runs it as a
        // command-less task ("empty command" failure). Timeout knobs pair with it.
        let is_approval = i64::from(t.is_approval());
        let approval_timeout = t.approval_timeout_secs.map(|s| s as i64);
        let approval_on_timeout = t.approval_on_timeout.as_deref();
        // Task → spec-level → 'default', mirroring engine create_run: the
        // segmented claim path filters on this COLUMN, not the input JSON.
        let runner_class = t
            .runner_class
            .as_deref()
            .or(spec.runner_class.as_deref())
            .unwrap_or("default");
        sqlx::query(
            "INSERT INTO task_runs (id, run_id, name, status, remaining_deps, input, scheduled_at, trigger_rule,
                                    is_approval, approval_timeout_secs, approval_on_timeout, runner_class)
             VALUES ($1,$2,$3,'pending',$4,$5,$6,$7,$8,$9,$10,$11)",
        )
        .bind(&task_id).bind(&run_id).bind(&t.name)
        .bind(indeg[t.name.as_str()]).bind(&input_json).bind(&now).bind(trigger_rule)
        .bind(is_approval).bind(approval_timeout).bind(approval_on_timeout).bind(runner_class)
        .execute(&mut *tx).await?;
        ids.insert(t.name.clone(), task_id);
    }
    for t in &spec.tasks {
        let dependent = &ids[&t.name];
        for dep in &t.depends_on {
            sqlx::query("INSERT INTO task_dependencies (dependent_id, dependency_id) VALUES ($1,$2)")
                .bind(dependent).bind(&ids[dep])
                .execute(&mut *tx).await?;
        }
    }

    notify_tx(&mut tx, &run_id).await?;
    tx.commit().await?;
    Ok(run_id)
}

// ── helpers ─────────────────────────────────────────────────────────────────

/// NOTIFY inside a control handler's transaction; errors map to 500.
async fn notify(
    tx: &mut sqlx::Transaction<'_, crate::db::Db>,
    run_id: &str,
) -> Result<(), StatusCode> {
    notify_tx(tx, run_id).await.map_err(|e| {
        tracing::error!(error = ?e, "pg_notify failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

async fn notify_tx(
    tx: &mut sqlx::Transaction<'_, crate::db::Db>,
    run_id: &str,
) -> anyhow::Result<()> {
    #[cfg(feature = "postgres")]
    sqlx::query("SELECT pg_notify('task_events', $1)")
        .bind(run_id)
        .execute(&mut **tx)
        .await?;
    // SQLite has no NOTIFY: the SSE pump polls for what changed (`stream.rs`).
    #[cfg(feature = "sqlite")]
    let _ = (tx, run_id);
    Ok(())
}

fn internal(err: sqlx::Error) -> StatusCode {
    tracing::error!(error = ?err, "db query failed");
    StatusCode::INTERNAL_SERVER_ERROR
}

fn internal_msg(err: sqlx::Error) -> (StatusCode, String) {
    tracing::error!(error = ?err, "db query failed");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal server error".to_string())
}
