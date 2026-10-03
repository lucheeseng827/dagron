//! Observability + dead-letter endpoints for the authenticated UI edge.
//!
//! These surface engine-side ops capabilities (which `src/api.rs` also exposes
//! unauthenticated, in-process) through the JWT-gated public gateway, so the UI
//! has ONE coherent authed backend. Like control.rs the SQL is inlined and
//! mirrors the engine's `db` functions (dead_letters table, metrics gauges).
//! Redrive is the exception on the write side: it builds the run with
//! control.rs's submit pipeline and writes it through
//! `dagron_core::db::create_run_from_dead_letter`, which deletes the row in the
//! run's own transaction.

use std::collections::BTreeMap;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::auth::AuthUser;
use crate::routes::{artifacts, control};
use crate::state::AppState;

// ── Metrics (JSON gauges from the datastore) ────────────────────────────────

#[derive(Serialize, sqlx::FromRow)]
pub struct StatusCount {
    pub status: String,
    pub count: i64,
}

#[derive(Serialize)]
pub struct MetricsResponse {
    pub runs_by_status: Vec<StatusCount>,
    pub tasks_by_status: Vec<StatusCount>,
    pub dead_letters: i64,
}

/// `GET /api/metrics` — live run/task counts by status + dead-letter total.
/// JSON (UI-friendly) rather than the engine's Prometheus text at `/metrics`.
pub async fn metrics(
    _auth: AuthUser,
    State(state): State<AppState>,
) -> Result<Json<MetricsResponse>, StatusCode> {
    let runs = sqlx::query_as::<_, StatusCount>(
        "SELECT status, COUNT(*) AS count FROM workflow_runs GROUP BY status",
    )
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;
    let tasks = sqlx::query_as::<_, StatusCount>(
        "SELECT status, COUNT(*) AS count FROM task_runs GROUP BY status",
    )
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;
    let dead_letters: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dead_letters")
        .fetch_one(&state.read_pool)
        .await
        .map_err(internal)?;

    Ok(Json(MetricsResponse { runs_by_status: runs, tasks_by_status: tasks, dead_letters }))
}

// ── Metrics time-series ─────────────────────────────────────────────────────

#[derive(Serialize, sqlx::FromRow)]
pub struct DayBucket {
    /// Bucket day, `YYYY-MM-DD` (UTC).
    pub day: String,
    pub succeeded: i64,
    pub failed: i64,
    pub cancelled: i64,
    /// Still pending/running when queried.
    pub active: i64,
    /// Mean wall-clock of finished runs that day, seconds. Null = none finished.
    pub avg_duration_secs: Option<f64>,
    pub max_duration_secs: Option<f64>,
}

#[derive(Deserialize)]
pub struct TimeseriesParams {
    /// Look-back window in days (default 14, clamped to [1, 90]).
    pub days: Option<i64>,
    /// Restrict to one workflow (definition name, exact match).
    pub name: Option<String>,
}

/// `GET /api/metrics/timeseries?days=&name=` — per-day run counts by outcome and
/// duration stats, for the Metrics charts and the workflow detail trend.
/// Timestamps are stored as RFC-3339 TEXT, so buckets cast through timestamptz.
pub async fn metrics_timeseries(
    _auth: AuthUser,
    State(state): State<AppState>,
    Query(params): Query<TimeseriesParams>,
) -> Result<Json<Vec<DayBucket>>, StatusCode> {
    let days = params.days.unwrap_or(14).clamp(1, 90);
    let rows = sqlx::query_as::<_, DayBucket>(
        "SELECT to_char(date_trunc('day', wr.created_at::timestamptz), 'YYYY-MM-DD') AS day,
                COUNT(*) FILTER (WHERE wr.status = 'succeeded') AS succeeded,
                COUNT(*) FILTER (WHERE wr.status = 'failed') AS failed,
                COUNT(*) FILTER (WHERE wr.status = 'cancelled') AS cancelled,
                COUNT(*) FILTER (WHERE wr.status IN ('pending','running')) AS active,
                AVG(EXTRACT(EPOCH FROM (wr.finished_at::timestamptz - wr.created_at::timestamptz))::float8)
                    FILTER (WHERE wr.finished_at IS NOT NULL) AS avg_duration_secs,
                MAX(EXTRACT(EPOCH FROM (wr.finished_at::timestamptz - wr.created_at::timestamptz))::float8)
                    FILTER (WHERE wr.finished_at IS NOT NULL) AS max_duration_secs
         FROM workflow_runs wr
         LEFT JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE wr.created_at::timestamptz >= date_trunc('day', now()) - make_interval(days => $1::int)
           AND ($2::text IS NULL OR d.name = $2)
         GROUP BY 1 ORDER BY 1",
    )
    .bind(days)
    .bind(&params.name)
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;
    Ok(Json(rows))
}

// ── Pending approval gates ──────────────────────────────────────────────────

#[derive(Serialize)]
pub struct ApprovalArtifact {
    /// The `<task>/<name>` artifact key the workflow asked to show.
    pub path: String,
    /// Download URL on this API (`GET /api/runs/{run}/artifacts/{task}/{name}`).
    pub url: String,
    /// The gate's approval is bound to this artifact's exact bytes (`binds`).
    pub bound: bool,
    /// sha256 (hex) of a bound artifact right now; send it back as
    /// `digests[path]` to approve. `null` while the artifact is missing.
    pub sha256: Option<String>,
}

#[derive(Serialize)]
pub struct PendingApproval {
    pub run_id: String,
    pub task_id: String,
    pub task_name: String,
    pub workflow_name: Option<String>,
    /// When the gate parked (task scheduled_at), oldest first.
    pub since: Option<String>,
    /// The workflow author's note to the approver (`approval_message`).
    pub message: Option<String>,
    /// Artifacts to review before deciding (`approval_show`), e.g. a plan.
    pub show: Vec<ApprovalArtifact>,
    /// Who may decide this gate (`approvers`); empty = any authenticated user.
    pub approvers: Vec<String>,
    /// Whether the run's triggerer is barred from approving (`not_triggerer`).
    pub not_triggerer: bool,
    /// Whether the caller may approve / reject it — so the console can disable
    /// buttons the API would answer 403 to.
    pub can_approve: bool,
    pub can_reject: bool,
}

#[derive(sqlx::FromRow)]
struct PendingApprovalRow {
    run_id: String,
    task_id: String,
    task_name: String,
    workflow_name: Option<String>,
    since: Option<String>,
    input: Option<String>,
    triggered_by: Option<String>,
}

/// Percent-encode one URL path segment (everything but RFC 3986 unreserved).
fn encode_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

impl PendingApprovalRow {
    fn into_pending(self, me: &crate::auth::SessionClaims) -> PendingApproval {
        let authz: control::GateAuthz = self
            .input
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();
        let listed = dagron_core::dag::approver_permits(&authz.approvers, &me.email, &me.sub, &me.groups);
        let is_triggerer = self.triggered_by.as_deref().is_some_and(|t| {
            [me.email.as_str(), me.sub.as_str()]
                .iter()
                .any(|id| !id.is_empty() && id.eq_ignore_ascii_case(t))
        });
        let spec: serde_json::Value = self
            .input
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or(serde_json::Value::Null);
        let message = spec
            .get("approval_message")
            .and_then(|m| m.as_str())
            .map(str::to_owned);
        // Re-validate on read: `input` is data, and a URL is built from it.
        let listed_in = |field: &'static str| {
            spec.get(field)
                .and_then(|v| v.as_array())
                .into_iter()
                .flatten()
                .filter_map(|e| e.as_str())
                .filter(|e| dagron_core::dag::validate_approval_show(e).is_ok())
                .map(move |e| (e, field == "binds"))
        };
        let mut seen = std::collections::HashSet::new();
        let show = listed_in("binds")
            .chain(listed_in("approval_show"))
            .filter(|(e, _)| seen.insert(*e))
            .filter_map(|(e, bound)| {
                let (task, name) = e.split_once('/')?;
                Some(ApprovalArtifact {
                    path: e.to_string(),
                    url: format!(
                        "/api/runs/{}/artifacts/{}/{}",
                        encode_segment(&self.run_id),
                        encode_segment(task),
                        encode_segment(name)
                    ),
                    bound,
                    sha256: None,
                })
            })
            .collect();
        PendingApproval {
            run_id: self.run_id,
            task_id: self.task_id,
            task_name: self.task_name,
            workflow_name: self.workflow_name,
            since: self.since,
            message,
            show,
            can_approve: listed && !(authz.not_triggerer && is_triggerer),
            can_reject: listed,
            approvers: authz.approvers,
            not_triggerer: authz.not_triggerer,
        }
    }
}

/// `GET /api/approvals` — every task parked in `awaiting_approval`, oldest
/// first: the human-in-the-loop worklist behind the sidebar badge. Each entry
/// carries the gate's `approval_message` and `approval_show` artifact links so
/// the approver can see what they are approving.
pub async fn list_approvals(
    auth: AuthUser,
    State(state): State<AppState>,
) -> Result<Json<Vec<PendingApproval>>, StatusCode> {
    let rows = sqlx::query_as::<_, PendingApprovalRow>(
        "SELECT t.run_id, t.id AS task_id, t.name AS task_name,
                d.name AS workflow_name, t.scheduled_at AS since, t.input,
                wr.triggered_by
         FROM task_runs t
         JOIN workflow_runs wr ON wr.id = t.run_id
         LEFT JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE t.status = 'awaiting_approval'
         ORDER BY t.scheduled_at ASC NULLS FIRST",
    )
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;
    let mut pending: Vec<PendingApproval> = rows.into_iter().map(|r| r.into_pending(&auth.0)).collect();
    // Digests are read fresh each time: they are what an approval is checked
    // against, so a cached one could bless a plan that has since changed.
    for p in &mut pending {
        for a in p.show.iter_mut().filter(|a| a.bound) {
            if let Some((task, name)) = a.path.split_once('/') {
                let key = dagron_artifact::ArtifactKey::new(p.run_id.as_str(), task, name);
                a.sha256 = artifacts::artifact_sha256(&state, &key).await.ok().flatten();
            }
        }
    }
    Ok(Json(pending))
}

// ── Dead letters ────────────────────────────────────────────────────────────

#[derive(Serialize, sqlx::FromRow)]
pub struct DeadLetter {
    pub id: String,
    pub payload: String,
    pub error: String,
    pub source: String,
    pub failures: i64,
    pub first_seen_at: String,
    pub last_error_at: String,
}

#[derive(Deserialize)]
pub struct ListParams {
    pub limit: Option<i64>,
}

/// `GET /api/dead-letters?limit=` — parked poison submissions, newest first.
pub async fn list_dead_letters(
    _auth: AuthUser,
    State(state): State<AppState>,
    Query(params): Query<ListParams>,
) -> Result<Json<Vec<DeadLetter>>, StatusCode> {
    let limit = params.limit.unwrap_or(100).clamp(1, 500);
    // Newest-first by most-recent failure (last_error_at), not first_seen_at
    // (which is when the poison was first parked).
    let rows = sqlx::query_as::<_, DeadLetter>(
        "SELECT id, payload, error, source, failures, first_seen_at, last_error_at
         FROM dead_letters ORDER BY last_error_at DESC LIMIT $1",
    )
    .bind(limit)
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;
    Ok(Json(rows))
}

#[derive(Serialize)]
pub struct RedriveResponse {
    pub run_id: String,
    pub redriven_from: String,
}

/// `POST /api/dead-letters/:id/redrive` — re-submit a parked payload as a run.
///
/// All or nothing: the run is created and the dead letter deleted in one
/// transaction, so a redrive that fails (400 invalid spec or unknown
/// environment, 429 the workflow's run cap, 500) leaves the row exactly as it
/// was, with the same id and history, to redrive again once the cause is fixed.
/// Nothing is re-parked under a new id. The delete is the claim: concurrent
/// redrives of one id make at most one run, and the losers get 404, as does an
/// id already redriven or discarded.
pub async fn redrive_dead_letter(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<RedriveResponse>, (StatusCode, String)> {
    let dl = sqlx::query_as::<_, DeadLetter>(
        "SELECT id, payload, error, source, failures, first_seen_at, last_error_at
         FROM dead_letters WHERE id = $1",
    )
    .bind(&id)
    .fetch_optional(&state.write_pool)
    .await
    .map_err(|e| internal_msg(e))?
    .ok_or((StatusCode::NOT_FOUND, format!("dead letter '{id}' not found")))?;

    // Build the run exactly as submit would, before anything is written. These
    // are reads on the same pool the transaction below draws from, so they come
    // first rather than inside it: a redrive never holds one connection while
    // waiting for another.
    control::parse_and_validate(&dl.payload)?;
    let dag = control::build_dag(&state, &dl.payload, &BTreeMap::new()).await?;

    // Rows are never updated in place, so the payload read above is the one
    // being claimed.
    let claimed =
        dagron_core::db::create_run_from_dead_letter(&state.write_pool, &dag, &dl.payload, &id)
            .await
            .map_err(control::create_run_refusal)?;
    let Some(run_id) = claimed else {
        return Err((
            StatusCode::NOT_FOUND,
            format!("dead letter '{id}' was already redriven or discarded"),
        ));
    };

    // The row and its failure history are gone with the claim; the run keeps
    // only the payload, so the history is logged here.
    tracing::info!(
        dead_letter_id = %id, %run_id, source = %dl.source, failures = dl.failures,
        first_seen_at = %dl.first_seen_at, last_error_at = %dl.last_error_at, error = %dl.error,
        "dead letter redriven into a run"
    );
    Ok(Json(RedriveResponse { run_id, redriven_from: id }))
}

/// `DELETE /api/dead-letters/:id` — discard a parked payload. 404 if absent.
pub async fn delete_dead_letter(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let n = sqlx::query("DELETE FROM dead_letters WHERE id = $1")
        .bind(&id)
        .execute(&state.write_pool)
        .await
        .map_err(internal)?
        .rows_affected();
    if n == 0 {
        Err(StatusCode::NOT_FOUND)
    } else {
        Ok(StatusCode::NO_CONTENT)
    }
}

fn internal(err: sqlx::Error) -> StatusCode {
    tracing::error!(error = ?err, "db query failed");
    StatusCode::INTERNAL_SERVER_ERROR
}

fn internal_msg(err: sqlx::Error) -> (StatusCode, String) {
    tracing::error!(error = ?err, "db query failed");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string())
}

#[cfg(test)]
mod approval_authz_tests {
    use super::*;
    use crate::auth::SessionClaims;

    fn me(email: &str, groups: &[&str]) -> SessionClaims {
        SessionClaims {
            sub: format!("sub-{email}"),
            email: email.to_string(),
            name: String::new(),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            exp: 0,
        }
    }

    fn row(input: &str, triggered_by: Option<&str>) -> PendingApprovalRow {
        PendingApprovalRow {
            run_id: "r".into(),
            task_id: "t".into(),
            task_name: "gate".into(),
            workflow_name: None,
            since: None,
            input: Some(input.to_string()),
            triggered_by: triggered_by.map(str::to_string),
        }
    }

    #[test]
    fn worklist_reports_what_the_caller_may_do() {
        let gate = r#"{"approvers":["ops@example.com","group:release"],"not_triggerer":true}"#;

        let p = row(gate, Some("dev@example.com")).into_pending(&me("ops@example.com", &[]));
        assert!(p.can_approve && p.can_reject);
        assert_eq!(p.approvers.len(), 2);
        assert!(p.not_triggerer);

        // Listed but started the run: may reject, may not approve.
        let p = row(gate, Some("ops@example.com")).into_pending(&me("ops@example.com", &[]));
        assert!(!p.can_approve && p.can_reject);

        // Reaches the list through a group.
        let p = row(gate, None).into_pending(&me("x@example.com", &["release"]));
        assert!(p.can_approve && p.can_reject);

        // Not listed: neither.
        let p = row(gate, None).into_pending(&me("mallory@example.com", &["dev"]));
        assert!(!p.can_approve && !p.can_reject);

        // An unrestricted gate admits anyone.
        let p = row("{}", Some("dev@example.com")).into_pending(&me("dev@example.com", &[]));
        assert!(p.can_approve && p.can_reject && p.approvers.is_empty());
    }
}

/// The approval gate against a real datastore: who may decide, and what is
/// recorded. Skipped without `TEST_DATABASE_URL`.
#[cfg(test)]
mod approval_live_tests {
    use super::redrive_tests::test_state;
    use super::*;
    use crate::auth::SessionClaims;

    fn user(email: &str, groups: &[&str]) -> AuthUser {
        AuthUser(SessionClaims {
            sub: format!("sub-{email}"),
            email: email.to_string(),
            name: String::new(),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            exp: 0,
        })
    }

    /// A parked gate on a fresh run started by `dev@example.com`; returns
    /// `(run_id, task_id)`.
    async fn parked_gate(state: &AppState, name: &str) -> (String, String) {
        let yaml = format!(
            "name: {name}\ntasks:\n  - name: gate\n    type: approval\n    approvers: [\"ops@example.com\", \"group:release\"]\n    not_triggerer: true\n    approval_message: \"Apply?\"\n    approval_show: [\"plan/plan.txt\"]\n"
        );
        let run_id = control::submit_yaml_as(state, &yaml, &yaml, &BTreeMap::new(), Some("dev@example.com"))
            .await
            .unwrap();
        let task_id: String =
            sqlx::query_scalar("SELECT id FROM task_runs WHERE run_id = $1 AND name = 'gate'")
                .bind(&run_id)
                .fetch_one(&state.write_pool)
                .await
                .unwrap();
        // The engine parks it; no engine runs here.
        sqlx::query("UPDATE task_runs SET status = 'awaiting_approval' WHERE id = $1")
            .bind(&task_id)
            .execute(&state.write_pool)
            .await
            .unwrap();
        (run_id, task_id)
    }

    async fn decide(
        state: &AppState,
        who: AuthUser,
        run: &str,
        task: &str,
        approve: bool,
        comment: Option<&str>,
    ) -> Result<control::ApprovalResponse, (StatusCode, String)> {
        decide_with(state, who, run, task, approve, comment, &[]).await
    }

    async fn decide_with(
        state: &AppState,
        who: AuthUser,
        run: &str,
        task: &str,
        approve: bool,
        comment: Option<&str>,
        digests: &[(&str, &str)],
    ) -> Result<control::ApprovalResponse, (StatusCode, String)> {
        let body = Some(Json(control::DecisionBody {
            comment: comment.map(str::to_string),
            digests: digests.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        }));
        let path = Path((run.to_string(), task.to_string()));
        let r = if approve {
            control::approve_task(who, State(state.clone()), path, body).await
        } else {
            control::reject_task(who, State(state.clone()), path, body).await
        };
        r.map(|Json(v)| v)
    }

    #[tokio::test]
    async fn only_listed_non_triggerers_may_approve() {
        let Some(state) = test_state("approval-live").await else { return };
        let name = format!("appr-{}", uuid::Uuid::new_v4());
        let (run, task) = parked_gate(&state, &name).await;

        // Outsider: refused, and the gate stays parked.
        let e = decide(&state, user("mallory@example.com", &["dev"]), &run, &task, true, None)
            .await
            .err()
            .expect("outsider refused");
        assert_eq!(e.0, StatusCode::FORBIDDEN, "{e:?}");
        let e = decide(&state, user("mallory@example.com", &[]), &run, &task, false, None)
            .await
            .err()
            .expect("outsider may not reject either");
        assert_eq!(e.0, StatusCode::FORBIDDEN);

        // The triggerer is on the list by group but may not approve their own run.
        let e = decide(&state, user("dev@example.com", &["release"]), &run, &task, true, None)
            .await
            .err()
            .expect("triggerer refused");
        assert_eq!(e.0, StatusCode::FORBIDDEN, "{e:?}");
        assert!(e.1.contains("not_triggerer"), "{}", e.1);

        // The worklist tells each of them the same thing.
        let list = list_approvals(user("dev@example.com", &["release"]), State(state.clone()))
            .await
            .unwrap()
            .0;
        let mine = list.iter().find(|p| p.task_id == task).expect("gate listed");
        assert_eq!(mine.message.as_deref(), Some("Apply?"));
        assert!(!mine.can_approve && mine.can_reject);
        assert_eq!(mine.show[0].path, "plan/plan.txt");

        // A listed approver who did not start the run decides it.
        let ok = decide(&state, user("ops@example.com", &[]), &run, &task, true, Some("plan reviewed"))
            .await
            .unwrap();
        assert_eq!(ok.resolution, "approved");
        assert_eq!(ok.decided_by, "ops@example.com");
        let (by, why, status): (Option<String>, Option<String>, String) = sqlx::query_as(
            "SELECT decided_by, decision_comment, status FROM task_runs WHERE id = $1",
        )
        .bind(&task)
        .fetch_one(&state.write_pool)
        .await
        .unwrap();
        assert_eq!(by.as_deref(), Some("ops@example.com"));
        assert_eq!(why.as_deref(), Some("plan reviewed"));
        assert_eq!(status, "succeeded");

        // A second decision does not overwrite the first.
        let e = decide(&state, user("ops@example.com", &[]), &run, &task, false, Some("changed my mind"))
            .await
            .err()
            .expect("already decided");
        assert_eq!(e.0, StatusCode::CONFLICT);
    }

    /// The end the checks exist for: a plan whose approval was refused (outsider,
    /// or the person who started the run) or rejected is never applied. The
    /// scheduler runs between decisions, as the engine would.
    #[tokio::test]
    async fn a_refused_or_rejected_plan_is_never_applied() {
        use dagron_core::db;
        let Some(state) = test_state("approval-live-4").await else { return };
        let yaml = format!(
            "name: guarded-{}\ntasks:\n  - name: review\n    type: approval\n    approvers: [\"ops@example.com\", \"dev@example.com\"]\n    not_triggerer: true\n  - name: apply\n    command: [\"terraform\", \"apply\"]\n    depends_on: [review]\n",
            uuid::Uuid::new_v4()
        );
        let run = control::submit_yaml_as(&state, &yaml, &yaml, &BTreeMap::new(), Some("dev@example.com"))
            .await
            .unwrap();
        let pool = &state.write_pool;
        let status = |name: &'static str| {
            let run = run.clone();
            async move {
                sqlx::query_scalar::<_, String>("SELECT status FROM task_runs WHERE run_id = $1 AND name = $2")
                    .bind(&run)
                    .bind(name)
                    .fetch_one(pool)
                    .await
                    .unwrap()
            }
        };
        db::advance_ready_tasks(pool).await.unwrap();
        assert_eq!(status("review").await, "awaiting_approval");
        let review: String =
            sqlx::query_scalar("SELECT id FROM task_runs WHERE run_id = $1 AND name = 'review'")
                .bind(&run)
                .fetch_one(pool)
                .await
                .unwrap();

        // Refused approvals change nothing: the gate stays parked, apply never becomes ready.
        // dev is a listed approver, so its refusal is `not_triggerer`'s, not the list's.
        for (who, why) in [
            (user("dev@example.com", &[]), "not_triggerer"),
            (user("mallory@example.com", &[]), "not among this gate's approvers"),
        ] {
            let e = decide(&state, who, &run, &review, true, None).await.err().expect("refused");
            assert_eq!(e.0, StatusCode::FORBIDDEN);
            assert!(e.1.contains(why), "{}", e.1);
            db::advance_ready_tasks(pool).await.unwrap();
            assert_eq!(status("review").await, "awaiting_approval");
            assert_eq!(status("apply").await, "pending");
        }

        // A listed approver rejects: the gate fails and apply is skipped, not run.
        let r = decide(&state, user("ops@example.com", &[]), &run, &review, false, Some("drops the database"))
            .await
            .unwrap();
        assert_eq!(r.resolution, "rejected");
        db::advance_ready_tasks(pool).await.unwrap();
        assert_eq!(status("review").await, "failed");
        assert_eq!(status("apply").await, "skipped");
    }

    #[tokio::test]
    async fn triggerer_may_still_reject_and_group_members_may_approve() {
        let Some(state) = test_state("approval-live-2").await else { return };
        let name = format!("appr-{}", uuid::Uuid::new_v4());
        let (run, task) = parked_gate(&state, &name).await;
        let r = decide(&state, user("dev@example.com", &["release"]), &run, &task, false, Some("wrong stack"))
            .await
            .unwrap();
        assert_eq!(r.resolution, "rejected");
        // The decision reads in the run log: who, when, and the comment.
        let log = |task: String| {
            let pool = state.read_pool.clone();
            async move {
                sqlx::query_scalar::<_, Option<String>>("SELECT log FROM task_runs WHERE id = $1")
                    .bind(task)
                    .fetch_one(&pool)
                    .await
                    .unwrap()
                    .expect("a decided gate carries a log")
            }
        };
        let rejected = log(task.clone()).await;
        assert!(
            rejected.starts_with("rejected by dev@example.com at "),
            "{rejected}"
        );
        assert!(rejected.ends_with("\ncomment: wrong stack"), "{rejected}");

        let (run, task) = parked_gate(&state, &format!("appr-{}", uuid::Uuid::new_v4())).await;
        let r = decide(&state, user("lead@example.com", &["Release"]), &run, &task, true, None)
            .await
            .unwrap();
        assert_eq!(r.decided_by, "lead@example.com");
        let approved = log(task.clone()).await;
        assert!(
            approved.starts_with("approved by lead@example.com at "),
            "{approved}"
        );
        assert!(
            !approved.contains('\n'),
            "no comment, no comment line: {approved}"
        );
    }

    #[tokio::test]
    async fn parameters_are_validated_and_runs_are_capped_per_key() {
        let Some(state) = test_state("param-live-1").await else { return };
        let yaml = format!(
            "name: deploy-{}
max_active_runs: 1
concurrency_key: \"{{{{ stack }}}}\"
parameters: {{ stack: \"\" }}
param_schema:
  stack: {{ required: true, pattern: \"[a-z]+\" }}
tasks:
  - name: gate
    type: approval
",
            uuid::Uuid::new_v4()
        );
        let with = |stack: &str| -> BTreeMap<String, String> {
            [("stack".to_string(), stack.to_string())].into_iter().collect()
        };

        // A refused value is a 400 that names the parameter, not "invalid DAG".
        let e = control::submit_yaml_as(&state, &yaml, &yaml, &BTreeMap::new(), None).await.unwrap_err();
        assert_eq!((e.0, e.1.as_str()), (StatusCode::BAD_REQUEST, "parameter 'stack' is required"));
        let e = control::submit_yaml_as(&state, &yaml, &yaml, &with("Prod!"), None).await.unwrap_err();
        assert_eq!(e.0, StatusCode::BAD_REQUEST);
        assert!(e.1.starts_with("parameter 'stack' must match"), "{}", e.1);

        // One run per stack: a second prod run is a 429 naming the key, staging is free.
        control::submit_yaml_as(&state, &yaml, &yaml, &with("prod"), None).await.unwrap();
        let e = control::submit_yaml_as(&state, &yaml, &yaml, &with("prod"), None).await.unwrap_err();
        assert_eq!(e.0, StatusCode::TOO_MANY_REQUESTS);
        assert!(e.1.contains("concurrency_key 'prod'"), "{}", e.1);
        control::submit_yaml_as(&state, &yaml, &yaml, &with("staging"), None).await.unwrap();
    }

    #[tokio::test]
    async fn approval_is_pinned_to_the_reviewed_bytes() {
        use dagron_artifact::{ArtifactKey, ArtifactStore, LocalFsStore};
        use sha2::{Digest, Sha256};

        let Some(mut state) = test_state("approval-live-3").await else { return };
        let dir = std::env::temp_dir().join(format!("dagron-binds-{}", uuid::Uuid::new_v4()));
        let store = std::sync::Arc::new(LocalFsStore::new(&dir));
        state.artifact_store = Some(store.clone());

        let yaml = format!(
            "name: binds-{}
tasks:
  - name: gate
    type: approval
    binds: [\"plan/plan.tfplan\"]
",
            uuid::Uuid::new_v4()
        );
        let run = control::submit_yaml_as(&state, &yaml, &yaml, &BTreeMap::new(), Some("dev@example.com"))
            .await
            .unwrap();
        let task: String =
            sqlx::query_scalar("SELECT id FROM task_runs WHERE run_id = $1 AND name = 'gate'")
                .bind(&run)
                .fetch_one(&state.write_pool)
                .await
                .unwrap();
        sqlx::query("UPDATE task_runs SET status = 'awaiting_approval' WHERE id = $1")
            .bind(&task)
            .execute(&state.write_pool)
            .await
            .unwrap();
        let sha = |b: &[u8]| format!("{:x}", Sha256::digest(b));
        let key = ArtifactKey::new(run.as_str(), "plan", "plan.tfplan");
        let me = || user("ops@example.com", &[]);

        // No plan yet: listed, but nothing to pin, and approving is refused.
        let list = list_approvals(me(), State(state.clone())).await.unwrap().0;
        let g = list.iter().find(|p| p.task_id == task).expect("listed");
        assert!(g.show[0].bound && g.show[0].sha256.is_none());
        let e = decide_with(&state, me(), &run, &task, true, None, &[("plan/plan.tfplan", "00")])
            .await
            .err()
            .unwrap();
        assert_eq!(e.0, StatusCode::CONFLICT, "{e:?}");

        // The plan the approver reviews.
        store.put(&key, b"create 3 resources").await.unwrap();
        let list = list_approvals(me(), State(state.clone())).await.unwrap().0;
        let seen = list.iter().find(|p| p.task_id == task).unwrap().show[0].sha256.clone().unwrap();
        assert_eq!(seen, sha(b"create 3 resources"));

        // Approving without saying what was reviewed is refused.
        let e = decide_with(&state, me(), &run, &task, true, None, &[]).await.err().unwrap();
        assert_eq!(e.0, StatusCode::BAD_REQUEST, "{e:?}");

        // The plan changes after review: the old digest no longer approves it.
        store.put(&key, b"destroy everything").await.unwrap();
        let e = decide_with(&state, me(), &run, &task, true, None, &[("plan/plan.tfplan", &seen)])
            .await
            .err()
            .unwrap();
        assert_eq!(e.0, StatusCode::CONFLICT, "{e:?}");
        assert!(e.1.contains(&sha(b"destroy everything")), "{}", e.1);
        let status: String = sqlx::query_scalar("SELECT status FROM task_runs WHERE id = $1")
            .bind(&task)
            .fetch_one(&state.write_pool)
            .await
            .unwrap();
        assert_eq!(status, "awaiting_approval");

        // Reviewing the current bytes approves, and records what was approved.
        let now = sha(b"destroy everything");
        let ok = decide_with(&state, me(), &run, &task, true, None, &[("plan/plan.tfplan", &now.to_uppercase())])
            .await
            .unwrap();
        assert_eq!(ok.resolution, "approved");
        let doc = store.get(&ArtifactKey::new(run.as_str(), "gate", "approved.sha256")).await.unwrap();
        assert_eq!(String::from_utf8(doc).unwrap(), format!("{now}  plan/plan.tfplan
"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod redrive_tests {
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

    use super::*;

    /// A dead-letter row, whole: `(id, payload, error, source, failures,
    /// first_seen_at, last_error_at)`.
    type Row = (String, String, String, String, i64, String, String);

    /// Set once the engine's migrations have run in this test process.
    static MIGRATED: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();

    /// Disposable Postgres from `TEST_DATABASE_URL` (skipped when unset) with
    /// the engine's real migrations applied. Redrive writes through
    /// `dagron_core::db`, so those migrations, not a hand-made table, are what it
    /// has to agree with. The pool's connections carry `app` as their
    /// `application_name`, so a test can find its own backends in
    /// `pg_stat_activity`.
    pub(super) async fn test_state(app: &str) -> Option<AppState> {
        let Ok(url) = std::env::var("TEST_DATABASE_URL") else {
            eprintln!("TEST_DATABASE_URL unset - skipping a live-datastore test");
            return None;
        };
        MIGRATED
            .get_or_init(|| async {
                // The gitrepos fixture goes first. Under the DDL lock the other
                // live tests take, it creates the tables the migrations also
                // declare (`workflows`, `git_repos`), so on a fresh database a
                // parallel test's `CREATE TABLE IF NOT EXISTS` never races a
                // migration's.
                crate::routes::gitrepos::managed_tests::test_pool().await.unwrap().close().await;
                // The migrations run without that lock held. They include
                // `CREATE INDEX CONCURRENTLY`, which waits out every older
                // snapshot, and a test queued on the lock holds one.
                dagron_core::db::init_pool(&url).await.unwrap().close().await;
            })
            .await;
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .connect_with(PgConnectOptions::from_str(&url).unwrap().application_name(app))
            .await
            .unwrap();
        Some(AppState {
            read_pool: pool.clone(),
            write_pool: pool.clone(),
            tx: tokio::sync::broadcast::channel(16).0,
            jwt_secret: "test".to_string(),
            cookie_secure: false,
            identity: Arc::new(crate::identity::LocalIdentityProvider::new(pool)),
            artifact_store: None,
            rotation_lock: Arc::new(tokio::sync::Mutex::new(())),
            login_limiter: Arc::new(crate::ratelimit::RateLimiter::from_env()),
            listener_ready: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    /// Call the handler as a signed-in user and unwrap its JSON body.
    async fn redrive(state: &AppState, id: &str) -> Result<RedriveResponse, (StatusCode, String)> {
        let user = AuthUser(crate::auth::SessionClaims {
            sub: "u1".to_string(),
            email: "u1@example.com".to_string(),
            name: "U1".to_string(),
            groups: vec![],
            exp: 0,
        });
        redrive_dead_letter(user, State(state.clone()), Path(id.to_string())).await.map(|Json(r)| r)
    }

    /// The status a redrive was refused with. Panics if it made a run instead.
    async fn refused(state: &AppState, id: &str) -> (StatusCode, String) {
        match redrive(state, id).await {
            Ok(r) => panic!("expected a refusal, got run {}", r.run_id),
            Err(e) => e,
        }
    }

    /// Park `payload` with a history worth losing, and return the new id.
    async fn park(state: &AppState, payload: &str) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO dead_letters
                (id, payload, error, source, failures, first_seen_at, last_error_at)
             VALUES ($1, $2, 'upstream timed out', 'redis', 3,
                     '2026-01-01T00:00:00+00:00', '2026-01-02T00:00:00+00:00')",
        )
        .bind(&id)
        .bind(payload)
        .execute(&state.write_pool)
        .await
        .unwrap();
        id
    }

    /// The dead letter's whole row, or `None` once it is gone.
    async fn row(state: &AppState, id: &str) -> Option<Row> {
        sqlx::query_as(
            "SELECT id, payload, error, source, failures, first_seen_at, last_error_at
             FROM dead_letters WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&state.write_pool)
        .await
        .unwrap()
    }

    /// Runs of the workflow named `name`.
    async fn runs_of(state: &AppState, name: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM workflow_runs wr
             JOIN workflow_definitions d ON d.id = wr.definition_id WHERE d.name = $1",
        )
        .bind(name)
        .fetch_one(&state.write_pool)
        .await
        .unwrap()
    }

    /// A redrive that fails leaves the dead letter exactly as it was, and the
    /// same id redrives once the cause is gone. It covers a refusal before the
    /// claim (an unknown environment, 400) and one inside the claiming
    /// transaction (the run cap, 429). Both used to lose the row, because the
    /// claim's delete committed before the run was attempted.
    #[tokio::test]
    async fn a_failed_redrive_keeps_the_dead_letter_for_a_retry() {
        let Some(state) = test_state("dagron-redrive-retry").await else { return };
        let tag = uuid::Uuid::new_v4().simple().to_string();
        let name = format!("redrive-{tag}");
        let env = format!("env-{tag}");
        let payload = format!(
            "name: {name}\nenvironment: {env}\nmax_active_runs: 1\n\
             tasks:\n  - name: a\n    command: [\"true\"]\n"
        );
        let id = park(&state, &payload).await;
        let parked = row(&state, &id).await;
        assert!(parked.is_some());

        let (status, msg) = refused(&state, &id).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{msg}");
        assert!(msg.contains(&env), "{msg}");
        assert_eq!(row(&state, &id).await, parked, "an unknown environment keeps the row");

        // The environment exists now, but another run holds the workflow's one slot.
        sqlx::query(
            "INSERT INTO environments (id, name, variables, created_at, updated_at)
             VALUES ($1, $1, '{}', 'now', 'now')",
        )
        .bind(&env)
        .execute(&state.write_pool)
        .await
        .unwrap();
        let blocker = control::submit_yaml(&state, &payload, &payload).await.unwrap();
        let (status, msg) = refused(&state, &id).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{msg}");
        assert_eq!(row(&state, &id).await, parked, "the claim rolled back with the refused run");
        assert_eq!(runs_of(&state, &name).await, 1, "only the blocker");

        // The slot frees up; the same id redrives, and only now is the row gone.
        sqlx::query("UPDATE workflow_runs SET status = 'succeeded' WHERE id = $1")
            .bind(&blocker)
            .execute(&state.write_pool)
            .await
            .unwrap();
        let done = redrive(&state, &id).await.unwrap_or_else(|(s, m)| panic!("{s}: {m}"));
        assert_eq!(done.redriven_from, id);
        assert_eq!(row(&state, &id).await, None);
        let spec: String = sqlx::query_scalar(
            "SELECT d.spec FROM workflow_runs r
             JOIN workflow_definitions d ON d.id = r.definition_id WHERE r.id = $1",
        )
        .bind(&done.run_id)
        .fetch_one(&state.write_pool)
        .await
        .unwrap();
        assert_eq!(spec, payload, "the run is the parked payload");

        let (status, msg) = refused(&state, &id).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{msg}");
        assert_eq!(runs_of(&state, &name).await, 2, "a consumed id makes no second run");
    }

    /// Redrives racing on one id create exactly one run, and every loser gets
    /// 404. The test holds the row's lock until all of them are queued on it,
    /// so they really are in flight together rather than one after another.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_redrives_create_one_run() {
        const RACERS: i64 = 8;
        let app = format!("dagron-redrive-race-{}", uuid::Uuid::new_v4().simple());
        let Some(state) = test_state(&app).await else { return };
        let name = format!("{app}-wf");
        let payload = format!("name: {name}\ntasks:\n  - name: a\n    command: [\"true\"]\n");
        let id = park(&state, &payload).await;

        let mut gate = state.write_pool.begin().await.unwrap();
        sqlx::query("SELECT 1 FROM dead_letters WHERE id = $1 FOR UPDATE")
            .bind(&id)
            .execute(&mut *gate)
            .await
            .unwrap();
        let racers: Vec<_> = (0..RACERS)
            .map(|_| {
                let (state, id) = (state.clone(), id.clone());
                tokio::spawn(async move { redrive(&state, &id).await })
            })
            .collect();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let queued: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM pg_stat_activity
                 WHERE application_name = $1 AND wait_event_type = 'Lock'",
            )
            .bind(&app)
            .fetch_one(&state.read_pool)
            .await
            .unwrap();
            if queued == RACERS {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "only {queued} of {RACERS} redrives reached the claim"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        gate.commit().await.unwrap();

        let mut won = 0;
        for racer in racers {
            match racer.await.unwrap() {
                Ok(_) => won += 1,
                Err((status, msg)) => assert_eq!(status, StatusCode::NOT_FOUND, "{msg}"),
            }
        }
        assert_eq!(won, 1, "exactly one redrive wins the claim");
        assert_eq!(runs_of(&state, &name).await, 1);
        assert_eq!(row(&state, &id).await, None);
    }
}
