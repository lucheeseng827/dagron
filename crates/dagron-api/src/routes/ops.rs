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

#[derive(Serialize)]
pub struct StatusCount {
    pub status: String,
    pub count: i64,
}

#[derive(Serialize)]
pub struct MetricsResponse {
    pub runs_by_status: Vec<StatusCount>,
    pub tasks_by_status: Vec<StatusCount>,
    pub dead_letters: i64,
    /// How old the counts of the finished statuses are, in seconds: they are
    /// read less often than the rest (`DAGRON_METRICS_HISTORY_CACHE_SECS`).
    pub history_age_secs: f64,
    /// The limits over this replica's sessions, as it last read them (at
    /// start, then every 5 minutes): absent until a read has
    /// succeeded, and the last one's after a read that fails.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub db_connections: Option<DbConnections>,
}

/// `db_connections` in [`MetricsResponse`]: what this dagron-api may hold
/// at once, and each limit over it with the sessions under it. A limit of
/// `null` is none.
#[derive(Serialize, Debug, PartialEq)]
pub struct DbConnections {
    /// Its pool (`DAGRON_DB_MAX_CONNECTIONS`) and one for its LISTEN.
    pub holds: u32,
    pub database: ConnectionScope,
    pub login: ConnectionScope,
    pub server: ConnectionScope,
}

/// One limit and the sessions under it that its login may see. On
/// `database` and `login`, every login's sessions (any login sees every
/// session's database and login), the database's with its autovacuum and
/// parallel workers too. On `server`, which counts client sessions, every
/// login's with `pg_read_all_stats`, else its own: without it another
/// login's backend type is hidden.
#[derive(Serialize, Debug, PartialEq)]
pub struct ConnectionScope {
    pub limit: Option<i64>,
    pub sessions: i64,
}

impl DbConnections {
    pub fn new(holds: u32, limits: &dagron_core::connections::Limits) -> Self {
        let scope = |limit, sessions| ConnectionScope { limit, sessions };
        Self {
            holds,
            database: scope(limits.database, limits.database_sessions),
            login: scope(limits.login, limits.login_sessions),
            server: scope(Some(limits.server), limits.server_sessions),
        }
    }
}

/// The default for `DAGRON_METRICS_CACHE_SECS`: shorter than the console's
/// poll of its Metrics page (5 s), so a poll sees counts no older than this,
/// and the polls of every page open on this replica share one read. The
/// engine's 10 s is for scrapes, which nobody watches tick.
pub const METRICS_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(2);

/// The default for `DAGRON_METRICS_HISTORY_CACHE_SECS`, as the engine's
/// `METRICS_HISTORY_CACHE_SECS`: the finished statuses grow with every run,
/// and change little between two polls. 0 where `DAGRON_METRICS_CACHE_SECS`
/// is 0.
pub const METRICS_HISTORY_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(300);

/// The TTLs `DAGRON_METRICS_CACHE_SECS` and `DAGRON_METRICS_HISTORY_CACHE_SECS`
/// give: with the first 0 and the second not set, both are 0, so that the
/// first at 0 alone still reads the database on every call.
pub fn metrics_cache_ttls(
    live: Option<String>,
    history: Option<String>,
) -> (std::time::Duration, std::time::Duration) {
    let live = dagron_core::gauges::ttl_from(live, METRICS_CACHE_TTL);
    (
        live,
        dagron_core::gauges::history_ttl_from(history, live, METRICS_HISTORY_CACHE_TTL),
    )
}

/// The `GET /api/metrics` cache, as `DAGRON_METRICS_CACHE_SECS` and
/// `DAGRON_METRICS_HISTORY_CACHE_SECS` set it.
pub fn metrics_cache_from_env() -> dagron_core::gauges::GaugeCache {
    let (live, history) = metrics_cache_ttls(
        std::env::var("DAGRON_METRICS_CACHE_SECS").ok(),
        std::env::var("DAGRON_METRICS_HISTORY_CACHE_SECS").ok(),
    );
    dagron_core::gauges::GaugeCache::new(live, history)
}

/// `GET /api/metrics` — run/task counts by status + dead-letter total.
/// JSON (UI-friendly) rather than the engine's Prometheus text at `/metrics`.
///
/// In two tiers, each cached on its own, as the engine's
/// gauges are: the runs and tasks that have not finished, and the dead
/// letters, are read at most once every `DAGRON_METRICS_CACHE_SECS`, one
/// read at a time with the other callers waiting for it; the finished
/// statuses, which grow with every run, once every
/// `DAGRON_METRICS_HISTORY_CACHE_SECS`, read again behind the call that
/// found them stale, which is answered with the last. Only what this
/// answers is read, not the rest of the engine's gauges. A failed read of
/// what has not finished is never cached, and answers 500.
pub async fn metrics(
    _auth: AuthUser,
    State(state): State<AppState>,
) -> Result<Json<MetricsResponse>, StatusCode> {
    let pool = &state.read_pool;
    let history = state.read_pool.clone();
    let snap = state
        .metrics_cache
        .read(
            || dagron_core::db::status_counts_unfinished(pool),
            move || async move { dagron_core::db::status_counts_finished(&history).await },
        )
        .await
        .map_err(|e| {
            tracing::error!(error = ?e, "db query failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let counts = |rows: Vec<(String, i64)>| {
        rows.into_iter()
            .map(|(status, count)| StatusCount { status, count })
            .collect()
    };
    Ok(Json(MetricsResponse {
        runs_by_status: counts(snap.runs_by_status),
        tasks_by_status: counts(snap.tasks_by_status),
        dead_letters: snap.dead_letters,
        history_age_secs: snap.history_age_secs,
        db_connections: state
            .db_connections
            .snapshot()
            .map(|(holds, limits)| DbConnections::new(holds, &limits)),
    }))
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
    /// Whether the run's triggerer, and whoever put the gate back to ask
    /// again, are barred from approving (`not_triggerer`).
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
    /// Who put the gate back to ask again (a rerun, a retry or a clear).
    reset_by: Option<String>,
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
        let is = |who: Option<&str>| {
            who.is_some_and(|t| {
                [me.email.as_str(), me.sub.as_str()]
                    .iter()
                    .any(|id| !id.is_empty() && id.eq_ignore_ascii_case(t))
            })
        };
        let is_triggerer = is(self.triggered_by.as_deref()) || is(self.reset_by.as_deref());
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
    // The triggerer as the approval itself reads it (`resolve_approval`), so
    // that what a row says its reader may do is what the reader may do.
    let rows = sqlx::query_as::<_, PendingApprovalRow>(concat!(
        "SELECT t.run_id, t.id AS task_id, t.name AS task_name,
                d.name AS workflow_name, t.scheduled_at AS since, t.input,
                ",
        control::triggerer_sql!("wr"),
        " AS triggered_by, t.reset_by
         FROM task_runs t
         JOIN workflow_runs wr ON wr.id = t.run_id
         LEFT JOIN run_triggers rt ON rt.run_id = wr.id
         LEFT JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE t.status = 'awaiting_approval'
         ORDER BY t.scheduled_at ASC NULLS FIRST",
    ))
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
    auth: AuthUser,
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
    // The run is the caller's, who asked for it, of this dead letter.
    let trigger = control::manual_trigger(&control::caller_identity(&auth.0))
        .from_source(format!("dead-letter:{id}"));
    let dag = control::build_dag(&state, &dl.payload, &BTreeMap::new(), Some(&trigger)).await?;

    // Rows are never updated in place, so the payload read above is the one
    // being claimed.
    let claimed = dagron_core::db::create_run_from_dead_letter(
        &state.write_pool,
        &dag,
        &dl.payload,
        &id,
        Some(&trigger),
    )
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
            reset_by: None,
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

        // Listed, and put the gate back to ask again: the same.
        let rearmed = PendingApprovalRow {
            reset_by: Some("OPS@example.com".into()),
            ..row(gate, Some("dev@example.com"))
        };
        let p = rearmed.into_pending(&me("ops@example.com", &[]));
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

    /// `not_triggerer` bars whoever caused the run: the actor of its trigger
    /// record. It read the run's submitter, which for a run a CI service
    /// submits is the service's own token, so it never barred the person who
    /// asked; and a sub-workflow's run has no submitter at all, so a gate in
    /// one barred nobody, whoever had started its parent.
    #[tokio::test]
    async fn not_triggerer_bars_who_caused_the_run_not_who_submitted_it() {
        use dagron_core::db;
        use dagron_core::models::{trigger_kind, RunTrigger};
        let Some(state) = test_state("approval-live-5").await else {
            return;
        };
        let pool = &state.write_pool;
        let gated = |name: &str| {
            format!(
                "name: {name}-{}\ntasks:\n  - name: review\n    type: approval\n    \
                 approvers: [\"dev@example.com\", \"ci@example.com\", \"ops@example.com\"]\n    \
                 not_triggerer: true\n",
                uuid::Uuid::new_v4()
            )
        };
        // The run's gate, parked as the engine would park it.
        let parked = |run: String| async move {
            db::advance_ready_tasks(pool).await.unwrap();
            sqlx::query_scalar::<_, String>(
                "SELECT id FROM task_runs WHERE run_id = $1 AND name = 'review' AND status = 'awaiting_approval'",
            )
            .bind(&run)
            .fetch_one(pool)
            .await
            .unwrap()
        };
        let refused = |r: Result<control::ApprovalResponse, (StatusCode, String)>| {
            let e = r.err().expect("refused");
            assert_eq!(e.0, StatusCode::FORBIDDEN);
            assert!(e.1.contains("not_triggerer"), "{}", e.1);
        };
        let dev = || user("dev@example.com", &[]);
        // What a reader of the worklist is told they may do with a run's gate.
        let may_approve = |who: AuthUser, run: String| {
            let state = state.clone();
            async move {
                let Json(rows) = list_approvals(who, State(state)).await.unwrap();
                rows.into_iter()
                    .find(|r| r.run_id == run)
                    .expect("the gate is listed")
                    .can_approve
            }
        };

        // A run a CI service submitted for dev: its token submitted the run,
        // and dev caused it.
        let for_dev = RunTrigger {
            kind: trigger_kind::CI.to_string(),
            principal: "ci:ci@example.com".to_string(),
            actor: Some("dev@example.com".to_string()),
            trust: Some("trusted".to_string()),
            ..Default::default()
        };
        let yaml = gated("ci");
        let run = control::submit_yaml_recorded(
            &state,
            &yaml,
            &yaml,
            &BTreeMap::new(),
            Some("ci@example.com"),
            &for_dev,
        )
        .await
        .unwrap();
        let review = parked(run.clone()).await;
        assert!(!may_approve(dev(), run.clone()).await, "dev caused it");
        assert!(
            may_approve(user("ci@example.com", &[]), run.clone()).await,
            "the service only submitted it"
        );
        refused(decide(&state, dev(), &run, &review, true, None).await);
        decide(
            &state,
            user("ci@example.com", &[]),
            &run,
            &review,
            true,
            None,
        )
        .await
        .unwrap();

        // A sub-workflow's run of it: nobody submitted it, and it is dev's
        // all the same, as its parent is.
        let yaml = gated("child");
        let dag = control::build_dag(&state, &yaml, &BTreeMap::new(), None)
            .await
            .unwrap();
        let child = RunTrigger::child_of(Some(&for_dev), &run, "call");
        let child_run = db::create_run_triggered(pool, &dag, &yaml, &child)
            .await
            .unwrap();
        let review = parked(child_run.clone()).await;
        assert!(!may_approve(dev(), child_run.clone()).await);
        refused(decide(&state, dev(), &child_run, &review, true, None).await);
        decide(
            &state,
            user("ops@example.com", &[]),
            &child_run,
            &review,
            true,
            None,
        )
        .await
        .unwrap();

        // A run from before every run had a record: its submitter's, as it was.
        let yaml = gated("old");
        let old = control::submit_yaml_as(
            &state,
            &yaml,
            &yaml,
            &BTreeMap::new(),
            Some("dev@example.com"),
        )
        .await
        .unwrap();
        sqlx::query("DELETE FROM run_triggers WHERE run_id = $1")
            .bind(&old)
            .execute(pool)
            .await
            .unwrap();
        let review = parked(old.clone()).await;
        refused(decide(&state, dev(), &old, &review, true, None).await);
        decide(
            &state,
            user("ops@example.com", &[]),
            &old,
            &review,
            true,
            None,
        )
        .await
        .unwrap();

        // A record that names nobody bars nobody: a schedule's fire.
        let yaml = gated("fired");
        let dag = control::build_dag(&state, &yaml, &BTreeMap::new(), None)
            .await
            .unwrap();
        let fire = RunTrigger::engine(trigger_kind::SCHEDULE, "schedule:s1");
        let fired = db::create_run_triggered(pool, &dag, &yaml, &fire)
            .await
            .unwrap();
        let review = parked(fired.clone()).await;
        assert!(may_approve(dev(), fired.clone()).await);
        decide(&state, dev(), &fired, &review, true, None)
            .await
            .unwrap();
    }

    /// On Postgres: a whole rerun asks a gate again from nothing. The decision
    /// it replaces is kept as the gate's attempt and is not left on the gate,
    /// and whoever asked for the rerun may not approve a `not_triggerer` gate
    /// they put back, as the run's triggerer may not: else the person who
    /// re-armed a production deploy could approve it, the only person
    /// involved. The run's `run_timeout_secs` is counted from the rerun, so a
    /// rerun of a run older than it is not failed by the deadline sweep.
    #[tokio::test]
    async fn a_whole_rerun_asks_a_gate_again_and_who_asked_may_not_approve_it() {
        use dagron_core::db;
        let Some(state) = test_state("approval-live-rerun").await else {
            return;
        };
        let pool = &state.write_pool;
        let yaml = format!(
            "name: deploy-{}\nrun_timeout_secs: 3600\ntasks:\n  - name: gate\n    type: approval\n    \
             approvers: [\"group:release\"]\n    not_triggerer: true\n  - name: deploy\n    \
             command: [\"true\"]\n    depends_on: [gate]\n",
            uuid::Uuid::new_v4().simple()
        );
        let run = control::submit_yaml_as(
            &state,
            &yaml,
            &yaml,
            &BTreeMap::new(),
            Some("alice@example.com"),
        )
        .await
        .unwrap();
        let release = |email: &str| user(email, &["release"]);
        let gate: String =
            sqlx::query_scalar("SELECT id FROM task_runs WHERE run_id = $1 AND name = 'gate'")
                .bind(&run)
                .fetch_one(pool)
                .await
                .unwrap();
        db::advance_ready_tasks(pool).await.unwrap();
        decide(
            &state,
            release("bob@example.com"),
            &run,
            &gate,
            true,
            Some("ship it"),
        )
        .await
        .unwrap();
        // It ran, and succeeded, a day ago: its deadline has long passed.
        sqlx::query(
            "UPDATE task_runs SET status = 'succeeded', attempt = 1, output = 'ok'
              WHERE run_id = $1 AND name = 'deploy'",
        )
        .bind(&run)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE workflow_runs SET status = 'succeeded', finished_at = '2026-01-01T01:00:00+00:00',
                 deadline_at = '2026-01-01T01:00:00+00:00' WHERE id = $1",
        )
        .bind(&run)
        .execute(pool)
        .await
        .unwrap();

        let body = Some(Json(
            serde_json::from_value(serde_json::json!({ "from": "all" })).unwrap(),
        ));
        let Json(did) = control::rerun_run(
            release("carol@example.com"),
            State(state.clone()),
            Path(run.clone()),
            body,
        )
        .await
        .unwrap();
        assert_eq!(did.rerun, 2);
        assert!(
            !db::cancel_overdue_runs(pool)
                .await
                .unwrap()
                .iter()
                .any(|(id, _)| *id == run),
            "its deadline is counted from the rerun"
        );
        let (by, why, reset_by, base): (Option<String>, Option<String>, Option<String>, i64) = sqlx::query_as(
            "SELECT decided_by, decision_comment, reset_by, attempt_base FROM task_runs WHERE id = $1",
        )
        .bind(&gate)
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!((by, why), (None, None), "bob's decision is not this one's");
        assert_eq!((reset_by.as_deref(), base), (Some("carol@example.com"), 1));
        let kept: Vec<(i64, String, Option<String>)> =
            sqlx::query_as("SELECT attempt, reason, output FROM task_attempts WHERE task_id = $1")
                .bind(&gate)
                .fetch_all(pool)
                .await
                .unwrap();
        assert_eq!(kept.len(), 1, "{kept:?}");
        assert_eq!((kept[0].0, kept[0].1.as_str()), (1, "rerun"));
        assert!(
            kept[0]
                .2
                .as_deref()
                .unwrap_or_default()
                .contains("bob@example.com"),
            "{kept:?}"
        );

        // Asked again: carol put it back, so carol may not approve it.
        db::advance_ready_tasks(pool).await.unwrap();
        let list = list_approvals(release("carol@example.com"), State(state.clone()))
            .await
            .unwrap()
            .0;
        let mine = list
            .iter()
            .find(|p| p.task_id == gate)
            .expect("asked again");
        assert!(!mine.can_approve && mine.can_reject);
        let e = decide(
            &state,
            release("carol@example.com"),
            &run,
            &gate,
            true,
            None,
        )
        .await
        .err()
        .expect("carol refused");
        assert_eq!(e.0, StatusCode::FORBIDDEN, "{e:?}");
        assert!(
            e.1.contains("not_triggerer") && e.1.contains("put it back"),
            "{}",
            e.1
        );
        let ok = decide(&state, release("bob@example.com"), &run, &gate, true, None)
            .await
            .unwrap();
        assert_eq!(ok.decided_by, "bob@example.com");
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

/// A cancel against a real datastore: the Postgres half of
/// `dagron_core::db`'s stop (`cancel_run`, `tasks_to_stop`,
/// `settle_stopped_task`, `release_stopping_task`, `claim_teardowns`), and
/// what the cancel and the run detail then answer. Skipped without
/// `TEST_DATABASE_URL`.
#[cfg(test)]
mod stop_live_tests {
    use dagron_core::db;

    use super::redrive_tests::test_state;
    use super::*;
    use crate::routes::runs;

    const WORKER: &str = "stop-live-worker";

    fn viewer() -> AuthUser {
        AuthUser(crate::auth::SessionClaims {
            sub: "u1".to_string(),
            email: "u1@example.com".to_string(),
            name: "U1".to_string(),
            groups: vec![],
            exp: 0,
        })
    }

    /// A fresh run of `tasks` (YAML list items), in a runner class of its
    /// own: `claim_teardowns` takes what any test left stopping in the shared
    /// database, and the class is what keeps this test's claim to its own
    /// rows. Returns the run and the class.
    async fn run_of(state: &AppState, tasks: &str) -> (String, String) {
        let class = format!("stop-{}", uuid::Uuid::new_v4().simple());
        let yaml = format!("name: {class}\nrunner_class: {class}\ntasks:\n{tasks}");
        let run = control::submit_yaml_as(state, &yaml, &yaml, &BTreeMap::new(), None)
            .await
            .unwrap();
        (run, class)
    }

    async fn task_id(state: &AppState, run: &str, name: &str) -> String {
        sqlx::query_scalar("SELECT id FROM task_runs WHERE run_id = $1 AND name = $2")
            .bind(run)
            .bind(name)
            .fetch_one(&state.write_pool)
            .await
            .unwrap()
    }

    /// The engine's claim of the task, as the one statement it is: no engine
    /// runs here, and `claim_ready` would take other tests' rows. Returns the
    /// claim's fence.
    async fn claim(state: &AppState, task: &str) -> i64 {
        let lease = (chrono::Utc::now() + chrono::TimeDelta::seconds(300)).to_rfc3339();
        sqlx::query_scalar(
            "UPDATE task_runs
             SET status = 'running', claimed_by = $1, lease_expires_at = $2,
                 attempt = attempt + 1, version = version + 1
             WHERE id = $3
             RETURNING version",
        )
        .bind(WORKER)
        .bind(&lease)
        .bind(task)
        .fetch_one(&state.write_pool)
        .await
        .unwrap()
    }

    async fn detail(state: &AppState, run: &str) -> runs::RunDetail {
        let Json(detail) = runs::get_run(viewer(), State(state.clone()), Path(run.to_string()))
            .await
            .unwrap();
        detail
    }

    async fn cancel(state: &AppState, run: &str) -> (u64, u64) {
        let Json(did) = control::cancel_run(viewer(), State(state.clone()), Path(run.to_string()))
            .await
            .unwrap();
        (did.cancelled, did.stopping)
    }

    async fn completed_events(state: &AppState, run: &str) -> Vec<String> {
        sqlx::query_scalar(
            "SELECT payload FROM event_outbox
             WHERE run_id = $1 AND event_type = 'run.completed' ORDER BY created_at",
        )
        .bind(run)
        .fetch_all(&state.write_pool)
        .await
        .unwrap()
    }

    /// A cancel asks the task a worker holds, and says so. The run and the
    /// task read running, with the request beside them, until that worker
    /// says its work is gone; then both are cancelled, and the run's end is
    /// announced once.
    #[tokio::test]
    async fn a_cancel_asks_a_running_task_and_the_run_ends_on_its_worker_s_word() {
        let Some(state) = test_state("stop-live").await else { return };
        let pool = &state.write_pool;
        let (run, _) = run_of(
            &state,
            "  - name: job\n    command: [\"true\"]\n  - name: after\n    command: [\"true\"]\n    depends_on: [job]\n",
        )
        .await;
        let job = task_id(&state, &run, "job").await;
        let fence = claim(&state, &job).await;

        assert_eq!(
            cancel(&state, &run).await,
            (2, 1),
            "two tasks called off, and the one a worker holds is still to stop"
        );
        let seen = detail(&state, &run).await;
        assert_eq!(seen.status, "running", "its work has not stopped");
        assert_eq!(seen.stopping_as.as_deref(), Some("cancelled"));
        assert!(seen.stop_requested_at.is_some() && seen.finished_at.is_none());
        let by_name = |name: &str| seen.tasks.iter().find(|t| t.name == name).unwrap();
        assert_eq!(by_name("job").status, "running");
        assert!(by_name("job").stop_requested_at.is_some());
        assert_eq!(by_name("after").status, "cancelled");
        assert!(by_name("after").stop_requested_at.is_none());
        assert!(completed_events(&state, &run).await.is_empty(), "no end is announced");

        // Its worker's claim holds, and its engine is told at its next sweep.
        assert!(db::renew_task_lease(pool, &job, WORKER, fence, 300).await.unwrap());
        assert_eq!(
            db::tasks_to_stop(pool, WORKER, &[(job.clone(), fence)]).await.unwrap(),
            [job.clone()]
        );
        assert!(db::tasks_to_stop(pool, WORKER, &[]).await.unwrap().is_empty());

        // A second cancel changes nothing; a result of its own is not applied.
        assert_eq!(cancel(&state, &run).await, (1, 1));
        assert!(!db::settle_stopped_task(pool, &job, "another", fence).await.unwrap());
        assert!(!db::settle_stopped_task(pool, &job, WORKER, fence + 1).await.unwrap());

        assert!(db::settle_stopped_task(pool, &job, WORKER, fence).await.unwrap());
        let seen = detail(&state, &run).await;
        assert_eq!(seen.status, "cancelled");
        assert_eq!((seen.stopping_as.as_deref(), seen.stop_requested_at.as_deref()), (None, None));
        assert!(seen.finished_at.is_some());
        assert!(seen.tasks.iter().all(|t| t.status == "cancelled" && t.stop_requested_at.is_none()));
        let events = completed_events(&state, &run).await;
        assert_eq!(events.len(), 1, "{events:?}");
        assert!(events[0].contains("\"status\":\"cancelled\""), "{events:?}");
        assert_eq!(cancel(&state, &run).await, (0, 0), "nothing is left to cancel");
    }

    /// A task that was asked to stop takes no result of its own: one that
    /// ends by itself first is cancelled, where it would have succeeded.
    #[tokio::test]
    async fn a_task_asked_to_stop_is_cancelled_whatever_it_reports() {
        let Some(state) = test_state("stop-live").await else { return };
        let pool = &state.write_pool;
        let (run, _) = run_of(&state, "  - name: job\n    command: [\"true\"]\n").await;
        let job = task_id(&state, &run, "job").await;
        let fence = claim(&state, &job).await;
        assert_eq!(cancel(&state, &run).await, (1, 1));

        assert!(!db::mark_task_succeeded(pool, &job, WORKER, fence, Some("done".into())).await.unwrap());
        let seen = detail(&state, &run).await;
        assert_eq!((seen.status.as_str(), seen.tasks[0].status.as_str()), ("cancelled", "cancelled"));
        assert_eq!(completed_events(&state, &run).await.len(), 1);
    }

    /// Two tasks of one run stopping at the same moment end the run between
    /// them. Each settles in its own transaction, and would otherwise see the
    /// other still stopping and leave the run running for good.
    #[tokio::test]
    async fn two_tasks_of_one_run_that_stop_at_once_end_the_run_between_them() {
        let Some(state) = test_state("stop-live").await else { return };
        let pool = &state.write_pool;
        for round in 0..8 {
            let (run, _) = run_of(
                &state,
                "  - name: a\n    command: [\"true\"]\n  - name: b\n    command: [\"true\"]\n",
            )
            .await;
            let (a, b) = (task_id(&state, &run, "a").await, task_id(&state, &run, "b").await);
            let (fa, fb) = (claim(&state, &a).await, claim(&state, &b).await);
            assert_eq!(cancel(&state, &run).await, (2, 2));

            let (one, other) = tokio::join!(
                db::settle_stopped_task(pool, &a, WORKER, fa),
                db::settle_stopped_task(pool, &b, WORKER, fb),
            );
            assert!(one.unwrap() && other.unwrap(), "round {round}");
            assert_eq!(detail(&state, &run).await.status, "cancelled", "round {round}");
            assert_eq!(completed_events(&state, &run).await.len(), 1, "round {round}");
        }
    }

    /// A task that had outlived its lease, as one may with no lease heartbeat,
    /// is given a whole lease from the request to answer in: it is not claimed
    /// for a teardown ahead of the worker that is stopping it.
    #[tokio::test]
    async fn a_task_asked_to_stop_is_given_a_lease_to_answer_in() {
        let Some(state) = test_state("stop-live").await else { return };
        let pool = &state.write_pool;
        let (run, class) = run_of(&state, "  - name: job\n    command: [\"true\"]\n").await;
        let classes = [class];
        let job = task_id(&state, &run, "job").await;
        let fence = claim(&state, &job).await;
        sqlx::query("UPDATE task_runs SET lease_expires_at = '2020-01-01T00:00:00+00:00' WHERE id = $1")
            .bind(&job)
            .execute(pool)
            .await
            .unwrap();

        let asked_at = chrono::Utc::now().to_rfc3339();
        assert_eq!(cancel(&state, &run).await, (1, 1));
        let lease: Option<String> =
            sqlx::query_scalar("SELECT lease_expires_at FROM task_runs WHERE id = $1")
                .bind(&job)
                .fetch_one(pool)
                .await
                .unwrap();
        assert!(lease.is_some_and(|lease| lease > asked_at), "a lease from the request on");
        assert!(db::claim_teardowns(pool, "w2", 10, &classes, &[]).await.unwrap().is_empty());
        assert!(db::settle_stopped_task(pool, &job, WORKER, fence).await.unwrap());
        assert_eq!(detail(&state, &run).await.status, "cancelled");
    }

    /// An engine from before the stop request finishes a task that was asked
    /// to stop as it always did, and ends no run. The sweep ends the run it
    /// leaves, and clears the request the task still carried.
    #[tokio::test]
    async fn a_run_an_older_engine_left_with_nothing_stopping_is_ended_by_the_sweep() {
        let Some(state) = test_state("stop-live").await else { return };
        let pool = &state.write_pool;
        let (run, _) = run_of(&state, "  - name: job
    command: [\"true\"]
").await;
        let job = task_id(&state, &run, "job").await;
        claim(&state, &job).await;
        assert_eq!(cancel(&state, &run).await, (1, 1));

        // Its mark, which knows no request.
        sqlx::query("UPDATE task_runs SET status = 'succeeded', claimed_by = NULL WHERE id = $1")
            .bind(&job)
            .execute(pool)
            .await
            .unwrap();
        assert_eq!(detail(&state, &run).await.status, "running", "nothing ended it");

        // Other tests' runs are in the same datastore, so only this one is
        // looked at: it was among those the sweep ended.
        assert!(db::end_stopped_runs(pool).await.unwrap() >= 1);
        let seen = detail(&state, &run).await;
        assert_eq!(seen.status, "cancelled");
        assert_eq!(seen.tasks[0].status, "succeeded", "what it did is not rewritten");
        assert!(seen.tasks[0].stop_requested_at.is_none());
        assert_eq!(completed_events(&state, &run).await.len(), 1);
    }

    /// A stopping task whose lease has lapsed is claimed for a teardown
    /// alone: by an engine that serves its class, under a new claim, with no
    /// attempt spent. It is never handed back to run again.
    #[tokio::test]
    async fn a_stopping_task_whose_lease_lapsed_is_claimed_for_a_teardown_alone() {
        let Some(state) = test_state("stop-live").await else { return };
        let pool = &state.write_pool;
        let (run, class) = run_of(&state, "  - name: job\n    command: [\"true\"]\n").await;
        let classes = [class];
        let job = task_id(&state, &run, "job").await;
        let fence = claim(&state, &job).await;
        assert_eq!(cancel(&state, &run).await, (1, 1));
        assert!(
            db::claim_teardowns(pool, "w2", 10, &classes, &[]).await.unwrap().is_empty(),
            "its worker's lease still holds"
        );

        // Its worker could not see the teardown through, and says so.
        assert!(!db::release_stopping_task(pool, &job, "another", fence, -1).await.unwrap());
        assert!(db::release_stopping_task(pool, &job, WORKER, fence, -1).await.unwrap());
        assert!(
            db::claim_teardowns(pool, "w2", 10, &["some-other-class".to_string()], &[])
                .await
                .unwrap()
                .is_empty(),
            "not by an engine that does not serve its class"
        );
        assert!(
            db::claim_teardowns(pool, WORKER, 10, &classes, &[job.clone()]).await.unwrap().is_empty(),
            "nor by the engine that still has its execution out"
        );

        let torn = db::claim_teardowns(pool, "w2", 10, &classes, &[]).await.unwrap();
        assert_eq!(torn.len(), 1);
        assert_eq!((torn[0].id.as_str(), torn[0].version), (job.as_str(), fence));
        assert!(torn[0].stop_requested_at.is_some());
        let (status, attempt, by): (String, i64, Option<String>) =
            sqlx::query_as("SELECT status, attempt, claimed_by FROM task_runs WHERE id = $1")
                .bind(&job)
                .fetch_one(pool)
                .await
                .unwrap();
        assert_eq!(
            (status.as_str(), attempt, by.as_deref()),
            ("running", 1, Some("w2")),
            "still running, and no attempt spent on a teardown"
        );
        assert!(db::claim_teardowns(pool, "w3", 10, &classes, &[]).await.unwrap().is_empty());

        // The worker that held it is too late; the teardown's word counts.
        assert!(!db::settle_stopped_task(pool, &job, WORKER, fence).await.unwrap());
        assert!(db::settle_stopped_task(pool, &job, "w2", fence + 1).await.unwrap());
        assert_eq!(detail(&state, &run).await.status, "cancelled");
    }
}

/// Quota back-pressure against a real datastore: the Postgres half of
/// `dagron_core::db::requeue_quota_wait`, and what the run detail and the graph
/// then serve for the task. Skipped without `TEST_DATABASE_URL`.
#[cfg(test)]
mod quota_wait_live_tests {
    use dagron_core::db;
    use dagron_core::quota_wait::QuotaRequeue;

    use super::redrive_tests::test_state;
    use super::*;
    use crate::routes::{graph, runs};

    const WORKER: &str = "quota-live-worker";

    const REFUSAL: &str = "not started: pod refused by namespace ResourceQuota 'tenant-quota' \
        (requested: limits.cpu=550m, used: limits.cpu=3750m, limited: limits.cpu=4)";

    /// `task_runs` as these tests need to see it: the raw columns, not the
    /// guarded projection the read endpoints serve.
    #[derive(Debug, PartialEq, sqlx::FromRow)]
    struct QuotaRow {
        status: String,
        attempt: i64,
        version: i64,
        claimed_by: Option<String>,
        lease_expires_at: Option<String>,
        scheduled_at: Option<String>,
        quota_wait_since: Option<String>,
        quota_waits: i64,
        quota_wait_version: Option<i64>,
        quota_wait_detail: Option<String>,
    }

    async fn quota_row(state: &AppState, task: &str) -> QuotaRow {
        sqlx::query_as(
            "SELECT status, attempt, version, claimed_by, lease_expires_at, scheduled_at,
                    quota_wait_since, quota_waits, quota_wait_version, quota_wait_detail
             FROM task_runs WHERE id = $1",
        )
        .bind(task)
        .fetch_one(&state.write_pool)
        .await
        .unwrap()
    }

    /// A fresh one-task run; returns `(run_id, task_id)`.
    async fn one_task_run(state: &AppState) -> (String, String) {
        let yaml = format!(
            "name: quota-{}\ntasks:\n  - name: pod\n    command: [\"true\"]\n",
            uuid::Uuid::new_v4()
        );
        let run_id = control::submit_yaml_as(state, &yaml, &yaml, &BTreeMap::new(), None)
            .await
            .unwrap();
        let task_id: String =
            sqlx::query_scalar("SELECT id FROM task_runs WHERE run_id = $1 AND name = 'pod'")
                .bind(&run_id)
                .fetch_one(&state.write_pool)
                .await
                .unwrap();
        (run_id, task_id)
    }

    /// The engine's next dispatch of the task, as the one statement a claim is.
    /// No engine runs here, and `claim_ready` would take other tests' rows from
    /// the shared database. It does not wait for `scheduled_at`, so it stands in
    /// for the backoff elapsing too. Returns the fence of the claim.
    async fn claim(state: &AppState, task: &str) -> i64 {
        let lease = (chrono::Utc::now() + chrono::TimeDelta::seconds(300)).to_rfc3339();
        sqlx::query_scalar(
            "UPDATE task_runs
             SET status = 'running', claimed_by = $1, lease_expires_at = $2,
                 attempt = attempt + 1, version = version + 1
             WHERE id = $3
             RETURNING version",
        )
        .bind(WORKER)
        .bind(&lease)
        .bind(task)
        .fetch_one(&state.write_pool)
        .await
        .unwrap()
    }

    /// The run's one task as the console is told about it: its row in
    /// `GET /api/runs/{id}` and its node in the graph.
    async fn served(state: &AppState, run: &str) -> (runs::TaskRow, graph::GraphNode) {
        let viewer = || {
            AuthUser(crate::auth::SessionClaims {
                sub: "u1".to_string(),
                email: "u1@example.com".to_string(),
                name: "U1".to_string(),
                groups: vec![],
                exp: 0,
            })
        };
        let Json(detail) = runs::get_run(viewer(), State(state.clone()), Path(run.to_string()))
            .await
            .unwrap();
        let Json(dag) = graph::get_graph(viewer(), State(state.clone()), Path(run.to_string()))
            .await
            .unwrap();
        (
            detail.tasks.into_iter().next().expect("the run has its task"),
            dag.nodes.into_iter().next().expect("the graph has its node"),
        )
    }

    fn rfc3339(t: &Option<String>) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339(t.as_deref().expect("timestamp is set"))
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    /// The give-back: the row is `ready` again with no claim, the attempt the
    /// claim consumed, a probe time ahead of it and the marker that says it is
    /// waiting. Only the holder of the claim can do it, the console is told why
    /// the task is not running, and a second refusal continues the wait.
    #[tokio::test]
    async fn a_quota_refusal_hands_the_claim_back() {
        let Some(state) = test_state("quota-wait-live").await else { return };
        let pool = &state.write_pool;
        let (run, task) = one_task_run(&state).await;
        let fresh = quota_row(&state, &task).await;

        let fence = claim(&state, &task).await;
        let held = quota_row(&state, &task).await;
        assert_eq!(
            (held.status.as_str(), held.attempt, held.version),
            ("running", fresh.attempt + 1, fence)
        );

        // Anyone but the holder of this claim gets nothing back.
        for (worker, f) in [("another-worker", fence), (WORKER, fence + 1), (WORKER, fence - 1)] {
            assert_eq!(
                db::requeue_quota_wait(pool, &task, worker, f, REFUSAL, 3600).await.unwrap(),
                QuotaRequeue::Stale,
                "{worker} with fence {f} does not hold the claim"
            );
        }
        assert_eq!(quota_row(&state, &task).await, held, "a stale call changes nothing");

        let asked = chrono::Utc::now();
        let outcome = db::requeue_quota_wait(pool, &task, WORKER, fence, REFUSAL, 3600).await.unwrap();
        let QuotaRequeue::Requeued { waits, waited_secs, retry_in_secs } = outcome else {
            panic!("a first refusal inside the bound is a requeue, got {outcome:?}");
        };
        assert_eq!((waits, waited_secs), (1, 0));
        assert!((3..=6).contains(&retry_in_secs), "5 s give or take the jitter, got {retry_in_secs}");

        let waiting = quota_row(&state, &task).await;
        assert_eq!(waiting.status, "ready");
        assert!(waiting.claimed_by.is_none() && waiting.lease_expires_at.is_none(), "the claim is gone");
        assert_eq!(waiting.attempt, fresh.attempt, "the attempt the claim consumed is given back");
        assert_eq!(waiting.version, fence, "version is left alone, as retry_task leaves it");
        assert_eq!(waiting.quota_wait_version, Some(waiting.version), "which is what marks the row as waiting");
        assert_eq!(waiting.quota_waits, 1);
        assert_eq!(waiting.quota_wait_detail.as_deref(), Some(REFUSAL));
        let since = rfc3339(&waiting.quota_wait_since);
        assert!(since >= asked, "the clock starts at this refusal");
        assert!(
            rfc3339(&waiting.scheduled_at) >= since + chrono::TimeDelta::milliseconds(3_750),
            "due a backoff after the refusal, not at it"
        );

        // Other tests' waiters share the database, so at least this one.
        let stats = db::quota_wait_stats(pool).await.unwrap();
        assert!(stats.waiting >= 1 && stats.oldest_since.is_some(), "the waiter is counted: {stats:?}");

        let (row, node) = served(&state, &run).await;
        assert_eq!(row.status, "ready");
        assert_eq!(
            (row.quota_wait_since.as_ref(), row.quota_wait_detail.as_deref(), row.quota_waits),
            (waiting.quota_wait_since.as_ref(), Some(REFUSAL), 1)
        );
        assert_eq!(
            (node.quota_wait_since.as_ref(), node.quota_wait_detail.as_deref()),
            (waiting.quota_wait_since.as_ref(), Some(REFUSAL))
        );

        // The next probe is claimed. The columns still hold the wait, but a
        // task that is running is not served as waiting.
        let fence = claim(&state, &task).await;
        let (row, node) = served(&state, &run).await;
        assert_eq!(row.status, "running");
        assert_eq!((row.quota_wait_since, row.quota_wait_detail, row.quota_waits), (None, None, 0));
        assert_eq!((node.quota_wait_since, node.quota_wait_detail), (None, None));

        // It is refused too: the same episode, on the same clock.
        let again = format!("{REFUSAL} again");
        let outcome = db::requeue_quota_wait(pool, &task, WORKER, fence, &again, 3600).await.unwrap();
        assert!(
            matches!(outcome, QuotaRequeue::Requeued { waits: 2, .. }),
            "a second refusal continues the episode, got {outcome:?}"
        );
        let second = quota_row(&state, &task).await;
        assert_eq!(second.quota_wait_since, waiting.quota_wait_since, "one episode, one clock");
        assert_eq!(
            (second.status.as_str(), second.attempt, second.quota_waits),
            ("ready", fresh.attempt, 2),
            "two refusals consumed no attempt"
        );
        assert_eq!((second.version, second.quota_wait_version), (fence, Some(fence)));
        assert_eq!(second.quota_wait_detail.as_deref(), Some(again.as_str()));
    }

    /// At the bound the datastore reports and steps back: nothing is written,
    /// the claim is still the caller's to fail under the same fence, and a
    /// failed task that once waited is not served as waiting.
    #[tokio::test]
    async fn a_quota_wait_expires_without_writing() {
        let Some(state) = test_state("quota-wait-live-2").await else { return };
        let pool = &state.write_pool;
        let (run, task) = one_task_run(&state).await;
        let fence = claim(&state, &task).await;
        assert!(matches!(
            db::requeue_quota_wait(pool, &task, WORKER, fence, REFUSAL, 600).await.unwrap(),
            QuotaRequeue::Requeued { waits: 1, .. }
        ));
        // The episode began fifteen minutes ago, against a ten-minute bound.
        let since = (chrono::Utc::now() - chrono::TimeDelta::seconds(900)).to_rfc3339();
        sqlx::query("UPDATE task_runs SET quota_wait_since = $1 WHERE id = $2")
            .bind(&since)
            .bind(&task)
            .execute(pool)
            .await
            .unwrap();

        let fence = claim(&state, &task).await;
        let held = quota_row(&state, &task).await;
        let outcome = db::requeue_quota_wait(pool, &task, WORKER, fence, REFUSAL, 600).await.unwrap();
        let QuotaRequeue::Expired { waits, waited_secs } = outcome else {
            panic!("past the bound on a continuing episode, got {outcome:?}");
        };
        assert_eq!(waits, 2, "the refusal that reached the bound is counted");
        assert!(waited_secs >= 900, "measured from the first refusal, got {waited_secs}");

        assert_eq!(quota_row(&state, &task).await, held, "nothing was written");
        assert_eq!(
            (held.status.as_str(), held.claimed_by.as_deref()),
            ("running", Some(WORKER)),
            "still running and still claimed"
        );
        assert_eq!((held.quota_waits, held.quota_wait_since.as_deref()), (1, Some(since.as_str())));

        assert!(
            db::mark_task_failed(pool, &task, WORKER, fence, Some("gave up waiting for namespace quota".into()))
                .await
                .unwrap(),
            "the fence still holds for the failure that follows"
        );
        let (row, node) = served(&state, &run).await;
        assert_eq!(row.status, "failed");
        assert_eq!((row.quota_wait_since, row.quota_wait_detail, row.quota_waits), (None, None, 0));
        assert_eq!((node.quota_wait_since, node.quota_wait_detail), (None, None));
    }
}

#[cfg(test)]
mod run_end_live_tests {
    use super::redrive_tests::test_state;
    use super::*;
    use crate::auth::SessionClaims;

    const SHA: &str = "4b4708e725f29891d22b4c96af9f0a403ff51c7c";

    fn caller() -> AuthUser {
        AuthUser(SessionClaims {
            sub: "sub-ops".to_string(),
            email: "ops@example.com".to_string(),
            name: String::new(),
            groups: vec![],
            exp: 0,
        })
    }

    /// The statuses of `run_id`'s `run.completed` outbox events, oldest first.
    async fn completed_events(state: &AppState, run_id: &str) -> Vec<String> {
        let payloads: Vec<String> = sqlx::query_scalar(
            "SELECT payload FROM event_outbox WHERE run_id = $1 AND event_type = 'run.completed'
             ORDER BY created_at",
        )
        .bind(run_id)
        .fetch_all(&state.write_pool)
        .await
        .unwrap();
        payloads
            .iter()
            .map(|p| {
                let v: serde_json::Value = serde_json::from_str(p).unwrap();
                assert_eq!(v["run_id"], run_id, "{p}");
                v["status"].as_str().unwrap().to_string()
            })
            .collect()
    }

    /// How many times the end-of-run sink `ends` is handed `run_id`'s end,
    /// draining every end it is handed as an engine's end-of-run worker would.
    /// Other runs' ends it is handed (other tests', in the shared database)
    /// are done and ignored: the sink is the test's own.
    async fn times_claimed(
        state: &AppState,
        ends: &dagron_core::db::OutboxSink<'_>,
        run_id: &str,
    ) -> (usize, Option<String>) {
        let (mut n, mut status) = (0, None);
        loop {
            let claimed = dagron_core::db::claim_outbox_batch(&state.write_pool, ends, 1000, 60)
                .await
                .unwrap();
            if claimed.is_empty() {
                return (n, status);
            }
            for ev in claimed {
                dagron_core::db::mark_outbox_delivered(&state.write_pool, ends.name, &ev.id)
                    .await
                    .unwrap();
                if ev.run_id == run_id {
                    let v: serde_json::Value = serde_json::from_str(&ev.payload).unwrap();
                    n += 1;
                    status = v["status"].as_str().map(str::to_string);
                }
            }
        }
    }

    /// How many times the start claim hands `run_id` out, draining every
    /// stamped run in the database as an engine replica would.
    async fn times_announced(state: &AppState, run_id: &str) -> usize {
        let mut n = 0;
        loop {
            let claimed = dagron_core::db::claim_pending_starts(&state.write_pool, 1000)
                .await
                .unwrap();
            if claimed.is_empty() {
                return n;
            }
            n += claimed.iter().filter(|id| *id == run_id).count();
        }
    }

    /// The `pending` status on Postgres: a `notify.git` run submitted through
    /// the gateway (which holds no forge token) is stamped for the engine to
    /// announce, once, and a run cancelled before any engine announced it never
    /// is.
    #[tokio::test]
    async fn a_notify_git_run_is_announced_once_unless_it_ends_first() {
        let Some(state) = test_state("run-end-live-start").await else {
            return;
        };
        let yaml = format!(
            "name: ci-{}
parameters: {{ commit_sha: \"\" }}
notify:
  git: {{ provider: github, repo: acme/etl, sha: \"{{{{ commit_sha }}}}\" }}
tasks:
  - name: gate
    type: approval
",
            uuid::Uuid::new_v4()
        );
        let params: BTreeMap<String, String> = [("commit_sha".to_string(), SHA.to_string())]
            .into_iter()
            .collect();

        let announced = control::submit_yaml_as(&state, &yaml, &yaml, &params, None)
            .await
            .unwrap();
        let cancelled = control::submit_yaml_as(&state, &yaml, &yaml, &params, None)
            .await
            .unwrap();
        let Json(r) = control::cancel_run(caller(), State(state.clone()), Path(cancelled.clone()))
            .await
            .unwrap();
        assert_eq!(r.cancelled, 1);

        // One drain sees both runs: only the running one is handed out.
        let mut seen = std::collections::HashMap::new();
        loop {
            let claimed = dagron_core::db::claim_pending_starts(&state.write_pool, 1000)
                .await
                .unwrap();
            if claimed.is_empty() {
                break;
            }
            for id in claimed {
                *seen.entry(id).or_insert(0) += 1;
            }
        }
        assert_eq!(seen.get(&announced), Some(&1));
        assert_eq!(seen.get(&cancelled), None, "its end cleared its start");
        assert_eq!(
            times_announced(&state, &announced).await,
            0,
            "announced once"
        );
        assert_eq!(
            dagron_core::db::run_status(&state.write_pool, &cancelled)
                .await
                .unwrap(),
            Some(dagron_core::models::RunStatus::Cancelled)
        );
    }

    /// On Postgres: a run keeps the parameters it was submitted with,
    /// merged over its spec's defaults, so `notify.git`'s `sha` resolves at
    /// the run's end.
    #[tokio::test]
    async fn a_run_keeps_the_parameters_it_was_submitted_with() {
        let Some(state) = test_state("run-end-live-params").await else {
            return;
        };
        let yaml = format!(
            "name: ci-{}\nparameters: {{ commit_sha: \"\", keep: k }}\ntasks:\n  - name: gate\n    type: approval\n",
            uuid::Uuid::new_v4()
        );
        let params: BTreeMap<String, String> = [("commit_sha".to_string(), SHA.to_string())]
            .into_iter()
            .collect();
        let run_id = control::submit_yaml_as(&state, &yaml, &yaml, &params, None)
            .await
            .unwrap();

        let stored = dagron_core::db::run_parameters(&state.write_pool, &run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.get("commit_sha").map(String::as_str), Some(SHA));
        assert_eq!(
            stored.get("keep").map(String::as_str),
            Some("k"),
            "a default the caller left alone"
        );
        assert!(
            dagron_core::db::run_parameters(&state.write_pool, "no-such-run")
                .await
                .unwrap()
                .is_none()
        );
    }

    /// On Postgres: a resubmit is a new run of the same definition with the
    /// same parameters. The stored definition holds only the defaults, and a
    /// resubmit passed nothing over them, so the new run was one nobody had
    /// passed anything to. A run submitted without parameters, and one from
    /// before they were kept, resubmit as they did.
    #[tokio::test]
    async fn a_resubmit_carries_the_run_s_parameters() {
        let Some(state) = test_state("run-end-live-resubmit").await else {
            return;
        };
        let yaml = format!(
            "name: ci-{}\nparameters: {{ commit_sha: \"\", keep: k }}\ntasks:\n  - name: gate\n    type: approval\n",
            uuid::Uuid::new_v4()
        );
        let params: BTreeMap<String, String> = [("commit_sha".to_string(), SHA.to_string())]
            .into_iter()
            .collect();
        let stored = |run_id: String| {
            let state = state.clone();
            async move {
                dagron_core::db::run_parameters(&state.write_pool, &run_id)
                    .await
                    .unwrap()
                    .unwrap()
            }
        };
        let resubmit = |run_id: String| {
            let state = state.clone();
            async move { control::resubmit_run(caller(), State(state), Path(run_id)).await }
        };

        let first = control::submit_yaml_as(&state, &yaml, &yaml, &params, None)
            .await
            .unwrap();
        let (code, Json(again)) = resubmit(first.clone()).await.unwrap();
        assert_eq!(code, StatusCode::CREATED);
        assert_ne!(again.run_id, first, "a run of its own");
        let carried = stored(again.run_id.clone()).await;
        assert_eq!(carried.get("commit_sha").map(String::as_str), Some(SHA));
        assert_eq!(carried, stored(first.clone()).await, "the parameters it was created with");
        // And a resubmit of the resubmit, which has them stored in its turn.
        let (_, Json(third)) = resubmit(again.run_id.clone()).await.unwrap();
        assert_eq!(stored(third.run_id).await, carried);

        // Submitted without any: its spec's defaults, as before.
        let plain = control::submit_yaml(&state, &yaml, &yaml).await.unwrap();
        let (_, Json(plain_again)) = resubmit(plain).await.unwrap();
        let defaults = stored(plain_again.run_id).await;
        assert_eq!(defaults.get("commit_sha").map(String::as_str), Some(""));
        assert_eq!(defaults.get("keep").map(String::as_str), Some("k"));

        // A run from before parameters were kept has none stored: the same.
        sqlx::query("UPDATE workflow_runs SET parameters = NULL WHERE id = $1")
            .bind(&first)
            .execute(&state.write_pool)
            .await
            .unwrap();
        let (_, Json(old_again)) = resubmit(first.clone()).await.unwrap();
        assert_eq!(stored(old_again.run_id).await, defaults);

        // Parameters that cannot be read are no parameters to run with.
        sqlx::query("UPDATE workflow_runs SET parameters = 'not json' WHERE id = $1")
            .bind(&first)
            .execute(&state.write_pool)
            .await
            .unwrap();
        let (code, why) = resubmit(first.clone()).await.err().expect("refused");
        assert_eq!(code, StatusCode::INTERNAL_SERVER_ERROR, "{why}");
        assert!(why.contains("stored parameters"), "{why}");

        let (code, _) = resubmit("no-such-run".to_string()).await.err().expect("refused");
        assert_eq!(code, StatusCode::NOT_FOUND);
    }

    /// On Postgres: a resubmit of a run pinned to an environment reads the
    /// environment again, as any new run does. What the first run was given
    /// from it is not carried as if a caller had passed it: a variable since
    /// changed is the new value, and one since removed is gone. The caller's
    /// own parameters are carried all the same.
    #[tokio::test]
    async fn a_resubmit_reads_a_declared_environment_again() {
        let Some(state) = test_state("run-end-live-resubmit-env").await else {
            return;
        };
        let tag = uuid::Uuid::new_v4().simple().to_string();
        let env = format!("env-{tag}");
        let set_variables = |variables: &'static str| {
            let (state, env) = (state.clone(), env.clone());
            async move {
                sqlx::query(
                    "INSERT INTO environments (id, name, variables, created_at, updated_at)
                     VALUES ($1, $1, $2, 'now', 'now')
                     ON CONFLICT (id) DO UPDATE SET variables = EXCLUDED.variables",
                )
                .bind(&env)
                .bind(variables)
                .execute(&state.write_pool)
                .await
                .unwrap();
            }
        };
        set_variables(r#"{"REGION": "us", "OLD": "gone-soon"}"#).await;
        let yaml = format!(
            "name: ci-{tag}\nenvironment: {env}\nparameters: {{ channel: stable }}\ntasks:\n  - name: gate\n    type: approval\n"
        );
        let params: BTreeMap<String, String> = [("channel".to_string(), "beta".to_string())]
            .into_iter()
            .collect();
        let first = control::submit_yaml_as(&state, &yaml, &yaml, &params, None)
            .await
            .unwrap();
        let of = |run_id: String| {
            let state = state.clone();
            async move {
                dagron_core::db::run_parameters(&state.write_pool, &run_id)
                    .await
                    .unwrap()
                    .unwrap()
            }
        };
        let was = of(first.clone()).await;
        assert_eq!(was.get("env.REGION").map(String::as_str), Some("us"));
        assert_eq!(was.get("env.OLD").map(String::as_str), Some("gone-soon"));

        set_variables(r#"{"REGION": "eu"}"#).await;
        let (_, Json(again)) = control::resubmit_run(caller(), State(state.clone()), Path(first))
            .await
            .unwrap();
        let now = of(again.run_id).await;
        assert_eq!(now.get("channel").map(String::as_str), Some("beta"), "the caller's own");
        assert_eq!(now.get("env.REGION").map(String::as_str), Some("eu"), "as it is now");
        assert_eq!(now.get("env.OLD"), None, "removed since: not brought back");
    }

    /// A run of `a → b → c` under `head`, ended as a failure at `a` leaves
    /// it: `a` ran once and failed with `boom`, `b` was cancelled before it
    /// ran, and `c`, which runs whatever became of `b`, was skipped. Returns
    /// the run and the three tasks' ids.
    async fn failed_chain(state: &AppState, head: &str) -> (String, [String; 3]) {
        let yaml = format!(
            "{head}tasks:\n  - name: a\n    command: [\"true\"]\n  - name: b\n    command: [\"true\"]\n    depends_on: [a]\n  - name: c\n    command: [\"true\"]\n    depends_on: [b]\n"
        );
        let run_id = control::submit_yaml(state, &yaml, &yaml).await.unwrap();
        fail_run(state, &run_id).await;
        let mut ids = Vec::new();
        for name in ["a", "b", "c"] {
            let id: String =
                sqlx::query_scalar("SELECT id FROM task_runs WHERE run_id = $1 AND name = $2")
                    .bind(&run_id)
                    .bind(name)
                    .fetch_one(&state.write_pool)
                    .await
                    .unwrap();
            ids.push(id);
        }
        (run_id, ids.try_into().unwrap())
    }

    /// End `run_id` as [`failed_chain`] says.
    async fn fail_run(state: &AppState, run_id: &str) {
        for (name, set) in [
            ("a", "status = 'failed', attempt = 1, output = 'boom', log = 'boom', fault_class = 'user',
                   scheduled_at = '2026-01-01T00:00:00Z', finished_at = '2026-01-01T00:00:05Z'"),
            ("b", "status = 'cancelled'"),
            ("c", "status = 'skipped'"),
        ] {
            sqlx::query(&format!("UPDATE task_runs SET {set} WHERE run_id = $1 AND name = $2"))
                .bind(run_id)
                .bind(name)
                .execute(&state.write_pool)
                .await
                .unwrap();
        }
        sqlx::query(
            "UPDATE workflow_runs SET status = 'failed', finished_at = '2026-01-01T00:00:05Z' WHERE id = $1",
        )
        .bind(run_id)
        .execute(&state.write_pool)
        .await
        .unwrap();
    }

    /// `(status, attempt, attempt_base, log, fault_class)` of a task's row.
    async fn task_round(state: &AppState, id: &str) -> (String, i64, i64, Option<String>, Option<String>) {
        sqlx::query_as("SELECT status, attempt, attempt_base, log, fault_class FROM task_runs WHERE id = $1")
            .bind(id)
            .fetch_one(&state.write_pool)
            .await
            .unwrap()
    }

    /// [`task_round`], of a task that was just put back and waits on nothing.
    /// It is `pending` until a sweep finds it, and the tests beside these
    /// share its database and sweep all of it (`db::advance_ready_tasks`), so
    /// by the time it is read it may be `ready`. Either is a task put back
    /// and not yet run, and this reads `pending` for both.
    async fn put_back(
        state: &AppState,
        id: &str,
    ) -> (String, i64, i64, Option<String>, Option<String>) {
        let (status, attempt, base, log, fault) = task_round(state, id).await;
        let status = if status == "ready" {
            "pending".to_string()
        } else {
            status
        };
        (status, attempt, base, log, fault)
    }

    /// A task's kept attempts, oldest first: `(attempt, reason, status, output)`.
    async fn kept(state: &AppState, id: &str) -> Vec<(i64, String, Option<String>, Option<String>)> {
        sqlx::query_as(
            "SELECT attempt, reason, status, output FROM task_attempts WHERE task_id = $1 ORDER BY attempt",
        )
        .bind(id)
        .fetch_all(&state.write_pool)
        .await
        .unwrap()
    }

    /// The events that say tasks of `run_id` were put back to run again.
    async fn run_again_events(state: &AppState, run_id: &str) -> Vec<serde_json::Value> {
        let payloads: Vec<String> = sqlx::query_scalar(
            "SELECT payload FROM event_outbox WHERE run_id = $1 AND event_type = 'run.rerun'
             ORDER BY created_at, id",
        )
        .bind(run_id)
        .fetch_all(&state.write_pool)
        .await
        .unwrap();
        payloads.iter().map(|p| serde_json::from_str(p).unwrap()).collect()
    }

    async fn run_status(state: &AppState, run_id: &str) -> String {
        sqlx::query_scalar("SELECT status FROM workflow_runs WHERE id = $1")
            .bind(run_id)
            .fetch_one(&state.write_pool)
            .await
            .unwrap()
    }

    /// On Postgres: a rerun through the gateway is the engine's rerun. It
    /// resets a rule-skipped task with the failed and the cancelled ones, and
    /// clears a reset task's log and fault verdict: this handler's own copy of
    /// the statement did neither. It keeps the attempt it replaces, numbers
    /// the task's attempts on from it, and writes one event.
    #[tokio::test]
    async fn a_rerun_through_the_gateway_is_the_engine_s() {
        let Some(state) = test_state("run-again-rerun").await else {
            return;
        };
        let name = format!("again-{}", uuid::Uuid::new_v4().simple());
        let (run, [a, b, c]) = failed_chain(&state, &format!("name: {name}\n")).await;

        let Json(did) = control::rerun_run(caller(), State(state.clone()), Path(run.clone()), None)
            .await
            .unwrap();
        assert_eq!(did.rerun, 3, "the failed, the cancelled and the skipped");
        assert_eq!(run_status(&state, &run).await, "running");
        assert_eq!(
            put_back(&state, &a).await,
            ("pending".to_string(), 0, 1, None, None),
            "its count starts again, the round before is counted, and its log and verdict are gone"
        );
        assert_eq!(task_round(&state, &c).await.0, "pending", "skipped for a's failure: run again");
        assert_eq!(
            kept(&state, &a).await,
            vec![(1, "rerun".to_string(), Some("failed".to_string()), Some("boom".to_string()))]
        );
        assert!(kept(&state, &b).await.is_empty(), "b never ran: nothing to keep");
        assert_eq!(
            run_again_events(&state, &run).await,
            vec![serde_json::json!({ "run_id": run, "by": "rerun", "reset_tasks": 3 })]
        );

        // The task view numbers the attempt on the row by the task's life, as
        // its history is numbered.
        let Json(history) = crate::routes::logs::get_task_attempts(
            caller(),
            State(state.clone()),
            Path((run.clone(), a.clone())),
            axum::extract::RawQuery(None),
        )
        .await
        .unwrap();
        assert_eq!(history.current_attempt, 2, "one attempt made: the next is its second");
        assert_eq!(history.attempts.len(), 1);
        assert_eq!(history.attempts[0].status.as_deref(), Some("failed"));
        assert_eq!(history.attempts[0].finished_at, "2026-01-01T00:00:05Z");
        assert!(!history.evicted);
        // And the task's own view says an attempt is behind it, though the row
        // has started none of its own yet.
        let Json(view) = crate::routes::logs::get_task_logs(
            caller(),
            State(state.clone()),
            Path((run.clone(), a.clone())),
            axum::extract::RawQuery(None),
        )
        .await
        .unwrap();
        assert_eq!((view.attempt, view.earlier_attempts), (1, 1));
        // The run's own log view reads the same rows, and still answers.
        let Json(whole) = crate::routes::logs::get_run_logs(
            caller(),
            State(state.clone()),
            Path(run.clone()),
            axum::extract::RawQuery(None),
        )
        .await
        .unwrap();
        assert_eq!(whole.run_id, run);

        // Running: not one to rerun. Unknown: not found.
        let (code, _) = control::rerun_run(caller(), State(state.clone()), Path(run.clone()), None)
            .await
            .err()
            .expect("refused");
        assert_eq!(code, StatusCode::CONFLICT);
        let (code, _) = control::rerun_run(caller(), State(state.clone()), Path("no-such-run".into()), None)
            .await
            .err()
            .expect("refused");
        assert_eq!(code, StatusCode::NOT_FOUND);
        assert_eq!(run_again_events(&state, &run).await.len(), 1, "one event for the one rerun");
    }

    /// A rerun's body, as a caller sends it.
    fn rerun_body(body: serde_json::Value) -> Option<Json<control::RerunBody>> {
        Some(Json(serde_json::from_value(body).unwrap()))
    }

    /// On Postgres: `{"from": "all"}` reruns a run whole, one that succeeded
    /// too, through the engine's own rerun of every task. What succeeded is
    /// kept as the attempt it replaces, and the event says `from`. A run that
    /// succeeded is still none to rerun from its failed tasks.
    #[tokio::test]
    async fn a_whole_rerun_through_the_gateway_puts_back_what_succeeded() {
        let Some(state) = test_state("run-again-all").await else {
            return;
        };
        let name = format!("again-{}", uuid::Uuid::new_v4().simple());
        let (run, [a, b, c]) = failed_chain(&state, &format!("name: {name}\n")).await;
        sqlx::query(
            "UPDATE task_runs SET status = 'succeeded', attempt = 1, output = 'ok', log = 'ok',
                 fault_class = NULL WHERE run_id = $1",
        )
        .bind(&run)
        .execute(&state.write_pool)
        .await
        .unwrap();
        sqlx::query("UPDATE workflow_runs SET status = 'succeeded' WHERE id = $1")
            .bind(&run)
            .execute(&state.write_pool)
            .await
            .unwrap();
        let rerun = |body: serde_json::Value| {
            let (state, run) = (state.clone(), run.clone());
            async move { control::rerun_run(caller(), State(state), Path(run), rerun_body(body)).await }
        };

        let (code, why) = rerun(serde_json::json!({})).await.err().expect("refused");
        assert_eq!(code, StatusCode::CONFLICT);
        assert!(why.ends_with("(failed/cancelled)"), "{why}");
        let (code, why) = rerun(serde_json::json!({ "from": "every" }))
            .await
            .err()
            .expect("refused");
        assert_eq!(code, StatusCode::BAD_REQUEST, "{why}");

        let Json(did) = rerun(serde_json::json!({ "from": "all" })).await.unwrap();
        assert_eq!(did.rerun, 3, "every task, each of which succeeded");
        assert_eq!(run_status(&state, &run).await, "running");
        assert_eq!(
            put_back(&state, &a).await,
            ("pending".to_string(), 0, 1, None, None)
        );
        for id in [&b, &c] {
            assert_eq!(
                task_round(&state, id).await,
                ("pending".to_string(), 0, 1, None, None)
            );
        }
        assert_eq!(
            kept(&state, &a).await,
            vec![(
                1,
                "rerun".to_string(),
                Some("succeeded".to_string()),
                Some("ok".to_string())
            )]
        );
        assert_eq!(
            run_again_events(&state, &run).await,
            vec![
                serde_json::json!({ "run_id": run, "by": "rerun", "reset_tasks": 3, "from": "all" })
            ]
        );

        let (code, why) = rerun(serde_json::json!({ "from": "all" }))
            .await
            .err()
            .expect("refused");
        assert_eq!(code, StatusCode::CONFLICT);
        assert!(why.ends_with("(succeeded/failed/cancelled)"), "{why}");
        let (code, _) = control::rerun_run(
            caller(),
            State(state.clone()),
            Path("no-such-run".into()),
            rerun_body(serde_json::json!({ "from": "all" })),
        )
        .await
        .err()
        .expect("refused");
        assert_eq!(code, StatusCode::NOT_FOUND);
    }

    /// On Postgres: a whole rerun of a run a task of which still owes the
    /// teardown of a remote job is a `409`, with nothing written. The rerun
    /// would reset the handle the teardown sweep has still to reach, and the
    /// job would run on. Once the debt is settled the run reruns.
    #[tokio::test]
    async fn a_whole_rerun_through_the_gateway_waits_for_an_owed_teardown() {
        let Some(state) = test_state("run-again-teardown").await else {
            return;
        };
        let pool = &state.write_pool;
        let name = format!("again-{}", uuid::Uuid::new_v4().simple());
        let (run, [a, _, _]) = failed_chain(&state, &format!("name: {name}\n")).await;
        // `a` was a deferred task cancelled while its job ran: the handle is
        // the teardown it owes.
        sqlx::query(
            "UPDATE task_runs SET status = 'cancelled', external_kind = 'http',
                 external_handle = 'job-1' WHERE id = $1",
        )
        .bind(&a)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query("UPDATE workflow_runs SET status = 'cancelled' WHERE id = $1")
            .bind(&run)
            .execute(pool)
            .await
            .unwrap();
        let rerun = || {
            let (state, run) = (state.clone(), run.clone());
            async move {
                control::rerun_run(
                    caller(),
                    State(state),
                    Path(run),
                    rerun_body(serde_json::json!({ "from": "all" })),
                )
                .await
            }
        };
        let handle = || async {
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT external_handle FROM task_runs WHERE id = $1",
            )
            .bind(&a)
            .fetch_one(pool)
            .await
            .unwrap()
        };

        let (code, why) = rerun().await.err().expect("refused");
        assert_eq!(code, StatusCode::CONFLICT);
        assert!(why.contains("teardown"), "{why}");
        assert_eq!(
            run_status(&state, &run).await,
            "cancelled",
            "nothing was written"
        );
        assert_eq!(
            handle().await.as_deref(),
            Some("job-1"),
            "the sweep still sees the job"
        );

        assert!(dagron_core::db::clear_external_handle(pool, &a, "job-1")
            .await
            .unwrap());
        let Json(did) = rerun().await.unwrap();
        assert_eq!(did.rerun, 3);
    }

    /// On Postgres: a whole rerun of a run whose runtime fan-out failed drops
    /// the instances, their edges before them (`task_dependencies` does not
    /// cascade), and the barrier waits for its producer alone. It is expanded
    /// again from what the producer prints the next time.
    #[tokio::test]
    async fn a_whole_rerun_through_the_gateway_fans_out_again() {
        use dagron_core::db;
        let Some(state) = test_state("run-again-fanout").await else {
            return;
        };
        let pool = &state.write_pool;
        let yaml = format!(
            "name: fan-{}\ntasks:\n  - name: list\n    command: [\"true\"]\n  - name: process\n    \
             command: [\"handle\", \"{{{{ item }}}}\"]\n    with_output_of: list\n    depends_on: [list]\n  \
             - name: summarize\n    command: [\"true\"]\n    depends_on: [process]\n",
            uuid::Uuid::new_v4().simple()
        );
        let run = control::submit_yaml(&state, &yaml, &yaml).await.unwrap();
        let barrier: String =
            sqlx::query_scalar("SELECT id FROM task_runs WHERE run_id = $1 AND name = 'process'")
                .bind(&run)
                .fetch_one(pool)
                .await
                .unwrap();
        // `list` prints its items, as a worker would leave it, and the barrier
        // is expanded over them.
        let listed = |items: &'static str| {
            let run = run.clone();
            async move {
                sqlx::query(
                    "UPDATE task_runs SET status = 'succeeded', attempt = 1, output = $2
                      WHERE run_id = $1 AND name = 'list'",
                )
                .bind(&run)
                .bind(items)
                .execute(pool)
                .await
                .unwrap();
                sqlx::query("UPDATE task_runs SET remaining_deps = 0 WHERE run_id = $1 AND name = 'process'")
                    .bind(&run)
                    .execute(pool)
                    .await
                    .unwrap();
                db::advance_ready_tasks(pool).await.unwrap();
                db::reconcile_fanouts(pool).await.unwrap();
            }
        };
        let instances = || async {
            sqlx::query_scalar::<_, String>(
                "SELECT id FROM task_runs WHERE fanout_parent = $1 ORDER BY id",
            )
            .bind(&barrier)
            .fetch_all(pool)
            .await
            .unwrap()
        };
        listed(r#"["a","b"]"#).await;
        let made = instances().await;
        assert_eq!(made.len(), 2, "expanded");
        // One instance succeeded and one failed: the run failed.
        for (id, status) in made.iter().zip(["succeeded", "failed"]) {
            sqlx::query("UPDATE task_runs SET status = $2, attempt = 1, output = $2 WHERE id = $1")
                .bind(id)
                .bind(status)
                .execute(pool)
                .await
                .unwrap();
        }
        for (name, status) in [("process", "failed"), ("summarize", "cancelled")] {
            sqlx::query("UPDATE task_runs SET status = $3 WHERE run_id = $1 AND name = $2")
                .bind(&run)
                .bind(name)
                .bind(status)
                .execute(pool)
                .await
                .unwrap();
        }
        sqlx::query("UPDATE workflow_runs SET status = 'failed' WHERE id = $1")
            .bind(&run)
            .execute(pool)
            .await
            .unwrap();

        let Json(did) = control::rerun_run(
            caller(),
            State(state.clone()),
            Path(run.clone()),
            rerun_body(serde_json::json!({ "from": "all" })),
        )
        .await
        .unwrap();
        assert_eq!(
            did.rerun, 3,
            "list, process and summarize: not the instances"
        );
        assert!(instances().await.is_empty(), "the instances went");
        let gone: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_runs WHERE id = ANY($1)")
            .bind(&made)
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(gone, 0);
        let waits_on: Vec<String> = sqlx::query_scalar(
            "SELECT t.name FROM task_dependencies d JOIN task_runs t ON t.id = d.dependency_id
              WHERE d.dependent_id = $1",
        )
        .bind(&barrier)
        .fetch_all(pool)
        .await
        .unwrap();
        assert_eq!(
            waits_on,
            ["list"],
            "the barrier waits for its producer alone"
        );

        listed(r#"["x","y","z"]"#).await;
        assert_eq!(
            instances().await.len(),
            3,
            "expanded again over the producer's new output"
        );
    }

    /// On Postgres, in the build that takes `params` on a rerun: they reach
    /// every task the rerun resets, the rule-skipped one with the failed and
    /// the cancelled. A task the override missed would run again on the input
    /// it had, beside siblings given the new one.
    #[cfg(feature = "enterprise")]
    #[tokio::test]
    async fn a_rerun_s_params_reach_every_task_it_resets() {
        let Some(state) = test_state("run-again-params").await else {
            return;
        };
        let name = format!("again-{}", uuid::Uuid::new_v4().simple());
        let (run, tasks) = failed_chain(&state, &format!("name: {name}
")).await;
        let body: control::RerunBody =
            serde_json::from_value(serde_json::json!({ "params": { "channel": "beta" } })).unwrap();

        let Json(did) =
            control::rerun_run(caller(), State(state.clone()), Path(run.clone()), Some(Json(body)))
                .await
                .unwrap();
        assert_eq!(did.rerun, 3);
        for (task, was) in tasks.iter().zip(["failed", "cancelled", "skipped"]) {
            let input: String = sqlx::query_scalar("SELECT input FROM task_runs WHERE id = $1")
                .bind(task)
                .fetch_one(&state.write_pool)
                .await
                .unwrap();
            let spec: serde_json::Value = serde_json::from_str(&input).unwrap();
            assert_eq!(spec["input"]["channel"], "beta", "the task that was {was}: {input}");
        }
    }

    /// On Postgres, in the build that takes `params`: a whole rerun's reach
    /// every task it puts back, the ones that succeeded among them.
    #[cfg(feature = "enterprise")]
    #[tokio::test]
    async fn a_whole_rerun_s_params_reach_every_task() {
        let Some(state) = test_state("run-again-all-params").await else {
            return;
        };
        let name = format!("again-{}", uuid::Uuid::new_v4().simple());
        let (run, tasks) = failed_chain(&state, &format!("name: {name}\n")).await;
        for sql in [
            "UPDATE task_runs SET status = 'succeeded' WHERE run_id = $1",
            "UPDATE workflow_runs SET status = 'succeeded' WHERE id = $1",
        ] {
            sqlx::query(sql)
                .bind(&run)
                .execute(&state.write_pool)
                .await
                .unwrap();
        }
        let body =
            rerun_body(serde_json::json!({ "from": "all", "params": { "channel": "beta" } }));

        let Json(did) = control::rerun_run(caller(), State(state.clone()), Path(run.clone()), body)
            .await
            .unwrap();
        assert_eq!(did.rerun, 3);
        for task in &tasks {
            let input: String = sqlx::query_scalar("SELECT input FROM task_runs WHERE id = $1")
                .bind(task)
                .fetch_one(&state.write_pool)
                .await
                .unwrap();
            let spec: serde_json::Value = serde_json::from_str(&input).unwrap();
            assert_eq!(spec["input"]["channel"], "beta", "{input}");
        }
    }

    /// On Postgres: a retry and a clear through the gateway are the engine's.
    /// Each keeps what it replaces and says so in an event, and each answers
    /// for a task that is not the run's, or not one it can act on.
    #[tokio::test]
    async fn a_retry_and_a_clear_through_the_gateway_are_the_engine_s() {
        let Some(state) = test_state("run-again-retry-clear").await else {
            return;
        };
        let name = format!("again-{}", uuid::Uuid::new_v4().simple());
        let (run, [a, b, _]) = failed_chain(&state, &format!("name: {name}\n")).await;
        let (other, _) = failed_chain(&state, &format!("name: {name}-other\n")).await;
        let retry = |run: String, task: String| {
            let state = state.clone();
            async move { control::retry_task(caller(), State(state), Path((run, task))).await }
        };
        let clear = |run: String, task: String| {
            let state = state.clone();
            async move { control::clear_task(caller(), State(state), Path((run, task))).await }
        };

        // Not this run's task; then a task that is not failed.
        let (code, _) = retry(other.clone(), a.clone()).await.err().expect("refused");
        assert_eq!(code, StatusCode::NOT_FOUND);
        sqlx::query("UPDATE task_runs SET status = 'running' WHERE id = $1")
            .bind(&b)
            .execute(&state.write_pool)
            .await
            .unwrap();
        let (code, _) = retry(run.clone(), b.clone()).await.err().expect("refused");
        assert_eq!(code, StatusCode::CONFLICT);
        let (code, _) = clear(run.clone(), b.clone()).await.err().expect("refused");
        assert_eq!(code, StatusCode::CONFLICT);
        let (code, _) = clear(other.clone(), a.clone()).await.err().expect("refused");
        assert_eq!(code, StatusCode::NOT_FOUND);
        sqlx::query("UPDATE task_runs SET status = 'cancelled' WHERE id = $1")
            .bind(&b)
            .execute(&state.write_pool)
            .await
            .unwrap();

        let Json(did) = retry(run.clone(), a.clone()).await.unwrap();
        assert!(did.retried);
        assert_eq!(run_status(&state, &run).await, "running");
        assert_eq!(
            task_round(&state, &a).await,
            ("ready".to_string(), 1, 0, None, None),
            "its count goes on, and its log and verdict are gone"
        );
        assert_eq!(kept(&state, &a).await.len(), 1);

        // Its second attempt succeeds, and is cleared with what depends on it.
        sqlx::query(
            "UPDATE task_runs SET status = 'succeeded', attempt = 2, output = 'ok',
                 finished_at = '2026-01-01T00:01:00Z' WHERE id = $1",
        )
        .bind(&a)
        .execute(&state.write_pool)
        .await
        .unwrap();
        let Json(did) = clear(run.clone(), a.clone()).await.unwrap();
        assert_eq!(did.cleared, 3, "a, and b and c behind it");
        assert_eq!(put_back(&state, &a).await, ("pending".to_string(), 0, 2, None, None));
        assert_eq!(
            kept(&state, &a).await,
            vec![
                (1, "retry".to_string(), Some("failed".to_string()), Some("boom".to_string())),
                (2, "clear".to_string(), Some("succeeded".to_string()), Some("ok".to_string())),
            ]
        );
        assert_eq!(
            run_again_events(&state, &run).await,
            vec![
                serde_json::json!({ "run_id": run, "by": "retry", "reset_tasks": 1, "task_id": a }),
                serde_json::json!({ "run_id": run, "by": "clear", "reset_tasks": 3, "task_id": a }),
            ]
        );
        assert!(run_again_events(&state, &other).await.is_empty());
    }

    /// On Postgres: a run that had ended asks its concurrency key before it
    /// runs again, as a new run of the key would. While the key is full a
    /// rerun, a retry and a clear each answer 429 and write nothing; once it
    /// is free the rerun goes through.
    #[tokio::test]
    async fn running_again_through_the_gateway_asks_the_concurrency_key() {
        let Some(state) = test_state("run-again-key").await else {
            return;
        };
        let name = format!("again-{}", uuid::Uuid::new_v4().simple());
        let head = format!("name: {name}\nmax_active_runs: 1\nconcurrency_key: stack-a\n");
        let (older, [a, _, _]) = failed_chain(&state, &head).await;
        // The key's next run, which holds it.
        let yaml = format!("{head}tasks:\n  - name: gate\n    type: approval\n");
        let newer = control::submit_yaml(&state, &yaml, &yaml).await.unwrap();

        let refused = [
            control::rerun_run(caller(), State(state.clone()), Path(older.clone()), None)
                .await
                .err()
                .expect("refused"),
            control::rerun_run(
                caller(),
                State(state.clone()),
                Path(older.clone()),
                rerun_body(serde_json::json!({ "from": "all" })),
            )
            .await
            .err()
            .expect("refused"),
            control::retry_task(caller(), State(state.clone()), Path((older.clone(), a.clone())))
                .await
                .err()
                .expect("refused"),
            control::clear_task(caller(), State(state.clone()), Path((older.clone(), a.clone())))
                .await
                .err()
                .expect("refused"),
        ];
        for (code, why) in &refused {
            assert_eq!(*code, StatusCode::TOO_MANY_REQUESTS, "{why}");
            assert!(why.contains(&name) && why.contains("stack-a"), "{why}");
        }
        assert_eq!(run_status(&state, &older).await, "failed");
        assert_eq!(task_round(&state, &a).await.0, "failed", "nothing was reset");
        assert!(kept(&state, &a).await.is_empty(), "and nothing kept");
        assert!(run_again_events(&state, &older).await.is_empty(), "and nothing announced");

        sqlx::query("UPDATE workflow_runs SET status = 'succeeded' WHERE id = $1")
            .bind(&newer)
            .execute(&state.write_pool)
            .await
            .unwrap();
        let Json(did) = control::rerun_run(caller(), State(state.clone()), Path(older.clone()), None)
            .await
            .unwrap();
        assert_eq!(did.rerun, 3);
        // And a new run of the key is the one refused now.
        assert!(control::submit_yaml(&state, &yaml, &yaml).await.is_err());
    }

    /// A one-task workflow under a name of its own, and `head` before its
    /// tasks: `(name, yaml)`.
    fn keyed_spec(head: &str) -> (String, String) {
        let name = format!("keyed-{}", uuid::Uuid::new_v4().simple());
        let yaml = format!("name: {name}\n{head}tasks:\n  - name: gate\n    type: approval\n");
        (name, yaml)
    }

    /// How many runs there are of the workflow `name`.
    async fn runs_named(state: &AppState, name: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM workflow_runs wr
             JOIN workflow_definitions d ON d.id = wr.definition_id WHERE d.name = $1",
        )
        .bind(name)
        .fetch_one(&state.write_pool)
        .await
        .unwrap()
    }

    /// The run `key` names for `principal`: `None` with no claim on it,
    /// `Some(None)` while one is unresolved.
    async fn key_names(state: &AppState, principal: &str, key: &str) -> Option<Option<String>> {
        sqlx::query_scalar(
            "SELECT run_id FROM run_idempotency WHERE principal = $1 AND idempotency_key = $2",
        )
        .bind(principal)
        .bind(key)
        .fetch_optional(&state.write_pool)
        .await
        .unwrap()
    }

    /// Submit `yaml` under `key`, as either route does.
    async fn keyed(
        state: &AppState,
        yaml: &str,
        principal: &str,
        key: &str,
    ) -> Result<control::Keyed, (StatusCode, String)> {
        control::submit_yaml_keyed(
            state, yaml, yaml, &BTreeMap::new(), "ops@example.com", None, principal, key, "same-request",
        )
        .await
    }

    /// On Postgres: a key and its run are one write. A submit under a key
    /// leaves the key naming its run; a repeat is answered with that run and
    /// makes none; a refused submit gives the key back; and two submits of one
    /// key at once make one run between them.
    #[tokio::test]
    async fn an_idempotency_key_names_its_run_from_the_write_that_creates_it() {
        let Some(state) = test_state("keyed-submit").await else {
            return;
        };
        let principal = format!("p-{}", uuid::Uuid::new_v4().simple());
        let (name, yaml) = keyed_spec("");

        let Ok(control::Keyed::Created(first)) = keyed(&state, &yaml, &principal, "k1").await else {
            panic!("the first submit under a key creates its run");
        };
        assert_eq!(key_names(&state, &principal, "k1").await, Some(Some(first.clone())));
        let Ok(control::Keyed::Replayed(again)) = keyed(&state, &yaml, &principal, "k1").await else {
            panic!("a repeat is answered with the first run");
        };
        assert_eq!(again, first);
        assert_eq!(runs_named(&state, &name).await, 1, "and makes no second run");

        // Refused: its workflow's one slot is taken. The key is usable again.
        let (serial, serial_yaml) = keyed_spec("max_active_runs: 1\n");
        control::submit_yaml(&state, &serial_yaml, &serial_yaml).await.unwrap();
        let Err((code, _)) = keyed(&state, &serial_yaml, &principal, "k2").await else {
            panic!("the workflow's cap refuses the submit");
        };
        assert_eq!(code, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(key_names(&state, &principal, "k2").await, None, "a refused submit holds no key");
        assert_eq!(runs_named(&state, &serial).await, 1, "only the run that holds the slot");

        // Two submits of one key at once: one run between them, whichever
        // way the second is answered.
        let (raced, raced_yaml) = keyed_spec("");
        let (a, b) = tokio::join!(
            keyed(&state, &raced_yaml, &principal, "k3"),
            keyed(&state, &raced_yaml, &principal, "k3"),
        );
        let mut created = Vec::new();
        for outcome in [a, b] {
            match outcome {
                Ok(control::Keyed::Created(run)) => created.push(run),
                Ok(control::Keyed::Replayed(run)) => {
                    assert_eq!(key_names(&state, &principal, "k3").await, Some(Some(run)))
                }
                Err((code, why)) => assert_eq!(code, StatusCode::CONFLICT, "{why}"),
            }
        }
        assert_eq!(created.len(), 1, "one of them created the run");
        assert_eq!(runs_named(&state, &raced).await, 1);
        assert_eq!(key_names(&state, &principal, "k3").await, Some(Some(created.remove(0))));
    }

    /// On Postgres: a submit whose claim was taken over while it was in
    /// flight creates nothing. The claim used to be pointed at the run in a
    /// statement of its own after the run was written, so a submit slower
    /// than the stale window, or one whose retry came after it died with the
    /// run already made, left two runs under one key.
    #[tokio::test]
    async fn a_submit_whose_claim_was_taken_over_creates_nothing() {
        let Some(state) = test_state("keyed-takeover").await else {
            return;
        };
        let principal = format!("p-{}", uuid::Uuid::new_v4().simple());
        let (name, yaml) = keyed_spec("");
        let claim = |key: &'static str| {
            let (state, principal) = (state.clone(), principal.clone());
            async move {
                match control::claim_idempotency_key(&state, &principal, key, "same-request").await {
                    Ok(control::Claim::Held(claimed_at)) => claimed_at,
                    _ => panic!("the key is free, or stale: this request holds it"),
                }
            }
        };
        let write = |claimed_at: String| {
            let (state, principal, yaml) = (state.clone(), principal.clone(), yaml.clone());
            async move {
                let dag = control::build_dag(&state, &yaml, &BTreeMap::new(), None)
                    .await
                    .unwrap();
                let claim = dagron_core::db::IdempotencyClaim {
                    principal: &principal,
                    key: "k",
                    claimed_at: &claimed_at,
                };
                dagron_core::db::create_run_claimed(&state.write_pool, &dag, &yaml, None, None, &claim)
                    .await
                    .unwrap()
            }
        };

        // A submit claims the key, and is slow. Its claim goes stale, and a
        // retry of the same request takes the key over.
        let slow = claim("k").await;
        sqlx::query(
            "UPDATE run_idempotency SET created_at = '2000-01-01T00:00:00+00:00'
              WHERE principal = $1 AND idempotency_key = 'k'",
        )
        .bind(&principal)
        .execute(&state.write_pool)
        .await
        .unwrap();
        let retry = claim("k").await;

        // The slow one comes to write its run. Its claim is not its own any
        // more (the row it wrote reads another time), and it writes nothing.
        let stale = "2000-01-01T00:00:00+00:00".to_string();
        assert_ne!(slow, retry);
        assert_eq!(write(stale.clone()).await, None);
        assert_eq!(write(slow.clone()).await, None);
        assert_eq!(runs_named(&state, &name).await, 0, "no run was left behind");
        assert_eq!(key_names(&state, &principal, "k").await, Some(None), "the retry still holds the key");

        // Nor can it give away a key that is not its own.
        control::release_idempotency_claim(&state, &principal, "k", &slow).await;
        assert_eq!(key_names(&state, &principal, "k").await, Some(None));

        // The retry's write makes the run and names it, in one step.
        let run = write(retry.clone()).await.expect("the claim is the retry's");
        assert_eq!(runs_named(&state, &name).await, 1);
        assert_eq!(key_names(&state, &principal, "k").await, Some(Some(run.clone())));
        // A resolved key is nobody's to resolve again, or to release.
        assert_eq!(write(retry.clone()).await, None);
        control::release_idempotency_claim(&state, &principal, "k", &retry).await;
        assert_eq!(key_names(&state, &principal, "k").await, Some(Some(run.clone())));
        assert_eq!(runs_named(&state, &name).await, 1);

        // And a request that comes for the key now is answered with that run.
        let Ok(control::Keyed::Replayed(answer)) = keyed(&state, &yaml, &principal, "k").await else {
            panic!("the key names a run");
        };
        assert_eq!(answer, run);
    }

    /// A run's end on Postgres. A cancel through the gateway and the
    /// run_timeout_secs sweep each end their run once: one `run.completed`
    /// event, handed once to an end-of-run sink, this test's own, configured
    /// before either.
    #[tokio::test]
    async fn a_run_ended_by_a_cancel_or_its_deadline_ends_once() {
        let Some(state) = test_state("run-end-live-cancel").await else {
            return;
        };
        let name = format!("test.ends.{}", uuid::Uuid::new_v4());
        let ends = dagron_core::db::OutboxSink {
            name: &name,
            event_types: &["run.completed"],
            adopts_legacy: false,
        };
        dagron_core::db::register_outbox_sink(&state.write_pool, &ends)
            .await
            .unwrap();

        // Cancel through dagron-api. The gate is parked on a human, which the
        // gateway's cancel used to skip, leaving the run running.
        let yaml = format!(
            "name: cancel-{}\ntasks:\n  - name: gate\n    type: approval\n  - name: after\n    command: [\"true\"]\n    depends_on: [gate]\n",
            uuid::Uuid::new_v4()
        );
        let run_id = control::submit_yaml_as(&state, &yaml, &yaml, &BTreeMap::new(), None)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE task_runs SET status = 'awaiting_approval' WHERE run_id = $1 AND name = 'gate'",
        )
        .bind(&run_id)
        .execute(&state.write_pool)
        .await
        .unwrap();

        let Json(r) = control::cancel_run(caller(), State(state.clone()), Path(run_id.clone()))
            .await
            .unwrap();
        assert_eq!(r.cancelled, 2, "the parked gate and the task behind it");
        let status: String = sqlx::query_scalar("SELECT status FROM workflow_runs WHERE id = $1")
            .bind(&run_id)
            .fetch_one(&state.write_pool)
            .await
            .unwrap();
        assert_eq!(status, "cancelled");
        let Json(r) = control::cancel_run(caller(), State(state.clone()), Path(run_id.clone()))
            .await
            .unwrap();
        assert_eq!(r.cancelled, 0, "a second cancel finds nothing to call off");
        assert_eq!(completed_events(&state, &run_id).await, ["cancelled"]);
        match control::cancel_run(caller(), State(state.clone()), Path("no-such-run".into())).await
        {
            Err(code) => assert_eq!(code, StatusCode::NOT_FOUND),
            Ok(_) => panic!("cancelling a run that does not exist answered 200"),
        }

        assert_eq!(
            times_claimed(&state, &ends, &run_id).await,
            (1, Some("cancelled".to_string()))
        );
        assert_eq!(
            times_claimed(&state, &ends, &run_id).await,
            (0, None),
            "claimed once"
        );

        // The deadline sweep, as an engine replica runs it.
        let yaml = format!(
            "name: slow-{}\nrun_timeout_secs: 3600\ntasks:\n  - name: gate\n    type: approval\n",
            uuid::Uuid::new_v4()
        );
        let run_id = control::submit_yaml_as(&state, &yaml, &yaml, &BTreeMap::new(), None)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE workflow_runs SET deadline_at = '2020-01-01T00:00:00+00:00' WHERE id = $1",
        )
        .bind(&run_id)
        .execute(&state.write_pool)
        .await
        .unwrap();
        let failed = dagron_core::db::cancel_overdue_runs(&state.write_pool)
            .await
            .unwrap();
        assert!(failed.iter().any(|(id, _)| *id == run_id), "{failed:?}");
        assert!(!dagron_core::db::cancel_overdue_runs(&state.write_pool)
            .await
            .unwrap()
            .iter()
            .any(|(id, _)| *id == run_id));
        assert_eq!(completed_events(&state, &run_id).await, ["failed"]);
        assert_eq!(
            times_claimed(&state, &ends, &run_id).await,
            (1, Some("failed".to_string()))
        );
        assert_eq!(times_claimed(&state, &ends, &run_id).await, (0, None));
    }
}

#[cfg(test)]
mod workflow_run_live_tests {
    use super::redrive_tests::test_state;
    use super::*;
    use crate::auth::SessionClaims;
    use crate::routes::workflows::{run_workflow, RunWorkflowBody};

    fn caller() -> AuthUser {
        AuthUser(SessionClaims {
            sub: "sub-ci".to_string(),
            email: "ci@example.com".to_string(),
            name: String::new(),
            groups: vec![],
            exp: 0,
        })
    }

    async fn run(
        state: &AppState,
        id: &str,
        key: Option<&str>,
        sha: &str,
    ) -> Result<(StatusCode, String), (StatusCode, String)> {
        let mut headers = axum::http::HeaderMap::new();
        if let Some(key) = key {
            headers.insert("idempotency-key", key.parse().unwrap());
        }
        let body = RunWorkflowBody {
            parameters: [("ci_sha".to_string(), sha.to_string())]
                .into_iter()
                .collect(),
            trigger: None,
        };
        run_workflow(
            caller(),
            State(state.clone()),
            Path(id.to_string()),
            headers,
            Some(Json(body)),
        )
        .await
        .map(|(code, Json(v))| (code, v["run_id"].as_str().unwrap().to_string()))
    }

    /// Running a stored workflow honours `Idempotency-Key` as `POST /api/runs`
    /// does: a retry gets the first run back, a different request under the
    /// same key is a 409, and no key is no idempotency.
    #[tokio::test]
    async fn running_a_stored_workflow_is_idempotent_under_a_key() {
        let Some(state) = test_state("workflow-run-idempotent").await else {
            return;
        };
        let id = uuid::Uuid::new_v4().to_string();
        let spec = format!("name: wf-{id}\nparameters: {{ ci_sha: \"\" }}\ntasks:\n  - name: gate\n    type: approval\n");
        sqlx::query(
            "INSERT INTO workflows (id, name, spec, created_at, updated_at) VALUES ($1, $2, $3, 'now', 'now')",
        )
        .bind(&id)
        .bind(format!("wf-{id}"))
        .bind(&spec)
        .execute(&state.write_pool)
        .await
        .unwrap();
        let key = format!("ci-{id}");

        let (code, first) = run(&state, &id, Some(&key), "a1").await.unwrap();
        assert_eq!(code, StatusCode::CREATED);
        let (code, again) = run(&state, &id, Some(&key), "a1").await.unwrap();
        assert_eq!(
            (code, again.as_str()),
            (StatusCode::OK, first.as_str()),
            "a replay, not a new run"
        );

        let (code, msg) = run(&state, &id, Some(&key), "b2").await.unwrap_err();
        assert_eq!(code, StatusCode::CONFLICT, "{msg}");

        let (_, one) = run(&state, &id, None, "a1").await.unwrap();
        let (_, two) = run(&state, &id, None, "a1").await.unwrap();
        assert_ne!(one, two, "without a key every request is a run");
        assert!(![one.as_str(), two.as_str()].contains(&first.as_str()));
    }
}

/// The trigger record, end to end against the engine's own schema: who may
/// write one, what a list and a run then say, and what `?include=ci` lists.
#[cfg(test)]
mod trigger_live_tests {
    use super::redrive_tests::test_state;
    use super::*;
    use crate::auth::SessionClaims;
    use crate::routes::control::{submit_run, SubmitBody};
    use crate::routes::runs::{
        get_run, list_refs, list_runs, list_sources, ListParams, RefsParams, SourcesParams,
    };
    use crate::routes::workflows::{list_workflows, run_workflow, ListQuery, RunWorkflowBody};

    fn caller(groups: &[&str]) -> AuthUser {
        AuthUser(SessionClaims {
            sub: "sub-ci".to_string(),
            email: "ci@example.com".to_string(),
            name: String::new(),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            exp: 0,
        })
    }

    fn trigger(event: &str) -> serde_json::Value {
        serde_json::json!({
            "kind": "ci", "actor": "octocat", "trust": "trusted", "forge_host": "github.com",
            "repo": "acme/widgets", "ref": "refs/pull/7/head", "sha": "a1", "base_sha": "b2",
            "pr": 7, "event_id": event,
        })
    }

    /// A pipeline as a CI service compiles it: tagged `ci`, and parked on an
    /// approval so that no engine is needed for it to stay a run.
    fn pipeline(name: &str) -> String {
        format!("name: {name}\ntags: [team.build, ci]\ntasks:\n  - name: gate\n    type: approval\n")
    }

    async fn submit(
        state: &AppState,
        groups: &[&str],
        name: &str,
        trigger: Option<serde_json::Value>,
        key: Option<&str>,
    ) -> Result<(StatusCode, String), (StatusCode, String)> {
        let mut headers = axum::http::HeaderMap::new();
        if let Some(key) = key {
            headers.insert("idempotency-key", key.parse().unwrap());
        }
        let body: SubmitBody =
            serde_json::from_value(serde_json::json!({ "yaml": pipeline(name), "trigger": trigger })).unwrap();
        submit_run(caller(groups), State(state.clone()), headers, Json(body))
            .await
            .map(|(code, Json(r))| (code, r.run_id))
    }

    async fn runs(state: &AppState, name: &str, trigger: Option<&str>) -> Vec<(String, String)> {
        let params = ListParams {
            status: None,
            name: Some(name.to_string()),
            trigger: trigger.map(str::to_string),
            repo: None,
            event: None,
            limit: None,
            offset: None,
        };
        let Json(rows) = list_runs(caller(&[]), State(state.clone()), Query(params)).await.unwrap();
        rows.into_iter().map(|r| (r.id, r.trigger_kind)).collect()
    }

    /// The ids of `name`'s runs for `repo`, sorted.
    async fn runs_for(state: &AppState, name: &str, repo: Option<&str>) -> Vec<String> {
        let params = ListParams {
            status: None,
            name: Some(name.to_string()),
            trigger: None,
            repo: repo.map(str::to_string),
            event: None,
            limit: None,
            offset: None,
        };
        let Json(rows) = list_runs(caller(&[]), State(state.clone()), Query(params)).await.unwrap();
        let mut ids: Vec<String> = rows.into_iter().map(|r| r.id).collect();
        ids.sort();
        ids
    }

    async fn workflows(state: &AppState, include: Option<&str>, tag: Option<&str>) -> Vec<crate::routes::workflows::WorkflowRow> {
        let q = ListQuery { tag: tag.map(str::to_string), include: include.map(str::to_string) };
        let Json(rows) = list_workflows(caller(&[]), State(state.clone()), Query(q)).await.unwrap();
        rows
    }

    /// A member's submit is CI's from then on: in the list, under the filter,
    /// on the run, and as a pipeline beside the stored workflows. The same
    /// spec submitted by a person with no record stays manual.
    #[tokio::test]
    async fn a_member_s_run_reads_ci_everywhere_and_another_s_stays_manual() {
        let Some(state) = test_state("trigger-recorded").await else {
            return;
        };
        let name = format!("ci-{}", uuid::Uuid::new_v4());
        let (code, marked) = submit(&state, &["ci:trigger"], &name, Some(trigger("e-1")), None).await.unwrap();
        assert_eq!(code, StatusCode::CREATED);
        let (_, plain) = submit(&state, &[], &name, None, None).await.unwrap();

        let mut all = runs(&state, &name, None).await;
        all.sort_by(|a, b| a.1.cmp(&b.1));
        assert_eq!(all, [(marked.clone(), "ci".to_string()), (plain.clone(), "manual".to_string())]);
        assert_eq!(runs(&state, &name, Some("ci")).await, [(marked.clone(), "ci".to_string())]);
        assert_eq!(runs(&state, &name, Some("manual")).await, [(plain.clone(), "manual".to_string())]);
        assert_eq!(runs(&state, &name, Some("schedule")).await, []);

        let Json(run) = get_run(caller(&[]), State(state.clone()), Path(marked.clone())).await.unwrap();
        let t = run.trigger.expect("the run carries its record");
        assert_eq!(run.trigger_kind, "ci");
        assert_eq!(
            (t.kind.as_str(), t.principal.as_str(), t.actor.as_deref(), t.trust.as_deref()),
            ("ci", "ci:ci@example.com", Some("octocat"), Some("trusted"))
        );
        assert_eq!(
            (t.forge_host.as_deref(), t.repo.as_deref(), t.git_ref.as_deref(), t.pr),
            (Some("github.com"), Some("acme/widgets"), Some("refs/pull/7/head"), Some(7))
        );
        assert_eq!(
            (t.sha.as_deref(), t.base_sha.as_deref(), t.event_id.as_deref()),
            (Some("a1"), Some("b2"), Some("e-1"))
        );
        // A person's run is theirs, and says so.
        let Json(run) = get_run(caller(&[]), State(state.clone()), Path(plain)).await.unwrap();
        let t = run.trigger.expect("a person's run carries a record too");
        assert_eq!(run.trigger_kind, "manual");
        assert_eq!(
            (
                t.kind.as_str(),
                t.principal.as_str(),
                t.actor.as_deref(),
                t.source.as_deref()
            ),
            (
                "manual",
                "user:ci@example.com",
                Some("ci@example.com"),
                None
            )
        );
        assert_eq!((t.trust, t.repo, t.event_id), (None, None, None));

        // Listed with the workflows only when asked for, and read-only: no
        // workflow has this id.
        assert!(workflows(&state, None, None).await.iter().all(|w| w.name != name));
        let listed = workflows(&state, Some("ci"), None).await;
        let row = listed.iter().find(|w| w.name == name).expect("the compiled pipeline is listed");
        assert_eq!(
            (row.source.as_str(), row.description.as_deref(), row.id.as_str()),
            ("ci", Some("github.com/acme/widgets"), format!("ci:github.com/acme/widgets:{name}").as_str())
        );
        assert_eq!(row.tags, ["team.build", "ci"]);
        assert_eq!((row.run_count, row.history.len()), (1, 1), "CI's run of it, not the person's");
        assert!(workflows(&state, Some("ci"), Some("ci")).await.iter().any(|w| w.name == name));
        assert!(workflows(&state, Some("ci"), Some("nightly")).await.iter().all(|w| w.name != name));
        assert!(workflows(&state, None, Some("ci")).await.iter().all(|w| w.name != name));
    }

    /// Two repositories may each have a pipeline of one name. Each is listed
    /// as a row of its own, and a row's repository narrows the run list to
    /// that row's runs, which the name alone does not.
    #[tokio::test]
    async fn a_pipeline_s_row_narrows_the_runs_to_its_repository() {
        let Some(state) = test_state("trigger-repo").await else {
            return;
        };
        let name = format!("ci-{}", uuid::Uuid::new_v4());
        let for_repo = |repo: &str, event: &str| {
            let mut t = trigger(event);
            t["repo"] = serde_json::json!(repo);
            t
        };
        let member = ["ci:trigger"];
        let (_, widgets) = submit(&state, &member, &name, Some(for_repo("acme/widgets", "e-1")), None).await.unwrap();
        let (_, gadgets) = submit(&state, &member, &name, Some(for_repo("acme/gadgets", "e-2")), None).await.unwrap();
        let (_, plain) = submit(&state, &[], &name, None, None).await.unwrap();

        let mut all = vec![widgets.clone(), gadgets.clone(), plain];
        all.sort();
        assert_eq!(runs_for(&state, &name, None).await, all, "the name alone: every run of it");
        assert_eq!(runs_for(&state, &name, Some("")).await, all, "an empty repo names none");
        assert_eq!(runs_for(&state, &name, Some("github.com/acme/widgets")).await, [widgets]);
        assert_eq!(runs_for(&state, &name, Some("github.com/acme/gadgets")).await, [gadgets]);
        assert!(runs_for(&state, &name, Some("github.com/acme/other")).await.is_empty());
        assert!(runs_for(&state, &name, Some("acme/widgets")).await.is_empty(), "the host is part of it");

        // What a row says its repository is, is what narrows to it.
        let listed = workflows(&state, Some("ci"), None).await;
        let rows: Vec<_> = listed.iter().filter(|w| w.name == name).collect();
        assert_eq!(rows.len(), 2, "a row for each repository");
        for row in rows {
            assert_eq!(row.run_count, 1);
            let repo = row.description.as_deref().expect("a pipeline's row names its repository");
            assert_eq!(runs_for(&state, &name, Some(repo)).await.len(), 1, "{repo}");
        }
    }

    /// The repositories runs came from are listed, and a repository's refs
    /// each have the runs for their latest commit and for no earlier one: a
    /// run of each pipeline, the latest where the commit was run for twice,
    /// whichever event each came of. An event's runs are found by its id.
    #[tokio::test]
    async fn a_repository_s_refs_each_have_the_runs_for_their_latest_commit() {
        let Some(state) = test_state("trigger-refs").await else {
            return;
        };
        // A repository of this test's own: the database is every test's.
        let unique = uuid::Uuid::new_v4();
        let repo = format!("acme/widgets-{unique}");
        let full = format!("github.com/{repo}");
        let (build, test) = (format!("build-{unique}"), format!("test-{unique}"));
        let on = |git_ref: &str, pr: Option<i64>, sha: &str, event: &str| {
            let mut t = trigger(event);
            t["repo"] = serde_json::json!(repo);
            t["ref"] = serde_json::json!(git_ref);
            t["pr"] = serde_json::json!(pr);
            t["sha"] = serde_json::json!(sha);
            Some(t)
        };
        let member = ["ci:trigger"];
        // Each event a moment after the one before: a run's time is what
        // orders them.
        let later = || tokio::time::sleep(std::time::Duration::from_millis(20));
        let main = "refs/heads/main";
        let (_, old) = submit(&state, &member, &build, on(main, None, "a1", "e-1"), None)
            .await
            .unwrap();
        later().await;
        let (_, main_build) = submit(&state, &member, &build, on(main, None, "a2", "e-2"), None)
            .await
            .unwrap();
        let (_, main_test) = submit(&state, &member, &test, on(main, None, "a2", "e-2"), None)
            .await
            .unwrap();
        later().await;
        let (_, pr_build) = submit(
            &state,
            &member,
            &build,
            on("refs/pull/7/head", Some(7), "b1", "e-3"),
            None,
        )
        .await
        .unwrap();

        let refs = |repo: Option<&str>, limit: Option<i64>| {
            let params = RefsParams {
                repo: repo.map(str::to_string),
                limit,
            };
            list_refs(caller(&[]), State(state.clone()), Query(params))
        };
        let Json(listed) = refs(Some(&full), None).await.unwrap();
        let ids = |of: &crate::routes::runs::RefRuns| {
            of.runs.iter().map(|r| r.id.clone()).collect::<Vec<_>>()
        };
        assert_eq!(listed.len(), 2, "a row a ref: {listed:?}");
        // The ref with the latest run first.
        let (pr, branch) = (&listed[0], &listed[1]);
        assert_eq!(
            (
                pr.git_ref.as_str(),
                pr.pr,
                pr.sha.as_deref(),
                pr.event_id.as_deref()
            ),
            ("refs/pull/7/head", Some(7), Some("b1"), Some("e-3"))
        );
        assert_eq!(ids(pr), [pr_build.clone()]);
        assert_eq!(
            (
                branch.git_ref.as_str(),
                branch.pr,
                branch.sha.as_deref(),
                branch.event_id.as_deref()
            ),
            (main, None, Some("a2"), Some("e-2"))
        );
        assert_eq!(branch.actor.as_deref(), Some("octocat"));
        assert_eq!(
            ids(branch),
            [main_build.clone(), main_test.clone()],
            "its latest commit's runs, by pipeline"
        );
        assert_eq!(
            branch
                .runs
                .iter()
                .map(|r| r.name.clone())
                .collect::<Vec<_>>(),
            [Some(build.clone()), Some(test.clone())]
        );
        assert!(!ids(branch).contains(&old), "and not an earlier push's");

        let Json(first) = refs(Some(&full), Some(1)).await.unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(
            first[0].git_ref, "refs/pull/7/head",
            "so many refs, the latest first"
        );
        // A repository is its host and its name, and is asked for by name.
        assert!(
            refs(Some(&repo), None).await.unwrap().0.is_empty(),
            "the host is part of it"
        );
        assert!(refs(Some("github.com/acme/no-such"), None)
            .await
            .unwrap()
            .0
            .is_empty());
        assert_eq!(refs(None, None).await.unwrap_err(), StatusCode::BAD_REQUEST);
        assert_eq!(
            refs(Some(""), None).await.unwrap_err(),
            StatusCode::BAD_REQUEST
        );

        // The branch's commit is run for again, by hand and for one
        // pipeline. The branch still has a run of each: the later of that
        // pipeline's two, and the push's run of the other.
        later().await;
        let (_, again) = submit(&state, &member, &build, on(main, None, "a2", "e-4"), None)
            .await
            .unwrap();
        let Json(listed) = refs(Some(&full), None).await.unwrap();
        assert_eq!(listed[0].git_ref, main, "now the ref with the latest run");
        assert_eq!(ids(&listed[0]), [again.clone(), main_test.clone()]);
        assert_eq!(
            (listed[0].sha.as_deref(), listed[0].event_id.as_deref()),
            (Some("a2"), Some("e-4"))
        );

        // It is among the repositories runs came from, with its runs counted.
        let Json(sources) = list_sources(
            caller(&[]),
            State(state.clone()),
            Query(SourcesParams { limit: Some(500) }),
        )
        .await
        .unwrap();
        let mine = sources
            .iter()
            .find(|s| s.repo == full)
            .expect("the repository is listed");
        assert_eq!(mine.runs, 5);
        assert!(
            sources.iter().all(|s| !s.repo.is_empty()),
            "and a run of no repository is under none"
        );

        // An event's runs are found by its id, within its repository.
        let params = ListParams {
            status: None,
            name: None,
            trigger: None,
            repo: Some(full.clone()),
            event: Some("e-2".to_string()),
            limit: None,
            offset: None,
        };
        let Json(rows) = list_runs(caller(&[]), State(state.clone()), Query(params))
            .await
            .unwrap();
        let mut of_event: Vec<String> = rows.into_iter().map(|r| r.id).collect();
        of_event.sort();
        let mut both = vec![main_build, main_test];
        both.sort();
        assert_eq!(of_event, both);
    }

    /// A caller outside the group cannot mark a run, and gets no run at all:
    /// a submit whose record is refused must not run unmarked behind its
    /// sender's back.
    #[tokio::test]
    async fn a_trigger_from_outside_the_group_makes_no_run() {
        let Some(state) = test_state("trigger-refused").await else {
            return;
        };
        let name = format!("ci-{}", uuid::Uuid::new_v4());
        // A key of this run's own: a database an earlier run of these tests
        // used still holds theirs.
        let key = format!("refused-{name}");
        for groups in [&[][..], &["admin"]] {
            let (code, why) = submit(&state, groups, &name, Some(trigger("e-1")), Some(&key)).await.unwrap_err();
            assert_eq!(code, StatusCode::FORBIDDEN, "{why}");
        }
        assert_eq!(runs(&state, &name, None).await, []);
        // And the key it sent was not spent on the refusal.
        let (code, _) = submit(&state, &["ci:trigger"], &name, Some(trigger("e-1")), Some(&key)).await.unwrap();
        assert_eq!(code, StatusCode::CREATED);
    }

    /// Two events for one commit share a key and differ in their event ids.
    /// The second is a replay of the first run, whose record stays the first
    /// event's.
    #[tokio::test]
    async fn a_replay_under_one_key_keeps_the_first_record() {
        let Some(state) = test_state("trigger-replay").await else {
            return;
        };
        let name = format!("ci-{}", uuid::Uuid::new_v4());
        let key = format!("commit-{name}");
        let (code, first) = submit(&state, &["ci:trigger"], &name, Some(trigger("webhook-1")), Some(&key)).await.unwrap();
        assert_eq!(code, StatusCode::CREATED);
        let (code, again) = submit(&state, &["ci:trigger"], &name, Some(trigger("poll-2")), Some(&key)).await.unwrap();
        assert_eq!((code, again.as_str()), (StatusCode::OK, first.as_str()), "a replay, not a conflict");
        let events: Vec<(Option<String>,)> = sqlx::query_as("SELECT event_id FROM run_triggers WHERE run_id = $1")
            .bind(&first)
            .fetch_all(&state.read_pool)
            .await
            .unwrap();
        assert_eq!(events, [(Some("webhook-1".to_string()),)]);
    }

    /// A stored workflow CI is bound to: its run is marked, and the workflow
    /// is not listed a second time as a pipeline.
    #[tokio::test]
    async fn a_bound_workflow_s_run_is_marked_and_listed_once() {
        let Some(state) = test_state("trigger-bound").await else {
            return;
        };
        let id = uuid::Uuid::new_v4().to_string();
        let name = format!("wf-{id}");
        sqlx::query("INSERT INTO workflows (id, name, spec, created_at, updated_at) VALUES ($1, $2, $3, 'now', 'now')")
            .bind(&id)
            .bind(&name)
            .bind(format!("name: {name}\ntasks:\n  - name: gate\n    type: approval\n"))
            .execute(&state.write_pool)
            .await
            .unwrap();
        let body: RunWorkflowBody = serde_json::from_value(serde_json::json!({ "trigger": trigger("e-9") })).unwrap();
        let (code, Json(answer)) = run_workflow(
            caller(&["ci:trigger"]),
            State(state.clone()),
            Path(id.clone()),
            axum::http::HeaderMap::new(),
            Some(Json(body)),
        )
        .await
        .unwrap();
        assert_eq!(code, StatusCode::CREATED);
        let run = answer["run_id"].as_str().unwrap().to_string();
        assert_eq!(runs(&state, &name, Some("ci")).await, [(run, "ci".to_string())]);
        let listed = workflows(&state, Some("ci"), None).await;
        let mine: Vec<&str> = listed.iter().filter(|w| w.name == name).map(|w| w.source.as_str()).collect();
        assert_eq!(mine, ["manual"], "the stored workflow, once");

        // The same body from outside the group is refused here too.
        let body: RunWorkflowBody = serde_json::from_value(serde_json::json!({ "trigger": trigger("e-9") })).unwrap();
        let refused = run_workflow(caller(&[]), State(state.clone()), Path(id), axum::http::HeaderMap::new(), Some(Json(body)))
            .await
            .unwrap_err();
        assert_eq!(refused.0, StatusCode::FORBIDDEN);
    }
}

/// Log appends against a real Postgres: the statement that writes a task's
/// chunk, which does not notify, and the one that tells of the runs a batch
/// of them was made to. Skipped without `TEST_DATABASE_URL`.
#[cfg(test)]
mod log_append_live_tests {
    use super::redrive_tests::test_state;
    use super::*;
    use std::time::Duration;

    /// A run of `tasks` tasks, each claimed as an engine claims one, by a
    /// statement of this test's own: no engine runs here, and `claim_ready`
    /// would take other tests' rows. Returns the run, and each task with its
    /// fence.
    ///
    /// A row a statement. The tests beside these sweep the whole table
    /// (`db::advance_ready_tasks`), these rows among it, and one statement
    /// that took all of a run's rows could take them in another order than
    /// the sweep did: Postgres ended one of the two as a deadlock, about one
    /// run of the suite in twenty. A statement that holds one row waits for
    /// nothing while it holds it.
    async fn running(state: &AppState, tasks: usize) -> (String, Vec<(String, i64)>) {
        let name = format!("append-{}", uuid::Uuid::new_v4().simple());
        let list: String = (0..tasks)
            .map(|n| format!("  - name: t{n}\n    command: [\"true\"]\n"))
            .collect();
        let yaml = format!("name: {name}\nrunner_class: {name}\ntasks:\n{list}");
        let run = control::submit_yaml_as(state, &yaml, &yaml, &BTreeMap::new(), None)
            .await
            .unwrap();
        let lease = (chrono::Utc::now() + chrono::TimeDelta::seconds(300)).to_rfc3339();
        let ids: Vec<String> =
            sqlx::query_scalar("SELECT id FROM task_runs WHERE run_id = $1 ORDER BY name")
                .bind(&run)
                .fetch_all(&state.write_pool)
                .await
                .unwrap();
        assert_eq!(ids.len(), tasks);
        let mut claimed = Vec::with_capacity(tasks);
        for id in ids {
            let fence: i64 = sqlx::query_scalar(
                "UPDATE task_runs
                 SET status = 'running', claimed_by = 'append-live', lease_expires_at = $1,
                     attempt = attempt + 1, version = version + 1
                 WHERE id = $2
                 RETURNING version",
            )
            .bind(&lease)
            .bind(&id)
            .fetch_one(&state.write_pool)
            .await
            .unwrap();
            claimed.push((id, fence));
        }
        (run, claimed)
    }

    async fn log_of(state: &AppState, task: &str) -> Option<String> {
        sqlx::query_scalar("SELECT log FROM task_runs WHERE id = $1")
            .bind(task)
            .fetch_one(&state.write_pool)
            .await
            .unwrap()
    }

    /// How many notifications name `run`, of those that arrive until none
    /// has for half a second. Other tests notify the channel too, of their
    /// own runs.
    async fn told_of(listener: &mut sqlx::postgres::PgListener, run: &str) -> usize {
        let mut told = 0;
        while let Ok(Ok(notification)) =
            tokio::time::timeout(Duration::from_millis(500), listener.recv()).await
        {
            if notification.payload() == run {
                told += 1;
            }
        }
        told
    }

    /// An append writes its chunk and says which run it was made to: the
    /// first of an attempt in place of what was there, the next after it.
    /// It tells nobody. One that reaches no row, an attempt that is over or
    /// a task that has ended, writes nothing and names no run. Telling is a
    /// statement of its own, one notification a run however many are named.
    #[tokio::test]
    async fn an_append_writes_and_names_its_run_and_telling_is_apart() {
        let Some(state) = test_state("append-live").await else {
            return;
        };
        let pool = &state.write_pool;
        let (run, tasks) = running(&state, 1).await;
        let (task, fence) = (tasks[0].0.as_str(), tasks[0].1);
        let mut listener = sqlx::postgres::PgListener::connect_with(pool)
            .await
            .unwrap();
        listener.listen("task_events").await.unwrap();
        let append = |fence: i64, text: &'static str, first: bool| async move {
            dagron_core::db::append_task_output(pool, task, fence, text, first)
                .await
                .unwrap()
        };

        assert_eq!(
            append(fence, "stale\n", true).await.as_deref(),
            Some(run.as_str())
        );
        assert_eq!(
            append(fence, "one\n", true).await.as_deref(),
            Some(run.as_str())
        );
        assert_eq!(
            append(fence, "two\n", false).await.as_deref(),
            Some(run.as_str())
        );
        assert_eq!(log_of(&state, task).await.as_deref(), Some("one\ntwo\n"));
        assert_eq!(
            told_of(&mut listener, &run).await,
            0,
            "an append tells nobody"
        );

        // An attempt that is over.
        assert_eq!(append(fence - 1, "late\n", false).await, None);
        // And a task that has ended.
        sqlx::query("UPDATE task_runs SET status = 'succeeded' WHERE id = $1")
            .bind(task)
            .execute(pool)
            .await
            .unwrap();
        // The end tells its run, by migrations_pg/107's trigger: once.
        assert_eq!(told_of(&mut listener, &run).await, 1, "a task's end");
        assert_eq!(append(fence, "after the end\n", false).await, None);
        assert_eq!(log_of(&state, task).await.as_deref(), Some("one\ntwo\n"));

        // Telling: each run once, and nothing for none.
        let other = format!("no-such-run-{}", uuid::Uuid::new_v4().simple());
        dagron_core::db::notify_runs(pool, &[run.clone(), other.clone()])
            .await
            .unwrap();
        dagron_core::db::notify_runs(pool, &[]).await.unwrap();
        let mut told = Vec::new();
        while let Ok(Ok(notification)) =
            tokio::time::timeout(Duration::from_millis(500), listener.recv()).await
        {
            if [run.as_str(), other.as_str()].contains(&notification.payload()) {
                told.push(notification.payload().to_string());
            }
        }
        told.sort();
        let mut wanted = vec![run, other];
        wanted.sort();
        assert_eq!(told, wanted);
    }

    /// One append of a batch, as the reconcile loop hands them over.
    fn one(task: &(String, i64), text: String, first: bool) -> dagron_core::db::LogAppend {
        dagron_core::db::LogAppend {
            task_id: task.0.clone(),
            fence: task.1,
            text,
            reset: first,
        }
    }

    /// A batch of appends, made as the reconcile loop makes them, each reach
    /// their own row: however many are in flight at once, and more asked for
    /// than the most there are is the most there are.
    #[tokio::test]
    async fn the_appends_of_a_batch_each_reach_their_own_row_whatever_the_lanes() {
        let Some(state) = test_state("append-live-lanes").await else {
            return;
        };
        let pool = &state.write_pool;
        let (_run, tasks) = running(&state, 9).await;
        let mut wanted = vec![String::new(); tasks.len()];
        for (round, lanes) in [1, 3, 4, 9].into_iter().enumerate() {
            let batch = tasks
                .iter()
                .enumerate()
                .map(|(n, task)| {
                    let text = format!("{n} with {lanes} lanes\n");
                    wanted[n].push_str(&text);
                    one(task, text, round == 0)
                })
                .collect();
            assert_eq!(
                dagron_core::db::append_task_outputs_in(pool, batch, lanes).await,
                9,
                "{lanes} lanes"
            );
        }
        for (task, wanted) in tasks.iter().zip(wanted) {
            assert_eq!(log_of(&state, &task.0).await, Some(wanted));
        }
        assert_eq!(
            dagron_core::db::append_task_outputs(pool, Vec::new()).await,
            0
        );
    }

    /// A batch tells of each run it appended to once, however many of the
    /// run's tasks it appended to and whichever lanes they fell in; and of
    /// no run where an append reached nothing. This is the call the
    /// reconcile loop makes, and what a browser's tail waits on: the appends
    /// themselves tell nobody.
    #[tokio::test]
    async fn a_batch_of_appends_tells_of_each_of_its_runs_once() {
        let Some(state) = test_state("append-live-told").await else {
            return;
        };
        let pool = &state.write_pool;
        let (many, many_tasks) = running(&state, 5).await;
        let (few, few_tasks) = running(&state, 2).await;
        let (stale, stale_tasks) = running(&state, 1).await;
        let mut listener = sqlx::postgres::PgListener::connect_with(pool)
            .await
            .unwrap();
        listener.listen("task_events").await.unwrap();

        let mut batch: Vec<_> = many_tasks
            .iter()
            .chain(&few_tasks)
            .map(|task| one(task, "a line\n".to_string(), true))
            .collect();
        // An attempt that is over: made, and it reaches no row.
        let over = (stale_tasks[0].0.clone(), stale_tasks[0].1 - 1);
        batch.push(one(&over, "late\n".to_string(), false));
        assert_eq!(dagron_core::db::append_task_outputs(pool, batch).await, 8);

        let mut told = Vec::new();
        while let Ok(Ok(notification)) =
            tokio::time::timeout(Duration::from_millis(500), listener.recv()).await
        {
            // Other tests notify the channel too, of their own runs.
            if [&many, &few, &stale].contains(&&notification.payload().to_string()) {
                told.push(notification.payload().to_string());
            }
        }
        told.sort();
        let mut wanted = vec![many, few];
        wanted.sort();
        assert_eq!(
            told, wanted,
            "each run once, and the run nothing reached not at all"
        );
        for task in many_tasks.iter().chain(&few_tasks) {
            assert_eq!(log_of(&state, &task.0).await.as_deref(), Some("a line\n"));
        }
        assert_eq!(log_of(&state, &stale_tasks[0].0).await, None);
    }

    /// As many appends at once as half the pool, four at most, and one where
    /// the pool is the smallest an engine runs with.
    #[tokio::test]
    async fn the_lanes_are_half_the_pool_and_four_at_most() {
        let lanes = |max: u32| {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(max)
                .connect_lazy("postgres://nobody@localhost/nothing")
                .unwrap();
            dagron_core::db::log_append_lanes(&pool)
        };
        assert_eq!([2, 3, 4, 6, 8, 16, 64].map(lanes), [1, 1, 2, 3, 4, 4, 4]);
    }

    /// The live relay's statements, against Postgres: a log's
    /// bytes, by the writing they name (its attempt and generation), from a
    /// byte; the closed segments summed in the statement that reads the row,
    /// so a close since is seen; a log begun again, in one transaction with
    /// its segments' rows going; and a log stays its attempt's when a
    /// teardown moves the row's version on.
    #[tokio::test]
    async fn the_relay_reads_the_bytes_of_the_writing_it_names() {
        let Some(state) = test_state("relay-live").await else {
            return;
        };
        let pool = &state.write_pool;
        let (run, tasks) = running(&state, 1).await;
        let (task, fence) = (tasks[0].0.as_str(), tasks[0].1);
        let first = "é€🦀\u{FFFD} one\n";
        let begun = dagron_core::db::reset_task_log(pool, task, fence, first)
            .await
            .unwrap()
            .expect("running");
        assert!(!begun.swept, "its first writing leaves nothing behind");
        dagron_core::db::append_task_output(pool, task, fence, "two\n", false)
            .await
            .unwrap();
        let whole = format!("{first}two\n");

        let tails = dagron_core::db::log_tails(pool, std::slice::from_ref(&run))
            .await
            .unwrap();
        assert_eq!(tails.len(), 1);
        let row = &tails[0];
        assert_eq!(
            (row.fence, row.gen, row.segments, row.closed),
            (fence, begun.gen, 0, 0)
        );
        assert_eq!(
            row.row_bytes,
            Some(whole.len() as i64),
            "bytes, not characters"
        );
        assert_eq!(
            dagron_core::db::log_tail_of(pool, task)
                .await
                .unwrap()
                .as_ref(),
            Some(row)
        );
        let read = |pos: i64, gen: i64| dagron_core::db::RowRead {
            task_id: task.to_string(),
            fence,
            gen,
            pos,
            base: 0,
            max: 1000,
        };
        let got = dagron_core::db::log_row_bytes(pool, &[read(3, begun.gen)])
            .await
            .unwrap();
        assert_eq!(
            got[0].bytes,
            whole.as_bytes()[3..],
            "from a byte, inside a character"
        );
        assert!(
            dagron_core::db::log_row_bytes(pool, &[read(0, begun.gen + 1)])
                .await
                .unwrap()
                .is_empty(),
            "another writing's bytes are not read"
        );

        // A segment closed: its bytes leave the row in the statement that
        // records it, and the read sees the close.
        dagron_core::db::close_log_segments(
            pool,
            task,
            fence,
            &[(0, "seg-0".to_string(), first.len() as i64, 1)],
            Some(first.chars().count() as i64),
        )
        .await
        .unwrap();
        let got = dagron_core::db::log_row_bytes(pool, &[read(first.len() as i64, begun.gen)])
            .await
            .unwrap();
        assert_eq!(
            (got[0].closed, got[0].bytes.as_slice()),
            (first.len() as i64, &b"two\n"[..])
        );
        assert_eq!(
            dagron_core::db::log_segment_spans(pool, task, fence)
                .await
                .unwrap(),
            [(0, first.len() as i64)]
        );

        // A teardown's claim moves the version on: the log is still its
        // attempt's, segments and all.
        sqlx::query("UPDATE task_runs SET version = version + 1 WHERE id = $1")
            .bind(task)
            .execute(pool)
            .await
            .unwrap();
        let row = dagron_core::db::log_tail_of(pool, task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((row.fence, row.segments), (fence, 1));
        sqlx::query("UPDATE task_runs SET version = version - 1 WHERE id = $1")
            .bind(task)
            .execute(pool)
            .await
            .unwrap();

        // Begun again: the next writing, the segments' rows gone with it, and
        // the writing before recorded for the log store's GC, due at once.
        let again = dagron_core::db::reset_task_log(pool, task, fence, "***\n")
            .await
            .unwrap()
            .expect("running");
        assert_eq!((again.gen, again.swept), (begun.gen + 1, true));
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM log_segments WHERE task_id = $1")
            .bind(task)
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(rows, 0);
        let (reason, due_now): (String, bool) = sqlx::query_as(
            "SELECT reason, due_at <= to_char(clock_timestamp() AT TIME ZONE 'UTC', \
             'YYYY-MM-DD\"T\"HH24:MI:SS.US\"+00:00\"') FROM log_sweeps WHERE task_id = $1",
        )
        .bind(task)
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!((reason.as_str(), due_now), ("superseded", true));
        // No engine sweeps here: the record is this test's to drop.
        sqlx::query("DELETE FROM log_sweeps WHERE task_id = $1")
            .bind(task)
            .execute(pool)
            .await
            .unwrap();
        let row = dagron_core::db::log_tail_of(pool, task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (row.gen, row.segments, row.row_bytes),
            (again.gen, 0, Some(4))
        );

        // A reset of an attempt that is over begins nothing.
        sqlx::query("UPDATE task_runs SET status = 'succeeded', output = 'the end' WHERE id = $1")
            .bind(task)
            .execute(pool)
            .await
            .unwrap();
        assert!(dagron_core::db::reset_task_log(pool, task, fence, "late\n")
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            dagron_core::db::log_output_bytes(pool, task, 4, 10)
                .await
                .unwrap()
                .as_deref(),
            Some(&b"end"[..])
        );
    }

    /// A record of the log store's GC: why, its
    /// stamp and tries, its last error, and its due and marked times.
    type Sweep = (String, i64, i64, Option<String>, String, String);

    async fn sweep_of(pool: &sqlx::PgPool, task: &str) -> Option<Sweep> {
        sqlx::query_as(
            "SELECT reason, stamp, tries, last_error, due_at, marked_at FROM log_sweeps \
             WHERE task_id = $1",
        )
        .bind(task)
        .fetch_optional(pool)
        .await
        .unwrap()
    }

    /// Seconds from `from` to `to`, two of a record's times.
    async fn secs_between(pool: &sqlx::PgPool, from: &str, to: &str) -> f64 {
        sqlx::query_scalar(
            "SELECT CAST(EXTRACT(EPOCH FROM (CAST($2 AS timestamptz) - CAST($1 AS timestamptz))) \
             AS DOUBLE PRECISION)",
        )
        .bind(from)
        .bind(to)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// The database's clock as a record's times are written.
    async fn db_now(pool: &sqlx::PgPool) -> String {
        sqlx::query_scalar(
            "SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', \
             'YYYY-MM-DD\"T\"HH24:MI:SS.US\"+00:00\"')",
        )
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// No engine sweeps here: a test drops its own records, a row a statement.
    async fn drop_sweeps(pool: &sqlx::PgPool, tasks: &[&str]) {
        for task in tasks {
            sqlx::query("DELETE FROM log_sweeps WHERE task_id = $1")
                .bind(task)
                .execute(pool)
                .await
                .unwrap();
        }
    }

    /// The trigger on `task_runs` records a deleted task whose log the store
    /// holds, as an archive's purge deletes it or a hand does, due a minute
    /// on in the outbox trigger's form; and no task that wrote nothing there.
    #[tokio::test]
    async fn the_trigger_records_a_deleted_task_that_wrote_and_no_other() {
        let Some(state) = test_state("log-sweep-trigger").await else {
            return;
        };
        let pool = &state.write_pool;
        let (run, tasks) = running(&state, 2).await;
        let (wrote, quiet) = (tasks[0].0.as_str(), tasks[1].0.as_str());
        dagron_core::db::reset_task_log(pool, wrote, tasks[0].1, "a line\n")
            .await
            .unwrap()
            .expect("running");
        sqlx::query("UPDATE workflow_runs SET status = 'succeeded' WHERE id = $1")
            .bind(&run)
            .execute(pool)
            .await
            .unwrap();
        assert_eq!(
            dagron_core::db::purge_runs_by_id(pool, std::slice::from_ref(&run))
                .await
                .unwrap(),
            1
        );
        let (reason, stamp, tries, error, due, marked) =
            sweep_of(pool, wrote).await.expect("recorded");
        assert_eq!((reason.as_str(), stamp, tries, error), ("gone", 1, 0, None));
        assert_eq!(sweep_of(pool, quiet).await, None, "it wrote nothing there");
        // `2026-10-22T10:00:00.123456+00:00`, the outbox trigger's form.
        let form = |t: &str| {
            t.len() == 32
                && t.as_bytes()[10] == b'T'
                && t.as_bytes()[19] == b'.'
                && t.ends_with("+00:00")
        };
        assert!(form(&due) && form(&marked), "{due} {marked}");
        let after = secs_between(pool, &marked, &due).await;
        assert!((59.9..=60.1).contains(&after), "due a minute on: {after}");

        // A DELETE by hand is recorded too.
        let (_, other) = running(&state, 1).await;
        let by_hand = other[0].0.as_str();
        dagron_core::db::reset_task_log(pool, by_hand, other[0].1, "")
            .await
            .unwrap()
            .expect("running");
        sqlx::query("DELETE FROM task_runs WHERE id = $1")
            .bind(by_hand)
            .execute(pool)
            .await
            .unwrap();
        assert_eq!(sweep_of(pool, by_hand).await.unwrap().0, "gone");
        drop_sweeps(pool, &[wrote, by_hand]).await;
    }

    /// A task deleted while it is recorded already is recorded as gone, its
    /// stamp moved on and its failures forgotten: one waiting out a failure
    /// is due a minute on, one due now stays due now, and a finish at the
    /// stamp before changes nothing.
    #[tokio::test]
    async fn a_task_deleted_while_recorded_is_recorded_gone() {
        let Some(state) = test_state("log-sweep-conflict").await else {
            return;
        };
        let pool = &state.write_pool;
        let (_run, tasks) = running(&state, 2).await;
        let ids: Vec<&str> = tasks.iter().map(|(id, _)| id.as_str()).collect();
        for (task, fence) in &tasks {
            for text in ["a line\n", "a line, sealed\n"] {
                dagron_core::db::reset_task_log(pool, task, *fence, text)
                    .await
                    .unwrap()
                    .expect("running");
            }
        }
        let (waiting, due_now) = (ids[0], ids[1]);
        assert_eq!(sweep_of(pool, waiting).await.unwrap().0, "superseded");
        sqlx::query(
            "UPDATE log_sweeps SET tries = 3, last_error = 'refused', stamp = stamp + 1, \
             due_at = to_char((clock_timestamp() + interval '1 hour') AT TIME ZONE 'UTC', \
             'YYYY-MM-DD\"T\"HH24:MI:SS.US\"+00:00\"') WHERE task_id = $1",
        )
        .bind(waiting)
        .execute(pool)
        .await
        .unwrap();
        let (_, before, ..) = sweep_of(pool, waiting).await.unwrap();
        let (_, _, _, _, was_due, _) = sweep_of(pool, due_now).await.unwrap();
        for task in [waiting, due_now] {
            sqlx::query("DELETE FROM task_runs WHERE id = $1")
                .bind(task)
                .execute(pool)
                .await
                .unwrap();
        }
        let (reason, stamp, tries, error, due, marked) = sweep_of(pool, waiting).await.unwrap();
        assert_eq!((reason.as_str(), tries, error), ("gone", 0, None));
        assert_eq!(stamp, before + 1);
        let after = secs_between(pool, &marked, &due).await;
        assert!(
            (59.9..=60.1).contains(&after),
            "a minute on, not an hour: {after}"
        );
        dagron_core::db::finish_log_sweeps(pool, &[(waiting.to_string(), before)], &[], 900)
            .await
            .unwrap();
        assert_eq!(
            sweep_of(pool, waiting).await.unwrap().1,
            stamp,
            "still recorded"
        );
        let (reason, _, _, _, due, _) = sweep_of(pool, due_now).await.unwrap();
        assert_eq!(
            (reason.as_str(), due),
            ("gone", was_due),
            "still due as it was"
        );
        drop_sweeps(pool, &ids).await;
    }

    #[tokio::test]
    async fn a_rolled_back_delete_leaves_no_record() {
        let Some(state) = test_state("log-sweep-rollback").await else {
            return;
        };
        let pool = &state.write_pool;
        let (_run, tasks) = running(&state, 1).await;
        let task = tasks[0].0.as_str();
        dagron_core::db::reset_task_log(pool, task, tasks[0].1, "")
            .await
            .unwrap()
            .expect("running");
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("DELETE FROM task_runs WHERE id = $1")
            .bind(task)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        assert_eq!(sweep_of(pool, task).await, None);
    }

    /// A reset takes the rows of every attempt's closed segments, its own and
    /// those before it, and records the task where a writing was begun in
    /// the store before it; a task's first does not.
    #[tokio::test]
    async fn a_reset_takes_every_earlier_attempt_s_rows_and_records_the_writing_before() {
        let Some(state) = test_state("log-sweep-reset").await else {
            return;
        };
        let pool = &state.write_pool;
        let (_run, tasks) = running(&state, 2).await;
        let (task, fence) = (tasks[0].0.as_str(), tasks[0].1);
        let first = dagron_core::db::reset_task_log(pool, task, fence, "")
            .await
            .unwrap()
            .expect("running");
        assert!(!first.swept);
        assert_eq!(sweep_of(pool, task).await, None);
        let next = |v: i64| async move {
            // The next attempt's claim, as a retry's.
            sqlx::query(
                "UPDATE task_runs SET version = $2, log = NULL, log_fence = NULL, log_gen = NULL \
                 WHERE id = $1",
            )
            .bind(task)
            .bind(v)
            .execute(pool)
            .await
            .unwrap();
        };
        let close = |at: i64| async move {
            dagron_core::db::close_log_segments(
                pool,
                task,
                at,
                &[(0, format!("{task}/{at}.1/000000.seg.zst"), 10, 1)],
                None,
            )
            .await
            .unwrap();
        };
        close(fence).await;
        next(fence + 1).await;
        close(fence + 1).await;
        next(fence + 2).await;
        let again = dagron_core::db::reset_task_log(pool, task, fence + 2, "")
            .await
            .unwrap()
            .expect("running");
        assert!(again.swept);
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM log_segments WHERE task_id = $1")
            .bind(task)
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(rows, 0, "every attempt's rows went with the reset");
        let (reason, _, _, _, due, _) = sweep_of(pool, task).await.expect("recorded");
        assert_eq!(reason, "superseded");
        assert!(due <= db_now(pool).await, "due at once: {due}");

        // A fresh task's first is no writing before.
        let fresh = dagron_core::db::reset_task_log(pool, &tasks[1].0, tasks[1].1, "")
            .await
            .unwrap()
            .expect("running");
        assert!(!fresh.swept);
        assert_eq!(sweep_of(pool, &tasks[1].0).await, None);
        drop_sweeps(pool, &[task]).await;
    }

    /// Two claims at once take records apart (`SKIP LOCKED`), and a finish
    /// or a deferral at a stamp that moved on since changes nothing. This
    /// test's own records alone are due in 1970, so they are the ones taken;
    /// no other test here claims.
    #[tokio::test]
    async fn two_claimers_take_disjoint_records_and_only_the_last_holder_finishes() {
        let Some(state) = test_state("log-sweep-claims").await else {
            return;
        };
        let pool = &state.write_pool;
        // What an earlier run of this test left, were it stopped half way.
        sqlx::query("DELETE FROM log_sweeps WHERE task_id LIKE 'claims-%'")
            .execute(pool)
            .await
            .unwrap();
        let ids: Vec<String> = (0..2)
            .map(|_| format!("claims-{}", uuid::Uuid::new_v4().simple()))
            .collect();
        for (n, id) in ids.iter().enumerate() {
            sqlx::query(
                "INSERT INTO log_sweeps (task_id, reason, marked_at, due_at) \
                 VALUES ($1, 'gone', $2, $2)",
            )
            .bind(id)
            .bind(format!("1970-01-01T00:00:00.00000{n}+00:00"))
            .execute(pool)
            .await
            .unwrap();
        }
        let claim =
            || dagron_core::db::claim_log_sweeps(pool, 1, 600, dagron_core::db::ClaimFirst::Any);
        let (a, b) = tokio::join!(claim(), claim());
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!((a.len(), b.len()), (1, 1));
        let mut took = vec![a[0].task_id.clone(), b[0].task_id.clone()];
        took.sort();
        let mut ours = ids.clone();
        ours.sort();
        assert_eq!(took, ours, "each its own record");
        assert_eq!(a[0].stamp, 2, "the claim moved the stamp on");

        let (id, stamp) = (a[0].task_id.clone(), a[0].stamp);
        let stale = [(id.clone(), stamp - 1)];
        dagron_core::db::finish_log_sweeps(pool, &stale, &[], 900)
            .await
            .unwrap();
        dagron_core::db::finish_log_sweeps(pool, &[], &stale, 900)
            .await
            .unwrap();
        assert!(
            !dagron_core::db::defer_log_sweep(pool, &id, stamp - 1, 60, "late")
                .await
                .unwrap()
        );
        assert_eq!(
            sweep_of(pool, &id).await.unwrap().1,
            stamp,
            "nothing changed"
        );
        dagron_core::db::finish_log_sweeps(pool, &[(id.clone(), stamp)], &[], 900)
            .await
            .unwrap();
        assert_eq!(sweep_of(pool, &id).await, None, "the holder's finish");
        drop_sweeps(pool, &[&ids[0], &ids[1]]).await;
    }

    /// The sweeper's statements, against Postgres: the writing each row is
    /// on and none for a row that is gone, a teardown's claim moving nothing;
    /// and a deferral and a finish by a record's stamp.
    #[tokio::test]
    async fn the_sweep_statements_on_postgres() {
        let Some(state) = test_state("log-sweep-statements").await else {
            return;
        };
        let pool = &state.write_pool;
        let (_run, tasks) = running(&state, 1).await;
        let (task, fence) = (tasks[0].0.as_str(), tasks[0].1);
        let begun = dagron_core::db::reset_task_log(pool, task, fence, "")
            .await
            .unwrap()
            .expect("running");
        let gone = format!("gone-{}", uuid::Uuid::new_v4().simple());
        let asked = [task.to_string(), gone.clone()];
        let writings = dagron_core::db::log_writings_of(pool, &asked)
            .await
            .unwrap();
        assert_eq!(writings, [(task.to_string(), fence, begun.gen)]);
        // A teardown's claim moves the version and keeps the log.
        sqlx::query("UPDATE task_runs SET version = version + 1 WHERE id = $1")
            .bind(task)
            .execute(pool)
            .await
            .unwrap();
        let writings = dagron_core::db::log_writings_of(pool, &asked)
            .await
            .unwrap();
        assert_eq!(writings, [(task.to_string(), fence, begun.gen)]);

        let id = format!("statements-{}", uuid::Uuid::new_v4().simple());
        let now = db_now(pool).await;
        sqlx::query(
            "INSERT INTO log_sweeps (task_id, reason, marked_at, due_at) VALUES ($1, 'orphan', $2, $2)",
        )
        .bind(&id)
        .bind(&now)
        .execute(pool)
        .await
        .unwrap();
        assert!(
            dagron_core::db::defer_log_sweep(pool, &id, 1, 60, "refused")
                .await
                .unwrap()
        );
        assert!(!dagron_core::db::defer_log_sweep(pool, &id, 1, 60, "again")
            .await
            .unwrap());
        let (_, stamp, tries, error, due, _) = sweep_of(pool, &id).await.unwrap();
        assert_eq!((stamp, tries, error.as_deref()), (2, 1, Some("refused")));
        let wait = secs_between(pool, &now, &due).await;
        assert!((59.0..=70.0).contains(&wait), "a minute on: {wait}");
        dagron_core::db::finish_log_sweeps(pool, &[], &[(id.clone(), 2)], 900)
            .await
            .unwrap();
        let (_, stamp, tries, error, due, _) = sweep_of(pool, &id).await.unwrap();
        assert_eq!((stamp, tries, error), (3, 0, None));
        let wait = secs_between(pool, &now, &due).await;
        assert!((899.0..=910.0).contains(&wait), "looked at again: {wait}");
        dagron_core::db::finish_log_sweeps(pool, &[(id.clone(), 3)], &[], 900)
            .await
            .unwrap();
        assert_eq!(sweep_of(pool, &id).await, None);
    }

    /// `/metrics`' gauges of the log store's GC count this test's records
    /// among the rest.
    #[tokio::test]
    async fn status_counts_reports_the_sweeps() {
        let Some(state) = test_state("log-sweep-gauges").await else {
            return;
        };
        let pool = &state.write_pool;
        let ids: Vec<String> = (0..2)
            .map(|_| format!("gauges-{}", uuid::Uuid::new_v4().simple()))
            .collect();
        for (n, id) in ids.iter().enumerate() {
            sqlx::query(
                "INSERT INTO log_sweeps (task_id, reason, marked_at, due_at, tries) \
                 VALUES ($1, 'gone', '2020-01-01T00:00:00.000000+00:00', '2099-01-01T00:00:00.000000+00:00', $2)",
            )
            .bind(id)
            .bind(n as i64)
            .execute(pool)
            .await
            .unwrap();
        }
        let snap = dagron_core::db::status_counts(pool).await.unwrap();
        assert!(snap.log_sweeps_waiting >= 2, "{}", snap.log_sweeps_waiting);
        assert!(snap.log_sweeps_failing >= 1, "{}", snap.log_sweeps_failing);
        let oldest = snap.log_sweeps_oldest_marked_at.expect("the oldest");
        assert!(
            oldest.as_str() <= "2020-01-01T00:00:00.000000+00:00",
            "{oldest}"
        );
        drop_sweeps(pool, &[&ids[0], &ids[1]]).await;
    }

    /// A stream on Postgres, of a log whose open part is its row: woken by
    /// the run's notification (the relay's timer is far off), it sends each
    /// append as it is made, then the task's end and `eof`.
    #[tokio::test]
    async fn a_stream_is_woken_by_the_run_s_notification() {
        use futures::StreamExt;
        let Some(mut state) = test_state("relay-notify").await else {
            return;
        };
        let dir = std::env::temp_dir().join(format!(
            "dagron-api-relay-{}",
            uuid::Uuid::new_v4().simple()
        ));
        state.log_store = Some(std::sync::Arc::new(
            dagron_artifact::log_segments::SegmentStore::DirOpenInRow(dir.clone()),
        ));
        state.relay = std::sync::Arc::new(crate::relay::Relay::new(crate::relay::Settings {
            poll: Duration::from_secs(30),
            min_gap: Duration::from_millis(5),
            ..Default::default()
        }));
        crate::stream::spawn_listener(
            state.read_pool.clone(),
            state.tx.clone(),
            state.listener_ready.clone(),
        );
        for _ in 0..100 {
            if state
                .listener_ready
                .load(std::sync::atomic::Ordering::Acquire)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let pool = state.write_pool.clone();
        let (run, tasks) = running(&state, 1).await;
        let (task, fence) = (tasks[0].0.clone(), tasks[0].1);
        let begun = dagron_core::db::reset_task_log(&pool, &task, fence, "first\n")
            .await
            .unwrap()
            .unwrap();
        dagron_core::db::notify_runs(&pool, std::slice::from_ref(&begun.run_id))
            .await
            .unwrap();

        let resp = crate::routes::logstream::run_log_stream(
            crate::auth::AuthUser(crate::auth::SessionClaims {
                sub: "sub-ops".to_string(),
                email: "ops@example.com".to_string(),
                name: String::new(),
                groups: vec![],
                exp: 0,
            }),
            axum::extract::State(state.clone()),
            axum::extract::Path(run.clone()),
            axum::extract::RawQuery(Some("from=start".to_string())),
        )
        .await;
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
        let mut body = resp.into_body().into_data_stream();
        let (mut seen, mut text) = (String::new(), String::new());
        /// What the stream sends, until `want` is in it.
        async fn read_until(
            body: &mut axum::body::BodyDataStream,
            seen: &mut String,
            text: &mut String,
            want: &str,
        ) {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            while !seen.contains(want) {
                let chunk = tokio::time::timeout_at(deadline, body.next())
                    .await
                    .unwrap_or_else(|_| {
                        panic!("no {want:?} within 5 s, the relay's timer 30 s off: {seen}")
                    })
                    .expect("the stream went on")
                    .unwrap();
                let chunk = String::from_utf8_lossy(&chunk).into_owned();
                for line in chunk.lines() {
                    if let Some(data) = line.strip_prefix("data: ") {
                        let v: serde_json::Value = serde_json::from_str(data).unwrap();
                        if let Some(t) = v["text"].as_str() {
                            text.push_str(t);
                        }
                    }
                }
                seen.push_str(&chunk);
            }
        }
        read_until(&mut body, &mut seen, &mut text, "first\\n").await;
        dagron_core::db::append_task_outputs(
            &pool,
            vec![dagron_core::db::LogAppend {
                task_id: task.clone(),
                fence,
                text: "second\n".to_string(),
                reset: false,
            }],
        )
        .await;
        read_until(&mut body, &mut seen, &mut text, "second\\n").await;
        sqlx::query("UPDATE task_runs SET status = 'succeeded' WHERE id = $1")
            .bind(&task)
            .execute(&pool)
            .await
            .unwrap();
        dagron_core::db::notify_runs(&pool, std::slice::from_ref(&run))
            .await
            .unwrap();
        read_until(&mut body, &mut seen, &mut text, "event: eof").await;
        assert_eq!(text, "first\nsecond\n");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// The reconcile loop's waker against a real Postgres: what `LISTEN/NOTIFY`
/// hands it. Skipped without `TEST_DATABASE_URL`.
#[cfg(test)]
mod waker_live_tests {
    use super::redrive_tests::test_state;
    use std::time::Duration;

    /// A burst of notifications is taken in a few waits, not in one wait each.
    ///
    /// The waker's caller ticks once a wait, and a tick looks at everything,
    /// so one wait each is a tick each: five hundred lines of log were five
    /// hundred passes of the reconcile loop.
    ///
    /// The other live tests notify the same channel while this runs. What
    /// they send can only add to the count taken, so the bound is on the
    /// waits it took to take what was sent here.
    #[tokio::test]
    async fn a_burst_of_notifications_is_taken_in_a_few_waits() {
        let Some(state) = test_state("waker-live").await else {
            return;
        };
        let mut waker = dagron_core::db::Waker::connect(&state.write_pool)
            .await
            .unwrap();

        const SENT: usize = 500;
        const WAITS: usize = 50;
        // One statement, so they are delivered together. Each has a payload
        // of its own: Postgres folds equal ones in a transaction into one.
        sqlx::query("SELECT pg_notify('task_events', 'waker-live-' || n::text) FROM generate_series(1, $1) AS n")
            .bind(SENT as i32)
            .execute(&state.write_pool)
            .await
            .unwrap();

        let (mut taken, mut waits) = (0, 0);
        while taken < SENT && waits < WAITS {
            taken += waker.wait(Duration::from_secs(5)).await.unwrap();
            waits += 1;
        }
        assert!(
            taken >= SENT,
            "{taken} of {SENT} notifications taken in {waits} waits"
        );
    }

    /// A notification the loop took to learn which run changed is still a
    /// reason to tick: the wait that follows returns at once.
    ///
    /// The loop asks which runs changed partway through a tick, and that
    /// takes what is waiting off the connection. A run made while the tick
    /// was going is among it, and its first task is the next tick's to
    /// start. Spent there, its notification left the wait to sleep out the
    /// poll interval, and the run waited half a second for no reason: the
    /// whole-stack rig found it, as an approval that was not yet waiting.
    #[tokio::test]
    async fn a_notification_taken_for_its_run_s_name_still_ends_the_wait() {
        use dagron_core::db::{self, Woken};

        let Some(state) = test_state("wake-owed-live").await else {
            return;
        };
        let mut waker = db::Waker::connect(&state.write_pool).await.unwrap();
        assert_eq!(waker.take_woken().await, Woken::All);

        let mine = format!("wake-owed-{}", uuid::Uuid::new_v4());
        sqlx::query("SELECT pg_notify('task_events', $1)")
            .bind(&mine)
            .execute(&state.write_pool)
            .await
            .unwrap();
        // Taken as a tick takes it: by asking which runs changed, with no
        // wait between. It arrives a moment after it is sent.
        let mut named = false;
        for _ in 0..500 {
            match waker.take_woken().await {
                Woken::Runs(runs) => named |= runs.contains(&mine),
                Woken::All => panic!("the waker lost count of what it heard"),
            }
            if named {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(named, "the notification was never taken");

        // The wait that follows does not sleep over it.
        let began = std::time::Instant::now();
        let taken = waker.wait(Duration::from_secs(30)).await.unwrap();
        assert!(
            taken >= 1,
            "the notification taken for its name was owed a tick"
        );
        assert!(
            began.elapsed() < Duration::from_secs(5),
            "the wait slept {:?} over a notification already taken",
            began.elapsed()
        );
    }

    /// A run with one task, and that task as it is told to be. Its id.
    async fn run_whose_task_is(state: &crate::state::AppState, status: &str) -> String {
        let yaml = format!(
            "name: wake-{}\ntasks:\n  - name: only\n    command: [\"true\"]\n",
            uuid::Uuid::new_v4()
        );
        let run = crate::routes::control::submit_yaml_as(
            state,
            &yaml,
            &yaml,
            &std::collections::BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
        sqlx::query("UPDATE task_runs SET status = $2 WHERE run_id = $1")
            .bind(&run)
            .bind(status)
            .execute(&state.write_pool)
            .await
            .unwrap();
        run
    }

    async fn status_of(state: &crate::state::AppState, run: &str) -> String {
        sqlx::query_scalar("SELECT status FROM workflow_runs WHERE id = $1")
            .bind(run)
            .fetch_one(&state.write_pool)
            .await
            .unwrap()
    }

    /// A wake names its run, and a reap that is told which runs changed ends
    /// those that are over and looks at no other.
    ///
    /// The reap was a join of every running run to all its tasks, on every
    /// tick. Here two runs are over and one is named: the other is left
    /// running, which a reap that looked at everything could not do. It is
    /// the slow sweep's to find, a slice at a time.
    ///
    /// No other live test reaps, so a run this one leaves running stays so.
    /// Every reap here is of runs named or of a slice placed on one run:
    /// this test ends no run of another's.
    #[tokio::test]
    async fn a_wake_names_its_run_and_the_reap_looks_at_the_runs_it_is_told_of() {
        use dagron_core::db::{self, Woken};
        use dagron_core::models::RunStatus;

        let Some(state) = test_state("wake-names-live").await else {
            return;
        };
        let pool = &state.write_pool;
        let mut waker = db::Waker::connect(pool).await.unwrap();
        // What was sent before it listened was not heard: it cannot say.
        assert_eq!(waker.take_woken().await, Woken::All);

        let named = run_whose_task_is(&state, "succeeded").await;
        let unnamed = run_whose_task_is(&state, "succeeded").await;
        let failed = run_whose_task_is(&state, "failed").await;
        let unfinished = run_whose_task_is(&state, "running").await;

        // A notification is the run a task changed state in, and is kept.
        // (A run's creation notifies too, so every run here may be heard of:
        // what is looked at below is which runs the reap is told of.)
        for run in [&named, &failed, &unfinished] {
            sqlx::query("SELECT pg_notify('task_events', $1)")
                .bind(run)
                .execute(pool)
                .await
                .unwrap();
        }
        let mut heard = std::collections::HashSet::new();
        for _ in 0..50 {
            waker.wait(Duration::from_millis(200)).await.unwrap();
            match waker.take_woken().await {
                Woken::Runs(runs) => heard.extend(runs),
                Woken::All => panic!("the waker lost count of what it heard"),
            }
            if [&named, &failed, &unfinished]
                .iter()
                .all(|run| heard.contains(*run))
            {
                break;
            }
        }
        assert!(heard.contains(&named) && heard.contains(&failed) && heard.contains(&unfinished));
        // Taken, they are not told of again.
        match waker.take_woken().await {
            Woken::Runs(again) => assert!(!again.contains(&named)),
            Woken::All => panic!("the waker lost count of what it heard"),
        }

        // The reap of the runs named: each by what its tasks came to, the one
        // with a task still going left, and a run that was not named not
        // looked at.
        let told = Woken::Runs(vec![named.clone(), failed.clone(), unfinished.clone()]);
        let mut ended = db::reap_woken(pool, &told).await.unwrap();
        ended.sort_by(|a, b| a.0.cmp(&b.0));
        let mut expected = vec![
            (named.clone(), RunStatus::Succeeded),
            (failed.clone(), RunStatus::Failed),
        ];
        expected.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(ended, expected);
        assert_eq!(status_of(&state, &named).await, "succeeded");
        assert_eq!(status_of(&state, &failed).await, "failed");
        assert_eq!(status_of(&state, &unfinished).await, "running");
        assert_eq!(
            status_of(&state, &unnamed).await,
            "running",
            "it was not named"
        );
        let events: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM event_outbox WHERE run_id = $1 AND event_type = 'run.completed'",
        )
        .bind(&named)
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(events, 1, "ended with its event, as any reap ends a run");
        // Told again, there is nothing to end; told of nothing, nothing is asked.
        assert!(db::reap_woken(pool, &told).await.unwrap().is_empty());
        assert!(db::reap_woken(pool, &Woken::Runs(Vec::new()))
            .await
            .unwrap()
            .is_empty());

        // The slow sweep: a slice that starts just before the run nobody
        // named finds it, and says where the next slice starts.
        let mut before = unnamed.clone().into_bytes();
        let last = before.len() - 1;
        before[last] -= 1;
        let before = String::from_utf8(before).unwrap();
        let (swept, next) = db::reap_slice(pool, Some(&before), 1).await.unwrap();
        assert_eq!(swept, vec![(unnamed.clone(), RunStatus::Succeeded)]);
        assert_eq!(
            next.as_deref(),
            Some(unnamed.as_str()),
            "a full slice: more may follow"
        );
        assert_eq!(status_of(&state, &unnamed).await, "succeeded");
        // Past the last run there is nothing, and the next slice begins again.
        assert_eq!(
            db::reap_slice(pool, Some("~"), 10).await.unwrap(),
            (Vec::new(), None)
        );
    }
}

#[cfg(test)]
pub(crate) mod redrive_tests {
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
    pub(crate) async fn test_state(app: &str) -> Option<AppState> {
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
                let migrated = dagron_core::db::init_pool(&url).await.unwrap();
                // Then the tables dagron-api makes as it starts (`identity_links`
                // and the audit trail among them), so that a test does not depend
                // on another having made them first.
                crate::ensure_schemas(&migrated).await.unwrap();
                migrated.close().await;
            })
            .await;
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .connect_with(PgConnectOptions::from_str(&url).unwrap().application_name(app))
            .await
            .unwrap();
        Some(AppState {
            token_cache: Default::default(),
            read_pool: pool.clone(),
            write_pool: pool.clone(),
            tx: tokio::sync::broadcast::channel(16).0,
            jwt_secret: "test".to_string(),
            cookie_secure: false,
            identity: Arc::new(crate::identity::LocalIdentityProvider::new(pool)),
            artifact_store: None,
            log_store: None,
            artifacts: crate::routes::artifact_index::Settings::default(),
            rotation_lock: Arc::new(tokio::sync::Mutex::new(())),
            login_limiter: Arc::new(crate::ratelimit::RateLimiter::from_env()),
            listener_ready: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            oidc: None,
            relay: Arc::new(crate::relay::Relay::new(Default::default())),
            metrics_cache: Arc::new(dagron_core::gauges::GaugeCache::new(
                METRICS_CACHE_TTL,
                METRICS_HISTORY_CACHE_TTL,
            )),
            db_connections: Default::default(),
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

/// An environment's policy, on a real database: who writes one, what the view
/// shows, and a submit that names an environment its run may not use. Skipped
/// without `TEST_DATABASE_URL`.
#[cfg(test)]
mod environment_policy_live_tests {
    use super::redrive_tests::test_state;
    use super::*;
    use crate::auth::SessionClaims;
    use crate::routes::environments::{self, EnvironmentView, UpsertBody};
    use dagron_core::envpolicy::EnvironmentPolicy;
    use dagron_core::models::{trigger_kind, RunTrigger};

    fn user(groups: &[&str]) -> AuthUser {
        AuthUser(SessionClaims {
            sub: "sub-pol".to_string(),
            email: "pol@example.com".to_string(),
            name: String::new(),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            exp: 0,
        })
    }

    /// A fresh environment with one variable, under a name of this test's own.
    async fn environment(state: &AppState, name: &str) -> EnvironmentView {
        let body = UpsertBody {
            name: Some(name.to_string()),
            description: None,
            variables: Some([("REGION".to_string(), "eu".to_string())].into()),
        };
        let (status, Json(view)) =
            environments::create(user(&[]), State(state.clone()), Json(body))
                .await
                .expect("the environment is made");
        assert_eq!(status, StatusCode::CREATED);
        view
    }

    fn policy(
        trust: &[&str],
        refs: Option<&[&str]>,
        pipelines: Option<&[&str]>,
    ) -> EnvironmentPolicy {
        let list = |l: &[&str]| l.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        EnvironmentPolicy {
            allowed_trust: list(trust),
            allowed_refs: refs.map(list),
            allowed_pipelines: pipelines.map(list),
            allowed_identity: Vec::new(),
            approvers: Vec::new(),
        }
    }

    async fn set(
        state: &AppState,
        as_user: AuthUser,
        id: &str,
        p: EnvironmentPolicy,
    ) -> Result<EnvironmentView, (StatusCode, String)> {
        environments::put_policy(as_user, State(state.clone()), Path(id.to_string()), Json(p))
            .await
            .map(|Json(view)| view)
    }

    /// A CI service's record of a run: its class, and the ref it was for.
    fn ci(trust: &str, git_ref: &str) -> RunTrigger {
        RunTrigger {
            kind: trigger_kind::CI.to_string(),
            principal: "ci:svc@example.com".to_string(),
            trust: Some(trust.to_string()),
            repo: Some("acme/widgets".to_string()),
            git_ref: Some(git_ref.to_string()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn only_an_admin_writes_a_policy_and_the_view_shows_it() {
        let Some(state) = test_state("env-policy-write").await else {
            return;
        };
        let name = format!("pol-{}", uuid::Uuid::new_v4().simple());
        let env = environment(&state, &name).await;
        assert!(env.policy.is_none(), "a new environment has no policy");
        let main_only = policy(&["trusted"], Some(&["refs/heads/main"]), None);

        // Someone who may edit the environment may not say who uses it.
        let (status, why) = set(&state, user(&[]), &env.id, main_only.clone())
            .await
            .unwrap_err();
        assert_eq!(status, StatusCode::FORBIDDEN, "{why}");
        assert!(why.contains("an admin's to write"), "{why}");
        // What an admin writes is checked.
        for bad in [
            policy(&["everyone"], None, None),
            policy(&["trusted"], Some(&[]), None),
        ] {
            let (status, why) = set(&state, user(&["admin"]), &env.id, bad)
                .await
                .unwrap_err();
            assert_eq!(status, StatusCode::BAD_REQUEST, "{why}");
        }
        let view = set(&state, user(&["admin"]), &env.id, main_only.clone())
            .await
            .unwrap();
        assert_eq!(view.policy.as_ref(), Some(&main_only));
        // The whole policy is replaced, not merged.
        let closed = policy(&[], None, None);
        let view = set(&state, user(&["admin"]), &env.id, closed.clone())
            .await
            .unwrap();
        assert_eq!(view.policy.as_ref(), Some(&closed));
        // What the engine reads is what was written.
        let read = dagron_core::db::environment_policy(&state.read_pool, &name)
            .await
            .unwrap();
        assert_eq!(read.as_ref(), Some(&closed));
        // An agent's identity, and a protected environment's
        // approvers, written and read the same way.
        let agents = EnvironmentPolicy {
            allowed_identity: vec!["agent".into()],
            approvers: vec!["group:release".into()],
            ..policy(&["trusted", "agent"], None, None)
        };
        let view = set(&state, user(&["admin"]), &env.id, agents.clone())
            .await
            .unwrap();
        assert_eq!(view.policy.as_ref(), Some(&agents));
        let read = dagron_core::db::environment_policy(&state.read_pool, &name)
            .await
            .unwrap();
        assert_eq!(read.as_ref(), Some(&agents));
        // An identity for a class the policy does not admit is refused.
        let (status, why) = set(
            &state,
            user(&["admin"]),
            &env.id,
            EnvironmentPolicy {
                allowed_identity: vec!["agent".into()],
                ..main_only.clone()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST, "{why}");
        // No such environment.
        let (status, _) = set(&state, user(&["admin"]), "no-such-id", main_only.clone())
            .await
            .unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Taking it away is an admin's too, and leaves no policy.
        let gone = |as_user| {
            environments::delete_policy(as_user, State(state.clone()), Path(env.id.clone()))
        };
        assert_eq!(gone(user(&[])).await.unwrap_err().0, StatusCode::FORBIDDEN);
        assert_eq!(
            gone(user(&["admin"])).await.unwrap(),
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            gone(user(&["admin"])).await.unwrap(),
            StatusCode::NO_CONTENT
        );
        assert!(dagron_core::db::environment_policy(&state.read_pool, &name)
            .await
            .unwrap()
            .is_none());
        // No such environment: not a 204, which would say the one that was
        // meant had been reset.
        let (status, why) = environments::delete_policy(
            user(&["admin"]),
            State(state.clone()),
            Path("no-such-id".to_string()),
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND, "{why}");

        // A policy goes with its environment.
        set(&state, user(&["admin"]), &env.id, main_only)
            .await
            .unwrap();
        environments::delete(user(&[]), State(state.clone()), Path(env.id.clone()))
            .await
            .unwrap();
        let left: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM environment_policies WHERE environment_id = $1",
        )
        .bind(&env.id)
        .fetch_one(&state.read_pool)
        .await
        .unwrap();
        assert_eq!(left, 0);
    }

    /// A repository's environment is that repository's CI runs' alone: its
    /// own run is admitted though no one made the environment (its secrets
    /// read as empty), and another repository's run, and a run of no CI
    /// service's, are refused. Only an admin makes one.
    #[tokio::test]
    async fn a_repository_s_environment_is_that_repository_s_runs_alone() {
        let Some(state) = test_state("env-repository").await else {
            return;
        };
        let id = uuid::Uuid::new_v4().simple().to_string();
        let repo = format!("acme/r-{id}");
        let name = format!("github.com/{repo}");
        let own = RunTrigger {
            forge_host: Some("github.com".to_string()),
            repo: Some(repo.clone()),
            ..ci("trusted", "refs/heads/main")
        };
        let other = RunTrigger {
            repo: Some(format!("acme/other-{id}")),
            ..own.clone()
        };
        let yaml = format!(
            "name: repo-{id}\ntasks:\n  - name: deploy\n    environment: {name}\n    command: [\"true\"]\n"
        );
        let built = |trigger: Option<RunTrigger>| {
            let (state, yaml) = (state.clone(), yaml.clone());
            async move { control::build_dag(&state, &yaml, &BTreeMap::new(), trigger.as_ref()).await }
        };
        assert!(built(Some(own.clone())).await.is_ok(), "never made, and its own run's");
        for trigger in [Some(other), None] {
            let (status, why) = built(trigger).await.err().expect("refused");
            assert_eq!(status, StatusCode::FORBIDDEN, "{why}");
            assert!(why.contains("it is a repository's"), "{why}");
        }
        let body = || UpsertBody {
            name: Some(name.clone()),
            description: None,
            variables: None,
        };
        let (status, _) = environments::create(user(&[]), State(state.clone()), Json(body()))
            .await
            .err()
            .expect("a writer does not make one");
        assert_eq!(status, StatusCode::FORBIDDEN);
        let (status, _) = environments::create(user(&["admin"]), State(state.clone()), Json(body()))
            .await
            .expect("an admin does");
        assert_eq!(status, StatusCode::CREATED);
        assert!(built(Some(own)).await.is_ok());
    }

    #[tokio::test]
    async fn a_submit_that_names_an_environment_its_run_may_not_use_is_refused() {
        let Some(state) = test_state("env-policy-submit").await else {
            return;
        };
        let id = uuid::Uuid::new_v4().simple().to_string();
        let (release, open, previews) = (
            format!("rel-{id}"),
            format!("open-{id}"),
            format!("pre-{id}"),
        );
        let env = environment(&state, &release).await;
        set(
            &state,
            user(&["admin"]),
            &env.id,
            policy(&["trusted"], Some(&["refs/heads/main"]), None),
        )
        .await
        .unwrap();
        environment(&state, &open).await;
        let env = environment(&state, &previews).await;
        set(
            &state,
            user(&["admin"]),
            &env.id,
            policy(&["trusted", "untrusted"], None, None),
        )
        .await
        .unwrap();

        let workflow = format!("pol-{id}");
        let naming = |environment: &str| {
            format!(
                "name: {workflow}\nenvironment: {environment}\ntasks:\n  - name: a\n    command: [\"deploy\", \"{{{{ env.REGION }}}}\"]\n"
            )
        };
        let none = BTreeMap::new();
        let built = |yaml: String, trigger: Option<RunTrigger>| {
            let state = state.clone();
            async move { control::build_dag(&state, &yaml, &BTreeMap::new(), trigger.as_ref()).await }
        };
        let refused = |r: Result<dagron_core::dag::DagGraph, (StatusCode, String)>| {
            let (status, why) = r.err().expect("the submit is refused");
            assert_eq!(status, StatusCode::FORBIDDEN, "{why}");
            why
        };

        // Main's run is admitted, and has the environment's variables.
        let dag = built(naming(&release), Some(ci("trusted", "refs/heads/main")))
            .await
            .unwrap();
        assert_eq!(dag.task_spec("a").unwrap().command, ["deploy", "eu"]);
        // A member's pull request is trusted, and is not for main.
        let why = refused(built(naming(&release), Some(ci("trusted", "refs/pull/7/head"))).await);
        assert!(
            why.contains(&format!(
                "environment '{release}' may not be used by this run"
            )),
            "{why}"
        );
        assert!(
            why.contains("and this run is for refs/pull/7/head"),
            "{why}"
        );
        // A run with no record is this installation's own, and for no ref.
        let why = refused(built(naming(&release), None).await);
        assert!(why.contains("this run was made for no ref"), "{why}");

        // An environment with no policy: trusted runs, and no other class.
        assert!(built(naming(&open), None).await.is_ok());
        let fork = || Some(ci("untrusted", "refs/pull/8/head"));
        let why = refused(built(naming(&open), fork()).await);
        assert!(
            why.contains("its policy admits trusted runs, and this one is untrusted"),
            "{why}"
        );
        // One an admin opened to the class.
        assert!(built(naming(&previews), fork()).await.is_ok());
        // An environment that is not there: a trusted run is told so, and an
        // untrusted one is told what it would be told of any it may not use.
        let (status, why) = built(naming("no-such-environment"), None)
            .await
            .err()
            .unwrap();
        assert_eq!(status, StatusCode::BAD_REQUEST, "{why}");
        let why = refused(built(naming("no-such-environment"), fork()).await);
        assert!(why.contains("admits trusted runs"), "{why}");

        // A task's own environment is asked as the workflow's is.
        let on_a_task = |environment: &str| {
            format!(
                "name: {workflow}\ntasks:\n  - name: a\n    command: [\"true\"]\n  - name: publish\n    environment: {environment}\n    command: [\"true\"]\n"
            )
        };
        assert!(
            built(on_a_task(&release), Some(ci("trusted", "refs/heads/main")))
                .await
                .is_ok()
        );
        let why =
            refused(built(on_a_task(&release), Some(ci("trusted", "refs/pull/7/head"))).await);
        assert!(
            why.contains(&format!("environment '{release}' may not be used")),
            "{why}"
        );
        let (status, why) = built(on_a_task("no-such-environment"), None)
            .await
            .err()
            .unwrap();
        assert_eq!(
            (status, why.as_str()),
            (
                StatusCode::BAD_REQUEST,
                "environment 'no-such-environment' not found"
            )
        );

        // A refused submit makes nothing: no run of the workflow is there.
        let pull = ci("trusted", "refs/pull/7/head");
        let yaml = naming(&release);
        let (status, _) = control::submit_yaml_recorded(
            &state,
            &yaml,
            &yaml,
            &none,
            Some("svc@example.com"),
            &pull,
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::FORBIDDEN);
        let runs: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM workflow_runs wr JOIN workflow_definitions wd ON wd.id = wr.definition_id
              WHERE wd.name = $1",
        )
        .bind(&workflow)
        .fetch_one(&state.read_pool)
        .await
        .unwrap();
        assert_eq!(runs, 0);
        // And main's is made, with what it was recorded as.
        let main = ci("trusted", "refs/heads/main");
        let run = control::submit_yaml_recorded(
            &state,
            &yaml,
            &yaml,
            &none,
            Some("svc@example.com"),
            &main,
        )
        .await
        .unwrap();
        let record = dagron_core::db::run_trigger(&state.read_pool, &run)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.git_ref.as_deref(), Some("refs/heads/main"));
    }

    /// A run that is not trusted is confined (`dagron_core::confine`). Its
    /// submit is refused for what such a run may not use, and makes nothing.
    /// One that is made has rows without what it is not given. And a
    /// resubmit of it, by anyone, is a run of its class.
    #[tokio::test]
    async fn a_submit_that_is_not_trusted_is_confined_and_so_is_its_resubmit() {
        let Some(state) = test_state("confined-submit").await else {
            return;
        };
        let id = uuid::Uuid::new_v4().simple().to_string();
        let workflow = format!("fork-{id}");
        let none = BTreeMap::new();
        let fork = ci("untrusted", "refs/pull/8/head");
        let runs = || {
            let (state, workflow) = (state.clone(), workflow.clone());
            async move {
                sqlx::query_scalar::<_, i64>(
                    "SELECT count(*) FROM workflow_runs wr JOIN workflow_definitions wd ON wd.id = wr.definition_id
                      WHERE wd.name = $1",
                )
                .bind(&workflow)
                .fetch_one(&state.read_pool)
                .await
                .unwrap()
            }
        };

        // What the engine would do on the workflow's word is refused, with
        // the reason, and whichever part of it asks.
        for (asks, what) in [
            (
                "notify:\n  webhook: { url: \"http://127.0.0.1:9/\" }\n",
                "`notify`",
            ),
            ("on_datasets: [\"s3://lake/orders\"]\n", "`on_datasets`"),
        ] {
            let yaml =
                format!("name: {workflow}\n{asks}tasks:\n  - name: a\n    command: [\"true\"]\n");
            let (status, why) = control::submit_yaml_recorded(
                &state,
                &yaml,
                &yaml,
                &none,
                Some("svc@example.com"),
                &fork,
            )
            .await
            .unwrap_err();
            assert_eq!(status, StatusCode::FORBIDDEN, "{why}");
            assert!(
                why.starts_with(&format!(
                    "this run is untrusted, and {} {what}: ",
                    dagron_core::confine::REFUSAL_MARK
                )),
                "{why}"
            );
        }
        let on_a_task = |asks: &str| {
            format!("name: {workflow}\ntasks:\n  - name: a\n    command: [\"true\"]\n{asks}")
        };
        for (asks, what) in [
            (
                "    service_account: deployer\n",
                "`service_account` (task 'a')",
            ),
            (
                "    produces: [\"s3://lake/orders\"]\n",
                "`produces` (task 'a')",
            ),
            (
                "  - name: gate\n    type: approval\n    approvers: [\"group:release\"]\n",
                "`type: approval` (task 'gate')",
            ),
            (
                "  - name: call\n    type: workflow\n    workflow: deploy\n",
                "`type: workflow` (task 'call')",
            ),
        ] {
            let yaml = on_a_task(asks);
            let (status, why) = control::build_dag(&state, &yaml, &none, Some(&fork))
                .await
                .err()
                .expect("the submit is refused");
            assert_eq!(status, StatusCode::FORBIDDEN, "{why}");
            assert!(why.contains(what), "{why}");
            // A trusted run's workflow is its author's to write.
            let main = ci("trusted", "refs/heads/main");
            assert!(
                control::build_dag(&state, &yaml, &none, Some(&main))
                    .await
                    .is_ok(),
                "{asks}"
            );
        }
        assert_eq!(runs().await, 0, "a refused submit makes nothing");

        // What other runs share is taken from it, and it is not refused for
        // having written it.
        let yaml = on_a_task("    cache: { key: one }\n    pool: builders\n    priority: 9\n");
        let run = control::submit_yaml_recorded(
            &state,
            &yaml,
            &yaml,
            &none,
            Some("svc@example.com"),
            &fork,
        )
        .await
        .unwrap();
        let row = |run: String| {
            let state = state.clone();
            async move {
                let (pool, priority, input): (Option<String>, i64, Option<String>) =
                    sqlx::query_as(
                        "SELECT pool, priority::bigint, input FROM task_runs WHERE run_id = $1",
                    )
                    .bind(&run)
                    .fetch_one(&state.read_pool)
                    .await
                    .unwrap();
                let stored: dagron_core::dag::TaskSpec =
                    serde_json::from_str(&input.expect("the task's spec is stored")).unwrap();
                (pool, priority, stored.cache.is_some())
            }
        };
        // Its own pool, not the one it named.
        let theirs = || (Some("untrusted".to_string()), 0, false);
        assert_eq!(row(run.clone()).await, theirs());

        // The same again, asked for by a person: theirs, and still a fork's.
        let dev = AuthUser(SessionClaims {
            sub: "sub-dev".to_string(),
            email: "dev@example.com".to_string(),
            name: String::new(),
            groups: vec![],
            exp: 0,
        });
        let (_, Json(again)) = control::resubmit_run(dev, State(state.clone()), Path(run.clone()))
            .await
            .unwrap();
        let record = dagron_core::db::run_trigger(&state.read_pool, &again.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(dagron_core::envpolicy::class_of(Some(&record)), "untrusted");
        assert_eq!(
            (
                record.kind.as_str(),
                record.principal.as_str(),
                record.source.as_deref(),
                record.git_ref.as_deref(),
            ),
            (
                "manual",
                "user:dev@example.com",
                Some(format!("resubmit:{run}").as_str()),
                Some("refs/pull/8/head"),
            )
        );
        assert_eq!(row(again.run_id).await, theirs());

        // A trusted run of the same workflow has all of it, and its
        // resubmit is its caller's own.
        let main = ci("trusted", "refs/heads/main");
        let own = control::submit_yaml_recorded(
            &state,
            &yaml,
            &yaml,
            &none,
            Some("svc@example.com"),
            &main,
        )
        .await
        .unwrap();
        assert_eq!(
            row(own.clone()).await,
            (Some("builders".to_string()), 9, true)
        );
        let dev = user(&[]);
        let (_, Json(again)) = control::resubmit_run(dev, State(state.clone()), Path(own))
            .await
            .unwrap();
        let record = dagron_core::db::run_trigger(&state.read_pool, &again.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (record.trust.as_deref(), record.git_ref.as_deref()),
            (None, None)
        );
    }
}

/// A run that waits for its workflow's cap, and one that takes another's
/// place, against a real datastore: the Postgres half of `dagron_core::db`'s
/// `create_run` under `when_full`, `admit_waiting_runs` and what the run's
/// detail then answers. Skipped without `TEST_DATABASE_URL`.
///
/// The database is every live test's, and `admit_waiting_runs` lets in
/// whatever waits in it: so a test here says what its own runs read after a
/// pass, and never what a pass returned. A run of this module's waits behind
/// a run of its own workflow, which no other test ends.
#[cfg(test)]
mod wait_live_tests {
    use dagron_core::dag::DagGraph;
    use dagron_core::db;
    use dagron_core::models::{trigger_kind, MaxActiveRunsReached, RunTrigger};

    use super::redrive_tests::test_state;
    use super::*;
    use crate::routes::runs;

    const WORKER: &str = "wait-live-worker";

    /// Held by each test here that lets runs in or reads that one waits.
    /// `admit_waiting_runs` lets in what waits across the whole database, so
    /// one test's pass would let in a run another reads as still waiting.
    static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn viewer() -> AuthUser {
        AuthUser(crate::auth::SessionClaims {
            sub: "u1".to_string(),
            email: "u1@example.com".to_string(),
            name: "U1".to_string(),
            groups: vec![],
            exp: 0,
        })
    }

    /// A workflow of one task under a name of its own, with `head` above its
    /// tasks. In a runner class of its own, as `stop_live_tests` has one: a
    /// claim made here is of this test's rows.
    fn spec(head: &str) -> String {
        let name = format!("wait-{}", uuid::Uuid::new_v4().simple());
        format!("name: {name}\nrunner_class: {name}\n{head}tasks:\n  - name: job\n    command: [\"true\"]\n")
    }

    async fn submit(state: &AppState, yaml: &str) -> Result<String, (StatusCode, String)> {
        control::submit_yaml_as(state, yaml, yaml, &BTreeMap::new(), None).await
    }

    async fn detail(state: &AppState, run: &str) -> runs::RunDetail {
        let Json(detail) = runs::get_run(viewer(), State(state.clone()), Path(run.to_string()))
            .await
            .unwrap();
        detail
    }

    /// What a run reads: its status, whether it waits, and what it is to be.
    async fn reads(state: &AppState, run: &str) -> (String, bool, Option<String>) {
        let seen = detail(state, run).await;
        let waits = seen.status == "running" && seen.queued_at.is_some() && seen.admitted_at.is_none();
        (seen.status, waits, seen.stopping_as)
    }

    fn waiting() -> (String, bool, Option<String>) {
        ("running".to_string(), true, None)
    }

    fn running() -> (String, bool, Option<String>) {
        ("running".to_string(), false, None)
    }

    fn ended(status: &str) -> (String, bool, Option<String>) {
        (status.to_string(), false, None)
    }

    /// What the run's one task still waits for.
    async fn held_by(state: &AppState, run: &str) -> i64 {
        sqlx::query_scalar("SELECT remaining_deps FROM task_runs WHERE run_id = $1 AND name = 'job'")
            .bind(run)
            .fetch_one(&state.write_pool)
            .await
            .unwrap()
    }

    async fn job(state: &AppState, run: &str) -> String {
        sqlx::query_scalar("SELECT id FROM task_runs WHERE run_id = $1 AND name = 'job'")
            .bind(run)
            .fetch_one(&state.write_pool)
            .await
            .unwrap()
    }

    /// The engine's claim of the task, as the one statement it is: no engine
    /// runs here, and `claim_ready` would take other tests' rows.
    async fn claim(state: &AppState, task: &str) -> i64 {
        let lease = (chrono::Utc::now() + chrono::TimeDelta::seconds(300)).to_rfc3339();
        sqlx::query_scalar(
            "UPDATE task_runs
             SET status = 'running', claimed_by = $1, lease_expires_at = $2,
                 attempt = attempt + 1, version = version + 1
             WHERE id = $3
             RETURNING version",
        )
        .bind(WORKER)
        .bind(&lease)
        .bind(task)
        .fetch_one(&state.write_pool)
        .await
        .unwrap()
    }

    async fn cancel(state: &AppState, run: &str) -> (u64, u64) {
        let Json(did) = control::cancel_run(viewer(), State(state.clone()), Path(run.to_string()))
            .await
            .unwrap();
        (did.cancelled, did.stopping)
    }

    async fn completed_events(state: &AppState, run: &str) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM event_outbox WHERE run_id = $1 AND event_type = 'run.completed'")
            .bind(run)
            .fetch_one(&state.write_pool)
            .await
            .unwrap()
    }

    /// A run that arrives at a full cap is made, and waits: it reads
    /// `running` with `queued_at`, its task is held, and no pass lets it in
    /// while the run before it has not ended. Then it is let in, and the next
    /// one after it. One cancelled as it waits is over at once.
    #[tokio::test]
    async fn a_run_over_its_cap_waits_and_is_let_in_when_the_one_before_has_ended() {
        let Some(state) = test_state("wait-live-queue").await else { return };
        let _alone = ONE_AT_A_TIME.lock().await;
        let pool = &state.write_pool;
        let yaml = spec("max_active_runs: 1\nwhen_full: queue\nrun_timeout_secs: 600\n");
        let first = submit(&state, &yaml).await.unwrap();
        let second = submit(&state, &yaml).await.expect("made, where it was refused");
        let third = submit(&state, &yaml).await.unwrap();
        assert_eq!(reads(&state, &first).await, running());
        assert_eq!(reads(&state, &second).await, waiting());
        assert_eq!(reads(&state, &third).await, waiting());
        assert_eq!((held_by(&state, &first).await, held_by(&state, &second).await), (0, 1));

        // The counts every scrape reads say that something waits.
        let snap = db::status_counts(pool).await.unwrap();
        assert!(snap.runs_waiting >= 2 && snap.runs_waiting_oldest_since.is_some(), "{}", snap.runs_waiting);

        db::admit_waiting_runs(pool).await.unwrap();
        assert_eq!(reads(&state, &second).await, waiting(), "the first has not ended");
        // Its deadline does not end it as it waits, however long that is.
        sqlx::query("UPDATE workflow_runs SET deadline_at = '2020-01-01T00:00:00+00:00' WHERE id = $1")
            .bind(&third)
            .execute(pool)
            .await
            .unwrap();
        db::cancel_overdue_runs(pool).await.unwrap();
        assert_eq!(reads(&state, &third).await, waiting());

        // The first ends: the second is let in, and the third is not.
        assert_eq!(cancel(&state, &first).await, (1, 0));
        db::admit_waiting_runs(pool).await.unwrap();
        let seen = detail(&state, &second).await;
        assert_eq!((seen.status.as_str(), seen.admitted_at.is_some()), ("running", true));
        assert_eq!(held_by(&state, &second).await, 0);
        assert_eq!(reads(&state, &third).await, waiting());
        // Its ten minutes are counted from here.
        let deadline: Option<String> = sqlx::query_scalar("SELECT deadline_at FROM workflow_runs WHERE id = $1")
            .bind(&second)
            .fetch_one(pool)
            .await
            .unwrap();
        assert!(deadline > seen.admitted_at, "{deadline:?} {:?}", seen.admitted_at);
        db::admit_waiting_runs(pool).await.unwrap();
        assert_eq!(held_by(&state, &second).await, 0, "a run is let in once");

        // Cancelled as it waits, the third is over, and its end is told.
        assert_eq!(cancel(&state, &third).await, (1, 0));
        assert_eq!(reads(&state, &third).await, ended("cancelled"));
        assert_eq!(completed_events(&state, &third).await, 1);
        // Run again while its key is full, it is refused: it does not wait.
        let (code, why) = control::rerun_run(viewer(), State(state.clone()), Path(third.clone()), None)
            .await
            .err()
            .expect("refused");
        assert_eq!(code, StatusCode::TOO_MANY_REQUESTS, "{why}");
        // With the key free it is let in by being run again, with the
        // dependencies it has, and no later pass takes one off it.
        assert_eq!(cancel(&state, &second).await, (1, 0));
        let Json(again) = control::rerun_run(viewer(), State(state.clone()), Path(third.clone()), None)
            .await
            .unwrap();
        assert_eq!(again.rerun, 1);
        assert_eq!(reads(&state, &third).await, running());
        db::admit_waiting_runs(pool).await.unwrap();
        assert_eq!(held_by(&state, &third).await, 0);
    }

    /// Submits that arrive together at a workflow whose cap is one: one run
    /// has the cap, and every other waits. None is refused, and none is let
    /// past the one that has it.
    #[tokio::test]
    async fn runs_that_arrive_together_wait_behind_one() {
        let Some(state) = test_state("wait-live-together").await else { return };
        let _alone = ONE_AT_A_TIME.lock().await;
        let yaml = spec("max_active_runs: 1\nwhen_full: queue\n");
        let made = futures::future::join_all((0..8).map(|_| submit(&state, &yaml))).await;
        let mut waits = 0;
        for run in &made {
            let run = run.as_ref().expect("made");
            waits += i32::from(reads(&state, run).await == waiting());
        }
        assert_eq!(waits, 7, "one of eight has the cap");
        // Nothing has ended, so a pass lets none of them in.
        db::admit_waiting_runs(&state.write_pool).await.unwrap();
        let mut still = 0;
        for run in &made {
            still += i32::from(reads(&state, run.as_ref().unwrap()).await == waiting());
        }
        assert_eq!(still, 7);
    }

    /// A workflow that says nothing of a full cap refuses, as it did: `429`.
    /// One that queues refuses where as many wait as may, and says how many.
    #[tokio::test]
    async fn a_workflow_that_does_not_queue_refuses_as_it_did() {
        let Some(state) = test_state("wait-live-refuse").await else { return };
        let yaml = spec("max_active_runs: 1\n");
        submit(&state, &yaml).await.unwrap();
        let (code, why) = submit(&state, &yaml).await.expect_err("refused");
        assert_eq!(code, StatusCode::TOO_MANY_REQUESTS);
        assert!(why.contains("(1 active, cap 1)"), "{why}");

        let said = control::create_run_refusal(anyhow::Error::new(MaxActiveRunsReached {
            name: "w".into(),
            max: 1,
            active: 101,
            waiting: 100,
            key: Some("k".into()),
        }));
        assert_eq!(said.0, StatusCode::TOO_MANY_REQUESTS);
        assert!(said.1.contains("(101 active, 100 of them waiting: no more may wait, cap 1)"), "{}", said.1);
    }

    /// A run that supersedes cancels the run of its key before it. One with
    /// nothing a worker holds is over where the new run is made, which then
    /// waits for nothing. One whose work is still running holds the key
    /// until that is gone, and the new run waits for it.
    #[tokio::test]
    async fn a_run_that_supersedes_cancels_what_is_before_it_and_waits_for_its_work() {
        let Some(state) = test_state("wait-live-supersede").await else { return };
        let _alone = ONE_AT_A_TIME.lock().await;
        let pool = &state.write_pool;
        let yaml = spec("max_active_runs: 1\nwhen_full: supersede\n");
        let first = submit(&state, &yaml).await.unwrap();
        let second = submit(&state, &yaml).await.unwrap();
        assert_eq!(reads(&state, &first).await, ended("cancelled"));
        assert_eq!(detail(&state, &first).await.output, Some(format!("superseded by run {second}")));
        assert_eq!(completed_events(&state, &first).await, 1);
        assert_eq!(reads(&state, &second).await, running());
        assert_eq!(held_by(&state, &second).await, 0);

        // The second's task is a worker's: the third asks it to stop, and
        // waits until that worker says the work is gone.
        let task = job(&state, &second).await;
        let fence = claim(&state, &task).await;
        let third = submit(&state, &yaml).await.unwrap();
        assert_eq!(reads(&state, &second).await, ("running".to_string(), false, Some("cancelled".to_string())));
        assert_eq!(completed_events(&state, &second).await, 0);
        assert_eq!(reads(&state, &third).await, waiting());
        db::admit_waiting_runs(pool).await.unwrap();
        assert_eq!(reads(&state, &third).await, waiting(), "the second's work is still there");

        // A fourth takes the third's place, which had started nothing.
        let fourth = submit(&state, &yaml).await.unwrap();
        assert_eq!(reads(&state, &third).await, ended("cancelled"));
        assert_eq!(completed_events(&state, &third).await, 1);
        assert_eq!(reads(&state, &fourth).await, waiting());

        assert!(db::settle_stopped_task(pool, &task, WORKER, fence).await.unwrap());
        assert_eq!(reads(&state, &second).await, ended("cancelled"));
        db::admit_waiting_runs(pool).await.unwrap();
        assert_eq!(reads(&state, &fourth).await, running());
        assert_eq!(held_by(&state, &fourth).await, 0);
    }

    /// A run that is not trusted writes its own key, so it takes the place of
    /// its own source's runs alone: another pull request's stays, and is in
    /// its way.
    #[tokio::test]
    async fn a_run_that_is_not_trusted_supersedes_its_own_source_s_runs_alone() {
        let Some(state) = test_state("wait-live-confined").await else { return };
        let _alone = ONE_AT_A_TIME.lock().await;
        let pool = &state.write_pool;
        let yaml = spec("max_active_runs: 1\nwhen_full: supersede\nconcurrency_key: pr-7\n");
        let dag = DagGraph::from_yaml(&yaml).unwrap();
        let from = |pr: i64| RunTrigger {
            kind: trigger_kind::CI.to_string(),
            principal: "ci:svc@example.com".to_string(),
            trust: Some("untrusted".to_string()),
            forge_host: Some("github.com".to_string()),
            repo: Some("acme/widgets".to_string()),
            git_ref: Some(format!("refs/pull/{pr}/head")),
            pr: Some(pr),
            ..Default::default()
        };
        let theirs = db::create_run_triggered(pool, &dag, &yaml, &from(7)).await.unwrap();
        let intruder = db::create_run_triggered(pool, &dag, &yaml, &from(8)).await.unwrap();
        assert_eq!(reads(&state, &theirs).await, running(), "another pull request's is not its to cancel");
        assert_eq!(reads(&state, &intruder).await, waiting());

        let next = db::create_run_triggered(pool, &dag, &yaml, &from(7)).await.unwrap();
        assert_eq!(reads(&state, &theirs).await, ended("cancelled"), "its own is");
        assert_eq!(reads(&state, &intruder).await, waiting());
        assert_eq!(reads(&state, &next).await, waiting());
        // Oldest first: the one that came before it.
        db::admit_waiting_runs(pool).await.unwrap();
        assert_eq!(reads(&state, &intruder).await, running());
        assert_eq!(reads(&state, &next).await, waiting());

        let key: Option<String> = sqlx::query_scalar("SELECT concurrency_key FROM workflow_runs WHERE id = $1")
            .bind(&next)
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(key.as_deref(), Some("untrusted:pr-7"));
    }
}

#[cfg(test)]
mod sweep_index_live_tests {
    use super::redrive_tests::test_state;

    /// The indexes migrations_pg 101 to 105 make for the sweeps' reads.
    const SWEEP_INDEXES: [&str; 5] = [
        "idx_task_runs_fanout_parked",
        "idx_task_runs_wake_due",
        "idx_task_runs_wait_url_due",
        "idx_task_runs_wait_dataset_parked",
        "idx_task_runs_sub_run_parked",
    ];

    /// Each sweep's index is built, valid, led by `status` and of running
    /// rows, and the start-up check finds none of them INVALID. No plan is
    /// asserted: the database is small and shared, and on it the planner may
    /// read any index it likes.
    #[tokio::test]
    async fn sweep_indexes_are_built_and_valid() {
        let Some(state) = test_state("sweep-indexes").await else {
            return;
        };
        let pool = &state.write_pool;
        for index in SWEEP_INDEXES {
            let (valid, def): (bool, String) = sqlx::query_as(
                "SELECT i.indisvalid, pg_get_indexdef(i.indexrelid)
                   FROM pg_index i
                   JOIN pg_class c ON c.oid = i.indexrelid
                   JOIN pg_namespace n ON n.oid = c.relnamespace
                  WHERE c.relname = $1 AND n.nspname = current_schema()",
            )
            .bind(index)
            .fetch_optional(pool)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{index} was not built"));
            assert!(valid, "{index} is INVALID: {def}");
            assert!(def.contains("task_runs USING btree (status, "), "{def}");
            assert!(def.contains("(status = 'running'::text)"), "{def}");
        }
        let invalid = dagron_core::db::invalid_indexes(pool).await.unwrap();
        assert!(
            SWEEP_INDEXES
                .iter()
                .all(|index| !invalid.iter().any(|i| i == index)),
            "{invalid:?}"
        );
    }
}

/// The sweeps on Postgres: the slots two engines share, the
/// claim of the HTTP sensors, the reads that leave out what is not due, and
/// the gauges in two tiers. Each test reads and writes its own rows alone:
/// slots of names of its own, runs in a runner class of its own, and each
/// row changed one statement at a time.
#[cfg(test)]
mod sweep_live_tests {
    use dagron_core::dag::DagGraph;
    use dagron_core::db;

    use super::redrive_tests::test_state;
    use super::*;

    /// A slot's name no engine sweeps.
    fn slot_name() -> String {
        format!("test-{}", uuid::Uuid::new_v4().simple())
    }

    async fn holder(pool: &sqlx::PgPool, name: &str) -> String {
        sqlx::query_scalar("SELECT holder FROM sweep_slots WHERE name = $1")
            .bind(name)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn forget(pool: &sqlx::PgPool, name: &str) {
        sqlx::query("DELETE FROM sweep_slots WHERE name = $1")
            .bind(name)
            .execute(pool)
            .await
            .unwrap();
    }

    /// Two engines asking at once for a slot that is missing, or due: one of
    /// them takes it. At once again, neither: the window is the first's. A
    /// hand-back makes it the next one's, at the next turn, and the first's
    /// hand-back at its own turn then changes nothing. A deleted row is made
    /// again at the next claim.
    #[tokio::test]
    async fn two_engines_take_a_sweep_slot_once_a_window() {
        let Some(state) = test_state("sweep-slots").await else {
            return;
        };
        let pool = &state.write_pool;
        let name = slot_name();
        let wanted = [(name.as_str(), 3600.0)];

        let (a, b) = tokio::join!(
            db::claim_sweep_slots(pool, "engine-a", &wanted),
            db::claim_sweep_slots(pool, "engine-b", &wanted),
        );
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!(
            a.len() + b.len(),
            1,
            "one engine takes a missing slot: {a:?} {b:?}"
        );
        let (first, other) = if a.is_empty() {
            (b[0].clone(), "engine-a")
        } else {
            (a[0].clone(), "engine-b")
        };
        assert_eq!(first.turn, 1);
        assert!(first.cursor.is_none());
        let winner = holder(pool, &name).await;
        assert_ne!(winner, other);

        for engine in ["engine-a", "engine-b"] {
            let again = db::claim_sweep_slots(pool, engine, &wanted).await.unwrap();
            assert!(again.is_empty(), "the window is the first's: {again:?}");
        }

        assert!(db::hand_back_sweep_slot(pool, &name, first.turn)
            .await
            .unwrap());
        let next = db::claim_sweep_slots(pool, other, &wanted).await.unwrap();
        assert_eq!(next.len(), 1, "a slot handed back is due");
        assert_eq!(next[0].turn, 2);
        assert_eq!(holder(pool, &name).await, other);
        assert!(
            !db::hand_back_sweep_slot(pool, &name, first.turn)
                .await
                .unwrap(),
            "a hand-back at a turn gone by changes nothing"
        );
        assert!(db::claim_sweep_slots(pool, &winner, &wanted)
            .await
            .unwrap()
            .is_empty());

        // Due again: still one winner of two at once.
        sqlx::query(
            "UPDATE sweep_slots SET due_at = '1970-01-01T00:00:00.000000+00:00' WHERE name = $1",
        )
        .bind(&name)
        .execute(pool)
        .await
        .unwrap();
        let (a, b) = tokio::join!(
            db::claim_sweep_slots(pool, "engine-a", &wanted),
            db::claim_sweep_slots(pool, "engine-b", &wanted),
        );
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!(
            a.len() + b.len(),
            1,
            "one engine takes a due slot: {a:?} {b:?}"
        );
        assert_eq!(a.iter().chain(&b).next().unwrap().turn, 3);

        forget(pool, &name).await;
        let healed = db::claim_sweep_slots(pool, "engine-b", &wanted)
            .await
            .unwrap();
        assert_eq!(healed.len(), 1, "a deleted slot is made again");
        assert_eq!(healed[0].turn, 1);
        forget(pool, &name).await;
    }

    /// A slot due far later than a claim would set it, as every slot is once
    /// the database's clock has stepped back, is taken, and by one of two
    /// engines asking at once. Then a claim of the same interval leaves it
    /// to its window, and one of a shorter interval takes it early.
    #[tokio::test]
    async fn a_slot_due_later_than_a_claim_would_set_it_is_taken() {
        let Some(state) = test_state("sweep-slot-ahead").await else {
            return;
        };
        let pool = &state.write_pool;
        let name = slot_name();
        let wanted = [(name.as_str(), 3600.0)];
        let first = db::claim_sweep_slots(pool, "engine-a", &wanted)
            .await
            .unwrap();
        assert_eq!(first.len(), 1);

        sqlx::query(
            "UPDATE sweep_slots SET due_at = '2999-01-01T00:00:00.000000+00:00' WHERE name = $1",
        )
        .bind(&name)
        .execute(pool)
        .await
        .unwrap();
        let (a, b) = tokio::join!(
            db::claim_sweep_slots(pool, "engine-a", &wanted),
            db::claim_sweep_slots(pool, "engine-b", &wanted),
        );
        let (a, b) = (a.unwrap(), b.unwrap());
        assert_eq!(
            a.len() + b.len(),
            1,
            "one engine takes a slot due far ahead: {a:?} {b:?}"
        );
        assert_eq!(a.iter().chain(&b).next().unwrap().turn, 2);

        for engine in ["engine-a", "engine-b"] {
            let again = db::claim_sweep_slots(pool, engine, &wanted).await.unwrap();
            assert!(again.is_empty(), "the window is the taker's: {again:?}");
        }
        let shorter = [(name.as_str(), 60.0)];
        let early = db::claim_sweep_slots(pool, "engine-a", &shorter)
            .await
            .unwrap();
        assert_eq!(early.len(), 1, "a shorter interval takes it early");
        assert_eq!(early[0].turn, 3);
        forget(pool, &name).await;
    }

    /// Where the reap got to is written at the turn its slot was taken at: an
    /// engine that took it before, and stalled, writes nothing over the one
    /// that took it since, and the next engine reads the latter's.
    #[tokio::test]
    async fn a_reap_cursor_written_at_a_stale_turn_is_dropped() {
        let Some(state) = test_state("sweep-cursor").await else {
            return;
        };
        let pool = &state.write_pool;
        let name = slot_name();
        let wanted = [(name.as_str(), 3600.0)];

        let stalled = db::claim_sweep_slots(pool, "engine-a", &wanted)
            .await
            .unwrap()
            .remove(0);
        assert!(db::hand_back_sweep_slot(pool, &name, stalled.turn)
            .await
            .unwrap());
        let taken = db::claim_sweep_slots(pool, "engine-b", &wanted)
            .await
            .unwrap()
            .remove(0);
        assert!(taken.turn > stalled.turn);

        assert!(db::set_sweep_cursor(pool, &name, taken.turn, Some("run-b"))
            .await
            .unwrap());
        assert!(
            !db::set_sweep_cursor(pool, &name, stalled.turn, Some("run-a"))
                .await
                .unwrap(),
            "a write at a turn gone by is dropped"
        );
        assert!(db::hand_back_sweep_slot(pool, &name, taken.turn)
            .await
            .unwrap());
        let next = db::claim_sweep_slots(pool, "engine-a", &wanted)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(next.cursor.as_deref(), Some("run-b"));
        forget(pool, &name).await;
    }

    /// A workflow of `tasks` under a name of its own.
    fn spec(tasks: &str) -> String {
        let name = format!("sweep-{}", uuid::Uuid::new_v4().simple());
        format!("name: {name}\ntasks:\n{tasks}")
    }

    /// A task of this test's run, claimed: its id, its name, the claim's fence.
    #[derive(Debug)]
    struct Claimed {
        id: String,
        name: String,
        fence: i64,
    }

    /// Make a run of `yaml` and claim each of its tasks by its id, as the one
    /// statement the engine's claim is: no engine runs here, `claim_ready`
    /// would take other tests' rows, and another test's advance may hold
    /// this one's row when this one's would make it ready.
    async fn started(pool: &sqlx::PgPool, yaml: &str) -> (String, Vec<Claimed>) {
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run = db::create_run(pool, &dag, yaml).await.unwrap();
        let tasks: Vec<(String, String)> =
            sqlx::query_as("SELECT id, name FROM task_runs WHERE run_id = $1 ORDER BY name")
                .bind(&run)
                .fetch_all(pool)
                .await
                .unwrap();
        let lease = (chrono::Utc::now() + chrono::TimeDelta::seconds(300)).to_rfc3339();
        let mut claimed = Vec::new();
        for (id, name) in tasks {
            let fence: i64 = sqlx::query_scalar(
                "UPDATE task_runs
                 SET status = 'running', claimed_by = 'sweep-live', lease_expires_at = $1,
                     attempt = attempt + 1, version = version + 1
                 WHERE id = $2
                 RETURNING version",
            )
            .bind(&lease)
            .bind(&id)
            .fetch_one(pool)
            .await
            .unwrap();
            claimed.push(Claimed { id, name, fence });
        }
        (run, claimed)
    }

    /// Two engines claiming the HTTP sensors due at once take each of this
    /// test's once between them: one GET each, where each engine sent one.
    #[tokio::test]
    async fn a_due_url_wait_is_claimed_once_by_two_engines() {
        let Some(state) = test_state("sweep-url").await else {
            return;
        };
        let pool = &state.write_pool;
        let yaml = spec(
            "  - { name: u1, type: wait, wait: { url: \"https://example.com/a\" } }\n  \
             - { name: u2, type: wait, wait: { url: \"https://example.com/b\" } }\n",
        );
        let (run, claimed) = started(pool, &yaml).await;
        assert_eq!(claimed.len(), 2, "{claimed:?}");
        let due = "2026-01-01T00:00:00+00:00";
        for t in &claimed {
            let url = format!("https://example.com/{}", t.name);
            assert!(db::park_wait_url(pool, &t.id, t.fence, &url, due)
                .await
                .unwrap());
        }

        let (a, b) = tokio::join!(
            db::claim_due_url_waits(pool, 32, 30),
            db::claim_due_url_waits(pool, 32, 30),
        );
        let ours = |rows: Vec<(String, String, String)>| -> Vec<String> {
            rows.into_iter()
                .filter(|(id, _, _)| claimed.iter().any(|t| t.id == *id))
                .map(|(id, _, _)| id)
                .collect()
        };
        let (a, b) = (ours(a.unwrap()), ours(b.unwrap()));
        let mut both: Vec<&String> = a.iter().chain(&b).collect();
        both.sort();
        let mut want: Vec<&String> = claimed.iter().map(|t| &t.id).collect();
        want.sort();
        assert_eq!(both, want, "each sensor claimed once: {a:?} {b:?}");
        let again = ours(db::claim_due_url_waits(pool, 32, 30).await.unwrap());
        assert!(again.is_empty(), "{again:?}");

        db::cancel_run(pool, &run).await.unwrap();
    }

    /// A time sensor that has come due is resolved once, by one of two
    /// passes at once, and with when it was due.
    #[tokio::test]
    async fn reconcile_waits_resolves_a_due_sensor_once() {
        let Some(state) = test_state("sweep-waits").await else {
            return;
        };
        let pool = &state.write_pool;
        let yaml = spec("  - { name: w, type: wait, wait: { for: 1h } }\n");
        let (run, claimed) = started(pool, &yaml).await;
        assert_eq!(claimed.len(), 1, "{claimed:?}");
        let sensor = &claimed[0];
        let wake_at = "2026-01-01T00:00:00+00:00";
        assert!(db::park_wait(pool, &sensor.id, sensor.fence, wake_at)
            .await
            .unwrap());

        let (a, b) = tokio::join!(db::reconcile_waits(pool), db::reconcile_waits(pool));
        let resolved: Vec<(String, String)> = a
            .unwrap()
            .done
            .into_iter()
            .chain(b.unwrap().done)
            .filter(|(id, _)| *id == sensor.id)
            .collect();
        assert_eq!(resolved, vec![(sensor.id.clone(), wake_at.to_string())]);
        let status: String = sqlx::query_scalar("SELECT status FROM task_runs WHERE id = $1")
            .bind(&sensor.id)
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(status, "succeeded");

        db::cancel_run(pool, &run).await.unwrap();
    }

    /// The sub-workflow sweep reads a trigger once its child has ended, and
    /// not before: the trigger of a child still running is not resolved.
    #[tokio::test]
    async fn reconcile_subworkflows_reads_only_ended_children() {
        let Some(state) = test_state("sweep-subworkflows").await else {
            return;
        };
        let pool = &state.write_pool;
        let child_yaml = spec("  - { name: c, command: [\"true\"] }\n");
        let child_dag = DagGraph::from_yaml(&child_yaml).unwrap();
        let child = db::create_run(pool, &child_dag, &child_yaml).await.unwrap();
        let yaml = spec("  - { name: trigger, type: workflow, workflow: child }\n");
        let (run, claimed) = started(pool, &yaml).await;
        assert_eq!(claimed.len(), 1, "{claimed:?}");
        let trigger = &claimed[0];
        assert!(
            db::park_subworkflow(pool, &trigger.id, trigger.fence, &child)
                .await
                .unwrap()
        );

        let ours = |pass: db::Pass<(String, bool)>| -> Vec<(String, bool)> {
            pass.done
                .into_iter()
                .filter(|(id, _)| *id == trigger.id)
                .collect()
        };
        assert!(ours(db::reconcile_subworkflows(pool).await.unwrap()).is_empty());

        sqlx::query("UPDATE workflow_runs SET status = 'succeeded' WHERE id = $1")
            .bind(&child)
            .execute(pool)
            .await
            .unwrap();
        assert_eq!(
            ours(db::reconcile_subworkflows(pool).await.unwrap()),
            vec![(trigger.id.clone(), true)]
        );
        assert!(ours(db::reconcile_subworkflows(pool).await.unwrap()).is_empty());

        db::cancel_run(pool, &run).await.unwrap();
        // The child's one task, which nothing ran, ends with its run.
        sqlx::query("UPDATE task_runs SET status = 'cancelled' WHERE run_id = $1")
            .bind(&child)
            .execute(pool)
            .await
            .unwrap();
    }

    /// The two tiers of the gauges read statuses that do not meet: what has
    /// not finished, and the rest.
    #[tokio::test]
    async fn status_counts_live_and_history_keys_are_disjoint() {
        let Some(state) = test_state("sweep-gauges").await else {
            return;
        };
        let pool = &state.write_pool;
        let live = db::status_counts_live(pool).await.unwrap();
        let history = db::status_counts_history(pool).await.unwrap();
        for (status, _) in &live.runs_by_status {
            assert!(dagron_core::models::LIVE_RUN_STATUSES.contains(&status.as_str()));
            assert!(!history.runs_by_status.iter().any(|(s, _)| s == status));
        }
        for (status, _) in &live.tasks_by_status {
            assert!(dagron_core::models::LIVE_TASK_STATUSES.contains(&status.as_str()));
            assert!(!history.tasks_by_status.iter().any(|(s, _)| s == status));
        }
        for (status, _) in &history.runs_by_status {
            assert!(!dagron_core::models::LIVE_RUN_STATUSES.contains(&status.as_str()));
        }
        // The recent runs too: those that have not ended are read for a
        // scrape, the rest with the history.
        for row in &live.recent_runs {
            assert!(
                dagron_core::models::LIVE_RUN_STATUSES.contains(&row.status.as_str()),
                "{row:?}"
            );
        }
        for row in &history.recent_runs {
            assert!(
                !dagron_core::models::LIVE_RUN_STATUSES.contains(&row.status.as_str()),
                "{row:?}"
            );
        }

        // Two runs of a workflow of this test's own, one running and one
        // ended: one in each tier, and the two together are what one read
        // of the last day's runs gives.
        let workflow = format!("sweep-gauges-{}", uuid::Uuid::new_v4().simple());
        let yaml = format!("name: {workflow}\ntasks:\n  - {{ name: a, command: [\"true\"] }}\n");
        let (running, _) = started(pool, &yaml).await;
        let (ended, tasks) = started(pool, &yaml).await;
        sqlx::query("UPDATE task_runs SET status = 'succeeded' WHERE id = $1")
            .bind(&tasks[0].id)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("UPDATE workflow_runs SET status = 'succeeded' WHERE id = $1")
            .bind(&ended)
            .execute(pool)
            .await
            .unwrap();
        let ours = |rows: &[dagron_core::models::RecentRuns]| -> Vec<(String, i64)> {
            let mut rows: Vec<(String, i64)> = rows
                .iter()
                .filter(|r| r.workflow == workflow)
                .map(|r| (r.status.clone(), r.count))
                .collect();
            rows.sort();
            rows
        };
        let live_ours = ours(&db::status_counts_live(pool).await.unwrap().recent_runs);
        let history_ours = ours(&db::status_counts_history(pool).await.unwrap().recent_runs);
        assert_eq!(live_ours.len(), 1, "{live_ours:?}");
        assert!(
            dagron_core::models::LIVE_RUN_STATUSES.contains(&live_ours[0].0.as_str()),
            "{live_ours:?}"
        );
        assert_eq!(live_ours[0].1, 1);
        assert_eq!(history_ours, [("succeeded".to_string(), 1)]);
        let since = (chrono::Utc::now()
            - chrono::TimeDelta::seconds(dagron_core::models::RECENT_RUNS_WINDOW_SECS))
        .to_rfc3339();
        let mut one_read: Vec<(String, i64)> = sqlx::query_as(
            "SELECT wr.status, COUNT(*) FROM workflow_runs wr
             JOIN workflow_definitions wd ON wd.id = wr.definition_id
             WHERE wd.name = $1 AND wr.created_at >= $2
             GROUP BY wr.status",
        )
        .bind(&workflow)
        .bind(&since)
        .fetch_all(pool)
        .await
        .unwrap();
        one_read.sort();
        let mut both = [live_ours, history_ours].concat();
        both.sort();
        assert_eq!(both, one_read);
        assert_eq!(
            ours(&db::status_counts(pool).await.unwrap().recent_runs),
            one_read
        );
        db::cancel_run(pool, &running).await.unwrap();

        // What `GET /api/metrics` reads of each: the counts by status (and
        // the dead letters) in the same statuses, and nothing more. The
        // other tests change rows meanwhile, so the counts are not compared.
        let unfinished = db::status_counts_unfinished(pool).await.unwrap();
        for (status, _) in &unfinished.runs_by_status {
            assert!(dagron_core::models::LIVE_RUN_STATUSES.contains(&status.as_str()));
        }
        for (status, _) in &unfinished.tasks_by_status {
            assert!(dagron_core::models::LIVE_TASK_STATUSES.contains(&status.as_str()));
        }
        assert!(unfinished.active_tasks.is_empty() && unfinished.ready_by_class.is_empty());
        let finished = db::status_counts_finished(pool).await.unwrap();
        for (status, _) in &finished.runs_by_status {
            assert!(!dagron_core::models::LIVE_RUN_STATUSES.contains(&status.as_str()));
        }
        for (status, _) in &finished.tasks_by_status {
            assert!(!dagron_core::models::LIVE_TASK_STATUSES.contains(&status.as_str()));
        }
        assert!(finished.recent_runs.is_empty());
    }

    fn viewer() -> AuthUser {
        AuthUser(crate::auth::SessionClaims {
            sub: "u1".to_string(),
            email: "u1@example.com".to_string(),
            name: "U1".to_string(),
            groups: vec![],
            exp: 0,
        })
    }

    /// Fill `state`'s empty cache with counts no database holds.
    async fn poison(state: &AppState) {
        state
            .metrics_cache
            .read(
                || async {
                    Ok(dagron_core::models::MetricsSnapshot {
                        runs_by_status: vec![("running".to_string(), -2)],
                        dead_letters: -7,
                        ..Default::default()
                    })
                },
                || async {
                    Ok(dagron_core::models::MetricsHistory {
                        runs_by_status: vec![("succeeded".to_string(), -3)],
                        ..Default::default()
                    })
                },
            )
            .await
            .unwrap();
    }

    /// `GET /api/metrics` reads through its cache, both tiers in one answer,
    /// with how old the history is; with both TTLs zero it reads the
    /// database every time.
    #[tokio::test]
    async fn the_metrics_route_reads_its_two_tiers_through_its_cache() {
        let Some(mut state) = test_state("sweep-api-metrics").await else {
            return;
        };
        let hour = std::time::Duration::from_secs(3600);
        state.metrics_cache = std::sync::Arc::new(dagron_core::gauges::GaugeCache::new(hour, hour));
        poison(&state).await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let Json(m) = metrics(viewer(), State(state.clone())).await.unwrap();
        assert_eq!(m.dead_letters, -7);
        let count = |rows: &[StatusCount], status: &str| {
            rows.iter().find(|r| r.status == status).map(|r| r.count)
        };
        assert_eq!(count(&m.runs_by_status, "running"), Some(-2));
        assert_eq!(count(&m.runs_by_status, "succeeded"), Some(-3));
        assert!(
            (0.02..60.0).contains(&m.history_age_secs),
            "the history was read 20 ms before, and is that old: {}",
            m.history_age_secs
        );

        state.metrics_cache = std::sync::Arc::new(dagron_core::gauges::GaugeCache::new(
            std::time::Duration::ZERO,
            std::time::Duration::ZERO,
        ));
        poison(&state).await;
        let Json(m) = metrics(viewer(), State(state.clone())).await.unwrap();
        assert!(
            m.dead_letters >= 0,
            "nothing is cached with the TTLs at zero"
        );
        assert!(m.runs_by_status.iter().all(|r| r.count >= 0));
        assert_eq!(m.history_age_secs, 0.0);
    }

    /// `DAGRON_METRICS_CACHE_SECS=0` alone turns both tiers off;
    /// `DAGRON_METRICS_HISTORY_CACHE_SECS` set is its own.
    #[test]
    fn a_zero_metrics_cache_secs_alone_turns_the_history_cache_off_too() {
        use std::time::Duration;
        let zero = || Some("0".to_string());
        assert_eq!(
            metrics_cache_ttls(zero(), None),
            (Duration::ZERO, Duration::ZERO)
        );
        assert_eq!(
            metrics_cache_ttls(zero(), Some("300".into())),
            (Duration::ZERO, Duration::from_secs(300))
        );
        assert_eq!(
            metrics_cache_ttls(Some("10".into()), None),
            (Duration::from_secs(10), METRICS_HISTORY_CACHE_TTL)
        );
        assert_eq!(
            metrics_cache_ttls(None, None),
            (METRICS_CACHE_TTL, METRICS_HISTORY_CACHE_TTL)
        );
    }
}
