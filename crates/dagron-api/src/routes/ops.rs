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
use crate::routes::control;
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

#[derive(Serialize, sqlx::FromRow)]
pub struct PendingApproval {
    pub run_id: String,
    pub task_id: String,
    pub task_name: String,
    pub workflow_name: Option<String>,
    /// When the gate parked (task scheduled_at), oldest first.
    pub since: Option<String>,
}

/// `GET /api/approvals` — every task parked in `awaiting_approval`, oldest
/// first: the human-in-the-loop worklist behind the sidebar badge.
pub async fn list_approvals(
    _auth: AuthUser,
    State(state): State<AppState>,
) -> Result<Json<Vec<PendingApproval>>, StatusCode> {
    let rows = sqlx::query_as::<_, PendingApproval>(
        "SELECT t.run_id, t.id AS task_id, t.name AS task_name,
                d.name AS workflow_name, t.scheduled_at AS since
         FROM task_runs t
         JOIN workflow_runs wr ON wr.id = t.run_id
         LEFT JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE t.status = 'awaiting_approval'
         ORDER BY t.scheduled_at ASC NULLS FIRST",
    )
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;
    Ok(Json(rows))
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
    async fn test_state(app: &str) -> Option<AppState> {
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
