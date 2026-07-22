//! Global search backing the ⌘K command palette.
//!
//! One capped, parameterized query per entity — built to stay cheap on large
//! installs while matching any substring on small ones:
//!
//! * every branch is `LIMIT`-capped (default 8/category, max 20), so the
//!   response is bounded no matter how big the tables are;
//! * run-id matching is **prefix-only** (`q%`), the shape a UUID lookup
//!   actually takes and one a btree/text-pattern index can serve;
//! * name/description matching is `ILIKE %q%` — full substring, which is fine
//!   on the small `workflows`/`schedules` tables at any scale, and bounded on
//!   `workflow_runs` by the newest-first `ORDER BY … LIMIT`;
//! * LIKE wildcards in the query are escaped, so a literal `%` searches `%`.

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::auth::AuthUser;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct SearchParams {
    pub q: Option<String>,
    /// Per-category result cap (default 8, clamped to [1, 20]).
    pub limit: Option<i64>,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct WorkflowHit {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct RunHit {
    pub id: String,
    pub name: Option<String>,
    pub status: String,
    pub created_at: String,
}

#[derive(Serialize, sqlx::FromRow)]
pub struct ScheduleHit {
    pub id: String,
    pub workflow_id: String,
    pub workflow_name: String,
    pub cron_expr: String,
    pub enabled: i64,
}

#[derive(Serialize)]
pub struct SearchResponse {
    pub query: String,
    pub workflows: Vec<WorkflowHit>,
    pub runs: Vec<RunHit>,
    pub schedules: Vec<ScheduleHit>,
}

/// Escape LIKE wildcards so user input matches literally (paired with
/// `ESCAPE '\'` in each pattern).
fn escape_like(q: &str) -> String {
    q.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_")
}

/// `GET /api/search?q=&limit=` — capped cross-entity search for the palette.
pub async fn search(
    _auth: AuthUser,
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
) -> Result<Json<SearchResponse>, StatusCode> {
    let q = params.q.unwrap_or_default().trim().to_string();
    if q.is_empty() {
        return Ok(Json(SearchResponse {
            query: q,
            workflows: vec![],
            runs: vec![],
            schedules: vec![],
        }));
    }
    let limit = params.limit.unwrap_or(8).clamp(1, 20);
    let sub = format!("%{}%", escape_like(&q));
    let prefix = format!("{}%", escape_like(&q));

    let workflows = sqlx::query_as::<_, WorkflowHit>(
        "SELECT id, name, description FROM workflows
         WHERE name ILIKE $1 ESCAPE '\\' OR COALESCE(description,'') ILIKE $1 ESCAPE '\\'
         ORDER BY name LIMIT $2",
    )
    .bind(&sub)
    .bind(limit)
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;

    let runs = sqlx::query_as::<_, RunHit>(
        "SELECT wr.id, d.name, wr.status, wr.created_at
         FROM workflow_runs wr
         LEFT JOIN workflow_definitions d ON d.id = wr.definition_id
         WHERE wr.id ILIKE $1 ESCAPE '\\' OR d.name ILIKE $2 ESCAPE '\\'
         ORDER BY wr.created_at DESC LIMIT $3",
    )
    .bind(&prefix)
    .bind(&sub)
    .bind(limit)
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;

    let schedules = sqlx::query_as::<_, ScheduleHit>(
        "SELECT s.id, s.workflow_id, w.name AS workflow_name, s.cron_expr, s.enabled
         FROM schedules s JOIN workflows w ON w.id = s.workflow_id
         WHERE w.name ILIKE $1 ESCAPE '\\' OR s.cron_expr ILIKE $1 ESCAPE '\\'
         ORDER BY w.name LIMIT $2",
    )
    .bind(&sub)
    .bind(limit)
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;

    Ok(Json(SearchResponse { query: q, workflows, runs, schedules }))
}

fn internal(err: sqlx::Error) -> StatusCode {
    tracing::error!(error = ?err, "db query failed");
    StatusCode::INTERNAL_SERVER_ERROR
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn like_wildcards_are_escaped() {
        assert_eq!(escape_like("50%_done\\x"), "50\\%\\_done\\\\x");
        assert_eq!(escape_like("plain"), "plain");
    }
}
