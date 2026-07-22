//! Archived-run history reads (ee/STATE_STORE.md hot/cold split).
//!
//! Once the archive-before-purge GC moves a terminal run out of the hot store,
//! it lives on as a `run-<id>.json` document in the archive sink, mapped by the
//! engine's `archived_runs` index table. These endpoints serve that history:
//!
//! * `GET /api/archive/runs` — list from the index (no sink round-trips).
//! * `GET /api/archive/runs/{id}` — fetch the run's full document from the
//!   sink (`GC_ARCHIVE_DIR` or, with the `archive-s3` feature,
//!   `GC_ARCHIVE_URL=s3://…` — the same env contract as the engine).
//!
//! A run that `dagron archive-compact` already folded into the Parquet dataset
//! has no per-run document any more: the detail endpoint answers **410 Gone**
//! with the part-file path, pointing the caller at the analytics tier instead
//! of pretending the run vanished.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::auth::AuthUser;
use crate::state::AppState;

const DEFAULT_LIMIT: i64 = 100;
const MAX_LIMIT: i64 = 500;

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct ArchivedRunSummary {
    pub run_id: String,
    pub name: String,
    pub status: String,
    pub created_at: Option<String>,
    pub finished_at: Option<String>,
    pub archived_at: String,
    pub compacted_at: Option<String>,
    pub parquet_path: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ListParams {
    /// Filter by workflow name (exact match).
    pub name: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// `GET /api/archive/runs?name=&limit=&offset=` — newest-finished-first page of
/// the archive index. Pure index read; the sink is never touched.
pub async fn list_archived_runs(
    _auth: AuthUser,
    State(state): State<AppState>,
    Query(params): Query<ListParams>,
) -> Result<Json<Vec<ArchivedRunSummary>>, StatusCode> {
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let offset = params.offset.unwrap_or(0).max(0);
    let rows = sqlx::query_as::<_, ArchivedRunSummary>(
        "SELECT run_id, name, status, created_at, finished_at, archived_at,
                compacted_at, parquet_path
         FROM archived_runs
         WHERE ($1::text IS NULL OR name = $1)
         ORDER BY finished_at DESC NULLS LAST, run_id ASC
         LIMIT $2 OFFSET $3",
    )
    .bind(&params.name)
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;
    Ok(Json(rows))
}

/// `GET /api/archive/runs/{id}` — the run's full archive document
/// (`dagron.run-archive.v1`: run + definition + tasks + outbox events), plus
/// the index row under `"index"`. 404 = never archived (or GC'd before the
/// index existed); 410 = compacted to Parquet (body carries `parquet_path`).
pub async fn get_archived_run(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    // The id becomes a sink object name — same conservative charset the engine
    // uses for run ids (uuids), so a crafted id can't traverse the sink.
    if id.is_empty() || !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
        return Err(err(StatusCode::BAD_REQUEST, json!({"error": "invalid run id"})));
    }

    let row = sqlx::query_as::<_, ArchivedRunSummary>(
        "SELECT run_id, name, status, created_at, finished_at, archived_at,
                compacted_at, parquet_path
         FROM archived_runs WHERE run_id = $1",
    )
    .bind(&id)
    .fetch_optional(&state.read_pool)
    .await
    .map_err(|e| err(internal(e), json!({"error": "internal error"})))?
    .ok_or_else(|| err(StatusCode::NOT_FOUND, json!({"error": "run not in the archive index"})))?;

    if row.compacted_at.is_some() {
        return Err(err(
            StatusCode::GONE,
            json!({
                "error": "run compacted to the parquet dataset (analytics only)",
                "run_id": row.run_id,
                "compacted_at": row.compacted_at,
                "parquet_path": row.parquet_path,
            }),
        ));
    }

    let bytes = fetch_document(&id).await.map_err(|e| {
        tracing::error!(run_id = %id, error = %e, "archive document fetch failed");
        err(
            StatusCode::BAD_GATEWAY,
            json!({"error": "archive sink unreachable or document missing", "run_id": id}),
        )
    })?;
    let mut doc: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| {
        tracing::error!(run_id = %id, error = %e, "archive document unparseable");
        err(StatusCode::BAD_GATEWAY, json!({"error": "archive document unparseable"}))
    })?;
    // Never serve arbitrary sink JSON as a run: the document must be the
    // expected format AND describe exactly the run the index sent us to — a
    // stale/mismatched object is a sink integrity problem, same 502 class.
    if doc.get("format").and_then(|v| v.as_str()) != Some("dagron.run-archive.v1")
        || doc["run"]["id"].as_str() != Some(id.as_str())
    {
        tracing::error!(run_id = %id, "archive document does not match index entry");
        return Err(err(
            StatusCode::BAD_GATEWAY,
            json!({"error": "archive document does not match index entry", "run_id": id}),
        ));
    }
    let obj = doc.as_object_mut().expect("format-checked document is an object");
    obj.insert("archived".into(), json!(true));
    obj.insert("index".into(), serde_json::to_value(&row).unwrap_or_default());
    Ok(Json(doc))
}

/// Fetch `run-<id>.json` from the configured sink. Mirrors the engine's env
/// contract: `GC_ARCHIVE_URL` (s3, feature `archive-s3`) wins over
/// `GC_ARCHIVE_DIR`; neither configured is an error (the operator enabled
/// archive GC on the engine but not here).
async fn fetch_document(id: &str) -> anyhow::Result<Vec<u8>> {
    use anyhow::Context;
    if let Ok(url) = std::env::var("GC_ARCHIVE_URL") {
        let url = url.trim().to_string();
        if !url.is_empty() {
            #[cfg(feature = "archive-cloud")]
            {
                // Scheme (s3/gs/az) dispatched by objstore::from_url — the same
                // env contract the engine's GC writes with.
                let (store, prefix) = crate::objstore::from_url(&url)?;
                let path = prefix.child(format!("run-{id}.json"));
                let get = object_store::ObjectStore::get(&store, &path).await?;
                return Ok(get.bytes().await?.to_vec());
            }
            #[cfg(not(feature = "archive-cloud"))]
            anyhow::bail!(
                "GC_ARCHIVE_URL is set but dagron-api was built without a cloud archive feature \
                 (archive-s3 / archive-gcs / archive-azure)"
            );
        }
    }
    let dir = std::env::var("GC_ARCHIVE_DIR")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .context("neither GC_ARCHIVE_URL nor GC_ARCHIVE_DIR is configured on dagron-api")?;
    let path = std::path::Path::new(dir.trim()).join(format!("run-{id}.json"));
    Ok(tokio::fs::read(&path).await.with_context(|| format!("reading {}", path.display()))?)
}

fn err(code: StatusCode, body: serde_json::Value) -> (StatusCode, Json<serde_json::Value>) {
    (code, Json(body))
}

fn internal(e: sqlx::Error) -> StatusCode {
    tracing::error!(error = ?e, "db query failed");
    StatusCode::INTERNAL_SERVER_ERROR
}
