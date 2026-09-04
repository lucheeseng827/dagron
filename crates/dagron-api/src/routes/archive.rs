//! Archived-run history reads (the hot/cold split).
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
/// Bounds `run-<id>.json` well under the 255-byte NAME_MAX every
/// filesystem we archive to enforces. Run ids are uuids today; this is
/// only here so a hand-made id fails as a 400 and not as a sink error.
const MAX_RUN_ID_LEN: usize = 128;

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
    // uses for run ids (uuids), so a crafted id can't traverse the sink, and
    // the same length bound as the write path so one rule describes the name in
    // both directions. (Here it is belt-and-braces: the index lookup below
    // answers 404 long before the sink is asked for anything.)
    if id.is_empty()
        || id.len() > MAX_RUN_ID_LEN
        || !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
    {
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

/// `POST /api/runs/{id}/archive` — archive one terminal run **now**, instead of
/// waiting for the retention window to reach it.
///
/// This is the same archive-before-purge the GC performs, aimed at one run:
/// export the document, verify it landed, index it, then delete the run from
/// the hot store. It is therefore **destructive** — the run leaves `/api/runs`
/// and reappears under `/api/archive/runs` — and the order matters as much
/// here as it does in the sweep. Write, then index, then purge: a run purged
/// before its index row exists is history nothing can list.
///
/// Admin-only. The retention window is an instance-wide policy set by an
/// operator; letting any signed-in user pull individual runs out of the hot
/// store ahead of it is the same authority, exercised one run at a time.
///
/// Refusals are specific on purpose, because each has a different fix:
/// * `400` — malformed id.
/// * `403` — not an admin.
/// * `404` — no such run in the hot store (already archived, or never existed).
/// * `409` — the run is not terminal. Archiving a live run would purge state
///   the scheduler is still driving.
/// * `501` — no sink configured on dagron-api. Without one there is nothing to
///   archive *to*, and proceeding would be a delete wearing a kinder word.
/// * `502` — the sink refused the write. The run stays in the hot store.
pub async fn archive_run(
    AuthUser(claims): AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    if !claims.groups.iter().any(|g| g == "admin") {
        return Err(err(StatusCode::FORBIDDEN, json!({"error": "admin group required"})));
    }
    // The id becomes a sink object name — same conservative charset as the read
    // path, so a crafted id cannot traverse the sink. The length bound earns its
    // place on this route: nothing here has consulted the archive index, so an
    // over-long id reaches the sink and fails the write with ENAMETOOLONG,
    // answering 502 "archive sink unreachable" for what is plainly a bad
    // request.
    if id.is_empty()
        || id.len() > MAX_RUN_ID_LEN
        || !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
    {
        return Err(err(StatusCode::BAD_REQUEST, json!({"error": "invalid run id"})));
    }
    if !sink_configured() {
        return Err(err(
            StatusCode::NOT_IMPLEMENTED,
            json!({
                "error": "no archive sink configured on dagron-api \
                          (set GC_ARCHIVE_DIR, or GC_ARCHIVE_URL with a cloud archive feature)"
            }),
        ));
    }

    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM workflow_runs WHERE id = $1")
            .bind(&id)
            .fetch_optional(&state.write_pool)
            .await
            .map_err(|e| err(internal(e), json!({"error": "internal error"})))?;
    let Some(status) = status else {
        return Err(err(StatusCode::NOT_FOUND, json!({"error": "run not found in the hot store"})));
    };
    if !matches!(status.as_str(), "succeeded" | "failed" | "cancelled") {
        return Err(err(
            StatusCode::CONFLICT,
            json!({"error": "run is not terminal", "status": status}),
        ));
    }

    let doc = dagron_core::db::archive_doc_for_run(&state.write_pool, &id)
        .await
        .map_err(|e| {
            tracing::error!(run_id = %id, error = %e, "archive document build failed");
            err(StatusCode::INTERNAL_SERVER_ERROR, json!({"error": "internal error"}))
        })?
        .ok_or_else(|| {
            err(StatusCode::NOT_FOUND, json!({"error": "run not found in the hot store"}))
        })?;

    // Fail closed: no index row and no purge unless the document verifiably
    // landed, exactly as the sweep does.
    store_document(&id, &doc).await.map_err(|e| {
        tracing::error!(run_id = %id, error = %e, "archive write failed — run kept in hot store");
        err(
            StatusCode::BAD_GATEWAY,
            json!({"error": "archive sink write failed — run kept in the hot store", "run_id": id}),
        )
    })?;

    let run = &doc["run"];
    dagron_core::db::index_archived_run(
        &state.write_pool,
        &id,
        run["definition_name"].as_str().unwrap_or(""),
        run["status"].as_str().unwrap_or("unknown"),
        run["created_at"].as_str(),
        run["finished_at"].as_str(),
    )
    .await
    .map_err(|e| {
        // 500, not 502: this is our own index write on `write_pool`. The 502
        // above means the archive sink is unreachable, which is a different
        // page for whoever is holding it.
        tracing::error!(run_id = %id, error = %e, "archive index write failed — run kept in hot store");
        err(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error": "archive index write failed — run kept in the hot store", "run_id": id}),
        )
    })?;

    let purged = dagron_core::db::purge_runs_by_id(&state.write_pool, std::slice::from_ref(&id))
        .await
        .map_err(|e| {
            // Archived and indexed but not purged: the run is listable in both
            // places until the next sweep reaches it, which is the harmless
            // direction to fail in.
            tracing::error!(run_id = %id, error = %e, "archive purge failed after a durable write");
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({"error": "archived, but purging from the hot store failed", "run_id": id}),
            )
        })?;

    tracing::info!(run_id = %id, by = %claims.email, purged, "run archived on request");
    Ok(Json(json!({"run_id": id, "archived": true, "purged": purged})))
}

/// True when this process can write to an archive sink — the same env contract
/// the reads use, asked before anything is mutated.
fn sink_configured() -> bool {
    let set = |k: &str| std::env::var(k).ok().is_some_and(|v| !v.trim().is_empty());
    set("GC_ARCHIVE_URL") || set("GC_ARCHIVE_DIR")
}

/// Durably write `run-<id>.json` to the configured sink. The mirror of
/// [`fetch_document`], and the same precedence: `GC_ARCHIVE_URL` over
/// `GC_ARCHIVE_DIR`.
///
/// Returning `Ok` is purge permission, so the local path uses the engine's own
/// fsync chain (`dagron_core::archive`) rather than a second copy of it, and
/// the object path relies on a completed PUT being atomic.
async fn store_document(id: &str, doc: &serde_json::Value) -> anyhow::Result<()> {
    use anyhow::Context;
    if let Ok(url) = std::env::var("GC_ARCHIVE_URL") {
        let url = url.trim().to_string();
        if !url.is_empty() {
            #[cfg(feature = "archive-cloud")]
            {
                let (store, prefix) = crate::objstore::from_url(&url)?;
                let path = prefix.child(dagron_core::archive::document_name(id));
                let bytes = serde_json::to_vec(doc)?;
                object_store::ObjectStore::put(
                    &store,
                    &path,
                    object_store::PutPayload::from(bytes),
                )
                .await?;
                return Ok(());
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
    let dir = std::path::PathBuf::from(dir.trim());
    let (id, doc) = (id.to_string(), doc.clone());
    // The fsync chain is blocking; keep it off the async worker.
    tokio::task::spawn_blocking(move || dagron_core::archive::write_document(&dir, &id, &doc))
        .await??;
    Ok(())
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
