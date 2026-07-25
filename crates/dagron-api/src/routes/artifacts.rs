//! Programmatic artifact store API (the live G-C2 path): PUT/GET/exists artifacts
//! by `(run_id, task, name)`.
//!
//! Bytes are **envelope-encrypted at rest** when a KEK provider is configured —
//! transparently, via `dagron_artifact::store_from_env` (see `AppState`). This is
//! the sealed checkpoint/output channel a confidential task uses to persist state
//! the operator cannot read. Disabled (503) when `DAGRON_ARTIFACT_DIR` is unset.

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures::TryStreamExt;
use serde_json::json;
use tokio_util::io::{ReaderStream, StreamReader};

use dagron_artifact::{ArtifactKey, ArtifactStore};

use crate::auth::AuthUser;
use crate::state::AppState;

/// The configured store, or a 503 when artifacts are off.
fn store(state: &AppState) -> Result<&dyn ArtifactStore, (StatusCode, String)> {
    state.artifact_store.as_deref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "artifact store not configured (set DAGRON_ARTIFACT_DIR)".to_string(),
    ))
}

/// Log the real store error server-side and return a generic 500 — store errors
/// can carry filesystem paths / internals that must not reach the client.
fn internal(what: &str, e: impl std::fmt::Display) -> (StatusCode, String) {
    tracing::error!(error = %e, "{what}");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string())
}

/// True when a store error is an underlying filesystem "not found" (→ 404, not
/// 500) — works through the `EncryptedStore` decorator since it propagates the
/// inner error unchanged.
fn is_not_found(e: &anyhow::Error) -> bool {
    e.downcast_ref::<std::io::Error>()
        .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound)
}

/// `PUT /api/runs/{run_id}/artifacts/{task}/{name}` — store the request body under
/// the key (encrypted at rest when a KEK provider is configured).
pub async fn put_artifact(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path((run_id, task, name)): Path<(String, String, String)>,
    body: Body,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    let s = store(&state)?;
    let key = ArtifactKey::new(run_id, task, name);
    // Stream the request body straight into the store (encrypted per-chunk when a
    // KEK provider is configured) — the artifact is never fully buffered in memory.
    // The body size is still bounded by the route's RequestBodyLimitLayer.
    let reader = StreamReader::new(
        body.into_data_stream()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
    );
    let loc = s
        .put_stream(&key, Box::new(reader))
        .await
        .map_err(|e| internal("artifact put failed", e))?;
    Ok((StatusCode::CREATED, loc))
}

/// `GET /api/runs/{run_id}/artifacts/{task}/{name}` — stream the (decrypted) bytes.
///
/// The response body streams straight from the store's decrypting reader, so a
/// large streamed artifact never fully buffers server-side. A missing artifact is
/// a clean 404 (detected before any bytes are sent); any other error before the
/// first byte is a logged, generic 500. A decryption/IO error *mid-stream*
/// (tamper/truncation) aborts the connection rather than a clean status — a
/// necessary consequence of streaming.
pub async fn get_artifact(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path((run_id, task, name)): Path<(String, String, String)>,
) -> Result<Response, (StatusCode, String)> {
    let s = store(&state)?;
    let key = ArtifactKey::new(run_id, task, name);
    // Open the reader first (not exists()-then-open, which races): a missing
    // artifact is a clean 404, any other open error a logged 500.
    let reader = match s.get_stream(&key).await {
        Ok(r) => r,
        Err(e) if is_not_found(&e) => {
            return Err((StatusCode::NOT_FOUND, "artifact not found".to_string()))
        }
        Err(e) => return Err(internal("artifact get failed", e)),
    };
    let body = Body::from_stream(ReaderStream::new(reader));
    Ok(([(header::CONTENT_TYPE, "application/octet-stream")], body).into_response())
}

/// `POST /api/artifacts/rotate` (admin) — re-key every stored artifact from the
/// previous KEK (`*_OLD` env) to the current one. Rewraps the per-object data key
/// only (no payload re-encryption). Returns `{ "rotated": N }`.
pub async fn rotate_artifacts(
    AuthUser(claims): AuthUser,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if !claims.groups.iter().any(|g| g == "admin") {
        return Err((StatusCode::FORBIDDEN, "admin group required".to_string()));
    }
    let s = store(&state)?;
    // Single-flight: refuse a second rotation while one is running — overlapping
    // sweeps widen the write-vs-rotate window and double the work.
    let _guard = state.rotation_lock.try_lock().map_err(|_| {
        (StatusCode::CONFLICT, "a key rotation is already in progress".to_string())
    })?;
    // Reuse the already-built store rather than re-resolving it (which would
    // reconstruct the KEK provider — a network round-trip for a KMS backend). Only
    // the retiring KEK is read here, from the `*_OLD` env vars.
    let old = dagron_crypto::old_provider_from_env()
        .map_err(|e| internal("resolving previous KEK (DAGRON_ENV_KEK_PROVIDER_OLD)", e))?
        .ok_or((
            StatusCode::BAD_REQUEST,
            "no previous KEK configured (set DAGRON_ENV_KEK_PROVIDER_OLD and its *_OLD vars)"
                .to_string(),
        ))?;
    let rotated = s
        .rotate_from(old.as_ref())
        .await
        .map_err(|e| internal("artifact key rotation failed", e))?;
    Ok(Json(json!({ "rotated": rotated })))
}

/// `GET /api/runs/{run_id}/artifacts/{task}/{name}/exists` — presence check.
pub async fn artifact_exists(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path((run_id, task, name)): Path<(String, String, String)>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let s = store(&state)?;
    let key = ArtifactKey::new(run_id, task, name);
    let exists = s.exists(&key).await.map_err(|e| internal("artifact exists check failed", e))?;
    Ok(Json(json!({ "exists": exists })))
}
