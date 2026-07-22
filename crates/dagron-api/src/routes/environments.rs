//! Environments: named variable sets + write-only encrypted secrets.
//!
//! An environment is referenced from a workflow spec via `environment: <name>`:
//! its `variables` become `{{ env.NAME }}` template references at run creation
//! and its secrets resolve `value_from: {secret: NAME}` at dispatch. Secret
//! values are **write-only**: stored AES-256-GCM-encrypted (dagron-crypto,
//! `DAGRON_ENV_SECRET_KEY` shared with the engine) and never returned by any
//! endpoint — reads list secret *names* only.
//!
//! Authorization is the standard operator level (viewers blocked by the
//! read-only middleware, writes audit-logged): anyone who can author
//! workflows can already exfiltrate any secret a task can resolve, so a
//! stricter gate here would not reduce actual reach.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::auth::AuthUser;
use crate::state::AppState;

/// Ensure the tables exist (mirrors migrations_pg/026 — dagron-api may boot
/// before the engine's migrations have run).
pub async fn ensure_schema(pool: &sqlx::postgres::PgPool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS environments (
            id          TEXT PRIMARY KEY NOT NULL,
            name        TEXT NOT NULL UNIQUE,
            description TEXT,
            variables   TEXT NOT NULL DEFAULT '{}',
            created_at  TEXT NOT NULL,
            updated_at  TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS environment_secrets (
            environment_id TEXT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
            name           TEXT NOT NULL,
            ciphertext     TEXT NOT NULL,
            updated_at     TEXT NOT NULL,
            PRIMARY KEY (environment_id, name)
        )",
    )
    .execute(pool)
    .await?;
    // The run-stamp column normally arrives with the engine migration; ensure
    // it here too so an api-only boot can still create runs against it.
    sqlx::query("ALTER TABLE IF EXISTS workflow_runs ADD COLUMN IF NOT EXISTS environment TEXT")
        .execute(pool)
        .await?;
    Ok(())
}

#[derive(Serialize)]
pub struct EnvironmentView {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub variables: BTreeMap<String, String>,
    /// Names only — secret values are write-only by design.
    pub secret_names: Vec<String>,
    /// Whether the server can store secrets (DAGRON_ENV_SECRET_KEY configured).
    pub secrets_configured: bool,
    pub created_at: String,
    pub updated_at: String,
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
}

/// Variable/secret names become `{{ env.NAME }}` keys and env-var names, so
/// keep them identifier-shaped.
fn valid_var_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

async fn fetch_view(state: &AppState, id: &str) -> Result<Option<EnvironmentView>, sqlx::Error> {
    let row: Option<(String, String, Option<String>, String, String, String)> = sqlx::query_as(
        "SELECT id, name, description, variables, created_at, updated_at
         FROM environments WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.read_pool)
    .await?;
    let Some((id, name, description, variables, created_at, updated_at)) = row else {
        return Ok(None);
    };
    let secret_names: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM environment_secrets WHERE environment_id = $1 ORDER BY name",
    )
    .bind(&id)
    .fetch_all(&state.read_pool)
    .await?;
    Ok(Some(EnvironmentView {
        id,
        name,
        description,
        variables: serde_json::from_str(&variables).unwrap_or_default(),
        secret_names,
        secrets_configured: dagron_crypto::key_configured(),
        created_at,
        updated_at,
    }))
}

/// `GET /api/environments` — every environment (variables + secret names).
pub async fn list(
    _auth: AuthUser,
    State(state): State<AppState>,
) -> Result<Json<Vec<EnvironmentView>>, StatusCode> {
    let ids: Vec<String> = sqlx::query_scalar("SELECT id FROM environments ORDER BY name")
        .fetch_all(&state.read_pool)
        .await
        .map_err(internal)?;
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(v) = fetch_view(&state, &id).await.map_err(internal)? {
            out.push(v);
        }
    }
    Ok(Json(out))
}

#[derive(Deserialize)]
pub struct UpsertBody {
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub variables: Option<BTreeMap<String, String>>,
}

fn validate_vars(vars: &BTreeMap<String, String>) -> Result<(), (StatusCode, String)> {
    for k in vars.keys() {
        if !valid_var_name(k) {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("invalid variable name '{k}' (use [A-Za-z0-9_], max 128 chars)"),
            ));
        }
    }
    Ok(())
}

/// `POST /api/environments` — create. 409 on duplicate name.
pub async fn create(
    _auth: AuthUser,
    State(state): State<AppState>,
    Json(body): Json<UpsertBody>,
) -> Result<(StatusCode, Json<EnvironmentView>), (StatusCode, String)> {
    let name = body.name.as_deref().unwrap_or("").trim().to_string();
    if !valid_name(&name) {
        return Err((
            StatusCode::BAD_REQUEST,
            "environment name must be 1-64 chars of [A-Za-z0-9_-]".to_string(),
        ));
    }
    let vars = body.variables.unwrap_or_default();
    validate_vars(&vars)?;
    let id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let res = sqlx::query(
        "INSERT INTO environments (id, name, description, variables, created_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$5) ON CONFLICT (name) DO NOTHING",
    )
    .bind(&id)
    .bind(&name)
    .bind(&body.description)
    .bind(serde_json::to_string(&vars).unwrap_or_else(|_| "{}".into()))
    .bind(&now)
    .execute(&state.write_pool)
    .await
    .map_err(internal_msg)?;
    if res.rows_affected() == 0 {
        return Err((StatusCode::CONFLICT, format!("environment '{name}' already exists")));
    }
    let view = fetch_view(&state, &id)
        .await
        .map_err(internal_msg)?
        .ok_or((StatusCode::INTERNAL_SERVER_ERROR, "environment vanished".to_string()))?;
    Ok((StatusCode::CREATED, Json(view)))
}

/// `PUT /api/environments/:id` — update description and/or variables (a
/// present `variables` replaces the whole map). The name is immutable —
/// workflow specs reference it.
pub async fn update(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<UpsertBody>,
) -> Result<Json<EnvironmentView>, (StatusCode, String)> {
    if let Some(vars) = &body.variables {
        validate_vars(vars)?;
    }
    let now = chrono::Utc::now().to_rfc3339();
    let res = sqlx::query(
        "UPDATE environments
         SET description = COALESCE($2, description),
             variables = COALESCE($3, variables),
             updated_at = $4
         WHERE id = $1",
    )
    .bind(&id)
    .bind(&body.description)
    .bind(body.variables.map(|v| serde_json::to_string(&v).unwrap_or_else(|_| "{}".into())))
    .bind(&now)
    .execute(&state.write_pool)
    .await
    .map_err(internal_msg)?;
    if res.rows_affected() == 0 {
        return Err((StatusCode::NOT_FOUND, format!("environment '{id}' not found")));
    }
    let view = fetch_view(&state, &id)
        .await
        .map_err(internal_msg)?
        .ok_or((StatusCode::NOT_FOUND, format!("environment '{id}' not found")))?;
    Ok(Json(view))
}

/// `DELETE /api/environments/:id` — remove the environment and its secrets.
/// Runs already created keep working (their params were resolved at creation);
/// future runs of specs naming it will fail loudly at submit.
pub async fn delete(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    // One transaction so a concurrent secret PUT can't land between the two
    // deletes (which would fail the parent delete on its FK and strand a
    // half-deleted environment). The explicit secrets delete also covers
    // tables created before the FK gained ON DELETE CASCADE.
    let mut tx = state.write_pool.begin().await.map_err(internal)?;
    sqlx::query("DELETE FROM environment_secrets WHERE environment_id = $1")
        .bind(&id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
    let n = sqlx::query("DELETE FROM environments WHERE id = $1")
        .bind(&id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?
        .rows_affected();
    tx.commit().await.map_err(internal)?;
    if n == 0 { Err(StatusCode::NOT_FOUND) } else { Ok(StatusCode::NO_CONTENT) }
}

#[derive(Deserialize)]
pub struct SecretBody {
    pub value: String,
}

/// `PUT /api/environments/:id/secrets/:name` — set (upsert) a secret value.
/// Write-only: the value is encrypted immediately and never readable back.
/// 503 when no `DAGRON_ENV_SECRET_KEY` is configured — storing plaintext is
/// not an acceptable fallback.
pub async fn put_secret(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path((id, name)): Path<(String, String)>,
    Json(body): Json<SecretBody>,
) -> Result<StatusCode, (StatusCode, String)> {
    if !valid_var_name(&name) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("invalid secret name '{name}' (use [A-Za-z0-9_], max 128 chars)"),
        ));
    }
    if body.value.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "secret value must not be empty".to_string()));
    }
    let key = dagron_crypto::load_key().map_err(|e| {
        (StatusCode::SERVICE_UNAVAILABLE, format!("secret storage not configured: {e}"))
    })?;
    let exists: Option<String> = sqlx::query_scalar("SELECT id FROM environments WHERE id = $1")
        .bind(&id)
        .fetch_optional(&state.read_pool)
        .await
        .map_err(internal_msg)?;
    if exists.is_none() {
        return Err((StatusCode::NOT_FOUND, format!("environment '{id}' not found")));
    }
    let ciphertext = dagron_crypto::encrypt(&key, &body.value)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO environment_secrets (environment_id, name, ciphertext, updated_at)
         VALUES ($1,$2,$3,$4)
         ON CONFLICT (environment_id, name) DO UPDATE SET ciphertext = $3, updated_at = $4",
    )
    .bind(&id)
    .bind(&name)
    .bind(&ciphertext)
    .bind(&now)
    .execute(&state.write_pool)
    .await
    .map_err(internal_msg)?;
    tracing::info!(environment = %id, secret = %name, "environment secret set");
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /api/environments/:id/secrets/:name`.
pub async fn delete_secret(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path((id, name)): Path<(String, String)>,
) -> Result<StatusCode, StatusCode> {
    let n = sqlx::query(
        "DELETE FROM environment_secrets WHERE environment_id = $1 AND name = $2",
    )
    .bind(&id)
    .bind(&name)
    .execute(&state.write_pool)
    .await
    .map_err(internal)?
    .rows_affected();
    if n == 0 { Err(StatusCode::NOT_FOUND) } else { Ok(StatusCode::NO_CONTENT) }
}

fn internal(err: sqlx::Error) -> StatusCode {
    tracing::error!(error = ?err, "db query failed");
    StatusCode::INTERNAL_SERVER_ERROR
}

fn internal_msg(err: sqlx::Error) -> (StatusCode, String) {
    tracing::error!(error = ?err, "db query failed");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string())
}
