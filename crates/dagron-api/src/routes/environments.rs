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
//! read-only middleware, writes audit-logged) for an environment and its
//! secrets: anyone who can author workflows can already read any secret a
//! task of theirs can resolve, so a stricter gate there would not reduce
//! actual reach.
//!
//! Its **policy** is another matter (`dagron_core::envpolicy`): which runs
//! may use the environment, by trust class, ref and pipeline. It is the line
//! between this installation's own runs and a run whose definition came with
//! a pull request, so only an admin writes one. An environment with no
//! policy admits `trusted` runs alone.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

use dagron_core::envpolicy::EnvironmentPolicy;

use crate::auth::AuthUser;
use crate::state::AppState;

/// Ensure the tables exist (mirrors migrations_pg/026 and 078 — dagron-api may
/// boot before the engine's migrations have run).
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
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS environment_policies (
            environment_id    TEXT PRIMARY KEY NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
            allowed_trust     TEXT NOT NULL DEFAULT '[\"trusted\"]',
            allowed_refs      TEXT,
            allowed_pipelines TEXT,
            updated_at        TEXT NOT NULL,
            updated_by        TEXT
        )",
    )
    .execute(pool)
    .await?;
    // As the engine's migration adds them: an identity for an agent's run,
    // and a protected environment's approvers.
    for column in ["allowed_identity", "approvers"] {
        sqlx::query(&format!(
            "ALTER TABLE environment_policies ADD COLUMN IF NOT EXISTS {column} TEXT"
        ))
        .execute(pool)
        .await?;
    }
    // The run-stamp column normally arrives with the engine migration; ensure
    // it here too so an api-only boot can still create runs against it. Only
    // where it is missing: `ADD COLUMN IF NOT EXISTS` takes an exclusive lock
    // on the table even when the column is there, and waiting for it behind
    // an engine's `CREATE INDEX CONCURRENTLY` of `workflow_runs` (an upgrade
    // that brings one) is a deadlock whose victim, once the build outlasts
    // deadlock_timeout, is the index build, which leaves the index INVALID.
    let missing: bool = sqlx::query_scalar(
        "SELECT to_regclass('workflow_runs') IS NOT NULL AND NOT EXISTS (
             SELECT 1 FROM pg_attribute
              WHERE attrelid = to_regclass('workflow_runs')
                AND attname = 'environment' AND NOT attisdropped)",
    )
    .fetch_one(pool)
    .await?;
    if missing {
        sqlx::query(
            "ALTER TABLE IF EXISTS workflow_runs ADD COLUMN IF NOT EXISTS environment TEXT",
        )
        .execute(pool)
        .await?;
    }
    Ok(())
}

#[derive(Debug, Serialize)]
pub struct EnvironmentView {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub variables: BTreeMap<String, String>,
    /// Names only — secret values are write-only by design.
    pub secret_names: Vec<String>,
    /// Whether the server can store secrets — true when the legacy
    /// `DAGRON_ENV_SECRET_KEY` **or** an envelope KEK provider
    /// (`DAGRON_ENV_KEK_PROVIDER`) is configured.
    pub secrets_configured: bool,
    /// Which runs may use this environment, where an admin has said. `null`
    /// is no policy: `trusted` runs alone.
    pub policy: Option<EnvironmentPolicy>,
    pub created_at: String,
    pub updated_at: String,
}

/// An environment's name: letters, digits, `_` and `-`, at most 64; or a
/// repository's, its address `<host>/<owner>/<repo>` in lower case, whose
/// secrets a workflow file of that repository reads as `secrets.NAME`
/// (dagron-ci's GitHub front-end), and which the engine gives that
/// repository's runs alone ([`dagron_core::envpolicy::is_repository_environment`]).
fn valid_name(name: &str) -> bool {
    let plain = |part: &str, dots: bool| {
        !part.is_empty()
            && part
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-') || (dots && c == '.'))
    };
    if name.contains('/') {
        // A repository's: `<host>/<owner>/<repo>`, in lower case.
        return dagron_core::envpolicy::is_repository_environment(name);
    }
    name.len() <= 64 && plain(name, false)
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
    let policy = fetch_policy(&state.read_pool, &id).await?;
    Ok(Some(EnvironmentView {
        id,
        name,
        description,
        variables: serde_json::from_str(&variables).unwrap_or_default(),
        secret_names,
        // Secret storage is available via either the legacy single key or an
        // envelope KEK provider (BYOK/KMS).
        secrets_configured: dagron_crypto::key_configured()
            || matches!(dagron_crypto::provider_from_env(), Ok(Some(_))),
        policy,
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
    AuthUser(claims): AuthUser,
    State(state): State<AppState>,
    Json(body): Json<UpsertBody>,
) -> Result<(StatusCode, Json<EnvironmentView>), (StatusCode, String)> {
    let name = body.name.as_deref().unwrap_or("").trim().to_string();
    if !valid_name(&name) {
        return Err((
            StatusCode::BAD_REQUEST,
            "environment name must be 1-64 chars of [A-Za-z0-9_-], or a repository's, \
             <host>/<owner>/<repo> in lower case"
                .to_string(),
        ));
    }
    // A repository's is the one its runs' secrets are read from, by its
    // name alone: it is an admin's to make, as a policy is.
    if name.contains('/') && !crate::auth::is_admin(&claims) {
        return Err((
            StatusCode::FORBIDDEN,
            "a repository's environment is an admin's to make: its runs read their secrets \
             from it"
                .to_string(),
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
    sqlx::query("DELETE FROM environment_policies WHERE environment_id = $1")
        .bind(&id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
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
    // Envelope-encrypt (v2) when a KEK provider is configured (BYOK/KMS); otherwise
    // fall back to the legacy single-key (v1). Reads dispatch on the stored
    // version, so enabling a provider is a safe, opt-in migration — old v1 secrets
    // keep decrypting.
    let provider = dagron_crypto::provider_from_env().map_err(|e| {
        (StatusCode::SERVICE_UNAVAILABLE, format!("KEK provider misconfigured: {e}"))
    })?;
    let exists: Option<String> = sqlx::query_scalar("SELECT id FROM environments WHERE id = $1")
        .bind(&id)
        .fetch_optional(&state.read_pool)
        .await
        .map_err(internal_msg)?;
    if exists.is_none() {
        return Err((StatusCode::NOT_FOUND, format!("environment '{id}' not found")));
    }
    let ciphertext = match &provider {
        Some(p) => dagron_crypto::encrypt_envelope(p.as_ref(), &body.value)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("envelope encrypt failed: {e}")))?,
        None => {
            let key = dagron_crypto::load_key().map_err(|e| {
                (StatusCode::SERVICE_UNAVAILABLE, format!("secret storage not configured: {e}"))
            })?;
            dagron_crypto::encrypt(&key, &body.value)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?
        }
    };
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

/// The stored policy of the environment `id`, where it has one.
async fn fetch_policy(
    pool: &sqlx::postgres::PgPool,
    id: &str,
) -> Result<Option<EnvironmentPolicy>, sqlx::Error> {
    type Row = (
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let row: Option<Row> = sqlx::query_as(
        "SELECT allowed_trust, allowed_refs, allowed_pipelines, allowed_identity, approvers
           FROM environment_policies WHERE environment_id = $1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    let Some((trust, refs, pipelines, identity, approvers)) = row else {
        return Ok(None);
    };
    // A row that does not read is shown as a policy that admits nothing, which
    // is how the engine treats it: it refuses, and never reads it as "none".
    let list = |json: &str| serde_json::from_str::<Vec<String>>(json).ok();
    Ok(Some(EnvironmentPolicy {
        allowed_trust: list(&trust).unwrap_or_default(),
        allowed_refs: refs.as_deref().map(|j| list(j).unwrap_or_default()),
        allowed_pipelines: pipelines.as_deref().map(|j| list(j).unwrap_or_default()),
        allowed_identity: identity.as_deref().and_then(list).unwrap_or_default(),
        approvers: approvers.as_deref().and_then(list).unwrap_or_default(),
    }))
}

/// Only an admin writes a policy: it is the line between this installation's
/// own runs and a run whose definition came with a pull request.
fn policy_writer(claims: &crate::auth::SessionClaims) -> Result<String, (StatusCode, String)> {
    if !crate::auth::is_admin(claims) {
        return Err((
            StatusCode::FORBIDDEN,
            "an environment's policy is an admin's to write: it says which runs may use the \
             environment"
                .to_string(),
        ));
    }
    Ok(crate::routes::control::caller_identity(claims))
}

/// `PUT /api/environments/{id}/policy` — which runs may use the environment:
/// `{"allowed_trust": ["trusted"], "allowed_refs": ["refs/heads/main"],
/// "allowed_pipelines": ["release"]}`. The whole policy is replaced. Admin
/// only. 400 for a class that is not one, or a list that is set and empty.
pub async fn put_policy(
    AuthUser(claims): AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(policy): Json<EnvironmentPolicy>,
) -> Result<Json<EnvironmentView>, (StatusCode, String)> {
    let who = policy_writer(&claims)?;
    policy
        .validate()
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let json = |l: &Vec<String>| serde_json::to_string(l).unwrap_or_else(|_| "[]".into());
    let now = chrono::Utc::now().to_rfc3339();
    // One statement: the row is written only where the environment is there,
    // so a policy never names an environment that was deleted meanwhile.
    let written = sqlx::query(
        "INSERT INTO environment_policies
                (environment_id, allowed_trust, allowed_refs, allowed_pipelines, updated_at, updated_by,
                 allowed_identity, approvers)
         SELECT id, $2, $3, $4, $5, $6, $7, $8 FROM environments WHERE id = $1
         ON CONFLICT (environment_id) DO UPDATE
            SET allowed_trust = EXCLUDED.allowed_trust,
                allowed_refs = EXCLUDED.allowed_refs,
                allowed_pipelines = EXCLUDED.allowed_pipelines,
                updated_at = EXCLUDED.updated_at,
                updated_by = EXCLUDED.updated_by,
                allowed_identity = EXCLUDED.allowed_identity,
                approvers = EXCLUDED.approvers",
    )
    .bind(&id)
    .bind(json(&policy.allowed_trust))
    .bind(policy.allowed_refs.as_ref().map(json))
    .bind(policy.allowed_pipelines.as_ref().map(json))
    .bind(&now)
    .bind(&who)
    .bind((!policy.allowed_identity.is_empty()).then(|| json(&policy.allowed_identity)))
    .bind((!policy.approvers.is_empty()).then(|| json(&policy.approvers)))
    .execute(&state.write_pool)
    .await
    .map_err(internal_msg)?
    .rows_affected();
    if written == 0 {
        return Err((
            StatusCode::NOT_FOUND,
            format!("environment '{id}' not found"),
        ));
    }
    tracing::info!(environment = %id, by = %who, ?policy, "environment policy set");
    let view = fetch_view(&state, &id)
        .await
        .map_err(internal_msg)?
        .ok_or((
            StatusCode::NOT_FOUND,
            format!("environment '{id}' not found"),
        ))?;
    Ok(Json(view))
}

/// `DELETE /api/environments/{id}/policy` — back to no policy, which admits
/// `trusted` runs alone. Admin only. 204 whether or not the environment had
/// one, and 404 for an environment that is not there: an id typed wrong is
/// not told that the right environment was reset.
pub async fn delete_policy(
    AuthUser(claims): AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let who = policy_writer(&claims)?;
    // Asked of the database that is written to: a replica may not have an
    // environment that was made a moment ago.
    let there: Option<i32> = sqlx::query_scalar("SELECT 1 FROM environments WHERE id = $1")
        .bind(&id)
        .fetch_optional(&state.write_pool)
        .await
        .map_err(internal_msg)?;
    if there.is_none() {
        return Err((
            StatusCode::NOT_FOUND,
            format!("environment '{id}' not found"),
        ));
    }
    sqlx::query("DELETE FROM environment_policies WHERE environment_id = $1")
        .bind(&id)
        .execute(&state.write_pool)
        .await
        .map_err(internal_msg)?;
    tracing::info!(environment = %id, by = %who, "environment policy removed");
    Ok(StatusCode::NO_CONTENT)
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
mod name_tests {
    use super::valid_name;

    /// An environment's name, or a repository's (`<host>/<owner>/<repo>`,
    /// in lower case), whose secrets its workflow files read.
    #[test]
    fn an_environment_is_named_plainly_or_for_a_repository() {
        for ok in [
            "prod",
            "release-1",
            "a_b",
            "github.com/acme/widgets",
            "github.com/acme/web.site",
            "git.example:3000/a-b/c_d.e",
        ] {
            assert!(valid_name(ok), "{ok}");
        }
        for bad in [
            "",
            "has space",
            "a/b",
            "github.com/acme",
            "github.com/acme/",
            "github.com/acme/a/b",
            "github.com/../x",
            "github.com/acme/..",
            "github.com/acme/.",
            "github.com/Acme/Widgets",
            "GitHub.com/acme/widgets",
            "web.site",
            &"x".repeat(65),
            &format!("github.com/acme/{}", "x".repeat(250)),
        ] {
            assert!(!valid_name(bad), "{bad:?}");
        }
    }
}
