//! Managing remote runners: `/api/runners`.
//!
//! An admin mints a join token, which names the labels the runners it enrols
//! may serve and how many it may enrol; a runner registers with it once
//! (`POST /runner/v1/register`). Minting one takes a signed-in session, as
//! minting an API token does: a token that could mint credentials would
//! outlive its own revocation.
//!
//! An admin can drain a runner, whose live sessions then take nothing more
//! and end once what they run is over, so that it can be stopped or upgraded
//! without losing a task; or revoke it, whose credential is refused from
//! then on, and whose sessions end at once, their tasks taken back by the
//! engine's session sweep.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use dagron_core::runner as proto;

use crate::auth::{AuthUser, SessionAuth, SessionClaims};
use crate::state::AppState;

type ApiError = (StatusCode, Json<Value>);

fn err(status: StatusCode, msg: impl Into<String>) -> ApiError {
    (status, Json(json!({ "error": msg.into() })))
}

fn internal(e: sqlx::Error) -> ApiError {
    tracing::error!(error = %e, "runners: a database call failed");
    err(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
}

fn require_admin(claims: &SessionClaims) -> Result<(), ApiError> {
    if crate::auth::is_admin(claims) {
        Ok(())
    } else {
        Err(err(StatusCode::FORBIDDEN, "admin group required"))
    }
}

/// A join token lives a day unless the admin says otherwise, and never more
/// than a month: it is a provisioning credential.
const JOIN_TOKEN_DEFAULT_TTL_SECS: i64 = 24 * 3600;
const JOIN_TOKEN_MAX_TTL_SECS: i64 = 30 * 24 * 3600;
const JOIN_TOKEN_MAX_USES: i32 = 1000;

#[derive(Debug, Deserialize)]
pub struct NewJoinToken {
    /// The labels the runners it enrols may serve: at least one.
    pub labels: Vec<String>,
    /// How many runners it may enrol. 1 unless said.
    #[serde(default)]
    pub max_uses: Option<i32>,
    #[serde(default)]
    pub ttl_secs: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct JoinTokenView {
    pub id: String,
    pub labels: Vec<String>,
    pub max_uses: i32,
    pub uses: i32,
    pub created_by: String,
    pub created_at: String,
    pub expires_at: String,
    pub revoked_at: Option<String>,
    /// The token itself, in the answer to its minting alone.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

/// `POST /api/runners/join-tokens` (admin, signed in): a join token, shown
/// this once.
pub async fn create_join_token(
    SessionAuth(claims): SessionAuth,
    State(state): State<AppState>,
    Json(body): Json<NewJoinToken>,
) -> Result<(StatusCode, Json<JoinTokenView>), ApiError> {
    require_admin(&claims)?;
    if body.labels.is_empty() {
        return Err(err(
            StatusCode::BAD_REQUEST,
            "a join token names the labels its runners may serve",
        ));
    }
    let labels =
        super::valid_labels(&body.labels, &[]).map_err(|e| err(StatusCode::BAD_REQUEST, e))?;
    let max_uses = body.max_uses.unwrap_or(1);
    if !(1..=JOIN_TOKEN_MAX_USES).contains(&max_uses) {
        return Err(err(
            StatusCode::BAD_REQUEST,
            format!("max_uses is 1 to {JOIN_TOKEN_MAX_USES}"),
        ));
    }
    let ttl = body.ttl_secs.unwrap_or(JOIN_TOKEN_DEFAULT_TTL_SECS);
    if !(60..=JOIN_TOKEN_MAX_TTL_SECS).contains(&ttl) {
        return Err(err(
            StatusCode::BAD_REQUEST,
            format!("ttl_secs is 60 to {JOIN_TOKEN_MAX_TTL_SECS}"),
        ));
    }
    let now_dt = chrono::Utc::now();
    let id = uuid::Uuid::new_v4().to_string();
    let (token, prefix, hash) = super::mint(proto::JOIN_TOKEN_PREFIX);
    let created_by = if claims.email.is_empty() {
        claims.sub.clone()
    } else {
        claims.email.clone()
    };
    let created_at = now_dt.to_rfc3339();
    let expires_at = (now_dt + chrono::TimeDelta::seconds(ttl)).to_rfc3339();
    sqlx::query(
        "INSERT INTO runner_join_tokens
             (id, prefix, token_hash, labels, max_uses, created_by, created_at, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(&id)
    .bind(&prefix)
    .bind(&hash)
    .bind(&labels)
    .bind(max_uses)
    .bind(&created_by)
    .bind(&created_at)
    .bind(&expires_at)
    .execute(&state.write_pool)
    .await
    .map_err(internal)?;
    tracing::info!(join_token = %id, labels = ?labels, max_uses, by = %created_by, "a runner join token was minted");
    Ok((
        StatusCode::CREATED,
        Json(JoinTokenView {
            id,
            labels,
            max_uses,
            uses: 0,
            created_by,
            created_at,
            expires_at,
            revoked_at: None,
            token: Some(token),
        }),
    ))
}

/// `GET /api/runners/join-tokens` (admin): every join token, without its
/// secret.
pub async fn list_join_tokens(
    AuthUser(claims): AuthUser,
    State(state): State<AppState>,
) -> Result<Json<Vec<JoinTokenView>>, ApiError> {
    require_admin(&claims)?;
    #[allow(clippy::type_complexity)]
    let rows: Vec<(
        String,
        Vec<String>,
        i32,
        i32,
        String,
        String,
        String,
        Option<String>,
    )> = sqlx::query_as(
        "SELECT id, labels, max_uses, uses, created_by, created_at, expires_at, revoked_at
             FROM runner_join_tokens ORDER BY created_at DESC LIMIT 500",
    )
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;
    Ok(Json(
        rows.into_iter()
            .map(
                |(id, labels, max_uses, uses, created_by, created_at, expires_at, revoked_at)| {
                    JoinTokenView {
                        id,
                        labels,
                        max_uses,
                        uses,
                        created_by,
                        created_at,
                        expires_at,
                        revoked_at,
                        token: None,
                    }
                },
            )
            .collect(),
    ))
}

/// `POST /api/runners/join-tokens/{id}/revoke` (admin).
pub async fn revoke_join_token(
    AuthUser(claims): AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&claims)?;
    let done = sqlx::query(
        "UPDATE runner_join_tokens SET revoked_at = COALESCE(revoked_at, $2) WHERE id = $1",
    )
    .bind(&id)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(&state.write_pool)
    .await
    .map_err(internal)?;
    if done.rows_affected() == 0 {
        return Err(err(StatusCode::NOT_FOUND, "no such join token"));
    }
    Ok(Json(json!({ "revoked": true })))
}

#[derive(Debug, Serialize)]
pub struct SessionView {
    pub id: String,
    pub labels: Vec<String>,
    pub capacity: i32,
    pub protocol: i32,
    pub secrets: String,
    pub version: Option<String>,
    pub lease_expires_at: String,
    pub started_at: String,
    pub draining_at: Option<String>,
    /// Tasks it holds now.
    pub running: i64,
    /// Whether its lease is still in force.
    pub live: bool,
}

#[derive(Debug, Serialize)]
pub struct RunnerView {
    pub id: String,
    pub name: String,
    pub labels: Vec<String>,
    pub os: Option<String>,
    pub arch: Option<String>,
    pub capacity: i32,
    pub version: Option<String>,
    pub created_at: String,
    pub last_seen_at: Option<String>,
    pub revoked_at: Option<String>,
    pub sessions: Vec<SessionView>,
}

/// `GET /api/runners`: every runner, and its sessions.
pub async fn list_runners(
    AuthUser(_claims): AuthUser,
    State(state): State<AppState>,
) -> Result<Json<Vec<RunnerView>>, ApiError> {
    #[allow(clippy::type_complexity)]
    let runners: Vec<(
        String,
        String,
        Vec<String>,
        Option<String>,
        Option<String>,
        i32,
        Option<String>,
        String,
        Option<String>,
        Option<String>,
    )> = sqlx::query_as(
        "SELECT id, name, labels, os, arch, capacity, version, created_at, last_seen_at, revoked_at
         FROM runners ORDER BY created_at DESC LIMIT 1000",
    )
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;
    #[allow(clippy::type_complexity)]
    let sessions: Vec<(
        String,
        String,
        Vec<String>,
        i32,
        i32,
        String,
        Option<String>,
        String,
        String,
        Option<String>,
        i64,
    )> = sqlx::query_as(
        "SELECT s.runner_id, s.id, s.labels, s.capacity, s.protocol, s.secrets, s.version,
                s.lease_expires_at, s.started_at, s.draining_at,
                (SELECT COUNT(*) FROM task_runs t
                 WHERE t.session_id = s.id AND t.status = 'running' AND t.claimed_by = s.holder)
         FROM runner_sessions s ORDER BY s.started_at",
    )
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;
    let now = chrono::Utc::now().to_rfc3339();
    let mut out: Vec<RunnerView> = runners
        .into_iter()
        .map(
            |(
                id,
                name,
                labels,
                os,
                arch,
                capacity,
                version,
                created_at,
                last_seen_at,
                revoked_at,
            )| {
                RunnerView {
                    id,
                    name,
                    labels,
                    os,
                    arch,
                    capacity,
                    version,
                    created_at,
                    last_seen_at,
                    revoked_at,
                    sessions: Vec::new(),
                }
            },
        )
        .collect();
    for (
        runner_id,
        id,
        labels,
        capacity,
        protocol,
        secrets,
        version,
        lease,
        started,
        draining,
        running,
    ) in sessions
    {
        if let Some(r) = out.iter_mut().find(|r| r.id == runner_id) {
            r.sessions.push(SessionView {
                id,
                labels,
                capacity,
                protocol,
                secrets,
                version,
                live: lease >= now,
                lease_expires_at: lease,
                started_at: started,
                draining_at: draining,
                running,
            });
        }
    }
    Ok(Json(out))
}

/// `POST /api/runners/{id}/drain` (admin): the runner's live sessions take
/// nothing more, and end once what they run is over. The runner sees it in
/// its next heartbeat or claim, and exits once drained.
pub async fn drain_runner(
    AuthUser(claims): AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&claims)?;
    let now = chrono::Utc::now().to_rfc3339();
    let exists: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM runners WHERE id = $1)")
        .bind(&id)
        .fetch_one(&state.read_pool)
        .await
        .map_err(internal)?;
    if !exists {
        return Err(err(StatusCode::NOT_FOUND, "no such runner"));
    }
    let drained = sqlx::query(
        "UPDATE runner_sessions SET draining_at = $2
         WHERE runner_id = $1 AND draining_at IS NULL AND lease_expires_at >= $2",
    )
    .bind(&id)
    .bind(&now)
    .execute(&state.write_pool)
    .await
    .map_err(internal)?
    .rows_affected();
    tracing::info!(runner = %format!("runner:{id}"), sessions = drained, by = %claims.email, "a runner was asked to drain");
    Ok(Json(json!({ "draining": drained })))
}

/// `POST /api/runners/{id}/revoke` (admin): the runner's credential is refused
/// from now on, and its sessions end at once: the engine's session sweep takes
/// their tasks back, and a word from it on any of them is refused.
pub async fn revoke_runner(
    AuthUser(claims): AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    require_admin(&claims)?;
    let now_dt = chrono::Utc::now();
    let now = now_dt.to_rfc3339();
    // A second before now, so that the sweep's `lease < now` holds already.
    let lapsed = (now_dt - chrono::TimeDelta::seconds(1)).to_rfc3339();
    let mut tx = state.write_pool.begin().await.map_err(internal)?;
    let revoked =
        sqlx::query("UPDATE runners SET revoked_at = COALESCE(revoked_at, $2) WHERE id = $1")
            .bind(&id)
            .bind(&now)
            .execute(&mut *tx)
            .await
            .map_err(internal)?
            .rows_affected();
    if revoked == 0 {
        return Err(err(StatusCode::NOT_FOUND, "no such runner"));
    }
    let sessions = sqlx::query(
        "UPDATE runner_sessions SET lease_expires_at = $2
         WHERE runner_id = $1 AND lease_expires_at >= $2",
    )
    .bind(&id)
    .bind(&lapsed)
    .execute(&mut *tx)
    .await
    .map_err(internal)?
    .rows_affected();
    tx.commit().await.map_err(internal)?;
    tracing::warn!(runner = %format!("runner:{id}"), sessions, by = %claims.email, "a runner was revoked");
    Ok(Json(json!({ "revoked": true, "sessions_ended": sessions })))
}

#[derive(Debug, Deserialize)]
pub struct QueueParams {
    /// Comma-separated. Unset: every label a runner that is not revoked
    /// serves.
    pub labels: Option<String>,
}

/// `GET /api/runners/queue?labels=`: the work there is for each label, and
/// the runners that serve it.
pub async fn queue(
    AuthUser(_claims): AuthUser,
    State(state): State<AppState>,
    Query(params): Query<QueueParams>,
) -> Result<Json<proto::Queue>, ApiError> {
    let asked: Vec<String> = params
        .labels
        .as_deref()
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect();
    let labels: Vec<String> = if asked.is_empty() {
        sqlx::query_scalar(
            "SELECT DISTINCT unnest(labels) AS label FROM runners
             WHERE revoked_at IS NULL ORDER BY label",
        )
        .fetch_all(&state.read_pool)
        .await
        .map_err(internal)?
    } else {
        super::valid_labels(&asked, &[]).map_err(|e| err(StatusCode::BAD_REQUEST, e))?
    };
    let labels = dagron_core::db::runner_queue(&state.read_pool, &labels)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "runners: the queue could not be read");
            err(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
        })?;
    Ok(Json(proto::Queue { labels }))
}
