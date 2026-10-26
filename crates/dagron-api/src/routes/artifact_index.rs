//! What the API knows of the artifacts it took: an index of them, so that a
//! run's artifacts can be listed, one can be deleted, and each expires when
//! its time is up. And the two routes that say where an artifact's bytes go:
//! straight to the store, by a URL the store signs, where it can sign one.
//!
//! **The index is of what went through this API.** A row is written when an
//! artifact is sent with `PUT`, and when an upload that went straight to the
//! store is said to be complete. A file a task writes for itself into the
//! run's directory, or into the bucket with credentials of its own, is served
//! as it always was and is not in the index: it is not listed and nothing
//! expires it, until someone records it (`POST …/complete`).
//!
//! **A row is begun before its bytes are written.** An upload first takes
//! the row's expiry away, and its size, in one statement ([`begin`]). So the
//! sweep, which deletes bytes only under a lock on a row that has expired,
//! never deletes bytes that are being written again; and a row never
//! describes bytes that an upload has since replaced. An upload that is
//! begun under a key that held nothing, and is never finished, expires in a
//! day: whatever arrived of it is deleted with its row. One begun over an
//! artifact that was complete is kept, as that artifact was: nothing deletes
//! an earlier upload because a later one failed.
//!
//! **Direct transfer.** `POST …/upload` and `GET …/download` answer with a
//! URL and whether it is direct. A direct URL is signed by the store for one
//! request on one object, and is good for a short time
//! (`DAGRON_ARTIFACT_PRESIGN_SECS`): whoever holds it may make that request
//! with no other credential, so it is a secret for as long as it lasts. A
//! store that has none to give (a directory, one that seals what it keeps,
//! one that tiers) answers with this API's own route, and the bytes are
//! relayed as before. A client handles both, and need not know which store
//! it is talking to.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPool;
use sqlx::Row;
use tokio::io::{AsyncRead, ReadBuf};

use dagron_artifact::{sanitize_component, ArtifactKey, ArtifactStore, Transfer, Unsupported};

use crate::auth::AuthUser;
use crate::state::AppState;

/// The longest an artifact may be asked to be kept, in days.
pub const MAX_RETENTION_DAYS: u32 = 3650;
/// How long an upload that was begun, and never said to be complete, is
/// left before it is cleared up, at the least.
const PENDING: TimeDelta = TimeDelta::hours(24);
/// How long an artifact whose bytes could not be deleted is left before the
/// sweep tries it again: so that one it cannot delete does not hold up the
/// rest.
const RETRY: TimeDelta = TimeDelta::hours(1);
/// How many rows the sweep takes under one transaction, and how many such
/// batches one pass makes at most.
const SWEEP_BATCH: i64 = 50;
const SWEEP_BATCHES: usize = 20;
/// How many artifacts a listing gives when it is not told, and at most.
const PAGE: u32 = 200;
const MAX_PAGE: u32 = 1000;

/// How the artifact routes are set: read once, as the API starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settings {
    /// Whether a transfer is given a URL signed by the store, where the
    /// store has one to give (`DAGRON_ARTIFACT_PRESIGN`).
    pub presign: bool,
    /// How long such a URL is good for (`DAGRON_ARTIFACT_PRESIGN_SECS`).
    pub url_secs: u64,
    /// How many days an artifact is kept, where the operator has said
    /// (`DAGRON_ARTIFACT_RETENTION_DAYS`). `None`: it is kept.
    pub retention_days: Option<u32>,
    /// How often what has expired is deleted
    /// (`DAGRON_ARTIFACT_SWEEP_SECS`). 0: never.
    pub sweep_secs: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            presign: true,
            url_secs: 900,
            retention_days: None,
            sweep_secs: 300,
        }
    }
}

impl Settings {
    pub fn from_env() -> Result<Self, String> {
        Self::parse(|name| std::env::var(name).ok())
    }

    /// Read the four knobs. A value that cannot be what its knob means is an
    /// error, and the API does not start on it: a retention that was
    /// mistyped must not read as "keep everything".
    pub fn parse(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let set = |name: &str| {
            get(name)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let number = |name: &str, least: u64, most: u64| -> Result<Option<u64>, String> {
            let Some(raw) = set(name) else {
                return Ok(None);
            };
            match raw.parse::<u64>() {
                Ok(n) if (least..=most).contains(&n) => Ok(Some(n)),
                _ => Err(format!(
                    "{name} is a number from {least} to {most}, and is '{raw}'"
                )),
            }
        };
        let d = Settings::default();
        let presign = match set("DAGRON_ARTIFACT_PRESIGN").as_deref() {
            None | Some("on") => true,
            Some("off") => false,
            Some(other) => {
                return Err(format!(
                    "DAGRON_ARTIFACT_PRESIGN is 'on' or 'off', and is '{other}'"
                ))
            }
        };
        Ok(Settings {
            presign,
            // A week is the longest a URL signed with SigV4 lasts.
            url_secs: number("DAGRON_ARTIFACT_PRESIGN_SECS", 60, 604_800)?.unwrap_or(d.url_secs),
            retention_days: number(
                "DAGRON_ARTIFACT_RETENTION_DAYS",
                0,
                u64::from(MAX_RETENTION_DAYS),
            )?
            .filter(|n| *n > 0)
            .map(|n| n as u32),
            sweep_secs: number("DAGRON_ARTIFACT_SWEEP_SECS", 0, 86_400)?.unwrap_or(d.sweep_secs),
        })
    }
}

/// An artifact as the index has it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Indexed {
    pub task: String,
    pub name: String,
    pub size_bytes: i64,
    /// Of the bytes as they passed through the API. `null` for an artifact
    /// that went straight to the store: it was not read here.
    pub sha256: Option<String>,
    pub created_at: String,
    /// `null`: it is kept.
    pub expires_at: Option<String>,
}

type Refusal = (StatusCode, String);

/// Make sure the index is there. The engine's migrations declare the same
/// table (`migrations_pg/079_artifacts.sql`), and the API may start before an
/// engine has run them: both statements are idempotent, and must stay the
/// same.
pub async fn ensure_schema(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS artifacts (
            run_id      TEXT NOT NULL,
            task_name   TEXT NOT NULL,
            name        TEXT NOT NULL,
            object_key  TEXT NOT NULL,
            size_bytes  BIGINT,
            sha256      TEXT,
            expires_at  TEXT,
            created_at  TEXT NOT NULL,
            PRIMARY KEY (run_id, task_name, name)
        )",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_artifacts_expiry
            ON artifacts(expires_at) WHERE expires_at IS NOT NULL",
    )
    .execute(pool)
    .await?;
    Ok(())
}

fn store(state: &AppState) -> Result<&dyn ArtifactStore, Refusal> {
    state.artifact_store.as_deref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "artifact store not configured (set DAGRON_ARTIFACT_DIR)".to_string(),
    ))
}

/// The real error is logged, and the caller is told only that there was one:
/// a store's error can carry a path or a bucket's own words.
fn internal(what: &str, e: impl std::fmt::Display) -> Refusal {
    tracing::error!(error = %e, "{what}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal error".to_string(),
    )
}

/// The three parts of a key as the store spells them, and as the index keys
/// a row: two spellings the store holds as one object are one row.
fn parts(key: &ArtifactKey) -> (String, String, String) {
    (
        sanitize_component(&key.run_id),
        sanitize_component(&key.task),
        sanitize_component(&key.name),
    )
}

/// This API's own route for an artifact: where its bytes go, or come from,
/// when the store has no URL to give.
fn route(key: &ArtifactKey) -> String {
    let (run, task, name) = parts(key);
    format!("/api/runs/{run}/artifacts/{task}/{name}")
}

/// When an artifact recorded at `now` expires: the days it was asked to be
/// kept, and no more than the operator's; the operator's where none were
/// asked; never, where neither is set.
pub fn expires_at(
    settings: &Settings,
    asked: Option<u32>,
    now: DateTime<Utc>,
) -> Result<Option<String>, Refusal> {
    if let Some(days) = asked {
        if days == 0 || days > MAX_RETENTION_DAYS {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("retention_days is a number of days from 1 to {MAX_RETENTION_DAYS}"),
            ));
        }
    }
    let days = match (asked, settings.retention_days) {
        (Some(asked), Some(most)) => Some(asked.min(most)),
        (Some(asked), None) => Some(asked),
        (None, most) => most,
    };
    Ok(days.map(|d| (now + TimeDelta::days(i64::from(d))).to_rfc3339()))
}

/// Say that `key`'s bytes are about to be written: before the first of them.
///
/// The row loses its size and its digest, which described bytes that are
/// being replaced, and its expiry, so that the sweep leaves the key alone
/// from here. A key that held nothing gets a row that expires in a day,
/// which is what clears up an upload that never finishes: or later, where a
/// signed URL is good for longer, so that no upload is cleared up while the
/// URL it was given still works. A row that was complete, or was begun over
/// one that was, is kept: bytes of an earlier upload may be under it.
pub async fn begin(
    pool: &PgPool,
    key: &ArtifactKey,
    now: DateTime<Utc>,
    settings: &Settings,
) -> sqlx::Result<()> {
    let (run, task, name) = parts(key);
    let url_secs = i64::try_from(settings.url_secs).unwrap_or(i64::MAX / 2);
    let left_for = PENDING.max(TimeDelta::seconds(url_secs) + TimeDelta::hours(1));
    sqlx::query(
        "INSERT INTO artifacts
             (run_id, task_name, name, object_key, size_bytes, sha256, expires_at, created_at)
         VALUES ($1, $2, $3, $4, NULL, NULL, $5, $6)
         ON CONFLICT (run_id, task_name, name) DO UPDATE SET
             size_bytes = NULL,
             sha256 = NULL,
             expires_at = CASE
                 WHEN artifacts.size_bytes IS NULL AND artifacts.expires_at IS NOT NULL
                 THEN EXCLUDED.expires_at
                 ELSE NULL
             END",
    )
    .bind(run)
    .bind(task)
    .bind(name)
    .bind(key.rel_path())
    .bind((now + left_for).to_rfc3339())
    .bind(now.to_rfc3339())
    .execute(pool)
    .await?;
    Ok(())
}

/// Record `key` as complete: what it holds, and when it expires.
pub async fn record(
    pool: &PgPool,
    key: &ArtifactKey,
    size_bytes: u64,
    sha256: Option<&str>,
    expires_at: Option<&str>,
    now: DateTime<Utc>,
) -> sqlx::Result<Indexed> {
    let (run, task, name) = parts(key);
    let row = sqlx::query(
        "INSERT INTO artifacts
             (run_id, task_name, name, object_key, size_bytes, sha256, expires_at, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (run_id, task_name, name) DO UPDATE SET
             object_key = EXCLUDED.object_key,
             size_bytes = EXCLUDED.size_bytes,
             sha256 = EXCLUDED.sha256,
             expires_at = EXCLUDED.expires_at,
             created_at = EXCLUDED.created_at
         RETURNING task_name, name, size_bytes, sha256, created_at, expires_at",
    )
    .bind(run)
    .bind(task)
    .bind(name)
    .bind(key.rel_path())
    .bind(i64::try_from(size_bytes).unwrap_or(i64::MAX))
    .bind(sha256)
    .bind(expires_at)
    .bind(now.to_rfc3339())
    .fetch_one(pool)
    .await?;
    indexed(&row)
}

fn indexed(row: &sqlx::postgres::PgRow) -> sqlx::Result<Indexed> {
    Ok(Indexed {
        task: row.try_get("task_name")?,
        name: row.try_get("name")?,
        size_bytes: row.try_get("size_bytes")?,
        sha256: row.try_get("sha256")?,
        created_at: row.try_get("created_at")?,
        expires_at: row.try_get("expires_at")?,
    })
}

/// The row of `key`, where it is complete.
async fn complete_row(pool: &PgPool, key: &ArtifactKey) -> sqlx::Result<Option<Indexed>> {
    let (run, task, name) = parts(key);
    let row = sqlx::query(
        "SELECT task_name, name, size_bytes, sha256, created_at, expires_at
           FROM artifacts
          WHERE run_id = $1 AND task_name = $2 AND name = $3 AND size_bytes IS NOT NULL",
    )
    .bind(run)
    .bind(task)
    .bind(name)
    .fetch_optional(pool)
    .await?;
    row.as_ref().map(indexed).transpose()
}

/// Take `key`'s row away. Whether there was one.
async fn forget(pool: &PgPool, key: &ArtifactKey) -> sqlx::Result<bool> {
    let (run, task, name) = parts(key);
    let gone =
        sqlx::query("DELETE FROM artifacts WHERE run_id = $1 AND task_name = $2 AND name = $3")
            .bind(run)
            .bind(task)
            .bind(name)
            .execute(pool)
            .await?;
    Ok(gone.rows_affected() > 0)
}

/// A reader that counts and hashes what passes through it, so that a store
/// that takes a stream need not be asked afterwards what it was given.
pub struct Measured<R> {
    inner: R,
    seen: Arc<Mutex<(u64, Sha256)>>,
}

/// What a [`Measured`] reader has seen, read after the reader is spent.
#[derive(Clone)]
pub struct Tally(Arc<Mutex<(u64, Sha256)>>);

impl Tally {
    /// How many bytes passed, and their sha256 in hex.
    pub fn read(&self) -> (u64, String) {
        let seen = self.0.lock().unwrap_or_else(|e| e.into_inner());
        (seen.0, format!("{:x}", seen.1.clone().finalize()))
    }
}

impl<R> Measured<R> {
    pub fn new(inner: R) -> (Self, Tally) {
        let seen = Arc::new(Mutex::new((0, Sha256::new())));
        (
            Measured {
                inner,
                seen: Arc::clone(&seen),
            },
            Tally(seen),
        )
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for Measured<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let polled = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &polled {
            let new = &buf.filled()[before..];
            if !new.is_empty() {
                let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
                seen.0 += new.len() as u64;
                seen.1.update(new);
            }
        }
        polled
    }
}

/// Store `bytes` under `key` and record them: for what the API itself
/// writes, as it writes what a gate approved.
pub async fn put_recorded(state: &AppState, key: &ArtifactKey, bytes: &[u8]) -> anyhow::Result<()> {
    let s = state
        .artifact_store
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("artifact store not configured"))?;
    let now = Utc::now();
    begin(&state.write_pool, key, now, &state.artifacts).await?;
    s.put(key, bytes).await?;
    let expires =
        expires_at(&state.artifacts, None, now).map_err(|(_, why)| anyhow::anyhow!(why))?;
    let sha = format!("{:x}", Sha256::digest(bytes));
    record(
        &state.write_pool,
        key,
        bytes.len() as u64,
        Some(&sha),
        expires.as_deref(),
        now,
    )
    .await?;
    Ok(())
}

/// How long an artifact is asked to be kept: `?retention_days=N`.
#[derive(Debug, Default, Deserialize)]
pub struct Keep {
    #[serde(default)]
    pub retention_days: Option<u32>,
}

/// Where a listing goes on from, and how many it gives.
#[derive(Debug, Default, Deserialize)]
pub struct Page {
    /// The `next` of the listing before: `<task>/<name>`.
    #[serde(default)]
    pub after: Option<String>,
    #[serde(default)]
    pub limit: Option<u32>,
}

/// `GET /api/runs/{run_id}/artifacts`: the run's artifacts that are in the
/// index and complete, by task and name. `next` is given where there are
/// more, to be sent back as `after`.
pub async fn list_artifacts(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(run_id): Path<String>,
    Query(page): Query<Page>,
) -> Result<Json<serde_json::Value>, Refusal> {
    store(&state)?;
    let limit = page.limit.unwrap_or(PAGE).clamp(1, MAX_PAGE);
    let (after_task, after_name) = match page.after.as_deref() {
        None | Some("") => (String::new(), String::new()),
        Some(after) => match after.split_once('/') {
            Some((task, name)) => (task.to_string(), name.to_string()),
            None => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "after is the next of an earlier listing: <task>/<name>".to_string(),
                ))
            }
        },
    };
    // One more than asked, to know whether there are more.
    let rows = sqlx::query(
        "SELECT task_name, name, size_bytes, sha256, created_at, expires_at
           FROM artifacts
          WHERE run_id = $1 AND size_bytes IS NOT NULL AND (task_name, name) > ($2, $3)
          ORDER BY task_name, name
          LIMIT $4",
    )
    .bind(sanitize_component(&run_id))
    .bind(after_task)
    .bind(after_name)
    .bind(i64::from(limit) + 1)
    .fetch_all(&state.read_pool)
    .await
    .map_err(|e| internal("artifact index: list failed", e))?;
    let mut artifacts = rows
        .iter()
        .map(indexed)
        .collect::<sqlx::Result<Vec<_>>>()
        .map_err(|e| internal("artifact index: a row could not be read", e))?;
    let more = artifacts.len() > limit as usize;
    artifacts.truncate(limit as usize);
    let next = match (more, artifacts.last()) {
        (true, Some(last)) => Some(format!("{}/{}", last.task, last.name)),
        _ => None,
    };
    Ok(Json(json!({ "artifacts": artifacts, "next": next })))
}

/// `DELETE /api/runs/{run_id}/artifacts/{task}/{name}`: the artifact's
/// bytes, then its row. `404` where there was neither.
///
/// The bytes go first. Were the row to go first and the store then fail, the
/// bytes would be left with nothing that says they are there, for nothing to
/// clear up; this way round a failure leaves a row, and the delete can be
/// asked again.
pub async fn delete_artifact(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path((run_id, task, name)): Path<(String, String, String)>,
) -> Result<StatusCode, Refusal> {
    let s = store(&state)?;
    let key = ArtifactKey::new(run_id, task, name);
    let held = match s.delete(&key).await {
        Ok(held) => held,
        Err(e) => {
            if let Some(why) = e.downcast_ref::<Unsupported>() {
                return Err((StatusCode::NOT_IMPLEMENTED, why.to_string()));
            }
            return Err(internal("artifact delete failed", e));
        }
    };
    let known = forget(&state.write_pool, &key)
        .await
        .map_err(|e| internal("artifact index: delete failed", e))?;
    if held || known {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, "artifact not found".to_string()))
    }
}

/// A URL of the store's for `key`, where direct transfer is on and the store
/// has one to give. A store that fails to sign is not an error to the
/// caller: it is told to send the bytes here, as it must be ready to anyway.
async fn signed(
    state: &AppState,
    s: &dyn ArtifactStore,
    key: &ArtifactKey,
    how: Transfer,
) -> Option<dagron_artifact::Presigned> {
    if !state.artifacts.presign {
        return None;
    }
    match s
        .presign(key, how, Duration::from_secs(state.artifacts.url_secs))
        .await
    {
        Ok(url) => url,
        Err(e) => {
            tracing::warn!(error = %e, "the artifact store signed no URL: the bytes are relayed");
            None
        }
    }
}

/// The headers a signed request must carry, as an object: none, for most
/// stores.
fn headers(direct: &dagron_artifact::Presigned) -> serde_json::Value {
    serde_json::Value::Object(
        direct
            .headers
            .iter()
            .map(|(name, value)| (name.to_string(), json!(value)))
            .collect(),
    )
}

/// `POST /api/runs/{run_id}/artifacts/{task}/{name}/upload`: where to send
/// the artifact's bytes.
///
/// Direct: `{direct: true, method: "PUT", url, headers, expires_in_secs,
/// complete}`. The bytes are sent to `url` with the `headers` it names (a
/// store may ask for one: Azure Blob does) and no credential, and then
/// `complete` is called, which records them. Relayed:
/// `{direct: false, method: "PUT", url}`, where `url` is this API's own
/// route and takes this API's credentials, and records the bytes itself.
pub async fn artifact_upload(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path((run_id, task, name)): Path<(String, String, String)>,
) -> Result<Json<serde_json::Value>, Refusal> {
    let s = store(&state)?;
    let key = ArtifactKey::new(run_id, task, name);
    let here = route(&key);
    let Some(direct) = signed(&state, s, &key, Transfer::Put).await else {
        return Ok(Json(
            json!({ "direct": false, "method": "PUT", "url": here }),
        ));
    };
    // Begun before the URL is given out: from here the key may hold bytes
    // nothing else knows of.
    begin(&state.write_pool, &key, Utc::now(), &state.artifacts)
        .await
        .map_err(|e| internal("artifact index: begin failed", e))?;
    Ok(Json(json!({
        "direct": true,
        "method": direct.method,
        "url": direct.url,
        "headers": headers(&direct),
        "expires_in_secs": state.artifacts.url_secs,
        "complete": format!("{here}/complete"),
    })))
}

/// `POST /api/runs/{run_id}/artifacts/{task}/{name}/complete`: record what
/// the store holds under the key, after an upload that went straight to it.
///
/// It also records a file that was never sent through the API (one a task
/// wrote into the run's directory), which is how such a file comes to be
/// listed and to expire. An artifact that is already complete is answered
/// as it is. `404` where the store holds nothing under the key; `409` where
/// the store cannot say what an artifact holds without reading it (one that
/// seals what it keeps): there the bytes are sent with `PUT`.
pub async fn artifact_complete(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path((run_id, task, name)): Path<(String, String, String)>,
    Query(keep): Query<Keep>,
) -> Result<Json<Indexed>, Refusal> {
    let s = store(&state)?;
    let key = ArtifactKey::new(run_id, task, name);
    let now = Utc::now();
    let expires = expires_at(&state.artifacts, keep.retention_days, now)?;
    if let Some(row) = complete_row(&state.write_pool, &key)
        .await
        .map_err(|e| internal("artifact index: read failed", e))?
    {
        return Ok(Json(row));
    }
    let size = match s.size(&key).await {
        Ok(Some(size)) => size,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                "the artifact store holds nothing under this key".to_string(),
            ))
        }
        Err(e) => {
            if e.downcast_ref::<Unsupported>().is_some() {
                return Err((
                    StatusCode::CONFLICT,
                    "this artifact store cannot say what an artifact holds without reading it: \
                     send the bytes with PUT, which records them"
                        .to_string(),
                ));
            }
            return Err(internal("artifact size failed", e));
        }
    };
    let row = record(&state.write_pool, &key, size, None, expires.as_deref(), now)
        .await
        .map_err(|e| internal("artifact index: record failed", e))?;
    Ok(Json(row))
}

/// `GET /api/runs/{run_id}/artifacts/{task}/{name}/download`: where to
/// fetch the artifact's bytes. `{direct: true, method: "GET", url, headers,
/// expires_in_secs}`, or `{direct: false, method: "GET", url}` with this
/// API's own route. `404` where there is no such artifact.
pub async fn artifact_download(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path((run_id, task, name)): Path<(String, String, String)>,
) -> Result<Json<serde_json::Value>, Refusal> {
    let s = store(&state)?;
    let key = ArtifactKey::new(run_id, task, name);
    if !s
        .exists(&key)
        .await
        .map_err(|e| internal("artifact exists check failed", e))?
    {
        return Err((StatusCode::NOT_FOUND, "artifact not found".to_string()));
    }
    Ok(Json(match signed(&state, s, &key, Transfer::Get).await {
        Some(direct) => json!({
            "direct": true,
            "method": direct.method,
            "url": direct.url,
            "headers": headers(&direct),
            "expires_in_secs": state.artifacts.url_secs,
        }),
        None => json!({ "direct": false, "method": "GET", "url": route(&key) }),
    }))
}

/// What one pass of the sweep did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Swept {
    /// Rows whose bytes are gone, and the rows with them.
    pub deleted: usize,
    /// Rows whose bytes the store failed to delete: left, to be tried again.
    pub failed: usize,
}

/// Delete what has expired by `now`: each artifact's bytes, then its row.
///
/// A row is taken under a lock, in a transaction that ends only after its
/// bytes are dealt with, and an upload begins by taking the row's expiry
/// away ([`begin`]), which waits for that lock. So the bytes deleted here
/// are those of the row that expired, and never those of an upload that
/// came after. Two replicas sweep at once without meeting: each skips the
/// rows the other holds.
///
/// A store that deletes nothing answers with [`Unsupported`], and that is
/// this function's error too: the caller stops sweeping, as it would be told
/// the same for ever.
pub async fn sweep(
    pool: &PgPool,
    s: &dyn ArtifactStore,
    now: DateTime<Utc>,
) -> anyhow::Result<Swept> {
    let mut swept = Swept::default();
    for _ in 0..SWEEP_BATCHES {
        let mut tx = pool.begin().await?;
        let rows = sqlx::query(
            "SELECT run_id, task_name, name FROM artifacts
              WHERE expires_at IS NOT NULL AND expires_at <= $1
              ORDER BY expires_at
              LIMIT $2
              FOR UPDATE SKIP LOCKED",
        )
        .bind(now.to_rfc3339())
        .bind(SWEEP_BATCH)
        .fetch_all(&mut *tx)
        .await?;
        if rows.is_empty() {
            break;
        }
        let taken = rows.len();
        for row in &rows {
            let (run, task, name): (String, String, String) = (
                row.try_get("run_id")?,
                row.try_get("task_name")?,
                row.try_get("name")?,
            );
            let key = ArtifactKey::new(run.as_str(), task.as_str(), name.as_str());
            match s.delete(&key).await {
                Ok(_) => {
                    sqlx::query(
                        "DELETE FROM artifacts WHERE run_id = $1 AND task_name = $2 AND name = $3",
                    )
                    .bind(&run)
                    .bind(&task)
                    .bind(&name)
                    .execute(&mut *tx)
                    .await?;
                    swept.deleted += 1;
                }
                Err(e) if e.downcast_ref::<Unsupported>().is_some() => {
                    // Rolled back here, and not left to the drop: a dropped
                    // transaction is rolled back a moment later, when its
                    // connection goes back to the pool, and until then the
                    // rows it took are still held. A sweep that followed at
                    // once would pass over them as another's.
                    tx.rollback().await?;
                    return Err(e);
                }
                Err(e) => {
                    tracing::warn!(error = %e, run = %run, task = %task, artifact = %name,
                        "an expired artifact could not be deleted: it is tried again later");
                    sqlx::query(
                        "UPDATE artifacts SET expires_at = $4
                          WHERE run_id = $1 AND task_name = $2 AND name = $3",
                    )
                    .bind(&run)
                    .bind(&task)
                    .bind(&name)
                    .bind((now + RETRY).to_rfc3339())
                    .execute(&mut *tx)
                    .await?;
                    swept.failed += 1;
                }
            }
        }
        tx.commit().await?;
        if (taken as i64) < SWEEP_BATCH {
            break;
        }
    }
    Ok(swept)
}

/// Sweep on a timer for as long as the API runs. It shares the lock of the
/// store's other sweeps (key rotation, the tier's drain) and skips its turn
/// while one of them holds it: a rotation reads an object and writes it
/// back, and a delete between the two would be undone.
pub fn spawn_sweep(state: AppState) {
    let secs = state.artifacts.sweep_secs;
    let Some(store) = state.artifact_store.clone() else {
        return;
    };
    if secs == 0 {
        return;
    }
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval(Duration::from_secs(secs));
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            let Ok(_guard) = state.rotation_lock.try_lock() else {
                continue;
            };
            match sweep(&state.write_pool, store.as_ref(), Utc::now()).await {
                Ok(Swept {
                    deleted: 0,
                    failed: 0,
                }) => {}
                Ok(swept) => {
                    tracing::info!(
                        deleted = swept.deleted,
                        failed = swept.failed,
                        "expired artifacts swept"
                    )
                }
                Err(e) if e.downcast_ref::<Unsupported>().is_some() => {
                    tracing::warn!(
                        "{e}: artifacts that expire are kept, and uploads that never finish are not cleared up"
                    );
                    return;
                }
                Err(e) => tracing::warn!(error = %e, "the artifact sweep failed"),
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::SessionClaims;
    use crate::routes::artifacts::{get_artifact, put_artifact};
    use crate::routes::ops::redrive_tests::test_state;
    use async_trait::async_trait;
    use axum::body::Body;
    use dagron_artifact::{LocalFsStore, Presigned};

    fn user() -> AuthUser {
        AuthUser(SessionClaims {
            sub: "sub-art".to_string(),
            email: "art@example.com".to_string(),
            name: String::new(),
            groups: vec![],
            exp: 0,
        })
    }

    /// A directory of this test's own, and a run nothing else has.
    fn fresh(tag: &str) -> (std::path::PathBuf, String) {
        let id = uuid::Uuid::new_v4();
        (
            std::env::temp_dir().join(format!("dagron-idx-{tag}-{id}")),
            format!("run-{tag}-{id}"),
        )
    }

    /// A directory that signs, as a bucket does: the URL names the file,
    /// and nothing fetches it.
    struct Signing(LocalFsStore);

    #[async_trait]
    impl ArtifactStore for Signing {
        async fn put(&self, key: &ArtifactKey, bytes: &[u8]) -> anyhow::Result<String> {
            self.0.put(key, bytes).await
        }
        async fn get(&self, key: &ArtifactKey) -> anyhow::Result<Vec<u8>> {
            self.0.get(key).await
        }
        async fn exists(&self, key: &ArtifactKey) -> anyhow::Result<bool> {
            self.0.exists(key).await
        }
        async fn delete(&self, key: &ArtifactKey) -> anyhow::Result<bool> {
            self.0.delete(key).await
        }
        async fn size(&self, key: &ArtifactKey) -> anyhow::Result<Option<u64>> {
            self.0.size(key).await
        }
        async fn presign(
            &self,
            key: &ArtifactKey,
            how: Transfer,
            expires_in: Duration,
        ) -> anyhow::Result<Option<Presigned>> {
            let (method, headers) = match how {
                Transfer::Put => ("PUT", vec![("x-ms-blob-type", "BlockBlob")]),
                Transfer::Get => ("GET", Vec::new()),
            };
            Ok(Some(Presigned {
                method,
                headers,
                url: format!(
                    "https://bucket.example/{}?for={}&sig=5ecret",
                    key.rel_path(),
                    expires_in.as_secs()
                ),
            }))
        }
    }

    /// A store whose delete does as it is told: fails, or says it never will.
    struct Stubborn(bool);

    #[async_trait]
    impl ArtifactStore for Stubborn {
        async fn put(&self, _key: &ArtifactKey, _bytes: &[u8]) -> anyhow::Result<String> {
            Ok(String::new())
        }
        async fn get(&self, _key: &ArtifactKey) -> anyhow::Result<Vec<u8>> {
            Ok(Vec::new())
        }
        async fn exists(&self, _key: &ArtifactKey) -> anyhow::Result<bool> {
            Ok(true)
        }
        async fn delete(&self, _key: &ArtifactKey) -> anyhow::Result<bool> {
            if self.0 {
                Err(Unsupported("this artifact store deletes nothing").into())
            } else {
                anyhow::bail!("the bucket is away")
            }
        }
    }

    async fn put(
        state: &AppState,
        run: &str,
        task: &str,
        name: &str,
        days: Option<u32>,
        bytes: &'static [u8],
    ) -> Result<(StatusCode, String), Refusal> {
        put_artifact(
            user(),
            State(state.clone()),
            Path((run.to_string(), task.to_string(), name.to_string())),
            Query(Keep {
                retention_days: days,
            }),
            Body::from(bytes),
        )
        .await
    }

    async fn listed(state: &AppState, run: &str, page: Page) -> serde_json::Value {
        list_artifacts(
            user(),
            State(state.clone()),
            Path(run.to_string()),
            Query(page),
        )
        .await
        .unwrap()
        .0
    }

    fn at(run: &str, task: &str, name: &str) -> Path<(String, String, String)> {
        Path((run.to_string(), task.to_string(), name.to_string()))
    }

    /// `(size_bytes, sha256, expires_at)` of a row, complete or not.
    async fn row(
        state: &AppState,
        run: &str,
        name: &str,
    ) -> Option<(Option<i64>, Option<String>, Option<String>)> {
        sqlx::query_as(
            "SELECT size_bytes, sha256, expires_at FROM artifacts WHERE run_id = $1 AND name = $2",
        )
        .bind(run)
        .bind(name)
        .fetch_optional(&state.write_pool)
        .await
        .unwrap()
    }

    /// Take a run's rows away, so that none is left to expire in a database
    /// that outlives this test.
    async fn forget_run(state: &AppState, run: &str) {
        sqlx::query("DELETE FROM artifacts WHERE run_id = $1")
            .bind(run)
            .execute(&state.write_pool)
            .await
            .unwrap();
    }

    fn days_from_now(stamp: &str) -> i64 {
        let when = DateTime::parse_from_rfc3339(stamp).unwrap();
        (when.with_timezone(&Utc) - Utc::now() + TimeDelta::minutes(1)).num_days()
    }

    #[test]
    fn the_knobs_are_read_strictly() {
        fn with(pairs: &[(&str, &str)]) -> Result<Settings, String> {
            Settings::parse(|name| {
                pairs
                    .iter()
                    .find(|(k, _)| *k == name)
                    .map(|(_, v)| v.to_string())
            })
        }
        assert_eq!(with(&[]), Ok(Settings::default()));
        assert_eq!(
            with(&[
                ("DAGRON_ARTIFACT_PRESIGN", " off "),
                ("DAGRON_ARTIFACT_PRESIGN_SECS", "120"),
                ("DAGRON_ARTIFACT_RETENTION_DAYS", "30"),
                ("DAGRON_ARTIFACT_SWEEP_SECS", "0"),
            ]),
            Ok(Settings {
                presign: false,
                url_secs: 120,
                retention_days: Some(30),
                sweep_secs: 0
            })
        );
        // No retention is said as nothing, or as nought.
        assert_eq!(
            with(&[("DAGRON_ARTIFACT_RETENTION_DAYS", "0")])
                .unwrap()
                .retention_days,
            None
        );
        assert_eq!(
            with(&[("DAGRON_ARTIFACT_RETENTION_DAYS", "")])
                .unwrap()
                .retention_days,
            None
        );
        for (name, value) in [
            ("DAGRON_ARTIFACT_PRESIGN", "yes"),
            ("DAGRON_ARTIFACT_PRESIGN_SECS", "5"),
            ("DAGRON_ARTIFACT_PRESIGN_SECS", "700000"),
            ("DAGRON_ARTIFACT_RETENTION_DAYS", "30d"),
            ("DAGRON_ARTIFACT_RETENTION_DAYS", "4000"),
            ("DAGRON_ARTIFACT_SWEEP_SECS", "-1"),
        ] {
            let refused = with(&[(name, value)]).unwrap_err();
            assert!(
                refused.starts_with(name) && refused.contains(value),
                "{refused}"
            );
        }
    }

    #[test]
    fn an_artifact_is_kept_for_what_was_asked_and_no_longer_than_the_operator_says() {
        let now = Utc::now();
        let days = |settings: &Settings, asked| {
            expires_at(settings, asked, now).map(|at| {
                at.map(|at| {
                    (DateTime::parse_from_rfc3339(&at)
                        .unwrap()
                        .with_timezone(&Utc)
                        - now)
                        .num_days()
                })
            })
        };
        let unset = Settings::default();
        let thirty = Settings {
            retention_days: Some(30),
            ..Settings::default()
        };
        assert_eq!(days(&unset, None), Ok(None), "kept, as it always was");
        assert_eq!(days(&unset, Some(7)), Ok(Some(7)));
        assert_eq!(days(&thirty, None), Ok(Some(30)));
        assert_eq!(days(&thirty, Some(7)), Ok(Some(7)));
        assert_eq!(
            days(&thirty, Some(90)),
            Ok(Some(30)),
            "no longer than the operator's"
        );
        for refused in [0, MAX_RETENTION_DAYS + 1] {
            assert_eq!(
                days(&thirty, Some(refused)).unwrap_err().0,
                StatusCode::BAD_REQUEST
            );
        }
    }

    #[tokio::test]
    async fn a_reader_is_measured_as_it_is_read() {
        use tokio::io::AsyncReadExt;
        let (mut reader, tally) =
            Measured::new(std::io::Cursor::new(b"create 3 resources".to_vec()));
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"create 3 resources");
        assert_eq!(
            tally.read(),
            (18, format!("{:x}", Sha256::digest(b"create 3 resources")))
        );
    }

    #[tokio::test]
    async fn an_artifact_put_through_the_api_is_listed_and_deleted() {
        let Some(mut state) = test_state("artifact-index-1").await else {
            return;
        };
        let (dir, run) = fresh("put");
        state.artifact_store = Some(Arc::new(LocalFsStore::new(&dir)));

        assert_eq!(
            listed(&state, &run, Page::default()).await,
            json!({ "artifacts": [], "next": null })
        );
        let (code, _) = put(&state, &run, "build", "app.tar", None, b"twelve bytes")
            .await
            .unwrap();
        assert_eq!(code, StatusCode::CREATED);
        put(&state, &run, "build", "notes.txt", Some(7), b"n")
            .await
            .unwrap();
        put(&state, &run, "test", "junit.xml", None, b"<testsuite/>")
            .await
            .unwrap();
        // Another run's artifact is another run's.
        let (_, other) = fresh("put");
        put(&state, &other, "build", "app.tar", None, b"x")
            .await
            .unwrap();

        let all = listed(&state, &run, Page::default()).await;
        let names: Vec<String> = all["artifacts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| {
                format!(
                    "{}/{}",
                    a["task"].as_str().unwrap(),
                    a["name"].as_str().unwrap()
                )
            })
            .collect();
        assert_eq!(
            names,
            ["build/app.tar", "build/notes.txt", "test/junit.xml"]
        );
        assert_eq!(all["next"], serde_json::Value::Null);
        let app = &all["artifacts"][0];
        assert_eq!(app["size_bytes"], 12);
        assert_eq!(
            app["sha256"],
            format!("{:x}", Sha256::digest(b"twelve bytes"))
        );
        assert_eq!(
            app["expires_at"],
            serde_json::Value::Null,
            "kept: nobody said for how long"
        );
        assert_eq!(
            days_from_now(all["artifacts"][1]["expires_at"].as_str().unwrap()),
            7
        );

        // A page at a time, each going on from the last.
        let first = listed(
            &state,
            &run,
            Page {
                after: None,
                limit: Some(2),
            },
        )
        .await;
        assert_eq!(first["artifacts"].as_array().unwrap().len(), 2);
        assert_eq!(first["next"], "build/notes.txt");
        let rest = listed(
            &state,
            &run,
            Page {
                after: Some("build/notes.txt".into()),
                limit: Some(2),
            },
        )
        .await;
        assert_eq!(rest["artifacts"][0]["name"], "junit.xml");
        assert_eq!(rest["next"], serde_json::Value::Null);
        let refused = list_artifacts(
            user(),
            State(state.clone()),
            Path(run.clone()),
            Query(Page {
                after: Some("no-slash".into()),
                limit: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(refused.0, StatusCode::BAD_REQUEST);

        // Sent again, it is recorded again: what it holds now.
        put(&state, &run, "build", "app.tar", None, b"shorter")
            .await
            .unwrap();
        assert_eq!(
            row(&state, &run, "app.tar").await.unwrap(),
            (
                Some(7),
                Some(format!("{:x}", Sha256::digest(b"shorter"))),
                None
            )
        );

        // Deleted: its bytes and its row. A second delete finds neither.
        let gone =
            delete_artifact(user(), State(state.clone()), at(&run, "build", "app.tar")).await;
        assert_eq!(gone, Ok(StatusCode::NO_CONTENT));
        assert!(row(&state, &run, "app.tar").await.is_none());
        let fetch = get_artifact(user(), State(state.clone()), at(&run, "build", "app.tar")).await;
        assert_eq!(fetch.err().map(|e| e.0), Some(StatusCode::NOT_FOUND));
        let again =
            delete_artifact(user(), State(state.clone()), at(&run, "build", "app.tar")).await;
        assert_eq!(again.unwrap_err().0, StatusCode::NOT_FOUND);
        // A retention that is no number of days is refused before a byte is kept.
        let refused = put(&state, &run, "build", "late.txt", Some(0), b"x")
            .await
            .unwrap_err();
        assert_eq!(refused.0, StatusCode::BAD_REQUEST);
        assert!(row(&state, &run, "late.txt").await.is_none());

        // A store that deletes nothing says so, and nothing is forgotten.
        state.artifact_store = Some(Arc::new(Stubborn(true)));
        let cannot = delete_artifact(user(), State(state.clone()), at(&run, "test", "junit.xml"))
            .await
            .unwrap_err();
        assert_eq!(
            cannot,
            (
                StatusCode::NOT_IMPLEMENTED,
                "this artifact store deletes nothing".to_string()
            )
        );
        assert!(row(&state, &run, "junit.xml").await.is_some());
        forget_run(&state, &run).await;
        forget_run(&state, &other).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn bytes_go_straight_to_a_store_that_signs_and_are_relayed_by_one_that_does_not() {
        let Some(mut state) = test_state("artifact-index-2").await else {
            return;
        };
        let (dir, run) = fresh("direct");
        let upload = |state: &AppState, name: &'static str| {
            artifact_upload(user(), State(state.clone()), at(&run, "build", name))
        };
        let download = |state: &AppState, name: &'static str| {
            artifact_download(user(), State(state.clone()), at(&run, "build", name))
        };
        let complete = |state: &AppState, name: &'static str, days| {
            artifact_complete(
                user(),
                State(state.clone()),
                at(&run, "build", name),
                Query(Keep {
                    retention_days: days,
                }),
            )
        };
        let here = format!("/api/runs/{run}/artifacts/build/app.tar");

        // A directory signs nothing: the bytes are sent here, and nothing is begun.
        state.artifact_store = Some(Arc::new(LocalFsStore::new(&dir)));
        assert_eq!(
            upload(&state, "app.tar").await.unwrap().0,
            json!({ "direct": false, "method": "PUT", "url": here })
        );
        assert!(row(&state, &run, "app.tar").await.is_none());

        // A store that signs: the URL is its own, and an upload is begun.
        state.artifact_store = Some(Arc::new(Signing(LocalFsStore::new(&dir))));
        let direct = upload(&state, "app.tar").await.unwrap().0;
        assert_eq!(
            direct,
            json!({
                "direct": true,
                "method": "PUT",
                "url": format!("https://bucket.example/{run}/build/app.tar?for=900&sig=5ecret"),
                "headers": { "x-ms-blob-type": "BlockBlob" },
                "expires_in_secs": 900,
                "complete": format!("{here}/complete"),
            })
        );
        let (size, sha, expires) = row(&state, &run, "app.tar").await.expect("begun");
        assert_eq!((size, sha), (None, None));
        assert_eq!(
            days_from_now(&expires.expect("a day")),
            1,
            "an upload never finished is cleared up"
        );
        assert_eq!(
            listed(&state, &run, Page::default()).await["artifacts"],
            json!([]),
            "not listed until it is complete"
        );
        // Nothing has arrived: there is nothing to complete, or to download.
        assert_eq!(
            complete(&state, "app.tar", None).await.unwrap_err().0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            download(&state, "app.tar").await.unwrap_err().0,
            StatusCode::NOT_FOUND
        );

        // The bytes arrive at the store, by no route of the API's.
        let key = ArtifactKey::new(run.as_str(), "build", "app.tar");
        LocalFsStore::new(&dir)
            .put(&key, b"straight there")
            .await
            .unwrap();
        let done = complete(&state, "app.tar", Some(3)).await.unwrap().0;
        assert_eq!(
            (done.size_bytes, done.sha256.clone()),
            (14, None),
            "not read here: no digest"
        );
        assert_eq!(days_from_now(done.expires_at.as_deref().unwrap()), 3);
        // Said again, it is answered as it is.
        assert_eq!(complete(&state, "app.tar", Some(9)).await.unwrap().0, done);
        assert_eq!(
            listed(&state, &run, Page::default()).await["artifacts"][0]["size_bytes"],
            14
        );
        assert_eq!(
            download(&state, "app.tar").await.unwrap().0,
            json!({
                "direct": true,
                "method": "GET",
                "url": format!("https://bucket.example/{run}/build/app.tar?for=900&sig=5ecret"),
                "headers": {},
                "expires_in_secs": 900,
            })
        );

        // Uploaded over: the row no longer says what the old bytes were, and
        // is kept, as bytes of the earlier upload are still under it.
        let _ = upload(&state, "app.tar").await.unwrap();
        assert_eq!(
            row(&state, &run, "app.tar").await.unwrap(),
            (None, None, None)
        );
        let _ = upload(&state, "app.tar").await.unwrap();
        assert_eq!(
            row(&state, &run, "app.tar").await.unwrap().2,
            None,
            "still kept, however often it is begun"
        );
        // One begun under a key that held nothing has its day begun again.
        let _ = upload(&state, "other.bin").await.unwrap();
        let _ = upload(&state, "other.bin").await.unwrap();
        assert!(row(&state, &run, "other.bin").await.unwrap().2.is_some());

        // A URL good for longer than a day leaves its upload that long,
        // and an hour: it is not cleared up while its URL still works.
        state.artifacts.url_secs = 3 * 86_400;
        let _ = upload(&state, "slow.bin").await.unwrap();
        let left = row(&state, &run, "slow.bin").await.unwrap().2.unwrap();
        assert_eq!(days_from_now(&left), 3, "{left}");
        state.artifacts.url_secs = 900;

        // With direct transfer off, a store that signs is not asked to.
        state.artifacts.presign = false;
        assert_eq!(upload(&state, "app.tar").await.unwrap().0["direct"], false);
        assert_eq!(
            download(&state, "app.tar").await.unwrap().0,
            json!({ "direct": false, "method": "GET", "url": here })
        );
        state.artifacts.presign = true;

        // A file a task wrote for itself is recorded by the same call.
        let own = ArtifactKey::new(run.as_str(), "build", "by-hand.txt");
        LocalFsStore::new(&dir).put(&own, b"hand").await.unwrap();
        assert_eq!(
            complete(&state, "by-hand.txt", None)
                .await
                .unwrap()
                .0
                .size_bytes,
            4
        );

        // A store that seals what it keeps cannot say what one holds.
        state.artifact_store = Some(Arc::new(Stubborn(false)));
        assert_eq!(
            complete(&state, "sealed.bin", None).await.unwrap_err().0,
            StatusCode::CONFLICT
        );
        forget_run(&state, &run).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Every check of the sweep is in this one test: a sweep takes every row
    /// that has expired, whichever test wrote it, so no other test here may
    /// leave one that has. And it reads the fate of its own rows, not how
    /// many a pass took: the database outlives a test run, and a row an
    /// earlier run left expires in time.
    #[tokio::test]
    async fn what_has_expired_is_deleted_and_what_cannot_be_is_tried_again() {
        let Some(mut state) = test_state("artifact-index-3").await else {
            return;
        };
        let (dir, run) = fresh("sweep");
        let local = Arc::new(LocalFsStore::new(&dir));
        state.artifact_store = Some(local.clone());
        state.artifacts.retention_days = Some(2);
        let expire = |name: &'static str| {
            let (pool, run) = (state.write_pool.clone(), run.clone());
            async move {
                sqlx::query("UPDATE artifacts SET expires_at = '2000-01-01T00:00:00+00:00' WHERE run_id = $1 AND name = $2")
                    .bind(run)
                    .bind(name)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
        };
        let key = |name: &str| ArtifactKey::new(run.as_str(), "build", name);

        put(&state, &run, "build", "old.tar", None, b"old")
            .await
            .unwrap();
        put(&state, &run, "build", "new.tar", None, b"new")
            .await
            .unwrap();
        put(&state, &run, "build", "kept.tar", Some(1), b"kept")
            .await
            .unwrap();
        assert_eq!(
            days_from_now(&row(&state, &run, "new.tar").await.unwrap().2.unwrap()),
            2
        );

        // Nothing of this run's has expired: nothing of it is touched.
        let now = Utc::now();
        sweep(&state.write_pool, local.as_ref(), now).await.unwrap();
        for name in ["old.tar", "new.tar", "kept.tar"] {
            assert!(row(&state, &run, name).await.is_some(), "{name}");
            assert!(local.exists(&key(name)).await.unwrap(), "{name}");
        }
        // One has: its bytes and its row go, and the others stay.
        expire("old.tar").await;
        let swept = sweep(&state.write_pool, local.as_ref(), now).await.unwrap();
        assert!(swept.deleted >= 1 && swept.failed == 0, "{swept:?}");
        assert!(row(&state, &run, "old.tar").await.is_none());
        assert!(!local.exists(&key("old.tar")).await.unwrap());
        assert!(local.exists(&key("new.tar")).await.unwrap());
        assert!(local.exists(&key("kept.tar")).await.unwrap());

        // An upload that was begun and never finished is cleared up with
        // whatever arrived of it.
        begin(&state.write_pool, &key("half.bin"), now, &state.artifacts)
            .await
            .unwrap();
        local.put(&key("half.bin"), b"ha").await.unwrap();
        expire("half.bin").await;
        sweep(&state.write_pool, local.as_ref(), now).await.unwrap();
        assert!(row(&state, &run, "half.bin").await.is_none());
        assert!(!local.exists(&key("half.bin")).await.unwrap());

        // An artifact that expired and is being sent again is left alone:
        // the upload took its expiry away before its first byte.
        expire("new.tar").await;
        begin(&state.write_pool, &key("new.tar"), now, &state.artifacts)
            .await
            .unwrap();
        sweep(&state.write_pool, local.as_ref(), now).await.unwrap();
        assert_eq!(
            row(&state, &run, "new.tar").await.unwrap(),
            (None, None, None)
        );
        assert!(local.exists(&key("new.tar")).await.unwrap());

        // A store that fails to delete: the row is left, and put off, so
        // that it does not hold up what is behind it.
        expire("kept.tar").await;
        let away = Stubborn(false);
        let swept = sweep(&state.write_pool, &away, now).await.unwrap();
        assert!(swept.deleted == 0 && swept.failed >= 1, "{swept:?}");
        let put_off = row(&state, &run, "kept.tar").await.unwrap().2.unwrap();
        assert!(put_off > now.to_rfc3339(), "{put_off}");
        assert_eq!(
            sweep(&state.write_pool, &away, now).await.unwrap(),
            Swept::default(),
            "nothing is due again yet"
        );
        assert!(local.exists(&key("kept.tar")).await.unwrap());

        // A store that deletes nothing: said once, in a type, and the row stays expired.
        expire("kept.tar").await;
        let never = sweep(&state.write_pool, &Stubborn(true), now)
            .await
            .unwrap_err();
        assert!(never.downcast_ref::<Unsupported>().is_some(), "{never:#}");
        assert!(row(&state, &run, "kept.tar").await.is_some());
        // The directory then deletes it.
        sweep(&state.write_pool, local.as_ref(), now).await.unwrap();
        assert!(row(&state, &run, "kept.tar").await.is_none());
        assert!(!local.exists(&key("kept.tar")).await.unwrap());
        forget_run(&state, &run).await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}
