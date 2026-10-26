//! The remote runner protocol: `/runner/v1`.
//!
//! A runner (`dagron-runner`) dials in here; it holds no database
//! credentials. It registers once with a join token an admin minted
//! (`admin`), and is `runner:<id>` from then on, with a credential of its own
//! (`dgr_…`) that is stored as an API token is: its prefix to find it by and
//! its SHA-256 to compare. Each process of it opens a session, long-polls it
//! for tasks, heartbeats it, streams each task's log, and says how each came
//! out. The wire types are [`dagron_core::runner`]'s.
//!
//! - **The claim is the engine's own** ([`db::claim_for_session`]): the same
//!   SQL, priorities and pool budgets, under the session's holder
//!   (`runner:<id>/<session>`), so a task's fence is what an engine's would
//!   be. An engine whose `RUNNER_CLASSES` leaves out a runner's labels never
//!   claims those tasks.
//! - **One statement a heartbeat** renews the session, and with it every task
//!   it holds: task rows are not rewritten. A session whose lease
//!   lapses has its tasks taken back by the engine's session sweep.
//! - **A result is kept, not applied** ([`db::record_runner_result`]): an
//!   engine takes it and applies it as its own workers' results are applied,
//!   so retries, `repeat:`, `defer:`, the cache and dead letters are the
//!   engine's, for a remote task as for a local one. A word under a fence that
//!   is not the task's any more gets 409.
//!
//! Errors are RFC 9457 problem documents.

pub mod admin;
pub(crate) mod jobs;

use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use rand::RngCore;
use serde::Deserialize;
use serde_json::json;
use subtle::ConstantTimeEq;
use tokio::sync::broadcast;

use dagron_core::db;
use dagron_core::runner::{
    self as proto, Claimed, HeartbeatAck, LogSegment, OpenSession, Outcome, RegisterRequest,
    Registered, Report, Secrets, Session,
};

use crate::routes::tokens::hash_token;
use crate::state::AppState;

/// The most tasks one session or runner runs at once.
pub const MAX_CAPACITY: u32 = 256;

/// How long a claim long-polls where it does not say, and the longest it may.
/// Under a minute, so that a proxy's idle timeout does not cut it.
const CLAIM_WAIT_DEFAULT_SECS: u64 = 30;
const CLAIM_WAIT_MAX_SECS: u64 = 55;

/// The most labels a runner serves.
const MAX_LABELS: usize = 32;

/// How much of a credential is its lookup prefix, as an API token's.
const PREFIX_LEN: usize = 10;

/// What the routes are set with, read once as dagron-api starts
/// ([`settings_from_env`]).
pub struct Settings {
    /// The engine's pools (`POOLS`), which a session's claims count against.
    pub pool_caps: BTreeMap<String, i64>,
    /// The oldest runner release a session is opened for
    /// (`DAGRON_RUNNER_MIN_VERSION`).
    pub min_version: Option<String>,
    /// How often a waiting claim looks for work it was not told of
    /// (`DAGRON_RUNNER_CLAIM_RECHECK_MS`): a task made ready by an engine's
    /// tick sends no notification.
    pub recheck: Duration,
    /// The most a runner's request may carry (`DAGRON_RUNNER_BODY_LIMIT`).
    pub body_limit: usize,
    /// The most of a task's log that is kept (`DAGRON_LOG_TASK_CAP_BYTES`, as
    /// the engine reads it).
    pub log_cap: usize,
}

static SETTINGS: OnceLock<Settings> = OnceLock::new();

/// Read the routes' settings from the environment, once. A setting that
/// cannot be read is an error, so that dagron-api does not start with it:
/// `POOLS` above all, whose limits would otherwise not hold for remote work.
pub fn settings_from_env() -> anyhow::Result<&'static Settings> {
    if let Some(settings) = SETTINGS.get() {
        return Ok(settings);
    }
    let pool_caps = dagron_core::dag::parse_pools(&std::env::var("POOLS").unwrap_or_default())?;
    let min_version = std::env::var("DAGRON_RUNNER_MIN_VERSION")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    if let Some(min) = &min_version {
        if proto::version_at_least(min, min).is_none() {
            anyhow::bail!(
                "DAGRON_RUNNER_MIN_VERSION is not a release (MAJOR.MINOR.PATCH): '{min}'"
            );
        }
    }
    let number = |name: &str, default: u64| -> anyhow::Result<u64> {
        match std::env::var(name) {
            Ok(raw) if !raw.trim().is_empty() => raw
                .trim()
                .parse::<u64>()
                .map_err(|_| anyhow::anyhow!("{name} is not a whole number: '{raw}'")),
            _ => Ok(default),
        }
    };
    let recheck = Duration::from_millis(number("DAGRON_RUNNER_CLAIM_RECHECK_MS", 1000)?.max(100));
    let body_limit = number("DAGRON_RUNNER_BODY_LIMIT", 16 * 1024 * 1024)?.max(64 * 1024) as usize;
    let log_cap = number("DAGRON_LOG_TASK_CAP_BYTES", 64 * 1024 * 1024)?.max(1) as usize;
    Ok(SETTINGS.get_or_init(|| Settings {
        pool_caps,
        min_version,
        recheck,
        body_limit,
        log_cap,
    }))
}

fn settings() -> &'static Settings {
    settings_from_env().unwrap_or_else(|e| panic!("runner settings: {e:#}"))
}

/// An RFC 9457 problem document.
pub(crate) fn problem(status: StatusCode, title: &str, detail: impl Into<String>) -> Response {
    let body = json!({
        "type": "about:blank",
        "title": title,
        "status": status.as_u16(),
        "detail": detail.into(),
    });
    (
        status,
        [(header::CONTENT_TYPE, "application/problem+json")],
        body.to_string(),
    )
        .into_response()
}

fn internal(e: impl std::fmt::Display) -> Response {
    tracing::error!(error = %e, "runner protocol: a database call failed");
    problem(
        StatusCode::INTERNAL_SERVER_ERROR,
        "Internal error",
        "internal server error",
    )
}

fn unauthorized() -> Response {
    let mut r = problem(
        StatusCode::UNAUTHORIZED,
        "Unauthorized",
        "a runner credential is required (Authorization: Bearer dgr_…)",
    );
    r.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        header::HeaderValue::from_static("Bearer"),
    );
    r
}

fn session_gone() -> Response {
    problem(
        StatusCode::GONE,
        "Session gone",
        "the session has expired or was closed: stop its tasks and open another",
    )
}

fn bad_request(detail: impl Into<String>) -> Response {
    problem(StatusCode::BAD_REQUEST, "Bad request", detail)
}

/// A newly minted credential (`prefix_tag` is `dgr_` or `dgrj_`): what the
/// caller sees once, its lookup prefix and its hash.
pub(crate) fn mint(prefix_tag: &str) -> (String, String, String) {
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    let secret = URL_SAFE_NO_PAD.encode(bytes);
    let plaintext = format!("{prefix_tag}{secret}");
    let prefix = secret[..PREFIX_LEN].to_string();
    let hash = hash_token(&plaintext);
    (plaintext, prefix, hash)
}

/// The lookup prefix of a presented credential of this kind, if it is one.
pub(crate) fn prefix_of<'a>(presented: &'a str, prefix_tag: &str) -> Option<&'a str> {
    let secret = presented.strip_prefix(prefix_tag)?;
    (secret.len() >= PREFIX_LEN).then(|| &secret[..PREFIX_LEN])
}

/// Whether a presented credential is the one stored as `hash`, compared in
/// constant time.
pub(crate) fn matches(presented: &str, hash: &str) -> bool {
    bool::from(hash_token(presented).as_bytes().ct_eq(hash.as_bytes()))
}

/// Labels as a runner gives them: each a runner class, none twice, at least
/// one, and at most [`MAX_LABELS`]. `allowed` empty allows any.
pub(crate) fn valid_labels(asked: &[String], allowed: &[String]) -> Result<Vec<String>, String> {
    let asked: Vec<String> = if asked.is_empty() {
        allowed.to_vec()
    } else {
        asked.to_vec()
    };
    if asked.is_empty() {
        return Err("a runner serves at least one label".into());
    }
    if asked.len() > MAX_LABELS {
        return Err(format!("a runner serves at most {MAX_LABELS} labels"));
    }
    let mut out: Vec<String> = Vec::with_capacity(asked.len());
    for label in asked {
        let label = label.trim().to_string();
        dagron_core::dag::validate_runner_class(&label).map_err(|e| format!("label: {e}"))?;
        if !allowed.is_empty() && !allowed.contains(&label) {
            return Err(format!("label '{label}' is not one this runner may serve"));
        }
        if !out.contains(&label) {
            out.push(label);
        }
    }
    Ok(out)
}

fn valid_capacity(capacity: u32) -> Result<i32, String> {
    if capacity == 0 || capacity > MAX_CAPACITY {
        return Err(format!("capacity is 1 to {MAX_CAPACITY}"));
    }
    Ok(capacity as i32)
}

/// A short, printable text a runner says of itself (a name, an OS, a
/// release), or `None`.
fn short(text: Option<&str>, max: usize) -> Option<String> {
    text.map(str::trim)
        .filter(|t| !t.is_empty())
        .map(|t| t.chars().filter(|c| !c.is_control()).take(max).collect())
}

/// The runner a request is from.
pub(crate) struct Caller {
    pub id: String,
    pub labels: Vec<String>,
}

async fn caller(headers: &HeaderMap, state: &AppState) -> Result<Caller, Response> {
    let presented = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(crate::auth::extract_bearer)
        .ok_or_else(unauthorized)?;
    let prefix = prefix_of(presented, proto::CREDENTIAL_PREFIX).ok_or_else(unauthorized)?;
    let row: Option<(String, String, Vec<String>, Option<String>)> =
        sqlx::query_as("SELECT id, token_hash, labels, revoked_at FROM runners WHERE prefix = $1")
            .bind(prefix)
            .fetch_optional(&state.read_pool)
            .await
            .map_err(internal)?;
    match row {
        Some((id, hash, labels, None)) if matches(presented, &hash) => Ok(Caller { id, labels }),
        _ => Err(unauthorized()),
    }
}

/// A session of the caller's, as its routes read it.
struct SessionRow {
    holder: String,
    labels: Vec<String>,
    capacity: i32,
    secrets: String,
    draining: bool,
}

async fn session_of(state: &AppState, runner: &Caller, id: &str) -> Result<SessionRow, Response> {
    #[allow(clippy::type_complexity)]
    let row: Option<(String, Vec<String>, i32, String, String, Option<String>)> = sqlx::query_as(
        "SELECT holder, labels, capacity, secrets, lease_expires_at, draining_at
         FROM runner_sessions WHERE id = $1 AND runner_id = $2",
    )
    .bind(id)
    .bind(&runner.id)
    .fetch_optional(&state.read_pool)
    .await
    .map_err(internal)?;
    let Some((holder, labels, capacity, secrets, lease_expires_at, draining_at)) = row else {
        return Err(session_gone());
    };
    if lease_expires_at < chrono::Utc::now().to_rfc3339() {
        return Err(session_gone());
    }
    Ok(SessionRow {
        holder,
        labels,
        capacity,
        secrets,
        draining: draining_at.is_some(),
    })
}

// ── routes ───────────────────────────────────────────────────────────────────

/// `GET /runner/v1`: what this dagron-api speaks, asked before anything else
/// and with no credential.
pub async fn about() -> Response {
    Json(json!({
        "protocols": proto::PROTOCOLS,
        "min_version": settings().min_version,
    }))
    .into_response()
}

/// `POST /runner/v1/register`: a join token exchanged for a runner's own
/// credential, shown this once.
pub async fn register(
    State(state): State<AppState>,
    Json(body): Json<RegisterRequest>,
) -> Response {
    let capacity = match valid_capacity(body.capacity) {
        Ok(c) => c,
        Err(e) => return bad_request(e),
    };
    let refused = || {
        problem(
            StatusCode::UNAUTHORIZED,
            "Unauthorized",
            "the join token is not valid: it is unknown, revoked, expired or used up",
        )
    };
    let Some(prefix) = prefix_of(&body.join_token, proto::JOIN_TOKEN_PREFIX) else {
        return refused();
    };
    let mut tx = match state.write_pool.begin().await {
        Ok(tx) => tx,
        Err(e) => return internal(e),
    };
    #[allow(clippy::type_complexity)]
    let row: Option<(
        String,
        String,
        Vec<String>,
        i32,
        i32,
        String,
        Option<String>,
    )> = match sqlx::query_as(
        "SELECT id, token_hash, labels, max_uses, uses, expires_at, revoked_at
             FROM runner_join_tokens WHERE prefix = $1 FOR UPDATE",
    )
    .bind(prefix)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(row) => row,
        Err(e) => return internal(e),
    };
    let now = chrono::Utc::now().to_rfc3339();
    let Some((token_id, hash, allowed, max_uses, uses, expires_at, None)) = row else {
        return refused();
    };
    if !matches(&body.join_token, &hash) || uses >= max_uses || expires_at < now {
        return refused();
    }
    let labels = match valid_labels(&body.labels, &allowed) {
        Ok(labels) => labels,
        Err(e) => return bad_request(e),
    };
    let runner_id = uuid::Uuid::new_v4().to_string();
    let name =
        short(body.name.as_deref(), 128).unwrap_or_else(|| format!("runner-{}", &runner_id[..8]));
    let (token, token_prefix, token_hash) = mint(proto::CREDENTIAL_PREFIX);
    let inserted = sqlx::query(
        "INSERT INTO runners
             (id, name, prefix, token_hash, labels, os, arch, capacity, version,
              join_token_id, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
    )
    .bind(&runner_id)
    .bind(&name)
    .bind(&token_prefix)
    .bind(&token_hash)
    .bind(&labels)
    .bind(short(body.os.as_deref(), 32))
    .bind(short(body.arch.as_deref(), 32))
    .bind(capacity)
    .bind(short(body.version.as_deref(), 64))
    .bind(&token_id)
    .bind(&now)
    .execute(&mut *tx)
    .await;
    if let Err(e) = inserted {
        return internal(e);
    }
    if let Err(e) = sqlx::query("UPDATE runner_join_tokens SET uses = uses + 1 WHERE id = $1")
        .bind(&token_id)
        .execute(&mut *tx)
        .await
    {
        return internal(e);
    }
    if let Err(e) = tx.commit().await {
        return internal(e);
    }
    tracing::info!(runner = %format!("runner:{runner_id}"), %name, labels = ?labels, "a runner registered");
    (
        StatusCode::CREATED,
        Json(Registered {
            runner_id,
            token,
            labels,
        }),
    )
        .into_response()
}

/// `POST /runner/v1/sessions`: a runner process opens its hold on the tasks
/// it takes, in the newest protocol both ends speak.
pub async fn open_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<OpenSession>,
) -> Response {
    let runner = match caller(&headers, &state).await {
        Ok(r) => r,
        Err(r) => return r,
    };
    let Some(protocol) = proto::negotiate(proto::PROTOCOLS, &body.protocols) else {
        return problem(
            StatusCode::UPGRADE_REQUIRED,
            "No common protocol",
            format!(
                "this dagron-api speaks runner protocols {:?}, and the runner {:?}: upgrade the \
                 older of the two",
                proto::PROTOCOLS,
                body.protocols
            ),
        );
    };
    if let Some(min) = &settings().min_version {
        if proto::version_at_least(&body.version, min) != Some(true) {
            return problem(
                StatusCode::UPGRADE_REQUIRED,
                "Runner too old",
                format!(
                    "this dagron-api takes runners of release {min} or later, and this one is '{}'",
                    body.version
                ),
            );
        }
    }
    let labels = match valid_labels(&body.labels, &runner.labels) {
        Ok(labels) => labels,
        Err(e) => return bad_request(e),
    };
    let capacity = match valid_capacity(body.capacity) {
        Ok(c) => c,
        Err(e) => return bad_request(e),
    };
    let session_id = uuid::Uuid::new_v4().to_string();
    let holder = format!("runner:{}/{session_id}", runner.id);
    let lease_secs = proto::session_lease_secs();
    let now_dt = chrono::Utc::now();
    let now = now_dt.to_rfc3339();
    let lease_expires_at = (now_dt + chrono::TimeDelta::seconds(lease_secs)).to_rfc3339();
    let version = short(Some(&body.version), 64);
    let os = short(body.os.as_deref(), 32);
    let arch = short(body.arch.as_deref(), 32);
    let opened = sqlx::query(
        "INSERT INTO runner_sessions
             (id, runner_id, holder, labels, capacity, version, protocol, secrets, os, arch,
              lease_expires_at, started_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
    )
    .bind(&session_id)
    .bind(&runner.id)
    .bind(&holder)
    .bind(&labels)
    .bind(capacity)
    .bind(&version)
    .bind(protocol as i32)
    .bind(body.secrets.as_str())
    .bind(&os)
    .bind(&arch)
    .bind(&lease_expires_at)
    .bind(&now)
    .execute(&state.write_pool)
    .await;
    if let Err(e) = opened {
        return internal(e);
    }
    // Best-effort: what the runner said of itself last, for its listing.
    let _ = sqlx::query(
        "UPDATE runners SET last_seen_at = $2, version = COALESCE($3, version),
                os = COALESCE($4, os), arch = COALESCE($5, arch)
         WHERE id = $1",
    )
    .bind(&runner.id)
    .bind(&now)
    .bind(&version)
    .bind(&os)
    .bind(&arch)
    .execute(&state.write_pool)
    .await;
    tracing::info!(
        runner = %format!("runner:{}", runner.id), session_id = %session_id, protocol,
        labels = ?labels, capacity, secrets = body.secrets.as_str(), "a runner opened a session"
    );
    (
        StatusCode::CREATED,
        Json(Session {
            session_id,
            protocol,
            labels,
            capacity: capacity as u32,
            lease_secs,
            heartbeat_secs: proto::heartbeat_secs(lease_secs),
            lease_expires_at,
        }),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
pub struct ClaimParams {
    pub wait: Option<u64>,
}

/// `POST /runner/v1/sessions/{id}/claim?wait=30`: up to the session's free
/// capacity of ready tasks of the classes it serves, waiting up to `wait`
/// seconds for one. A task this protocol cannot carry is failed with why,
/// and not given out.
pub async fn claim(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<ClaimParams>,
    headers: HeaderMap,
) -> Response {
    let runner = match caller(&headers, &state).await {
        Ok(r) => r,
        Err(r) => return r,
    };
    let session = match session_of(&state, &runner, &id).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    if session.draining {
        return Json(Claimed {
            tasks: Vec::new(),
            drain: true,
        })
        .into_response();
    }
    let secrets_mode = Secrets::parse(&session.secrets).unwrap_or_default();
    let wait = Duration::from_secs(
        params
            .wait
            .unwrap_or(CLAIM_WAIT_DEFAULT_SECS)
            .min(CLAIM_WAIT_MAX_SECS),
    );
    let deadline = tokio::time::Instant::now() + wait;
    // Subscribed before the first look, so that a task made ready between
    // the look and the wait still wakes it.
    let mut events = state.tx.subscribe();
    let pool = &state.write_pool;
    loop {
        // Is there work, then is there room for it: a waiting claim reads,
        // and writes only when it claims.
        let ready = match db::ready_for_classes(pool, &session.labels).await {
            Ok(ready) => ready,
            Err(e) => return internal(e),
        };
        let free = if ready {
            match db::session_running(pool, &id, &session.holder).await {
                Ok(held) => i64::from(session.capacity) - held,
                Err(e) => return internal(e),
            }
        } else {
            0
        };
        if free > 0 {
            let claimed = match db::claim_for_session(
                pool,
                &id,
                &session.holder,
                free,
                &session.labels,
                &settings().pool_caps,
            )
            .await
            {
                Ok(claimed) => claimed,
                Err(e) => return internal(e),
            };
            let mut tasks = Vec::with_capacity(claimed.len());
            let any = !claimed.is_empty();
            for task in claimed {
                let fence = task.version.saturating_add(1);
                match jobs::prepare(pool, &task, secrets_mode, state.oidc.is_some()).await {
                    Ok(jobs::Prepared::Job(job)) => tasks.push(*job),
                    Ok(jobs::Prepared::Refused(why)) => {
                        tracing::warn!(
                            task = %task.name, task_id = %task.id, reason = %why,
                            "a task cannot run on a remote runner — marking task failed"
                        );
                        if let Err(e) =
                            db::mark_task_failed(pool, &task.id, &session.holder, fence, Some(why))
                                .await
                        {
                            tracing::error!(task_id = %task.id, error = %e, "marking a refused task failed");
                        }
                    }
                    #[cfg(feature = "enterprise")]
                    Ok(jobs::Prepared::Wait(why)) => {
                        tracing::info!(task = %task.name, task_id = %task.id, reason = %why, "waiting for a token budget");
                        if let Err(e) =
                            dagron_core::tokens::wait_for_room(pool, &task.id, fence).await
                        {
                            tracing::error!(task_id = %task.id, error = %e, "handing back a task that waits for a token budget");
                        }
                    }
                    // Left held by the session, which its runner never
                    // names: the session's next heartbeat gives it back.
                    Err(e) => tracing::error!(
                        task_id = %task.id, error = %e,
                        "a claimed task could not be made ready for its runner"
                    ),
                }
            }
            if !tasks.is_empty() {
                return Json(Claimed {
                    tasks,
                    drain: false,
                })
                .into_response();
            }
            if any {
                // Every one was refused: there may be more behind them.
                continue;
            }
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Json(Claimed::default()).into_response();
        }
        let recheck = deadline.min(now + settings().recheck);
        tokio::select! {
            event = events.recv() => {
                if matches!(event, Err(broadcast::error::RecvError::Closed)) {
                    tokio::time::sleep_until(recheck).await;
                }
            }
            _ = tokio::time::sleep_until(recheck) => {}
        }
    }
}

/// `POST /runner/v1/sessions/{id}/heartbeat`: the session renewed, and every
/// task it holds with it; the answer names those to stop.
pub async fn heartbeat(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<proto::Heartbeat>,
) -> Response {
    let runner = match caller(&headers, &state).await {
        Ok(r) => r,
        Err(r) => return r,
    };
    let held: Vec<(String, i64)> = body
        .running
        .into_iter()
        .map(|h| (h.task_id, h.fence))
        .collect();
    match db::renew_session(&state.write_pool, &id, &runner.id, &held).await {
        Ok(Some(renewed)) => Json(HeartbeatAck {
            lease_expires_at: renewed.lease_expires_at,
            cancel: renewed.cancel,
            drain: renewed.drain,
        })
        .into_response(),
        Ok(None) => session_gone(),
        Err(e) => internal(e),
    }
}

/// `DELETE /runner/v1/sessions/{id}`: the session drains. It takes no
/// more tasks; what it runs runs on, and the session ends once it holds
/// nothing. `200 {"ended": true}` where it holds nothing now, `202` with how
/// many it still runs otherwise: the runner says so again once they are over.
pub async fn close_session(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let runner = match caller(&headers, &state).await {
        Ok(r) => r,
        Err(r) => return r,
    };
    let now = chrono::Utc::now().to_rfc3339();
    let holder: Option<String> = match sqlx::query_scalar(
        "UPDATE runner_sessions SET draining_at = COALESCE(draining_at, $3)
         WHERE id = $1 AND runner_id = $2
         RETURNING holder",
    )
    .bind(&id)
    .bind(&runner.id)
    .bind(&now)
    .fetch_optional(&state.write_pool)
    .await
    {
        Ok(h) => h,
        Err(e) => return internal(e),
    };
    let Some(holder) = holder else {
        // Gone already: nothing is left to drain.
        return Json(json!({ "ended": true })).into_response();
    };
    let running = match db::session_running(&state.write_pool, &id, &holder).await {
        Ok(n) => n,
        Err(e) => return internal(e),
    };
    if running > 0 {
        return (
            StatusCode::ACCEPTED,
            Json(json!({ "draining": true, "running": running })),
        )
            .into_response();
    }
    let ended = sqlx::query(
        "DELETE FROM runner_sessions s WHERE s.id = $1 AND s.runner_id = $2
           AND NOT EXISTS (
               SELECT 1 FROM task_runs t
               WHERE t.session_id = s.id AND t.status = 'running' AND t.claimed_by = s.holder
           )",
    )
    .bind(&id)
    .bind(&runner.id)
    .execute(&state.write_pool)
    .await;
    match ended {
        Ok(r) if r.rows_affected() > 0 => {
            tracing::info!(session_id = %id, "a runner's session ended");
            Json(json!({ "ended": true })).into_response()
        }
        // A task came to be held in between: it drains.
        Ok(_) => (StatusCode::ACCEPTED, Json(json!({ "draining": true }))).into_response(),
        Err(e) => internal(e),
    }
}

/// `POST /runner/v1/tasks/{id}/logs/{seq}`: what a task wrote since its last
/// segment, kept as its live log. Segment 0, and a `reset` one (a seal),
/// replace the attempt's log. A segment written already, sent again with its
/// answer lost or held up past a later one, is answered as the first was and
/// not written twice ([`db::append_session_log`]). Kept up to twice the cap
/// and a MiB ([`dagron_core::runner::session_log_most`]): a log past its cap
/// is its beginning, of the cap, and its end, of at most the cap, whatever
/// the runner's host keeps of it.
pub async fn logs(
    State(state): State<AppState>,
    Path((task_id, seq)): Path<(String, u64)>,
    headers: HeaderMap,
    Json(body): Json<LogSegment>,
) -> Response {
    let runner = match caller(&headers, &state).await {
        Ok(r) => r,
        Err(r) => return r,
    };
    let session = match session_of(&state, &runner, &body.session_id).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    match db::append_session_log(
        &state.write_pool,
        &task_id,
        &session.holder,
        body.fence,
        seq,
        body.reset,
        &body.text,
        dagron_core::runner::session_log_most(settings().log_cap),
    )
    .await
    {
        Ok(db::SessionLogKept::Appended(run_id)) => {
            let _ = db::notify_runs(&state.write_pool, &[run_id]).await;
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(db::SessionLogKept::Duplicate) => StatusCode::NO_CONTENT.into_response(),
        Ok(db::SessionLogKept::NotKept) => problem(
            StatusCode::CONFLICT,
            "Not kept",
            "the task is not this session's under that fence, or its log is past what dagron-api \
             keeps of one (twice DAGRON_LOG_TASK_CAP_BYTES and 1 MiB)",
        ),
        Err(e) => internal(e),
    }
}

/// `POST /runner/v1/tasks/{id}/result`: how a task came out, kept for an
/// engine to apply. 409 where the session's claim on it, under that fence, is
/// gone: the word changes nothing, and the runner is done with the task.
pub async fn result(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Report>,
) -> Response {
    let runner = match caller(&headers, &state).await {
        Ok(r) => r,
        Err(r) => return r,
    };
    // A session that has expired may still say how a task came out, while the
    // task is still held under its claim: its row is there until the sweep
    // takes it, and the fence says whether the word is in time.
    let holder: Option<String> = match sqlx::query_scalar(
        "SELECT holder FROM runner_sessions WHERE id = $1 AND runner_id = $2",
    )
    .bind(&body.session_id)
    .bind(&runner.id)
    .fetch_optional(&state.read_pool)
    .await
    {
        Ok(h) => h,
        Err(e) => return internal(e),
    };
    let Some(holder) = holder else {
        return session_gone();
    };
    let output = tail(&body.output, settings().log_cap);
    match db::record_runner_result(
        &state.write_pool,
        &task_id,
        &holder,
        body.fence,
        body.outcome,
        body.exit_code,
        output,
    )
    .await
    {
        Ok(db::RunnerResultRecorded::Recorded(_)) => {
            if body.outcome != Outcome::Succeeded {
                tracing::info!(task_id = %task_id, outcome = body.outcome.as_str(), exit_code = ?body.exit_code, "a runner said how a task came out");
            }
            Json(json!({ "recorded": true })).into_response()
        }
        Ok(db::RunnerResultRecorded::Duplicate) => {
            Json(json!({ "recorded": true, "duplicate": true })).into_response()
        }
        Ok(db::RunnerResultRecorded::Stale) => problem(
            StatusCode::CONFLICT,
            "Stale fence",
            "the task is not this session's under that fence any more: the result changes nothing",
        ),
        Err(e) => internal(e),
    }
}

/// The end of `text`, at most `cap` bytes of it, cut on a character.
fn tail(text: &str, cap: usize) -> &str {
    if text.len() <= cap {
        return text;
    }
    let mut start = text.len() - cap;
    while !text.is_char_boundary(start) {
        start += 1;
    }
    &text[start..]
}

#[derive(Debug, Deserialize)]
pub struct QueueParams {
    /// Comma-separated. Unset: every label the runner serves.
    pub labels: Option<String>,
}

/// `GET /runner/v1/queue?labels=a,b`: the work there is for some of the
/// labels the runner serves, for whatever sizes the runners that serve them:
/// an autoscaler that is itself a runner.
pub async fn queue(
    State(state): State<AppState>,
    Query(params): Query<QueueParams>,
    headers: HeaderMap,
) -> Response {
    let runner = match caller(&headers, &state).await {
        Ok(r) => r,
        Err(r) => return r,
    };
    let asked: Vec<String> = params
        .labels
        .as_deref()
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect();
    let labels = match valid_labels(&asked, &runner.labels) {
        Ok(labels) => labels,
        Err(e) => return bad_request(e),
    };
    match db::runner_queue(&state.read_pool, &labels).await {
        Ok(labels) => Json(proto::Queue { labels }).into_response(),
        Err(e) => internal(e),
    }
}

/// Anything else under `/runner/v1`: a 404 problem, never the console.
pub async fn not_found() -> Response {
    problem(
        StatusCode::NOT_FOUND,
        "Not found",
        "no such runner protocol route",
    )
}

#[cfg(test)]
mod tests;
