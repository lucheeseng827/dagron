//! UI-configurable instance settings, starting with global notification
//! defaults.
//!
//! Stored as JSON values in a `ui_settings` key/value table owned by
//! dagron-api. The engine reads the same row (best-effort) when a run
//! finalizes, so defaults configured here apply to **every** run — on top of,
//! not instead of, any per-workflow `notify:` block (a spec target with the
//! same URL wins, so nothing fires twice).

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::auth::{AuthUser, SessionClaims};
use crate::state::AppState;

/// `ui_settings` key holding the JSON-encoded [`NotificationSettings`].
pub const NOTIFY_KEY: &str = "notifications";

/// Instance-wide notification routing is an admin concern: the stored Slack
/// URL is a bearer-like secret, PUT redirects every run's outcome data, and
/// the test endpoint makes the server POST outbound — none of which a regular
/// operator account should control. Mirrors the admin gate on user/audit routes.
fn require_admin(claims: &SessionClaims) -> Result<(), (StatusCode, String)> {
    if claims.groups.iter().any(|g| g == "admin") {
        Ok(())
    } else {
        Err((StatusCode::FORBIDDEN, "admin group required".to_string()))
    }
}

const ALLOWED_EVENTS: &[&str] = &["succeeded", "failed", "cancelled", "deadline_exceeded"];

/// Ensure the `ui_settings` table exists. dagron-api is its sole writer; the
/// engine only reads it (and tolerates it not existing).
pub async fn ensure_schema(pool: &sqlx::postgres::PgPool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS ui_settings (
            key         TEXT PRIMARY KEY NOT NULL,
            value       TEXT NOT NULL,
            updated_at  TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Global notification defaults, applied by the engine to every finalized run
/// and soft-deadline breach. Empty `*_on` lists mean each target's built-in
/// default: Slack = incidents only (`failed` + `deadline_exceeded`), webhook =
/// every event.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NotificationSettings {
    #[serde(default)]
    pub slack_enabled: bool,
    #[serde(default)]
    pub slack_webhook_url: String,
    #[serde(default)]
    pub slack_on: Vec<String>,
    #[serde(default)]
    pub webhook_enabled: bool,
    #[serde(default)]
    pub webhook_url: String,
    #[serde(default)]
    pub webhook_on: Vec<String>,
}

/// `GET /api/settings/notifications` — the stored defaults (or all-off).
/// Admin only: the stored webhook URLs are effectively secrets.
pub async fn get_notifications(
    AuthUser(claims): AuthUser,
    State(state): State<AppState>,
) -> Result<Json<NotificationSettings>, (StatusCode, String)> {
    require_admin(&claims)?;
    let raw: Option<String> =
        sqlx::query_scalar("SELECT value FROM ui_settings WHERE key = $1")
            .bind(NOTIFY_KEY)
            .fetch_optional(&state.read_pool)
            .await
            .map_err(internal_msg)?;
    let settings = raw
        .and_then(|v| serde_json::from_str(&v).ok())
        .unwrap_or_default();
    Ok(Json(settings))
}

fn validate(s: &NotificationSettings) -> Result<(), (StatusCode, String)> {
    let url_ok = |u: &str| u.starts_with("https://") || u.starts_with("http://");
    if s.slack_enabled && !url_ok(s.slack_webhook_url.trim()) {
        return Err((
            StatusCode::BAD_REQUEST,
            "slack_webhook_url must be an http(s) URL when Slack is enabled".to_string(),
        ));
    }
    if s.webhook_enabled && !url_ok(s.webhook_url.trim()) {
        return Err((
            StatusCode::BAD_REQUEST,
            "webhook_url must be an http(s) URL when the webhook is enabled".to_string(),
        ));
    }
    for ev in s.slack_on.iter().chain(s.webhook_on.iter()) {
        if !ALLOWED_EVENTS.contains(&ev.as_str()) {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("unknown event '{ev}' (expected one of {ALLOWED_EVENTS:?})"),
            ));
        }
    }
    Ok(())
}

/// `PUT /api/settings/notifications` — validate + upsert the defaults.
/// Admin only; the write is audit-logged like every mutation.
pub async fn put_notifications(
    AuthUser(claims): AuthUser,
    State(state): State<AppState>,
    Json(mut body): Json<NotificationSettings>,
) -> Result<Json<NotificationSettings>, (StatusCode, String)> {
    require_admin(&claims)?;
    body.slack_webhook_url = body.slack_webhook_url.trim().to_string();
    body.webhook_url = body.webhook_url.trim().to_string();
    validate(&body)?;
    let json = serde_json::to_string(&body)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO ui_settings (key, value, updated_at) VALUES ($1,$2,$3)
         ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = $3",
    )
    .bind(NOTIFY_KEY)
    .bind(&json)
    .bind(&now)
    .execute(&state.write_pool)
    .await
    .map_err(internal_msg)?;
    tracing::info!("notification defaults updated");
    Ok(Json(body))
}

#[derive(Serialize)]
pub struct TestResult {
    /// Per-target outcome: "ok", "skipped (disabled)", or the error text.
    pub slack: String,
    pub webhook: String,
}

/// `POST /api/settings/notifications/test` — send a test message to each
/// *enabled* target in the request body (test what's on screen, saved or not),
/// and report per-target outcomes instead of failing the whole call.
/// Admin only: this makes the server POST to a caller-supplied URL.
pub async fn test_notifications(
    AuthUser(claims): AuthUser,
    State(_state): State<AppState>,
    Json(mut body): Json<NotificationSettings>,
) -> Result<Json<TestResult>, (StatusCode, String)> {
    require_admin(&claims)?;
    // Same preprocessing as the save path, so a URL that saves also tests.
    body.slack_webhook_url = body.slack_webhook_url.trim().to_string();
    body.webhook_url = body.webhook_url.trim().to_string();
    validate(&body)?;
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    let slack = if body.slack_enabled {
        let payload = serde_json::json!({
            "text": "🔔 dagron test notification — global Slack target is configured correctly."
        });
        send(&client, &body.slack_webhook_url, &payload).await
    } else {
        "skipped (disabled)".to_string()
    };
    let webhook = if body.webhook_enabled {
        let payload = serde_json::json!({
            "event": "test",
            "run_id": "00000000-0000-0000-0000-000000000000",
            "workflow": "dagron-test",
            "status": "test",
            "at": chrono::Utc::now().to_rfc3339(),
        });
        send(&client, &body.webhook_url, &payload).await
    } else {
        "skipped (disabled)".to_string()
    };

    Ok(Json(TestResult { slack, webhook }))
}

async fn send(client: &reqwest::Client, url: &str, payload: &serde_json::Value) -> String {
    match client.post(url).json(payload).send().await {
        Ok(res) if res.status().is_success() => "ok".to_string(),
        Ok(res) => format!("endpoint answered {}", res.status()),
        Err(e) => format!("request failed: {e}"),
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
