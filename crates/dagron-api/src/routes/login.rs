//! Self-contained auth: dagron-api owns login end-to-end, with no external IdP.
//!
//! `POST /api/login` verifies an email + password against the `users` table
//! (argon2 hashes) and mints the same HS256 `SessionClaims` JWT that the
//! `AuthUser` extractor already validates. `POST /api/users` (admin-only) adds
//! users. On startup `bootstrap_admin` seeds a first admin from env so the very
//! first login needs no manual DB step.

use argon2::password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use axum::extract::State;
use axum::http::header::SET_COOKIE;
use axum::http::{HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use jsonwebtoken::{encode, EncodingKey, Header};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::{AuthUser, SessionClaims, SESSION_COOKIE};
use crate::state::AppState;

/// Default session lifetime; override with DAGRON_SESSION_TTL_SECS.
fn ttl_secs() -> i64 {
    std::env::var("DAGRON_SESSION_TTL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(60 * 60 * 24 * 7)
}

// ── Login ───────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct LoginBody {
    pub email: String,
    pub password: String,
}

#[derive(Serialize)]
pub struct LoginResponse {
    pub token: String,
}

/// `POST /api/login` — verify credentials, set the HttpOnly session cookie and
/// also return the minted JWT in the body (for non-browser clients). 401 on any
/// bad email/password (message is deliberately generic).
pub async fn login(
    State(state): State<AppState>,
    Json(body): Json<LoginBody>,
) -> Result<impl IntoResponse, StatusCode> {
    let row: Option<(String, String, String, String)> = sqlx::query_as(
        "SELECT id, name, pw_hash, groups FROM users WHERE email = $1",
    )
    .bind(&body.email)
    // Auth is a primary-pool decision: read from write_pool so a just-created
    // user (or a fresh group change) is visible immediately and not subject to
    // replica lag if read_pool ever points at a read replica.
    .fetch_optional(&state.write_pool)
    .await
    .map_err(|e| {
        tracing::error!(error = ?e, "login query failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Verify the password. To avoid leaking which emails exist via response
    // timing, the no-such-user path still performs an Argon2 verify against a
    // fixed dummy hash before returning the same generic 401.
    let (id, name, pw_hash, groups) = match row {
        Some(r) => r,
        None => {
            if let Ok(dummy) = PasswordHash::new(dummy_pw_hash()) {
                let _ = Argon2::default().verify_password(body.password.as_bytes(), &dummy);
            }
            return Err(StatusCode::UNAUTHORIZED);
        }
    };

    let parsed = PasswordHash::new(&pw_hash).map_err(|e| {
        tracing::error!(error = ?e, "stored password hash is malformed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    if Argon2::default()
        .verify_password(body.password.as_bytes(), &parsed)
        .is_err()
    {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let groups: Vec<String> = serde_json::from_str(&groups).unwrap_or_default();
    let token = mint_token(&state.jwt_secret, &id, &body.email, &name, groups)
        .map_err(|e| {
            tracing::error!(error = ?e, "minting token failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let cookie = session_cookie(&token, ttl_secs(), state.cookie_secure);
    let set_cookie = HeaderValue::from_str(&cookie).map_err(|e| {
        tracing::error!(error = ?e, "building session cookie failed");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(([(SET_COOKIE, set_cookie)], Json(LoginResponse { token })))
}

/// `POST /api/logout` — clear the session cookie. Public on purpose: an expired
/// or otherwise unverifiable session must still be able to clear itself.
pub async fn logout(State(state): State<AppState>) -> Result<impl IntoResponse, StatusCode> {
    // Max-Age=0 with an empty value tells the browser to drop the cookie.
    let cookie = session_cookie("", 0, state.cookie_secure);
    let set_cookie = HeaderValue::from_str(&cookie).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(([(SET_COOKIE, set_cookie)], StatusCode::NO_CONTENT))
}

/// Build a `Set-Cookie` value for the session JWT. HttpOnly + SameSite=Lax;
/// `Secure` is added unless disabled for plain-HTTP local dev.
fn session_cookie(token: &str, max_age_secs: i64, secure: bool) -> String {
    let mut c =
        format!("{SESSION_COOKIE}={token}; HttpOnly; SameSite=Lax; Path=/; Max-Age={max_age_secs}");
    if secure {
        c.push_str("; Secure");
    }
    c
}

/// Encode an HS256 `SessionClaims` token valid for `ttl_secs()`.
fn mint_token(
    secret: &str,
    sub: &str,
    email: &str,
    name: &str,
    groups: Vec<String>,
) -> anyhow::Result<String> {
    let exp = (chrono::Utc::now().timestamp() + ttl_secs()) as usize;
    let claims = SessionClaims {
        sub: sub.to_string(),
        email: email.to_string(),
        name: name.to_string(),
        groups,
        exp,
    };
    Ok(encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )?)
}

// ── Create user (admin only) ──────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct CreateUserBody {
    pub email: String,
    pub password: String,
    pub name: String,
    #[serde(default)]
    pub groups: Vec<String>,
}

#[derive(Serialize)]
pub struct CreateUserResponse {
    pub id: String,
}

/// `POST /api/users` — create a user. Requires a caller in the `admin` group.
/// 409 if the email already exists.
pub async fn create_user(
    AuthUser(claims): AuthUser,
    State(state): State<AppState>,
    Json(body): Json<CreateUserBody>,
) -> Result<(StatusCode, Json<CreateUserResponse>), (StatusCode, String)> {
    if !claims.groups.iter().any(|g| g == "admin") {
        return Err((StatusCode::FORBIDDEN, "admin group required".to_string()));
    }
    if body.password.len() < 8 {
        return Err((StatusCode::BAD_REQUEST, "password must be at least 8 characters".to_string()));
    }

    let hash = hash_password(&body.password).map_err(|e| {
        tracing::error!(error = ?e, "hashing password failed");
        (StatusCode::INTERNAL_SERVER_ERROR, "unable to create user".to_string())
    })?;
    let id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let groups_json = serde_json::to_string(&body.groups).unwrap_or_else(|_| "[]".to_string());

    let res = sqlx::query(
        "INSERT INTO users (id, email, name, pw_hash, groups, created_at)
         VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (email) DO NOTHING",
    )
    .bind(&id)
    .bind(&body.email)
    .bind(&body.name)
    .bind(&hash)
    .bind(&groups_json)
    .bind(&now)
    .execute(&state.write_pool)
    .await
    .map_err(|e| {
        tracing::error!(error = ?e, "insert user failed");
        (StatusCode::INTERNAL_SERVER_ERROR, "unable to create user".to_string())
    })?;

    if res.rows_affected() == 0 {
        return Err((StatusCode::CONFLICT, "email already exists".to_string()));
    }
    Ok((StatusCode::CREATED, Json(CreateUserResponse { id })))
}

/// Argon2id hash of `password` with a random salt (PHC string form).
fn hash_password(password: &str) -> anyhow::Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| anyhow::anyhow!("argon2: {e}"))
}

/// A fixed, lazily-computed Argon2 hash used only to equalize timing on the
/// no-such-user login path, so account existence can't be probed via response
/// time. Never matches a real password.
fn dummy_pw_hash() -> &'static str {
    static H: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    H.get_or_init(|| {
        let salt = SaltString::generate(&mut OsRng);
        Argon2::default()
            .hash_password(b"timing-equalizer-not-a-real-password", &salt)
            .expect("hashing the dummy password should not fail")
            .to_string()
    })
}

// ── Bootstrap ─────────────────────────────────────────────────────────────────

/// Ensure the `users` table exists. dagron-api is its sole owner (the engine
/// never reads it), so it creates the table itself rather than depending on the
/// engine's migration having run first — removing any startup ordering race.
/// Mirrors migrations_pg/008_users.sql.
pub async fn ensure_schema(pool: &sqlx::postgres::PgPool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS users (
            id          TEXT PRIMARY KEY NOT NULL,
            email       TEXT NOT NULL UNIQUE,
            name        TEXT NOT NULL,
            pw_hash     TEXT NOT NULL,
            groups      TEXT NOT NULL DEFAULT '[]',
            created_at  TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await?;
    // No separate idx_users_email: the `email ... UNIQUE` constraint above
    // already creates a unique index Postgres uses for the WHERE email = $1 lookup.
    Ok(())
}

/// Seed a first admin from DAGRON_ADMIN_EMAIL / DAGRON_ADMIN_PASSWORD (and
/// optional DAGRON_ADMIN_NAME) if both are set. Idempotent: an existing email is
/// left untouched, so the password is not reset on every restart.
pub async fn bootstrap_admin(pool: &sqlx::postgres::PgPool) -> anyhow::Result<()> {
    let (Ok(email), Ok(password)) = (
        std::env::var("DAGRON_ADMIN_EMAIL"),
        std::env::var("DAGRON_ADMIN_PASSWORD"),
    ) else {
        return Ok(()); // not configured — skip
    };
    // Same minimum as API-created users; a weak seed must not slip through.
    if password.len() < 8 {
        anyhow::bail!("DAGRON_ADMIN_PASSWORD must be at least 8 characters");
    }
    let name = std::env::var("DAGRON_ADMIN_NAME").unwrap_or_else(|_| "Administrator".to_string());

    let hash = hash_password(&password)?;
    let id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();

    let res = sqlx::query(
        "INSERT INTO users (id, email, name, pw_hash, groups, created_at)
         VALUES ($1,$2,$3,$4,'[\"admin\"]',$5) ON CONFLICT (email) DO NOTHING",
    )
    .bind(&id)
    .bind(&email)
    .bind(&name)
    .bind(&hash)
    .bind(&now)
    .execute(pool)
    .await?;

    if res.rows_affected() > 0 {
        tracing::info!(%email, "bootstrapped admin user");
    } else {
        tracing::info!(%email, "admin user already exists — left unchanged");
    }
    Ok(())
}
