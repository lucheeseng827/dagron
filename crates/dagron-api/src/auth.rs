//! Request authentication: the session JWT, or a personal access token.
//!
//! The token is HS256-signed by `routes::login` with the secret in
//! `DAGRON_JWT_SECRET`. dagron-api both mints and validates it — self-contained,
//! with no external IdP. Browsers carry it in the HttpOnly `dagron_session` cookie (so
//! it is out of reach of JS/XSS); non-browser clients may instead send it as
//! `Authorization: Bearer <jwt>`. The extractor accepts either.
//!
//! It also accepts a personal access token ([`crate::routes::tokens`]) in the
//! same header, told apart by its `dgp_` prefix. A token resolves to the same
//! [`SessionClaims`] a login would produce, with the user's groups read live
//! rather than copied at mint time — so demoting a user takes effect on their
//! existing tokens at once.
//!
//! [`authenticate`] is the one place either credential is checked. The audit
//! middleware needs the same answer as the extractor, and it does more than log:
//! it enforces the read-only `viewer` role. A second, JWT-only path there would
//! mean a viewer's *token* sailing past a check their *session* fails.

use axum::extract::FromRequestParts;
use axum::http::header::{AUTHORIZATION, COOKIE};
use axum::http::request::Parts;
use axum::http::StatusCode;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

use crate::state::AppState;

/// Name of the HttpOnly cookie carrying the session JWT.
pub const SESSION_COOKIE: &str = "dagron_session";

/// Session claims encoded in the HS256 session token minted by `routes::login`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionClaims {
    pub sub: String,
    pub email: String,
    pub name: String,
    #[serde(default)]
    pub groups: Vec<String>,
    pub exp: usize,
}

/// Extractor that authenticates a request and yields the validated claims.
/// Returns 401 on any missing/invalid/expired token.
pub struct AuthUser(pub SessionClaims);

impl FromRequestParts<AppState> for AuthUser {
    type Rejection = StatusCode;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        // `validate_exp` is on by default in `Validation::new`, so an expired
        // session fails inside `authenticate`; a token's expiry is checked there
        // too, against its own `expires_at` column.
        authenticate(&parts.headers, state)
            .await
            .map(AuthUser)
            .ok_or(StatusCode::UNAUTHORIZED)
    }
}

/// Authenticate a request by either credential. `None` means "no valid
/// credential" — callers that must reject map it to 401 themselves.
///
/// The single entry point for both the extractor and the audit middleware; see
/// the module docs for why a second path would be a security bug rather than
/// duplication.
pub async fn authenticate(
    headers: &axum::http::HeaderMap,
    state: &AppState,
) -> Option<SessionClaims> {
    let presented = bearer_or_cookie(headers)?;
    // A personal access token is recognised by its prefix; anything else is
    // tried as a JWT, so an ordinary session is untouched by this path.
    if crate::routes::tokens::prefix_of(presented).is_some() {
        return authenticate_token(presented, state).await;
    }
    decode_session(presented, &state.jwt_secret)
}

/// Resolve a personal access token to the owning user's live claims.
///
/// Unknown, revoked and expired tokens all return `None` — "not authenticated",
/// with no way for a caller to tell which, because that difference would confirm
/// whether a guessed token exists.
async fn authenticate_token(presented: &str, state: &AppState) -> Option<SessionClaims> {
    let prefix = crate::routes::tokens::prefix_of(presented)?;

    // The cleartext prefix is a unique index, so this is one row read rather
    // than hashing the presented token against every row in the table.
    let row: (String, String, Option<String>, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT user_id, token_hash, expires_at, revoked_at, last_used_at
         FROM api_tokens WHERE prefix = $1",
    )
    .bind(prefix)
    .fetch_optional(&state.read_pool)
    .await
    .ok()??;
    let (user_id, stored_hash, expires_at, revoked_at, last_used_at) = row;

    if revoked_at.is_some() {
        return None;
    }
    if let Some(exp) = &expires_at {
        match chrono::DateTime::parse_from_rfc3339(exp) {
            Ok(t) if chrono::Utc::now() >= t.with_timezone(&chrono::Utc) => return None,
            // Fail closed on a corrupt expiry: a credential whose lifetime
            // cannot be established is not one to trust.
            Err(_) => return None,
            Ok(_) => {}
        }
    }

    // Constant-time compare. The prefix already narrowed this to a single row so
    // the timing channel is thin, but a hash comparison that leaks its match
    // length is free to get right and awkward to explain.
    let computed = crate::routes::tokens::hash_token(presented);
    if computed.as_bytes().ct_eq(stored_hash.as_bytes()).unwrap_u8() != 1 {
        return None;
    }

    // Groups are read live rather than frozen onto the token, so a user demoted
    // to `viewer` loses write access through their existing tokens immediately.
    let user: (String, String, String) =
        sqlx::query_as("SELECT email, name, groups FROM users WHERE id = $1")
            .bind(&user_id)
            .fetch_optional(&state.read_pool)
            .await
            .ok()??;
    let (email, name, groups_json) = user;
    let groups: Vec<String> = serde_json::from_str(&groups_json).unwrap_or_default();

    if crate::routes::tokens::should_touch(last_used_at.as_deref()) {
        // Best effort: a failed bookkeeping write must not fail the request it
        // was only observing.
        let _ = sqlx::query("UPDATE api_tokens SET last_used_at = $1 WHERE prefix = $2")
            .bind(chrono::Utc::now().to_rfc3339())
            .bind(prefix)
            .execute(&state.write_pool)
            .await;
    }

    Some(SessionClaims {
        sub: user_id,
        email,
        name,
        groups,
        // A token's lifetime is `expires_at`, checked above. This field only
        // satisfies the shared claims shape; nothing re-validates it.
        exp: usize::MAX,
    })
}

fn decode_session(token: &str, jwt_secret: &str) -> Option<SessionClaims> {
    decode::<SessionClaims>(
        token,
        &DecodingKey::from_secret(jwt_secret.as_bytes()),
        &Validation::new(Algorithm::HS256),
    )
    .ok()
    .map(|d| d.claims)
}

/// The presented credential, from the `Authorization` header or the session
/// cookie, whichever is present.
fn bearer_or_cookie(headers: &axum::http::HeaderMap) -> Option<&str> {
    headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(extract_bearer)
        .or_else(|| {
            headers.get(COOKIE).and_then(|v| v.to_str().ok()).and_then(|header| {
                header.split(';').find_map(|kv| {
                    kv.trim()
                        .strip_prefix(SESSION_COOKIE)
                        .and_then(|rest| rest.strip_prefix('='))
                        .filter(|v| !v.is_empty())
                })
            })
        })
}

/// Authenticate, accepting **only** a real session — a personal access token is
/// refused with 403.
///
/// Guards token management itself. If a token could mint tokens, one leaked
/// credential would replace itself faster than an operator could revoke it, and
/// revocation would stop being a control.
pub struct SessionAuth(pub SessionClaims);

impl FromRequestParts<AppState> for SessionAuth {
    type Rejection = (StatusCode, String);

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let presented = bearer_or_cookie(&parts.headers)
            .ok_or((StatusCode::UNAUTHORIZED, "not authenticated".to_string()))?;
        if crate::routes::tokens::prefix_of(presented).is_some() {
            return Err((
                StatusCode::FORBIDDEN,
                "managing API tokens requires signing in with a password, not an API token"
                    .to_string(),
            ));
        }
        decode_session(presented, &state.jwt_secret)
            .map(SessionAuth)
            .ok_or((StatusCode::UNAUTHORIZED, "not authenticated".to_string()))
    }
}

/// Decode the session claims straight from request headers, without going
/// through the extractor.
///
/// **JWT only**, and kept only for callers with no async context. Anything that
/// makes an authorization decision must use [`authenticate`] instead, or a
/// personal access token reads as "unauthenticated" and skips the check.
pub fn claims_from_request(
    headers: &axum::http::HeaderMap,
    jwt_secret: &str,
) -> Option<SessionClaims> {
    let from_bearer = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(extract_bearer);
    let from_cookie = headers.get(COOKIE).and_then(|v| v.to_str().ok()).and_then(|header| {
        header.split(';').find_map(|kv| {
            kv.trim()
                .strip_prefix(SESSION_COOKIE)
                .and_then(|rest| rest.strip_prefix('='))
                .filter(|v| !v.is_empty())
        })
    });
    let token = from_bearer.or(from_cookie)?;
    decode::<SessionClaims>(
        token,
        &DecodingKey::from_secret(jwt_secret.as_bytes()),
        &Validation::new(Algorithm::HS256),
    )
    .ok()
    .map(|d| d.claims)
}

/// Pull the session JWT out of the `dagron_session` cookie, if present.
fn token_from_cookie(parts: &Parts) -> Option<&str> {
    let header = parts.headers.get(COOKIE)?.to_str().ok()?;
    header.split(';').find_map(|kv| {
        kv.trim()
            .strip_prefix(SESSION_COOKIE)
            .and_then(|rest| rest.strip_prefix('='))
            .filter(|v| !v.is_empty())
    })
}

/// Pull the token out of a `Bearer <token>` header value (scheme case-insensitive).
fn extract_bearer(value: &str) -> Option<&str> {
    let mut parts = value.splitn(2, ' ');
    match (parts.next(), parts.next()) {
        (Some(scheme), Some(token)) if scheme.eq_ignore_ascii_case("Bearer") => {
            let t = token.trim();
            if t.is_empty() {
                None
            } else {
                Some(t)
            }
        }
        _ => None,
    }
}
