//! Bearer-token auth that validates dagron-api's own session JWT.
//!
//! The token is HS256-signed by `routes::login` with the secret in
//! `DAGRON_JWT_SECRET`. dagron-api both mints and validates it — self-contained,
//! with no external IdP. Browsers carry it in the HttpOnly `dagron_session` cookie (so
//! it is out of reach of JS/XSS); non-browser clients may instead send it as
//! `Authorization: Bearer <jwt>`. The extractor accepts either.

use axum::extract::FromRequestParts;
use axum::http::header::{AUTHORIZATION, COOKIE};
use axum::http::request::Parts;
use axum::http::StatusCode;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};

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
        let token = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(extract_bearer)
            .or_else(|| token_from_cookie(parts))
            .ok_or(StatusCode::UNAUTHORIZED)?;

        // validate_exp is on by default in Validation::new.
        let data = decode::<SessionClaims>(
            token,
            &DecodingKey::from_secret(state.jwt_secret.as_bytes()),
            &Validation::new(Algorithm::HS256),
        )
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

        Ok(AuthUser(data.claims))
    }
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
