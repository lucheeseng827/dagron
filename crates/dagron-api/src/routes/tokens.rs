//! Personal access tokens — long-lived, named, revocable API credentials.
//!
//! The session JWT that `routes::login` mints is the right credential for a
//! browser and the wrong one for everything else: it is short-lived by design,
//! so a CI job holding one has to store the *password* that mints it. A password
//! cannot be revoked without locking the human out, cannot be told apart from
//! the human's own traffic, and is the same secret everywhere it is copied.
//!
//! A token fixes each of those: it is named, it can be revoked on its own, and
//! `last_used_at` shows whether anything still depends on it.
//!
//! ## What is stored
//!
//! Only `sha256(token)`. A database dump yields no working credential, and the
//! plaintext exists exactly once, in the response to the call that created it —
//! there is no endpoint that can show it again, because there is nothing to
//! show.
//!
//! SHA-256, not Argon2, and the reason matters. Argon2 is deliberately slow to
//! make guessing a *low-entropy human password* expensive. A token is 32 random
//! bytes; there is no guess to slow down. And unlike a password hash, which is
//! computed once at login, this one is computed on **every authenticated
//! request** — an Argon2 verify there (19 MiB per call, by design) would be a
//! denial of service we inflicted on ourselves.
//!
//! ## Minting requires a password
//!
//! Creating and revoking tokens needs a *session* — a real login — and is
//! refused when the caller authenticated with a token ([`SessionAuth`]). Without
//! that rule a single leaked token mints replacements faster than an operator
//! can revoke them, and revocation stops being a control. It is the same reason
//! `sudo` asks again.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::auth::SessionAuth;
use crate::state::AppState;

/// Prefix on every token, so one found in a log or a config file is instantly
/// identifiable as a dagron credential — and greppable by secret scanners.
pub const TOKEN_PREFIX: &str = "dgp_";

/// Characters of the secret kept in cleartext as the lookup key and the human
/// disambiguator. 10 base64url chars is ~60 bits — collisions are not a concern
/// at any realistic number of tokens, and it is enough for a person to tell two
/// tokens apart in a list.
const PREFIX_LEN: usize = 10;

/// How stale `last_used_at` may get before a request refreshes it. The question
/// it answers is "is anything still using this", so minute resolution is ample —
/// and writing on every request would add a write to every read.
const TOUCH_INTERVAL_SECS: i64 = 60;

// ── wire types ───────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CreateToken {
    pub name: String,
    /// Days until the token expires. Omit for a token that never expires.
    pub expires_in_days: Option<i64>,
}

/// The **only** response that ever carries the plaintext token.
#[derive(Debug, Serialize)]
pub struct CreatedToken {
    pub id: String,
    pub name: String,
    pub prefix: String,
    /// Shown once. There is no way to retrieve it later.
    pub token: String,
    pub expires_at: Option<String>,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct TokenRow {
    pub id: String,
    pub name: String,
    /// The cleartext head only — never the secret.
    pub prefix: String,
    pub created_at: String,
    pub expires_at: Option<String>,
    pub last_used_at: Option<String>,
    pub revoked_at: Option<String>,
}

// ── minting ──────────────────────────────────────────────────────────────────

/// A freshly generated token: what the caller sees, and what we store.
pub struct MintedToken {
    pub plaintext: String,
    pub prefix: String,
    pub hash: String,
}

/// Generate a token from the OS CSPRNG.
///
/// 32 bytes — the same 256-bit floor the session signing key uses. Split out so
/// the tests can assert the shape without reaching into a handler.
pub fn mint() -> MintedToken {
    let mut bytes = [0u8; 32];
    // `OsRng` (via `rand::rngs::OsRng`) is the operating system's CSPRNG. A
    // seeded/userspace generator would be wrong here: a predictable token is a
    // forgeable one.
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    let secret = URL_SAFE_NO_PAD.encode(bytes);
    let plaintext = format!("{TOKEN_PREFIX}{secret}");
    let prefix = secret[..PREFIX_LEN].to_string();
    let hash = hash_token(&plaintext);
    MintedToken { plaintext, prefix, hash }
}

/// SHA-256 of the whole token string, lowercase hex. See the module docs for
/// why this is not a password KDF.
pub fn hash_token(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    let mut out = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Pull the lookup prefix out of a presented token, if it looks like one at all.
/// `None` means "this is not a dagron token" — the caller then tries the JWT
/// path instead, so an ordinary session bearer is unaffected.
pub fn prefix_of(token: &str) -> Option<&str> {
    let secret = token.strip_prefix(TOKEN_PREFIX)?;
    (secret.len() >= PREFIX_LEN).then(|| &secret[..PREFIX_LEN])
}

fn now() -> String {
    chrono::Utc::now().to_rfc3339()
}

// ── handlers ─────────────────────────────────────────────────────────────────

/// Map a sqlx error to an opaque 500 — the detail is logged server-side, never
/// returned, so a client cannot read database internals (column, constraint or
/// driver text) out of the response body. Mirrors `routes::environments`.
fn internal_msg(err: sqlx::Error) -> (StatusCode, String) {
    tracing::error!(error = ?err, "api token db query failed");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_string())
}

/// `POST /api/tokens` — mint a token for the calling user.
///
/// Requires a session ([`SessionAuth`]): a token cannot mint another token.
pub async fn create_token(
    auth: SessionAuth,
    State(state): State<AppState>,
    Json(body): Json<CreateToken>,
) -> Result<(StatusCode, Json<CreatedToken>), (StatusCode, String)> {
    let name = body.name.trim().to_string();
    if name.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "name must not be empty".into()));
    }
    if let Some(days) = body.expires_in_days {
        if days <= 0 {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("expires_in_days must be positive, got {days}"),
            ));
        }
    }

    let minted = mint();
    let id = uuid::Uuid::new_v4().to_string();
    // Bound the expiry against chrono's range before converting: an unchecked
    // `Duration::days` (or the datetime addition) panics on a huge value, which
    // would turn a token-mint request into a handler crash.
    let expires_at = match body.expires_in_days {
        None => None,
        Some(d) => {
            let at = chrono::Duration::try_days(d)
                .and_then(|dur| chrono::Utc::now().checked_add_signed(dur))
                .ok_or((
                    StatusCode::BAD_REQUEST,
                    format!("expires_in_days is out of range: {d}"),
                ))?;
            Some(at.to_rfc3339())
        }
    };

    sqlx::query(
        "INSERT INTO api_tokens
            (id, name, user_id, user_email, prefix, token_hash, created_at, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(&id)
    .bind(&name)
    .bind(&auth.0.sub)
    .bind(&auth.0.email)
    .bind(&minted.prefix)
    .bind(&minted.hash)
    .bind(now())
    .bind(&expires_at)
    .execute(&state.write_pool)
    .await
    .map_err(internal_msg)?;

    // Deliberately not logged, at any level: the whole point of hashing the
    // stored copy is defeated if the plaintext lands in a log aggregator.
    tracing::info!(token_id = %id, name = %name, user = %auth.0.email, "api token created");

    Ok((
        StatusCode::CREATED,
        Json(CreatedToken {
            id,
            name,
            prefix: format!("{TOKEN_PREFIX}{}", minted.prefix),
            token: minted.plaintext,
            expires_at,
        }),
    ))
}

/// `GET /api/tokens` — the calling user's tokens, revoked ones included.
///
/// Scoped to the caller: a token list is a map of an account's automated access,
/// and one user has no business reading another's.
pub async fn list_tokens(
    auth: SessionAuth,
    State(state): State<AppState>,
) -> Result<Json<Vec<TokenRow>>, (StatusCode, String)> {
    let rows = sqlx::query_as::<_, TokenRow>(
        "SELECT id, name, prefix, created_at, expires_at, last_used_at, revoked_at
         FROM api_tokens WHERE user_id = $1 ORDER BY created_at DESC",
    )
    .bind(&auth.0.sub)
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal_msg)?;
    // Show the prefix the way the caller first saw it, `dgp_` and all —
    // otherwise the string in this list does not visibly match the one they
    // copied, and picking the right token to revoke turns into guesswork.
    let rows = rows
        .into_iter()
        .map(|mut r| {
            r.prefix = format!("{TOKEN_PREFIX}{}", r.prefix);
            r
        })
        .collect();
    Ok(Json(rows))
}

/// `DELETE /api/tokens/{id}` — revoke.
///
/// Marks the row rather than deleting it: after an incident the question is
/// "what existed and when was it stopped", and a deleted row cannot answer it.
/// Revoking twice is not an error — the caller wanted it dead, and it is.
pub async fn revoke_token(
    auth: SessionAuth,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let res = sqlx::query(
        "UPDATE api_tokens SET revoked_at = $1
         WHERE id = $2 AND user_id = $3 AND revoked_at IS NULL",
    )
    .bind(now())
    .bind(&id)
    .bind(&auth.0.sub)
    .execute(&state.write_pool)
    .await
    .map_err(internal_msg)?;

    if res.rows_affected() == 0 {
        // Either it never existed, belongs to someone else, or is already
        // revoked. One answer for all three: which of them it is would tell an
        // attacker whether a token id is real.
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM api_tokens WHERE id = $1 AND user_id = $2",
        )
        .bind(&id)
        .bind(&auth.0.sub)
        .fetch_optional(&state.read_pool)
        .await
        .map_err(internal_msg)?
        .is_some();
        if !exists {
            return Err((StatusCode::NOT_FOUND, format!("token '{id}' not found")));
        }
    }
    tracing::info!(token_id = %id, user = %auth.0.email, "api token revoked");
    Ok(StatusCode::NO_CONTENT)
}

/// Should `last_used_at` be refreshed? See [`TOUCH_INTERVAL_SECS`].
pub fn should_touch(last_used_at: Option<&str>) -> bool {
    let Some(prev) = last_used_at else {
        return true;
    };
    match chrono::DateTime::parse_from_rfc3339(prev) {
        Ok(t) => (chrono::Utc::now() - t.with_timezone(&chrono::Utc)).num_seconds()
            >= TOUCH_INTERVAL_SECS,
        // An unparseable stamp is corrupt; overwriting it with a good one is
        // strictly an improvement.
        Err(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_minted_token_is_prefixed_and_high_entropy() {
        let t = mint();
        assert!(t.plaintext.starts_with(TOKEN_PREFIX));
        // 32 bytes base64url-encoded, unpadded.
        assert_eq!(t.plaintext.len(), TOKEN_PREFIX.len() + 43);
        assert_eq!(t.prefix.len(), PREFIX_LEN);
        assert!(t.plaintext[TOKEN_PREFIX.len()..].starts_with(&t.prefix));
    }

    #[test]
    fn tokens_do_not_repeat() {
        // Not a statistical test — just a tripwire for a generator wired to a
        // constant seed, which is the way this goes wrong in practice.
        let a = mint();
        let b = mint();
        assert_ne!(a.plaintext, b.plaintext);
        assert_ne!(a.hash, b.hash);
    }

    #[test]
    fn the_stored_hash_is_not_the_token() {
        let t = mint();
        assert_ne!(t.hash, t.plaintext);
        assert_eq!(t.hash.len(), 64, "sha256 as hex");
        assert!(!t.hash.contains(&t.plaintext[TOKEN_PREFIX.len()..]));
        // And it is reproducible from the plaintext, which is what verification
        // relies on.
        assert_eq!(hash_token(&t.plaintext), t.hash);
    }

    #[test]
    fn hashing_is_sensitive_to_every_character() {
        let a = hash_token("dgp_aaaaaaaaaa");
        let b = hash_token("dgp_aaaaaaaaab");
        assert_ne!(a, b);
    }

    #[test]
    fn prefix_of_only_matches_dagron_tokens() {
        let t = mint();
        assert_eq!(prefix_of(&t.plaintext), Some(t.prefix.as_str()));
        // A session JWT must fall through to the JWT path untouched.
        assert_eq!(prefix_of("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.abc.def"), None);
        assert_eq!(prefix_of(""), None);
        // Prefixed but too short to carry a lookup key: not a token.
        assert_eq!(prefix_of("dgp_short"), None);
    }

    #[test]
    fn touch_throttles_but_never_skips_a_first_use() {
        assert!(should_touch(None), "a token never used must record its first use");
        let just_now = chrono::Utc::now().to_rfc3339();
        assert!(!should_touch(Some(&just_now)));
        let stale = (chrono::Utc::now() - chrono::Duration::seconds(TOUCH_INTERVAL_SECS + 1))
            .to_rfc3339();
        assert!(should_touch(Some(&stale)));
        assert!(should_touch(Some("not a timestamp")));
    }
}
