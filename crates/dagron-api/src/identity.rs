//! Local identity provider: verify credentials against the local `users` table.
//!
//! This is the default behind the [`IdentityProvider`] seam. An alternate
//! provider can swap an SSO backend in its place; dagron-api keeps owning the
//! session cookie either way (see [`routes::login`](crate::routes::login)).

use anyhow::Result;
use argon2::password_hash::{rand_core::OsRng, PasswordHash, PasswordVerifier, SaltString};
use argon2::{Argon2, PasswordHasher};
use async_trait::async_trait;
use dagron_identity::{IdentityProvider, VerifiedUser};
use sqlx::postgres::PgPool;

/// Verifies email + password against the `users` table (argon2 PHC hashes).
pub struct LocalIdentityProvider {
    pool: PgPool,
}

impl LocalIdentityProvider {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl IdentityProvider for LocalIdentityProvider {
    async fn authenticate_password(
        &self,
        email: &str,
        password: &str,
    ) -> Result<Option<VerifiedUser>> {
        // Auth reads the primary so a just-created user / fresh group change is
        // visible immediately (no read-replica lag).
        let row: Option<(String, String, String, String, String)> =
            sqlx::query_as("SELECT id, email, name, pw_hash, groups FROM users WHERE email = $1")
                .bind(email)
                .fetch_optional(&self.pool)
                .await?;

        // To avoid leaking which emails exist via response timing, the no-such-user
        // path still performs an Argon2 verify against a fixed dummy hash.
        let (id, db_email, name, pw_hash, groups) = match row {
            Some(r) => r,
            None => {
                if let Ok(dummy) = PasswordHash::new(dummy_pw_hash()) {
                    let _ = Argon2::default().verify_password(password.as_bytes(), &dummy);
                }
                return Ok(None);
            }
        };

        let parsed = PasswordHash::new(&pw_hash)
            .map_err(|e| anyhow::anyhow!("stored password hash is malformed: {e}"))?;
        if Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_err()
        {
            return Ok(None);
        }

        // Surface corrupt authorization data rather than silently treating it as
        // "no groups" — an empty set here would be a quiet RBAC downgrade.
        let groups: Vec<String> = serde_json::from_str(&groups)
            .map_err(|e| anyhow::anyhow!("stored groups JSON is malformed: {e}"))?;
        Ok(Some(VerifiedUser {
            id,
            // Canonical email from the DB, not the (possibly variant-cased) request
            // input, so the verified principal can't carry an alias of the account.
            email: db_email,
            name,
            groups,
        }))
    }
}

/// A fixed, lazily-computed Argon2 hash used only to equalize timing on the
/// no-such-user path, so account existence can't be probed via response time.
/// Never matches a real password.
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
