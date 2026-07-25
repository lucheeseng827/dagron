//! Local identity provider: verify credentials against the local `users` table.
//!
//! This is the default behind the [`IdentityProvider`] seam. An alternate
//! provider can swap an SSO backend in its place; dagron-api keeps owning the
//! session cookie either way (see [`routes::login`](crate::routes::login)).

use anyhow::Result;
use async_trait::async_trait;
use dagron_identity::{IdentityProvider, VerifiedUser};
use sqlx::postgres::PgPool;

use crate::pwhash;

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

        // Hashing is bounded and off the async runtime — see `crate::pwhash`. The
        // no-such-user case goes through the same call (with `None`), which still
        // runs a full verify against a dummy hash, so response time doesn't leak
        // which emails exist.
        let stored = row.as_ref().map(|(_, _, _, pw_hash, _)| pw_hash.clone());
        if !pwhash::verify(password.to_string(), stored).await? {
            return Ok(None);
        }
        let Some((id, db_email, name, _pw_hash, groups)) = row else {
            // Unreachable: `verify` returns false for `None`, handled above.
            return Ok(None);
        };

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
