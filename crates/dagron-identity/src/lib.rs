//! The dagron-api identity seam.
//!
//! [`IdentityProvider`] abstracts **authentication** — *who is this user* — so the
//! OSS edition ships self-contained local login (argon2 against the `users` table)
//! while a downstream edition can swap in an external IdP behind the same trait,
//! without forking dagron-api. dagron-api always owns the **session** (its HS256
//! cookie); a provider only resolves an identity from a credential.
//!
//! One-way dependency: this trait is OSS; downstream identity impls depend on it,
//! never the reverse.

use anyhow::Result;
use async_trait::async_trait;

/// An authenticated principal resolved by an [`IdentityProvider`]. dagron-api mints
/// its session JWT from this.
#[derive(Debug, Clone)]
pub struct VerifiedUser {
    pub id: String,
    pub email: String,
    pub name: String,
    pub groups: Vec<String>,
}

/// Pluggable authentication backend for dagron-api.
#[async_trait]
pub trait IdentityProvider: Send + Sync {
    /// Verify an email + password credential. `Ok(Some)` on success; `Ok(None)` on
    /// bad credentials (callers return a generic 401). `Err` only for an actual
    /// backend failure.
    ///
    /// Redirect-based SSO providers (e.g. OIDC) return `Ok(None)` here and drive
    /// the browser flow via the redirect/callback surface added with their
    /// implementation; they advertise that via [`supports_password_login`].
    ///
    /// [`supports_password_login`]: IdentityProvider::supports_password_login
    async fn authenticate_password(
        &self,
        email: &str,
        password: &str,
    ) -> Result<Option<VerifiedUser>>;

    /// Whether direct email+password login is offered (drives the login UI). The
    /// OSS local provider returns `true`; a pure-SSO provider returns `false`.
    fn supports_password_login(&self) -> bool {
        true
    }
}
