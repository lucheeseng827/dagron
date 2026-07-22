# dagron-identity — the dagron-api identity seam

`dagron-identity` is the **authentication seam** for `dagron-api`: the
`IdentityProvider` trait plus the `VerifiedUser` it resolves. It abstracts *who
is this user* so dagron can ship a self-contained local login (argon2 against the
`users` table) while an alternate provider swaps in an external IdP behind the
same trait, without forking dagron-api. dagron-api always owns the **session**
(its HS256 cookie); a provider only resolves an identity from a credential.

The dependency is one-way: alternate identity impls depend on this trait, never
the reverse.

## What it does

- `IdentityProvider` — pluggable, `async` authentication backend (`Send + Sync`):
  - `authenticate_password(email, password)` — verifies an email + password
    credential. `Ok(Some)` on success, `Ok(None)` on bad credentials (callers
    return a generic 401), `Err` only for an actual backend failure. Redirect-based
    SSO providers return `Ok(None)` here and drive the browser flow via their own
    redirect/callback surface.
  - `supports_password_login()` — whether direct email+password login is offered
    (drives the login UI). Defaults to `true`; a pure-SSO provider returns `false`.
- `VerifiedUser` — an authenticated principal (`id`, `email`, `name`, `groups`)
  that dagron-api mints its session JWT from.

This crate defines only the trait and type; the local argon2 provider and session
handling live in `dagron-api`.

## Quickstart

Implement the trait to plug in an identity backend:

```rust
use async_trait::async_trait;
use dagron_identity::{IdentityProvider, VerifiedUser};

struct MyProvider;

#[async_trait]
impl IdentityProvider for MyProvider {
    async fn authenticate_password(&self, email: &str, password: &str)
        -> anyhow::Result<Option<VerifiedUser>>
    {
        // resolve the credential; Ok(None) on bad creds
        todo!()
    }
}
```

dagron-api takes any `IdentityProvider`, calls `authenticate_password` on login,
and mints its session cookie from the returned `VerifiedUser`.
