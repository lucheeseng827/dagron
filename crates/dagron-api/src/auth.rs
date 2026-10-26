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
//! existing tokens at once. A service's token (a user in `ci:trigger`, such
//! as dagron-ci's own) is the one exception: who it is is kept for
//! [`SERVICE_TOKEN_CACHE`], so a change to its user made other than by a
//! revoke through this process holds within that, not at once.
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

/// Whether a session carries the `admin` group — the one privilege level this API
/// distinguishes. The console calls the three roles admin / operator / viewer, but only
/// `admin` is a group: an operator is a user with no groups at all, so "not a viewer" is
/// not a thing a handler can ask. Route modules wrap this in their own `require_admin` to
/// build whatever error body they return (plain text, JSON); the predicate is shared so the
/// group name is written once.
pub fn is_admin(claims: &SessionClaims) -> bool {
    claims.groups.iter().any(|g| g == "admin")
}

/// Whether a session is read-only — the `viewer` group. A viewer may read anything they are
/// authenticated for and change nothing; the refusal is [`read_only_refusal`], applied by the
/// middleware in `routes::audit` before a request reaches any handler.
pub fn is_viewer(claims: &SessionClaims) -> bool {
    claims.groups.iter().any(|g| g == "viewer")
}

/// The group whose members may say what started a run they submit.
pub const TRIGGER_GROUP: &str = "ci:trigger";

/// Whether a caller may set a run's trigger record (`run_triggers`): the
/// [`TRIGGER_GROUP`] group. It is what lets a list be narrowed to "the runs CI
/// submitted" and believed, so it is a service's own identity that holds it,
/// not a person's: anyone else's submit that names a trigger is refused, and a
/// run nobody could vouch for reads as it always did.
pub fn may_set_trigger(claims: &SessionClaims) -> bool {
    claims.groups.iter().any(|g| g == TRIGGER_GROUP)
}

/// Whether a request is a *control* mutation: the ones a read-only session may not make, and the
/// ones the enterprise build records in its audit trail. Anything but `GET`/`HEAD`/`OPTIONS`,
/// except logging in and out — those are auth plumbing rather than control actions, and a viewer
/// who cannot sign in cannot read either.
///
/// Shared by both builds so the two cannot answer "is this a mutation?" differently: the open
/// build enforces the role, the enterprise build enforces it *and* audits, off one definition.
pub fn is_control_mutation(method: &axum::http::Method, path: &str) -> bool {
    use axum::http::Method;
    !matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS)
        && !matches!(path, "/api/login" | "/api/logout")
}

/// The refusal a read-only session gets for a control mutation. JSON, per the error envelope
/// `docs/API.md` documents — a middleware refusal is no less a client-facing error than a
/// handler's.
pub fn read_only_refusal() -> axum::response::Response {
    use axum::response::IntoResponse;
    (
        axum::http::StatusCode::FORBIDDEN,
        axum::Json(serde_json::json!({ "error": "viewer role is read-only" })),
    )
        .into_response()
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
    let computed = crate::routes::tokens::hash_token(presented);
    if let Some(claims) = state.token_cache.get(&computed) {
        return Some(claims);
    }
    // What is read from here on is kept only if no token was revoked
    // meanwhile: a revoke as this reads would otherwise be undone by it.
    let generation = state.token_cache.generation();

    // The cleartext prefix is a unique index, so this is one row read rather
    // than hashing the presented token against every row in the table.
    #[allow(clippy::type_complexity)]
    let row: (
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT user_id, token_hash, expires_at, revoked_at, last_used_at, agent
         FROM api_tokens WHERE prefix = $1",
    )
    .bind(prefix)
    .fetch_optional(&state.read_pool)
    .await
    .ok()??;
    let (user_id, stored_hash, expires_at, revoked_at, last_used_at, agent) = row;

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

    // An agent's token presents as the agent, never as its owner: its
    // own subject, no email to be matched as a person's, and its owner's
    // groups less what no agent holds.
    if let Some(agent) = agent {
        return Some(SessionClaims {
            sub: format!("{AGENT_PREFIX}{agent}"),
            email: String::new(),
            name: format!("{agent} (an agent of {email})"),
            groups: agent_groups(groups),
            exp: usize::MAX,
        });
    }
    let claims = SessionClaims {
        sub: user_id,
        email,
        name,
        groups,
        // A token's lifetime is `expires_at`, checked above. This field only
        // satisfies the shared claims shape; nothing re-validates it.
        exp: usize::MAX,
    };
    // A service's token, which asks on every call it makes, is answered from
    // the cache for a while; and only while its own lifetime lasts.
    if claims.groups.iter().any(|g| g == TRIGGER_GROUP) {
        let lasts = expires_at
            .as_deref()
            .and_then(|e| chrono::DateTime::parse_from_rfc3339(e).ok())
            .map(|t| t.with_timezone(&chrono::Utc));
        state.token_cache.put(&computed, &claims, lasts, generation);
    }
    Some(claims)
}

/// How long a service's token is taken for who it was without asking the
/// database again. A token revoked through this dagron-api is
/// forgotten at once; one revoked through another replica, or a user's
/// groups changed in the database by hand, is refused or seen within this.
pub const SERVICE_TOKEN_CACHE: std::time::Duration = std::time::Duration::from_secs(10);

/// The most tokens the cache holds: past it, it is emptied and fills again.
const SERVICE_TOKEN_CACHE_MAX: usize = 1024;

/// Who each service token was, by its hash, until when. A service
/// is a user in [`TRIGGER_GROUP`]: dagron-ci, whose reporter calls on every
/// run it follows, and was two reads a call. Only such a token is kept;
/// a person's token, and an agent's, are read every call, as before, so
/// that what is done to them holds at once.
#[derive(Default)]
pub struct TokenCache {
    seen: std::sync::Mutex<Seen>,
}

/// What the cache holds, and how many times a revoke emptied it.
#[derive(Default)]
struct Seen {
    tokens: std::collections::HashMap<String, (SessionClaims, std::time::Instant)>,
    generation: u64,
}

impl TokenCache {
    /// Who the token of `hash` was, if that was seen within
    /// [`SERVICE_TOKEN_CACHE`] and its own time has not passed.
    pub fn get(&self, hash: &str) -> Option<SessionClaims> {
        let mut seen = self.seen.lock().ok()?;
        match seen.tokens.get(hash) {
            Some((claims, until)) if std::time::Instant::now() < *until => Some(claims.clone()),
            Some(_) => {
                seen.tokens.remove(hash);
                None
            }
            None => None,
        }
    }

    /// How many times a revoke emptied the cache: taken before a token is
    /// read, and given to [`Self::put`].
    pub fn generation(&self) -> u64 {
        self.seen
            .lock()
            .map(|seen| seen.generation)
            .unwrap_or(u64::MAX)
    }

    /// Keep who the token of `hash` is, for [`SERVICE_TOKEN_CACHE`], or
    /// until `expires` where that is sooner; and not where a token was
    /// revoked since `generation`, so that what was read before a revoke is
    /// not kept after it.
    pub fn put(
        &self,
        hash: &str,
        claims: &SessionClaims,
        expires: Option<chrono::DateTime<chrono::Utc>>,
        generation: u64,
    ) {
        let mut lasts = SERVICE_TOKEN_CACHE;
        if let Some(expires) = expires {
            match (expires - chrono::Utc::now()).to_std() {
                Ok(left) => lasts = lasts.min(left),
                Err(_) => return,
            }
        }
        if let Ok(mut seen) = self.seen.lock() {
            if seen.generation != generation {
                return;
            }
            if seen.tokens.len() >= SERVICE_TOKEN_CACHE_MAX {
                seen.tokens.clear();
            }
            seen.tokens.insert(
                hash.to_string(),
                (claims.clone(), std::time::Instant::now() + lasts),
            );
        }
    }

    /// Forget every token: one was revoked.
    pub fn clear(&self) {
        if let Ok(mut seen) = self.seen.lock() {
            seen.tokens.clear();
            seen.generation += 1;
        }
    }
}

/// The subject an agent's token presents: `agent:<name>`.
pub const AGENT_PREFIX: &str = "agent:";

/// Whether a caller is an agent.
pub fn is_agent(claims: &SessionClaims) -> bool {
    claims.sub.starts_with(AGENT_PREFIX)
}

/// An agent's groups: its owner's, less `admin` (an agent administers
/// nothing) and [`TRIGGER_GROUP`] (an agent never says what started a run).
pub fn agent_groups(owner: Vec<String>) -> Vec<String> {
    owner
        .into_iter()
        .filter(|g| g != "admin" && g != TRIGGER_GROUP)
        .collect()
}

/// What an agent may change through dagron-api: a run it asked for, rerun or
/// cancelled. The run's id, where `path` is one of those; `None` otherwise.
pub fn agent_may_touch<'p>(method: &axum::http::Method, path: &'p str) -> Option<&'p str> {
    if *method != axum::http::Method::POST {
        return None;
    }
    let rest = path.strip_prefix("/api/runs/")?;
    let (id, what) = rest.split_once('/')?;
    (matches!(what, "rerun" | "cancel") && !id.is_empty() && !id.contains('/')).then_some(id)
}

/// The refusal an agent gets for any other change.
fn agent_refusal(why: &str) -> axum::response::Response {
    use axum::response::IntoResponse;
    (
        axum::http::StatusCode::FORBIDDEN,
        axum::Json(serde_json::json!({ "error": why })),
    )
        .into_response()
}

/// The gate an agent's control mutation passes, or its refusal. An
/// agent's token reads, and reruns or cancels the runs it asked for, which
/// dagron-ci records with it as their actor. It makes no run here (its runs
/// are made through dagron-ci, as the `agent` trust class), and approves,
/// triages or configures nothing. `None` for any caller not an agent.
pub async fn agent_gate(
    claims: &SessionClaims,
    method: &axum::http::Method,
    path: &str,
    state: &AppState,
) -> Option<axum::response::Response> {
    if !is_agent(claims) {
        return None;
    }
    let Some(run_id) = agent_may_touch(method, path) else {
        return Some(agent_refusal(
            "an agent's token reads, and reruns or cancels the runs it asked for; \
             its runs are made through dagron-ci, as the agent trust class",
        ));
    };
    let actor = dagron_core::db::run_trigger(&state.read_pool, run_id)
        .await
        .ok()
        .flatten()
        .and_then(|t| t.actor);
    (actor.as_deref() != Some(claims.sub.as_str()))
        .then(|| agent_refusal("an agent reruns and cancels the runs it asked for, and no other"))
}

/// Who an audit line names: the caller's email, or, for an agent, its name,
/// which names the person whose agent it is.
pub fn audit_who(claims: &SessionClaims) -> &str {
    if claims.email.is_empty() {
        &claims.name
    } else {
        &claims.email
    }
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
pub(crate) fn extract_bearer(value: &str) -> Option<&str> {
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

#[cfg(test)]
mod read_only_tests {
    use super::*;
    use axum::http::Method;

    fn claims(groups: &[&str]) -> SessionClaims {
        SessionClaims {
            sub: "u1".to_string(),
            email: "u1@example.com".to_string(),
            name: "U1".to_string(),
            groups: groups.iter().map(|g| g.to_string()).collect(),
            exp: 0,
        }
    }

    /// The console's three roles as the API sees them: `admin` and `viewer` are groups, and an
    /// operator is a user with none. Only the viewer is read-only — an operator authoring
    /// workflows is the role's whole purpose, so "no groups" must *not* be caught here.
    #[test]
    fn only_the_viewer_group_is_read_only() {
        assert!(is_viewer(&claims(&["viewer"])));
        assert!(is_viewer(&claims(&["viewer", "admin"])), "a group list is not an ordered role");
        assert!(!is_viewer(&claims(&[])), "an operator has no groups and may write");
        assert!(!is_viewer(&claims(&["admin"])));
        assert!(!is_viewer(&claims(&["viewers"])), "a near-miss group name is not the role");
    }

    /// Reads are never refused, and logging in and out are not control actions — a viewer who
    /// cannot POST /api/login cannot read either, which would make the role useless rather than
    /// read-only.
    #[test]
    fn control_mutations_exclude_reads_and_auth_plumbing() {
        for (m, p) in [(Method::GET, "/api/workflows"), (Method::HEAD, "/api/workflows"), (Method::OPTIONS, "/api/workflows")] {
            assert!(!is_control_mutation(&m, p), "{m} {p} is a read");
        }
        assert!(!is_control_mutation(&Method::POST, "/api/login"));
        assert!(!is_control_mutation(&Method::POST, "/api/logout"));

        for (m, p) in [
            (Method::POST, "/api/workflows"),
            (Method::PUT, "/api/workflows/w1"),
            (Method::DELETE, "/api/workflows/w1"),
            (Method::POST, "/api/workflows/w1/state"),
            (Method::POST, "/api/git-repos"),
            // Not a prefix match: only the two exact auth paths are exempt.
            (Method::POST, "/api/login/extra"),
        ] {
            assert!(is_control_mutation(&m, p), "{m} {p} must be judged");
        }
    }

    /// The refusal carries the documented JSON envelope, not a plain-text body.
    #[tokio::test]
    async fn the_refusal_is_a_json_envelope() {
        let res = read_only_refusal();
        assert_eq!(res.status(), axum::http::StatusCode::FORBIDDEN);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).expect("a JSON envelope");
        assert_eq!(body["error"], "viewer role is read-only");
    }

    /// An agent holds its owner's groups less `admin` and the trigger group,
    /// and is known by its subject alone.
    #[test]
    fn an_agent_is_its_owner_less_what_no_agent_holds() {
        let owner = vec![
            "admin".to_string(),
            TRIGGER_GROUP.to_string(),
            "release".to_string(),
        ];
        assert_eq!(agent_groups(owner), ["release"]);
        assert_eq!(
            agent_groups(vec!["viewer".into()]),
            ["viewer"],
            "a viewer's agent reads"
        );
        let mut agent = claims(&[]);
        assert!(!is_agent(&agent));
        agent.sub = "agent:fixer".into();
        assert!(is_agent(&agent));
        // An audit line names a person by email, and an agent by its name,
        // which names its owner.
        assert_eq!(audit_who(&agent), "u1@example.com");
        agent.email.clear();
        agent.name = "fixer (an agent of u1@example.com)".into();
        assert_eq!(audit_who(&agent), "fixer (an agent of u1@example.com)");
    }

    /// What an agent may change is a run, rerun or cancelled: nothing else,
    /// and nothing by a near miss.
    #[test]
    fn an_agent_may_touch_a_run_s_rerun_and_cancel_alone() {
        assert_eq!(
            agent_may_touch(&Method::POST, "/api/runs/r-1/rerun"),
            Some("r-1")
        );
        assert_eq!(
            agent_may_touch(&Method::POST, "/api/runs/r-1/cancel"),
            Some("r-1")
        );
        for (m, p) in [
            (Method::GET, "/api/runs/r-1/rerun"),
            (Method::DELETE, "/api/runs/r-1/cancel"),
            (Method::POST, "/api/runs/r-1/approve"),
            (Method::POST, "/api/runs/r-1/rerun/x"),
            (Method::POST, "/api/runs//rerun"),
            (Method::POST, "/api/runs/r-1"),
            (Method::POST, "/api/workflows/w1/run"),
            (Method::POST, "/api/tokens"),
        ] {
            assert_eq!(agent_may_touch(&m, p), None, "{m} {p}");
        }
    }
}

#[cfg(test)]
mod token_cache_tests {
    use super::*;

    fn service(sub: &str) -> SessionClaims {
        SessionClaims {
            sub: sub.into(),
            email: format!("{sub}@acme.test"),
            name: sub.into(),
            groups: vec![TRIGGER_GROUP.into()],
            exp: usize::MAX,
        }
    }

    /// A service token is answered from the cache within its time, never
    /// past its own expiry, and not at all once one is revoked, nor from
    /// what was read before the revoke.
    #[test]
    fn a_service_token_is_kept_for_a_while_and_forgotten_on_a_revoke() {
        let cache = TokenCache::default();
        assert!(cache.get("h-1").is_none());
        let g = cache.generation();
        cache.put("h-1", &service("dagron-ci"), None, g);
        assert_eq!(
            cache.get("h-1").map(|c| c.sub),
            Some("dagron-ci".to_string())
        );
        // One whose own time has passed is not kept at all, and one that
        // lapses sooner than the cache is kept no longer than it lasts.
        let past = chrono::Utc::now() - chrono::Duration::seconds(1);
        cache.put("h-2", &service("old"), Some(past), g);
        assert!(cache.get("h-2").is_none());
        let soon = chrono::Utc::now() + chrono::Duration::milliseconds(50);
        cache.put("h-3", &service("soon"), Some(soon), g);
        assert!(cache.get("h-3").is_some());
        std::thread::sleep(std::time::Duration::from_millis(80));
        assert!(cache.get("h-3").is_none(), "gone with the token's own time");
        cache.clear();
        assert!(cache.get("h-1").is_none(), "a revoke forgets every one");
        // A lookup that read the token before the revoke does not put it
        // back after.
        cache.put("h-1", &service("dagron-ci"), None, g);
        assert!(
            cache.get("h-1").is_none(),
            "read before the revoke, put after it"
        );
        // A cache that is full starts again rather than grow.
        let g = cache.generation();
        for n in 0..=SERVICE_TOKEN_CACHE_MAX {
            cache.put(&format!("h-{n}"), &service("many"), None, g);
        }
        assert!(cache.seen.lock().unwrap().tokens.len() <= SERVICE_TOKEN_CACHE_MAX);
    }
}
