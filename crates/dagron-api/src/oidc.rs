//! **The OIDC issuer**: tokens that name a run, for a
//! cloud's federation to trade for credentials, so that a task holds no
//! secret of the cloud's.
//!
//! - `DAGRON_OIDC_ISSUER` is the issuer's URL, which the cloud fetches the
//!   discovery document and the keys from: `{issuer}/.well-known/openid-configuration`
//!   and `{issuer}/.well-known/jwks`, served here with no session.
//! - `DAGRON_OIDC_KEYS` is a PEM file of an RSA private key, or a directory of
//!   them (`*.pem`). Every key is published; the one whose file name sorts last
//!   signs. A key is rotated by adding a newer file, and the old one removed
//!   once no token it signed is alive (`DAGRON_OIDC_TOKEN_SECS`, 600 s).
//! - Enterprise: the newest key may be held by a KMS. Its public half is the
//!   file that sorts last, and `DAGRON_OIDC_SIGN_CMD` signs with it: given the
//!   SHA-256 of a token's signing input, base64, on stdin, it writes the
//!   RSASSA-PKCS1-v1_5 signature, base64, on stdout
//!   ([`dagron_crypto::sign_digest_by_command`]; `aws kms sign
//!   --message-type DIGEST --signing-algorithm RSASSA_PKCS1_V1_5_SHA_256`,
//!   say). Each signature is checked against that public half before a token
//!   is given. The private keys of earlier files are still read, and
//!   published; a public key alone is published, and signs nothing.
//! - `GET /oidc/token?audience=…`, with `Authorization: Bearer` and the request
//!   credential the engine gave the task (`DAGRON_OIDC_REQUEST_TOKEN`), answers
//!   `{"value": "<jwt>"}`, as GitHub's request endpoint does. Only while the
//!   task runs, for an audience the task was given, and for a trusted run, or
//!   an agent's where its environment's policy says so (`allowed_identity`).
//!
//! The claims are the run record's: `sub` is `repo:<host>/<owner>/<name>` and
//! then `:environment:<name>`, `:pull_request` or `:ref:<ref>`; beside it
//! `repository`, `forge_host`, `ref`, `sha`, `pr`, `trust`, `pipeline`,
//! `run_id`, `task`, `attempt`, `environment` and `actor`, each where the run
//! has one.

use std::path::Path;

use anyhow::{bail, Context, Result};
use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use base64::Engine as _;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header};
use rsa::pkcs1::{DecodeRsaPrivateKey, DecodeRsaPublicKey};
use rsa::pkcs8::{DecodePrivateKey, DecodePublicKey};
use rsa::traits::PublicKeyParts;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use crate::state::AppState;

/// How long a token lasts unless `DAGRON_OIDC_TOKEN_SECS` says otherwise.
const TOKEN_SECS: u64 = 600;

/// One key, and what is published of it. A private key signs here; a
/// public key alone signs only through the KMS command, where it is the
/// newest (`DAGRON_OIDC_SIGN_CMD`).
pub struct Key {
    pub kid: String,
    encoding: Option<EncodingKey>,
    pub jwk: Value,
}

/// A key is said by its id alone: its private half is never printed.
impl std::fmt::Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Key")
            .field("kid", &self.kid)
            .finish_non_exhaustive()
    }
}

/// The issuer: its URL, its keys (the last signs) and how long a token lasts.
#[derive(Debug)]
pub struct Issuer {
    pub issuer: String,
    /// The issuer's path, where its two documents are served: "" or "/oidc".
    pub path: String,
    keys: Vec<Key>,
    pub token_secs: u64,
    /// The command that signs with the newest key, held by a KMS.
    sign_cmd: Option<Vec<String>>,
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

impl Issuer {
    /// The issuer `DAGRON_OIDC_ISSUER` names, or `None` when it is unset.
    pub fn from_env() -> Result<Option<Self>> {
        let read = |name: &str| {
            std::env::var(name)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let Some(issuer) = read("DAGRON_OIDC_ISSUER") else {
            return Ok(None);
        };
        let keys_at = read("DAGRON_OIDC_KEYS")
            .context("DAGRON_OIDC_ISSUER is set, and DAGRON_OIDC_KEYS names no key")?;
        let token_secs = match read("DAGRON_OIDC_TOKEN_SECS") {
            None => TOKEN_SECS,
            Some(raw) => match raw.parse::<u64>() {
                Ok(secs) if (60..=3600).contains(&secs) => secs,
                _ => bail!(
                    "DAGRON_OIDC_TOKEN_SECS={raw:?} is not a number of seconds from 60 to 3600"
                ),
            },
        };
        let sign_cmd = read("DAGRON_OIDC_SIGN_CMD").map(|cmd| dagron_crypto::command_line(&cmd));
        if sign_cmd.is_some() && !cfg!(feature = "enterprise") {
            bail!(
                "DAGRON_OIDC_SIGN_CMD signs with a KMS key, which is the enterprise build's: \
                 sign with a key of your own (DAGRON_OIDC_KEYS)"
            );
        }
        Ok(Some(Self::signed_by(
            &issuer,
            load_keys(Path::new(&keys_at))?,
            token_secs,
            sign_cmd,
        )?))
    }

    /// [`Issuer::signed_by`] keys of its own alone.
    #[cfg(test)]
    pub fn new(issuer: &str, keys: Vec<Key>, token_secs: u64) -> Result<Self> {
        Self::signed_by(issuer, keys, token_secs, None)
    }

    /// [`Issuer::new`], whose newest key a KMS holds and `sign_cmd` signs
    /// with, where it is given.
    pub fn signed_by(
        issuer: &str,
        keys: Vec<Key>,
        token_secs: u64,
        sign_cmd: Option<Vec<String>>,
    ) -> Result<Self> {
        let issuer = issuer.trim_end_matches('/').to_string();
        let Some(rest) = issuer.strip_prefix("https://") else {
            bail!("DAGRON_OIDC_ISSUER={issuer:?} is not an https:// URL: a cloud fetches the issuer's keys over TLS");
        };
        if rest.contains(['?', '#', '@'])
            || rest.chars().any(|c| c.is_whitespace() || c.is_control())
        {
            bail!("DAGRON_OIDC_ISSUER={issuer:?} is a URL with no user, query or fragment");
        }
        let path = rest
            .find('/')
            .map(|at| rest[at..].to_string())
            .unwrap_or_default();
        let Some(newest) = keys.last() else {
            bail!("DAGRON_OIDC_KEYS holds no key");
        };
        match (newest.encoding.is_some(), sign_cmd.is_some()) {
            (true, true) => bail!(
                "DAGRON_OIDC_SIGN_CMD signs with a KMS key, and the newest key in \
                 DAGRON_OIDC_KEYS is a private key: the newest is the KMS key's public half"
            ),
            (false, false) => bail!(
                "the newest key in DAGRON_OIDC_KEYS is a public key alone: the newest signs, \
                 so it is a private key, or a KMS key's public half with DAGRON_OIDC_SIGN_CMD"
            ),
            _ => {}
        }
        Ok(Self {
            issuer,
            path,
            keys,
            token_secs,
            sign_cmd,
        })
    }

    /// `/.well-known/openid-configuration`.
    pub fn discovery(&self) -> Value {
        json!({
            "issuer": self.issuer,
            "jwks_uri": format!("{}/.well-known/jwks", self.issuer),
            "subject_types_supported": ["public"],
            "response_types_supported": ["id_token"],
            "id_token_signing_alg_values_supported": ["RS256"],
            "scopes_supported": ["openid"],
            "claims_supported": [
                "sub", "aud", "exp", "iat", "nbf", "iss", "jti", "repository", "forge_host", "ref",
                "sha", "pr", "trust", "pipeline", "run_id", "task", "attempt", "environment", "actor"
            ],
        })
    }

    /// `/.well-known/jwks`: every key.
    pub fn jwks(&self) -> Value {
        json!({ "keys": self.keys.iter().map(|k| k.jwk.clone()).collect::<Vec<_>>() })
    }

    /// The claims, signed RS256 with the newest key: here, or by the KMS
    /// that holds it, which may take its time (a blocking call).
    pub fn sign(&self, claims: &Value) -> Result<String> {
        let key = self.keys.last().context("no key to sign with")?;
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(key.kid.clone());
        if let Some(encoding) = &key.encoding {
            return Ok(jsonwebtoken::encode(&header, claims, encoding)?);
        }
        let cmd = self
            .sign_cmd
            .as_ref()
            .context("the newest key is a public key alone, and nothing signs with it")?;
        let input = format!(
            "{}.{}",
            b64(&serde_json::to_vec(&header)?),
            b64(&serde_json::to_vec(claims)?)
        );
        let digest: [u8; 32] = Sha256::digest(input.as_bytes()).into();
        let signature = b64(&dagron_crypto::sign_digest_by_command(cmd, &digest)
            .context("the OIDC signing command (DAGRON_OIDC_SIGN_CMD)")?);
        let public = DecodingKey::from_rsa_components(
            key.jwk["n"].as_str().unwrap_or_default(),
            key.jwk["e"].as_str().unwrap_or_default(),
        )?;
        if !jsonwebtoken::crypto::verify(&signature, input.as_bytes(), &public, Algorithm::RS256)? {
            bail!(
                "the signing command's signature does not verify with the newest key in \
                 DAGRON_OIDC_KEYS: is it the same key?"
            );
        }
        Ok(format!("{input}.{signature}"))
    }
}

/// The keys at `at`: one PEM file, or every `*.pem` of a directory in the
/// order of their names. PKCS#8 or PKCS#1, RSA of 2048 bits or more: a
/// private key, or a public key alone (a KMS key's).
pub fn load_keys(at: &Path) -> Result<Vec<Key>> {
    let files = if at.is_dir() {
        let mut files: Vec<_> = std::fs::read_dir(at)
            .with_context(|| format!("DAGRON_OIDC_KEYS={}", at.display()))?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "pem"))
            .collect();
        files.sort();
        files
    } else {
        vec![at.to_path_buf()]
    };
    files
        .iter()
        .map(|file| {
            let pem = std::fs::read_to_string(file)
                .with_context(|| format!("reading {}", file.display()))?;
            key_of(&pem).with_context(|| format!("the key in {}", file.display()))
        })
        .collect()
}

/// A key from its PEM: a private key, or a public key alone.
pub fn key_of(pem: &str) -> Result<Key> {
    let private = rsa::RsaPrivateKey::from_pkcs8_pem(pem)
        .or_else(|_| rsa::RsaPrivateKey::from_pkcs1_pem(pem))
        .ok();
    let public = match &private {
        Some(private) => private.to_public_key(),
        None => rsa::RsaPublicKey::from_public_key_pem(pem)
            .or_else(|_| rsa::RsaPublicKey::from_pkcs1_pem(pem))
            .context("not an RSA key in PEM (PKCS#8 or PKCS#1, private or public)")?,
    };
    if public.size() < 256 {
        bail!("an RSA key of {} bits: 2048 or more", public.size() * 8);
    }
    let n = b64(&public.n().to_bytes_be());
    let e = b64(&public.e().to_bytes_be());
    // RFC 7638: the SHA-256 of the required members, in this order.
    let kid = b64(&Sha256::digest(
        format!(r#"{{"e":"{e}","kty":"RSA","n":"{n}"}}"#).as_bytes(),
    ));
    Ok(Key {
        jwk: json!({ "kty": "RSA", "use": "sig", "alg": "RS256", "kid": kid, "n": n, "e": e }),
        kid,
        encoding: match private {
            Some(_) => Some(EncodingKey::from_rsa_pem(pem.as_bytes())?),
            None => None,
        },
    })
}

/// What a token says of a task's run.
pub fn claims(
    issuer: &Issuer,
    request: &dagron_core::models::OidcRequest,
    trigger: Option<&dagron_core::models::RunTrigger>,
    pipeline: &str,
    audience: &str,
    now: i64,
) -> Value {
    let class = dagron_core::envpolicy::class_of(trigger).to_string();
    let some = |v: Option<&String>| v.filter(|v| !v.is_empty()).cloned();
    let repo = trigger.and_then(|t| some(t.repo.as_ref()));
    let host = trigger.and_then(|t| some(t.forge_host.as_ref()));
    let git_ref = trigger.and_then(|t| some(t.git_ref.as_ref()));
    let pr = trigger.and_then(|t| t.pr);
    let sub = match &repo {
        Some(repo) => {
            let at = format!("repo:{}/{repo}", host.as_deref().unwrap_or("-"));
            match (&request.environment, pr, &git_ref) {
                (Some(env), _, _) => format!("{at}:environment:{env}"),
                (None, Some(_), _) => format!("{at}:pull_request"),
                (None, None, Some(r)) => format!("{at}:ref:{r}"),
                (None, None, None) => at,
            }
        }
        None => format!("pipeline:{pipeline}"),
    };
    let mut c = Map::new();
    c.insert("iss".into(), json!(issuer.issuer));
    c.insert("sub".into(), json!(sub));
    c.insert("aud".into(), json!(audience));
    c.insert("iat".into(), json!(now));
    c.insert("nbf".into(), json!(now - 5));
    c.insert(
        "exp".into(),
        json!(now + i64::try_from(issuer.token_secs).unwrap_or(600)),
    );
    c.insert("jti".into(), json!(uuid::Uuid::new_v4().to_string()));
    for (name, value) in [
        ("repository", repo.map(Value::from)),
        ("forge_host", host.map(Value::from)),
        ("ref", git_ref.map(Value::from)),
        (
            "sha",
            trigger.and_then(|t| some(t.sha.as_ref())).map(Value::from),
        ),
        ("pr", pr.map(Value::from)),
        ("trust", Some(Value::from(class))),
        (
            "pipeline",
            Some(pipeline).filter(|p| !p.is_empty()).map(Value::from),
        ),
        ("run_id", Some(Value::from(request.run_id.clone()))),
        ("task", Some(Value::from(request.task_name.clone()))),
        ("attempt", Some(Value::from(request.attempt))),
        ("environment", request.environment.clone().map(Value::from)),
        (
            "actor",
            trigger
                .and_then(|t| some(t.actor.as_ref()))
                .map(Value::from),
        ),
    ] {
        if let Some(value) = value {
            c.insert(name.into(), value);
        }
    }
    Value::Object(c)
}

fn none() -> (StatusCode, String) {
    (
        StatusCode::NOT_FOUND,
        "this installation has no OIDC issuer (DAGRON_OIDC_ISSUER)".into(),
    )
}

/// `GET {issuer path}/.well-known/openid-configuration`
pub async fn discovery(State(state): State<AppState>) -> Result<Json<Value>, (StatusCode, String)> {
    state
        .oidc
        .as_ref()
        .map(|i| Json(i.discovery()))
        .ok_or_else(none)
}

/// `GET {issuer path}/.well-known/jwks`
pub async fn jwks(State(state): State<AppState>) -> Result<Json<Value>, (StatusCode, String)> {
    state.oidc.as_ref().map(|i| Json(i.jwks())).ok_or_else(none)
}

#[derive(serde::Deserialize)]
pub struct TokenQuery {
    audience: Option<String>,
}

/// `GET /oidc/token?audience=…`, as a task asks for a token.
pub async fn token(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<TokenQuery>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let issuer = state.oidc.as_ref().ok_or_else(none)?;
    let refused = |code: StatusCode, why: &str| (code, why.to_string());
    let credential = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer ")))
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            refused(
                StatusCode::UNAUTHORIZED,
                "ask with the task's request credential: Authorization: Bearer $DAGRON_OIDC_REQUEST_TOKEN",
            )
        })?;
    let sha: String = Sha256::digest(credential.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let internal = |e: anyhow::Error| {
        tracing::error!(error = %format!("{e:#}"), "a token request could not be answered");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "the request could not be answered".to_string(),
        )
    };
    let Some((request, status)) = dagron_core::db::oidc_request(&state.read_pool, &sha)
        .await
        .map_err(internal)?
    else {
        return Err(refused(
            StatusCode::UNAUTHORIZED,
            "not the request credential of a task",
        ));
    };
    let now = chrono::Utc::now();
    if !chrono::DateTime::parse_from_rfc3339(&request.expires_at).is_ok_and(|at| at >= now) {
        return Err(refused(
            StatusCode::UNAUTHORIZED,
            "the request credential has expired",
        ));
    }
    if status.as_deref() != Some("running") {
        return Err(refused(StatusCode::FORBIDDEN, "its task is not running"));
    }
    let audience = q
        .audience
        .filter(|a| !a.is_empty())
        .unwrap_or_else(|| request.audiences.first().cloned().unwrap_or_default());
    if !request.audiences.contains(&audience) {
        return Err((
            StatusCode::FORBIDDEN,
            format!("{audience:?} is not an audience this task was given"),
        ));
    }
    let trigger = dagron_core::db::run_trigger(&state.read_pool, &request.run_id)
        .await
        .map_err(internal)?;
    // A trusted run's task, or an agent's where its environment's policy
    // gives an agent an identity: asked again here, of
    // the policy as it stands now, and not only when the credential was. Of
    // the primary, as the run's own gate reads it: a replica behind it would
    // still give an identity a policy has since taken away.
    let class = dagron_core::envpolicy::class_of(trigger.as_ref());
    if let Err(e) = dagron_core::secrets::admit_identity(
        &state.write_pool,
        request.environment.as_deref(),
        class,
    )
    .await
    {
        return Err(refused(
            StatusCode::FORBIDDEN,
            &format!("a token is given to trusted runs, and to an agent's only where its environment's policy says so: {e:#}"),
        ));
    }
    let pipeline = dagron_core::db::workflow_name_for_run(&state.read_pool, &request.run_id)
        .await
        .map_err(internal)?
        .unwrap_or_default();
    let claims = claims(
        issuer,
        &request,
        trigger.as_ref(),
        &pipeline,
        &audience,
        now.timestamp(),
    );
    // A KMS that holds the key may take its time: not on the server's threads.
    let signer = std::sync::Arc::clone(issuer);
    let jwt = tokio::task::spawn_blocking(move || signer.sign(&claims))
        .await
        .map_err(|e| internal(e.into()))?
        .map_err(internal)?;
    tracing::info!(run_id = %request.run_id, task = %request.task_name, %audience, "an OIDC token was given");
    Ok(Json(json!({ "value": jwt })))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 2048-bit key made for these tests alone.
    fn pem() -> String {
        use rsa::pkcs8::EncodePrivateKey;
        let mut rng = rsa::rand_core::OsRng;
        let key = rsa::RsaPrivateKey::new(&mut rng, 2048).unwrap();
        key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap()
            .to_string()
    }

    fn request(environment: Option<&str>) -> dagron_core::models::OidcRequest {
        dagron_core::models::OidcRequest {
            token_sha256: "x".into(),
            run_id: "run-1".into(),
            task_id: "task-1".into(),
            task_name: "deploy".into(),
            attempt: 1,
            audiences: vec!["sts.amazonaws.com".into()],
            environment: environment.map(Into::into),
            expires_at: String::new(),
        }
    }

    fn trigger(pr: Option<i64>) -> dagron_core::models::RunTrigger {
        dagron_core::models::RunTrigger {
            kind: "ci".into(),
            principal: "ci".into(),
            trust: Some("trusted".into()),
            forge_host: Some("github.com".into()),
            repo: Some("acme/widgets".into()),
            git_ref: Some("refs/heads/main".into()),
            sha: Some("a".repeat(40)),
            pr,
            actor: Some("octo".into()),
            ..Default::default()
        }
    }

    #[test]
    fn a_token_is_signed_by_the_newest_key_and_verifies_against_the_published_one() {
        let (old, new) = (key_of(&pem()).unwrap(), key_of(&pem()).unwrap());
        let new_kid = new.kid.clone();
        let issuer = Issuer::new("https://ci.example.com/oidc/", vec![old, new], 600).unwrap();
        assert_eq!(issuer.issuer, "https://ci.example.com/oidc");
        assert_eq!(issuer.path, "/oidc");
        assert_eq!(
            issuer.discovery()["jwks_uri"],
            "https://ci.example.com/oidc/.well-known/jwks"
        );
        let jwks = issuer.jwks();
        assert_eq!(
            jwks["keys"].as_array().unwrap().len(),
            2,
            "both keys are published"
        );
        let claims = claims(
            &issuer,
            &request(None),
            Some(&trigger(None)),
            "ci",
            "sts.amazonaws.com",
            1_800_000_000,
        );
        let jwt = issuer.sign(&claims).unwrap();
        let header = jsonwebtoken::decode_header(&jwt).unwrap();
        assert_eq!(
            header.kid.as_deref(),
            Some(new_kid.as_str()),
            "the newest key signs"
        );
        let jwk = jwks["keys"]
            .as_array()
            .unwrap()
            .iter()
            .find(|k| k["kid"] == new_kid.as_str())
            .unwrap();
        let key = jsonwebtoken::DecodingKey::from_rsa_components(
            jwk["n"].as_str().unwrap(),
            jwk["e"].as_str().unwrap(),
        )
        .unwrap();
        let mut v = jsonwebtoken::Validation::new(Algorithm::RS256);
        v.set_audience(&["sts.amazonaws.com"]);
        v.set_issuer(&["https://ci.example.com/oidc"]);
        v.validate_exp = false;
        v.validate_nbf = false;
        let got = jsonwebtoken::decode::<Value>(&jwt, &key, &v)
            .unwrap()
            .claims;
        assert_eq!(
            got["sub"],
            "repo:github.com/acme/widgets:ref:refs/heads/main"
        );
        assert_eq!(got["repository"], "acme/widgets");
        assert_eq!(got["trust"], "trusted");
        assert_eq!(got["pipeline"], "ci");
        assert_eq!(
            got["exp"].as_i64().unwrap() - got["iat"].as_i64().unwrap(),
            600
        );
        assert!(got.get("environment").is_none() && got.get("pr").is_none());
    }

    #[test]
    fn the_subject_says_the_environment_else_a_pull_request_else_the_ref() {
        let issuer =
            Issuer::new("https://ci.example.com", vec![key_of(&pem()).unwrap()], 300).unwrap();
        assert_eq!(issuer.path, "");
        let sub = |env: Option<&str>, pr: Option<i64>| {
            claims(&issuer, &request(env), Some(&trigger(pr)), "ci", "a", 0)["sub"]
                .as_str()
                .unwrap()
                .to_string()
        };
        assert_eq!(
            sub(Some("production"), Some(7)),
            "repo:github.com/acme/widgets:environment:production"
        );
        assert_eq!(
            sub(None, Some(7)),
            "repo:github.com/acme/widgets:pull_request"
        );
        assert_eq!(
            sub(None, None),
            "repo:github.com/acme/widgets:ref:refs/heads/main"
        );
        assert_eq!(
            claims(&issuer, &request(None), None, "nightly", "a", 0)["sub"],
            "pipeline:nightly"
        );
    }

    /// A key's public half alone, as a KMS gives it.
    fn public_of(pem: &str) -> String {
        use rsa::pkcs8::EncodePublicKey;
        rsa::RsaPrivateKey::from_pkcs8_pem(pem)
            .unwrap()
            .to_public_key()
            .to_public_key_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap()
    }

    /// A public key alone is published, and signs only through a KMS command
    /// where it is the newest; the command is for a public key, not a private
    /// one.
    #[test]
    fn a_public_key_alone_is_published_and_the_newest_signs_through_a_kms_command() {
        let (old, kms) = (pem(), pem());
        let keys = || vec![key_of(&old).unwrap(), key_of(&public_of(&kms)).unwrap()];
        assert_eq!(
            key_of(&public_of(&kms)).unwrap().kid,
            key_of(&kms).unwrap().kid,
            "a key's id is its public half's"
        );
        let e = Issuer::new("https://ci.example.com", keys(), 600)
            .unwrap_err()
            .to_string();
        assert!(e.contains("DAGRON_OIDC_SIGN_CMD"), "{e}");
        let e = Issuer::signed_by(
            "https://ci.example.com",
            vec![key_of(&kms).unwrap()],
            600,
            Some(vec!["kms-sign".into()]),
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("public half"), "{e}");
        let issuer = Issuer::signed_by(
            "https://ci.example.com",
            keys(),
            600,
            Some(vec!["false".into()]),
        )
        .unwrap();
        assert_eq!(issuer.jwks()["keys"].as_array().unwrap().len(), 2);
        // A command that fails gives no token.
        assert!(issuer.sign(&json!({"sub": "x"})).is_err());
    }

    /// The KMS, stood in for by a script that signs a digest through openssl
    /// as a KMS does: the token verifies with the published key, and a
    /// command that signs with another key gives none.
    #[cfg(all(unix, feature = "enterprise"))]
    #[test]
    fn a_token_signed_by_a_kms_command_verifies_with_the_published_key() {
        if std::process::Command::new("openssl")
            .arg("version")
            .output()
            .is_err()
        {
            return;
        }
        let dir = std::env::temp_dir().join(format!("dagron-oidc-kms-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let script = |name: &str, private: &str| {
            use std::os::unix::fs::PermissionsExt;
            let key = dir.join(format!("{name}.pem"));
            std::fs::write(&key, private).unwrap();
            let path = dir.join(name);
            std::fs::write(
                &path,
                format!(
                    "#!/bin/sh\nbase64 -d | openssl pkeyutl -sign -inkey {} -pkeyopt digest:sha256 | base64 | tr -d '\\n'\n",
                    key.display()
                ),
            )
            .unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            vec![path.to_str().unwrap().to_string()]
        };
        let kms = pem();
        let published = key_of(&public_of(&kms)).unwrap();
        let jwk = published.jwk.clone();
        let issuer = Issuer::signed_by(
            "https://ci.example.com",
            vec![published],
            600,
            Some(script("sign", &kms)),
        )
        .unwrap();
        let jwt = issuer
            .sign(&json!({"sub": "x", "aud": "sts.amazonaws.com", "iss": "https://ci.example.com"}))
            .unwrap();
        assert_eq!(
            jsonwebtoken::decode_header(&jwt).unwrap().kid.as_deref(),
            jwk["kid"].as_str()
        );
        let key = DecodingKey::from_rsa_components(
            jwk["n"].as_str().unwrap(),
            jwk["e"].as_str().unwrap(),
        )
        .unwrap();
        let mut v = jsonwebtoken::Validation::new(Algorithm::RS256);
        v.set_audience(&["sts.amazonaws.com"]);
        v.required_spec_claims.clear();
        v.validate_exp = false;
        assert_eq!(
            jsonwebtoken::decode::<Value>(&jwt, &key, &v)
                .unwrap()
                .claims["sub"],
            "x"
        );
        let wrong = Issuer::signed_by(
            "https://ci.example.com",
            vec![key_of(&public_of(&kms)).unwrap()],
            600,
            Some(script("wrong", &pem())),
        )
        .unwrap();
        let e = wrong.sign(&json!({"sub": "x"})).unwrap_err().to_string();
        assert!(e.contains("same key"), "{e}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_issuer_is_https_and_a_key_is_rsa_of_2048_bits_or_more() {
        let key = || key_of(&pem()).unwrap();
        assert!(Issuer::new("http://ci.example.com", vec![key()], 600)
            .unwrap_err()
            .to_string()
            .contains("https://"));
        assert!(Issuer::new("https://u@ci.example.com", vec![key()], 600).is_err());
        assert!(Issuer::new("https://ci.example.com", vec![], 600).is_err());
        assert!(key_of("not a key").is_err());
        let small = {
            use rsa::pkcs8::EncodePrivateKey;
            let mut rng = rsa::rand_core::OsRng;
            rsa::RsaPrivateKey::new(&mut rng, 1024)
                .unwrap()
                .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
                .unwrap()
                .to_string()
        };
        assert!(key_of(&small)
            .unwrap_err()
            .to_string()
            .contains("2048 or more"));
        // A directory: every *.pem, in the order of their names.
        let dir = std::env::temp_dir().join(format!("dagron-oidc-keys-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("2026-01.pem"), pem()).unwrap();
        std::fs::write(dir.join("2026-02.pem"), pem()).unwrap();
        std::fs::write(dir.join("README"), "not a key").unwrap();
        assert_eq!(load_keys(&dir).unwrap().len(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
