//! The engine's side of a checkout's credential: the operator's token service
//! (`DAGRON_CHECKOUT_TOKEN_URL`), asked for each attempt's credential and told
//! to revoke it when the attempt is over (`dagron_executor::checkout`).
//! dagron-ci's `POST /ci/v1/checkout-tokens` is one.
//!
//! The service is `https://`, or `http://` to this machine (a sidecar that
//! carries it on, a test): the engine's secret goes there, and every
//! credential comes back from there, so the engine starts on no other. And
//! it is the one that was named: a redirect is not followed, since a revoke
//! carries the credential in its body and would take it wherever one led. Nor, over plain
//! http, is a proxy used that the environment names (HTTP_PROXY, ALL_PROXY):
//! it would be handed the secret and carry it off the machine.
//!
//! The protocol, JSON both ways, authenticated with the bearer secret in
//! `DAGRON_CHECKOUT_TOKEN_SECRET_FILE`:
//! - `POST` `{repository, commit, run_id, task_id, attempt}`: `200`
//!   `{username, password}` (and `expires_at`, read by nobody here), or `204`
//!   for a repository fetched without a credential. Anything else fails the
//!   attempt, saying what the service said.
//! - `DELETE` `{username, password}`: `2xx` once it is revoked.
//! - `POST` the same with `"patch": true`, for a patched checkout (plan
//!   1.6): `200` `{patch_url}`, a link to the patch the attempt's checkout
//!   lays over its commit, or anything else, which fails the attempt.

use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use dagron_executor::checkout::{CheckoutTokens, Credential, TokenRequest};

pub const ENV_URL: &str = "DAGRON_CHECKOUT_TOKEN_URL";
pub const ENV_SECRET_FILE: &str = "DAGRON_CHECKOUT_TOKEN_SECRET_FILE";
/// `allow`: a patch's link may be plain http to another host.
pub const ENV_PATCH_INSECURE: &str = "DAGRON_CHECKOUT_PATCH_INSECURE";

/// How long one call to the service may take: a minted credential is on the
/// path of an attempt's start, whose lease is renewed only once it runs.
const TIMEOUT: Duration = Duration::from_secs(10);

pub struct HttpTokens {
    url: reqwest::Url,
    secret: String,
    http: reqwest::Client,
    /// ENV_PATCH_INSECURE: a patch's link may be plain http to another host.
    plain_patch: bool,
}

impl std::fmt::Debug for HttpTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpTokens")
            .field("url", &self.url.as_str())
            .field("secret", &"***")
            .finish()
    }
}

impl HttpTokens {
    /// The service the environment names, or `None` when it names none; a
    /// half-set or unreadable setting stops the engine here.
    pub fn from_env() -> Result<Option<Self>> {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
        let (url, file) = (var(ENV_URL), var(ENV_SECRET_FILE));
        let plain_patch = match var(ENV_PATCH_INSECURE).as_deref().map(str::trim) {
            None => false,
            Some("allow") => true,
            Some(other) => bail!("{ENV_PATCH_INSECURE}={other:?} is allow, or unset"),
        };
        let secret = match &file {
            Some(path) => Some(
                std::fs::read_to_string(path.trim())
                    .with_context(|| format!("{ENV_SECRET_FILE}: cannot read {path}"))?,
            ),
            None => None,
        };
        Ok(Self::parse(url.as_deref(), secret.as_deref())?
            .map(|t| t.allowing_plain_patch(plain_patch)))
    }

    /// Whether a patch's link may be plain http to another host
    /// ([`ENV_PATCH_INSECURE`]).
    pub fn allowing_plain_patch(mut self, allow: bool) -> Self {
        self.plain_patch = allow;
        self
    }

    /// The URL and the secret file's content, each `None` when unset.
    pub fn parse(url: Option<&str>, secret: Option<&str>) -> Result<Option<Self>> {
        let (url, secret) = match (url, secret) {
            (None, None) => return Ok(None),
            (Some(url), Some(secret)) => (url.trim(), secret.trim()),
            (Some(_), None) => bail!("{ENV_URL} is set without {ENV_SECRET_FILE}: the service would refuse every request"),
            (None, Some(_)) => bail!("{ENV_SECRET_FILE} is set without {ENV_URL}: nothing would be asked"),
        };
        if secret.is_empty() {
            bail!("{ENV_SECRET_FILE} holds nothing");
        }
        if secret.chars().any(|c| c.is_control() || c == ' ') {
            bail!("{ENV_SECRET_FILE} holds more than one token: a space or a control character");
        }
        let parsed = reqwest::Url::parse(url).map_err(|e| anyhow!("{ENV_URL}={url:?}: {e}"))?;
        match parsed.scheme() {
            "https" => {}
            "http" => {
                let host = parsed.host_str().unwrap_or_default();
                let loopback = host == "localhost"
                    || host
                        .trim_start_matches('[')
                        .trim_end_matches(']')
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|ip| ip.is_loopback());
                if !loopback {
                    bail!(
                        "{ENV_URL}={url:?} is plain http to another host: {ENV_SECRET_FILE}'s secret, and every credential the service gives, would cross the network unencrypted. It must be https://, or http:// to this machine"
                    );
                }
            }
            other => bail!("{ENV_URL}={url:?}: a {other}:// URL, not https://"),
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            bail!("{ENV_URL} carries a user: the secret goes in {ENV_SECRET_FILE}");
        }
        let builder = reqwest::Client::builder()
            .timeout(TIMEOUT)
            .redirect(reqwest::redirect::Policy::none());
        // Plain http is this machine, by the check above, and what goes to
        // it leaves no interface: not by a proxy either. An https request
        // through a proxy is still TLS to its end.
        let builder = match parsed.scheme() {
            "http" => builder.no_proxy(),
            _ => builder,
        };
        let http = builder
            .user_agent(concat!("dagron-engine/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| anyhow!("an HTTP client for {ENV_URL}: {e}"))?;
        Ok(Some(Self {
            url: parsed,
            secret: secret.to_string(),
            http,
            plain_patch: false,
        }))
    }

    pub fn url(&self) -> &str {
        self.url.as_str()
    }
}

/// A patch's link as the service gave it, if the checkout may fetch it: a
/// bearer's link to the change, so https://, or http:// to this machine, or
/// to another where `plain` (ENV_PATCH_INSECURE) allows it.
fn patch_link(link: &str, plain: bool) -> Result<String> {
    let parsed = reqwest::Url::parse(link)
        .map_err(|e| anyhow!("{ENV_URL} answered a patch link that is not a URL: {e}"))?;
    match parsed.scheme() {
        "https" => {}
        "http" => {
            let host = parsed.host_str().unwrap_or_default();
            let loopback = host == "localhost"
                || host
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback());
            if !loopback && !plain {
                bail!(
                    "{ENV_URL} answered a patch link over plain http to another host: whoever sees \
                     it on the way reads the change. dagron-ci gives one at CI_URL_FOR_JOBS, which \
                     wants https://; on a closed network where that is accepted, set \
                     {ENV_PATCH_INSECURE}=allow"
                );
            }
        }
        _ => bail!("{ENV_URL} answered a patch link that is not http(s)"),
    }
    Ok(link.to_string())
}

/// What a service said, cut to a line that fits a task's error.
async fn said(response: reqwest::Response) -> String {
    let text = response.text().await.unwrap_or_default();
    let line: String = text.trim().chars().take(300).collect();
    if line.is_empty() {
        "nothing".to_string()
    } else {
        line
    }
}

#[derive(serde::Deserialize)]
struct Minted {
    username: String,
    password: String,
}

#[async_trait]
impl CheckoutTokens for HttpTokens {
    async fn mint(&self, request: &TokenRequest) -> Result<Option<Credential>> {
        let mut body = serde_json::json!({
            "repository": request.repository,
            "commit": request.commit,
            "run_id": request.run_id,
            "task_id": request.task_id,
            "attempt": request.attempt,
        });
        // A forge token the task holds says what it may do; the checkout's
        // own says nothing, as before.
        if let Some(permissions) = &request.permissions {
            body["permissions"] = serde_json::json!(permissions);
        }
        // An action's fetch says so, and its ref is the commit's place.
        if request.action {
            body["action"] = serde_json::json!(true);
        }
        let response = self
            .http
            .post(self.url.clone())
            .bearer_auth(&self.secret)
            .json(&body)
            .send()
            .await
            .map_err(|e| anyhow!("{ENV_URL} could not be reached: {e}"))?;
        let status = response.status();
        match status.as_u16() {
            200 => {
                let minted: Minted = response
                    .json()
                    .await
                    .map_err(|e| anyhow!("{ENV_URL} answered 200 with no credential: {e}"))?;
                if minted.password.is_empty() {
                    bail!("{ENV_URL} answered 200 with an empty password");
                }
                Ok(Some(Credential {
                    username: minted.username,
                    password: minted.password,
                }))
            }
            204 => Ok(None),
            _ => bail!("{ENV_URL} answered {status}: {}", said(response).await),
        }
    }

    async fn patch(&self, request: &TokenRequest) -> Result<String> {
        #[derive(serde::Deserialize)]
        struct Patch {
            patch_url: String,
        }
        let response = self
            .http
            .post(self.url.clone())
            .bearer_auth(&self.secret)
            .json(&serde_json::json!({
                "repository": request.repository,
                "commit": request.commit,
                "run_id": request.run_id,
                "task_id": request.task_id,
                "attempt": request.attempt,
                "patch": true,
            }))
            .send()
            .await
            .map_err(|e| anyhow!("{ENV_URL} could not be reached: {e}"))?;
        let status = response.status();
        if status.as_u16() != 200 {
            bail!(
                "{ENV_URL} gave no patch: it answered {status}: {}",
                said(response).await
            );
        }
        let patch: Patch = response
            .json()
            .await
            .map_err(|e| anyhow!("{ENV_URL} answered 200 with no patch link: {e}"))?;
        patch_link(&patch.patch_url, self.plain_patch)
    }

    async fn revoke(&self, credential: &Credential) -> Result<()> {
        let response = self
            .http
            .delete(self.url.clone())
            .bearer_auth(&self.secret)
            .json(&serde_json::json!({
                "username": credential.username,
                "password": credential.password,
            }))
            .send()
            .await
            .map_err(|e| anyhow!("{ENV_URL} could not be reached: {e}"))?;
        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            bail!(
                "{ENV_URL} answered {status} to the revoke: {}",
                said(response).await
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "ops")]
    use std::sync::{Arc, Mutex};

    #[test]
    fn the_service_is_named_whole_or_not_at_all() {
        assert!(HttpTokens::parse(None, None).unwrap().is_none());
        let ok = HttpTokens::parse(
            Some(" https://ci.example.com/ci/v1/checkout-tokens "),
            Some("s3cret\n"),
        )
        .unwrap()
        .unwrap();
        assert_eq!(ok.url(), "https://ci.example.com/ci/v1/checkout-tokens");
        assert_eq!(ok.secret, "s3cret");
        assert!(!format!("{ok:?}").contains("s3cret"));
        // Plain http is taken to this machine alone, however it is named.
        for here in [
            "http://127.0.0.1:8080/t",
            "http://localhost:8080/t",
            "http://[::1]:8080/t",
        ] {
            assert!(
                HttpTokens::parse(Some(here), Some("s")).unwrap().is_some(),
                "{here}"
            );
        }
        for (url, secret, why) in [
            (
                Some("https://ci/t"),
                None,
                "without DAGRON_CHECKOUT_TOKEN_SECRET_FILE",
            ),
            (None, Some("s"), "without DAGRON_CHECKOUT_TOKEN_URL"),
            (Some("https://ci/t"), Some("  "), "holds nothing"),
            (Some("https://ci/t"), Some("a b"), "more than one token"),
            (Some("ftp://ci/t"), Some("s"), "not https://"),
            (
                Some("http://dagron-ci.ci.svc:8090/ci/v1/checkout-tokens"),
                Some("s"),
                "is plain http to another host",
            ),
            (
                Some("http://localhost.example.com/t"),
                Some("s"),
                "must be https://, or http:// to this machine",
            ),
            (Some("https://u:p@ci/t"), Some("s"), "carries a user"),
            (
                Some("not a url"),
                Some("s"),
                "DAGRON_CHECKOUT_TOKEN_URL=\"not a url\"",
            ),
        ] {
            let err = HttpTokens::parse(url, secret).unwrap_err().to_string();
            assert!(err.contains(why), "{url:?} {secret:?}: {err}");
        }
    }

    #[test]
    fn a_patch_link_is_https_or_to_this_machine_unless_allowed() {
        let ok = "https://ci.example/ci/v1/bundles/b/content?expires=1&sig=00";
        assert_eq!(patch_link(ok, false).unwrap(), ok);
        assert!(patch_link("http://127.0.0.1:8090/ci/v1/bundles/b/content", false).is_ok());
        assert!(patch_link("http://localhost:8090/x", false).is_ok());
        let plain = "http://ci-door.ws.svc:8090/ci/v1/bundles/b/content";
        let err = patch_link(plain, false).unwrap_err().to_string();
        assert!(err.contains(ENV_PATCH_INSECURE), "{err}");
        assert_eq!(patch_link(plain, true).unwrap(), plain);
        assert!(patch_link("ftp://ci/x", true).is_err());
        assert!(patch_link("not a link", true).is_err());
    }

    /// A service of the test's own on a loopback port: what it was asked, by
    /// whom, and the answer each call gets.
    #[cfg(feature = "ops")]
    struct Service {
        url: String,
        asked: Arc<Mutex<Vec<(String, String, serde_json::Value)>>>,
    }

    #[cfg(feature = "ops")]
    async fn service(answer: axum::http::StatusCode, body: &'static str) -> Service {
        use axum::{extract::State, http::HeaderMap, routing::any, Router};
        type Asked = Arc<Mutex<Vec<(String, String, serde_json::Value)>>>;
        let asked: Asked = Arc::default();
        let app = Router::new()
            .route(
                "/ci/v1/checkout-tokens",
                any(
                    move |State(asked): State<Asked>,
                          method: axum::http::Method,
                          headers: HeaderMap,
                          payload: String| async move {
                        let auth = headers
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or_default()
                            .to_string();
                        let json =
                            serde_json::from_str(&payload).unwrap_or(serde_json::Value::Null);
                        asked.lock().unwrap().push((method.to_string(), auth, json));
                        (answer, [("content-type", "application/json")], body)
                    },
                ),
            )
            .with_state(asked.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Service {
            url: format!("http://{addr}/ci/v1/checkout-tokens"),
            asked,
        }
    }

    #[cfg(feature = "ops")]
    fn request() -> TokenRequest {
        TokenRequest {
            repository: "https://github.com/acme/widgets.git".into(),
            commit: "4b4708e725f29891d22b4c96af9f0a403ff51c7c".into(),
            run_id: "r-1".into(),
            task_id: "t-1".into(),
            attempt: 2,
            permissions: None,
            action: false,
        }
    }

    #[cfg(feature = "ops")]
    #[tokio::test]
    async fn a_credential_is_asked_for_with_the_secret_and_revoked_the_same_way() {
        let svc = service(
            axum::http::StatusCode::OK,
            r#"{"username":"x-access-token","password":"ghs_1","expires_at":"2026-10-05T12:00:00Z"}"#,
        )
        .await;
        let tokens = HttpTokens::parse(Some(&svc.url), Some("s3cret"))
            .unwrap()
            .unwrap();
        let credential = tokens.mint(&request()).await.unwrap().unwrap();
        assert_eq!(credential.username, "x-access-token");
        assert_eq!(credential.password, "ghs_1");
        tokens.revoke(&credential).await.unwrap();
        let asked = svc.asked.lock().unwrap().clone();
        assert_eq!(asked.len(), 2);
        assert_eq!(asked[0].0, "POST");
        assert_eq!(asked[0].1, "Bearer s3cret");
        assert_eq!(
            asked[0].2,
            serde_json::json!({
                "repository": "https://github.com/acme/widgets.git",
                "commit": "4b4708e725f29891d22b4c96af9f0a403ff51c7c",
                "run_id": "r-1", "task_id": "t-1", "attempt": 2,
            })
        );
        assert_eq!(asked[1].0, "DELETE");
        assert_eq!(asked[1].1, "Bearer s3cret");
        assert_eq!(
            asked[1].2,
            serde_json::json!({"username": "x-access-token", "password": "ghs_1"})
        );

        // An action's fetch says so; its ref is where the commit is.
        let action = TokenRequest {
            repository: "https://github.com/acme/tool.git".into(),
            commit: "v1".into(),
            action: true,
            ..request()
        };
        tokens.mint(&action).await.unwrap().unwrap();
        assert_eq!(
            svc.asked.lock().unwrap()[2].2,
            serde_json::json!({
                "repository": "https://github.com/acme/tool.git",
                "commit": "v1",
                "run_id": "r-1", "task_id": "t-1", "attempt": 2,
                "action": true,
            })
        );
    }

    /// A service that answers with a redirect is not followed there: its
    /// answer is a refusal like any other, and where it points hears neither
    /// the secret nor, on a revoke, the credential.
    #[cfg(feature = "ops")]
    #[tokio::test]
    async fn a_redirect_is_not_followed() {
        use axum::http::StatusCode;
        let elsewhere = service(
            StatusCode::OK,
            r#"{"username":"x-access-token","password":"ghs_1"}"#,
        )
        .await;
        let to = elsewhere.url.clone();
        let app = axum::Router::new().fallback(move || {
            let to = to.clone();
            async move { (StatusCode::TEMPORARY_REDIRECT, [("location", to)]) }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/ci/v1/checkout-tokens",
            listener.local_addr().unwrap()
        );
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let tokens = HttpTokens::parse(Some(&url), Some("s3cret"))
            .unwrap()
            .unwrap();

        let err = tokens.mint(&request()).await.unwrap_err().to_string();
        assert!(err.contains("answered 307 Temporary Redirect"), "{err}");
        let credential = Credential {
            username: "x-access-token".into(),
            password: "ghs_1".into(),
        };
        let err = tokens.revoke(&credential).await.unwrap_err().to_string();
        assert!(err.contains("answered 307 Temporary Redirect"), "{err}");
        assert!(elsewhere.asked.lock().unwrap().is_empty());
    }

    #[cfg(feature = "ops")]
    #[tokio::test]
    async fn no_content_is_no_credential_and_a_refusal_fails_in_the_service_s_words() {
        let none = service(axum::http::StatusCode::NO_CONTENT, "").await;
        let tokens = HttpTokens::parse(Some(&none.url), Some("s"))
            .unwrap()
            .unwrap();
        assert_eq!(tokens.mint(&request()).await.unwrap(), None);

        let refused = service(
            axum::http::StatusCode::FORBIDDEN,
            r#"{"error":"run r-1 is not for this repository"}"#,
        )
        .await;
        let tokens = HttpTokens::parse(Some(&refused.url), Some("s"))
            .unwrap()
            .unwrap();
        let err = tokens.mint(&request()).await.unwrap_err().to_string();
        assert!(err.contains("answered 403 Forbidden"), "{err}");
        assert!(err.contains("not for this repository"), "{err}");

        let empty = service(
            axum::http::StatusCode::OK,
            r#"{"username":"x","password":""}"#,
        )
        .await;
        let tokens = HttpTokens::parse(Some(&empty.url), Some("s"))
            .unwrap()
            .unwrap();
        assert!(tokens
            .mint(&request())
            .await
            .unwrap_err()
            .to_string()
            .contains("empty password"));
    }
}
