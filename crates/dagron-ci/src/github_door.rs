//! **The GitHub API door**: where a workflow's job reaches GitHub's REST API,
//! where the operator says so (`CI_GITHUB_API_FOR_JOBS`), so that
//! `actions/download-artifact`'s `run-id` reads another run's artifacts of
//! dagron's. A job's `GITHUB_API_URL` (`github.api_url`) is then this door,
//! `/ci/v1/github-api`, at the URL the operator names.
//!
//! The door answers two of GitHub's calls itself, for a run of dagron's:
//!
//! - `GET /repos/{owner}/{repo}/actions/runs/{run_id}/artifacts`: the run's
//!   artifacts, as GitHub lists them, where `run_id` is a `github.run_id`
//!   dagron-ci gave a run of that repository;
//! - `GET /repos/{owner}/{repo}/actions/artifacts/{id}/zip`: a redirect,
//!   as GitHub's, to a link of the door's own that streams the artifact's
//!   zip from the run's cell (`dagron-api`), good for five minutes.
//!
//! Either is answered only for a token dagron-ci gave a job of that same
//! repository, in a trusted run, that it has not revoked (a job's
//! `GITHUB_TOKEN`): the repository's own runs, as GitHub's token reads them.
//! A run that is not dagron's is GitHub's to answer.
//!
//! **Everything else is GitHub's**, passed on to its API
//! (`CI_GITHUB_API_URL`) as it came, credential and all, and its answer
//! handed back as it was, a redirect not followed. A request that carries
//! no credential is refused: the door is no way to call GitHub as nobody
//! from dagron-ci's address.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderName, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Json;
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::server::AppState;

/// Where the door is served.
pub const PATH: &str = "/ci/v1/github-api";

/// The most a request passed on to GitHub may carry.
pub const MAX_BODY: usize = 32 << 20;

/// How long a link to an artifact's zip is good for.
const LINK_SECS: u64 = 300;

/// What the door needs: where jobs reach it, where GitHub's API is, the
/// token dagron-ci asks a cell's dagron-api with, and the key its links are
/// signed with.
pub struct Door {
    /// The door, as a job reaches it: what its `GITHUB_API_URL` is.
    pub public: String,
    /// GitHub's API, where what the door does not answer goes.
    pub upstream: String,
    /// dagron-ci's own token for each cell's dagron-api (`CI_API_TOKEN_FILE`).
    pub api_token: Option<String>,
    key: [u8; 32],
    /// One client for plain http to this machine and one that uses the
    /// environment's proxy, as dagron-ci's others ([`crate::config::Clients`]).
    http: crate::config::Clients,
}

impl Door {
    /// The door at `public`, passing on to `upstream`, its links signed with
    /// a key made from `secret`, which no job holds.
    pub fn new(public: String, upstream: String, api_token: Option<String>, secret: &[u8]) -> Door {
        let mut digest = Sha256::new();
        digest.update(b"dagron-ci github api door\0");
        digest.update(secret);
        Door {
            public: public.trim_end_matches('/').to_string(),
            upstream: upstream.trim_end_matches('/').to_string(),
            api_token,
            key: digest.finalize().into(),
            // No bound on the whole of a request, which a large zip streamed
            // through would outlast; a bound on making the connection, and on
            // each wait for more of the answer.
            http: crate::config::Clients::new(|| {
                reqwest::Client::builder()
                    .redirect(reqwest::redirect::Policy::none())
                    .connect_timeout(Duration::from_secs(15))
                    .read_timeout(Duration::from_secs(120))
                    .user_agent(concat!("dagron-ci/", env!("CARGO_PKG_VERSION")))
            }),
        }
    }

    fn sign(&self, run: &str, name: &str, expires: u64) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key).expect("any key length");
        mac.update(format!("{run}\n{name}\n{expires}").as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    fn verified(&self, run: &str, name: &str, expires: u64, sig: &str) -> bool {
        let Ok(sig) = hex::decode(sig) else {
            return false;
        };
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key).expect("any key length");
        mac.update(format!("{run}\n{name}\n{expires}").as_bytes());
        mac.verify_slice(&sig).is_ok()
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

/// GitHub's own way of saying no.
fn said(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({ "message": message.into() }))).into_response()
}

/// `{PATH}/{*rest}`: one of GitHub's calls.
pub async fn door(
    State(st): State<AppState>,
    Path(rest): Path<String>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(door) = st.door.clone() else {
        return said(
            StatusCode::NOT_FOUND,
            "this dagron-ci serves no GitHub API door (CI_GITHUB_API_FOR_JOBS)",
        );
    };
    let parts: Vec<&str> = rest.split('/').collect();
    if method == Method::GET {
        let query = uri.query().unwrap_or_default();
        let answered = match parts.as_slice() {
            ["repos", owner, repo, "actions", "runs", run, "artifacts"] => {
                runs_artifacts(&st, &door, owner, repo, run, query, &headers).await
            }
            ["repos", owner, repo, "actions", "artifacts", id, "zip"] => {
                artifact_zip(&st, &door, owner, repo, id, &headers).await
            }
            ["_dagron", "artifacts", run, name] => {
                return linked(&st, &door, run, name, query).await;
            }
            _ => None,
        };
        if let Some(answer) = answered {
            return answer;
        }
    }
    // GitHub's path as it was sent, its escapes and all: what was decoded
    // to read it here is not what is passed on.
    let raw = uri
        .path()
        .strip_prefix(PATH)
        .unwrap_or(uri.path())
        .trim_start_matches('/');
    passed_on(&door, method, raw, uri.query(), &headers, body).await
}

/// The value of `key` in a URL's query, decoded as a form's is.
fn param(query: &str, key: &str) -> Option<String> {
    reqwest::Url::parse(&format!("http://query.invalid/?{query}"))
        .ok()?
        .query_pairs()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.into_owned())
}

/// The repository `owner/repo` on github.com, where dagron-ci serves it.
async fn served(st: &AppState, owner: &str, repo: &str) -> Option<crate::db::Repo> {
    st.db
        .find_repo(crate::github::DEFAULT_HOST, &format!("{owner}/{repo}"))
        .await
        .ok()
        .flatten()
}

/// Whether the request's token is one dagron-ci gave a job of `repo`, in a
/// trusted run, and has not revoked; why not otherwise, as GitHub says no.
async fn authorized(
    st: &AppState,
    repo: &crate::db::Repo,
    headers: &HeaderMap,
) -> Result<(), Response> {
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.strip_prefix("token ")
                .or_else(|| v.strip_prefix("Bearer "))
        })
        .map(str::trim)
        .filter(|t| !t.is_empty());
    let Some(token) = token else {
        return Err(said(StatusCode::UNAUTHORIZED, "Requires authentication"));
    };
    let sha = hex::encode(Sha256::digest(token.as_bytes()));
    let refused = || {
        said(
            StatusCode::FORBIDDEN,
            "a run of dagron's is read with a token dagron-ci gave a job of the same repository, \
             in a trusted run: its GITHUB_TOKEN",
        )
    };
    let Ok(Some((run, repository, _))) = st.db.live_credential(&sha).await else {
        return Err(refused());
    };
    let own = format!("https://{}/{}.git", repo.forge_host, repo.full_name);
    // Revoked as its attempt ends: one not revoked is a job's still.
    if !repository.eq_ignore_ascii_case(&own) {
        return Err(refused());
    }
    match st.db.run_commit(&run).await {
        Ok(Some(of)) if of.trust == "trusted" && of.repo.id == repo.id => Ok(()),
        _ => Err(refused()),
    }
}

/// The cell's dagron-api's answer to `path`, as dagron-ci.
async fn cell_get(
    st: &AppState,
    door: &Door,
    cell: &str,
    path: &str,
) -> Result<reqwest::Response, Response> {
    let (Some(base), Some(token)) = (st.cells.get(cell), door.api_token.as_deref()) else {
        return Err(said(
            StatusCode::SERVICE_UNAVAILABLE,
            "the run's cell cannot be asked: it is not in CI_CELLS, or there is no CI_API_TOKEN_FILE",
        ));
    };
    let url = format!("{}{path}", base.trim_end_matches('/'));
    door.http
        .to(&url)
        .get(&url)
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| {
            tracing::warn!(error = %e, cell, "a cell's dagron-api could not be asked for a run's artifacts");
            said(StatusCode::BAD_GATEWAY, "the run's cell could not be asked")
        })
}

/// An artifact as GitHub's REST API writes one.
fn as_github(door: &Door, owner: &str, repo: &str, github_run: i64, a: &Value) -> Value {
    let id = a["id"].as_i64().unwrap_or_default();
    let base = format!(
        "{}/repos/{owner}/{repo}/actions/artifacts/{id}",
        door.public
    );
    json!({
        "id": id,
        "node_id": "",
        "name": a["name"],
        "size_in_bytes": a["size_bytes"].as_i64().unwrap_or_default(),
        "url": base,
        "archive_download_url": format!("{base}/zip"),
        "expired": false,
        "digest": a["sha256"].as_str().map(|s| format!("sha256:{s}")),
        "created_at": a["created_at"],
        "updated_at": a["created_at"],
        "expires_at": a["expires_at"],
        "workflow_run": { "id": github_run },
    })
}

/// GitHub's listing of a run's artifacts, for a run of dagron's; `None` for
/// one that is not, which is GitHub's to answer.
async fn runs_artifacts(
    st: &AppState,
    door: &Door,
    owner: &str,
    repo: &str,
    run: &str,
    query: &str,
    headers: &HeaderMap,
) -> Option<Response> {
    let github_run: i64 = run.parse().ok()?;
    let served = served(st, owner, repo).await?;
    let (run_id, cell) = st.db.run_numbered(&served.id, github_run).await.ok()??;
    if let Err(refusal) = authorized(st, &served, headers).await {
        return Some(refusal);
    }
    let listed = match cell_get(
        st,
        door,
        &cell,
        &format!("/api/runs/{run_id}/workflow-artifacts"),
    )
    .await
    {
        Ok(r) if r.status().is_success() => r.json::<Value>().await.ok(),
        Ok(r) => {
            tracing::warn!(status = %r.status(), run_id, "a cell's dagron-api would not list a run's artifacts");
            None
        }
        Err(refusal) => return Some(refusal),
    };
    let Some(listed) = listed else {
        return Some(said(
            StatusCode::BAD_GATEWAY,
            "the run's cell would not list its artifacts",
        ));
    };
    let param = |key: &str| param(query, key);
    let named = param("name");
    let all: Vec<Value> = listed["artifacts"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|a| {
            named
                .as_deref()
                .is_none_or(|n| a["name"].as_str() == Some(n))
        })
        .map(|a| as_github(door, owner, repo, github_run, a))
        .collect();
    let per_page: usize = param("per_page")
        .and_then(|p| p.parse().ok())
        .unwrap_or(30)
        .clamp(1, 100);
    let page: usize = param("page")
        .and_then(|p| p.parse().ok())
        .unwrap_or(1)
        .max(1);
    let shown: Vec<Value> = all
        .iter()
        .skip((page - 1) * per_page)
        .take(per_page)
        .cloned()
        .collect();
    Some(Json(json!({ "total_count": all.len(), "artifacts": shown })).into_response())
}

/// GitHub's redirect to an artifact's zip, for one of a run of dagron's;
/// `None` for one that is not.
async fn artifact_zip(
    st: &AppState,
    door: &Door,
    owner: &str,
    repo: &str,
    id: &str,
    headers: &HeaderMap,
) -> Option<Response> {
    let number: i64 = id.parse().ok()?;
    let served = served(st, owner, repo).await?;
    let found = match cell_get(
        st,
        door,
        &served.cell,
        &format!("/api/workflow-artifacts/{number}"),
    )
    .await
    {
        Ok(r) if r.status().is_success() => r.json::<Value>().await.ok()?,
        // Not one of dagron's: GitHub's to answer.
        Ok(_) => return None,
        Err(refusal) => return Some(refusal),
    };
    let run_id = found["run_id"].as_str()?.to_string();
    let name = found["name"].as_str()?.to_string();
    // One of another repository's runs is none of this one's.
    match st.db.run_place(&run_id).await {
        Ok(Some((repo_id, _))) if repo_id == served.id => {}
        _ => return Some(said(StatusCode::NOT_FOUND, "Not Found")),
    }
    if let Err(refusal) = authorized(st, &served, headers).await {
        return Some(refusal);
    }
    let expires = now() + LINK_SECS;
    let sig = door.sign(&run_id, &name, expires);
    let link = format!(
        "{}/_dagron/artifacts/{run_id}/{}?expires={expires}&sig={sig}",
        door.public,
        segment(&name)
    );
    Some(
        Response::builder()
            .status(StatusCode::FOUND)
            .header(axum::http::header::LOCATION, link)
            .body(Body::empty())
            .expect("a redirect is made"),
    )
}

/// The zip a link of the door's names, streamed from the run's cell, where
/// the link is the door's and has not lapsed.
async fn linked(st: &AppState, door: &Door, run: &str, name: &str, query: &str) -> Response {
    let param = |key: &str| param(query, key);
    let (Some(expires), Some(sig)) = (
        param("expires").and_then(|e| e.parse::<u64>().ok()),
        param("sig"),
    ) else {
        return said(StatusCode::FORBIDDEN, "not a link of this door's");
    };
    if expires < now() || !door.verified(run, &name, expires, &sig) {
        return said(
            StatusCode::FORBIDDEN,
            "not a link of this door's, or one that has lapsed",
        );
    }
    let Ok(Some((_, cell))) = st.db.run_place(run).await else {
        return said(StatusCode::NOT_FOUND, "Not Found");
    };
    let path = format!("/api/runs/{run}/workflow-artifacts/{}", segment(&name));
    let answer = match cell_get(st, door, &cell, &path).await {
        Ok(answer) => answer,
        Err(refusal) => return refusal,
    };
    let status = StatusCode::from_u16(answer.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut response = Response::builder().status(status);
    for key in ["content-type", "content-length", "content-disposition"] {
        if let Some(value) = answer.headers().get(key) {
            response = response.header(key, value.as_bytes());
        }
    }
    response
        .body(Body::from_stream(answer.bytes_stream()))
        .unwrap_or_else(|_| said(StatusCode::BAD_GATEWAY, "the zip could not be handed on"))
}

/// `text` as one segment of a URL path has it.
fn segment(text: &str) -> String {
    text.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                char::from(b).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// Headers that are a connection's, not a request's: not passed on.
fn hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "host"
            | "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "content-length"
            | "accept-encoding"
    )
}

/// The request passed on to GitHub's API as it came, and its answer handed
/// back as it was.
async fn passed_on(
    door: &Door,
    method: Method,
    rest: &str,
    query: Option<&str>,
    headers: &HeaderMap,
    body: Bytes,
) -> Response {
    if !headers.contains_key(axum::http::header::AUTHORIZATION) {
        return said(
            StatusCode::UNAUTHORIZED,
            "dagron-ci passes on to GitHub a request that carries a credential, and no other",
        );
    }
    let mut url = format!("{}/{rest}", door.upstream);
    if let Some(query) = query {
        url.push('?');
        url.push_str(query);
    }
    let Ok(method) = reqwest::Method::from_bytes(method.as_str().as_bytes()) else {
        return said(StatusCode::METHOD_NOT_ALLOWED, "not a method GitHub takes");
    };
    let mut request = door.http.to(&url).request(method, &url);
    for (name, value) in headers {
        if !hop(name) {
            request = request.header(name.as_str(), value.as_bytes());
        }
    }
    if !body.is_empty() {
        request = request.body(body.to_vec());
    }
    let answer = match request.send().await {
        Ok(answer) => answer,
        Err(e) => {
            tracing::warn!(error = %e, "GitHub's API could not be reached through the door");
            return said(StatusCode::BAD_GATEWAY, "GitHub's API could not be reached");
        }
    };
    let status = StatusCode::from_u16(answer.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut response = Response::builder().status(status);
    for (name, value) in answer.headers() {
        if !matches!(
            name.as_str(),
            "connection" | "transfer-encoding" | "keep-alive"
        ) {
            response = response.header(name.as_str(), value.as_bytes());
        }
    }
    response
        .body(Body::from_stream(answer.bytes_stream()))
        .unwrap_or_else(|_| {
            said(
                StatusCode::BAD_GATEWAY,
                "GitHub's answer could not be handed on",
            )
        })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::body::Body;
    use axum::http::Request;
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt;

    use super::*;
    use crate::config::Config;
    use crate::db::tests::sqlite;
    use crate::db::{CheckoutCredential, SubmittedRun};
    use crate::dispatch::tests::claimed;
    use crate::event::{Kind, Trust};
    use crate::server::router;
    use crate::Seams;

    const DOOR: &str = "http://door.example/ci/v1/github-api";
    const TOKEN: &str = "ghs_the_jobs_own";

    /// What GitHub's API stand-in was asked: each path and its credential.
    type Asked = Arc<Mutex<Vec<(String, String)>>>;

    /// A cell's dagron-api that has one run, `run-1`, with one artifact,
    /// `built` (number 41), whose zip is `PK-the-zip`; and GitHub's API,
    /// which answers anything with what it was asked.
    async fn stubs() -> (String, String, Asked) {
        async fn listed() -> Json<Value> {
            Json(json!({ "run_id": "run-1", "artifacts": [{
                "id": 41, "name": "built", "size_bytes": 10, "sha256": "ab",
                "created_at": "2026-10-17T00:00:00Z", "expires_at": "2026-10-19T00:00:00Z",
            }]}))
        }
        async fn numbered(Path(id): Path<i64>) -> Response {
            match id {
                41 => Json(json!({ "id": 41, "run_id": "run-1", "name": "built" })).into_response(),
                _ => StatusCode::NOT_FOUND.into_response(),
            }
        }
        async fn zip() -> Response {
            ([("content-type", "application/zip")], "PK-the-zip").into_response()
        }
        let cell = Router::new()
            .route("/api/runs/run-1/workflow-artifacts", get(listed))
            .route("/api/runs/run-1/workflow-artifacts/built", get(zip))
            .route("/api/workflow-artifacts/{id}", get(numbered));
        let asked: Asked = Arc::default();
        let kept = asked.clone();
        let github = Router::new().fallback(move |uri: Uri, headers: HeaderMap| {
            let kept = kept.clone();
            async move {
                let auth = headers
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_string();
                kept.lock().unwrap().push((uri.to_string(), auth));
                Json(json!({ "github": uri.path() }))
            }
        });
        let mut bases = Vec::new();
        for app in [cell, github] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            bases.push(format!("http://{}", listener.local_addr().unwrap()));
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        }
        (bases.remove(0), bases.remove(0), asked)
    }

    async fn call(app: &Router, path: &str, token: Option<&str>) -> Response {
        let mut req = Request::get(path);
        if let Some(token) = token {
            req = req.header("authorization", format!("token {token}"));
        }
        app.clone()
            .oneshot(req.body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    async fn body(res: Response) -> Vec<u8> {
        axum::body::to_bytes(res.into_body(), 1 << 20)
            .await
            .unwrap()
            .to_vec()
    }

    /// download-artifact's `run-id`, for a run of dagron's: the run's
    /// artifacts listed as GitHub lists them, and a redirect to a link of
    /// the door's own that streams the zip; only for a token dagron-ci gave
    /// a job of the same repository in a trusted run. A run that is not
    /// dagron's, and any other call, is GitHub's, passed on as it came, and
    /// one with no credential is refused.
    #[tokio::test]
    async fn a_run_s_artifacts_are_the_door_s_and_every_other_call_is_github_s() {
        let (db, _db_dir) = sqlite().await;
        let (cell, github, asked) = stubs().await;
        let dir = tempfile::tempdir().unwrap();
        let token_file = dir.path().join("token");
        std::fs::write(&token_file, "dgr_dagron_ci\n").unwrap();
        let secret_file = dir.path().join("secret");
        std::fs::write(&secret_file, "engines-secret\n").unwrap();
        let config = Config::from_lookup(|name| match name {
            "CI_DATABASE_URL" => Some("sqlite://unused.db".into()),
            "CI_CELLS" => Some(format!("default={cell}")),
            "CI_API_TOKEN_FILE" => Some(token_file.to_str().unwrap().into()),
            "CI_CHECKOUT_TOKEN_SECRET_FILE" => Some(secret_file.to_str().unwrap().into()),
            "CI_GITHUB_API_URL" => Some(github.clone()),
            "CI_GITHUB_API_FOR_JOBS" => Some(DOOR.into()),
            _ => None,
        })
        .unwrap();
        let app = router(crate::server::AppState::new(
            db.clone(),
            &config,
            Seams::default(),
        ));
        let (repo, event) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        assert!(db
            .record_run(&SubmittedRun {
                run_id: "run-1".into(),
                event_id: event.id.clone(),
                repo_id: repo.id.clone(),
                cell: "default".into(),
                pipeline: ".github/workflows/build.yml".into(),
                idempotency_key: "ci-run-1".into(),
                on_failed: false,
            })
            .await
            .unwrap());
        let (github_run, _) = db
            .run_numbers(&repo.id, ".github/workflows/build.yml", "ci-run-1")
            .await
            .unwrap();
        db.record_credential(&CheckoutCredential {
            token_sha256: hex::encode(Sha256::digest(TOKEN.as_bytes())),
            run_id: "run-1".into(),
            task_id: None,
            attempt: None,
            repository: "https://github.com/acme/widgets.git".into(),
            commit: event.sha.clone(),
            cell: "default".into(),
            expires_at: None,
            revoked: false,
            permissions: Some(r#"{"contents":"read"}"#.into()),
            forge_token_id: None,
        })
        .await
        .unwrap();

        let listing = format!("{PATH}/repos/acme/widgets/actions/runs/{github_run}/artifacts");
        let res = call(&app, &listing, Some(TOKEN)).await;
        assert_eq!(res.status(), StatusCode::OK);
        let listed: Value = serde_json::from_slice(&body(res).await).unwrap();
        assert_eq!(listed["total_count"], 1, "{listed}");
        let artifact = &listed["artifacts"][0];
        assert_eq!(artifact["name"], "built");
        assert_eq!(artifact["size_in_bytes"], 10);
        assert_eq!(artifact["digest"], "sha256:ab");
        assert_eq!(
            artifact["archive_download_url"],
            format!("{DOOR}/repos/acme/widgets/actions/artifacts/41/zip")
        );
        let named = call(&app, &format!("{listing}?name=other"), Some(TOKEN)).await;
        let named: Value = serde_json::from_slice(&body(named).await).unwrap();
        assert_eq!(named["total_count"], 0);
        // A name is read as a query writes it, escapes and all.
        let named = call(&app, &format!("{listing}?name=bui%6Ct"), Some(TOKEN)).await;
        let named: Value = serde_json::from_slice(&body(named).await).unwrap();
        assert_eq!(named["total_count"], 1);

        // A token that has lapsed, as the forge said it would, is none.
        db.record_credential(&CheckoutCredential {
            token_sha256: hex::encode(Sha256::digest(b"ghs_lapsed")),
            run_id: "run-1".into(),
            task_id: None,
            attempt: None,
            repository: "https://github.com/acme/widgets.git".into(),
            commit: event.sha.clone(),
            cell: "default".into(),
            expires_at: Some("2020-01-01T00:00:00Z".into()),
            revoked: false,
            permissions: None,
            forge_token_id: None,
        })
        .await
        .unwrap();
        assert_eq!(
            call(&app, &listing, Some("ghs_lapsed")).await.status(),
            StatusCode::FORBIDDEN
        );

        // Not the job's own: refused.
        assert_eq!(
            call(&app, &listing, None).await.status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            call(&app, &listing, Some("ghs_someone_else"))
                .await
                .status(),
            StatusCode::FORBIDDEN
        );

        // The zip: a redirect to the door's own link, which streams it.
        let res = call(
            &app,
            &format!("{PATH}/repos/acme/widgets/actions/artifacts/41/zip"),
            Some(TOKEN),
        )
        .await;
        assert_eq!(res.status(), StatusCode::FOUND);
        let location = res.headers()["location"].to_str().unwrap().to_string();
        let path = location
            .strip_prefix("http://door.example")
            .unwrap()
            .to_string();
        assert!(
            path.starts_with(&format!("{PATH}/_dagron/artifacts/run-1/built?")),
            "{path}"
        );
        let res = call(&app, &path, None).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(body(res).await, b"PK-the-zip");
        let forged = path.replace("sig=", "sig=00");
        assert_eq!(
            call(&app, &forged, None).await.status(),
            StatusCode::FORBIDDEN
        );

        // Not a run of dagron's, and any other call: GitHub's, as it came.
        assert!(asked.lock().unwrap().is_empty());
        let res = call(
            &app,
            &format!("{PATH}/repos/acme/widgets/actions/runs/987654/artifacts"),
            Some(TOKEN),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        let res = call(
            &app,
            &format!("{PATH}/repos/acme/widgets/pulls?state=open"),
            Some("ghp_a_person"),
        )
        .await;
        let said: Value = serde_json::from_slice(&body(res).await).unwrap();
        assert_eq!(said["github"], "/repos/acme/widgets/pulls");
        // A path is passed on as it was sent, an escaped slash still one.
        call(
            &app,
            &format!("{PATH}/repos/acme/widgets/contents/a%2Fb"),
            Some("ghp_a_person"),
        )
        .await;
        let asked = asked.lock().unwrap().clone();
        assert_eq!(
            asked.last().unwrap().0,
            "/repos/acme/widgets/contents/a%2Fb"
        );
        let asked = asked[..2].to_vec();
        assert_eq!(
            asked,
            [
                (
                    "/repos/acme/widgets/actions/runs/987654/artifacts".to_string(),
                    format!("token {TOKEN}")
                ),
                (
                    "/repos/acme/widgets/pulls?state=open".to_string(),
                    "token ghp_a_person".to_string()
                ),
            ]
        );
        assert_eq!(
            call(&app, &format!("{PATH}/user"), None).await.status(),
            StatusCode::UNAUTHORIZED,
            "the door is no way to call GitHub as nobody"
        );
    }

    /// A door that could answer nothing of its own does not start: one with
    /// nothing to sign its links with, or no token to ask a cell with, and
    /// one at a URL that is not the door's.
    #[test]
    fn a_door_that_could_not_answer_does_not_start() {
        let dir = tempfile::tempdir().unwrap();
        let token = dir.path().join("token");
        std::fs::write(&token, "dgr_dagron_ci\n").unwrap();
        let secret = dir.path().join("secret");
        std::fs::write(&secret, "engines-secret\n").unwrap();
        let with = |door: &str, secret_set: bool, token_set: bool| {
            Config::from_lookup(|name| match name {
                "CI_DATABASE_URL" => Some("sqlite://unused.db".into()),
                "CI_CELLS" if token_set => Some("default=http://127.0.0.1:1".into()),
                "CI_API_TOKEN_FILE" if token_set => Some(token.to_str().unwrap().into()),
                "CI_CHECKOUT_TOKEN_SECRET_FILE" if secret_set => {
                    Some(secret.to_str().unwrap().into())
                }
                "CI_GITHUB_API_FOR_JOBS" => Some(door.into()),
                _ => None,
            })
            .map(|_| ())
            .map_err(|e| e.to_string())
        };
        assert_eq!(with(DOOR, true, true), Ok(()));
        assert_eq!(with(&format!("{DOOR}/"), true, true), Ok(()));
        assert!(with(DOOR, false, true)
            .unwrap_err()
            .contains("CI_CHECKOUT_TOKEN_SECRET_FILE"));
        assert!(with(DOOR, true, false)
            .unwrap_err()
            .contains("CI_API_TOKEN_FILE"));
        assert!(with("http://door.example/api", true, true)
            .unwrap_err()
            .contains("/ci/v1/github-api"));
    }

    /// A run that is not trusted (a fork's pull request) reads no run's
    /// artifacts through the door, its own included: GitHub gives such a
    /// run's token nothing of the repository's to write, and the door
    /// gives it nothing of dagron's.
    #[tokio::test]
    async fn a_token_of_a_run_that_is_not_trusted_reads_no_run_s_artifacts() {
        let (db, _db_dir) = sqlite().await;
        let (cell, github, _asked) = stubs().await;
        let dir = tempfile::tempdir().unwrap();
        let token_file = dir.path().join("token");
        std::fs::write(
            &token_file,
            "dgr_dagron_ci
",
        )
        .unwrap();
        let secret_file = dir.path().join("secret");
        std::fs::write(
            &secret_file,
            "engines-secret
",
        )
        .unwrap();
        let config = Config::from_lookup(|name| match name {
            "CI_DATABASE_URL" => Some("sqlite://unused.db".into()),
            "CI_CELLS" => Some(format!("default={cell}")),
            "CI_API_TOKEN_FILE" => Some(token_file.to_str().unwrap().into()),
            "CI_CHECKOUT_TOKEN_SECRET_FILE" => Some(secret_file.to_str().unwrap().into()),
            "CI_GITHUB_API_URL" => Some(github.clone()),
            "CI_GITHUB_API_FOR_JOBS" => Some(DOOR.into()),
            _ => None,
        })
        .unwrap();
        let app = router(crate::server::AppState::new(
            db.clone(),
            &config,
            Seams::default(),
        ));
        let (repo, event) = claimed(&db, None, Kind::Push, Trust::Untrusted).await;
        assert!(db
            .record_run(&SubmittedRun {
                run_id: "run-1".into(),
                event_id: event.id.clone(),
                repo_id: repo.id.clone(),
                cell: "default".into(),
                pipeline: ".github/workflows/build.yml".into(),
                idempotency_key: "ci-run-1".into(),
                on_failed: false,
            })
            .await
            .unwrap());
        let (github_run, _) = db
            .run_numbers(&repo.id, ".github/workflows/build.yml", "ci-run-1")
            .await
            .unwrap();
        db.record_credential(&CheckoutCredential {
            token_sha256: hex::encode(Sha256::digest(TOKEN.as_bytes())),
            run_id: "run-1".into(),
            task_id: None,
            attempt: None,
            repository: "https://github.com/acme/widgets.git".into(),
            commit: event.sha.clone(),
            cell: "default".into(),
            expires_at: None,
            revoked: false,
            permissions: Some(r#"{"contents":"read"}"#.into()),
            forge_token_id: None,
        })
        .await
        .unwrap();
        let listing = format!("{PATH}/repos/acme/widgets/actions/runs/{github_run}/artifacts");
        assert_eq!(
            call(&app, &listing, Some(TOKEN)).await.status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            call(
                &app,
                &format!("{PATH}/repos/acme/widgets/actions/artifacts/41/zip"),
                Some(TOKEN)
            )
            .await
            .status(),
            StatusCode::FORBIDDEN
        );
    }
}
