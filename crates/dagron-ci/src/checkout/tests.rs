//! The checkout credential endpoint, through the router, against a stub
//! GitHub: who is answered, for which repository and commit, and what GitHub
//! is asked for and told.

use std::path::Path;
use std::process::Command;
use std::sync::Mutex;

use axum::body::Body;
use axum::http::{Method, Request, Uri};
use axum::Router;
use jsonwebtoken::{DecodingKey, Validation};
use serde_json::{json, Value};
use tower::ServiceExt;

use super::*;
use crate::config::Config;
use crate::db::tests::{event, sqlite};
use crate::db::{Claimed, Db, Repo, ReportWrite, SubmittedRun};
use crate::dispatch::tests::claimed;
use crate::event::{Kind, Trust};
use crate::server::router;
use crate::Seams;

/// Which of GitHub's answers to a mint are its last word, and which may be
/// answered otherwise next time: the caller falls back on the first, and must
/// not take the second for a repository it has no token for.
#[test]
fn a_mint_s_answer_is_github_s_last_word_or_nothing_to_go_on() {
    use reqwest::header::{HeaderMap, HeaderValue};
    let none = HeaderMap::new();
    for status in [401, 404, 422] {
        assert!(is_refusal(status, &none, ""), "{status}");
    }
    let suspended = r#"{"message":"This installation has been suspended"}"#;
    assert!(is_refusal(403, &none, suspended));
    for status in [408, 429, 500, 502, 503] {
        assert!(!is_refusal(status, &none, ""), "{status}");
    }
    // A 403 that is a rate limit: by either header, or by its words alone.
    for (name, value) in [("retry-after", "30"), ("x-ratelimit-remaining", "0")] {
        let mut headers = HeaderMap::new();
        headers.insert(name, HeaderValue::from_static(value));
        assert!(!is_refusal(403, &headers, suspended), "{name}");
    }
    let limited = r#"{"message":"You have exceeded a secondary rate limit."}"#;
    assert!(!is_refusal(403, &none, limited));
}

const SECRET: &str = "engine-s3cret";
const APP_ID: u64 = 12345;
const INSTALLATION: u64 = 42;
const MINTED: &str = "ghs_for_one_attempt";
const EXPIRES: &str = "2026-10-05T13:00:00Z";
const REPOSITORY: &str = "https://github.com/acme/widgets.git";

/// An RSA key pair, PEM, the private half as GitHub hands an App's out:
/// `None`, and the test skipped, where there is no `openssl` to make one.
pub(crate) fn rsa_key(dir: &Path) -> Option<(Vec<u8>, Vec<u8>)> {
    let path = dir.join("app.pem");
    let made = Command::new("openssl")
        .args([
            "genpkey",
            "-algorithm",
            "RSA",
            "-pkeyopt",
            "rsa_keygen_bits:2048",
            "-out",
        ])
        .arg(&path)
        .output();
    if !made.is_ok_and(|out| out.status.success()) {
        eprintln!("openssl cannot make an RSA key here - skipping");
        return None;
    }
    let public = Command::new("openssl")
        .args(["pkey", "-pubout", "-in"])
        .arg(&path)
        .output()
        .unwrap();
    assert!(public.status.success(), "{public:?}");
    Some((std::fs::read(&path).unwrap(), public.stdout))
}

#[derive(Default)]
struct GithubState {
    /// Whether the App is installed on acme/widgets.
    installed: bool,
    /// A status the token mint answers instead of a token.
    mint_answer: Option<u16>,
    /// How long it takes to answer anything.
    delay: Option<Duration>,
    /// Each request: its method, path, Authorization and body.
    seen: Vec<(String, String, String, String)>,
    /// Another acme repository's visibility and Actions access, where one
    /// is asked for: whether private, and its `access_level`.
    access: Option<(bool, &'static str)>,
}

type StubGithub = Arc<Mutex<GithubState>>;

/// github.com's API, as much of it as a checkout's credential needs.
async fn stub_github() -> (String, StubGithub) {
    async fn handle(
        State(gh): State<StubGithub>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        let delay = gh.lock().unwrap().delay;
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        let mut gh = gh.lock().unwrap();
        let authorization = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        gh.seen.push((
            method.to_string(),
            uri.path().to_string(),
            authorization,
            String::from_utf8_lossy(&body).into_owned(),
        ));
        let path = uri.path();
        if method == Method::GET
            && path.starts_with("/repos/acme/")
            && path.ends_with("/installation")
            && gh.installed
        {
            return axum::Json(json!({ "id": INSTALLATION, "account": { "login": "acme" } }))
                .into_response();
        }
        if method == Method::POST
            && path == format!("/app/installations/{INSTALLATION}/access_tokens")
        {
            if let Some(code) = gh.mint_answer {
                return StatusCode::from_u16(code).unwrap().into_response();
            }
            return (
                StatusCode::CREATED,
                axum::Json(json!({
                    "token": MINTED,
                    "expires_at": EXPIRES,
                    "permissions": { "contents": "read" },
                    "repository_selection": "selected",
                })),
            )
                .into_response();
        }
        if method == Method::DELETE && path == "/installation/token" {
            return StatusCode::NO_CONTENT.into_response();
        }
        if let (Some((private, level)), Some(rest)) = (gh.access, path.strip_prefix("/repos/acme/"))
        {
            if rest.ends_with("/actions/permissions/access") {
                return axum::Json(json!({ "access_level": level })).into_response();
            }
            if !rest.contains('/') {
                return axum::Json(json!({ "private": private })).into_response();
            }
        }
        StatusCode::NOT_FOUND.into_response()
    }
    let gh: StubGithub = Arc::default();
    let app = Router::new().fallback(handle).with_state(gh.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (origin, gh)
}

/// dagron-ci as it starts: its configuration read from these knobs, the
/// endpoint's secret and the App's key from files.
fn configured(dir: &Path, secret: bool, app_key: Option<&[u8]>, github_api: &str) -> Config {
    let secret_file = dir.join("checkout-secret");
    std::fs::write(&secret_file, format!("{SECRET}\n")).unwrap();
    let key_file = dir.join("app-key.pem");
    if let Some(key) = app_key {
        std::fs::write(&key_file, key).unwrap();
    }
    let path = |p: &Path| p.to_str().unwrap().to_string();
    Config::from_lookup(|name| match name {
        "CI_DATABASE_URL" => Some("sqlite://unused.db".into()),
        "CI_CHECKOUT_TOKEN_SECRET_FILE" if secret => Some(path(&secret_file)),
        "CI_GITHUB_APP_ID" if app_key.is_some() => Some(APP_ID.to_string()),
        "CI_GITHUB_APP_KEY_FILE" if app_key.is_some() => Some(path(&key_file)),
        "CI_GITHUB_API_URL" => Some(github_api.into()),
        _ => None,
    })
    .unwrap()
}

/// A run dagron-ci submitted for a push of acme/widgets at `a…a`.
async fn submitted(db: &Db, run_id: &str) -> String {
    submitted_as(db, run_id, Trust::Trusted).await
}

/// [`submitted`], for an event of this trust.
async fn submitted_as(db: &Db, run_id: &str, trust: Trust) -> String {
    let (repo, event) = claimed(db, None, Kind::Push, trust).await;
    assert!(db
        .record_run(&SubmittedRun {
            run_id: run_id.into(),
            event_id: event.id,
            repo_id: repo.id,
            cell: "default".into(),
            pipeline: "build".into(),
            idempotency_key: format!("ci-{run_id}"),
            on_failed: false,
        })
        .await
        .unwrap());
    event.sha
}

/// Record `run_id` for `event` once `after` has passed: dagron-ci reading
/// the answer that names a run its engine has already begun.
fn recorded_after(
    db: &Db,
    repo: &Repo,
    event: &Claimed,
    run_id: &str,
    after: Duration,
) -> tokio::task::JoinHandle<()> {
    let db = db.clone();
    let run = SubmittedRun {
        run_id: run_id.into(),
        event_id: event.id.clone(),
        repo_id: repo.id.clone(),
        cell: "default".into(),
        pipeline: "build".into(),
        idempotency_key: format!("ci-{run_id}"),
        on_failed: false,
    };
    tokio::spawn(async move {
        tokio::time::sleep(after).await;
        assert!(db.record_run(&run).await.unwrap());
    })
}

fn ask(run_id: &str, repository: &str, commit: &str) -> Value {
    json!({
        "repository": repository,
        "commit": commit,
        "run_id": run_id,
        "task_id": "build",
        "attempt": 1,
    })
}

async fn call(
    app: &Router,
    method: Method,
    bearer: Option<&str>,
    body: String,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method(method)
        .uri(PATH)
        .header("content-type", "application/json");
    if let Some(bearer) = bearer {
        req = req.header("authorization", format!("Bearer {bearer}"));
    }
    let res = app
        .clone()
        .oneshot(req.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
        .await
        .unwrap();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, body)
}

async fn mint(app: &Router, body: Value) -> (StatusCode, Value) {
    call(app, Method::POST, Some(SECRET), body.to_string()).await
}

async fn revoke(app: &Router, password: &str) -> (StatusCode, Value) {
    let body = json!({ "username": GITHUB_TOKEN_USER, "password": password });
    call(app, Method::DELETE, Some(SECRET), body.to_string()).await
}

/// `dagron_ci_checkout_tokens_total{outcome}`, as `/metrics` renders it.
async fn counted(app: &Router, outcome: &str) -> u64 {
    let res = app
        .clone()
        .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let text = axum::body::to_bytes(res.into_body(), 1 << 20)
        .await
        .unwrap();
    let series = format!("dagron_ci_checkout_tokens_total{{outcome=\"{outcome}\"}} ");
    String::from_utf8(text.to_vec())
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix(&series)?.parse().ok())
        .unwrap_or_else(|| panic!("no {series}"))
}

/// The claims of an App's token, checked against its public key.
fn claims(token: &str, public: &[u8]) -> Value {
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_issuer(&[APP_ID.to_string()]);
    validation.set_required_spec_claims(&["exp", "iat", "iss"]);
    jsonwebtoken::decode::<Value>(
        token,
        &DecodingKey::from_rsa_pem(public).unwrap(),
        &validation,
    )
    .unwrap_or_else(|e| panic!("{token}: {e}"))
    .claims
}

#[test]
fn the_app_signs_a_token_github_takes() {
    let dir = tempfile::tempdir().unwrap();
    let Some((key, public)) = rsa_key(dir.path()) else {
        return;
    };
    let app = GitHubApp::new(APP_ID, &key).unwrap();
    assert_eq!(app.id(), APP_ID);
    assert!(!format!("{app:?}").contains("PRIVATE"), "{app:?}");

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let claims = claims(&app.jwt(now).unwrap(), &public);
    assert_eq!(claims["iss"], APP_ID.to_string());
    // Back a minute for a clock ahead of GitHub's, and ten minutes in all:
    // the most GitHub takes.
    assert_eq!(claims["iat"], now - 60);
    assert_eq!(claims["exp"], now + 540);
}

#[test]
fn an_app_key_that_is_not_rsa_is_refused() {
    assert!(GitHubApp::new(APP_ID, b"not a key").is_err());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ec.pem");
    let made = Command::new("openssl")
        .args([
            "genpkey",
            "-algorithm",
            "EC",
            "-pkeyopt",
            "ec_paramgen_curve:P-256",
            "-out",
        ])
        .arg(&path)
        .output();
    if !made.is_ok_and(|out| out.status.success()) {
        return eprintln!("openssl cannot make an EC key here - skipping");
    }
    let err = GitHubApp::new(APP_ID, &std::fs::read(&path).unwrap()).unwrap_err();
    assert!(err.to_string().contains("RSA"), "{err}");
}

#[tokio::test]
async fn only_an_engine_holding_the_secret_is_answered() {
    let dir = tempfile::tempdir().unwrap();
    let (origin, gh) = stub_github().await;
    let (db, _db_dir) = sqlite().await;
    let sha = submitted(&db, "run-1").await;
    let app = router(AppState::new(
        db,
        &configured(dir.path(), true, None, &origin),
        Seams::default(),
    ));
    let body = ask("run-1", REPOSITORY, &sha).to_string();

    for bearer in [None, Some("engine-s3cre"), Some("engine-s3cret2"), Some("")] {
        let (status, problem) = call(&app, Method::POST, bearer, body.clone()).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{bearer:?}: {problem}");
        let (status, _) = call(
            &app,
            Method::DELETE,
            bearer,
            json!({ "password": "x" }).to_string(),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{bearer:?}");
    }
    assert_eq!(counted(&app, "refused").await, 8);
    assert!(
        gh.lock().unwrap().seen.is_empty(),
        "GitHub was asked nothing"
    );

    // With it, the run's checkout is answered.
    assert_eq!(
        call(&app, Method::POST, Some(SECRET), body).await.0,
        StatusCode::NO_CONTENT
    );
}

/// Where cells are tenants, each cell's engines hold a secret of their own
/// and are answered for that cell's runs alone; the secret every cell's
/// engines may hold is answered for all of them.
#[tokio::test]
async fn a_cell_s_engine_is_answered_for_its_own_cell_s_runs_alone() {
    let dir = tempfile::tempdir().unwrap();
    let secrets = dir.path().join("checkout-secrets");
    std::fs::create_dir(&secrets).unwrap();
    std::fs::write(secrets.join("default"), "default-s3cret\n").unwrap();
    std::fs::write(secrets.join("other"), "other-s3cret\n").unwrap();
    let token = dir.path().join("api-token");
    std::fs::write(&token, "dgr_token").unwrap();
    let path = |p: &Path| p.to_str().unwrap().to_string();
    let config = Config::from_lookup(|name| match name {
        "CI_DATABASE_URL" => Some("sqlite://unused.db".into()),
        "CI_CELLS" => Some("default=http://127.0.0.1:9,other=http://127.0.0.1:9".into()),
        "CI_API_TOKEN_FILE" => Some(path(&token)),
        "CI_CHECKOUT_TOKEN_SECRETS_DIR" => Some(path(&secrets)),
        _ => None,
    })
    .unwrap();
    let (db, _db_dir) = sqlite().await;
    // Submitted to the `default` cell.
    let sha = submitted(&db, "run-1").await;
    let app = router(AppState::new(db, &config, Seams::default()));
    let body = ask("run-1", REPOSITORY, &sha).to_string();

    let (status, problem) = call(&app, Method::POST, Some("other-s3cret"), body.clone()).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{problem}");
    assert!(
        problem["detail"].as_str().unwrap().contains(
            "submitted to cell \"default\", and this engine's secret is cell \"other\"'s"
        ),
        "{problem}"
    );
    assert_eq!(counted(&app, "refused").await, 1);
    let (status, problem) = call(&app, Method::POST, Some("default-s3cret"), body.clone()).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{problem}");
    // No secret answers for every cell where there are two: dagron-ci does
    // not start on one (config.rs), and knows none here.
    let (status, _) = call(&app, Method::POST, Some(SECRET), body.clone()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    // Revoking takes nothing from anyone: any engine answered may.
    let revoking = json!({ "password": "x" }).to_string();
    assert_eq!(
        call(&app, Method::DELETE, Some("other-s3cret"), revoking)
            .await
            .0,
        StatusCode::NO_CONTENT
    );
}

#[tokio::test]
async fn without_its_secret_dagron_ci_mints_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _db_dir) = sqlite().await;
    let sha = submitted(&db, "run-1").await;
    let app = router(AppState::new(
        db,
        &configured(dir.path(), false, None, "https://api.github.com"),
        Seams::default(),
    ));
    let (status, problem) = mint(&app, ask("run-1", REPOSITORY, &sha)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        problem["detail"]
            .as_str()
            .unwrap()
            .contains("CI_CHECKOUT_TOKEN_SECRET_FILE"),
        "{problem}"
    );
    assert_eq!(revoke(&app, MINTED).await.0, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_credential_is_for_its_run_s_repository_and_commit_while_the_run_lasts() {
    let dir = tempfile::tempdir().unwrap();
    let Some((key, _)) = rsa_key(dir.path()) else {
        return;
    };
    let (origin, gh) = stub_github().await;
    gh.lock().unwrap().installed = true;
    let (db, _db_dir) = sqlite().await;
    let sha = submitted(&db, "run-1").await;
    let app = router(AppState::new(
        db.clone(),
        &configured(dir.path(), true, Some(&key), &origin),
        Seams::default(),
    ));

    let refused = |body: Value, want: StatusCode| {
        let app = app.clone();
        async move {
            let (status, problem) = mint(&app, body.clone()).await;
            assert_eq!(status, want, "{body}: {problem}");
            problem["detail"].as_str().unwrap().to_string()
        }
    };
    let detail = refused(ask("run-0", REPOSITORY, &sha), StatusCode::NOT_FOUND).await;
    assert!(detail.contains("not one dagron-ci submitted"), "{detail}");
    for repository in [
        "https://github.com/acme/other.git",
        "https://github.com/acme/widgets",
        "https://ghe.example.com/acme/widgets.git",
        "http://github.com/acme/widgets.git",
    ] {
        let detail = refused(ask("run-1", repository, &sha), StatusCode::FORBIDDEN).await;
        assert!(detail.contains(REPOSITORY), "{detail}");
    }
    refused(
        ask("run-1", REPOSITORY, &"b".repeat(40)),
        StatusCode::FORBIDDEN,
    )
    .await;
    refused(
        json!({ "repository": REPOSITORY, "run_id": "run-1" }),
        StatusCode::BAD_REQUEST,
    )
    .await;
    assert_eq!(
        counted(&app, "refused").await,
        6,
        "a body that does not read is not counted"
    );
    assert!(
        gh.lock().unwrap().seen.is_empty(),
        "GitHub was asked nothing"
    );

    // A run that is over gets none: its attempts are, too.
    let (claimed, _) = db
        .claim_report("reporter", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed, "run-1");
    assert!(db
        .finish_report(
            "run-1",
            "reporter",
            &ReportWrite {
                status: Some("succeeded".into()),
                ..ReportWrite::default()
            },
        )
        .await
        .unwrap());
    let detail = refused(ask("run-1", REPOSITORY, &sha), StatusCode::CONFLICT).await;
    assert!(detail.contains("over (succeeded)"), "{detail}");
    assert!(
        gh.lock().unwrap().seen.is_empty(),
        "GitHub was asked nothing"
    );
}

#[tokio::test]
async fn a_run_gets_no_credential_once_its_repository_is_disconnected() {
    let dir = tempfile::tempdir().unwrap();
    let Some((key, _)) = rsa_key(dir.path()) else {
        return;
    };
    let (origin, gh) = stub_github().await;
    gh.lock().unwrap().installed = true;
    let (db, _db_dir) = sqlite().await;
    let sha = submitted(&db, "run-1").await;
    let app = router(AppState::new(
        db.clone(),
        &configured(dir.path(), true, Some(&key), &origin),
        Seams::default(),
    ));

    // The run is going still, and its repository is served no longer.
    let gone = db
        .disconnect_repo("github.com", "acme/widgets", "alice")
        .await
        .unwrap();
    assert!(gone.is_some());
    let (status, problem) = mint(&app, ask("run-1", REPOSITORY, &sha)).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{problem}");
    let detail = problem["detail"].as_str().unwrap();
    assert!(
        detail.contains("github.com/acme/widgets, was disconnected"),
        "{detail}"
    );
    assert!(
        gh.lock().unwrap().seen.is_empty(),
        "GitHub was asked nothing"
    );

    // Connected again, the run is its repository's as it was.
    db.connect_repo(
        "github",
        "github.com",
        "acme/widgets",
        None,
        "default",
        None,
        crate::db::Mode::Compat,
        "alice",
    )
    .await
    .unwrap();
    let (status, credential) = mint(&app, ask("run-1", REPOSITORY, &sha)).await;
    assert_eq!(status, StatusCode::OK, "{credential}");
}

#[tokio::test]
async fn a_credential_is_an_installation_token_for_the_one_repository_revoked_after() {
    let dir = tempfile::tempdir().unwrap();
    let Some((key, public)) = rsa_key(dir.path()) else {
        return;
    };
    let (origin, gh) = stub_github().await;
    gh.lock().unwrap().installed = true;
    let (db, _db_dir) = sqlite().await;
    let sha = submitted(&db, "run-1").await;
    let app = router(AppState::new(
        db,
        &configured(dir.path(), true, Some(&key), &origin),
        Seams::default(),
    ));

    let (status, credential) = mint(&app, ask("run-1", REPOSITORY, &sha)).await;
    assert_eq!(status, StatusCode::OK, "{credential}");
    assert_eq!(
        credential,
        json!({ "username": "x-access-token", "password": MINTED, "expires_at": EXPIRES })
    );
    {
        let seen = std::mem::take(&mut gh.lock().unwrap().seen);
        let [(m1, p1, a1, _), (m2, p2, a2, b2)] = &seen[..] else {
            panic!("{seen:?}");
        };
        assert_eq!(
            (m1.as_str(), p1.as_str()),
            ("GET", "/repos/acme/widgets/installation")
        );
        assert_eq!(
            (m2.as_str(), p2.as_str()),
            ("POST", "/app/installations/42/access_tokens")
        );
        // Both as the App, with a token its key signed.
        for authorization in [a1, a2] {
            let jwt = authorization.strip_prefix("Bearer ").unwrap();
            assert_eq!(claims(jwt, &public)["iss"], APP_ID.to_string());
        }
        // For this repository alone, and only to read it.
        assert_eq!(
            serde_json::from_str::<Value>(b2).unwrap(),
            json!({ "repositories": ["widgets"], "permissions": { "contents": "read" } })
        );
    }
    assert_eq!(counted(&app, "minted").await, 1);

    // Revoked once the attempt is over, as itself.
    assert_eq!(revoke(&app, MINTED).await.0, StatusCode::NO_CONTENT);
    let seen = std::mem::take(&mut gh.lock().unwrap().seen);
    assert_eq!(
        seen,
        [(
            "DELETE".to_string(),
            "/installation/token".to_string(),
            format!("Bearer {MINTED}"),
            String::new()
        )]
    );
    assert_eq!(counted(&app, "revoked").await, 1);
    let (status, _) = call(&app, Method::DELETE, Some(SECRET), "{}".into()).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a revoke names the credential"
    );
}

/// What a token service was asked: its bearer, and the body.
type Asked = Arc<Mutex<Vec<(String, Value)>>>;

/// A service that holds an App's key for dagron-ci: it answers `acme`'s
/// repositories under [`BROKER_SECRET`] alone, and is installed on no
/// other owner's.
async fn stub_token_service() -> (String, Asked) {
    async fn handle(
        State(asked): State<Asked>,
        headers: HeaderMap,
        axum::Json(body): axum::Json<Value>,
    ) -> Response {
        let bearer = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        asked.lock().unwrap().push((bearer.clone(), body.clone()));
        if bearer != format!("Bearer {BROKER_SECRET}") {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        match body["repository"].as_str() {
            Some(r) if r.starts_with("acme/") => (
                StatusCode::CREATED,
                axum::Json(json!({ "token": MINTED, "expires_at": EXPIRES })),
            )
                .into_response(),
            _ => StatusCode::NOT_FOUND.into_response(),
        }
    }
    let asked: Asked = Arc::default();
    let app = Router::new()
        .route("/tokens", axum::routing::post(handle))
        .with_state(asked.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("{origin}/tokens"), asked)
}

const BROKER_SECRET: &str = "broker-s3cret";

/// dagron-ci configured with an App whose key its token service at `url`
/// holds, asked under `secret`.
fn brokered(dir: &Path, url: &str, secret: &str, github_api: &str) -> Config {
    let secret_file = dir.join("checkout-secret");
    std::fs::write(&secret_file, format!("{SECRET}\n")).unwrap();
    let broker_file = dir.join("broker-secret");
    std::fs::write(&broker_file, format!("{secret}\n")).unwrap();
    let path = |p: &Path| p.to_str().unwrap().to_string();
    Config::from_lookup(|name| match name {
        "CI_DATABASE_URL" => Some("sqlite://unused.db".into()),
        "CI_CHECKOUT_TOKEN_SECRET_FILE" => Some(path(&secret_file)),
        "CI_GITHUB_APP_ID" => Some(APP_ID.to_string()),
        "CI_GITHUB_APP_TOKEN_URL" => Some(url.into()),
        "CI_GITHUB_APP_TOKEN_SECRET_FILE" => Some(path(&broker_file)),
        "CI_GITHUB_API_URL" => Some(github_api.into()),
        _ => None,
    })
    .unwrap()
}

/// An App whose key a token service holds: the checkout's credential is
/// asked of the service, under dagron-ci's secret, for the one repository
/// and `contents: read`, and GitHub is asked nothing until the credential
/// is revoked, as itself. A repository the service has no installation for
/// takes none.
#[tokio::test]
async fn a_credential_is_asked_of_the_app_s_token_service_where_it_holds_the_key() {
    let dir = tempfile::tempdir().unwrap();
    let (origin, gh) = stub_github().await;
    let (url, asked) = stub_token_service().await;
    let (db, _db_dir) = sqlite().await;
    let sha = submitted(&db, "run-1").await;
    let app = router(AppState::new(
        db.clone(),
        &brokered(dir.path(), &url, BROKER_SECRET, &origin),
        Seams::default(),
    ));

    let (status, credential) = mint(&app, ask("run-1", REPOSITORY, &sha)).await;
    assert_eq!(status, StatusCode::OK, "{credential}");
    assert_eq!(credential["password"], MINTED);
    assert_eq!(
        std::mem::take(&mut *asked.lock().unwrap()),
        [(
            format!("Bearer {BROKER_SECRET}"),
            json!({
                "repository": "acme/widgets",
                "permissions": { "contents": "read" },
                "one_repository": true,
            })
        )]
    );
    assert!(
        gh.lock().unwrap().seen.is_empty(),
        "GitHub was asked nothing"
    );
    assert_eq!(revoke(&app, MINTED).await.0, StatusCode::NO_CONTENT);
    let seen = std::mem::take(&mut gh.lock().unwrap().seen);
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert_eq!(
        (seen[0].1.as_str(), seen[0].2.as_str()),
        ("/installation/token", "Bearer ghs_for_one_attempt")
    );

    // Another owner's repository, which the service has no installation
    // for: no credential, as for an App not installed on it.
    let other = db
        .add_repo("github", "github.com", "elsewhere/widgets", None, "default")
        .await
        .unwrap()
        .unwrap();
    db.record_delivery(&other, "d-other", &event(), b"{}")
        .await
        .unwrap();
    let ev = db
        .claim_event("w", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    assert!(db
        .record_run(&SubmittedRun {
            run_id: "run-other".into(),
            event_id: ev.id,
            repo_id: other.id,
            cell: "default".into(),
            pipeline: "build".into(),
            idempotency_key: "ci-run-other".into(),
            on_failed: false,
        })
        .await
        .unwrap());
    let (status, body) = mint(
        &app,
        ask(
            "run-other",
            "https://github.com/elsewhere/widgets.git",
            &ev.sha,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    assert_eq!(asked.lock().unwrap().len(), 1, "the service was asked");
    assert!(
        gh.lock().unwrap().seen.is_empty(),
        "GitHub was asked nothing"
    );
}

/// A token service that does not take dagron-ci's secret has refused: the
/// job is told why, and is not handed a credential.
#[tokio::test]
async fn a_token_service_that_refuses_dagron_ci_s_secret_hands_over_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (origin, _gh) = stub_github().await;
    let (url, asked) = stub_token_service().await;
    let (db, _db_dir) = sqlite().await;
    let sha = submitted(&db, "run-1").await;
    let app = router(AppState::new(
        db,
        &brokered(dir.path(), &url, "not-the-secret", &origin),
        Seams::default(),
    ));
    let (status, body) = mint(&app, ask("run-1", REPOSITORY, &sha)).await;
    assert!(!status.is_success(), "{status}: {body}");
    assert!(body.to_string().contains("token service"), "{body}");
    assert_eq!(asked.lock().unwrap().len(), 1);
}

/// An action's fetch is given a credential for its own repository, to read
/// it alone, where the run is trusted and the action is of the run's
/// repository's owner's, on its forge; its row says which, and at what
/// ref. Another owner's, a repository written otherwise, one that asks for
/// more, and a run that is not trusted are refused.
#[tokio::test]
async fn an_action_of_the_owner_s_is_given_a_credential_for_a_trusted_run_alone() {
    let dir = tempfile::tempdir().unwrap();
    let Some((key, _)) = rsa_key(dir.path()) else {
        return;
    };
    let (origin, gh) = stub_github().await;
    gh.lock().unwrap().installed = true;
    gh.lock().unwrap().access = Some((true, "organization"));
    let (db, _db_dir) = sqlite().await;
    submitted(&db, "run-1").await;
    let app = router(AppState::new(
        db.clone(),
        &configured(dir.path(), true, Some(&key), &origin),
        Seams::default(),
    ));
    let action = |run: &str, repository: &str| {
        let mut body = ask(run, repository, "v1");
        body["action"] = json!(true);
        body
    };
    let (status, said) = mint(&app, action("run-1", "https://github.com/acme/tool.git")).await;
    assert_eq!(status, StatusCode::OK, "{said}");
    assert_eq!(said["password"], MINTED);
    let seen = std::mem::take(&mut gh.lock().unwrap().seen);
    let paths: Vec<&str> = seen.iter().map(|s| s.1.as_str()).collect();
    assert_eq!(
        paths,
        [
            "/repos/acme/tool/installation",
            "/app/installations/42/access_tokens",
            "/repos/acme/tool",
            "/repos/acme/tool/actions/permissions/access",
            "/installation/token",
            "/repos/acme/tool/installation",
            "/app/installations/42/access_tokens",
        ],
        "whether it shares its actions is asked first, with a token that reads that alone"
    );
    assert_eq!(
        serde_json::from_str::<Value>(&seen[1].3).unwrap(),
        json!({ "repositories": ["tool"], "permissions": { "metadata": "read", "administration": "read" } })
    );
    assert_eq!(
        serde_json::from_str::<Value>(&seen[6].3).unwrap(),
        json!({ "repositories": ["tool"], "permissions": { "contents": "read" } })
    );
    let rows = db.credentials_of_run("run-1").await.unwrap();
    assert_eq!(rows[0].repository, "https://github.com/acme/tool.git");
    assert_eq!(rows[0].commit, "v1");

    // A private repository that shares none of its actions, as GitHub's
    // default is, is refused; a public one takes no credential; one whose
    // setting cannot be had is taken for one that shares none.
    gh.lock().unwrap().access = Some((true, "none"));
    let (status, said) = mint(&app, action("run-1", "https://github.com/acme/tool.git")).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{said}");
    assert!(
        said.to_string().contains("does not share its actions"),
        "{said}"
    );
    gh.lock().unwrap().access = Some((false, "none"));
    let (status, _) = mint(&app, action("run-1", "https://github.com/acme/tool.git")).await;
    assert_eq!(
        status,
        StatusCode::NO_CONTENT,
        "a public action is fetched with nothing"
    );
    gh.lock().unwrap().access = None;
    let (status, _) = mint(&app, action("run-1", "https://github.com/acme/tool.git")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    gh.lock().unwrap().access = Some((true, "organization"));

    for repository in [
        "https://github.com/octo/hello.git",
        "https://github.com/acme/tool",
        "https://mirror.example/acme/tool.git",
        "https://github.com/acme/a/b.git",
        "https://github.com/acme/...git",
    ] {
        let (status, said) = mint(&app, action("run-1", repository)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{repository}: {said}");
    }
    let mut asking = action("run-1", "https://github.com/acme/tool.git");
    asking["permissions"] = json!({"contents": "read"});
    assert_eq!(mint(&app, asking).await.0, StatusCode::FORBIDDEN);

    let (theirs, _theirs_dir) = sqlite().await;
    submitted_as(&theirs, "run-2", Trust::Untrusted).await;
    let app = router(AppState::new(
        theirs,
        &configured(dir.path(), true, Some(&key), &origin),
        Seams::default(),
    ));
    let (status, said) = mint(&app, action("run-2", "https://github.com/acme/tool.git")).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{said}");
    assert!(said.to_string().contains("trusted run alone"), "{said}");
}

/// Each minted credential has a row, written before it is handed over, which
/// names what it was for and does not hold it; and its revoke marks the row.
/// A job's forge token is an installation token with the permissions it
/// asked for, `metadata: read` beside them, and its row says what it was
/// given. A run that is not trusted is given read alone; a permission that
/// is not one is refused.
#[tokio::test]
async fn a_job_s_forge_token_has_what_it_asked_for_as_its_run_s_trust_allows() {
    let dir = tempfile::tempdir().unwrap();
    let Some((key, _)) = rsa_key(dir.path()) else {
        return;
    };
    let (origin, gh) = stub_github().await;
    gh.lock().unwrap().installed = true;
    let (db, _db_dir) = sqlite().await;
    let sha = submitted(&db, "run-1").await;
    let app = router(AppState::new(
        db.clone(),
        &configured(dir.path(), true, Some(&key), &origin),
        Seams::default(),
    ));
    let asking = |permissions: Value| {
        let mut body = ask("run-1", REPOSITORY, &sha);
        body["permissions"] = permissions;
        body
    };
    let (status, said) = mint(
        &app,
        asking(json!({"contents": "write", "pull_requests": "read"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{said}");
    let seen = std::mem::take(&mut gh.lock().unwrap().seen);
    assert_eq!(
        serde_json::from_str::<Value>(&seen[1].3).unwrap(),
        json!({
            "repositories": ["widgets"],
            "permissions": {"contents": "write", "metadata": "read", "pull_requests": "read"},
        })
    );
    let rows = db.credentials_of_run("run-1").await.unwrap();
    assert_eq!(
        rows[0].permissions.as_deref(),
        Some(r#"{"contents":"write","metadata":"read","pull_requests":"read"}"#)
    );

    for (asked, why) in [
        (json!({"administration": "write"}), "may not be asked for"),
        (json!({"contents": "admin"}), "read or write"),
        (json!({}), "asks for no permissions"),
    ] {
        let (status, said) = mint(&app, asking(asked.clone())).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{asked}");
        assert!(said.to_string().contains(why), "{asked}: {said}");
    }

    // By trust: what is asked to write, a run that is not trusted reads.
    let asked = BTreeMap::from([
        ("contents".to_string(), "write".to_string()),
        ("statuses".to_string(), "write".to_string()),
    ]);
    assert_eq!(
        forge_permissions(&asked, "untrusted").unwrap(),
        BTreeMap::from([
            ("contents".to_string(), "read".to_string()),
            ("metadata".to_string(), "read".to_string()),
            ("statuses".to_string(), "read".to_string()),
        ])
    );
    assert_eq!(
        forge_permissions(&asked, "trusted").unwrap()["statuses"],
        "write"
    );
    assert_eq!(
        FORGE_TOKEN_PERMISSIONS,
        dagron_core::forge_token::PERMISSIONS,
        "the names dagron takes are the names dagron-ci gives"
    );
}

#[tokio::test]
async fn a_minted_credential_has_a_row_and_its_revoke_marks_it() {
    let dir = tempfile::tempdir().unwrap();
    let Some((key, _)) = rsa_key(dir.path()) else {
        return;
    };
    let (origin, gh) = stub_github().await;
    gh.lock().unwrap().installed = true;
    let (db, _db_dir) = sqlite().await;
    let sha = submitted(&db, "run-1").await;
    let app = router(AppState::new(
        db.clone(),
        &configured(dir.path(), true, Some(&key), &origin),
        Seams::default(),
    ));
    let rows = || async { db.credentials_of_run("run-1").await.unwrap() };

    assert!(rows().await.is_empty());
    assert_eq!(
        mint(&app, ask("run-1", REPOSITORY, &sha)).await.0,
        StatusCode::OK
    );
    let row = CheckoutCredential {
        token_sha256: hex::encode(Sha256::digest(MINTED.as_bytes())),
        run_id: "run-1".into(),
        task_id: Some("build".into()),
        attempt: Some(1),
        repository: REPOSITORY.into(),
        commit: sha.clone(),
        cell: "default".into(),
        expires_at: Some(EXPIRES.into()),
        revoked: false,
        permissions: None,
        forge_token_id: None,
    };
    assert_eq!(rows().await, [row.clone()]);
    assert!(!format!("{row:?}").contains(MINTED), "never the credential");

    // A refusal mints nothing, and writes no row.
    let elsewhere = "https://github.com/acme/vault.git";
    assert_eq!(
        mint(&app, ask("run-1", elsewhere, &sha)).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(rows().await.len(), 1);

    // Revoked once the attempt is over: the row says so, and says it once.
    assert_eq!(revoke(&app, MINTED).await.0, StatusCode::NO_CONTENT);
    assert_eq!(
        rows().await,
        [CheckoutCredential {
            revoked: true,
            ..row.clone()
        }]
    );
    assert!(!db.credential_revoked(&row.token_sha256).await.unwrap());

    // An engine that names neither a task nor an attempt has its row too.
    let bare = json!({ "repository": REPOSITORY, "commit": sha, "run_id": "run-1" });
    assert_eq!(mint(&app, bare).await.0, StatusCode::OK);
    let now = rows().await;
    assert_eq!(now.len(), 2);
    assert_eq!(
        (now[1].task_id.as_deref(), now[1].attempt, now[1].revoked),
        (None, None, false)
    );
    // A credential with no row, one from before the table, is revoked all
    // the same, and marks nothing.
    assert_eq!(
        revoke(&app, "ghs_of_before").await.0,
        StatusCode::NO_CONTENT
    );
    assert!(!rows().await[1].revoked);
}

/// No row, no credential: a mint whose row cannot be written hands nothing
/// over, and what GitHub gave is revoked.
#[tokio::test]
async fn a_credential_that_cannot_be_recorded_is_not_handed_over() {
    let dir = tempfile::tempdir().unwrap();
    let Some((key, _)) = rsa_key(dir.path()) else {
        return;
    };
    let (origin, gh) = stub_github().await;
    gh.lock().unwrap().installed = true;
    let (db, _db_dir) = sqlite().await;
    let sha = submitted(&db, "run-1").await;
    let app = router(AppState::new(
        db.clone(),
        &configured(dir.path(), true, Some(&key), &origin),
        Seams::default(),
    ));
    let Db::Sqlite(pool) = &db else {
        unreachable!("sqlite() is SQLite");
    };
    sqlx::query("DROP TABLE ci_checkout_credentials")
        .execute(pool)
        .await
        .unwrap();

    let (status, problem) = mint(&app, ask("run-1", REPOSITORY, &sha)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{problem}");
    assert!(
        problem["detail"]
            .as_str()
            .unwrap()
            .starts_with("the credential could not be recorded, and is not handed over"),
        "{problem}"
    );
    assert!(!problem.to_string().contains(MINTED));
    assert_eq!(counted(&app, "minted").await, 0);
    assert_eq!(counted(&app, "failed").await, 1);
    // On a task of its own, so it is waited for.
    let revoked = || {
        gh.lock()
            .unwrap()
            .seen
            .iter()
            .any(|(method, path, as_whom, _)| {
                method == "DELETE"
                    && path == "/installation/token"
                    && as_whom == &format!("Bearer {MINTED}")
            })
    };
    for _ in 0..300 {
        if revoked() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(revoked(), "{:?}", gh.lock().unwrap().seen);
}

#[tokio::test]
async fn without_an_app_installed_on_github_com_the_checkout_takes_no_credential() {
    let dir = tempfile::tempdir().unwrap();
    let Some((key, _)) = rsa_key(dir.path()) else {
        return;
    };
    let (origin, gh) = stub_github().await;
    let (db, _db_dir) = sqlite().await;
    let sha = submitted(&db, "run-1").await;

    // No App: nothing is asked of GitHub, and nothing is revoked.
    let without = router(AppState::new(
        db.clone(),
        &configured(dir.path(), true, None, &origin),
        Seams::default(),
    ));
    let (status, body) = mint(&without, ask("run-1", REPOSITORY, &sha)).await;
    assert_eq!((status, body), (StatusCode::NO_CONTENT, Value::Null));
    assert_eq!(revoke(&without, "anything").await.0, StatusCode::NO_CONTENT);
    assert!(
        gh.lock().unwrap().seen.is_empty(),
        "GitHub was asked nothing"
    );
    assert_eq!(counted(&without, "anonymous").await, 1);

    // An App that is not installed on the repository.
    let app = router(AppState::new(
        db.clone(),
        &configured(dir.path(), true, Some(&key), &origin),
        Seams::default(),
    ));
    assert_eq!(
        mint(&app, ask("run-1", REPOSITORY, &sha)).await.0,
        StatusCode::NO_CONTENT
    );
    let seen = std::mem::take(&mut gh.lock().unwrap().seen);
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert_eq!(seen[0].1, "/repos/acme/widgets/installation");

    // A repository on another forge host: the App is github.com's.
    let other = db
        .add_repo("github", "ghe.example.com", "acme/widgets", None, "default")
        .await
        .unwrap()
        .unwrap();
    db.record_delivery(&other, "d-ghe", &event(), b"{}")
        .await
        .unwrap();
    let ev = db
        .claim_event("w", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ev.repo, other);
    assert!(db
        .record_run(&SubmittedRun {
            run_id: "run-ghe".into(),
            event_id: ev.id,
            repo_id: other.id,
            cell: "default".into(),
            pipeline: "build".into(),
            idempotency_key: "ci-run-ghe".into(),
            on_failed: false,
        })
        .await
        .unwrap());
    let (status, body) = mint(
        &app,
        ask(
            "run-ghe",
            "https://ghe.example.com/acme/widgets.git",
            &ev.sha,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    assert!(
        gh.lock().unwrap().seen.is_empty(),
        "GitHub was asked nothing"
    );
    assert_eq!(counted(&app, "anonymous").await, 2);
}

/// dagron-api creates a run, and its engine may start the run's first task,
/// before dagron-ci has the answer to record. The engine then asks for a run
/// that is not on record yet. It used to be told at once that the run was
/// not dagron-ci's, which failed the job; the run is looked for again.
#[tokio::test]
async fn a_run_asked_for_a_moment_before_it_is_recorded_is_answered() {
    let dir = tempfile::tempdir().unwrap();
    let (db, _db_dir) = sqlite().await;
    let (repo, event) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
    let app = router(AppState::new(
        db.clone(),
        &configured(dir.path(), true, None, "https://api.github.com"),
        Seams::default(),
    ));

    let recording = recorded_after(&db, &repo, &event, "run-late", Duration::from_millis(200));
    let (status, body) = mint(&app, ask("run-late", REPOSITORY, &event.sha)).await;
    recording.await.unwrap();
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    assert_eq!(counted(&app, "refused").await, 0);
    assert_eq!(counted(&app, "anonymous").await, 1);
}

/// A run that never comes on record is refused as it was, once it has been
/// looked for: and that look is out of the answer's budget, not on top of it.
#[tokio::test]
async fn a_run_that_is_never_recorded_is_refused_once_it_has_been_looked_for() {
    let dir = tempfile::tempdir().unwrap();
    let refused_after = |checkouts: Checkouts| {
        let config = configured(dir.path(), true, None, "https://api.github.com");
        async move {
            let (db, _db_dir) = sqlite().await;
            // acme/widgets is connected: a run of it that is not on record
            // is one dagron-ci should have submitted, and did not.
            claimed(&db, None, Kind::Push, Trust::Trusted).await;
            let mut state = AppState::new(db, &config, Seams::default());
            state.checkouts = Some(Arc::new(checkouts));
            let app = router(state);
            let started = std::time::Instant::now();
            let (status, problem) = mint(&app, ask("run-0", REPOSITORY, &"a".repeat(40))).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{problem}");
            assert!(
                problem["detail"]
                    .as_str()
                    .unwrap()
                    .contains("not one dagron-ci submitted"),
                "{problem}"
            );
            assert_eq!(counted(&app, "refused").await, 1);
            started.elapsed()
        }
    };
    let endpoint = || Checkouts::new(Some(SECRET.into()), None, None);

    let took = refused_after(endpoint().with_record_wait(Duration::from_millis(300))).await;
    assert!(took >= Duration::from_millis(300), "at once: {took:?}");
    assert!(took < RECORD_WAIT, "{took:?}");

    // With less time to answer in than to look for, it is refused inside
    // that time: the last look it could be answered is the last it makes.
    let took = refused_after(endpoint().with_budget(Duration::from_secs(1))).await;
    assert!(took >= Duration::from_millis(100), "at once: {took:?}");
    assert!(took < RECORD_WAIT, "past the budget: {took:?}");
}

/// An engine that names dagron-ci as its token service asks it about every
/// checkout, a workflow's that is not CI's included. One of a repository
/// dagron-ci does not serve is told there is no credential (204), as an engine
/// with no token service has none, and its commit is fetched without one. It
/// used to be refused (404), which failed the task: naming dagron-ci made
/// every checkout on the engine CI's. One of a repository dagron-ci serves is
/// still refused, however its URL is written.
#[tokio::test]
async fn a_run_that_is_not_ci_s_takes_no_credential_unless_its_repository_is_served() {
    let dir = tempfile::tempdir().unwrap();
    let config = configured(dir.path(), true, None, "https://api.github.com");
    let (db, _db_dir) = sqlite().await;
    claimed(&db, None, Kind::Push, Trust::Trusted).await; // connects acme/widgets
    let mut state = AppState::new(db, &config, Seams::default());
    state.checkouts = Some(Arc::new(
        Checkouts::new(Some(SECRET.into()), None, None)
            .with_record_wait(Duration::from_millis(200)),
    ));
    let app = router(state);
    let sha = "b".repeat(40);

    // Not dagron-ci's repository: no credential, once the run was looked for.
    let started = std::time::Instant::now();
    for repository in [
        "https://github.com/acme/tools.git",
        "https://gitlab.example.com/acme/widgets.git",
        "git@github.com:acme/tools.git",
        "not a url",
    ] {
        let (status, body) = mint(&app, ask("run-0", repository, &sha)).await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{repository}: {body}");
    }
    assert!(
        started.elapsed() >= Duration::from_millis(200),
        "it was looked for first"
    );
    assert_eq!(counted(&app, "anonymous").await, 4);
    assert_eq!(counted(&app, "refused").await, 0);

    // dagron-ci's repository, under every name it goes by: still refused.
    for repository in [
        REPOSITORY,
        "https://github.com/acme/widgets",
        "https://github.com/Acme/Widgets.git/",
        "https://x-access-token@github.com/acme/widgets.git",
        "git@github.com:acme/widgets.git",
        "ssh://git@github.com/acme/widgets",
        "https://github.com:443/acme/widgets.git",
        "https://github.com./acme/widgets.git",
    ] {
        let (status, problem) = mint(&app, ask("run-0", repository, &sha)).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{repository}: {problem}");
        let detail = problem["detail"].as_str().unwrap();
        assert!(detail.contains("is a repository it serves"), "{detail}");
    }
    assert_eq!(counted(&app, "refused").await, 8);
    assert_eq!(
        counted(&app, "minted").await,
        0,
        "and neither answer hands anything out"
    );
}

/// What a checkout's URL names, as a connected repository is kept.
#[test]
fn a_repository_url_names_a_host_and_a_path() {
    let name = |url: &str| repository_name(url).map(|(h, p)| format!("{h} {p}"));
    for url in [
        "https://github.com/acme/widgets.git",
        "https://github.com/acme/widgets",
        "https://github.com/acme/widgets.git/",
        "http://github.com/acme/widgets.git?x=1#frag",
        "https://user:secret@github.com/acme/widgets.git",
        "ssh://git@github.com/acme/widgets.git",
        "git@github.com:acme/widgets.git",
        "  https://github.com/acme/widgets.git  ",
        // A scheme's own port, and a dot after the host, name no other host.
        "https://github.com:443/acme/widgets.git",
        "HTTPS://github.com:443/acme/widgets",
        "http://github.com:80/acme/widgets.git",
        "ssh://git@github.com:22/acme/widgets.git",
        "https://github.com./acme/widgets.git",
        "https://github.com.:443/acme/widgets.git",
    ] {
        assert_eq!(
            name(url).as_deref(),
            Some("github.com acme/widgets"),
            "{url}"
        );
    }
    assert_eq!(
        name("https://git.example.com:8443/team/sub/repo.git").as_deref(),
        Some("git.example.com:8443 team/sub/repo")
    );
    // Another scheme's port is not this one's own.
    assert_eq!(
        name("http://github.com:443/acme/widgets.git").as_deref(),
        Some("github.com:443 acme/widgets")
    );
    for url in [
        "",
        "widgets",
        "https://github.com",
        "https://github.com/",
        "https:///acme/widgets.git",
    ] {
        assert_eq!(name(url), None, "{url:?}");
    }
}

/// A database that does not answer has not said the run is not there. The
/// look ends when the answer is due, not when the looking would have, and
/// says which it was.
#[tokio::test]
async fn a_database_that_does_not_answer_is_not_a_run_that_is_not_there() {
    let now = tokio::time::Instant::now();
    let started = std::time::Instant::now();
    let read = recorded(
        std::future::pending::<sqlx::Result<Option<RunCommit>>>,
        "run-1",
        now + Duration::from_secs(5),
        now + Duration::from_millis(200),
    )
    .await;
    let why = read.expect_err("no answer is no word on the run");
    assert!(why.contains("the database did not answer within "), "{why}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );

    // One that answers with an error is that error, at once.
    let read = recorded(
        || async { Err::<Option<RunCommit>, _>(sqlx::Error::PoolTimedOut) },
        "run-1",
        now + Duration::from_secs(5),
        tokio::time::Instant::now() + Duration::from_secs(5),
    )
    .await;
    let why = read.expect_err("an error is no word on the run either");
    assert!(why.starts_with("the database could not be read: "), "{why}");
}

/// What looking for the run took, the mint does not have: GitHub is given
/// what is left of the budget, so the engine is still answered in time.
#[tokio::test]
async fn a_late_run_s_mint_has_what_is_left_of_the_budget() {
    let dir = tempfile::tempdir().unwrap();
    let Some((key, _)) = rsa_key(dir.path()) else {
        return;
    };
    let (origin, gh) = stub_github().await;
    {
        let mut gh = gh.lock().unwrap();
        gh.installed = true;
        gh.delay = Some(Duration::from_secs(5));
    }
    let (db, _db_dir) = sqlite().await;
    let (repo, event) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
    let mut state = AppState::new(
        db.clone(),
        &configured(dir.path(), true, Some(&key), &origin),
        Seams::default(),
    );
    state.checkouts = Some(Arc::new(
        Checkouts::new(
            Some(SECRET.into()),
            Some(GitHubApp::new(APP_ID, &key).unwrap()),
            Some(origin),
        )
        .with_budget(Duration::from_secs(1)),
    ));
    let app = router(state);

    let recording = recorded_after(&db, &repo, &event, "run-late", Duration::from_millis(400));
    let started = std::time::Instant::now();
    let (status, problem) = mint(&app, ask("run-late", REPOSITORY, &event.sha)).await;
    recording.await.unwrap();
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{problem}");
    // Less than the whole second: the look took its share.
    assert!(
        problem["detail"]
            .as_str()
            .unwrap()
            .contains("GitHub did not answer within 0."),
        "{problem}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(counted(&app, "failed").await, 1);
}

#[tokio::test]
async fn a_mint_github_fails_is_a_bad_gateway() {
    let dir = tempfile::tempdir().unwrap();
    let Some((key, _)) = rsa_key(dir.path()) else {
        return;
    };
    let (origin, gh) = stub_github().await;
    {
        let mut gh = gh.lock().unwrap();
        gh.installed = true;
        gh.mint_answer = Some(422);
    }
    let (db, _db_dir) = sqlite().await;
    let sha = submitted(&db, "run-1").await;
    let app = router(AppState::new(
        db,
        &configured(dir.path(), true, Some(&key), &origin),
        Seams::default(),
    ));
    let (status, problem) = mint(&app, ask("run-1", REPOSITORY, &sha)).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{problem}");
    assert!(
        problem["detail"].as_str().unwrap().contains("422"),
        "{problem}"
    );
    assert_eq!(counted(&app, "failed").await, 1);
    assert_eq!(counted(&app, "minted").await, 0);
}

#[tokio::test]
async fn a_github_slower_than_the_mint_s_budget_is_a_bad_gateway_in_time() {
    let dir = tempfile::tempdir().unwrap();
    let Some((key, _)) = rsa_key(dir.path()) else {
        return;
    };
    let (origin, gh) = stub_github().await;
    {
        let mut gh = gh.lock().unwrap();
        gh.installed = true;
        gh.delay = Some(Duration::from_secs(5));
    }
    let (db, _db_dir) = sqlite().await;
    let sha = submitted(&db, "run-1").await;
    let mut state = AppState::new(
        db,
        &configured(dir.path(), true, Some(&key), &origin),
        Seams::default(),
    );
    state.checkouts = Some(Arc::new(
        Checkouts::new(
            Some(SECRET.into()),
            Some(GitHubApp::new(APP_ID, &key).unwrap()),
            Some(origin),
        )
        .with_budget(Duration::from_millis(300)),
    ));
    let app = router(state);

    let started = std::time::Instant::now();
    let (status, problem) = mint(&app, ask("run-1", REPOSITORY, &sha)).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{problem}");
    // What was left of the budget once the run had been read: 0.3 s, or
    // under it by as long as the read took.
    let given: f32 = problem["detail"]
        .as_str()
        .unwrap()
        .split("GitHub did not answer within ")
        .nth(1)
        .and_then(|rest| rest.split(' ').next()?.parse().ok())
        .unwrap_or_else(|| panic!("{problem}"));
    assert!(given <= 0.3, "{problem}");
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(counted(&app, "failed").await, 1);
}

/// A run dagron-ci submitted for a push of a GitLab project, `group/app` on
/// `host`: its id, and the commit.
async fn submitted_on_gitlab(db: &Db, host: &str, run_id: &str) -> String {
    let repo = db
        .add_repo("gitlab", host, "group/app", None, "default")
        .await
        .unwrap()
        .unwrap();
    db.record_delivery(&repo, &format!("d-{run_id}"), &event(), b"{}")
        .await
        .unwrap();
    let ev = db
        .claim_event("w", Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    assert!(db
        .record_run(&SubmittedRun {
            run_id: run_id.into(),
            event_id: ev.id,
            repo_id: repo.id,
            cell: "default".into(),
            pipeline: "build".into(),
            idempotency_key: format!("ci-{run_id}"),
            on_failed: false,
        })
        .await
        .unwrap());
    ev.sha
}

/// A GitLab project's checkout is a project access token of its own, minted
/// with the host's token, which never leaves dagron-ci: Reporter, to read the
/// repository, and revoked by its id when the attempt is over. A host
/// without a token, a token that may not mint, an action and a job's forge
/// token take none.
#[tokio::test]
async fn a_gitlab_checkout_is_a_project_token_of_its_own_revoked_after() {
    use crate::forge::gitlab::tests::stub_gitlab;
    let dir = tempfile::tempdir().unwrap();
    let (origin, gl) = stub_gitlab().await;
    let (db, _db_dir) = sqlite().await;
    let sha = submitted_on_gitlab(&db, "gitlab.example", "run-gl").await;
    let tokens = BTreeMap::from([("gitlab.example".to_string(), "glpat-host".to_string())]);
    let mut state = AppState::new(
        db.clone(),
        &configured(dir.path(), true, None, &origin),
        Seams::default(),
    );
    state.checkouts = Some(Arc::new(
        Checkouts::new(Some(SECRET.into()), None, None)
            .with_gitlab_tokens(&tokens)
            .with_gitlab_origin(&origin),
    ));
    let app = router(state);
    let at = "/projects/group%2Fapp/access_tokens";
    gl.lock().unwrap().answers.insert(
        at.into(),
        (
            201,
            Vec::new(),
            json!({"id": 77, "token": "glpat-checkout", "expires_at": "2026-10-20"}).to_string(),
        ),
    );
    gl.lock()
        .unwrap()
        .answers
        .insert(format!("{at}/77"), (204, Vec::new(), String::new()));
    let repository = "https://gitlab.example/group/app.git";

    let (status, body) = mint(&app, ask("run-gl", repository, &sha)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["username"], "oauth2");
    assert_eq!(body["password"], "glpat-checkout");
    let seen = std::mem::take(&mut gl.lock().unwrap().seen);
    assert_eq!(seen.len(), 1, "{seen:?}");
    let (method, path, auth, asked) = &seen[0];
    assert_eq!(
        (method.as_str(), path.as_str(), auth.as_str()),
        ("POST", at, "Bearer glpat-host")
    );
    let asked: Value = serde_json::from_str(asked).unwrap();
    assert_eq!(asked["scopes"], json!(["read_repository"]));
    assert_eq!(asked["access_level"], 20);
    let rows = db.credentials_of_run("run-gl").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].forge_token_id.as_deref(), Some("77"));
    assert_eq!(rows[0].expires_at.as_deref(), Some("2026-10-20"));

    // Revoked by its id, with the host's token.
    assert_eq!(
        revoke(&app, "glpat-checkout").await.0,
        StatusCode::NO_CONTENT
    );
    let seen = std::mem::take(&mut gl.lock().unwrap().seen);
    assert_eq!(seen.len(), 1, "{seen:?}");
    let target = format!("{at}/77");
    assert_eq!(
        (seen[0].0.as_str(), seen[0].1.as_str(), seen[0].2.as_str()),
        ("DELETE", target.as_str(), "Bearer glpat-host")
    );
    assert!(db.credentials_of_run("run-gl").await.unwrap()[0].revoked);
    // Revoked again: nothing is asked of either forge.
    assert_eq!(
        revoke(&app, "glpat-checkout").await.0,
        StatusCode::NO_CONTENT
    );
    assert!(
        gl.lock().unwrap().seen.is_empty(),
        "GitLab was asked nothing"
    );

    // A dagron-ci started again without the host's token: a live GitLab
    // token is not revoked as GitHub's, and the answer says it was not.
    gl.lock().unwrap().answers.insert(
        at.into(),
        (
            201,
            Vec::new(),
            json!({"id": 78, "token": "glpat-second", "expires_at": "2026-10-20"}).to_string(),
        ),
    );
    assert_eq!(
        mint(&app, ask("run-gl", repository, &sha)).await.0,
        StatusCode::OK
    );
    std::mem::take(&mut gl.lock().unwrap().seen);
    let mut restarted = AppState::new(
        db.clone(),
        &configured(dir.path(), true, None, &origin),
        Seams::default(),
    );
    restarted.checkouts = Some(Arc::new(
        Checkouts::new(Some(SECRET.into()), None, None).with_gitlab_origin(&origin),
    ));
    let restarted = router(restarted);
    assert_eq!(
        revoke(&restarted, "glpat-second").await.0,
        StatusCode::BAD_GATEWAY
    );
    assert!(
        gl.lock().unwrap().seen.is_empty(),
        "nothing was sent anywhere"
    );
    assert_eq!(revoke(&app, "glpat-second").await.0, StatusCode::NO_CONTENT);
    std::mem::take(&mut gl.lock().unwrap().seen);
    gl.lock().unwrap().answers.insert(
        at.into(),
        (
            201,
            Vec::new(),
            json!({"id": 77, "token": "glpat-checkout", "expires_at": "2026-10-20"}).to_string(),
        ),
    );

    // An action's credential and a job's forge token are a GitHub App's.
    let mut action = ask("run-gl", repository, &sha);
    action["permissions"] = json!({"contents": "read"});
    assert_eq!(mint(&app, action).await.0, StatusCode::NO_CONTENT);
    assert!(
        gl.lock().unwrap().seen.is_empty(),
        "GitLab was asked nothing"
    );

    // A host's token that may not mint here: no credential, and the job's
    // fetch is a public project's.
    gl.lock()
        .unwrap()
        .answers
        .insert(at.into(), (403, Vec::new(), "{}".into()));
    assert_eq!(
        mint(&app, ask("run-gl", repository, &sha)).await.0,
        StatusCode::NO_CONTENT
    );

    // A host CI_GITLAB_TOKENS_DIR keeps no token for: nothing is asked.
    std::mem::take(&mut gl.lock().unwrap().seen);
    let other = submitted_on_gitlab(&db, "gitlab.other", "run-other").await;
    assert_eq!(
        mint(
            &app,
            ask("run-other", "https://gitlab.other/group/app.git", &other)
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );
    assert!(
        gl.lock().unwrap().seen.is_empty(),
        "GitLab was asked nothing"
    );
}
