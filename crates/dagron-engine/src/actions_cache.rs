//! **The cache service a workflow's job's `actions/cache` speaks**: GitHub's
//! cache service, version 2, served by the engine to its own jobs, over the
//! path caches' index and store ([`crate::path_cache`]).
//!
//! As it dispatches a task with a workspace, of a run with a repository, the
//! engine gives it what `@actions/cache` reads: `ACTIONS_CACHE_SERVICE_V2`,
//! `ACTIONS_RESULTS_URL` (this service, as the jobs reach it,
//! `DAGRON_ACTIONS_CACHE_URL`) and `ACTIONS_RUNTIME_TOKEN`, a credential of
//! the attempt's own, signed by this engine, that names the run's
//! repository, the scopes it reads and whether it may write.
//!
//! - **Restore** (`GetCacheEntryDownloadURL`): the entry of the key, else the
//!   newest whose key starts with each restore key in turn, in the run's own
//!   ref, then the ref a pull request merges into. The answer is a URL the
//!   store signed: the bytes go from the store to the job, and never through
//!   the engine.
//! - **Save** (`CreateCacheEntry`, then the upload, then
//!   `FinalizeCacheEntryUpload`): a run that is not trusted saves nothing,
//!   as a path cache. The action uploads with Azure's Blob SDK, which no
//!   other store's signed URL takes, so the upload is to this service: the
//!   part of Azure Blob's API the SDK uses (a blob put whole, or in blocks
//!   and their list), written to a directory of the engine's own
//!   (`DAGRON_ACTIONS_CACHE_DIR`), and put in the store as it is finalized.
//!   The entry is then ready, in the place of any of the same key.
//!
//! The same listener is GitHub's **artifact service**, which
//! `actions/upload-artifact` and `actions/download-artifact` v4 speak
//! ([`artifacts`]): a run's artifacts, by name, in the same store.
//!
//! An upload is kept in this engine's directory until it is finalized, and
//! its credential is signed with a key of this process's, so the URL jobs
//! reach is this engine's own: one engine of several has its own URL.
//! Entries are the path caches' (prefix [`PREFIX`]), and their quota, their
//! eviction and their sweep are the path caches'.
//!
//! The credential is a JWT (HS256), as GitHub's is: `@actions/artifact`
//! reads the run and the job it is for from its `scp` claim
//! (`Actions.Results:<run>:<job>`), and checks no signature. This service
//! checks it.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{post, put};
use axum::{Json, Router};
use dagron_artifact::Transfer;
use dagron_core::db;
use dagron_core::models::{NewPathCache, RunTrigger};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::path_cache::{self, PathCaches};

mod artifacts;
mod provenance;
mod steps;
pub use artifacts::{
    artifact_settings, ArtifactSettings, ENV_MAX_BYTES as ENV_ARTIFACT_MAX_BYTES,
    ENV_RETENTION_DAYS as ENV_ARTIFACT_RETENTION_DAYS,
};
pub use provenance::Provenance;

pub const ENV_LISTEN: &str = "DAGRON_ACTIONS_CACHE_LISTEN";
pub const ENV_URL: &str = "DAGRON_ACTIONS_CACHE_URL";
pub const ENV_INSECURE: &str = "DAGRON_ACTIONS_CACHE_INSECURE";
pub const ENV_DIR: &str = "DAGRON_ACTIONS_CACHE_DIR";
/// A task's own variable: the ref its restores read after its own, the
/// branch a pull request merges into (dagron-ci writes it, as it writes a
/// path cache's `restore_from`).
pub const RESTORE_FROM: &str = "DAGRON_ACTIONS_CACHE_RESTORE_FROM";

/// The path caches' prefix an action's entries are kept under.
pub const PREFIX: &str = "actions";
/// The twirp services the actions call.
const CACHE_SERVICE: &str = "github.actions.results.api.v1.CacheService";
const ARTIFACT_SERVICE: &str = "github.actions.results.api.v1.ArtifactService";
/// The longest a key or a version is.
const MAX_KEY: usize = 512;
/// The most restore keys a restore is asked with.
const MAX_RESTORE_KEYS: usize = 10;
/// How many of a scope's newest entries a restore looks through.
const LOOK_THROUGH: i64 = 200;
/// The most uploads one attempt has open at once.
const MAX_OPEN_PER_TASK: usize = 32;
/// A block list's body is ids alone.
const MAX_BLOCK_LIST: usize = 1 << 20;
/// An upload not finalized in this long is dropped.
const OPEN_FOR_AT_MOST: Duration = Duration::from_secs(24 * 3600);
/// How long a download URL the store signs is good for.
const DOWNLOAD_SECS: u64 = 3600;

/// What a job's credential says: its task, its run, its run's repository,
/// the cache scopes it reads in order, whether it may save a cache, and
/// when it lapses; and GitHub's `scp`, which names the run and the task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Claims {
    t: String,
    u: String,
    r: String,
    s: Vec<String>,
    w: bool,
    exp: i64,
    scp: String,
}

/// An upload a job has reserved and not yet finalized: its id, its task, when
/// it was reserved, the most it may hold, and what it is of.
#[derive(Debug, Clone)]
struct Open {
    id: String,
    task: String,
    at: Instant,
    max: u64,
    what: What,
}

#[derive(Debug, Clone)]
enum What {
    /// A cache entry, in `scope`, under its key and version.
    Cache {
        scope: String,
        key: String,
        version: String,
        object_key: String,
    },
    /// An artifact of `run`, by its name.
    Artifact {
        run: String,
        name: String,
        artifact_id: i64,
        object_key: String,
        expires_at: String,
    },
}

/// Why a call is answered with a twirp error, not its message.
#[derive(Debug)]
enum Refusal {
    /// A status, a twirp code and what the job is told.
    Said(StatusCode, &'static str, String),
    /// The engine failed: logged, and the job told only that.
    Failed(anyhow::Error),
}

impl From<anyhow::Error> for Refusal {
    fn from(e: anyhow::Error) -> Self {
        Refusal::Failed(e)
    }
}

type Answer = std::result::Result<Value, Refusal>;

/// The engine's cache service.
pub struct ActionsCache {
    caches: Arc<PathCaches>,
    pool: db::Pool,
    url: String,
    dir: PathBuf,
    secret: [u8; 32],
    open: Mutex<HashMap<String, Open>>,
    artifacts: ArtifactSettings,
    /// What runs a job's step in an image of its own ([`steps`]), and the
    /// images a run that is not trusted may name there.
    executor: Option<Arc<dyn dagron_executor::executor::Executor>>,
    images: Vec<String>,
    steps: steps::Runs,
    /// The key each artifact's provenance is signed with, where the
    /// operator gave one.
    provenance: Option<Provenance>,
}

/// The two settings that turn the service on, and whether plain http may
/// carry its credential, as [`crate::oidc_request`] reads its own.
pub fn settings(
    listen: Option<&str>,
    url: Option<&str>,
    insecure: Option<&str>,
) -> Result<Option<(SocketAddr, String)>> {
    let listen = listen.map(str::trim).filter(|v| !v.is_empty());
    let url = url.map(str::trim).filter(|v| !v.is_empty());
    let insecure = match insecure.map(str::trim) {
        None | Some("") => false,
        Some("allow") => true,
        Some(other) => bail!("{ENV_INSECURE}={other:?} is allow, or unset"),
    };
    let (listen, url) = match (listen, url) {
        (None, None) => return Ok(None),
        (Some(l), Some(u)) => (l, u),
        (Some(_), None) => {
            bail!("{ENV_LISTEN} is set without {ENV_URL}: jobs would not be told where it is")
        }
        (None, Some(_)) => {
            bail!("{ENV_URL} is set without {ENV_LISTEN}: nothing would answer there")
        }
    };
    let addr: SocketAddr = listen
        .parse()
        .map_err(|e| anyhow!("{ENV_LISTEN}={listen:?} is no address to listen on: {e}"))?;
    let parsed = reqwest::Url::parse(url).map_err(|e| anyhow!("{ENV_URL}={url:?}: {e}"))?;
    if parsed.host_str().is_none_or(str::is_empty) || parsed.query().is_some() {
        bail!("{ENV_URL}={url:?} is no service's URL: a host, and no query");
    }
    // The artifact actions keep the URL's origin alone.
    if parsed.path() != "/" {
        bail!("{ENV_URL}={url:?} has a path: the service is at its host's root, as the actions reach it");
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        bail!("{ENV_URL}={url:?} carries a user");
    }
    match parsed.scheme() {
        "https" => {}
        "http" if insecure => {}
        "http" => bail!(
            "{ENV_URL}={url:?} is plain http: a job sends its credential there from its own \
             container, across the network. Give it an https:// URL, or, on a closed network \
             where that is accepted, set {ENV_INSECURE}=allow"
        ),
        other => bail!("{ENV_URL}={url:?} is {other}://, not https://"),
    }
    let mut base = parsed.to_string();
    if !base.ends_with('/') {
        base.push('/');
    }
    Ok(Some((addr, base)))
}

impl ActionsCache {
    pub fn new(
        caches: Arc<PathCaches>,
        pool: db::Pool,
        url: String,
        dir: PathBuf,
        artifacts: ArtifactSettings,
    ) -> Result<Self> {
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("{ENV_DIR}={}: it could not be made", dir.display()))?;
        // What an engine before a restart left: its uploads were never
        // finalized, and no credential of then is taken now. One older than
        // an upload is kept for, as another engine's of this host may be
        // using the directory too.
        for left in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
            let old = left
                .metadata()
                .and_then(|m| m.modified())
                .ok()
                .and_then(|at| at.elapsed().ok())
                .is_some_and(|age| age > OPEN_FOR_AT_MOST);
            if old {
                let _ = std::fs::remove_dir_all(left.path());
            }
        }
        let mut secret = [0u8; 32];
        for chunk in secret.chunks_mut(16) {
            chunk.copy_from_slice(&uuid::Uuid::new_v4().into_bytes());
        }
        Ok(Self {
            caches,
            pool,
            url,
            dir,
            secret,
            open: Mutex::new(HashMap::new()),
            artifacts,
            executor: None,
            images: Vec::new(),
            steps: Mutex::default(),
            provenance: None,
        })
    }

    /// Run a job's steps in images of their own on `executor`, where it does
    /// (a workflow's Docker actions): a run that is not trusted names one of
    /// `images` there, where the operator listed any.
    pub fn with_steps(
        mut self,
        executor: Arc<dyn dagron_executor::executor::Executor>,
        images: Vec<String>,
    ) -> Self {
        self.executor = Some(executor);
        self.images = images;
        self
    }

    /// Sign each artifact's provenance with `provenance` as it is finalized.
    pub fn with_provenance(mut self, provenance: Option<Provenance>) -> Self {
        self.provenance = provenance;
        self
    }

    /// The variables a task is given: `None` for a run with no repository.
    pub fn offer(
        &self,
        task_id: &str,
        run_id: &str,
        trigger: Option<&RunTrigger>,
        base_ref: Option<&str>,
        trusted: bool,
        timeout_secs: Option<u64>,
    ) -> Option<Vec<(&'static str, String)>> {
        let (repo, own) = path_cache::home(trigger)?;
        let mut scopes = vec![own.clone()];
        if let Some(base) = base_ref.map(str::trim).filter(|b| !b.is_empty()) {
            let base = path_cache::scope(&repo, base);
            if base != own {
                scopes.push(base);
            }
        }
        let lasts = timeout_secs
            .unwrap_or(6 * 3600)
            .saturating_add(900)
            .min(7 * 24 * 3600);
        let claims = Claims {
            t: task_id.to_string(),
            u: run_id.to_string(),
            r: repo,
            s: scopes,
            w: trusted,
            exp: chrono::Utc::now().timestamp() + i64::try_from(lasts).unwrap_or(i64::MAX / 2),
            scp: format!("Actions.Results:{run_id}:{task_id}"),
        };
        Some(vec![
            ("ACTIONS_CACHE_SERVICE_V2", "true".to_string()),
            ("ACTIONS_RESULTS_URL", self.url.clone()),
            ("ACTIONS_RUNTIME_TOKEN", self.token(&claims)),
        ])
    }

    fn mac(&self, message: &[u8]) -> [u8; 32] {
        hmac(&self.secret, message)
    }

    /// A JWT of `claims`, signed with this process's key (HS256).
    fn token(&self, claims: &Claims) -> String {
        let head = b64url(br#"{"alg":"HS256","typ":"JWT"}"#);
        let body = b64url(&serde_json::to_vec(claims).unwrap_or_default());
        let signed = format!("{head}.{body}");
        let mac = b64url(&self.mac(signed.as_bytes()));
        format!("{signed}.{mac}")
    }

    fn claims(&self, headers: &HeaderMap) -> Option<Claims> {
        let bearer = headers
            .get("authorization")?
            .to_str()
            .ok()?
            .strip_prefix("Bearer ")?;
        let (signed, mac) = bearer.rsplit_once('.')?;
        if !same(&b64url(&self.mac(signed.as_bytes())), mac) {
            return None;
        }
        let (_, body) = signed.split_once('.')?;
        let claims: Claims = serde_json::from_slice(&unb64url(body)?).ok()?;
        (claims.exp > chrono::Utc::now().timestamp()).then_some(claims)
    }

    /// The URL an upload of `id` is put to, good while the job's credential is.
    fn upload_url(&self, id: &str, claims: &Claims) -> String {
        let exp = claims.exp;
        format!(
            "{}blob/{id}?exp={exp}&sig={}",
            self.url,
            self.upload_sig(id, exp)
        )
    }

    fn upload_sig(&self, id: &str, exp: i64) -> String {
        hex(&self.mac(format!("put\n{id}\n{exp}").as_bytes()))
    }

    /// Answer jobs at `addr` for the life of the process; and drop uploads
    /// left open too long.
    pub async fn serve(self: Arc<Self>, addr: SocketAddr) -> Result<()> {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("{ENV_LISTEN}={addr}"))?;
        tracing::info!(%addr, url = %self.url, "the actions cache service is listening");
        let sweeper = Arc::clone(&self);
        tokio::spawn(async move {
            let mut every = tokio::time::interval(Duration::from_secs(3600));
            every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                every.tick().await;
                sweeper.drop_stale();
                match artifacts::sweep(&sweeper).await {
                    Ok(0) => {}
                    Ok(gone) => tracing::info!(gone, "expired workflow artifacts were deleted"),
                    Err(e) => {
                        tracing::warn!(error = %format!("{e:#}"), "the workflow artifact sweep failed")
                    }
                }
            }
        });
        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, router(self)).await {
                tracing::error!(error = %e, "the actions cache service stopped");
            }
        });
        Ok(())
    }

    fn drop_stale(&self) {
        let stale: Vec<Open> = {
            let mut open = self.open.lock().unwrap();
            let gone: Vec<String> = open
                .iter()
                .filter(|(_, o)| o.at.elapsed() > OPEN_FOR_AT_MOST)
                .map(|(id, _)| id.clone())
                .collect();
            gone.iter().filter_map(|id| open.remove(id)).collect()
        };
        for o in stale {
            let _ = std::fs::remove_dir_all(self.dir.join(&o.id));
        }
        steps::drop_stale(&self.steps);
    }
}

pub fn router(svc: Arc<ActionsCache>) -> Router {
    Router::new()
        .route("/twirp/{service}/{method}", post(twirp))
        .route("/blob/{id}", put(blob))
        .route("/dagron/v1/steps", post(steps::start))
        .route(
            "/dagron/v1/steps/images",
            post(steps::build).layer(axum::extract::DefaultBodyLimit::max(
                steps::MAX_CONTEXT_BYTES,
            )),
        )
        .route(
            "/dagron/v1/steps/{id}",
            axum::routing::get(steps::poll).delete(steps::stop),
        )
        .with_state(svc)
}

/// A twirp error, as the action's client reads one.
fn twirp_error(status: StatusCode, code: &str, msg: &str) -> Response {
    (status, Json(json!({ "code": code, "msg": msg }))).into_response()
}

/// A field of a request, by its proto name or its JSON name.
fn field<'a>(body: &'a Value, proto: &str, json: &str) -> Option<&'a Value> {
    body.get(proto).or_else(|| body.get(json))
}

fn text(body: &Value, proto: &str, json: &str) -> String {
    field(body, proto, json)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn plain(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_KEY && !value.chars().any(char::is_control)
}

async fn twirp(
    State(svc): State<Arc<ActionsCache>>,
    Path((service, method)): Path<(String, String)>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let Some(claims) = svc.claims(&headers) else {
        return twirp_error(
            StatusCode::UNAUTHORIZED,
            "unauthenticated",
            "no credential this engine gave a job, or one that has lapsed",
        );
    };
    let Ok(body) = serde_json::from_slice::<Value>(&body) else {
        return twirp_error(
            StatusCode::BAD_REQUEST,
            "malformed",
            "the request is not JSON",
        );
    };
    let answered = match (service.as_str(), method.as_str()) {
        (CACHE_SERVICE, "CreateCacheEntry") => create(&svc, &claims, &body).await,
        (CACHE_SERVICE, "FinalizeCacheEntryUpload") => finalize(&svc, &claims, &body).await,
        (CACHE_SERVICE, "GetCacheEntryDownloadURL") => download(&svc, &claims, &body).await,
        (ARTIFACT_SERVICE, "CreateArtifact") => artifacts::create(&svc, &claims, &body).await,
        (ARTIFACT_SERVICE, "FinalizeArtifact") => artifacts::finalize(&svc, &claims, &body).await,
        (ARTIFACT_SERVICE, "ListArtifacts") => artifacts::list(&svc, &claims, &body).await,
        (ARTIFACT_SERVICE, "GetSignedArtifactURL") => {
            artifacts::signed_url(&svc, &claims, &body).await
        }
        (ARTIFACT_SERVICE, "DeleteArtifact") => artifacts::delete(&svc, &claims, &body).await,
        _ => return twirp_error(StatusCode::NOT_FOUND, "bad_route", "no such method"),
    };
    match answered {
        Ok(value) => Json(value).into_response(),
        Err(Refusal::Said(status, code, msg)) => twirp_error(status, code, &msg),
        Err(Refusal::Failed(e)) => {
            tracing::warn!(service, method, task = %claims.t, error = %format!("{e:#}"), "the actions results service failed a call");
            twirp_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                "the results service failed",
            )
        }
    }
}

/// Reserve an entry: the URL the action uploads its archive to.
async fn create(svc: &ActionsCache, claims: &Claims, body: &Value) -> Answer {
    let (key, version) = (text(body, "key", "key"), text(body, "version", "version"));
    if !claims.w {
        return Ok(json!({ "ok": false, "message": "a run that is not trusted saves no cache" }));
    }
    if !plain(&key) || !plain(&version) {
        return Ok(
            json!({ "ok": false, "message": "a cache's key and version are 1 to 512 characters" }),
        );
    }
    let Some(scope) = claims.s.first().cloned() else {
        return Ok(json!({ "ok": false, "message": "the credential names no scope" }));
    };
    let id = uuid::Uuid::new_v4().simple().to_string();
    let object_key = path_cache::object_key(&scope, PREFIX, &id);
    // Reserved as it is checked, under the one lock: two saves of a key at
    // once are one save, and the other is told so.
    {
        let mut open = svc.open.lock().unwrap();
        if open.values().filter(|o| o.task == claims.t).count() >= MAX_OPEN_PER_TASK {
            return Ok(json!({ "ok": false, "message": "this job has too many uploads open" }));
        }
        let saving = |o: &Open| {
            matches!(&o.what, What::Cache { scope: s, key: k, version: v, .. }
                if *s == scope && *k == key && *v == version)
        };
        if open.values().any(saving) {
            return Ok(json!({ "ok": false, "message": "another job is saving this key" }));
        }
        open.insert(
            id.clone(),
            Open {
                id: id.clone(),
                task: claims.t.clone(),
                at: Instant::now(),
                max: svc.caches.settings().max_entry_bytes,
                what: What::Cache {
                    scope: scope.clone(),
                    key,
                    version: version.clone(),
                    object_key: object_key.clone(),
                },
            },
        );
    }
    let offered = db::path_cache_offered(
        &svc.pool,
        &NewPathCache {
            id: id.clone(),
            repo: claims.r.clone(),
            scope,
            prefix: PREFIX.to_string(),
            version,
            object_key,
            task_id: claims.t.clone(),
        },
    )
    .await;
    if let Err(e) = offered {
        svc.open.lock().unwrap().remove(&id);
        return Err(e.into());
    }
    Ok(json!({ "ok": true, "signed_upload_url": svc.upload_url(&id, claims) }))
}

/// Finalize an entry: its archive put in the store, and the entry ready.
async fn finalize(svc: &ActionsCache, claims: &Claims, body: &Value) -> Answer {
    let (key, version) = (text(body, "key", "key"), text(body, "version", "version"));
    let size: u64 = match field(body, "size_bytes", "sizeBytes") {
        Some(Value::String(s)) => s.parse().unwrap_or(u64::MAX),
        Some(Value::Number(n)) => n.as_u64().unwrap_or(u64::MAX),
        _ => u64::MAX,
    };
    let scope = claims.s.first().cloned().unwrap_or_default();
    let found = {
        let open = svc.open.lock().unwrap();
        open.values().find_map(|o| match &o.what {
            What::Cache {
                scope: s,
                key: k,
                version: v,
                object_key,
            } if o.task == claims.t && *s == scope && *k == key && *v == version => {
                Some((o.id.clone(), object_key.clone()))
            }
            _ => None,
        })
    };
    let Some((open_id, object_key)) = found else {
        return Ok(json!({ "ok": false, "message": "no upload of this key is open for this job" }));
    };
    let open = (open_id, object_key, key);
    let (id, object_key, key) = open;
    let whole = svc.dir.join(&id).join("whole");
    let had = tokio::fs::metadata(&whole).await.map(|m| m.len()).ok();
    if had != Some(size) || size > svc.caches.settings().max_entry_bytes {
        svc.forget(&id).await;
        return Ok(
            json!({ "ok": false, "message": "the upload is not the size the job says, or is too large" }),
        );
    }
    let (archive, manifest) = path_cache::objects(&object_key)?;
    let file = tokio::fs::File::open(&whole)
        .await
        .map_err(anyhow::Error::from)?;
    let store = svc.caches.store();
    store
        .put_stream(&archive, Box::new(file))
        .await
        .context("the archive could not be put in the cache store")?;
    // The manifest a path cache's task writes beside its archive: an entry's
    // two objects are there whoever saved it.
    let written = serde_json::to_vec(&json!({ "v": 1, "key": key, "size": size }))
        .map_err(anyhow::Error::from)?;
    store
        .put(&manifest, &written)
        .await
        .context("the manifest could not be put in the cache store")?;
    let replaced = db::path_cache_ready(
        &svc.pool,
        &id,
        &key,
        i64::try_from(size).unwrap_or(i64::MAX),
    )
    .await?
    .unwrap_or_default();
    for object_key in replaced {
        svc.caches.delete(&object_key).await;
    }
    svc.forget(&id).await;
    tracing::info!(task = %claims.t, key = %key, size, "an actions cache entry is ready");
    Ok(json!({ "ok": true, "entry_id": number_of(&id).to_string() }))
}

/// The number a job knows an entry or an artifact by, from its id: below
/// 2^53, which JavaScript reads as it is, and never 0.
fn number_of(id: &str) -> i64 {
    let digest = Sha256::digest(id.as_bytes());
    let mut first = [0u8; 8];
    first.copy_from_slice(&digest[..8]);
    i64::try_from(u64::from_be_bytes(first) >> 11)
        .unwrap_or(1)
        .max(1)
}

impl ActionsCache {
    /// An upload done with, finalized or not: its reservation and its files.
    async fn forget(&self, id: &str) {
        self.open.lock().unwrap().remove(id);
        let _ = tokio::fs::remove_dir_all(self.dir.join(id)).await;
    }
}

/// The entry a restore reads, and a URL the store signed for it.
async fn download(svc: &ActionsCache, claims: &Claims, body: &Value) -> Answer {
    let (key, version) = (text(body, "key", "key"), text(body, "version", "version"));
    let restore_keys: Vec<String> = field(body, "restore_keys", "restoreKeys")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|k| plain(k))
        .take(MAX_RESTORE_KEYS)
        .map(str::to_string)
        .collect();
    if !plain(&key) || !plain(&version) {
        return Ok(json!({ "ok": false }));
    }
    for scope in &claims.s {
        let entries =
            db::path_cache_entries(&svc.pool, scope, PREFIX, &version, LOOK_THROUGH).await?;
        let found = entries.iter().find(|e| e.cache_key == key).or_else(|| {
            restore_keys.iter().find_map(|prefix| {
                entries
                    .iter()
                    .find(|e| e.cache_key.starts_with(prefix.as_str()))
            })
        });
        if let Some(entry) = found {
            let (archive, _) = path_cache::objects(&entry.object_key)?;
            let signed = svc
                .caches
                .sign(&archive, Transfer::Get, DOWNLOAD_SECS)
                .await?;
            db::path_cache_used(&svc.pool, &entry.id).await?;
            return Ok(json!({
                "ok": true,
                "signed_download_url": signed.url,
                "matched_key": entry.cache_key,
            }));
        }
    }
    Ok(json!({ "ok": false }))
}

#[derive(Deserialize)]
struct BlobQuery {
    exp: i64,
    sig: String,
    #[serde(default)]
    comp: Option<String>,
    #[serde(default)]
    blockid: Option<String>,
}

/// What Azure's Blob SDK puts: a blob whole, a block of one, or the list of
/// its blocks, in their order. Answered as Azure answers it.
async fn blob(
    State(svc): State<Arc<ActionsCache>>,
    Path(id): Path<String>,
    Query(q): Query<BlobQuery>,
    body: Body,
) -> Response {
    let refuse = |status: StatusCode, code: &str| {
        let mut r = (status, code.to_string()).into_response();
        r.headers_mut().insert(
            "x-ms-error-code",
            HeaderValue::from_str(code).unwrap_or(HeaderValue::from_static("Error")),
        );
        r
    };
    if q.exp <= chrono::Utc::now().timestamp() || !same(&svc.upload_sig(&id, q.exp), &q.sig) {
        return refuse(StatusCode::FORBIDDEN, "AuthenticationFailed");
    }
    let Some(max) = svc.open.lock().unwrap().get(&id).map(|o| o.max) else {
        return refuse(StatusCode::NOT_FOUND, "BlobNotFound");
    };
    let dir = svc.dir.join(&id);
    let done = match (q.comp.as_deref(), q.blockid.as_deref()) {
        (None, _) => receive(body, &dir.join("whole"), max).await,
        (Some("block"), Some(block)) => {
            let name = hex(&Sha256::digest(block.as_bytes())[..16]);
            receive(body, &dir.join("blocks").join(name), max).await
        }
        (Some("blocklist"), _) => commit(body, &dir, max).await,
        _ => return refuse(StatusCode::BAD_REQUEST, "InvalidQueryParameterValue"),
    };
    match done {
        Ok(()) => {
            let mut r = StatusCode::CREATED.into_response();
            let h = r.headers_mut();
            h.insert("etag", HeaderValue::from_static("\"0x8D0000000000000\""));
            h.insert("x-ms-version", HeaderValue::from_static("2024-11-04"));
            h.insert(
                "x-ms-request-server-encrypted",
                HeaderValue::from_static("true"),
            );
            if let Ok(v) = HeaderValue::from_str(&uuid::Uuid::new_v4().to_string()) {
                h.insert("x-ms-request-id", v);
            }
            r
        }
        Err(e) => {
            tracing::warn!(id, error = %format!("{e:#}"), "an actions cache upload was refused");
            if e.downcast_ref::<TooLarge>().is_some() {
                refuse(StatusCode::PAYLOAD_TOO_LARGE, "RequestBodyTooLarge")
            } else {
                refuse(StatusCode::BAD_REQUEST, "InvalidBlockList")
            }
        }
    }
}

/// An upload larger than an entry may be.
#[derive(Debug)]
struct TooLarge(u64);

impl std::fmt::Display for TooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "more than {} bytes", self.0)
    }
}

impl std::error::Error for TooLarge {}

/// Write `body` to `path`, to `max` bytes.
async fn receive(body: Body, path: &std::path::Path, max: u64) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp = beside(path);
    let written = async {
        let mut file = tokio::fs::File::create(&tmp).await?;
        let mut stream = body.into_data_stream();
        let mut written: u64 = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| anyhow!("the upload broke off: {e}"))?;
            written += chunk.len() as u64;
            if written > max {
                return Err(TooLarge(max).into());
            }
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        Ok(())
    }
    .await;
    into_place(written, &tmp, path).await
}

/// A file of its own beside `path`, which a body is written to before it
/// takes `path`'s place: a block, or the blob whole, is either what one
/// request put, all of it, or what was there before.
fn beside(path: &std::path::Path) -> PathBuf {
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("part");
    path.with_file_name(format!(".{name}.{}", uuid::Uuid::new_v4().simple()))
}

/// `tmp` in `path`'s place once it was `written`; else gone.
async fn into_place(
    written: Result<()>,
    tmp: &std::path::Path,
    path: &std::path::Path,
) -> Result<()> {
    let placed = match written {
        Ok(()) => tokio::fs::rename(tmp, path).await.map_err(Into::into),
        Err(e) => Err(e),
    };
    if placed.is_err() {
        let _ = tokio::fs::remove_file(tmp).await;
    }
    placed
}

/// Put the blocks a block list names, in its order, into the blob whole.
async fn commit(body: Body, dir: &std::path::Path, max: u64) -> Result<()> {
    let bytes = axum::body::to_bytes(body, MAX_BLOCK_LIST)
        .await
        .map_err(|e| anyhow!("the block list: {e}"))?;
    let list = String::from_utf8_lossy(&bytes);
    let ids = block_ids(&list);
    if ids.is_empty() {
        bail!("the block list names no block");
    }
    let whole = dir.join("whole");
    let tmp = beside(&whole);
    let written = async {
        let mut out = tokio::fs::File::create(&tmp).await?;
        let mut written: u64 = 0;
        for block in &ids {
            let name = hex(&Sha256::digest(block.as_bytes())[..16]);
            let mut part = tokio::fs::File::open(dir.join("blocks").join(name))
                .await
                .with_context(|| format!("the block list names {block:?}, which was not put"))?;
            written += tokio::io::copy(&mut part, &mut out).await?;
            if written > max {
                return Err(TooLarge(max).into());
            }
        }
        out.flush().await?;
        Ok(())
    }
    .await;
    into_place(written, &tmp, &whole).await?;
    let _ = tokio::fs::remove_dir_all(dir.join("blocks")).await;
    Ok(())
}

/// The ids of an Azure block list, in its order: what each `<Latest>`,
/// `<Committed>` or `<Uncommitted>` element holds.
fn block_ids(list: &str) -> Vec<String> {
    let mut ids = Vec::new();
    let mut rest = list;
    while let Some(open) = rest.find('<') {
        rest = &rest[open + 1..];
        let Some(close) = rest.find('>') else { break };
        let tag = &rest[..close];
        rest = &rest[close + 1..];
        if matches!(tag, "Latest" | "Committed" | "Uncommitted") {
            let end = format!("</{tag}>");
            let Some(at) = rest.find(&end) else { break };
            ids.push(rest[..at].trim().to_string());
            rest = &rest[at + end.len()..];
        }
    }
    ids
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

const B64URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Base64url, unpadded (RFC 4648 §5), as a JWT's parts are.
fn b64url(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, b)| n | (u32::from(*b) << (16 - 8 * i)));
        for i in 0..=chunk.len() {
            out.push(char::from(B64URL[((n >> (18 - 6 * i)) & 63) as usize]));
        }
    }
    out
}

fn unb64url(text: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(text.len() * 3 / 4);
    let (mut n, mut bits) = (0u32, 0u32);
    for c in text.bytes() {
        let v = B64URL.iter().position(|b| *b == c)? as u32;
        n = (n << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(((n >> bits) & 0xff) as u8);
        }
    }
    // What is left over is padding's bits, which are zero.
    (bits < 6 && n & ((1 << bits) - 1) == 0).then_some(out)
}

/// Whether two texts are the same, in a time that does not say where they
/// part.
fn same(a: &str, b: &str) -> bool {
    a.len() == b.len() && a.bytes().zip(b.bytes()).fold(0u8, |d, (x, y)| d | (x ^ y)) == 0
}

/// HMAC-SHA256 (RFC 2104).
fn hmac(key: &[u8; 32], message: &[u8]) -> [u8; 32] {
    let mut pad = [0u8; 64];
    pad[..32].copy_from_slice(key);
    let (mut inner, mut outer) = (Sha256::new(), Sha256::new());
    inner.update(pad.map(|b| b ^ 0x36));
    inner.update(message);
    outer.update(pad.map(|b| b ^ 0x5c));
    outer.update(inner.finalize());
    outer.finalize().into()
}

#[cfg(test)]
mod tests;
