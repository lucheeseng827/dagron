//! **Path caches**: what a task's
//! `workspace.cache` keeps between runs, in the operator's cache store.
//!
//! The store is an object store that signs URLs (`DAGRON_CACHE_URL`:
//! `s3://`, `gs://`, `az://`). The engine never moves a cache's bytes. As it
//! dispatches a task with caches it gives it, in `DAGRON_CACHE`:
//!
//! - for each cache, signed URLs to read the newest entries of it in the run's
//!   repository and ref, and the newest of the ref it names to restore from
//!   (a pull request's base branch); each with the key it was saved under;
//! - where the run is trusted, signed URLs to write one more entry: its
//!   archive and its manifest. A run that is not trusted is given none, so it
//!   never writes what a later run would read: the rule against poisoning.
//!
//! `dagron-ci-cache`, in the task, computes the key from the cache's
//! `key_files`, restores the entry with that key or the newest, and after the
//! task's steps succeed saves under the key unless it restored that key. An
//! entry it was given URLs for is `pending` in `path_caches` until this
//! module's sweep sees its task succeed and its manifest in the store; it is
//! then `ready`, in the place of any entry of the same key. The sweep also
//! drops what a task that did not succeed left, entries not read for
//! `DAGRON_CACHE_UNUSED_DAYS`, and, a repository at a time, the least recently
//! read entries past `DAGRON_CACHE_QUOTA_BYTES`.
//!
//! A run is kept to its own repository: entries are scoped by the forge host,
//! the repository and the ref of the run's record, never by anything the
//! task's spec says, and a ref to restore from is read in the same
//! repository. A run with no repository in its record has no cache.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use dagron_artifact::{ArtifactKey, ArtifactStore, Presigned, Transfer};
use dagron_core::db;
use dagron_core::models::{NewPathCache, RunTrigger};
use dagron_core::workspace::PathCacheSpec;
use serde_json::json;
use sha2::{Digest, Sha256};

/// The variable a task is given its caches in.
pub const ENV: &str = "DAGRON_CACHE";
/// The format of an entry: a tar archive, zstd-compressed, of the cache's
/// paths, `<index>/…` for each. Part of an entry's version, so a change of it
/// reads no entry of the old.
pub const FORMAT: &str = "tar+zstd/1";
/// Where in the store the entries are: under this, a directory a scope.
const OBJECTS: &str = "path-caches";
/// How many of a scope's newest entries of a cache a task is offered.
const OWN_ENTRIES: i64 = 3;
/// The longest a URL is signed for: what S3 and GCS allow.
const MAX_URL_SECS: u64 = 7 * 24 * 3600;
/// A pending entry this old is dropped whatever its task's state: no URL it
/// was given still works.
const PENDING_FOR_AT_MOST: chrono::Duration = chrono::Duration::hours(7 * 24 + 1);
/// Rows a sweep takes at once, of each kind.
const SWEEP_BATCH: i64 = 64;
/// A manifest is a few hundred bytes; one larger is not one.
const MAX_MANIFEST_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    /// What one repository's entries may hold together.
    pub quota_bytes: i64,
    /// The largest entry a task saves; one larger is not saved.
    pub max_entry_bytes: u64,
    /// An entry not read for this long is dropped.
    pub unused_days: i64,
    /// How long a URL is signed for, for a task with no timeout. A task with
    /// one is given its timeout and fifteen minutes.
    pub url_secs: u64,
    /// How often the sweep runs.
    pub sweep_secs: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            quota_bytes: 10 << 30,
            max_entry_bytes: 4 << 30,
            unused_days: 7,
            url_secs: 6 * 3600,
            sweep_secs: 30,
        }
    }
}

impl Settings {
    pub fn from_env() -> Result<Self> {
        let d = Self::default();
        fn read<T: std::str::FromStr>(
            name: &str,
            default: T,
            ok: impl Fn(&T) -> bool,
        ) -> Result<T> {
            match std::env::var(name).ok().filter(|v| !v.trim().is_empty()) {
                None => Ok(default),
                Some(raw) => match raw.trim().parse::<T>() {
                    Ok(v) if ok(&v) => Ok(v),
                    _ => bail!("{name}={raw:?} is not a number this setting takes"),
                },
            }
        }
        Ok(Self {
            quota_bytes: read("DAGRON_CACHE_QUOTA_BYTES", d.quota_bytes, |v| *v > 0)?,
            max_entry_bytes: read("DAGRON_CACHE_MAX_ENTRY_BYTES", d.max_entry_bytes, |v| {
                *v > 0
            })?,
            unused_days: read("DAGRON_CACHE_UNUSED_DAYS", d.unused_days, |v| *v > 0)?,
            url_secs: read("DAGRON_CACHE_URL_SECS", d.url_secs, |v| {
                (60..=MAX_URL_SECS).contains(v)
            })?,
            sweep_secs: read("DAGRON_CACHE_SWEEP_SECS", d.sweep_secs, |v| *v > 0)?,
        })
    }
}

/// The engine's cache store and what it keeps to.
pub struct PathCaches {
    store: Arc<dyn ArtifactStore>,
    settings: Settings,
}

/// What a task asks of its caches as it is dispatched.
pub struct Asked<'a> {
    pub task_id: &'a str,
    pub trigger: Option<&'a RunTrigger>,
    /// The run's class is `trusted`: only then is the task given URLs to save.
    pub trusted: bool,
    pub caches: &'a [PathCacheSpec],
    pub timeout_secs: Option<u64>,
}

/// Where a run's entries are: its repository, and its repository and ref.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Home {
    repo: String,
    git_ref: String,
}

impl Home {
    fn of(trigger: Option<&RunTrigger>) -> Option<Self> {
        let t = trigger?;
        let repo = t.repo.as_deref().filter(|r| !r.is_empty())?;
        let git_ref = t.git_ref.as_deref().filter(|r| !r.is_empty())?;
        let host = t
            .forge_host
            .as_deref()
            .filter(|h| !h.is_empty())
            .unwrap_or("-");
        Some(Self {
            repo: format!("{host}/{repo}"),
            git_ref: git_ref.to_string(),
        })
    }

    fn scope(&self, git_ref: &str) -> String {
        format!("{}@{git_ref}", self.repo)
    }
}

/// The version of a cache's entries: of its paths, in order, and the format.
/// Two caches of one prefix that keep other paths read none of each other's.
pub fn version(cache: &PathCacheSpec) -> String {
    let mut h = Sha256::new();
    h.update(FORMAT.as_bytes());
    for path in &cache.paths {
        h.update([0u8]);
        h.update(path.as_bytes());
    }
    hex16(&h.finalize())
}

fn hex16(bytes: &[u8]) -> String {
    bytes.iter().take(8).map(|b| format!("{b:02x}")).collect()
}

/// A run's repository and its own scope, as its entries are kept: `None`
/// for a run whose record names no repository and ref. These four are the
/// cache service's (`actions_cache`, of the `ops` feature).
#[cfg_attr(not(feature = "ops"), allow(dead_code))]
pub(crate) fn home(trigger: Option<&RunTrigger>) -> Option<(String, String)> {
    let home = Home::of(trigger)?;
    let own = home.scope(&home.git_ref);
    Some((home.repo, own))
}

/// The scope of a repository's entries for another of its refs.
#[cfg_attr(not(feature = "ops"), allow(dead_code))]
pub(crate) fn scope(repo: &str, git_ref: &str) -> String {
    format!("{repo}@{git_ref}")
}

/// Where a new entry of `prefix` in `scope` is kept: `<scope's dir>/<prefix>-<id>`.
#[cfg_attr(not(feature = "ops"), allow(dead_code))]
pub(crate) fn object_key(scope: &str, prefix: &str, id: &str) -> String {
    let dir = hex16(&Sha256::digest(scope.as_bytes()));
    format!("{dir}/{prefix}-{id}")
}

/// An entry's two objects, from the key its row keeps: `<scope>/<prefix>-<id>`.
pub(crate) fn objects(object_key: &str) -> Result<(ArtifactKey, ArtifactKey)> {
    let Some((dir, name)) = object_key.split_once('/') else {
        bail!("a path cache's object key {object_key:?} names no directory");
    };
    Ok((
        ArtifactKey::new(OBJECTS, dir, format!("{name}.tar.zst")),
        ArtifactKey::new(OBJECTS, dir, format!("{name}.json")),
    ))
}

fn signed(p: &Presigned) -> serde_json::Value {
    json!({
        "method": p.method,
        "url": p.url,
        "headers": p.headers.iter().map(|(k, v)| [*k, *v]).collect::<Vec<_>>(),
    })
}

/// What a task wrote with its entry.
#[derive(Debug, serde::Deserialize)]
struct Manifest {
    key: String,
    size: u64,
}

impl PathCaches {
    /// The store `DAGRON_CACHE_URL` names, or `None` when it is unset. A store
    /// that cannot sign a URL is refused: the engine moves no cache's bytes.
    pub async fn from_env() -> Result<Option<Arc<Self>>> {
        let Some(url) = std::env::var("DAGRON_CACHE_URL")
            .ok()
            .map(|u| u.trim().trim_end_matches('/').to_string())
            .filter(|u| !u.is_empty())
        else {
            return Ok(None);
        };
        let caches = Self::new(open(&url)?, Settings::from_env()?);
        caches
            .sign(
                &ArtifactKey::new(OBJECTS, "probe", "probe"),
                Transfer::Get,
                60,
            )
            .await
            .with_context(|| format!("DAGRON_CACHE_URL={url}"))?;
        Ok(Some(Arc::new(caches)))
    }

    pub fn new(store: Arc<dyn ArtifactStore>, settings: Settings) -> Self {
        Self { store, settings }
    }

    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    /// The store entries are kept in.
    #[cfg_attr(not(feature = "ops"), allow(dead_code))]
    pub(crate) fn store(&self) -> &Arc<dyn ArtifactStore> {
        &self.store
    }

    pub(crate) async fn sign(
        &self,
        key: &ArtifactKey,
        how: Transfer,
        secs: u64,
    ) -> Result<Presigned> {
        match self
            .store
            .presign(key, how, Duration::from_secs(secs))
            .await?
        {
            Some(p) => Ok(p),
            None => bail!("the cache store signs no URL: name an s3://, gs:// or az:// store"),
        }
    }

    /// What to give a task in [`ENV`]: `None` for a task with no cache, or of
    /// a run with no repository.
    pub async fn offer(&self, pool: &db::Pool, asked: Asked<'_>) -> Result<Option<String>> {
        if asked.caches.is_empty() {
            return Ok(None);
        }
        let Some(home) = Home::of(asked.trigger) else {
            tracing::info!(
                task_id = asked.task_id,
                "workspace.cache is kept for a run of a repository, and this run names none: the task starts cold"
            );
            return Ok(None);
        };
        let secs = asked
            .timeout_secs
            .map_or(self.settings.url_secs, |t| t.saturating_add(900))
            .min(MAX_URL_SECS);
        let own = home.scope(&home.git_ref);
        let mut caches = Vec::with_capacity(asked.caches.len());
        for cache in asked.caches {
            let version = version(cache);
            let mut restore = Vec::new();
            let mut first = None;
            let mut froms = vec![(own.clone(), OWN_ENTRIES, "own".to_string())];
            if let Some(base) = cache.restore_from.as_deref().filter(|b| *b != home.git_ref) {
                froms.push((home.scope(base), 1, base.to_string()));
            }
            for (scope, at_most, from) in froms {
                for entry in
                    db::path_cache_entries(pool, &scope, &cache.prefix, &version, at_most).await?
                {
                    let (archive, _) = objects(&entry.object_key)?;
                    let get = self.sign(&archive, Transfer::Get, secs).await?;
                    first.get_or_insert(entry.id.clone());
                    let mut read = signed(&get);
                    read["key"] = json!(entry.cache_key);
                    read["from"] = json!(from);
                    restore.push(read);
                }
            }
            // What is offered first is what a task most often restores: it
            // was used now, for the quota's order.
            if let Some(id) = first {
                db::path_cache_used(pool, &id).await?;
            }
            let save = if asked.trusted {
                let id = uuid::Uuid::new_v4().to_string();
                let dir = hex16(&Sha256::digest(own.as_bytes()));
                let object_key = format!("{dir}/{}-{id}", cache.prefix);
                let (archive, manifest) = objects(&object_key)?;
                let put_archive = self.sign(&archive, Transfer::Put, secs).await?;
                let put_manifest = self.sign(&manifest, Transfer::Put, secs).await?;
                db::path_cache_offered(
                    pool,
                    &NewPathCache {
                        id,
                        repo: home.repo.clone(),
                        scope: own.clone(),
                        prefix: cache.prefix.clone(),
                        version: version.clone(),
                        object_key,
                        task_id: asked.task_id.to_string(),
                    },
                )
                .await?;
                json!({ "archive": signed(&put_archive), "manifest": signed(&put_manifest) })
            } else {
                serde_json::Value::Null
            };
            caches.push(json!({
                "prefix": cache.prefix,
                "version": version,
                "key_files": cache.key_files,
                "paths": cache.paths,
                "restore": restore,
                "save": save,
            }));
        }
        Ok(Some(
            json!({ "v": 1, "max_bytes": self.settings.max_entry_bytes, "caches": caches })
                .to_string(),
        ))
    }

    /// One pass: make ready what tasks that succeeded saved, drop what the
    /// rest left, and evict. Safe on every replica at once: each row is taken
    /// by the statement that changes it.
    pub async fn sweep(&self, pool: &db::Pool) -> Result<Swept> {
        let mut swept = Swept::default();
        let stale = (chrono::Utc::now() - PENDING_FOR_AT_MOST).to_rfc3339();
        for pending in db::path_caches_settled(pool, &stale, SWEEP_BATCH).await? {
            let succeeded =
                pending.task_status.as_deref() == Some("succeeded") && pending.created_at >= stale;
            if succeeded {
                match self.settle(pool, &pending.id, &pending.object_key).await {
                    Ok(true) => swept.ready += 1,
                    Ok(false) => swept.dropped += 1,
                    // The store did not answer: asked again next time.
                    Err(e) => {
                        tracing::warn!(id = %pending.id, error = %format!("{e:#}"), "a path cache entry was not settled")
                    }
                }
            } else if self.forget(pool, &pending.id).await? {
                swept.dropped += 1;
            }
        }
        let unused =
            (chrono::Utc::now() - chrono::Duration::days(self.settings.unused_days)).to_rfc3339();
        for id in
            db::path_cache_victims(pool, self.settings.quota_bytes, &unused, SWEEP_BATCH).await?
        {
            if self.forget(pool, &id).await? {
                swept.evicted += 1;
            }
        }
        Ok(swept)
    }

    /// A pending entry whose task succeeded: ready if its task saved it,
    /// dropped if it did not (it restored the key it would have saved, or its
    /// save failed). `Ok(true)` when it is now ready.
    async fn settle(&self, pool: &db::Pool, id: &str, object_key: &str) -> Result<bool> {
        let (archive, manifest) = objects(object_key)?;
        if !self.store.exists(&manifest).await? {
            self.forget(pool, id).await?;
            return Ok(false);
        }
        let bytes = self.store.get(&manifest).await?;
        let read = (bytes.len() <= MAX_MANIFEST_BYTES)
            .then(|| serde_json::from_slice::<Manifest>(&bytes).ok())
            .flatten()
            .filter(|m| {
                !m.key.is_empty() && m.key.len() <= 512 && !m.key.chars().any(char::is_control)
            });
        let Some(m) = read else {
            tracing::warn!(id, "a path cache entry's manifest is not one: dropped");
            self.forget(pool, id).await?;
            return Ok(false);
        };
        let size = match self.store.size(&archive).await {
            Ok(Some(size)) => size,
            Ok(None) => {
                self.forget(pool, id).await?;
                return Ok(false);
            }
            // A store that does not say: the task's word.
            Err(_) => m.size,
        };
        match db::path_cache_ready(pool, id, &m.key, i64::try_from(size).unwrap_or(i64::MAX))
            .await?
        {
            Some(replaced) => {
                for object_key in replaced {
                    self.delete(&object_key).await;
                }
                tracing::info!(id, key = %m.key, size, "a path cache entry is ready");
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Drop an entry's row, then its objects. `false` when another sweep
    /// already had.
    async fn forget(&self, pool: &db::Pool, id: &str) -> Result<bool> {
        match db::path_cache_drop(pool, id).await? {
            Some(object_key) => {
                self.delete(&object_key).await;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// An object the index no longer names: a failure is said and left; it
    /// is the store's bytes, and nothing reads them.
    pub(crate) async fn delete(&self, object_key: &str) {
        let Ok((archive, manifest)) = objects(object_key) else {
            return;
        };
        for key in [archive, manifest] {
            if let Err(e) = self.store.delete(&key).await {
                tracing::warn!(object = %key.rel_path(), error = %format!("{e:#}"), "a path cache object was not deleted");
            }
        }
    }

    /// The sweep, every `DAGRON_CACHE_SWEEP_SECS`, for the life of the process.
    pub fn spawn_sweeper(self: Arc<Self>, pool: db::Pool) {
        tokio::spawn(async move {
            let mut every = tokio::time::interval(Duration::from_secs(self.settings.sweep_secs));
            every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                every.tick().await;
                match self.sweep(&pool).await {
                    Ok(s) if s != Swept::default() => {
                        tracing::info!(
                            ready = s.ready,
                            dropped = s.dropped,
                            evicted = s.evicted,
                            "path caches swept"
                        )
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(error = %format!("{e:#}"), "the path cache sweep failed")
                    }
                }
            }
        });
    }
}

/// What a sweep did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Swept {
    pub ready: usize,
    pub dropped: usize,
    pub evicted: usize,
}

#[cfg(any(
    feature = "archive-s3",
    feature = "archive-gcs",
    feature = "archive-azure"
))]
fn open(url: &str) -> Result<Arc<dyn ArtifactStore>> {
    Ok(Arc::new(
        dagron_artifact::cloud::CloudStore::from_url(url)
            .with_context(|| format!("DAGRON_CACHE_URL={url}"))?,
    ))
}

#[cfg(not(any(
    feature = "archive-s3",
    feature = "archive-gcs",
    feature = "archive-azure"
)))]
fn open(url: &str) -> Result<Arc<dyn ArtifactStore>> {
    bail!(
        "DAGRON_CACHE_URL={url} names an object store, and this engine was built with none \
         (features archive-s3, archive-gcs, archive-azure)"
    )
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// A store in memory that signs `mem://<path>?<how>` URLs.
    #[derive(Default)]
    struct Mem(Mutex<HashMap<String, Vec<u8>>>);

    #[async_trait::async_trait]
    impl ArtifactStore for Mem {
        async fn put(&self, key: &ArtifactKey, bytes: &[u8]) -> Result<String> {
            self.0
                .lock()
                .unwrap()
                .insert(key.rel_path(), bytes.to_vec());
            Ok(key.rel_path())
        }
        async fn get(&self, key: &ArtifactKey) -> Result<Vec<u8>> {
            self.0
                .lock()
                .unwrap()
                .get(&key.rel_path())
                .cloned()
                .context("no such object")
        }
        async fn exists(&self, key: &ArtifactKey) -> Result<bool> {
            Ok(self.0.lock().unwrap().contains_key(&key.rel_path()))
        }
        async fn delete(&self, key: &ArtifactKey) -> Result<bool> {
            Ok(self.0.lock().unwrap().remove(&key.rel_path()).is_some())
        }
        async fn size(&self, key: &ArtifactKey) -> Result<Option<u64>> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .get(&key.rel_path())
                .map(|b| b.len() as u64))
        }
        async fn presign(
            &self,
            key: &ArtifactKey,
            how: Transfer,
            _: Duration,
        ) -> Result<Option<Presigned>> {
            Ok(Some(Presigned {
                method: if how == Transfer::Put { "PUT" } else { "GET" },
                url: format!("mem://{}", key.rel_path()),
                headers: vec![],
            }))
        }
    }

    fn trigger(trust: &str, git_ref: &str) -> RunTrigger {
        RunTrigger {
            kind: "ci".into(),
            principal: "ci".into(),
            trust: Some(trust.into()),
            forge_host: Some("github.com".into()),
            repo: Some("acme/widgets".into()),
            git_ref: Some(git_ref.into()),
            ..RunTrigger::default()
        }
    }

    fn cache(restore_from: Option<&str>) -> Vec<PathCacheSpec> {
        vec![PathCacheSpec {
            prefix: "cargo".into(),
            key_files: vec!["Cargo.lock".into()],
            paths: vec!["/usr/local/cargo/registry".into()],
            restore_from: restore_from.map(Into::into),
        }]
    }

    async fn pool() -> (db::Pool, std::path::PathBuf) {
        let path =
            std::env::temp_dir().join(format!("dagron-path-cache-{}.db", uuid::Uuid::new_v4()));
        (db::init_pool(path.to_str().unwrap()).await.unwrap(), path)
    }

    /// A task saves what its URLs point at, as `dagron-ci-cache` would.
    fn save(mem: &Mem, offered: &serde_json::Value, key: &str, bytes: &[u8]) {
        let rel = |url: &serde_json::Value| {
            url.as_str()
                .unwrap()
                .trim_start_matches("mem://")
                .to_string()
        };
        let save = &offered["caches"][0]["save"];
        let mut m = mem.0.lock().unwrap();
        m.insert(rel(&save["archive"]["url"]), bytes.to_vec());
        m.insert(
            rel(&save["manifest"]["url"]),
            serde_json::to_vec(&json!({"key": key, "size": bytes.len()})).unwrap(),
        );
    }

    fn asked<'a>(
        trigger: &'a RunTrigger,
        trusted: bool,
        task_id: &'a str,
        specs: &'a [PathCacheSpec],
    ) -> Asked<'a> {
        Asked {
            task_id,
            trigger: Some(trigger),
            trusted,
            caches: specs,
            timeout_secs: Some(600),
        }
    }

    #[tokio::test]
    async fn a_trusted_run_saves_once_its_task_succeeds_and_a_pull_request_reads_its_base() {
        let (pool, path) = pool().await;
        let mem = Arc::new(Mem::default());
        let caches = PathCaches::new(mem.clone(), Settings::default());
        let push = trigger("trusted", "refs/heads/main");
        let specs = &cache(None);
        // A push to main: nothing to restore, URLs to save.
        let offered: serde_json::Value = serde_json::from_str(
            &caches
                .offer(&pool, asked(&push, true, "t1", specs))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(offered["caches"][0]["restore"], json!([]));
        assert_eq!(offered["caches"][0]["save"]["archive"]["method"], "PUT");
        save(&mem, &offered, "cargo-aaa", b"archive one");
        // Its task's row is gone (no run in this database), so the sweep
        // reads it as ended and not succeeded: dropped, with its objects.
        let swept = caches.sweep(&pool).await.unwrap();
        assert_eq!(swept.dropped, 1);
        assert!(mem.0.lock().unwrap().is_empty(), "its objects are gone too");

        // Settled as a task that succeeded is.
        let offered: serde_json::Value = serde_json::from_str(
            &caches
                .offer(&pool, asked(&push, true, "t2", specs))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        save(&mem, &offered, "cargo-aaa", b"archive two");
        let pending = db::path_caches_settled(&pool, &chrono::Utc::now().to_rfc3339(), 10)
            .await
            .unwrap();
        assert!(caches
            .settle(&pool, &pending[0].id, &pending[0].object_key)
            .await
            .unwrap());

        // A pull request that is not trusted reads main's entry from its base,
        // and is given nothing to write with.
        let pr = trigger("untrusted", "refs/pull/7/merge");
        let pr_specs = &cache(Some("refs/heads/main"));
        let offered: serde_json::Value = serde_json::from_str(
            &caches
                .offer(&pool, asked(&pr, false, "t3", pr_specs))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let restore = &offered["caches"][0]["restore"];
        assert_eq!(restore.as_array().unwrap().len(), 1);
        assert_eq!(restore[0]["key"], "cargo-aaa");
        assert_eq!(restore[0]["from"], "refs/heads/main");
        assert!(offered["caches"][0]["save"].is_null());
        // Another repository's run reads nothing of acme/widgets.
        let mut elsewhere = trigger("trusted", "refs/heads/main");
        elsewhere.repo = Some("acme/other".into());
        let offered: serde_json::Value = serde_json::from_str(
            &caches
                .offer(&pool, asked(&elsewhere, true, "t4", specs))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(offered["caches"][0]["restore"], json!([]));
        // A run with no repository has no cache.
        let none = Asked {
            trigger: None,
            ..asked(&push, true, "t5", specs)
        };
        assert!(caches.offer(&pool, none).await.unwrap().is_none());
        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_version_is_of_the_paths_in_order() {
        let mut a = cache(None).remove(0);
        let v = version(&a);
        assert_eq!(v.len(), 16);
        a.key_files = vec!["other.lock".into()];
        assert_eq!(
            version(&a),
            v,
            "the key files make the key, not the version"
        );
        a.paths.push("/root/.cargo/git".into());
        assert_ne!(version(&a), v);
    }
}
