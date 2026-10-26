#![cfg(feature = "sqlite")]

use super::*;
use dagron_artifact::{ArtifactKey, ArtifactStore, Presigned};
use std::collections::HashMap as Map;

/// A store in memory that signs `mem://<path>` URLs, and deletes nothing
/// while it is told to fail.
#[derive(Default)]
struct Mem(Mutex<Map<String, Vec<u8>>>, std::sync::atomic::AtomicBool);

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
        if self.1.load(std::sync::atomic::Ordering::SeqCst) {
            bail!("the store is down");
        }
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

fn trigger(git_ref: &str) -> RunTrigger {
    RunTrigger {
        kind: "ci".into(),
        principal: "ci".into(),
        forge_host: Some("github.com".into()),
        repo: Some("acme/widgets".into()),
        git_ref: Some(git_ref.into()),
        ..RunTrigger::default()
    }
}

/// The service, answering at a port of its own, over a store in memory.
struct Up {
    svc: Arc<ActionsCache>,
    mem: Arc<Mem>,
    base: String,
    db: PathBuf,
    dir: PathBuf,
    http: reqwest::Client,
}

impl Up {
    async fn new(max_entry_bytes: u64) -> Self {
        Self::built(max_entry_bytes, |svc| svc).await
    }

    /// [`Up::new`], with the service as `made` makes it.
    async fn built(max_entry_bytes: u64, made: impl FnOnce(ActionsCache) -> ActionsCache) -> Self {
        let id = uuid::Uuid::new_v4();
        let db = std::env::temp_dir().join(format!("dagron-actions-cache-{id}.db"));
        let dir = std::env::temp_dir().join(format!("dagron-actions-cache-{id}"));
        let pool = db::init_pool(db.to_str().unwrap()).await.unwrap();
        let mem = Arc::new(Mem::default());
        let settings = path_cache::Settings {
            max_entry_bytes,
            ..path_cache::Settings::default()
        };
        let caches = Arc::new(PathCaches::new(mem.clone(), settings));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}/", listener.local_addr().unwrap());
        let artifacts = ArtifactSettings {
            retention_days: 90,
            max_bytes: max_entry_bytes,
        };
        let svc = Arc::new(made(
            ActionsCache::new(caches, pool, base.clone(), dir.clone(), artifacts).unwrap(),
        ));
        let served = router(Arc::clone(&svc));
        tokio::spawn(async move { axum::serve(listener, served).await });
        Self {
            svc,
            mem,
            base,
            db,
            dir,
            http: reqwest::Client::new(),
        }
    }

    /// What a task of a run of `git_ref` is given: of run `run-1`.
    fn token(&self, task: &str, git_ref: &str, base: Option<&str>, trusted: bool) -> String {
        self.token_of(task, "run-1", git_ref, base, trusted)
    }

    fn token_of(
        &self,
        task: &str,
        run: &str,
        git_ref: &str,
        base: Option<&str>,
        trusted: bool,
    ) -> String {
        let given = self
            .svc
            .offer(task, run, Some(&trigger(git_ref)), base, trusted, Some(600))
            .unwrap();
        let var = |name: &str| given.iter().find(|(n, _)| *n == name).unwrap().1.clone();
        assert_eq!(var("ACTIONS_CACHE_SERVICE_V2"), "true");
        assert_eq!(var("ACTIONS_RESULTS_URL"), self.base);
        var("ACTIONS_RUNTIME_TOKEN")
    }

    async fn call(&self, token: &str, method: &str, body: Value) -> (StatusCode, Value) {
        self.call_to(CACHE_SERVICE, token, method, body).await
    }

    async fn artifacts(&self, token: &str, method: &str, body: Value) -> (StatusCode, Value) {
        self.call_to(ARTIFACT_SERVICE, token, method, body).await
    }

    async fn call_to(
        &self,
        service: &str,
        token: &str,
        method: &str,
        body: Value,
    ) -> (StatusCode, Value) {
        let r = self
            .http
            .post(format!("{}twirp/{service}/{method}", self.base))
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = StatusCode::from_u16(r.status().as_u16()).unwrap();
        (status, r.json().await.unwrap())
    }

    async fn put(&self, url: &str, body: &[u8]) -> u16 {
        self.http
            .put(url)
            .header("x-ms-blob-type", "BlockBlob")
            .body(body.to_vec())
            .send()
            .await
            .unwrap()
            .status()
            .as_u16()
    }

    /// Save `bytes` under `key` as the action does: whole, or in two blocks.
    async fn save(&self, token: &str, key: &str, bytes: &[u8], in_blocks: bool) -> Value {
        let (_, created) = self
            .call(
                token,
                "CreateCacheEntry",
                json!({"key": key, "version": "v1"}),
            )
            .await;
        assert_eq!(created["ok"], true, "{created}");
        let url = created["signed_upload_url"].as_str().unwrap().to_string();
        if in_blocks {
            let (a, b) = bytes.split_at(bytes.len() / 2);
            assert_eq!(
                self.put(&format!("{url}&comp=block&blockid=QUFB"), a).await,
                201
            );
            assert_eq!(
                self.put(&format!("{url}&comp=block&blockid=QkJC"), b).await,
                201
            );
            let list = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\
                <BlockList><Latest>QUFB</Latest><Latest>QkJC</Latest></BlockList>";
            assert_eq!(
                self.put(&format!("{url}&comp=blocklist"), list.as_bytes())
                    .await,
                201
            );
        } else {
            assert_eq!(self.put(&url, bytes).await, 201);
        }
        let (_, done) = self
            .call(
                token,
                "FinalizeCacheEntryUpload",
                json!({"key": key, "version": "v1", "size_bytes": bytes.len().to_string()}),
            )
            .await;
        done
    }

    /// The bytes a restore's URL names, or `None` for a miss.
    async fn restore(
        &self,
        token: &str,
        key: &str,
        restore_keys: &[&str],
    ) -> Option<(String, Vec<u8>)> {
        let (_, found) = self
            .call(
                token,
                "GetCacheEntryDownloadURL",
                json!({"key": key, "version": "v1", "restore_keys": restore_keys}),
            )
            .await;
        if found["ok"] != true {
            return None;
        }
        let rel = found["signed_download_url"]
            .as_str()
            .unwrap()
            .trim_start_matches("mem://")
            .to_string();
        let bytes = self.mem.0.lock().unwrap().get(&rel).cloned().unwrap();
        Some((found["matched_key"].as_str().unwrap().to_string(), bytes))
    }
}

/// What a test made is removed however it ends: a failed assertion too.
impl Drop for Up {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.db);
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[tokio::test]
async fn a_trusted_run_saves_and_a_pull_request_restores_from_its_base() {
    let up = Up::new(1 << 20).await;
    let main = up.token("t1", "refs/heads/main", None, true);
    // Nothing yet: a miss.
    assert_eq!(up.restore(&main, "cargo-abc", &["cargo-"]).await, None);
    // Saved whole, then in blocks under another key.
    let done = up.save(&main, "cargo-abc", b"first archive", false).await;
    assert_eq!(done["ok"], true, "{done}");
    assert!(
        done["entry_id"].as_str().unwrap().parse::<u64>().is_ok(),
        "{done}"
    );
    let done = up
        .save(&main, "cargo-def", b"the second archive", true)
        .await;
    assert_eq!(done["ok"], true, "{done}");
    // The key itself, then the newest of a restore key's prefix.
    assert_eq!(
        up.restore(&main, "cargo-abc", &["cargo-"]).await,
        Some(("cargo-abc".into(), b"first archive".to_vec()))
    );
    assert_eq!(
        up.restore(&main, "cargo-zzz", &["npm-", "cargo-"]).await,
        Some(("cargo-def".into(), b"the second archive".to_vec()))
    );
    // A manifest is beside each archive, as a path cache's task writes it.
    let manifests = up
        .mem
        .0
        .lock()
        .unwrap()
        .keys()
        .filter(|k| k.ends_with(".json"))
        .count();
    assert_eq!(
        manifests,
        2,
        "{:?}",
        up.mem.0.lock().unwrap().keys().collect::<Vec<_>>()
    );

    // A pull request that is not trusted: it reads its base's entries, and
    // saves nothing.
    let pr = up.token("t2", "refs/pull/7/merge", Some("refs/heads/main"), false);
    assert_eq!(
        up.restore(&pr, "cargo-abc", &[]).await,
        Some(("cargo-abc".into(), b"first archive".to_vec()))
    );
    let (_, refused) = up
        .call(
            &pr,
            "CreateCacheEntry",
            json!({"key": "cargo-pr", "version": "v1"}),
        )
        .await;
    assert_eq!(refused["ok"], false, "{refused}");
    // Another branch's run reads none of main's: it names no base.
    let other = up.token("t3", "refs/heads/feature", None, true);
    assert_eq!(up.restore(&other, "cargo-abc", &["cargo-"]).await, None);
    // Nor does another version of the same key.
    let (_, other_version) = up
        .call(
            &main,
            "GetCacheEntryDownloadURL",
            json!({"key": "cargo-abc", "version": "v2"}),
        )
        .await;
    assert_eq!(other_version["ok"], false);

    // Saved again, the key's entry is the new one, and the old one's
    // objects are gone from the store.
    let before = up.mem.0.lock().unwrap().len();
    let done = up.save(&main, "cargo-abc", b"replaced", false).await;
    assert_eq!(done["ok"], true, "{done}");
    assert_eq!(up.mem.0.lock().unwrap().len(), before);
    assert_eq!(
        up.restore(&main, "cargo-abc", &[]).await,
        Some(("cargo-abc".into(), b"replaced".to_vec()))
    );
}

#[tokio::test]
async fn the_service_answers_only_its_own_credentials_and_upload_urls() {
    let up = Up::new(16).await;
    let main = up.token("t1", "refs/heads/main", None, true);
    // No credential, another engine's, or one altered: refused.
    let other = ActionsCache::new(
        Arc::clone(&up.svc.caches),
        up.svc.pool.clone(),
        up.base.clone(),
        up.dir.clone(),
        ArtifactSettings::default(),
    )
    .unwrap();
    let foreign = other
        .offer(
            "t1",
            "run-1",
            Some(&trigger("refs/heads/main")),
            None,
            true,
            None,
        )
        .unwrap()
        .into_iter()
        .find(|(n, _)| *n == "ACTIONS_RUNTIME_TOKEN")
        .unwrap()
        .1;
    let (signed, mac) = main.rsplit_once('.').unwrap();
    let (head, body) = signed.split_once('.').unwrap();
    let mut claims: Claims = serde_json::from_slice(&unb64url(body).unwrap()).unwrap();
    claims.s = vec!["github.com/acme/secret@refs/heads/main".into()];
    let altered = format!(
        "{head}.{}.{mac}",
        b64url(&serde_json::to_vec(&claims).unwrap())
    );
    for token in ["", "nonsense", foreign.as_str(), altered.as_str()] {
        let (status, err) = up
            .call(
                token,
                "GetCacheEntryDownloadURL",
                json!({"key": "k", "version": "v1"}),
            )
            .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{token}: {err}");
        assert_eq!(err["code"], "unauthenticated");
    }
    // A lapsed one too.
    claims = serde_json::from_slice(&unb64url(body).unwrap()).unwrap();
    claims.exp = chrono::Utc::now().timestamp() - 1;
    let (status, _) = up
        .call(
            &up.svc.token(&claims),
            "GetCacheEntryDownloadURL",
            json!({"key": "k", "version": "v1"}),
        )
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = up.call(&main, "DeleteCacheEntry", json!({})).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (_, created) = up
        .call(
            &main,
            "CreateCacheEntry",
            json!({"key": "k", "version": "v1"}),
        )
        .await;
    let url = created["signed_upload_url"].as_str().unwrap().to_string();
    // A second save of the key while the first is open is refused.
    let (_, twice) = up
        .call(
            &main,
            "CreateCacheEntry",
            json!({"key": "k", "version": "v1"}),
        )
        .await;
    assert_eq!(twice["ok"], false, "{twice}");
    // An upload URL altered is refused; and one larger than an entry may be.
    let forged = url.replace("sig=", "sig=0");
    assert_eq!(up.put(&forged, b"x").await, 403);
    assert_eq!(up.put(&url, &[0u8; 17]).await, 413);
    // A put refused, or broken off, leaves what was there before, and
    // nothing beside it.
    assert_eq!(up.put(&url, b"0123456789").await, 201);
    assert_eq!(up.put(&url, &[0u8; 17]).await, 413);
    let id = url
        .split("/blob/")
        .nth(1)
        .unwrap()
        .split('?')
        .next()
        .unwrap();
    let held = up.dir.join(id);
    assert_eq!(std::fs::read(held.join("whole")).unwrap(), b"0123456789");
    assert_eq!(std::fs::read_dir(&held).unwrap().count(), 1);
    // Finalized at a size it is not: the upload is dropped.
    let (_, wrong) = up
        .call(
            &main,
            "FinalizeCacheEntryUpload",
            json!({"key": "k", "version": "v1", "size_bytes": "11"}),
        )
        .await;
    assert_eq!(wrong["ok"], false, "{wrong}");
    assert_eq!(up.put(&url, b"0123456789").await, 404);
    // A key of control characters, or too long, is no key.
    let (_, bad) = up
        .call(
            &main,
            "CreateCacheEntry",
            json!({"key": "a\nb", "version": "v1"}),
        )
        .await;
    assert_eq!(bad["ok"], false);
    let (_, bad) = up
        .call(
            &main,
            "CreateCacheEntry",
            json!({"key": "k".repeat(513), "version": "v1"}),
        )
        .await;
    assert_eq!(bad["ok"], false);
}

#[tokio::test]
async fn two_saves_of_a_key_at_once_are_one_save() {
    let up = Up::new(1 << 20).await;
    let (a, b) = (
        up.token("t1", "refs/heads/main", None, true),
        up.token("t2", "refs/heads/main", None, true),
    );
    let ask = json!({"key": "k", "version": "v1"});
    let ((_, first), (_, second)) = tokio::join!(
        up.call(&a, "CreateCacheEntry", ask.clone()),
        up.call(&b, "CreateCacheEntry", ask.clone()),
    );
    let mut oks = [&first["ok"], &second["ok"]].map(|ok| ok == true);
    oks.sort();
    assert_eq!(oks, [false, true], "{first} {second}");
    assert_eq!(up.svc.open.lock().unwrap().len(), 1);
}

#[test]
fn the_service_is_on_with_both_settings_and_over_https() {
    assert_eq!(settings(None, None, None).unwrap(), None);
    assert_eq!(settings(Some(" "), Some(""), None).unwrap(), None);
    let (addr, url) = settings(Some("0.0.0.0:8090"), Some("https://cache.example"), None)
        .unwrap()
        .unwrap();
    assert_eq!(addr.port(), 8090);
    assert_eq!(url, "https://cache.example/");
    assert_eq!(
        settings(
            Some("0.0.0.0:8090"),
            Some("HTTP://Engine:8090"),
            Some("allow")
        )
        .unwrap()
        .unwrap()
        .1,
        "http://engine:8090/"
    );
    for (listen, url, insecure, why) in [
        (
            Some("0.0.0.0:8090"),
            None,
            None,
            "without DAGRON_ACTIONS_CACHE_URL",
        ),
        (
            None,
            Some("https://c"),
            None,
            "without DAGRON_ACTIONS_CACHE_LISTEN",
        ),
        (Some("8090"), Some("https://c"), None, "no address"),
        (
            Some("0.0.0.0:8090"),
            Some("http://engine:8090"),
            None,
            "is plain http",
        ),
        (Some("0.0.0.0:8090"), Some("ftp://c"), None, "is ftp://"),
        (
            Some("0.0.0.0:8090"),
            Some("https://u:p@c"),
            None,
            "carries a user",
        ),
        (
            Some("0.0.0.0:8090"),
            Some("https://c/?a=b"),
            None,
            "no query",
        ),
        (
            Some("0.0.0.0:8090"),
            Some("https://c"),
            Some("yes"),
            "is allow, or unset",
        ),
    ] {
        let err = settings(listen, url, insecure).unwrap_err().to_string();
        assert!(err.contains(why), "{listen:?} {url:?} {insecure:?}: {err}");
    }
}

#[test]
fn a_block_list_is_read_in_its_order() {
    let list = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<BlockList>\n  \
        <Latest>QUFB</Latest>\n  <Committed> QkJC </Committed>\n  <Uncommitted>Q0ND</Uncommitted>\n\
        </BlockList>";
    assert_eq!(block_ids(list), ["QUFB", "QkJC", "Q0ND"]);
    assert!(block_ids("<BlockList></BlockList>").is_empty());
    assert!(block_ids("<BlockList><Latest>QUFB").is_empty());
}

#[test]
fn hmac_is_rfc_4231s() {
    // Test case 2: the key "Jefe", padded with zeros as any short key is.
    let mut key = [0u8; 32];
    key[..4].copy_from_slice(b"Jefe");
    assert_eq!(
        hex(&hmac(&key, b"what do ya want for nothing?")),
        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
    );

    assert!(same("abc", "abc") && !same("abc", "abd") && !same("abc", "ab"));
}

#[test]
fn base64url_is_rfc_4648s_unpadded() {
    for (bytes, text) in [
        (&b""[..], ""),
        (b"f", "Zg"),
        (b"fo", "Zm8"),
        (b"foo", "Zm9v"),
        (b"foobar", "Zm9vYmFy"),
        (&[0xfb, 0xff][..], "-_8"),
    ] {
        assert_eq!(b64url(bytes), text);
        assert_eq!(unb64url(text).unwrap(), bytes);
    }
    let all: Vec<u8> = (0..=255).collect();
    assert_eq!(unb64url(&b64url(&all)).unwrap(), all);
    assert_eq!(unb64url("Zg=="), None);
    assert_eq!(unb64url("Zh"), None, "bits past the last byte are zero");
}

#[tokio::test]
async fn the_credential_is_a_jwt_whose_scp_names_the_run_and_the_job() {
    // As @actions/artifact reads it: the second part, base64url JSON, and in
    // its `scp` the scope `Actions.Results:<run>:<job>`.
    let up = Up::new(16).await;
    let token = up.token_of("t9", "run-9", "refs/heads/main", None, false);
    let parts: Vec<&str> = token.split('.').collect();
    assert_eq!(parts.len(), 3);
    let head: Value = serde_json::from_slice(&unb64url(parts[0]).unwrap()).unwrap();
    assert_eq!(head, json!({"alg": "HS256", "typ": "JWT"}));
    let body: Value = serde_json::from_slice(&unb64url(parts[1]).unwrap()).unwrap();
    assert_eq!(body["scp"], "Actions.Results:run-9:t9");
    assert!(body["exp"].as_i64().unwrap() > chrono::Utc::now().timestamp());
}

/// A zip's bytes, uploaded in two blocks as the action uploads them, and
/// finalized with their hash.
async fn upload(up: &Up, token: &str, name: &str, bytes: &[u8]) -> (StatusCode, Value) {
    let (status, created) = up
        .artifacts(
            token,
            "CreateArtifact",
            json!({
                "workflow_run_backend_id": "",
                "workflow_job_run_backend_id": "",
                "name": name,
                "version": 4,
            }),
        )
        .await;
    if created["ok"] != true {
        return (status, created);
    }
    let url = created["signed_upload_url"].as_str().unwrap().to_string();
    let (a, b) = bytes.split_at(bytes.len() / 2);
    assert_eq!(
        up.put(&format!("{url}&comp=block&blockid=QUFB"), a).await,
        201
    );
    assert_eq!(
        up.put(&format!("{url}&comp=block&blockid=QkJC"), b).await,
        201
    );
    let list = "<BlockList><Latest>QUFB</Latest><Latest>QkJC</Latest></BlockList>";
    assert_eq!(
        up.put(&format!("{url}&comp=blocklist"), list.as_bytes())
            .await,
        201
    );
    let hash = format!("sha256:{}", hex(&Sha256::digest(bytes)));
    up.artifacts(
        token,
        "FinalizeArtifact",
        json!({"name": name, "size": bytes.len().to_string(), "hash": hash}),
    )
    .await
}

#[tokio::test]
async fn a_runs_jobs_upload_list_download_and_delete_its_artifacts() {
    let up = Up::new(1 << 20).await;
    let zip: &[u8] = b"PK the bytes of a zip";
    // A run that is not trusted uploads too: to its own run.
    let build = up.token_of("build", "run-1", "refs/pull/7/merge", None, false);
    let (status, done) = upload(&up, &build, "dist", zip).await;
    assert_eq!(status, StatusCode::OK, "{done}");
    assert_eq!(done["ok"], true, "{done}");
    let number = done["artifact_id"].as_str().unwrap().to_string();
    assert!(number.parse::<i64>().unwrap() < 1 << 53);

    // Another job of the run lists it, by name or by number, with its digest.
    let test = up.token_of("test", "run-1", "refs/pull/7/merge", None, false);
    let digest = format!("sha256:{}", hex(&Sha256::digest(zip)));
    for filter in [
        json!({}),
        json!({"name_filter": "dist"}),
        json!({"id_filter": number}),
    ] {
        let (_, listed) = up.artifacts(&test, "ListArtifacts", filter.clone()).await;
        let artifacts = listed["artifacts"].as_array().unwrap();
        assert_eq!(artifacts.len(), 1, "{filter}: {listed}");
        let a = &artifacts[0];
        assert_eq!(a["workflow_run_backend_id"], "run-1");
        assert_eq!(a["workflow_job_run_backend_id"], "build");
        assert_eq!(a["name"], "dist");
        assert_eq!(a["size"], zip.len().to_string());
        assert_eq!(a["database_id"], json!(number));
        assert_eq!(a["digest"], json!(digest));
        let at = a["created_at"].as_str().unwrap();
        assert!(
            at.ends_with('Z') && at.len() == 24,
            "as protobuf writes a time: {at}"
        );
    }
    let (_, none) = up
        .artifacts(&test, "ListArtifacts", json!({"name_filter": "other"}))
        .await;
    assert_eq!(none["artifacts"], json!([]));
    // And downloads it from the store, at a URL the store signed.
    let (_, signed) = up
        .artifacts(
            &test,
            "GetSignedArtifactURL",
            json!({"workflow_run_backend_id": "run-1", "name": "dist"}),
        )
        .await;
    let rel = signed["signed_url"]
        .as_str()
        .unwrap()
        .trim_start_matches("mem://")
        .to_string();
    assert!(rel.starts_with("workflow-artifacts/run-1/"), "{rel}");
    assert_eq!(up.mem.0.lock().unwrap().get(&rel).unwrap(), zip);

    // A job of another run sees none of it, and may not name the run.
    let stranger = up.token_of("x", "run-2", "refs/heads/main", None, true);
    let (_, theirs) = up.artifacts(&stranger, "ListArtifacts", json!({})).await;
    assert_eq!(theirs["artifacts"], json!([]));
    let (status, _) = up
        .artifacts(&stranger, "GetSignedArtifactURL", json!({"name": "dist"}))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, err) = up
        .artifacts(
            &stranger,
            "ListArtifacts",
            json!({"workflow_run_backend_id": "run-1"}),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(err["code"], "permission_denied");

    // A name already in the run is refused; deleted, it may be uploaded again.
    let (status, err) = upload(&up, &test, "dist", b"again").await;
    assert_eq!(status, StatusCode::CONFLICT, "{err}");
    assert_eq!(err["code"], "already_exists");
    // Where the store does not delete its bytes, the delete fails, and the
    // artifact is kept, to be deleted again.
    up.mem.1.store(true, std::sync::atomic::Ordering::SeqCst);
    let (status, _) = up
        .artifacts(&test, "DeleteArtifact", json!({"name": "dist"}))
        .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    up.mem.1.store(false, std::sync::atomic::Ordering::SeqCst);
    let (_, still) = up
        .artifacts(&test, "ListArtifacts", json!({"name_filter": "dist"}))
        .await;
    assert_eq!(still["artifacts"].as_array().unwrap().len(), 1);
    let (_, deleted) = up
        .artifacts(&test, "DeleteArtifact", json!({"name": "dist"}))
        .await;
    assert_eq!(deleted, json!({"ok": true, "artifact_id": number}));
    assert!(
        up.mem.0.lock().unwrap().get(&rel).is_none(),
        "its bytes went with it"
    );
    let (_, done) = upload(&up, &test, "dist", b"again, anew").await;
    assert_eq!(done["ok"], true, "{done}");
}

#[tokio::test]
async fn an_upload_is_what_the_job_says_and_what_the_service_takes() {
    let up = Up::new(1 << 20).await;
    let job = up.token_of("build", "run-1", "refs/heads/main", None, true);
    // Version 4 alone; a name GitHub would take; a time to keep it to come.
    for (body, why) in [
        (json!({"name": "a", "version": 3}), "version 4"),
        (json!({"name": "a/b", "version": 4}), "none of"),
        (json!({"name": "", "version": 4}), "1 to 512"),
        (
            json!({"name": "a", "version": 4, "expires_at": "2000-01-01T00:00:00Z"}),
            "has passed",
        ),
    ] {
        let (status, err) = up.artifacts(&job, "CreateArtifact", body.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(err["msg"].as_str().unwrap().contains(why), "{body}: {err}");
    }
    // A hash that is not the bytes': refused, and the upload dropped.
    let (_, created) = up
        .artifacts(&job, "CreateArtifact", json!({"name": "a", "version": 4}))
        .await;
    let url = created["signed_upload_url"].as_str().unwrap().to_string();
    assert_eq!(up.put(&url, b"zip").await, 201);
    let (status, err) = up
        .artifacts(
            &job,
            "FinalizeArtifact",
            json!({"name": "a", "size": "3", "hash": "sha256:00"}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
    assert_eq!(up.put(&url, b"zip").await, 404);
    let (_, listed) = up.artifacts(&job, "ListArtifacts", json!({})).await;
    assert_eq!(listed["artifacts"], json!([]));
    // One larger than the most an artifact may be is refused as it arrives.
    let (_, created) = up
        .artifacts(&job, "CreateArtifact", json!({"name": "big", "version": 4}))
        .await;
    let url = created["signed_upload_url"].as_str().unwrap().to_string();
    assert_eq!(up.put(&url, &vec![0u8; (1 << 20) + 1]).await, 413);
    // Kept as long as the job asks, and no longer than the operator's most.
    let soon = chrono::Utc::now() + chrono::TimeDelta::days(1);
    let far = chrono::Utc::now() + chrono::TimeDelta::days(400);
    for (name, asked, at_most) in [("brief", soon, soon), ("long", far, far)] {
        let (_, created) = up
            .artifacts(
                &job,
                "CreateArtifact",
                json!({"name": name, "version": 4, "expires_at": asked.to_rfc3339()}),
            )
            .await;
        assert_eq!(created["ok"], true, "{created}");
        let kept = up
            .svc
            .open
            .lock()
            .unwrap()
            .values()
            .find_map(|o| match &o.what {
                What::Artifact {
                    name: n,
                    expires_at,
                    ..
                } if n == name => Some(expires_at.clone()),
                _ => None,
            })
            .unwrap();
        let kept = chrono::DateTime::parse_from_rfc3339(&kept).unwrap();
        let most = chrono::Utc::now() + chrono::TimeDelta::days(90);
        assert!(
            kept <= at_most.min(most) + chrono::TimeDelta::seconds(5),
            "{name}: {kept}"
        );
        assert!(
            kept >= at_most.min(most) - chrono::TimeDelta::seconds(5),
            "{name}: {kept}"
        );
    }
    // Finalized at a size it is not: refused, and dropped.
    let (_, created) = up
        .artifacts(
            &job,
            "CreateArtifact",
            json!({"name": "short", "version": 4}),
        )
        .await;
    let url = created["signed_upload_url"].as_str().unwrap().to_string();
    assert_eq!(up.put(&url, b"zip").await, 201);
    let (status, err) = up
        .artifacts(
            &job,
            "FinalizeArtifact",
            json!({"name": "short", "size": "4"}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
    assert_eq!(err["code"], "invalid_argument");
    assert_eq!(up.put(&url, b"zip").await, 404);
    // The sweep deletes what has expired, with its bytes; and while the
    // store does not delete them, keeps the row, to try again.
    let (_, done) = upload(&up, &job, "kept", b"bytes").await;
    assert_eq!(done["ok"], true);
    expire(&up, "kept").await;
    up.mem.1.store(true, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(artifacts::sweep(&up.svc).await.unwrap(), 0);
    up.mem.1.store(false, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(artifacts::sweep(&up.svc).await.unwrap(), 1);
    let left: Vec<String> = up.mem.0.lock().unwrap().keys().cloned().collect();
    assert!(
        !left.iter().any(|k| k.starts_with("workflow-artifacts/")),
        "{left:?}"
    );
}

/// Make the run's artifact of `name` one that has expired.
async fn expire(up: &Up, name: &str) {
    sqlx::query(
        "UPDATE workflow_artifacts SET expires_at = '2000-01-01T00:00:00+00:00' WHERE name = ?",
    )
    .bind(name)
    .execute(&up.svc.pool)
    .await
    .unwrap();
}

#[test]
fn the_url_is_a_hosts_root_and_artifacts_are_kept_as_the_operator_says() {
    // The artifact actions keep the URL's origin alone: a path would be lost.
    let err = settings(Some("0.0.0.0:8090"), Some("https://c/cache"), None)
        .unwrap_err()
        .to_string();
    assert!(err.contains("has a path"), "{err}");
    assert_eq!(
        artifact_settings(None, None).unwrap(),
        ArtifactSettings::default()
    );
    assert_eq!(
        artifact_settings(Some("30"), Some("1024")).unwrap(),
        ArtifactSettings {
            retention_days: 30,
            max_bytes: 1024
        }
    );
    for (days, bytes, why) in [
        (Some("0"), None, "from 1 to 3650"),
        (Some("3651"), None, "from 1 to 3650"),
        (None, Some("0"), "a number of bytes"),
        (None, Some("lots"), "a number of bytes"),
    ] {
        let err = artifact_settings(days, bytes).unwrap_err().to_string();
        assert!(err.contains(why), "{days:?} {bytes:?}: {err}");
    }
}

/// An executor that runs each step's container at once, as `said` says, and
/// keeps what it was asked; or, where `held`, one that never ends.
#[derive(Default)]
struct Steps {
    asked: Mutex<Vec<(String, dagron_executor::executor::StepContainer)>>,
    built: Mutex<Vec<(String, dagron_executor::executor::StepBuild)>>,
    held: bool,
}

#[async_trait::async_trait]
impl dagron_executor::executor::Executor for Steps {
    async fn execute(
        &self,
        _: &dagron_executor::executor::ExecContext,
    ) -> Result<dagron_executor::executor::ExecOutput> {
        bail!("no task runs here")
    }

    async fn run_step_container(
        &self,
        task_id: &str,
        step: dagron_executor::executor::StepContainer,
    ) -> Result<dagron_executor::executor::StepOutcome> {
        self.asked
            .lock()
            .unwrap()
            .push((task_id.to_string(), step.clone()));
        if self.held {
            std::future::pending::<()>().await;
        }
        if step.image == "broken:1" {
            bail!("the image could not be pulled");
        }
        Ok(dagron_executor::executor::StepOutcome {
            exit_code: 3,
            output: format!("ran {}\n", step.args.join(" ")),
            timed_out: false,
        })
    }

    async fn build_step_image(
        &self,
        task_id: &str,
        build: dagron_executor::executor::StepBuild,
    ) -> Result<dagron_executor::executor::StepBuilt> {
        self.built
            .lock()
            .unwrap()
            .push((task_id.to_string(), build.clone()));
        if build.dockerfile == "broken/Dockerfile" {
            bail!("the build failed at its second step");
        }
        Ok(dagron_executor::executor::StepBuilt {
            output: format!("built {}\n", build.image),
            image: build.image,
        })
    }
}

impl Up {
    /// The service, running steps' containers on `steps`, held to `images`
    /// for a run that is not trusted.
    async fn with_steps(steps: Arc<Steps>, images: &[&str]) -> Up {
        let images: Vec<String> = images.iter().map(|i| i.to_string()).collect();
        Up::built(1 << 20, move |svc| svc.with_steps(steps, images)).await
    }

    async fn step(
        &self,
        token: &str,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut asked = self
            .http
            .request(method, format!("{}dagron/v1/steps{path}", self.base))
            .bearer_auth(token);
        if let Some(body) = body {
            asked = asked.json(&body);
        }
        let r = asked.send().await.unwrap();
        let status = StatusCode::from_u16(r.status().as_u16()).unwrap();
        (status, r.json().await.unwrap_or(Value::Null))
    }
}

#[tokio::test]
async fn a_jobs_step_runs_in_its_container_and_the_job_has_its_end() {
    let steps = Arc::new(Steps::default());
    let up = Up::with_steps(Arc::clone(&steps), &[]).await;
    let job = up.token_of("t1", "run-1", "refs/heads/main", None, true);
    let (status, started) = up
        .step(
            &job,
            reqwest::Method::POST,
            "",
            Some(json!({
                "image": "alpine:3.20",
                "entrypoint": "/bin/sh",
                "args": ["-c", "echo hi"],
                "env": [["INPUT_WHO", "rig"]],
                "timeout_secs": 60,
            })),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{started}");
    let id = started["id"].as_str().unwrap().to_string();
    // Another job does not see it.
    let other = up.token_of("t2", "run-1", "refs/heads/main", None, true);
    let (status, _) = up
        .step(&other, reqwest::Method::GET, &format!("/{id}?wait=1"), None)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (_, ended) = up
        .step(&job, reqwest::Method::GET, &format!("/{id}?wait=5"), None)
        .await;
    assert_eq!(
        ended,
        json!({"done": true, "exit_code": 3, "timed_out": false, "output": "ran -c echo hi\n"})
    );
    // Its end is had once.
    let (status, _) = up
        .step(&job, reqwest::Method::GET, &format!("/{id}"), None)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let asked = steps.asked.lock().unwrap();
    let (task, step) = &asked[0];
    assert_eq!(task, "t1");
    assert_eq!(step.image, "alpine:3.20");
    assert_eq!(step.entrypoint.as_deref(), Some("/bin/sh"));
    assert_eq!(step.env, [("INPUT_WHO".to_string(), "rig".to_string())]);
    assert_eq!(step.timeout, Duration::from_secs(60));
}

#[tokio::test]
async fn a_step_the_executor_cannot_run_ends_with_why_and_bad_asks_are_refused() {
    let steps = Arc::new(Steps::default());
    let up = Up::with_steps(Arc::clone(&steps), &["docker.io/library/alpine:*"]).await;
    let job = up.token_of("t1", "run-1", "refs/heads/main", None, true);
    let (_, started) = up
        .step(
            &job,
            reqwest::Method::POST,
            "",
            Some(json!({"image": "broken:1"})),
        )
        .await;
    let id = started["id"].as_str().unwrap();
    let (_, ended) = up
        .step(&job, reqwest::Method::GET, &format!("/{id}?wait=5"), None)
        .await;
    assert_eq!(ended["done"], true);
    assert!(
        ended["error"]
            .as_str()
            .unwrap()
            .contains("could not be pulled"),
        "{ended}"
    );
    // No credential; no image; a variable that is none.
    let (status, _) = up
        .step(
            "nonsense",
            reqwest::Method::POST,
            "",
            Some(json!({"image": "a:1"})),
        )
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    for body in [
        json!({"image": ""}),
        json!({"image": "a b"}),
        json!({"image": "a:1", "env": [["1BAD", "x"]]}),
    ] {
        let (status, err) = up
            .step(&job, reqwest::Method::POST, "", Some(body.clone()))
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {err}");
    }
    // A run that is not trusted names an image of those listed; a trusted
    // one names any.
    let fork = up.token_of("t3", "run-2", "refs/pull/7/merge", None, false);
    let (status, err) = up
        .step(
            &fork,
            reqwest::Method::POST,
            "",
            Some(json!({"image": "ghcr.io/x/y:1"})),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(
        err["error"]
            .as_str()
            .unwrap()
            .contains("DAGRON_UNTRUSTED_IMAGES"),
        "{err}"
    );
    let (status, _) = up
        .step(
            &fork,
            reqwest::Method::POST,
            "",
            Some(json!({"image": "docker.io/library/alpine:3.20"})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
}

/// A trusted run's step has its image built from what it sends, named for
/// a digest of that, and asked after as a step's run is; a run that is not
/// trusted has none built, and a Dockerfile out of what is sent is none.
#[tokio::test]
async fn a_trusted_run_s_step_has_its_image_built_and_named_for_what_it_is() {
    let steps = Arc::new(Steps::default());
    let up = Up::with_steps(Arc::clone(&steps), &["docker.io/library/alpine:*"]).await;
    let job = up.token_of("t1", "run-1", "refs/heads/main", None, true);
    let build = |token: &str, query: &str, body: &'static [u8]| {
        let asked = up
            .http
            .post(format!("{}dagron/v1/steps/images{query}", up.base))
            .bearer_auth(token)
            .body(body);
        async move {
            let r = asked.send().await.unwrap();
            let status = StatusCode::from_u16(r.status().as_u16()).unwrap();
            (status, r.json::<Value>().await.unwrap_or(Value::Null))
        }
    };
    let (status, started) = build(&job, "?dockerfile=Dockerfile&timeout_secs=60", b"a tar").await;
    assert_eq!(status, StatusCode::CREATED, "{started}");
    let id = started["id"].as_str().unwrap().to_string();
    let (_, ended) = up
        .step(&job, reqwest::Method::GET, &format!("/{id}?wait=5"), None)
        .await;
    let named = super::steps::built_name("Dockerfile", b"a tar");
    assert!(named.starts_with("dagron-ci-action:"), "{named}");
    assert_eq!(
        ended,
        json!({"done": true, "image": named, "output": format!("built {named}\n")})
    );
    {
        let built = steps.built.lock().unwrap();
        let (task, asked) = &built[0];
        assert_eq!(task, "t1");
        assert_eq!(asked.context, b"a tar");
        assert_eq!(asked.timeout, Duration::from_secs(60));
    }
    // What it is built from names it: another Dockerfile, or other files,
    // is another image.
    assert_ne!(
        super::steps::built_name("other/Dockerfile", b"a tar"),
        named
    );
    assert_ne!(super::steps::built_name("Dockerfile", b"a tar!"), named);

    // A build that fails ends with why.
    let (_, started) = build(&job, "?dockerfile=broken/Dockerfile", b"a tar").await;
    let id = started["id"].as_str().unwrap().to_string();
    let (_, ended) = up
        .step(&job, reqwest::Method::GET, &format!("/{id}?wait=5"), None)
        .await;
    assert!(
        ended["error"].as_str().unwrap().contains("second step"),
        "{ended}"
    );

    // Not trusted; a Dockerfile that is no path of what is sent; nothing.
    let fork = up.token_of("t3", "run-2", "refs/pull/7/merge", None, false);
    let (status, err) = build(&fork, "", b"a tar").await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(
        err["error"].as_str().unwrap().contains("not trusted"),
        "{err}"
    );
    for query in [
        "?dockerfile=/Dockerfile",
        "?dockerfile=../Dockerfile",
        "?dockerfile=a/../../b",
    ] {
        let (status, err) = build(&job, query, b"a tar").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {err}");
    }
    let (status, _) = build(&job, "", b"").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        steps.built.lock().unwrap().len(),
        2,
        "nothing refused was built"
    );
}

/// An engine whose executor builds no image says so, as the step's end.
#[tokio::test]
async fn a_step_s_image_an_executor_does_not_build_ends_with_why() {
    struct Plain;
    #[async_trait::async_trait]
    impl dagron_executor::executor::Executor for Plain {
        async fn execute(
            &self,
            _: &dagron_executor::executor::ExecContext,
        ) -> Result<dagron_executor::executor::ExecOutput> {
            bail!("no task runs here")
        }
    }
    let up = Up::built(1 << 20, |svc| svc.with_steps(Arc::new(Plain), Vec::new())).await;
    let job = up.token_of("t1", "run-1", "refs/heads/main", None, true);
    let r = up
        .http
        .post(format!("{}dagron/v1/steps/images", up.base))
        .bearer_auth(&job)
        .body(&b"a tar"[..])
        .send()
        .await
        .unwrap();
    let started: Value = r.json().await.unwrap();
    let id = started["id"].as_str().unwrap().to_string();
    let (_, ended) = up
        .step(&job, reqwest::Method::GET, &format!("/{id}?wait=5"), None)
        .await;
    assert!(
        ended["error"]
            .as_str()
            .unwrap()
            .contains("DAGRON_DOCKER_BUILD_ACTIONS"),
        "{ended}"
    );
}

#[tokio::test]
async fn a_job_runs_two_steps_containers_at_once_at_most() {
    let steps = Arc::new(Steps {
        held: true,
        ..Steps::default()
    });
    let up = Up::with_steps(Arc::clone(&steps), &[]).await;
    let job = up.token_of("t1", "run-1", "refs/heads/main", None, true);
    let ask = || json!({"image": "alpine:3.20"});
    let (_, a) = up.step(&job, reqwest::Method::POST, "", Some(ask())).await;
    let (_, _) = up.step(&job, reqwest::Method::POST, "", Some(ask())).await;
    let (status, _) = up.step(&job, reqwest::Method::POST, "", Some(ask())).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    // One that has not ended says so; let go of, it leaves room.
    let id = a["id"].as_str().unwrap();
    let (_, polled) = up
        .step(&job, reqwest::Method::GET, &format!("/{id}?wait=0"), None)
        .await;
    assert_eq!(polled, json!({"done": false}));
    let (status, _) = up
        .step(&job, reqwest::Method::DELETE, &format!("/{id}"), None)
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    // Let go of, it is asked after no more; and while its container runs it
    // still counts: a job starts no more by letting go of them.
    let (status, _) = up
        .step(&job, reqwest::Method::GET, &format!("/{id}"), None)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = up.step(&job, reqwest::Method::POST, "", Some(ask())).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    // Another job has its own two.
    let other = up.token_of("t2", "run-1", "refs/heads/main", None, true);
    let (status, _) = up
        .step(&other, reqwest::Method::POST, "", Some(ask()))
        .await;
    assert_eq!(status, StatusCode::CREATED);
}

#[tokio::test]
async fn a_step_let_go_of_that_has_ended_leaves_room() {
    let steps = Arc::new(Steps::default());
    let up = Up::with_steps(Arc::clone(&steps), &[]).await;
    let job = up.token_of("t1", "run-1", "refs/heads/main", None, true);
    let ask = || json!({"image": "alpine:3.20"});
    for _ in 0..4 {
        let (status, started) = up.step(&job, reqwest::Method::POST, "", Some(ask())).await;
        assert_eq!(status, StatusCode::CREATED, "{started}");
        let id = started["id"].as_str().unwrap().to_string();
        // It has ended (this executor ends each at once): let go of, it is
        // gone, and the next has room.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (status, _) = up
            .step(&job, reqwest::Method::DELETE, &format!("/{id}"), None)
            .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }
    assert!(up.svc.steps.lock().unwrap().is_empty());
}

#[tokio::test]
async fn an_engine_whose_executor_runs_no_step_says_so() {
    let up = Up::new(16).await;
    let job = up.token_of("t1", "run-1", "refs/heads/main", None, true);
    let (status, err) = up
        .step(
            &job,
            reqwest::Method::POST,
            "",
            Some(json!({"image": "alpine:3.20"})),
        )
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{err}");
}

/// The step runner's own client of the service, as a GitLab job's
/// artifacts are sent: a zip it packs, uploaded in blocks, finalized with
/// its digest, and what the store then holds unpacks to the job's files.
/// Two attempts of one job: the second's stands in the first's place.
#[tokio::test]
async fn the_step_runner_s_artifacts_go_up_as_upload_artifact_s_do() {
    let up = Up::new(1 << 20).await;
    let token = up.token_of("build", "run-1", "refs/heads/main", None, true);
    let work = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(work.path().join("out/lib")).unwrap();
    std::fs::write(work.path().join("out/app"), "the app".repeat(1000)).unwrap();
    std::fs::write(work.path().join("out/lib/a.so"), "a library").unwrap();
    let files = dagron_ci_exec::artifacts::selected(work.path(), &["out/".into()], &[]).unwrap();
    let elsewhere = tempfile::tempdir().unwrap();
    let zip = elsewhere.path().join("artifact-test.zip");
    dagron_ci_exec::artifacts::pack(work.path(), &files, &zip).unwrap();
    let base = up.base.clone();
    for _ in 0..2 {
        let (base, token, zip) = (base.clone(), token.clone(), zip.clone());
        tokio::task::spawn_blocking(move || {
            dagron_ci_exec::artifacts::Service::new(&base, &token)
                .upload("build", &zip, Some(7))
                .unwrap()
        })
        .await
        .unwrap();
    }
    let (_, listed) = up
        .artifacts(&token, "ListArtifacts", json!({"name_filter": "build"}))
        .await;
    let artifacts = listed["artifacts"].as_array().unwrap();
    assert_eq!(artifacts.len(), 1, "{listed}");
    let sent = std::fs::read(&zip).unwrap();
    assert_eq!(
        artifacts[0]["digest"],
        json!(format!("sha256:{}", hex(&Sha256::digest(&sent))))
    );
    let (_, signed) = up
        .artifacts(&token, "GetSignedArtifactURL", json!({"name": "build"}))
        .await;
    let rel = signed["signed_url"]
        .as_str()
        .unwrap()
        .trim_start_matches("mem://")
        .to_string();
    let stored = up.mem.0.lock().unwrap().get(&rel).unwrap().clone();
    assert_eq!(stored, sent);
    let into = tempfile::tempdir().unwrap();
    let got = into.path().join("got.zip");
    std::fs::write(&got, &stored).unwrap();
    let unpacked = tempfile::tempdir().unwrap();
    assert_eq!(
        dagron_ci_exec::artifacts::unpack(&got, unpacked.path()).unwrap(),
        2
    );
    assert_eq!(
        std::fs::read_to_string(unpacked.path().join("out/app")).unwrap(),
        "the app".repeat(1000)
    );
}

/// An upload an attempt left open (it was cut short) does not keep the name
/// from the job's next attempt: the new one takes its place, and the old
/// one's finalize finds nothing open.
#[tokio::test]
async fn an_upload_left_open_gives_way_to_the_next_attempt_s() {
    let up = Up::new(1 << 20).await;
    let first = up.token_of("build", "run-1", "refs/heads/main", None, true);
    let (status, created) = up
        .artifacts(
            &first,
            "CreateArtifact",
            json!({"name": "dist", "version": 4}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let second = up.token_of("build-retry", "run-1", "refs/heads/main", None, true);
    let (status, done) = upload(&up, &second, "dist", b"PK the second attempt's").await;
    assert_eq!(status, StatusCode::OK, "{done}");
    let (status, err) = up
        .artifacts(
            &first,
            "FinalizeArtifact",
            json!({"name": "dist", "size": "0"}),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{err}");
    let (_, listed) = up
        .artifacts(&second, "ListArtifacts", json!({"name_filter": "dist"}))
        .await;
    assert_eq!(listed["artifacts"].as_array().unwrap().len(), 1, "{listed}");
}

/// A run of one task, `build`, started for a commit: its id and the task's.
async fn a_run_at_a_commit(up: &Up) -> (String, String) {
    let yaml = "name: ci\ntasks:\n  - name: build\n    command: [\"true\"]\n";
    let dag = dagron_core::dag::DagGraph::from_yaml(yaml).unwrap();
    let mut at = trigger("refs/heads/main");
    at.sha = Some("0123456789abcdef0123456789abcdef01234567".into());
    let run = db::create_run_triggered(&up.svc.pool, &dag, yaml, &at)
        .await
        .unwrap();
    let task = db::list_tasks(&up.svc.pool, &run)
        .await
        .unwrap()
        .into_iter()
        .find(|t| t.name == "build")
        .unwrap()
        .id;
    (run, task)
}

/// Where the engine is given a key, an artifact is made its run's with its
/// provenance: signed, of the zip's SHA-256, saying the run's record.
#[tokio::test]
async fn an_artifact_is_kept_with_its_signed_provenance() {
    use dagron_crypto::provenance::{covers, generate_key_pem, verify, KeySigner, Signer};
    let pem = generate_key_pem().unwrap();
    let public = KeySigner::from_pem(&pem)
        .unwrap()
        .public_key_pem()
        .to_string();
    let up = Up::built(1 << 20, |svc| {
        svc.with_provenance(Some(Provenance::new(
            Box::new(KeySigner::from_pem(&pem).unwrap()),
            None,
        )))
    })
    .await;
    let (run, task) = a_run_at_a_commit(&up).await;
    let token = up.token_of(&task, &run, "refs/heads/main", None, true);
    let zip: &[u8] = b"PK the bytes of a signed zip";
    let (status, done) = upload(&up, &token, "dist", zip).await;
    assert_eq!(status, StatusCode::OK, "{done}");
    let kept: Option<String> = sqlx::query_scalar(
        "SELECT provenance FROM workflow_artifacts WHERE run_id = ? AND name = 'dist'",
    )
    .bind(&run)
    .fetch_one(&up.svc.pool)
    .await
    .unwrap();
    let envelope: Value = serde_json::from_str(&kept.expect("signed")).unwrap();
    let statement = verify(&envelope, &public).unwrap();
    assert!(covers(&statement, &hex(&Sha256::digest(zip))));
    assert_eq!(statement["subject"][0]["name"], "dist.zip");
    let def = &statement["predicate"]["buildDefinition"];
    assert_eq!(def["externalParameters"]["job"], "build");
    assert_eq!(def["externalParameters"]["workflow"], "ci");
    assert_eq!(
        def["resolvedDependencies"][0]["digest"]["gitCommit"],
        "0123456789abcdef0123456789abcdef01234567"
    );
}

/// A job run again, its run rerun whole on the same engine run, uploads
/// what it uploaded the first time: the artifact its earlier attempt left
/// gives way to it, as `overwrite: true` would have it give way, where a
/// second upload of the name in one attempt is still refused. Each is
/// signed under the attempt that made it, numbered through the job's life.
#[tokio::test]
async fn a_job_run_again_replaces_what_it_uploaded_before() {
    use dagron_crypto::provenance::{generate_key_pem, verify, KeySigner, Signer};
    let pem = generate_key_pem().unwrap();
    let public = KeySigner::from_pem(&pem)
        .unwrap()
        .public_key_pem()
        .to_string();
    let up = Up::built(1 << 20, |svc| {
        svc.with_provenance(Some(Provenance::new(
            Box::new(KeySigner::from_pem(&pem).unwrap()),
            None,
        )))
    })
    .await;
    let (run, task) = a_run_at_a_commit(&up).await;
    let pool = up.svc.pool.clone();
    let set = |sql: &'static str| {
        let (pool, task) = (pool.clone(), task.clone());
        async move {
            sqlx::query(sql).bind(&task).execute(&pool).await.unwrap();
        }
    };
    // Claimed, as a worker claims it: its first attempt.
    set("UPDATE task_runs SET status = 'running', attempt = 1 WHERE id = ?").await;
    let token = up.token_of(&task, &run, "refs/heads/main", None, true);
    let invocation = || {
        let (pool, run, public) = (pool.clone(), run.clone(), public.clone());
        async move {
            let kept: Option<String> = sqlx::query_scalar(
                "SELECT provenance FROM workflow_artifacts WHERE run_id = ? AND name = 'dist'",
            )
            .bind(&run)
            .fetch_one(&pool)
            .await
            .unwrap();
            let envelope: Value = serde_json::from_str(&kept.expect("signed")).unwrap();
            let statement = verify(&envelope, &public).unwrap();
            statement["predicate"]["runDetails"]["metadata"]["invocationId"]
                .as_str()
                .unwrap()
                .to_string()
        }
    };

    let (status, done) = upload(&up, &token, "dist", b"PK the first run's").await;
    assert_eq!(status, StatusCode::OK, "{done}");
    assert_eq!(invocation().await, format!("{run}/{task}/1"));
    let (status, err) = upload(&up, &token, "dist", b"PK again, in one attempt").await;
    assert_eq!(status, StatusCode::CONFLICT, "{err}");

    // The run succeeds, and is rerun whole: the job runs again.
    set("UPDATE task_runs SET status = 'succeeded' WHERE id = ?").await;
    sqlx::query("UPDATE workflow_runs SET status = 'succeeded' WHERE id = ?")
        .bind(&run)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(db::rerun_all(&pool, &run).await.unwrap(), Some(1));
    set("UPDATE task_runs SET status = 'running', attempt = 1 WHERE id = ?").await;
    let zip: &[u8] = b"PK the rerun's";
    let (status, done) = upload(&up, &token, "dist", zip).await;
    assert_eq!(status, StatusCode::OK, "{done}");
    let (_, listed) = up
        .artifacts(&token, "ListArtifacts", json!({"name_filter": "dist"}))
        .await;
    let artifacts = listed["artifacts"].as_array().unwrap();
    assert_eq!(artifacts.len(), 1, "{listed}");
    assert_eq!(
        artifacts[0]["digest"],
        json!(format!("sha256:{}", hex(&Sha256::digest(zip))))
    );
    assert_eq!(
        invocation().await,
        format!("{run}/{task}/2"),
        "the second build is not signed as the first"
    );

    // Another job of the run is not given the name.
    let other = up.token_of("another-task", &run, "refs/heads/main", None, true);
    let (status, err) = upload(&up, &other, "dist", b"PK another job's").await;
    assert_eq!(status, StatusCode::CONFLICT, "{err}");
}

/// A job a person retries after it failed replaces what it uploaded before
/// it failed, as a job run again by a rerun does.
#[tokio::test]
async fn a_job_retried_replaces_what_it_uploaded_before() {
    let up = Up::new(1 << 20).await;
    let (run, task) = a_run_at_a_commit(&up).await;
    let pool = up.svc.pool.clone();
    let set = |sql: &'static str| {
        let (pool, task) = (pool.clone(), task.clone());
        async move {
            sqlx::query(sql).bind(&task).execute(&pool).await.unwrap();
        }
    };
    set("UPDATE task_runs SET status = 'running', attempt = 1 WHERE id = ?").await;
    let token = up.token_of(&task, &run, "refs/heads/main", None, true);
    let (status, done) = upload(&up, &token, "dist", b"PK the first attempt's").await;
    assert_eq!(status, StatusCode::OK, "{done}");

    // It fails at a later step, and a person retries it.
    set("UPDATE task_runs SET status = 'failed' WHERE id = ?").await;
    sqlx::query("UPDATE workflow_runs SET status = 'failed' WHERE id = ?")
        .bind(&run)
        .execute(&pool)
        .await
        .unwrap();
    assert!(db::retry_task_from_ui(&pool, &task).await.unwrap());
    set("UPDATE task_runs SET status = 'running', attempt = 2 WHERE id = ?").await;
    let zip: &[u8] = b"PK the retry's";
    let (status, done) = upload(&up, &token, "dist", zip).await;
    assert_eq!(status, StatusCode::OK, "{done}");
    let (_, listed) = up
        .artifacts(&token, "ListArtifacts", json!({"name_filter": "dist"}))
        .await;
    let artifacts = listed["artifacts"].as_array().unwrap();
    assert_eq!(artifacts.len(), 1, "{listed}");
    assert_eq!(
        artifacts[0]["digest"],
        json!(format!("sha256:{}", hex(&Sha256::digest(zip))))
    );
    let (status, err) = upload(&up, &token, "dist", b"PK again, in one attempt").await;
    assert_eq!(status, StatusCode::CONFLICT, "{err}");
}

/// An artifact whose provenance could not be signed is not made one: the
/// job is told, and nothing of it is kept.
#[cfg(unix)]
#[tokio::test]
async fn an_artifact_whose_provenance_could_not_be_signed_is_refused() {
    use dagron_crypto::provenance::{generate_key_pem, CommandSigner, KeySigner, Signer};
    let public = KeySigner::from_pem(&generate_key_pem().unwrap())
        .unwrap()
        .public_key_pem()
        .to_string();
    let up = Up::built(1 << 20, |svc| {
        svc.with_provenance(Some(Provenance::new(
            Box::new(CommandSigner::new(vec!["false".into()], &public).unwrap()),
            None,
        )))
    })
    .await;
    let (run, task) = a_run_at_a_commit(&up).await;
    let token = up.token_of(&task, &run, "refs/heads/main", None, true);
    let (status, err) = upload(&up, &token, "dist", b"PK unsigned").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{err}");
    let (_, listed) = up.artifacts(&token, "ListArtifacts", json!({})).await;
    assert_eq!(listed["artifacts"], json!([]));
    assert!(
        up.mem.0.lock().unwrap().is_empty(),
        "its bytes are not kept"
    );
}
