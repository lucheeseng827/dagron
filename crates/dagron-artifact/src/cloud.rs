//! Cloud object-store artifact backend (G-A3) — S3 / GCS / Azure Blob behind
//! the same [`ArtifactStore`] trait as the local store.
//!
//! `DAGRON_ARTIFACT_URL` selects it (see [`CloudStore::from_url`]):
//!
//! * `s3://bucket/prefix`    — feature `s3` (S3-compatible MinIO/Ceph via
//!   `AWS_ENDPOINT_URL`),
//! * `gs://bucket/prefix`    — feature `gcs`,
//! * `az://container/prefix` (or `azure://`) — feature `azure`.
//!
//! Credentials/region/endpoint come from each backend's standard environment
//! (`AWS_*` / `GOOGLE_*` / `AZURE_*`), the same `from_env()` convention as the
//! cloud archive sinks. A URL whose backend feature is not compiled in is a
//! hard error — never a silent fall-back to local.
//!
//! Layout mirrors the local store: `<prefix>/<run_id>/<task>/<name>` with the
//! same sanitized components, so a checkpoint written on one cloud is
//! addressable from any worker that can reach the bucket — the cross-cloud
//! resume substrate. Composes with [`EncryptedStore`](crate::EncryptedStore)
//! exactly like the local store (ciphertext in the bucket, BYOK/KMS keys).
//! Large artifacts stream: `put_stream` is a bounded-memory multipart upload,
//! `get_stream` reads the object's byte stream without buffering it whole.

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use futures_util::StreamExt;
use object_store::{path::Path as ObjPath, ObjectStore, WriteMultipart};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{sanitize_component, ArtifactKey, ArtifactStore, Presigned, Transfer};

/// Read granularity for streaming uploads. `WriteMultipart` assembles these
/// into provider-sized parts; capacity is awaited per chunk so an unbounded
/// producer cannot balloon memory.
const UPLOAD_CHUNK: usize = 1024 * 1024;
/// Max multipart parts in flight before `put_stream` awaits capacity.
const UPLOAD_CONCURRENCY: usize = 8;

/// [`ArtifactStore`] over any `object_store` backend (S3 / GCS / Azure — or an
/// in-memory store in tests/embeddings via [`CloudStore::with_store`]).
pub struct CloudStore {
    store: Arc<dyn ObjectStore>,
    /// In-bucket prefix (possibly empty) under which artifacts live.
    prefix: ObjPath,
    /// The original URL root (e.g. `s3://bucket/prefix`), used to render
    /// task-visible locators.
    url_root: String,
    /// What signs a URL for one object, where the backend can: the same
    /// client as `store`, kept by its own trait because `dyn ObjectStore`
    /// has forgotten it. `None` for a backend with nothing to sign with.
    signer: Option<Arc<dyn Signs>>,
    /// Headers an upload by a signed URL must carry on this backend.
    put_headers: Vec<(&'static str, &'static str)>,
}

/// What Azure Blob needs said of an upload: a `PUT` of a blob without its
/// type is refused, signed or not.
#[cfg(any(feature = "azure", test))]
const AZURE_PUT_HEADERS: &[(&str, &str)] = &[("x-ms-blob-type", "BlockBlob")];

/// Signs a URL for one object and one request. A trait of this crate's own,
/// so that a store built without a cloud backend names no type of one.
#[async_trait]
pub trait Signs: Send + Sync {
    async fn signed(&self, how: Transfer, path: &ObjPath, expires_in: std::time::Duration) -> Result<String>;
}

#[cfg(any(feature = "s3", feature = "gcs", feature = "azure"))]
#[async_trait]
impl<S: object_store::signer::Signer> Signs for S {
    async fn signed(&self, how: Transfer, path: &ObjPath, expires_in: std::time::Duration) -> Result<String> {
        let method = match how {
            Transfer::Put => http::Method::PUT,
            Transfer::Get => http::Method::GET,
        };
        Ok(self.signed_url(method, path, expires_in).await?.to_string())
    }
}

impl CloudStore {
    /// Dispatch a `s3://` / `gs://` / `az://` URL to whichever backend was
    /// compiled in. Mirrors the archive-sink URL convention.
    // With `cloud` alone (no backend feature) every arm bails, leaving the
    // final Ok unreachable — a legitimate feature combination, not dead code.
    #[allow(unreachable_code)]
    pub fn from_url(url: &str) -> Result<Self> {
        let url = url.trim().trim_end_matches('/');
        // With no backend compiled in, nothing below is reached to read these.
        #[allow(unused_mut, unused_assignments, unused_variables)]
        let mut signer: Option<Arc<dyn Signs>> = None;
        #[allow(unused_mut, unused_assignments, unused_variables)]
        let mut put_headers: Vec<(&'static str, &'static str)> = Vec::new();
        let (store, prefix): (Arc<dyn ObjectStore>, ObjPath) = match url.split_once("://") {
            Some(("s3", rest)) => {
                #[cfg(feature = "s3")]
                {
                    let (bucket, prefix) = split(rest, url)?;
                    let store = object_store::aws::AmazonS3Builder::from_env()
                        .with_bucket_name(bucket)
                        .build()
                        .map_err(|e| anyhow::anyhow!("{url} S3 config: {e}"))?;
                    let store = Arc::new(store);
                    signer = Some(store.clone());
                    (store, ObjPath::from(prefix))
                }
                #[cfg(not(feature = "s3"))]
                {
                    let _ = rest;
                    bail!("{url}: this build lacks the S3 artifact backend (enable the `s3` feature on dagron-artifact)");
                }
            }
            Some(("gs", rest)) => {
                #[cfg(feature = "gcs")]
                {
                    let (bucket, prefix) = split(rest, url)?;
                    let store = object_store::gcp::GoogleCloudStorageBuilder::from_env()
                        .with_bucket_name(bucket)
                        .build()
                        .map_err(|e| anyhow::anyhow!("{url} GCS config: {e}"))?;
                    let store = Arc::new(store);
                    signer = Some(store.clone());
                    (store, ObjPath::from(prefix))
                }
                #[cfg(not(feature = "gcs"))]
                {
                    let _ = rest;
                    bail!("{url}: this build lacks the GCS artifact backend (enable the `gcs` feature on dagron-artifact)");
                }
            }
            Some(("az" | "azure", rest)) => {
                #[cfg(feature = "azure")]
                {
                    let (container, prefix) = split(rest, url)?;
                    let store = object_store::azure::MicrosoftAzureBuilder::from_env()
                        .with_container_name(container)
                        .build()
                        .map_err(|e| anyhow::anyhow!("{url} Azure config: {e}"))?;
                    let store = Arc::new(store);
                    signer = Some(store.clone());
                    put_headers = AZURE_PUT_HEADERS.to_vec();
                    (store, ObjPath::from(prefix))
                }
                #[cfg(not(feature = "azure"))]
                {
                    let _ = rest;
                    bail!("{url}: this build lacks the Azure artifact backend (enable the `azure` feature on dagron-artifact)");
                }
            }
            // Azure Blob Storage's own URL for a container, which names the
            // account as `az://` cannot (17.4: a workspace's task identity on
            // AKS reports its prefix so).
            Some(("https", rest)) if azure_blob_url(rest).is_some() => {
                #[cfg(feature = "azure")]
                {
                    let (account, container, prefix) = azure_blob_url(rest).expect("matched above");
                    let store = object_store::azure::MicrosoftAzureBuilder::from_env()
                        .with_account(account)
                        .with_container_name(container)
                        .build()
                        .map_err(|e| anyhow::anyhow!("{url} Azure config: {e}"))?;
                    let store = Arc::new(store);
                    signer = Some(store.clone());
                    put_headers = AZURE_PUT_HEADERS.to_vec();
                    (store, ObjPath::from(prefix))
                }
                #[cfg(not(feature = "azure"))]
                {
                    bail!("{url}: this build lacks the Azure artifact backend (enable the `azure` feature on dagron-artifact)");
                }
            }
            _ => bail!(
                "DAGRON_ARTIFACT_URL must be s3://, gs://, az:// or \
                 https://<account>.blob.core.windows.net/…, got '{url}'"
            ),
        };
        Ok(Self { store, prefix, url_root: url.to_string(), signer, put_headers })
    }

    /// Wrap an already-built `object_store` backend — the seam tests (in-memory
    /// store) and embeddings use; `url_root` only shapes returned locators.
    pub fn with_store(
        store: Arc<dyn ObjectStore>,
        url_root: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Self {
        let url_root = url_root.into();
        Self {
            store,
            prefix: ObjPath::from(prefix.into()),
            url_root: url_root.trim_end_matches('/').to_string(),
            signer: None,
            put_headers: Vec::new(),
        }
    }

    /// Sign this store's URLs with `signer`: for a backend built elsewhere
    /// ([`CloudStore::with_store`]), and for a test.
    pub fn with_signer(mut self, signer: Arc<dyn Signs>) -> Self {
        self.signer = Some(signer);
        self
    }

    fn path(&self, key: &ArtifactKey) -> ObjPath {
        // ObjPath::child sanitizes separators per part; our sanitize keeps the
        // layout identical to the local store's.
        self.prefix
            .child(sanitize_component(&key.run_id))
            .child(sanitize_component(&key.task))
            .child(sanitize_component(&key.name))
    }

    fn locator(&self, key: &ArtifactKey) -> String {
        format!("{}/{}", self.url_root, key.rel_path())
    }

    /// Delete the objects under `<prefix>/<top>/` whose next part `doomed`
    /// names, and say how many there were and their bytes: what a log store
    /// removes of a task (`log_segments`). Nothing outside `top` is listed or
    /// touched. A bulk delete is asked for first (S3's takes 1,000 keys a
    /// request); what it did not delete is deleted one object at a time, as a
    /// store without one needs, and what is gone already is no error. A
    /// refusal does not stop the rest: it is the error once all were tried.
    pub(crate) async fn remove_under(
        &self,
        top: &str,
        doomed: impl Fn(&str) -> bool,
    ) -> Result<(u64, u64)> {
        let under = self.prefix.child(sanitize_component(top));
        let mut found = Vec::new();
        let mut bytes = 0u64;
        let mut listed = self.store.list(Some(&under));
        while let Some(meta) = listed.next().await {
            let meta = meta.with_context(|| format!("listing {}/{top}", self.url_root))?;
            let first = match meta.location.prefix_match(&under) {
                Some(mut rest) => rest.next().map(|p| p.as_ref().to_string()),
                None => continue,
            };
            if first.is_some_and(|part| doomed(&part)) {
                bytes += meta.size as u64;
                found.push(meta.location);
            }
        }
        if found.is_empty() {
            return Ok((0, 0));
        }
        let mut done = std::collections::HashSet::new();
        let paths = futures_util::stream::iter(found.clone().into_iter().map(Ok)).boxed();
        let mut deleted = self.store.delete_stream(paths);
        while let Some(result) = deleted.next().await {
            if let Ok(path) = result {
                done.insert(path);
            }
        }
        let mut refused = Vec::new();
        for path in found.iter().filter(|p| !done.contains(*p)) {
            match self.store.delete(path).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => refused.push(format!("{path}: {e}")),
            }
        }
        if let Some(first) = refused.first() {
            bail!(
                "{} of {} objects under {}/{top} were not deleted; the first: {first}",
                refused.len(),
                found.len(),
                self.url_root
            );
        }
        Ok((found.len() as u64, bytes))
    }
}

#[async_trait]
impl ArtifactStore for CloudStore {
    async fn put(&self, key: &ArtifactKey, bytes: &[u8]) -> Result<String> {
        self.store
            .put(&self.path(key), bytes.to_vec().into())
            .await
            .with_context(|| format!("put {}", self.locator(key)))?;
        Ok(self.locator(key))
    }

    async fn get(&self, key: &ArtifactKey) -> Result<Vec<u8>> {
        let res = self
            .store
            .get(&self.path(key))
            .await
            .with_context(|| format!("get {}", self.locator(key)))?;
        Ok(res.bytes().await?.to_vec())
    }

    async fn exists(&self, key: &ArtifactKey) -> Result<bool> {
        match self.store.head(&self.path(key)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e).with_context(|| format!("head {}", self.locator(key))),
        }
    }

    /// Delete the object. Whether one was there is asked first: a bucket
    /// answers a delete of nothing as it answers a delete of something.
    async fn delete(&self, key: &ArtifactKey) -> Result<bool> {
        if !self.exists(key).await? {
            return Ok(false);
        }
        match self.store.delete(&self.path(key)).await {
            Ok(()) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e).with_context(|| format!("delete {}", self.locator(key))),
        }
    }

    async fn size(&self, key: &ArtifactKey) -> Result<Option<u64>> {
        match self.store.head(&self.path(key)).await {
            Ok(meta) => Ok(Some(meta.size as u64)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e).with_context(|| format!("head {}", self.locator(key))),
        }
    }

    /// A URL for the one object, signed with the store's own credentials.
    /// `None` where the backend signs nothing (an in-memory store).
    async fn presign(
        &self,
        key: &ArtifactKey,
        how: Transfer,
        expires_in: std::time::Duration,
    ) -> Result<Option<Presigned>> {
        let Some(signer) = &self.signer else {
            return Ok(None);
        };
        let url = signer
            .signed(how, &self.path(key), expires_in)
            .await
            .with_context(|| format!("signing a URL for {}", self.locator(key)))?;
        let (method, headers) = match how {
            Transfer::Put => ("PUT", self.put_headers.clone()),
            Transfer::Get => ("GET", Vec::new()),
        };
        Ok(Some(Presigned { method, url, headers }))
    }

    /// The per-run URL prefix (`<url>/<run_id>`). Tasks reach it with their own
    /// cloud tooling — there is no shared local directory for a cloud store.
    fn run_location(&self, run_id: &str) -> Option<String> {
        Some(format!("{}/{}", self.url_root, sanitize_component(run_id)))
    }

    /// Enumerate `<prefix>/<run>/<task>/<name>` objects (anything shallower or
    /// deeper is skipped) — what the key-rotation sweep walks.
    async fn list(&self) -> Result<Vec<ArtifactKey>> {
        let mut out = Vec::new();
        let prefix = if self.prefix.as_ref().is_empty() { None } else { Some(&self.prefix) };
        let mut stream = self.store.list(prefix);
        while let Some(meta) = stream.next().await {
            let meta = meta.context("list artifacts")?;
            let parts: Vec<String> = match meta.location.prefix_match(&self.prefix) {
                Some(rest) => rest.map(|p| p.as_ref().to_string()).collect(),
                None => continue,
            };
            if let [run_id, task, name] = parts.as_slice() {
                out.push(ArtifactKey::new(run_id, task, name));
            }
        }
        Ok(out)
    }

    /// Bounded-memory multipart upload: read ~1 MiB at a time, hand chunks to
    /// `WriteMultipart` (which sizes provider parts), and await part capacity so
    /// at most a few parts are buffered. A failed upload is aborted best-effort
    /// so no incomplete multipart lingers billable.
    async fn put_stream(
        &self,
        key: &ArtifactKey,
        mut reader: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Result<String> {
        let upload = self
            .store
            .put_multipart(&self.path(key))
            .await
            .with_context(|| format!("start multipart {}", self.locator(key)))?;
        let mut writer = WriteMultipart::new(upload);
        let mut buf = vec![0u8; UPLOAD_CHUNK];
        let finished = loop {
            match reader.read(&mut buf).await {
                Ok(0) => break Ok(()),
                Ok(n) => {
                    if let Err(e) = writer.wait_for_capacity(UPLOAD_CONCURRENCY).await {
                        break Err(anyhow::Error::from(e));
                    }
                    writer.write(&buf[..n]);
                }
                Err(e) => break Err(anyhow::Error::from(e)),
            }
        };
        match finished {
            Ok(()) => {
                writer
                    .finish()
                    .await
                    .with_context(|| format!("finish multipart {}", self.locator(key)))?;
                Ok(self.locator(key))
            }
            Err(e) => {
                if let Err(abort_err) = writer.abort().await {
                    tracing::warn!(error = %abort_err, "multipart abort failed (incomplete upload may linger)");
                }
                Err(e).with_context(|| format!("stream {}", self.locator(key)))
            }
        }
    }

    /// Stream the object's bytes as they arrive — no whole-object buffering.
    async fn get_stream(
        &self,
        key: &ArtifactKey,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
        let res = self
            .store
            .get(&self.path(key))
            .await
            .with_context(|| format!("get {}", self.locator(key)))?;
        let stream = res.into_stream().map(|r| r.map_err(std::io::Error::from));
        Ok(Box::new(tokio_util::io::StreamReader::new(stream)))
    }
}

/// Split "bucket/prefix" → ("bucket", "prefix"), prefix trimmed of `/`.
#[cfg(any(feature = "s3", feature = "gcs", feature = "azure"))]
fn split<'a>(rest: &'a str, url: &str) -> Result<(&'a str, &'a str)> {
    let (bucket, prefix) = match rest.split_once('/') {
        Some((b, p)) => (b, p.trim_matches('/')),
        None => (rest, ""),
    };
    if bucket.is_empty() {
        bail!("artifact URL is missing a bucket/container: '{url}'");
    }
    Ok((bucket, prefix))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EncryptedStore;

    fn mem(prefix: &str) -> CloudStore {
        CloudStore::with_store(
            Arc::new(object_store::memory::InMemory::new()),
            "s3://test-bucket/arts",
            prefix,
        )
    }

    fn reader(bytes: Vec<u8>) -> Box<dyn AsyncRead + Send + Unpin> {
        Box::new(std::io::Cursor::new(bytes))
    }

    #[tokio::test]
    async fn cloud_store_round_trips_and_lists() {
        let s = mem("arts");
        let key = ArtifactKey::new("run1", "train", "ckpt.bin");

        assert!(!s.exists(&key).await.unwrap());
        let loc = s.put(&key, b"weights").await.unwrap();
        assert_eq!(loc, "s3://test-bucket/arts/run1/train/ckpt.bin");
        assert!(s.exists(&key).await.unwrap());
        assert_eq!(s.get(&key).await.unwrap(), b"weights");
        assert_eq!(
            s.run_location("run1").as_deref(),
            Some("s3://test-bucket/arts/run1"),
            "per-run URL prefix, no local dir"
        );

        s.put(&ArtifactKey::new("run2", "eval", "report"), b"ok").await.unwrap();
        let mut listed: Vec<String> =
            s.list().await.unwrap().iter().map(|k| k.rel_path()).collect();
        listed.sort();
        assert_eq!(listed, vec!["run1/train/ckpt.bin", "run2/eval/report"]);
    }

    /// Traversal-shaped keys land inside the store prefix, mirroring the local
    /// store's sanitization (an object key can't climb out of the layout).
    /// Signs as a bucket would, without one: the path, the request and how
    /// long, in a query.
    struct FakeSigner;

    #[async_trait]
    impl Signs for FakeSigner {
        async fn signed(&self, how: Transfer, path: &ObjPath, expires_in: std::time::Duration) -> Result<String> {
            Ok(format!("https://bucket.example/{path}?how={how:?}&for={}&sig=5ecret", expires_in.as_secs()))
        }
    }

    #[tokio::test]
    async fn cloud_store_deletes_and_signs_a_url_for_the_one_object() {
        let s = mem("arts");
        let k = ArtifactKey::new("run1", "build", "out.bin");
        let hour = std::time::Duration::from_secs(3600);
        assert_eq!(s.presign(&k, Transfer::Put, hour).await.unwrap(), None, "nothing to sign with");
        assert_eq!(s.size(&k).await.unwrap(), None, "nothing is there");
        s.put(&k, b"payload").await.unwrap();
        assert_eq!(s.size(&k).await.unwrap(), Some(7));
        assert!(s.delete(&k).await.unwrap());
        assert!(!s.exists(&k).await.unwrap());
        assert!(!s.delete(&k).await.unwrap(), "nothing was there the second time");

        let s = mem("arts").with_signer(Arc::new(FakeSigner));
        let put = s.presign(&k, Transfer::Put, hour).await.unwrap().unwrap();
        assert_eq!(
            (put.method, put.url.as_str()),
            ("PUT", "https://bucket.example/arts/run1/build/out.bin?how=Put&for=3600&sig=5ecret")
        );
        assert!(put.headers.is_empty(), "this store asks nothing more of an upload");
        let get = s.presign(&k, Transfer::Get, hour).await.unwrap().unwrap();
        assert!(get.method == "GET" && get.url.contains("how=Get"), "{get:?}");
        // A backend that takes an upload only with a header says which, for
        // the upload and not for the download.
        let mut blob = mem("arts").with_signer(Arc::new(FakeSigner));
        blob.put_headers = AZURE_PUT_HEADERS.to_vec();
        let put = blob.presign(&k, Transfer::Put, hour).await.unwrap().unwrap();
        assert_eq!(put.headers, [("x-ms-blob-type", "BlockBlob")]);
        assert!(blob.presign(&k, Transfer::Get, hour).await.unwrap().unwrap().headers.is_empty());
        // Under encryption there is none to give, whatever the bucket can sign.
        let sealed = EncryptedStore::new(
            mem("arts").with_signer(Arc::new(FakeSigner)),
            Box::new(dagron_crypto::LocalKekProvider::new([7u8; 32])),
        );
        assert_eq!(sealed.presign(&k, Transfer::Put, hour).await.unwrap(), None);
    }

    #[tokio::test]
    async fn cloud_store_sanitizes_keys() {
        let s = mem("arts");
        let key = ArtifactKey::new("../run", "t/ask", "..");
        let loc = s.put(&key, b"x").await.unwrap();
        assert_eq!(loc, "s3://test-bucket/arts/.._run/t_ask/_");
        assert_eq!(s.get(&key).await.unwrap(), b"x");
    }

    #[tokio::test]
    async fn cloud_store_streams_large_artifacts() {
        let s = mem("arts");
        let key = ArtifactKey::new("run", "train", "big.ckpt");
        // Several upload chunks worth, so the multipart write loop cycles.
        let big: Vec<u8> = (0..3 * UPLOAD_CHUNK + 137).map(|i| (i % 251) as u8).collect();

        let loc = s.put_stream(&key, reader(big.clone())).await.unwrap();
        assert_eq!(loc, "s3://test-bucket/arts/run/train/big.ckpt");

        let mut r = s.get_stream(&key).await.unwrap();
        let mut out = Vec::new();
        r.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, big, "streaming round trip");
    }

    /// The encrypting decorator composes over the cloud backend exactly as over
    /// the local one: ciphertext in the bucket, transparent reads, rotation.
    #[tokio::test]
    async fn encrypted_cloud_store_ciphertext_in_bucket() {
        let backend = Arc::new(object_store::memory::InMemory::new());
        let raw = || {
            CloudStore::with_store(backend.clone(), "s3://b/enc", "enc")
        };
        let key = ArtifactKey::new("run", "train", "ckpt.bin");
        let plaintext = b"MODEL-WEIGHTS".to_vec();

        let enc = EncryptedStore::new(
            raw(),
            Box::new(dagron_crypto::LocalKekProvider::new([7u8; 32])),
        );
        enc.put(&key, &plaintext).await.unwrap();

        let in_bucket = raw().get(&key).await.unwrap();
        assert_ne!(in_bucket, plaintext, "bucket holds ciphertext");
        assert!(
            !in_bucket.windows(plaintext.len()).any(|w| w == plaintext.as_slice()),
            "plaintext must not appear in the bucket"
        );
        assert_eq!(enc.get(&key).await.unwrap(), plaintext);
        assert!(enc.run_location("run").is_none(), "no task-visible location under encryption");

        // Rotation sweeps the cloud listing too.
        let old = || Box::new(dagron_crypto::LocalKekProvider::with_id([7u8; 32], "k1"));
        let newk = || Box::new(dagron_crypto::LocalKekProvider::with_id([9u8; 32], "k2"));
        let enc_old = EncryptedStore::new(raw(), old());
        enc_old.put(&key, &plaintext).await.unwrap();
        let enc_new = EncryptedStore::new(raw(), newk());
        assert_eq!(enc_new.rotate_from(old().as_ref()).await.unwrap(), 1);
        assert_eq!(enc_new.get(&key).await.unwrap(), plaintext);
    }

    /// An in-memory store whose bulk delete fails as a request, or that will
    /// not delete one object: what a store without S3's DeleteObjects, or
    /// one that keeps an object, answers.
    #[derive(Debug)]
    struct Refusing {
        inner: object_store::memory::InMemory,
        bulk_fails: bool,
        refused: Option<ObjPath>,
    }

    impl std::fmt::Display for Refusing {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("Refusing")
        }
    }

    fn refusal(what: &str) -> object_store::Error {
        object_store::Error::Generic {
            store: "Refusing",
            source: what.to_string().into(),
        }
    }

    #[async_trait]
    impl ObjectStore for Refusing {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: object_store::PutMultipartOpts,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &ObjPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        async fn delete(&self, location: &ObjPath) -> object_store::Result<()> {
            if self.refused.as_ref() == Some(location) {
                return Err(refusal("this one is kept"));
            }
            self.inner.delete(location).await
        }
        fn delete_stream<'a>(
            &'a self,
            locations: futures_util::stream::BoxStream<'a, object_store::Result<ObjPath>>,
        ) -> futures_util::stream::BoxStream<'a, object_store::Result<ObjPath>> {
            if self.bulk_fails {
                return futures_util::stream::once(async { Err(refusal("no bulk delete here")) })
                    .boxed();
            }
            locations
                .then(move |location| async move {
                    let location = location?;
                    self.delete(&location).await?;
                    Ok(location)
                })
                .boxed()
        }
        fn list(
            &self,
            prefix: Option<&ObjPath>,
        ) -> futures_util::stream::BoxStream<'_, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy(&self, from: &ObjPath, to: &ObjPath) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }
        async fn copy_if_not_exists(
            &self,
            from: &ObjPath,
            to: &ObjPath,
        ) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    /// Three objects of a task's log and one of another's, in `store`.
    async fn three_and_another(store: &Refusing) -> Vec<ObjPath> {
        let ours: Vec<ObjPath> = (0..3)
            .map(|seq| ObjPath::from(format!("logs/logs-t/1.1/{seq:06}.seg.zst")))
            .collect();
        let another = ObjPath::from("logs/logs-u/1.1/000000.seg.zst");
        for path in ours.iter().chain([&another]) {
            store.put(path, "segment".into()).await.unwrap();
        }
        ours
    }

    async fn names(store: &dyn ObjectStore) -> Vec<String> {
        let mut all: Vec<String> = Vec::new();
        let mut listed = store.list(None);
        while let Some(meta) = listed.next().await {
            all.push(meta.unwrap().location.to_string());
        }
        all.sort();
        all
    }

    #[tokio::test]
    async fn a_bulk_delete_that_fails_is_made_one_object_at_a_time() {
        let store = Arc::new(Refusing {
            inner: object_store::memory::InMemory::new(),
            bulk_fails: true,
            refused: None,
        });
        three_and_another(&store).await;
        let s = CloudStore::with_store(store.clone(), "s3://b/logs", "logs");
        assert_eq!(
            s.remove_under("logs-t", |_| true).await.unwrap(),
            (3, 3 * 7)
        );
        assert_eq!(
            names(store.as_ref()).await,
            ["logs/logs-u/1.1/000000.seg.zst"]
        );
        assert_eq!(s.remove_under("logs-t", |_| true).await.unwrap(), (0, 0));
    }

    #[tokio::test]
    async fn a_refused_delete_is_an_error_once_the_rest_are_tried() {
        let mut store = Refusing {
            inner: object_store::memory::InMemory::new(),
            bulk_fails: false,
            refused: None,
        };
        let ours = three_and_another(&store).await;
        store.refused = Some(ours[1].clone());
        let store = Arc::new(store);
        let s = CloudStore::with_store(store.clone(), "s3://b/logs", "logs");
        let err = s.remove_under("logs-t", |_| true).await.unwrap_err();
        assert!(format!("{err:#}").contains("1 of 3 objects"), "{err:#}");
        assert_eq!(
            names(store.as_ref()).await,
            [
                ours[1].to_string(),
                "logs/logs-u/1.1/000000.seg.zst".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn from_url_rejects_unknown_scheme() {
        let err = match CloudStore::from_url("ftp://nope/x") {
            Err(e) => e.to_string(),
            Ok(_) => panic!("ftp:// must be rejected"),
        };
        assert!(err.contains("s3://"), "names the accepted schemes: {err}");
    }
}

/// `<account>.blob.core.windows.net/<container>[/<prefix>]`, what follows
/// `https://` in an Azure Blob URL: the account, the container and the prefix,
/// or `None` for any other https URL.
pub fn azure_blob_url(rest: &str) -> Option<(&str, &str, &str)> {
    let (host, path) = rest.split_once('/').unwrap_or((rest, ""));
    let account = host.strip_suffix(".blob.core.windows.net")?;
    if account.is_empty() || !account.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()) {
        return None;
    }
    let (container, prefix) = path.split_once('/').unwrap_or((path, ""));
    if container.is_empty() {
        return None;
    }
    Some((account, container, prefix.trim_matches('/')))
}

#[cfg(test)]
mod azure_url_tests {
    use super::azure_blob_url;

    #[test]
    fn an_azure_blob_url_names_its_account_container_and_prefix() {
        assert_eq!(
            azure_blob_url("acct1.blob.core.windows.net/ws-8f3a/ws/8f3a"),
            Some(("acct1", "ws-8f3a", "ws/8f3a"))
        );
        assert_eq!(azure_blob_url("acct1.blob.core.windows.net/artifacts"), Some(("acct1", "artifacts", "")));
        assert_eq!(azure_blob_url("acct1.blob.core.windows.net/"), None, "no container");
        assert_eq!(azure_blob_url("example.com/c/p"), None, "not Azure Blob");
        assert_eq!(azure_blob_url("Acct.blob.core.windows.net/c"), None, "an account is lower case");
        assert_eq!(azure_blob_url(".blob.core.windows.net/c"), None);
    }

    #[cfg(feature = "azure")]
    #[test]
    fn a_store_is_made_from_one() {
        let store = super::CloudStore::from_url("https://acct1.blob.core.windows.net/ws-8f3a/ws/8f3a/")
            .expect("an Azure Blob URL is an artifact store");
        assert_eq!(store.prefix.as_ref(), "ws/8f3a");
        assert!(super::CloudStore::from_url("https://example.com/x").is_err());
    }
}
