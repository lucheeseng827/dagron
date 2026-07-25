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

use crate::{sanitize_component, ArtifactKey, ArtifactStore};

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
}

impl CloudStore {
    /// Dispatch a `s3://` / `gs://` / `az://` URL to whichever backend was
    /// compiled in. Mirrors the archive-sink URL convention.
    // With `cloud` alone (no backend feature) every arm bails, leaving the
    // final Ok unreachable — a legitimate feature combination, not dead code.
    #[allow(unreachable_code)]
    pub fn from_url(url: &str) -> Result<Self> {
        let url = url.trim().trim_end_matches('/');
        let (store, prefix): (Arc<dyn ObjectStore>, ObjPath) = match url.split_once("://") {
            Some(("s3", rest)) => {
                #[cfg(feature = "s3")]
                {
                    let (bucket, prefix) = split(rest, url)?;
                    let store = object_store::aws::AmazonS3Builder::from_env()
                        .with_bucket_name(bucket)
                        .build()
                        .map_err(|e| anyhow::anyhow!("{url} S3 config: {e}"))?;
                    (Arc::new(store), ObjPath::from(prefix))
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
                    (Arc::new(store), ObjPath::from(prefix))
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
                    (Arc::new(store), ObjPath::from(prefix))
                }
                #[cfg(not(feature = "azure"))]
                {
                    let _ = rest;
                    bail!("{url}: this build lacks the Azure artifact backend (enable the `azure` feature on dagron-artifact)");
                }
            }
            _ => bail!("DAGRON_ARTIFACT_URL must be s3://, gs://, or az://…, got '{url}'"),
        };
        Ok(Self { store, prefix, url_root: url.to_string() })
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
        }
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

    #[tokio::test]
    async fn from_url_rejects_unknown_scheme() {
        let err = match CloudStore::from_url("ftp://nope/x") {
            Err(e) => e.to_string(),
            Ok(_) => panic!("ftp:// must be rejected"),
        };
        assert!(err.contains("s3://"), "names the accepted schemes: {err}");
    }
}
