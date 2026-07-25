//! Artifact store seam — passing data files between tasks in a run.
//!
//! [`ArtifactStore`] abstracts *where* task artifacts live so the engine ships
//! a zero-infra local-filesystem store while an S3/GCS backend plugs in behind the
//! same trait. Tasks in a run share a per-run location (the engine injects
//! `DAGRON_ARTIFACTS`) to pass files; the trait is the programmatic surface the API
//! and alternate builds use to read/write artifacts by key.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

/// Identifies one artifact: produced by `task` in `run_id`, under a logical `name`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactKey {
    pub run_id: String,
    pub task: String,
    pub name: String,
}

impl ArtifactKey {
    /// Build a key from its `run_id` / `task` / `name` components.
    pub fn new(
        run_id: impl Into<String>,
        task: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            task: task.into(),
            name: name.into(),
        }
    }

    /// Sanitized relative locator `run_id/task/name` — safe path components only
    /// (no traversal), used by every backend to key the artifact.
    pub fn rel_path(&self) -> String {
        format!(
            "{}/{}/{}",
            sanitize(&self.run_id),
            sanitize(&self.task),
            sanitize(&self.name)
        )
    }
}

/// Cloud object-store backend (S3 / GCS / Azure) behind the same trait —
/// selected by `DAGRON_ARTIFACT_URL`, compiled in via the `s3`/`gcs`/`azure`
/// features.
#[cfg(feature = "cloud")]
pub mod cloud;

/// Sanitize one artifact path component the way every backend keys artifacts —
/// exposed so callers composing artifact *locations* (e.g. the engine's
/// per-task checkpoint URLs) match the store layout exactly.
pub fn sanitize_component(s: &str) -> String {
    sanitize(s)
}

/// Keep only path-safe characters; everything else becomes `_` (blocks slashes,
/// etc. from escaping the store root). `.` is allowed so file extensions survive,
/// but a component that is *only* dots (`.` / `..`) would still resolve to the
/// current/parent directory, so those (and empty) are rejected outright.
fn sanitize(s: &str) -> String {
    if s.is_empty() || s == "." || s == ".." {
        return "_".to_string();
    }
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') { c } else { '_' })
        .collect()
}

/// Where a run's artifacts live + how to read/write them by key.
#[async_trait]
pub trait ArtifactStore: Send + Sync {
    /// Store `bytes` under `key`; returns a backend locator (path / URI).
    async fn put(&self, key: &ArtifactKey, bytes: &[u8]) -> Result<String>;
    /// Fetch the bytes stored under `key`.
    async fn get(&self, key: &ArtifactKey) -> Result<Vec<u8>>;
    /// Whether `key` exists.
    async fn exists(&self, key: &ArtifactKey) -> Result<bool>;
    /// The locator handed to a run's tasks (e.g. a directory path) so they can pass
    /// data, or `None` if this store has no task-visible location.
    fn run_location(&self, _run_id: &str) -> Option<String> {
        None
    }

    /// Every stored artifact key. Default: empty (enumeration unsupported).
    /// Backends that can enumerate (local FS) override it; the key-rotation sweep
    /// depends on it.
    async fn list(&self) -> Result<Vec<ArtifactKey>> {
        Ok(Vec::new())
    }

    /// Stream bytes into `key` from an async reader — constant memory, for large
    /// artifacts. Default buffers then [`put`](ArtifactStore::put); backends that
    /// stream to their medium (and the encrypting decorator) override it.
    async fn put_stream(
        &self,
        key: &ArtifactKey,
        mut reader: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Result<String> {
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await?;
        self.put(key, &buf).await
    }

    /// Stream bytes out of `key`. Default: [`get`](ArtifactStore::get) into an
    /// in-memory cursor (correct for any backend; a truly streaming reader is a
    /// backend override).
    async fn get_stream(
        &self,
        key: &ArtifactKey,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
        Ok(Box::new(std::io::Cursor::new(self.get(key).await?)))
    }

    /// Re-key every stored artifact from `old`'s KEK to this store's current KEK
    /// (rotation). Default: unsupported — only the encrypting store rotates.
    async fn rotate_from(&self, _old: &dyn dagron_crypto::KeyProvider) -> Result<usize> {
        anyhow::bail!("key rotation requires an encrypted artifact store")
    }
}

/// Artifacts disabled — the default when unconfigured. Every operation errors
/// (so a workflow that genuinely needs artifacts fails loudly rather than silently
/// losing data); `run_location` is `None` so the engine injects no dir.
pub struct NoopStore;

#[async_trait]
impl ArtifactStore for NoopStore {
    async fn put(&self, _key: &ArtifactKey, _bytes: &[u8]) -> Result<String> {
        anyhow::bail!("artifact store not configured (set DAGRON_ARTIFACT_DIR)")
    }
    async fn get(&self, _key: &ArtifactKey) -> Result<Vec<u8>> {
        anyhow::bail!("artifact store not configured (set DAGRON_ARTIFACT_DIR)")
    }
    async fn exists(&self, _key: &ArtifactKey) -> Result<bool> {
        Ok(false)
    }
}

/// Local-filesystem artifact store rooted at `base`. Lays artifacts out at
/// `base/<run_id>/<task>/<name>`.
pub struct LocalFsStore {
    base: PathBuf,
}

impl LocalFsStore {
    /// Create a store rooted at `base` (artifacts live under `base/<run_id>/...`).
    pub fn new(base: impl Into<PathBuf>) -> Self {
        Self { base: base.into() }
    }

    /// Build from `DAGRON_ARTIFACT_DIR`, or `None` if unset/empty.
    pub fn from_env() -> Option<Self> {
        std::env::var("DAGRON_ARTIFACT_DIR")
            .ok()
            .filter(|d| !d.is_empty())
            .map(Self::new)
    }

    /// Create (if needed) and return the per-run directory that tasks share. This
    /// is what the engine injects as `DAGRON_ARTIFACTS`.
    pub async fn prepare_run_dir(&self, run_id: &str) -> Result<String> {
        let dir = self.base.join(sanitize(run_id));
        tokio::fs::create_dir_all(&dir).await?;
        Ok(dir.to_string_lossy().into_owned())
    }

    fn path(&self, key: &ArtifactKey) -> PathBuf {
        self.base.join(sanitize(&key.run_id)).join(sanitize(&key.task)).join(sanitize(&key.name))
    }
}

#[async_trait]
impl ArtifactStore for LocalFsStore {
    async fn put(&self, key: &ArtifactKey, bytes: &[u8]) -> Result<String> {
        let p = self.path(key);
        if let Some(parent) = p.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&p, bytes).await?;
        Ok(p.to_string_lossy().into_owned())
    }
    async fn get(&self, key: &ArtifactKey) -> Result<Vec<u8>> {
        Ok(tokio::fs::read(self.path(key)).await?)
    }
    async fn exists(&self, key: &ArtifactKey) -> Result<bool> {
        Ok(tokio::fs::try_exists(self.path(key)).await.unwrap_or(false))
    }
    fn run_location(&self, run_id: &str) -> Option<String> {
        Some(self.base.join(sanitize(run_id)).to_string_lossy().into_owned())
    }

    async fn list(&self) -> Result<Vec<ArtifactKey>> {
        let mut out = Vec::new();
        // Walk base/<run>/<task>/<name>. Missing root = empty (nothing stored yet).
        let mut runs = match tokio::fs::read_dir(&self.base).await {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(e.into()),
        };
        while let Some(run) = runs.next_entry().await? {
            if !run.file_type().await?.is_dir() {
                continue;
            }
            let run_id = run.file_name().to_string_lossy().into_owned();
            let mut tasks = tokio::fs::read_dir(run.path()).await?;
            while let Some(task) = tasks.next_entry().await? {
                if !task.file_type().await?.is_dir() {
                    continue;
                }
                let task_name = task.file_name().to_string_lossy().into_owned();
                let mut names = tokio::fs::read_dir(task.path()).await?;
                while let Some(name) = names.next_entry().await? {
                    if name.file_type().await?.is_file() {
                        out.push(ArtifactKey::new(
                            run_id.clone(),
                            task_name.clone(),
                            name.file_name().to_string_lossy().into_owned(),
                        ));
                    }
                }
            }
        }
        Ok(out)
    }

    async fn put_stream(
        &self,
        key: &ArtifactKey,
        mut reader: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Result<String> {
        let p = self.path(key);
        if let Some(parent) = p.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let mut file = tokio::fs::File::create(&p).await?;
        tokio::io::copy(&mut reader, &mut file).await?;
        file.flush().await?;
        Ok(p.to_string_lossy().into_owned())
    }

    async fn get_stream(
        &self,
        key: &ArtifactKey,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
        Ok(Box::new(tokio::fs::File::open(self.path(key)).await?))
    }
}

/// Envelope-encrypting decorator (G-C2): wraps any [`ArtifactStore`] so artifacts
/// are **ciphertext at rest** and opaque to the storage backend. `put` envelope-
/// encrypts with a fresh per-object data key (wrapped by the [`KeyProvider`]'s
/// KEK — BYOK/KMS); `get` decrypts. The backend (and anyone with disk/bucket
/// access) sees only ciphertext.
///
/// **Boundary:** `run_location` is `None` — a shared plaintext directory would let
/// tasks write cleartext to disk, defeating at-rest encryption. Encrypted
/// artifacts move through `put`/`get` only, not a task-visible `DAGRON_ARTIFACTS`
/// dir. (Encryption is CPU work done inline here; offloading very large artifacts
/// to a blocking pool is a follow-up.)
pub struct EncryptedStore<S> {
    inner: S,
    // Arc so the streaming `put_stream` can share it with a background sealer task.
    provider: Arc<dyn dagron_crypto::KeyProvider>,
}

/// Marks a stored blob written by the streaming path (`put_stream`) — a
/// length-delimited [`stream header`](dagron_crypto::StreamSealer) + chunk frames.
/// Whole-blob (`put`) writes begin with `0x02`; both are read transparently.
const STREAM_BLOB_TAG: u8 = 0x03;

impl<S: ArtifactStore> EncryptedStore<S> {
    /// Wrap `inner`, encrypting artifacts under the KEK the `provider` holds.
    pub fn new(inner: S, provider: Box<dyn dagron_crypto::KeyProvider>) -> Self {
        Self { inner, provider: Arc::from(provider) }
    }
}

#[async_trait]
impl<S: ArtifactStore> ArtifactStore for EncryptedStore<S> {
    async fn put(&self, key: &ArtifactKey, bytes: &[u8]) -> Result<String> {
        let ciphertext = dagron_crypto::encrypt_envelope_bytes(self.provider.as_ref(), bytes)?;
        self.inner.put(key, &ciphertext).await
    }
    async fn get(&self, key: &ArtifactKey) -> Result<Vec<u8>> {
        let raw = self.inner.get(key).await?;
        // Dispatch on the wire tag: streamed (0x03) vs whole-blob (0x02).
        if raw.first() == Some(&STREAM_BLOB_TAG) {
            decrypt_stream_blob(self.provider.as_ref(), &raw)
        } else {
            dagron_crypto::decrypt_envelope_bytes(self.provider.as_ref(), &raw)
        }
    }
    async fn exists(&self, key: &ArtifactKey) -> Result<bool> {
        self.inner.exists(key).await
    }
    fn run_location(&self, _run_id: &str) -> Option<String> {
        // No task-visible plaintext directory under encryption — see the type docs.
        None
    }
    async fn list(&self) -> Result<Vec<ArtifactKey>> {
        self.inner.list().await
    }
    async fn put_stream(
        &self,
        key: &ArtifactKey,
        mut reader: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Result<String> {
        // Seal plaintext chunk-by-chunk in a background task, piping the framed
        // ciphertext into the inner store's streaming write — so neither the
        // plaintext nor the ciphertext is ever fully buffered. Seal errors are
        // surfaced by joining the task.
        let (mut pipe_w, pipe_r) = tokio::io::duplex(64 * 1024);
        let provider = self.provider.clone();
        let sealer = tokio::spawn(async move {
            let (header, mut sealer) = dagron_crypto::StreamSealer::new(provider.as_ref())?;
            // Leading tag byte so the read path distinguishes streamed (0x03) from
            // whole-blob (0x02) at rest, then the length-delimited header + chunks.
            pipe_w.write_all(&[STREAM_BLOB_TAG]).await?;
            write_frame(&mut pipe_w, &header).await?;
            let mut buf = vec![0u8; 64 * 1024];
            let mut pending: Option<Vec<u8>> = None;
            loop {
                let n = reader.read(&mut buf).await?;
                if n == 0 {
                    // EOF: the pending chunk (or an empty one) is the final chunk.
                    let last = pending.take().unwrap_or_default();
                    let frame = sealer.seal_chunk(&last, true)?;
                    write_frame(&mut pipe_w, &frame).await?;
                    break;
                }
                if let Some(prev) = pending.replace(buf[..n].to_vec()) {
                    let frame = sealer.seal_chunk(&prev, false)?;
                    write_frame(&mut pipe_w, &frame).await?;
                }
            }
            pipe_w.shutdown().await?;
            Ok::<(), anyhow::Error>(())
        });
        let loc = self.inner.put_stream(key, Box::new(pipe_r)).await;
        // Join the seal task (a genuine panic still surfaces). If the inner store
        // failed, its error is the root cause — return it rather than the sealer's
        // likely broken-pipe symptom (the store dropped `pipe_r`). Only raise a seal
        // error when the write side actually succeeded.
        let sealed = sealer.await.context("seal task panicked")?;
        if loc.is_ok() {
            sealed?;
        }
        loc
    }
    async fn get_stream(
        &self,
        key: &ArtifactKey,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
        let mut inner = self.inner.get_stream(key).await?;
        // Peek the wire tag: streamed (0x03) can be decrypted chunk-by-chunk;
        // whole-blob (0x02) has one GCM tag over everything and must be buffered.
        let mut tag = [0u8; 1];
        match inner.read_exact(&mut tag).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(Box::new(std::io::Cursor::new(Vec::new()))); // empty artifact
            }
            Err(e) => return Err(e.into()),
        }
        if tag[0] == STREAM_BLOB_TAG {
            // Decrypt lazily as the consumer reads. Errors (tamper / truncation)
            // propagate through the reader as io::Error — never silent short reads.
            let provider = self.provider.clone();
            let stream = async_stream::try_stream! {
                let header = read_frame_async(&mut inner)
                    .await?
                    .ok_or_else(|| io_err("stream artifact: missing header"))?;
                let mut opener =
                    dagron_crypto::StreamOpener::new(provider.as_ref(), &header).map_err(io_map)?;
                loop {
                    match read_frame_async(&mut inner).await? {
                        Some(frame) => {
                            let (pt, is_final) = opener.open_chunk(&frame).map_err(io_map)?;
                            yield bytes::Bytes::from(pt);
                            if is_final {
                                break;
                            }
                        }
                        None => {
                            if !opener.finished() {
                                Err(io_err("truncated encrypted stream"))?;
                            }
                            break;
                        }
                    }
                }
            };
            let boxed: std::pin::Pin<
                Box<dyn futures_core::Stream<Item = std::io::Result<bytes::Bytes>> + Send>,
            > = Box::pin(stream);
            Ok(Box::new(tokio_util::io::StreamReader::new(boxed)))
        } else {
            // Whole-blob (0x02): read the rest, restore the tag, decrypt fully.
            let mut rest = Vec::new();
            inner.read_to_end(&mut rest).await?;
            let mut raw = Vec::with_capacity(1 + rest.len());
            raw.push(tag[0]);
            raw.extend_from_slice(&rest);
            let pt = dagron_crypto::decrypt_envelope_bytes(self.provider.as_ref(), &raw)?;
            Ok(Box::new(std::io::Cursor::new(pt)))
        }
    }
    async fn rotate_from(&self, old: &dyn dagron_crypto::KeyProvider) -> Result<usize> {
        // Re-key every artifact: unwrap-and-rewrap the data key only, never
        // re-encrypting the payload. Pairs with crypto-shred (destroy the old KEK
        // once every object is rotated off it).
        //
        // Resume-safe: a sweep interrupted partway (a transient get/put failure, a
        // restart) may be re-run. An artifact already rewrapped onto the current KEK
        // no longer unwraps under `old`; rather than aborting the whole loop on that
        // failure, confirm it reads under the current provider and skip it. A value
        // that unwraps under *neither* KEK is a genuine error and propagates.
        let keys = self.inner.list().await?;
        for key in &keys {
            let raw = self.inner.get(key).await?;
            let is_stream = raw.first() == Some(&STREAM_BLOB_TAG);
            let rewrap = |from: &dyn dagron_crypto::KeyProvider| {
                if is_stream {
                    rotate_stream_blob(from, self.provider.as_ref(), &raw)
                } else {
                    dagron_crypto::rewrap_envelope_bytes(from, self.provider.as_ref(), &raw)
                }
            };
            match rewrap(old) {
                Ok(rotated) => {
                    self.inner.put(key, &rotated).await?;
                }
                // Unwrap under `old` failed — but if it already reads under the
                // current KEK it was migrated by an earlier run, so skip it. If it
                // reads under neither, surface the original error.
                Err(e) => {
                    if rewrap(self.provider.as_ref()).is_err() {
                        return Err(e);
                    }
                }
            }
        }
        Ok(keys.len())
    }
}

// ── stream-blob framing (length-delimited: [u32 len][bytes]…) ─────────────────

/// Upper bound on a single length-delimited frame, enforced before allocating the
/// read buffer. Sealed chunks are 64 KiB of plaintext + a GCM tag and the header
/// frame is smaller still, so 8 MiB is ~128× the largest legitimate frame while
/// refusing an attacker/tamper-controlled length prefix (up to ~4 GiB) that would
/// otherwise force a huge allocation *before* GCM authentication runs.
const MAX_FRAME_LEN: usize = 8 * 1024 * 1024;

async fn write_frame<W: AsyncWriteExt + Unpin>(w: &mut W, data: &[u8]) -> Result<()> {
    w.write_all(&(data.len() as u32).to_le_bytes()).await?;
    w.write_all(data).await?;
    Ok(())
}

/// Async length-delimited frame read; `None` at clean EOF (before a length prefix).
async fn read_frame_async<R: AsyncRead + Unpin>(r: &mut R) -> std::io::Result<Option<Vec<u8>>> {
    let mut len = [0u8; 4];
    match r.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let n = u32::from_le_bytes(len) as usize;
    if n > MAX_FRAME_LEN {
        return Err(io_err("stream frame length exceeds sane bound"));
    }
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).await?;
    Ok(Some(buf))
}

fn io_err(msg: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg.to_string())
}

fn io_map(e: anyhow::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{e:#}"))
}

/// Read one length-delimited frame from a byte slice cursor; `None` at clean EOF.
fn read_frame_sync(cur: &mut &[u8]) -> Result<Option<Vec<u8>>> {
    if cur.is_empty() {
        return Ok(None);
    }
    if cur.len() < 4 {
        anyhow::bail!("truncated frame length prefix");
    }
    let (len_bytes, rest) = cur.split_at(4);
    let n = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
    if n > MAX_FRAME_LEN {
        anyhow::bail!("stream frame length {n} exceeds sane bound {MAX_FRAME_LEN}");
    }
    if rest.len() < n {
        anyhow::bail!("truncated frame body (need {n}, have {})", rest.len());
    }
    let (body, tail) = rest.split_at(n);
    *cur = tail;
    Ok(Some(body.to_vec()))
}

/// Decrypt a streamed (0x03) blob fully into memory (the buffered read path).
fn decrypt_stream_blob(
    provider: &dyn dagron_crypto::KeyProvider,
    raw: &[u8],
) -> Result<Vec<u8>> {
    let mut cur = raw.strip_prefix(&[STREAM_BLOB_TAG]).context("stream blob: missing tag")?;
    let header = read_frame_sync(&mut cur)?.context("stream blob: missing header")?;
    let mut opener = dagron_crypto::StreamOpener::new(provider, &header)?;
    let mut out = Vec::new();
    while let Some(frame) = read_frame_sync(&mut cur)? {
        let (pt, is_final) = opener.open_chunk(&frame)?;
        out.extend_from_slice(&pt);
        if is_final {
            break;
        }
    }
    if !opener.finished() {
        anyhow::bail!("truncated encrypted stream (no final chunk)");
    }
    Ok(out)
}

/// Rotate a streamed (0x03) blob: rewrap only the header frame; the chunk frames
/// (keyed by the same data key) are copied untouched.
fn rotate_stream_blob(
    old: &dyn dagron_crypto::KeyProvider,
    new: &dyn dagron_crypto::KeyProvider,
    raw: &[u8],
) -> Result<Vec<u8>> {
    let mut cur = raw.strip_prefix(&[STREAM_BLOB_TAG]).context("stream blob: missing tag")?;
    let header = read_frame_sync(&mut cur)?.context("stream blob: missing header")?;
    let new_header = dagron_crypto::rewrap_stream_header(old, new, &header)?;
    let mut out = Vec::with_capacity(raw.len());
    out.push(STREAM_BLOB_TAG);
    out.extend_from_slice(&(new_header.len() as u32).to_le_bytes());
    out.extend_from_slice(&new_header);
    out.extend_from_slice(cur); // remaining chunk frames, unchanged
    Ok(out)
}

/// Build the configured programmatic (`put`/`get`) artifact store:
///
/// 1. `DAGRON_ARTIFACT_URL` (s3:// / gs:// / az://) → the [`cloud`] backend —
///    the multi-cloud checkpoint substrate. Takes precedence when both are set
///    (the local dir can still serve the engine's task-visible scratch).
/// 2. else `DAGRON_ARTIFACT_DIR` → the local-FS store.
/// 3. neither → `None` (artifacts unconfigured).
///
/// Either store is transparently wrapped in an [`EncryptedStore`] when a KEK
/// provider is configured (`DAGRON_ENV_KEK_PROVIDER` — BYOK/KMS), so a cloud
/// bucket holds ciphertext. A `DAGRON_ARTIFACT_URL` in a build without the
/// matching backend feature is a hard error, never a silent local fall-back.
///
/// This is the seam for put/get consumers (an artifact API / alternate builds).
/// It intentionally does **not** touch the engine's per-run *shared directory*
/// (`prepare_run_dir` / `DAGRON_ARTIFACTS`) — that plaintext-on-disk file-passing
/// path is a separate mechanism a decorator cannot seal (tasks write raw files
/// straight to the dir).
pub fn store_from_env() -> Result<Option<Arc<dyn ArtifactStore>>> {
    let provider = dagron_crypto::provider_from_env()?;
    if let Some(url) = std::env::var("DAGRON_ARTIFACT_URL").ok().filter(|u| !u.trim().is_empty())
    {
        #[cfg(feature = "cloud")]
        {
            let store = cloud::CloudStore::from_url(&url)?;
            return Ok(Some(match provider {
                Some(p) => Arc::new(EncryptedStore::new(store, p)),
                None => Arc::new(store),
            }));
        }
        #[cfg(not(feature = "cloud"))]
        anyhow::bail!(
            "DAGRON_ARTIFACT_URL is set ('{url}') but this build has no cloud artifact \
             backend — enable the `s3`, `gcs`, or `azure` feature on dagron-artifact \
             (or unset it to use DAGRON_ARTIFACT_DIR)"
        );
    }
    Ok(build_store(LocalFsStore::from_env(), provider))
}

/// Pure assembly (no env reads) so the wiring is unit-testable: wrap `local` in an
/// [`EncryptedStore`] iff a `provider` is present.
fn build_store(
    local: Option<LocalFsStore>,
    provider: Option<Box<dyn dagron_crypto::KeyProvider>>,
) -> Option<Arc<dyn ArtifactStore>> {
    let local = local?;
    Some(match provider {
        Some(p) => Arc::new(EncryptedStore::new(local, p)),
        None => Arc::new(local),
    })
}

/// Store-wide key-rotation orchestrator: re-key every artifact from the previous
/// KEK (`*_OLD` env vars, [`dagron_crypto::old_provider_from_env`]) to the current
/// one ([`store_from_env`]). Returns the number rotated. Errors if artifacts are
/// unconfigured, if the current store is not encrypted, or if no previous KEK is
/// set. Resume-safe: an object already rewrapped onto the current KEK is detected
/// (it no longer unwraps under the retiring KEK but reads under the current one)
/// and skipped, so a sweep interrupted partway can be re-run without error.
pub async fn rotate_store_from_env() -> Result<usize> {
    let store = store_from_env()?
        .context("artifacts not configured (set DAGRON_ARTIFACT_DIR)")?;
    let old = dagron_crypto::old_provider_from_env()?.context(
        "no previous KEK configured (set DAGRON_ENV_KEK_PROVIDER_OLD and its *_OLD vars)",
    )?;
    store.rotate_from(old.as_ref()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn key_path_is_sanitized() {
        let k = ArtifactKey::new("r/1", "../t", "n a");
        assert_eq!(k.rel_path(), "r_1/.._t/n_a");

        // Standalone "." / ".." components must not survive — otherwise they'd
        // resolve to the current/parent directory and escape the store root.
        let k2 = ArtifactKey::new("..", ".", "..");
        assert_eq!(k2.rel_path(), "_/_/_");
        for comp in k2.rel_path().split('/') {
            assert!(comp != "." && comp != "..", "path traversal not blocked: {comp}");
        }
    }

    #[tokio::test]
    async fn local_store_round_trips() {
        let base = std::env::temp_dir().join(format!("dagron-art-{}", std::process::id()));
        let store = LocalFsStore::new(&base);
        let key = ArtifactKey::new("run1", "taskA", "out.txt");

        assert!(!store.exists(&key).await.unwrap());
        let loc = store.put(&key, b"hello").await.unwrap();
        assert!(loc.replace('\\', "/").ends_with("run1/taskA/out.txt"), "got {loc}");
        assert!(store.exists(&key).await.unwrap());
        assert_eq!(store.get(&key).await.unwrap(), b"hello");

        let run_dir = store.prepare_run_dir("run1").await.unwrap();
        assert!(Path::new(&run_dir).is_dir());

        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn noop_store_errors_on_put() {
        let s = NoopStore;
        assert!(s.put(&ArtifactKey::new("r", "t", "n"), b"x").await.is_err());
        assert!(!s.exists(&ArtifactKey::new("r", "t", "n")).await.unwrap());
        assert!(s.run_location("r").is_none());
    }

    #[tokio::test]
    async fn encrypted_store_ciphertext_at_rest_round_trips() {
        let base =
            std::env::temp_dir().join(format!("dagron-enc-{}", std::process::id()));
        let key = ArtifactKey::new("run1", "train", "checkpoint.bin");
        let plaintext = b"MODEL-WEIGHTS-do-not-leak".to_vec();

        let provider = Box::new(dagron_crypto::LocalKekProvider::new([7u8; 32]));
        let enc = EncryptedStore::new(LocalFsStore::new(&base), provider);
        enc.put(&key, &plaintext).await.unwrap();

        // What actually landed on disk is ciphertext — the plaintext must not appear.
        let on_disk = LocalFsStore::new(&base).get(&key).await.unwrap();
        assert_ne!(on_disk, plaintext, "stored bytes must be ciphertext");
        assert!(
            !on_disk.windows(plaintext.len()).any(|w| w == plaintext.as_slice()),
            "plaintext must not appear anywhere in the stored artifact"
        );
        assert_eq!(on_disk[0], 0x02, "binary envelope tag");

        // The decorator round-trips + delegates exists; no plaintext shared dir.
        assert_eq!(enc.get(&key).await.unwrap(), plaintext);
        assert!(enc.exists(&key).await.unwrap());
        assert!(enc.run_location("run1").is_none(), "no plaintext task dir under encryption");

        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn build_store_wraps_with_encryption_only_when_a_provider_is_present() {
        let base = std::env::temp_dir().join(format!("dagron-fac-{}", std::process::id()));
        let key = ArtifactKey::new("r", "t", "a.bin");

        // With a provider → ciphertext at rest.
        let enc = build_store(
            Some(LocalFsStore::new(&base)),
            Some(Box::new(dagron_crypto::LocalKekProvider::new([8u8; 32]))),
        )
        .unwrap();
        enc.put(&key, b"weights").await.unwrap();
        assert_ne!(LocalFsStore::new(&base).get(&key).await.unwrap(), b"weights");
        assert_eq!(enc.get(&key).await.unwrap(), b"weights");

        // Without a provider → plaintext (unchanged behavior).
        let key2 = ArtifactKey::new("r", "t", "b.bin");
        let plain = build_store(Some(LocalFsStore::new(&base)), None).unwrap();
        plain.put(&key2, b"weights").await.unwrap();
        assert_eq!(LocalFsStore::new(&base).get(&key2).await.unwrap(), b"weights");

        // No local dir → no store.
        assert!(build_store(None, None).is_none());

        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn encrypted_store_wrong_kek_cannot_read() {
        let base =
            std::env::temp_dir().join(format!("dagron-enc-wrong-{}", std::process::id()));
        let key = ArtifactKey::new("r", "t", "a.bin");

        EncryptedStore::new(LocalFsStore::new(&base), Box::new(dagron_crypto::LocalKekProvider::new([1u8; 32])))
            .put(&key, b"s3cret")
            .await
            .unwrap();
        // A different KEK cannot decrypt what a prior KEK sealed.
        let other = EncryptedStore::new(
            LocalFsStore::new(&base),
            Box::new(dagron_crypto::LocalKekProvider::new([2u8; 32])),
        );
        assert!(other.get(&key).await.is_err(), "wrong KEK must not decrypt");

        let _ = std::fs::remove_dir_all(&base);
    }

    fn reader(bytes: Vec<u8>) -> Box<dyn AsyncRead + Send + Unpin> {
        Box::new(std::io::Cursor::new(bytes))
    }

    #[tokio::test]
    async fn streaming_get_stream_decrypts_both_formats() {
        let base = std::env::temp_dir().join(format!("dagron-gs-{}", std::process::id()));
        let enc = EncryptedStore::new(
            LocalFsStore::new(&base),
            Box::new(dagron_crypto::LocalKekProvider::new([7u8; 32])),
        );
        // whole-blob (0x02) via put
        let kw = ArtifactKey::new("r", "t", "whole");
        enc.put(&kw, b"small-secret").await.unwrap();
        // streamed (0x03) multi-chunk via put_stream
        let ks = ArtifactKey::new("r", "t", "big");
        let big: Vec<u8> = (0..200_000).map(|i| (i % 251) as u8).collect();
        enc.put_stream(&ks, reader(big.clone())).await.unwrap();

        for (key, want) in [(&kw, b"small-secret".to_vec()), (&ks, big.clone())] {
            let mut r = enc.get_stream(key).await.unwrap();
            let mut out = Vec::new();
            r.read_to_end(&mut out).await.unwrap();
            assert_eq!(out, want, "get_stream decrypts");
        }
        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn streaming_put_round_trips_multi_chunk_encrypted() {
        let base = std::env::temp_dir().join(format!("dagron-strm-{}", std::process::id()));
        let key = ArtifactKey::new("run", "train", "ckpt.bin");
        // > one 64 KiB chunk so the chunked path (not a single final chunk) runs.
        let data: Vec<u8> = (0..200_000).map(|i| (i % 251) as u8).collect();

        let enc = EncryptedStore::new(
            LocalFsStore::new(&base),
            Box::new(dagron_crypto::LocalKekProvider::new([7u8; 32])),
        );
        enc.put_stream(&key, reader(data.clone())).await.unwrap();

        // On disk: streamed wire format, and the plaintext is absent.
        let on_disk = LocalFsStore::new(&base).get(&key).await.unwrap();
        assert_eq!(on_disk[0], STREAM_BLOB_TAG, "streamed wire tag");
        assert!(on_disk.windows(16).all(|w| w != &data[..16]), "no plaintext prefix on disk");
        // Round-trips through the format-aware get.
        assert_eq!(enc.get(&key).await.unwrap(), data);
        // Wrong KEK cannot read the streamed blob either.
        let wrong = EncryptedStore::new(
            LocalFsStore::new(&base),
            Box::new(dagron_crypto::LocalKekProvider::new([8u8; 32])),
        );
        assert!(wrong.get(&key).await.is_err());

        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn list_enumerates_stored_keys() {
        let base = std::env::temp_dir().join(format!("dagron-list-{}", std::process::id()));
        let s = LocalFsStore::new(&base);
        s.put(&ArtifactKey::new("r1", "t1", "a"), b"x").await.unwrap();
        s.put(&ArtifactKey::new("r1", "t2", "b"), b"y").await.unwrap();
        s.put(&ArtifactKey::new("r2", "t1", "c"), b"z").await.unwrap();
        let mut got: Vec<String> = s.list().await.unwrap().iter().map(|k| k.rel_path()).collect();
        got.sort();
        assert_eq!(got, vec!["r1/t1/a", "r1/t2/b", "r2/t1/c"]);
        // Missing root → empty, not an error.
        assert!(LocalFsStore::new(base.join("nope")).list().await.unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn rotate_from_rekeys_whole_blob_and_streamed_artifacts() {
        let base = std::env::temp_dir().join(format!("dagron-rot-{}", std::process::id()));
        let whole = ArtifactKey::new("r", "t", "whole.bin");
        let streamed = ArtifactKey::new("r", "t", "streamed.bin");
        let old_p = || Box::new(dagron_crypto::LocalKekProvider::with_id([1u8; 32], "k1"));
        let new_p = || Box::new(dagron_crypto::LocalKekProvider::with_id([2u8; 32], "k2"));
        let big: Vec<u8> = (0..150_000).map(|i| (i % 253) as u8).collect();

        // Write one whole-blob + one streamed artifact under the OLD KEK.
        let old_store = EncryptedStore::new(LocalFsStore::new(&base), old_p());
        old_store.put(&whole, b"weights-v1").await.unwrap();
        old_store.put_stream(&streamed, reader(big.clone())).await.unwrap();

        // Rotate everything to the NEW KEK.
        let new_store = EncryptedStore::new(LocalFsStore::new(&base), new_p());
        let n = new_store.rotate_from(old_p().as_ref()).await.unwrap();
        assert_eq!(n, 2, "both artifacts rotated");

        // New KEK reads both; old KEK now reads neither.
        assert_eq!(new_store.get(&whole).await.unwrap(), b"weights-v1");
        assert_eq!(new_store.get(&streamed).await.unwrap(), big);
        let old_again = EncryptedStore::new(LocalFsStore::new(&base), old_p());
        assert!(old_again.get(&whole).await.is_err(), "old KEK locked out (whole)");
        assert!(old_again.get(&streamed).await.is_err(), "old KEK locked out (streamed)");

        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn rotate_from_resume_skips_already_current_artifacts() {
        // A sweep interrupted partway leaves some artifacts already on the NEW KEK
        // and some still on the OLD one. Re-running rotate_from must migrate the
        // stragglers and skip the already-current ones (which no longer unwrap under
        // OLD) rather than aborting the whole loop on the first already-migrated one.
        let base =
            std::env::temp_dir().join(format!("dagron-rot-resume-{}", std::process::id()));
        let old_p = || Box::new(dagron_crypto::LocalKekProvider::with_id([1u8; 32], "k1"));
        let new_p = || Box::new(dagron_crypto::LocalKekProvider::with_id([2u8; 32], "k2"));
        let straggler = ArtifactKey::new("r", "t", "still-old.bin");
        let migrated = ArtifactKey::new("r", "t", "already-new.bin");
        let big: Vec<u8> = (0..150_000).map(|i| (i % 253) as u8).collect();

        // `straggler` (whole-blob) still under OLD; `migrated` (streamed) already NEW.
        EncryptedStore::new(LocalFsStore::new(&base), old_p())
            .put(&straggler, b"weights-v1")
            .await
            .unwrap();
        EncryptedStore::new(LocalFsStore::new(&base), new_p())
            .put_stream(&migrated, reader(big.clone()))
            .await
            .unwrap();

        // Resume the sweep: rotate OLD → NEW. Must not error on the already-new one.
        let new_store = EncryptedStore::new(LocalFsStore::new(&base), new_p());
        let n = new_store.rotate_from(old_p().as_ref()).await.unwrap();
        assert_eq!(n, 2, "both artifacts accounted for on the new KEK");

        // Both read under NEW; the straggler is now locked out of OLD too.
        assert_eq!(new_store.get(&straggler).await.unwrap(), b"weights-v1");
        assert_eq!(new_store.get(&migrated).await.unwrap(), big);
        assert!(
            EncryptedStore::new(LocalFsStore::new(&base), old_p())
                .get(&straggler)
                .await
                .is_err(),
            "straggler rotated off the old KEK",
        );

        let _ = std::fs::remove_dir_all(&base);
    }
}
