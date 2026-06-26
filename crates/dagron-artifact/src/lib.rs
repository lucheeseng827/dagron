//! Artifact store seam — task data passing (XCom / Argo-artifact parity).
//!
//! [`ArtifactStore`] abstracts *where* task artifacts live so the OSS engine ships
//! a zero-infra local-filesystem store while an S3/GCS backend plugs in behind the
//! same trait. Tasks in a run share a per-run location (the engine injects
//! `DAGRON_ARTIFACTS`) to pass files; the trait is the programmatic surface the API
//! and downstream editions use to read/write artifacts by key.

use std::path::PathBuf;

use anyhow::Result;
use async_trait::async_trait;

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
}

/// Artifacts disabled — the OSS default when unconfigured. Every operation errors
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
}
