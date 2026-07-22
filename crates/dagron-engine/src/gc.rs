//! Retention GC (v6) — with optional archive-before-purge.
//!
//! The datastore is the source of truth, which means it also accumulates every
//! run's history forever unless something reclaims it. This loop enforces a
//! retention window: terminal runs (`succeeded`/`failed`/`cancelled`) whose
//! `finished_at` is older than `retention` are purged with their tasks,
//! dependency edges, and now-orphaned definitions in one transaction
//! ([`db::gc_old_runs`]).
//!
//! **Archive-before-purge** (ee/STATE_STORE.md hot/cold split): when an
//! [`ArchiveSink`] is configured, each eligible run is first exported as a
//! self-contained JSON document ([`db::archivable_runs`] — run + definition +
//! tasks + outbox events) and only the runs whose export verifiably landed are
//! purged ([`db::purge_runs_by_id`]). A failed write skips that run's purge —
//! history never leaves the hot store before it durably exists elsewhere. Two
//! sinks:
//!
//! * `GC_ARCHIVE_DIR` — a local directory (point it at an object-store-synced
//!   PVC): atomic tmp → fsync → rename, then a parent-dir fsync.
//! * `GC_ARCHIVE_URL=s3://bucket/prefix` (feature `archive-s3`) — S3-native:
//!   one `PUT` per run document is the durability boundary (S3 PUTs are
//!   atomic); credentials/region from the standard `AWS_*` env.
//!
//! Idempotent either way: re-archiving after a crash overwrites the same
//! `run-<id>.json` object.
//!
//! Like cron, GC is **singleton-gated** — only the [leadership](crate::leadership)
//! holder sweeps, so N schedulers don't all race the same deletes. It is safe if
//! they did (the deletes are idempotent), but gating keeps the work to one node.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tracing::{info, warn};

use crate::db;

/// Where archive-before-purge writes run documents. Constructed once at
/// startup via [`ArchiveSink::from_env`]; `None` = plain purge (v6 behavior).
pub enum ArchiveSink {
    /// `GC_ARCHIVE_DIR`: local directory, atomic write + dir fsync.
    Dir(PathBuf),
    /// `GC_ARCHIVE_URL=<s3|gs|az>://bucket/prefix` (features `archive-s3` /
    /// `archive-gcs` / `archive-azure`).
    #[cfg(feature = "archive-cloud")]
    Object {
        store: Arc<dyn object_store::ObjectStore>,
        prefix: object_store::path::Path,
    },
}

impl ArchiveSink {
    /// Resolve the sink from env. `GC_ARCHIVE_URL` wins over `GC_ARCHIVE_DIR`;
    /// neither = `Ok(None)`. A URL without the `archive-s3` feature is a hard
    /// config error — never silently purge unarchived history because the
    /// build was lean.
    pub fn from_env() -> Result<Option<Self>> {
        if let Ok(url) = std::env::var("GC_ARCHIVE_URL") {
            let url = url.trim().to_string();
            if !url.is_empty() {
                #[cfg(feature = "archive-cloud")]
                {
                    // Scheme (s3/gs/az) dispatched by objstore::from_url;
                    // credentials/region/endpoint from the backend's standard env.
                    let (store, prefix) = crate::objstore::from_url(&url)?;
                    return Ok(Some(Self::Object { store, prefix }));
                }
                #[cfg(not(feature = "archive-cloud"))]
                anyhow::bail!(
                    "GC_ARCHIVE_URL is set but this build lacks a cloud archive feature \
                     (archive-s3 / archive-gcs / archive-azure) — rebuild with one or use \
                     GC_ARCHIVE_DIR"
                );
            }
        }
        Ok(std::env::var("GC_ARCHIVE_DIR")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .map(|v| Self::Dir(PathBuf::from(v.trim()))))
    }

    /// Durably write one run's archive document; returning `Ok` is the
    /// purge-permission signal, so each backend's success must mean "survives
    /// a crash" (fsync chain locally, completed PUT on S3).
    async fn write(&self, run_id: &str, doc: &serde_json::Value) -> Result<()> {
        match self {
            Self::Dir(dir) => {
                write_archive(dir, run_id, doc)?;
                Ok(())
            }
            #[cfg(feature = "archive-cloud")]
            Self::Object { store, prefix } => {
                let path = prefix.child(format!("run-{run_id}.json"));
                let bytes = serde_json::to_vec(doc)?;
                store
                    .put(&path, object_store::PutPayload::from(bytes))
                    .await
                    .map_err(|e| anyhow::anyhow!("archive PUT {path}: {e}"))?;
                Ok(())
            }
        }
    }
}

impl std::fmt::Debug for ArchiveSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dir(dir) => f.debug_tuple("Dir").field(dir).finish(),
            #[cfg(feature = "archive-cloud")]
            Self::Object { prefix, .. } => f.debug_tuple("Object").field(&prefix.as_ref()).finish(),
        }
    }
}

/// Runs exported+purged per DB round-trip.
const ARCHIVE_BATCH: i64 = 100;
/// Batches per sweep — bounds one sweep's work; the remainder waits for the
/// next interval rather than starving the tick.
const MAX_BATCHES_PER_SWEEP: usize = 50;

/// Run the GC sweep loop until the process exits. `retention_secs` is the age
/// past `finished_at` after which a terminal run is eligible for deletion;
/// `interval_secs` is how often to sweep. With an [`ArchiveSink`], runs are
/// exported there before being purged (and kept if the export fails).
pub async fn run(
    pool: db::Pool,
    retention_secs: i64,
    interval_secs: u64,
    is_leader: Arc<AtomicBool>,
    archive: Option<ArchiveSink>,
) {
    // A non-positive retention would make the cutoff `now` or a *future* time,
    // deleting all (or recent) terminal runs. Refuse to run rather than risk
    // mass deletion. (main.rs already filters to > 0; this is the backstop.)
    if retention_secs <= 0 {
        warn!(retention_secs, "retention GC disabled: retention_secs must be > 0");
        return;
    }
    let interval = Duration::from_secs(interval_secs.max(1));
    info!(retention_secs, interval_secs, archive = ?archive, "retention GC loop running");
    loop {
        tokio::time::sleep(interval).await;
        if !is_leader.load(Ordering::SeqCst) {
            continue;
        }
        let cutoff = (chrono::Utc::now() - chrono::TimeDelta::seconds(retention_secs)).to_rfc3339();
        match &archive {
            Some(sink) => match sweep_archive(&pool, &cutoff, sink).await {
                Ok((0, _)) => {}
                Ok((archived, purged)) => {
                    info!(archived, purged, %cutoff, "retention GC archived + purged old runs")
                }
                Err(e) => warn!(error = %e, "retention GC archive sweep failed"),
            },
            None => match db::gc_old_runs(&pool, &cutoff).await {
                Ok(0) => {}
                Ok(n) => info!(deleted = n, %cutoff, "retention GC purged old runs"),
                Err(e) => warn!(error = %e, "retention GC sweep failed"),
            },
        }
    }
}

/// One archive-then-purge sweep: batches of eligible runs are exported to the
/// sink and exactly the successfully-exported ids are purged. Returns
/// `(archived, purged)` totals. Bounded by [`MAX_BATCHES_PER_SWEEP`].
pub async fn sweep_archive(
    pool: &db::Pool,
    cutoff: &str,
    sink: &ArchiveSink,
) -> anyhow::Result<(usize, u64)> {
    let mut total_archived = 0usize;
    let mut total_purged = 0u64;
    for _ in 0..MAX_BATCHES_PER_SWEEP {
        let batch = db::archivable_runs(pool, cutoff, ARCHIVE_BATCH).await?;
        if batch.is_empty() {
            break;
        }
        let mut archived_ids = Vec::with_capacity(batch.len());
        let mut write_failed = false;
        for (run_id, doc) in &batch {
            match sink.write(run_id, doc).await {
                Ok(()) => {
                    // Index before purge: the `archived_runs` row is what keeps
                    // the run listable/fetchable once the hot rows are gone.
                    // An index failure fails closed like a write failure — an
                    // archived-but-unlisted run would be invisible history.
                    let run = &doc["run"];
                    if let Err(e) = db::index_archived_run(
                        pool,
                        run_id,
                        run["definition_name"].as_str().unwrap_or(""),
                        run["status"].as_str().unwrap_or("unknown"),
                        run["created_at"].as_str(),
                        run["finished_at"].as_str(),
                    )
                    .await
                    {
                        warn!(%run_id, error = %e, "archive index write failed — run kept in hot store");
                        write_failed = true;
                        break;
                    }
                    archived_ids.push(run_id.clone());
                }
                Err(e) => {
                    // Fail closed for THIS run: no purge without a verified
                    // archive. Stop the sweep — a full disk / bad bucket would
                    // fail every write; retry next interval instead of looping.
                    warn!(%run_id, error = %e, "archive write failed — run kept in hot store");
                    write_failed = true;
                    break;
                }
            }
        }
        total_archived += archived_ids.len();
        total_purged += db::purge_runs_by_id(pool, &archived_ids).await?;
        if write_failed || archived_ids.len() < batch.len() {
            break;
        }
    }
    Ok((total_archived, total_purged))
}

/// Atomically write one run's archive document: tmp file → fsync → rename to
/// `run-<id>.json`. Overwriting an existing archive (a crash between archive
/// and purge) is the idempotent re-do, not an error.
fn write_archive(dir: &Path, run_id: &str, doc: &serde_json::Value) -> std::io::Result<PathBuf> {
    use std::io::Write;
    std::fs::create_dir_all(dir)?;
    let final_path = dir.join(format!("run-{run_id}.json"));
    let tmp_path = dir.join(format!(".run-{run_id}.json.tmp"));
    let mut f = std::fs::File::create(&tmp_path)?;
    serde_json::to_writer(&mut f, doc).map_err(std::io::Error::other)?;
    f.flush()?;
    f.sync_all()?;
    std::fs::rename(&tmp_path, &final_path)?;
    // The rename's directory-entry update is not durable until the parent
    // directory is fsynced too — without this, a crash after the (verified)
    // archive+purge could revert the entry and lose the run entirely.
    // Propagate failure so the caller keeps the run in the hot store.
    std::fs::File::open(dir)?.sync_all()?;
    Ok(final_path)
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;
    use dagron_core::dag::DagGraph;

    /// Archive-before-purge end to end: a terminal run past the cutoff is
    /// exported as a complete JSON document and only then removed from the hot
    /// store; a second sweep finds nothing; a non-terminal run is untouched.
    #[tokio::test]
    async fn archive_sweep_exports_then_purges() {
        let db_path =
            std::env::temp_dir().join(format!("module54_gc_test_{}.db", uuid::Uuid::new_v4()));
        let dir = std::env::temp_dir().join(format!("module54_gc_arch_{}", uuid::Uuid::new_v4()));
        let pool = db::init_pool(db_path.to_str().unwrap()).await.unwrap();

        let yaml = "name: t\nrunner_class: etl\ntasks:\n  - name: a\n    command: [\"true\"]\n  - name: b\n    command: [\"true\"]\n    depends_on: [a]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let old_run = db::create_run(&pool, &dag, yaml).await.unwrap();
        let live_run = db::create_run(&pool, &dag, yaml).await.unwrap();

        // Force the first run terminal and past the cutoff; leave the second live.
        sqlx::query(
            "UPDATE workflow_runs SET status='succeeded', finished_at='2020-01-01T00:00:00+00:00' WHERE id = ?",
        )
        .bind(&old_run)
        .execute(&pool)
        .await
        .unwrap();

        let cutoff = chrono::Utc::now().to_rfc3339();
        let sink = ArchiveSink::Dir(dir.clone());
        let (archived, purged) = sweep_archive(&pool, &cutoff, &sink).await.unwrap();
        assert_eq!((archived, purged), (1, 1));

        // The archive document is complete: run + both tasks (with their
        // runner_class) + definition spec.
        let doc: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(dir.join(format!("run-{old_run}.json"))).unwrap(),
        )
        .unwrap();
        assert_eq!(doc["format"], "dagron.run-archive.v1");
        assert_eq!(doc["run"]["id"], old_run.as_str());
        assert_eq!(doc["run"]["definition_name"], "t");
        assert_eq!(doc["tasks"].as_array().unwrap().len(), 2);
        assert_eq!(doc["tasks"][0]["runner_class"], "etl");

        // Hot store: the old run is gone, the live one intact.
        let left: Vec<String> = sqlx::query_scalar("SELECT id FROM workflow_runs")
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(left, vec![live_run.clone()]);

        // The archive index maps the purged run for history reads.
        let (idx_name, idx_status, idx_compacted): (String, String, Option<String>) =
            sqlx::query_as(
                "SELECT name, status, compacted_at FROM archived_runs WHERE run_id = ?",
            )
            .bind(&old_run)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!((idx_name.as_str(), idx_status.as_str()), ("t", "succeeded"));
        assert!(idx_compacted.is_none(), "not compacted yet");
        let orphan_tasks: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM task_runs WHERE run_id = ?")
                .bind(&old_run)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(orphan_tasks, 0);

        // Idempotent: nothing left to archive.
        let again = sweep_archive(&pool, &cutoff, &sink).await.unwrap();
        assert_eq!(again, (0, 0));

        pool.close().await;
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The object-store sink (any cloud archive feature), exercised against an
    /// in-memory store: same archive-then-purge contract as the dir sink, with
    /// the document landing under the configured prefix.
    #[cfg(feature = "archive-cloud")]
    #[tokio::test]
    async fn archive_sweep_object_store_sink() {
        let db_path =
            std::env::temp_dir().join(format!("module54_gc_s3_test_{}.db", uuid::Uuid::new_v4()));
        let pool = db::init_pool(db_path.to_str().unwrap()).await.unwrap();

        let yaml = "name: t\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let run_id = db::create_run(&pool, &dag, yaml).await.unwrap();
        sqlx::query(
            "UPDATE workflow_runs SET status='succeeded', finished_at='2020-01-01T00:00:00+00:00' WHERE id = ?",
        )
        .bind(&run_id)
        .execute(&pool)
        .await
        .unwrap();

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let sink = ArchiveSink::Object {
            store: Arc::clone(&store),
            prefix: object_store::path::Path::from("cells/ws_x"),
        };
        let cutoff = chrono::Utc::now().to_rfc3339();
        let (archived, purged) = sweep_archive(&pool, &cutoff, &sink).await.unwrap();
        assert_eq!((archived, purged), (1, 1));

        let obj = store
            .get(&object_store::path::Path::from(format!("cells/ws_x/run-{run_id}.json")))
            .await
            .expect("archived object exists under the prefix");
        let doc: serde_json::Value =
            serde_json::from_slice(&obj.bytes().await.unwrap()).unwrap();
        assert_eq!(doc["format"], "dagron.run-archive.v1");
        assert_eq!(doc["run"]["id"], run_id.as_str());

        let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM workflow_runs")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(left, 0, "hot store purged after verified PUT");

        pool.close().await;
        let _ = std::fs::remove_file(&db_path);
    }
}
