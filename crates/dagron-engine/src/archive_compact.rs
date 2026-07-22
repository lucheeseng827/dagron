//! `dagron archive-compact` — fold archived run documents into Parquet.
//!
//! The third tier of the hot/cold story (ee/STATE_STORE.md): hot Postgres
//! (days) → per-run `run-<id>.json` documents in the archive sink (months,
//! individually retrievable via dagron-api's `/api/archive` endpoints) →
//! **date-partitioned Parquet** (analytics, forever). This subcommand walks
//! the sink, takes documents older than `GC_ARCHIVE_COMPACT_MIN_AGE_DAYS`
//! (default 30 — younger documents stay individually retrievable), flattens
//! them to one row per task, and writes
//! `compact/tasks/dt=<YYYY-MM-DD>/part-<uuid>.parquet` files.
//!
//! Safety contract mirrors the GC's archive-before-purge:
//! * a source document is deleted **only after** the Parquet part file
//!   containing its rows verifiably landed (`put` returned) and the
//!   `archived_runs` index row was stamped (`compacted_at` + `parquet_path`);
//! * a crash in between re-compacts the surviving documents into a new part
//!   file — **at-least-once**, so analytics queries dedup by
//!   `(run_id, task_id)` (documented in the dataset comment below).
//!
//! Run it as a k8s CronJob with the same env as the engine (`GC_ARCHIVE_DIR`
//! or `GC_ARCHIVE_URL` + `AWS_*`, plus `DATABASE_URL` so the index gets
//! stamped). One shot per invocation, bounded per sweep.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use object_store::{ObjectStore, PutPayload};
use tracing::{info, warn};

use crate::db;

/// Documents compacted per invocation, at most. A CronJob catches up across
/// runs rather than one invocation running unbounded.
const MAX_DOCS_PER_SWEEP: usize = 5_000;

/// One flattened row per task (run-level columns denormalized onto every row —
/// GROUP BY run_id reconstructs run-level views). All timestamps stay RFC-3339
/// strings: lossless, portable, and sortable lexicographically.
fn dataset_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("run_id", DataType::Utf8, false),
        Field::new("workflow", DataType::Utf8, true),
        Field::new("run_status", DataType::Utf8, true),
        Field::new("run_created_at", DataType::Utf8, true),
        Field::new("run_finished_at", DataType::Utf8, true),
        Field::new("task_id", DataType::Utf8, true),
        Field::new("task_name", DataType::Utf8, true),
        Field::new("task_status", DataType::Utf8, true),
        Field::new("runner_class", DataType::Utf8, true),
        Field::new("attempt", DataType::Int64, true),
        Field::new("scheduled_at", DataType::Utf8, true),
        Field::new("finished_at", DataType::Utf8, true),
        Field::new("output", DataType::Utf8, true),
    ]))
}

/// Flatten one `dagron.run-archive.v1` document into task rows (plus a
/// run-only row when a document somehow has no tasks, so the run is never
/// silently dropped from the dataset).
fn flatten(doc: &serde_json::Value) -> Vec<serde_json::Value> {
    let run = &doc["run"];
    let base = |task: Option<&serde_json::Value>| {
        serde_json::json!({
            "run_id": run["id"],
            "workflow": run["definition_name"],
            "run_status": run["status"],
            "run_created_at": run["created_at"],
            "run_finished_at": run["finished_at"],
            "task_id": task.map(|t| t["id"].clone()),
            "task_name": task.map(|t| t["name"].clone()),
            "task_status": task.map(|t| t["status"].clone()),
            "runner_class": task.map(|t| t["runner_class"].clone()),
            "attempt": task.map(|t| t["attempt"].clone()),
            "scheduled_at": task.map(|t| t["scheduled_at"].clone()),
            "finished_at": task.map(|t| t["finished_at"].clone()),
            "output": task.map(|t| t["output"].clone()),
        })
    };
    match doc["tasks"].as_array() {
        Some(tasks) if !tasks.is_empty() => tasks.iter().map(|t| base(Some(t))).collect(),
        _ => vec![base(None)],
    }
}

/// The `dt=` partition for a document: the run's finish date (UTC), falling
/// back to its creation date, else a literal `unknown` partition.
fn partition_of(doc: &serde_json::Value) -> String {
    let run = &doc["run"];
    run["finished_at"]
        .as_str()
        .or_else(|| run["created_at"].as_str())
        .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
        .map(|t| t.date_naive().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Encode rows into one Parquet file's bytes (in memory — part files are
/// bounded by [`MAX_DOCS_PER_SWEEP`], not unbounded history).
fn to_parquet(rows: &[serde_json::Value]) -> Result<Vec<u8>> {
    let schema = dataset_schema();
    let mut decoder = arrow_json::ReaderBuilder::new(Arc::clone(&schema)).build_decoder()?;
    decoder.serialize(rows)?;
    let batch: RecordBatch =
        decoder.flush()?.context("no record batch produced from archive rows")?;
    let mut buf = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buf, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(buf)
}

/// One bounded compaction sweep over `store`. `pool` (when given) gets the
/// `archived_runs` rows stamped with `compacted_at` + the part path; without
/// it the stamp is skipped with a warning (history reads then can't tell a
/// compacted run from a missing one). Returns `(docs_compacted, parts_written)`.
pub async fn compact_once(
    store: &Arc<dyn ObjectStore>,
    pool: Option<&db::Pool>,
    min_age_days: i64,
) -> Result<(usize, usize)> {
    // Eligibility by the object's own mtime — listable for free and monotone
    // with archive time; the partition key still comes from the run document.
    let cutoff = chrono::Utc::now() - chrono::TimeDelta::days(min_age_days.max(0));
    // Delimiter listing = top-level objects only: the per-run documents live at
    // the root and our own compact/ output is a common prefix the listing never
    // descends into — sweep I/O stays proportional to the pending backlog, not
    // to every part file ever written.
    let listing = store.list_with_delimiter(None).await?;
    let mut eligible = Vec::new();
    for meta in listing.objects {
        let name = meta.location.filename().unwrap_or("");
        if name.starts_with("run-") && name.ends_with(".json") && meta.last_modified <= cutoff {
            eligible.push(meta.location);
            if eligible.len() >= MAX_DOCS_PER_SWEEP {
                break;
            }
        }
    }
    if eligible.is_empty() {
        return Ok((0, 0));
    }

    // Group documents (and their flattened rows) by date partition.
    use std::collections::BTreeMap;
    let mut partitions: BTreeMap<String, (Vec<object_store::path::Path>, Vec<String>, Vec<serde_json::Value>)> =
        BTreeMap::new();
    for location in eligible {
        let bytes = store.get(&location).await?.bytes().await?;
        let doc: serde_json::Value = match serde_json::from_slice(&bytes) {
            Ok(doc) => doc,
            Err(e) => {
                // A corrupt document is left in place for a human — compaction
                // must never delete what it could not represent.
                warn!(location = %location, error = %e, "unparseable archive document skipped");
                continue;
            }
        };
        let run_id = doc["run"]["id"].as_str().unwrap_or("").to_string();
        let entry = partitions.entry(partition_of(&doc)).or_default();
        entry.2.extend(flatten(&doc));
        entry.0.push(location);
        entry.1.push(run_id);
    }

    let mut docs_compacted = 0usize;
    let mut parts_written = 0usize;
    for (dt, (locations, run_ids, rows)) in partitions {
        let part_path = object_store::path::Path::from(format!(
            "compact/tasks/dt={dt}/part-{}.parquet",
            uuid::Uuid::new_v4()
        ));
        // A JSON-parseable document can still violate the Arrow schema (null
        // run_id, non-numeric attempt, …). That must skip THIS partition and
        // keep sweeping — propagating would re-block the same partitions every
        // future sweep until a human noticed. Sources stay in place for one.
        let bytes = match to_parquet(&rows) {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!(%dt, error = %e, "partition skipped — schema-incompatible archive document(s) left in place");
                continue;
            }
        };
        store
            .put(&part_path, PutPayload::from(bytes))
            .await
            .map_err(|e| anyhow::anyhow!("parquet PUT {part_path}: {e}"))?;
        parts_written += 1;

        // Stamp the index, then delete sources. Order matters: a crash after
        // the PUT leaves documents present → re-compacted next sweep
        // (at-least-once, dedup on (run_id, task_id) at query time).
        if let Some(pool) = pool {
            db::mark_runs_compacted(pool, &run_ids, part_path.as_ref()).await?;
        }
        for location in &locations {
            store.delete(location).await?;
            docs_compacted += 1;
        }
        info!(%dt, part = %part_path, docs = locations.len(), "compacted archive partition");
    }
    Ok((docs_compacted, parts_written))
}

/// `dagron archive-compact [db_target]` — resolve the sink + optional
/// datastore from the environment and run one bounded sweep.
pub async fn run_cli(args: &[String]) -> Result<()> {
    let store = store_from_env()?;
    // The datastore is where archived_runs gets stamped. Optional (warn-skip)
    // so the compactor still works against a sink whose engine DB is gone.
    let db_target = args.first().cloned().or_else(|| std::env::var("DATABASE_URL").ok());
    let pool = match &db_target {
        Some(target) => Some(db::init_pool(target).await.context("connecting datastore")?),
        None => {
            warn!("no db target/DATABASE_URL — archived_runs will not be stamped as compacted");
            None
        }
    };
    let min_age_days: i64 = std::env::var("GC_ARCHIVE_COMPACT_MIN_AGE_DAYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    let (docs, parts) = compact_once(&store, pool.as_ref(), min_age_days).await?;
    info!(docs, parts, min_age_days, "archive compaction sweep complete");
    println!("compacted {docs} document(s) into {parts} parquet part file(s)");
    Ok(())
}

/// The archive sink as an [`ObjectStore`] view: `GC_ARCHIVE_URL` (s3/gs/az,
/// needs the matching `archive-s3`/`archive-gcs`/`archive-azure` feature too)
/// wins over `GC_ARCHIVE_DIR` (local dir).
fn store_from_env() -> Result<Arc<dyn ObjectStore>> {
    if let Ok(url) = std::env::var("GC_ARCHIVE_URL") {
        let url = url.trim().to_string();
        if !url.is_empty() {
            #[cfg(feature = "archive-cloud")]
            {
                let (store, prefix) = crate::objstore::from_url(&url)?;
                // Scope the whole view under the prefix so listing/putting is
                // relative, matching the dir sink's shape.
                return Ok(Arc::new(object_store::prefix::PrefixStore::new(store, prefix)));
            }
            #[cfg(not(feature = "archive-cloud"))]
            anyhow::bail!(
                "GC_ARCHIVE_URL requires a cloud archive feature (archive-s3 / archive-gcs / \
                 archive-azure) alongside archive-parquet"
            );
        }
    }
    let dir = std::env::var("GC_ARCHIVE_DIR")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .context("archive-compact needs GC_ARCHIVE_DIR or GC_ARCHIVE_URL")?;
    std::fs::create_dir_all(dir.trim())?;
    Ok(Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(dir.trim())
            .context("opening GC_ARCHIVE_DIR")?,
    ))
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;
    use dagron_core::dag::DagGraph;

    /// The full third tier: GC archives runs to JSON (indexed), compaction
    /// folds them into a date-partitioned Parquet part, stamps the index,
    /// and deletes the source documents; the parquet reads back with the
    /// expected task rows. A second sweep is a no-op.
    #[tokio::test]
    async fn compacts_archive_documents_into_parquet() {
        let db_path = std::env::temp_dir()
            .join(format!("module54_compact_test_{}.db", uuid::Uuid::new_v4()));
        let dir =
            std::env::temp_dir().join(format!("module54_compact_arch_{}", uuid::Uuid::new_v4()));
        let pool = db::init_pool(db_path.to_str().unwrap()).await.unwrap();

        // Two runs → archive JSON via the GC path (same finish date partition).
        let yaml = "name: t\nrunner_class: etl\ntasks:\n  - name: a\n    command: [\"true\"]\n  - name: b\n    command: [\"true\"]\n    depends_on: [a]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        let mut run_ids = Vec::new();
        for _ in 0..2 {
            let id = db::create_run(&pool, &dag, yaml).await.unwrap();
            sqlx::query(
                "UPDATE workflow_runs SET status='succeeded', finished_at='2020-01-05T12:00:00+00:00' WHERE id = ?",
            )
            .bind(&id)
            .execute(&pool)
            .await
            .unwrap();
            run_ids.push(id);
        }
        let sink = crate::gc::ArchiveSink::Dir(dir.clone());
        let cutoff = chrono::Utc::now().to_rfc3339();
        let swept = crate::gc::sweep_archive(&pool, &cutoff, &sink).await.unwrap();
        assert_eq!(swept, (2, 2));

        // Compact with min_age 0 (fresh mtimes are eligible immediately).
        let store: Arc<dyn ObjectStore> = Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(&dir).unwrap(),
        );
        let (docs, parts) = compact_once(&store, Some(&pool), 0).await.unwrap();
        assert_eq!((docs, parts), (2, 1), "one partition => one part file");

        // Source documents are gone; the part file exists under dt=2020-01-05.
        for id in &run_ids {
            assert!(!dir.join(format!("run-{id}.json")).exists(), "source doc deleted");
        }
        let part_dir = dir.join("compact/tasks/dt=2020-01-05");
        let part = std::fs::read_dir(&part_dir).unwrap().next().unwrap().unwrap().path();

        // Read the parquet back: 2 runs x 2 tasks = 4 rows, columns populated.
        let file = std::fs::File::open(&part).unwrap();
        let reader =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap()
                .build()
                .unwrap();
        let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<_, _>>().unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 4);
        let first = &batches[0];
        let workflow = first
            .column_by_name("workflow")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(workflow.value(0), "t");
        let class = first
            .column_by_name("runner_class")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(class.value(0), "etl");

        // Index stamped with the part path for both runs.
        for id in &run_ids {
            let (compacted, path): (Option<String>, Option<String>) = sqlx::query_as(
                "SELECT compacted_at, parquet_path FROM archived_runs WHERE run_id = ?",
            )
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert!(compacted.is_some());
            assert!(path.unwrap().starts_with("compact/tasks/dt=2020-01-05/part-"));
        }

        // Nothing left to compact.
        assert_eq!(compact_once(&store, Some(&pool), 0).await.unwrap(), (0, 0));

        pool.close().await;
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

