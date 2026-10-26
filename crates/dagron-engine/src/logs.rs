//! A task's log as the engine's own views read it: from the log
//! store where one keeps it ([`dagron_artifact::log_segments`]), else from
//! its row, as before.

use std::collections::HashMap;
use std::sync::OnceLock;

use dagron_artifact::log_segments::{is_moved, kept_logs, Moved, SegmentStore, TaskRow, READS};

use crate::db;

static STORE: OnceLock<Option<SegmentStore>> = OnceLock::new();

/// The log store this engine keeps logs in, set once as it starts.
pub(crate) fn keep_in(store: Option<SegmentStore>) {
    let _ = STORE.set(store);
}

fn store() -> Option<&'static SegmentStore> {
    STORE.get().and_then(Option::as_ref)
}

/// `rows`' logs: each its log as the store keeps it, else its row's `log`,
/// else its `output`. The rows were read with their segments counted, in one
/// statement, so what is put together is of one moment.
async fn logs_of(
    store: &SegmentStore,
    rows: Vec<db::TaskLogRow>,
) -> anyhow::Result<HashMap<String, String>> {
    let tasks: Vec<TaskRow<'_>> = rows
        .iter()
        .map(|(id, fence, gen, log, _, segments)| TaskRow {
            task_id: id,
            fence: *fence,
            gen: *gen,
            log: log.as_deref(),
            segments: *segments,
        })
        .collect();
    let mut kept = kept_logs(store, &tasks).await?;
    Ok(rows
        .into_iter()
        .map(|(id, _, _, log, output, _)| {
            let text = kept
                .remove(&id)
                .or(log.filter(|l| !l.is_empty()))
                .or(output)
                .unwrap_or_default();
            (id, text)
        })
        .collect())
}

/// Each task's log of `run_id`, by task id: its streamed log, else its
/// `output`, as `db::task_logs` gives them, or what the store keeps of it.
#[cfg(feature = "ops")]
pub(crate) async fn run_logs(
    pool: &db::Pool,
    run_id: &str,
) -> anyhow::Result<HashMap<String, String>> {
    let Some(store) = store() else {
        return db::task_logs(pool, run_id).await;
    };
    // Read again where a log moved on while it was read.
    for _ in 0..READS {
        match logs_of(store, db::task_log_rows(pool, run_id).await?).await {
            Err(e) if is_moved(&e) => continue,
            done => return done,
        }
    }
    Err(Moved.into())
}

/// One task's log of `run_id`, as [`run_logs`] reads it, and of that task
/// alone; `None` where the run has no such task.
#[cfg(feature = "ops")]
pub(crate) async fn task_log(
    pool: &db::Pool,
    run_id: &str,
    task_id: &str,
) -> anyhow::Result<Option<String>> {
    let Some(store) = store() else {
        return db::task_log(pool, run_id, task_id).await;
    };
    for _ in 0..READS {
        let Some(row) = db::task_log_row(pool, run_id, task_id).await? else {
            return Ok(None);
        };
        match logs_of(store, vec![row]).await {
            Err(e) if is_moved(&e) => continue,
            done => return Ok(done?.remove(task_id)),
        }
    }
    Err(Moved.into())
}
