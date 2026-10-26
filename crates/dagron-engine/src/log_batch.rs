//! Live-log chunks on their way to their task's row, coalesced.
//!
//! An executor sends a task's output a chunk at a time, a line at a time for a
//! local task. Each chunk used to be a statement of its own against the
//! scheduling database: an append to a `TEXT` value, which rewrites what is
//! already there, and on Postgres a notification to every subscriber. So the
//! work a log cost grew with the square of its length, and it was done on the
//! reconcile loop.
//!
//! Chunks are held here instead, and written as one append for each task:
//! when what waits for a task has waited [`HOLD`], or longer for a long log
//! (below), or the task has a result about to be recorded. So a task's log is
//! appended to once in that time at most, however many lines it writes.
//!
//! The appends of one write are the datastore's to make
//! ([`db::append_task_outputs`]). On Postgres it makes them side by side, as
//! many at once as its pool takes, and none of them notifies: the runs they
//! were made to are told of afterwards, together, in one statement. A
//! browser's tail of a task is what the notification is for, and it is as
//! prompt told once a write as once an append. An append that notified in its
//! own transaction was what held a cell with many tasks writing: see
//! [`db::append_task_output`].
//!
//! They are still made on the reconcile loop, which waits for the last of
//! them.
//!
//! The window is time, and nothing else gathered shortens it. An append
//! still rewrites the log so far (the open part of it, with an object
//! store), so one made because enough had gathered would be one more rewrite
//! exactly where a rewrite costs most; and what can wait is bounded already,
//! by the cap the executor's sink keeps to. What it rewrites lengthens it: a
//! second more for each whole MiB, up to [`HOLD_MOST`]. Every executor's log
//! is written as the task writes it, and at a second each, a log
//! that grew to tens of MiB was rewritten whole every second: what writing
//! it cost grew with the square of its length. Waiting a second a MiB keeps
//! it near the length's, and a reader of a long log is up to 30 s behind
//! it. A `file://` store's open part is a file, which an append adds to.
//!
//! What this gives up: the last second of a log, or a long one's longer
//! wait, is lost with the engine, and to an attempt whose row another claim
//! took before it is written, which takes no more of it. A task that ends
//! on its own loses nothing, and nor does one asked to stop: it stays
//! running until its stop is its result, and what waits is written first.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dagron_artifact::log_segments::{LogKey, SegmentStore};
use dagron_executor::executor::LogChunk;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::db;

/// How long a task's chunks wait for more before they are written. A tail of
/// the log is this far behind the task, and a tick of the loop.
const HOLD: Duration = Duration::from_secs(1);

/// How much of a log an append rewrites for each second more its chunks
/// wait, and the longest they wait.
const REWRITTEN_PER_SECOND: u64 = 1 << 20;
const HOLD_MOST: Duration = Duration::from_secs(30);

/// What waits to be written for one task: an attempt's chunks, joined.
struct Waiting {
    fence: i64,
    text: String,
    /// The row's log is replaced, not added to: this is the attempt's first
    /// chunk, or its whole log again.
    reset: bool,
    since: Instant,
}

/// The chunks taken from the executors and not yet written, by task.
#[derive(Default)]
pub(crate) struct LogBatch {
    waiting: HashMap<String, Waiting>,
    /// How much of each attempt's log this engine has put in its task's
    /// row, where the row keeps the whole log (no store): what its next
    /// append rewrites.
    in_row: HashMap<String, (i64, u64)>,
    /// Where logs are kept once out of the task rows, with what
    /// this engine knows of each attempt it is writing; `None`, in the rows.
    kept: Option<Kept>,
}

/// The log store a batch writes to ([`dagron_artifact::log_segments`]), the
/// size a segment closes at, the open segment of each task attempt this
/// engine is writing, and the store's GC to wake where a log was begun again
/// over a writing kept before ([`crate::log_sweep`]).
pub(crate) struct Kept {
    store: SegmentStore,
    segment_bytes: u64,
    open: HashMap<String, Open>,
    wake: Option<Arc<tokio::sync::Notify>>,
}

/// A task attempt's open segment, as this engine has written it: which
/// attempt and which writing of its log (what the store names it by), its
/// number, and how much it holds. Learnt from the database for an attempt
/// the engine began writing before it started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Open {
    fence: i64,
    gen: i64,
    seq: i64,
    bytes: u64,
}

/// `text` as the segments it fills, each of about `size` bytes and cut at
/// the end of a line where one ends within it (a line longer than `size` is
/// cut at it, on a character's boundary), and what is left, under `size`.
fn segments_of(text: &str, size: usize) -> (Vec<&str>, &str) {
    let size = size.max(1);
    let mut full = Vec::new();
    let mut rest = text;
    while rest.len() >= size {
        // The last character boundary within the size, and past one
        // character at least however long it is.
        let mut limit = size;
        while !rest.is_char_boundary(limit) {
            limit -= 1;
        }
        let cut = match rest[..limit].rfind('\n') {
            Some(at) => at + 1,
            None if limit > 0 => limit,
            None => rest.char_indices().nth(1).map_or(rest.len(), |(at, _)| at),
        };
        let (piece, after) = rest.split_at(cut);
        full.push(piece);
        rest = after;
    }
    (full, rest)
}

impl Kept {
    pub(crate) fn new(store: SegmentStore, segment_bytes: u64) -> Kept {
        Kept {
            store,
            segment_bytes,
            open: HashMap::new(),
            wake: None,
        }
    }

    /// Wake `wake` where a log is begun again over a writing kept before:
    /// the store's GC, which takes that writing ([`crate::log_sweep`]).
    pub(crate) fn waking(mut self, wake: Arc<tokio::sync::Notify>) -> Kept {
        self.wake = Some(wake);
        self
    }

    /// What is known of `task`'s attempt `fence`: this engine's, or, for an
    /// attempt it has not written, the database's and the store's. `None`
    /// where the task's row is gone: nothing more of it is written.
    ///
    /// An attempt learnt so is marked first ([`db::mark_log_kept`]): what the
    /// store holds of a task is a task its row says it began there, which is
    /// what records it for the store's GC when the row goes.
    async fn open(
        &mut self,
        pool: &db::Pool,
        task: &str,
        fence: i64,
    ) -> anyhow::Result<Option<Open>> {
        if let Some(open) = self.open.get(task).filter(|o| o.fence == fence) {
            return Ok(Some(*open));
        }
        db::mark_log_kept(pool, task).await?;
        let Some((seq, row, gen)) = db::log_open_state(pool, task, fence).await? else {
            return Ok(None);
        };
        let bytes = match self.store.open_in_row() {
            true => row as u64,
            false => {
                self.store
                    .open_len(&LogKey::new(task, fence, gen), seq)
                    .await?
            }
        };
        let open = Open {
            fence,
            gen,
            seq,
            bytes,
        };
        self.open.insert(task.to_string(), open);
        Ok(Some(open))
    }

    /// The attempt's log was begun again from nothing, as writing `gen` of it
    /// ([`db::reset_task_log`], which took the rows of its closed segments):
    /// its first chunk, or its whole log again. Nothing is removed here: what
    /// was kept of the writing before is no reader's now, and the reset
    /// recorded it for the store's GC, which takes it off the loop
    /// ([`crate::log_sweep`]).
    fn begin(&mut self, task: &str, fence: i64, reset: &db::LogReset) {
        self.open.insert(
            task.to_string(),
            Open {
                fence,
                gen: reset.gen,
                seq: 0,
                bytes: 0,
            },
        );
    }

    /// Close the attempt's open segment where it is full: its text into the
    /// store, a row for it, and out of where it was open.
    async fn close_if_full(
        &mut self,
        pool: &db::Pool,
        task: &str,
        open: Open,
    ) -> anyhow::Result<()> {
        if open.bytes < self.segment_bytes {
            return Ok(());
        }
        let key = LogKey::new(task, open.fence, open.gen);
        let text = match self.store.open_in_row() {
            true => db::read_row_log(pool, task, open.fence).await?,
            false => self.store.read_open(&key, open.seq).await?,
        };
        let Some(text) = text.filter(|t| !t.is_empty()) else {
            return Ok(());
        };
        // As many segments of about the size as the text fills, each cut at
        // a line's end; what is left stays open. A task's log can come in one
        // piece (a seal, its whole log again; what is read back of a
        // container once it is over; what a long log gathered while it
        // waited), which is not one segment of its whole size.
        let (full, rest) = segments_of(&text, self.segment_bytes as usize);
        // The segments' objects first, then their rows, all in one
        // transaction, or none: a close that fails leaves the open part as it
        // was, to be closed again, and the objects are written again under
        // the same names.
        let mut pieces = Vec::with_capacity(full.len());
        for (n, piece) in full.iter().enumerate() {
            let seq = open.seq + n as i64;
            let name = self.store.put_segment(&key, seq, piece).await?;
            pieces.push((
                seq,
                name,
                piece.len() as i64,
                piece.matches('\n').count() as i64,
            ));
        }
        let taken: usize = full.iter().map(|p| p.chars().count()).sum();
        let from_row = self.store.open_in_row().then_some(taken as i64);
        let seq = open.seq + full.len() as i64;
        if !self.store.open_in_row() {
            // The rest is the next open part, written before the segments
            // are recorded: a reader that counts them finds it there, and a
            // record that fails leaves it to be written again by the next
            // close, from the open part, which is as it was.
            self.store.write_open(&key, seq, rest).await?;
        }
        db::close_log_segments(pool, task, open.fence, &pieces, from_row).await?;
        if !self.store.open_in_row() {
            // The one just closed stays until the next close, so that a
            // reader that counted the segments before these finds it; the
            // ones before it go. A reader slower than that is told the log
            // moved on, and reads again.
            for older in 0..open.seq {
                self.store.remove_open(&key, older).await?;
            }
        }
        self.open.insert(
            task.to_string(),
            Open {
                fence: open.fence,
                gen: open.gen,
                seq,
                bytes: rest.len() as u64,
            },
        );
        Ok(())
    }

    /// One append's text, kept: into the row (an object store, whose open
    /// part is the row: the row's append was made already) or the open file
    /// (a `file://` store), and the segment closed where it is full. `begun`:
    /// the append began the log again ([`db::reset_task_log`], made already).
    async fn keep(
        &mut self,
        pool: &db::Pool,
        append: &db::LogAppend,
        begun: Option<&db::LogReset>,
    ) -> anyhow::Result<()> {
        let (task, fence) = (append.task_id.as_str(), append.fence);
        let known = begun.is_some() || self.open.get(task).is_some_and(|o| o.fence == fence);
        if let Some(reset) = begun {
            self.begin(task, fence, reset);
        }
        let Some(mut open) = self.open(pool, task, fence).await? else {
            // Its row is gone: the store's GC takes what was kept of it.
            return Ok(());
        };
        if !self.store.open_in_row() {
            // Learnt before this text was written: it is added now.
            self.store
                .append_open(&LogKey::new(task, fence, open.gen), open.seq, &append.text)
                .await?;
            open.bytes += append.text.len() as u64;
        } else if known {
            // A row learnt from the database holds this text already.
            open.bytes += append.text.len() as u64;
        }
        self.open.insert(task.to_string(), open);
        let closed = self.close_if_full(pool, task, open).await;
        if closed.is_err() {
            // Where the close stopped is not known here: the attempt's state
            // is learnt again, from the database and the store, at its next
            // write.
            self.open.remove(task);
        }
        closed
    }
}

impl LogBatch {
    /// A batch that keeps logs in `kept`'s store, or in the task rows.
    pub(crate) fn new(kept: Option<Kept>) -> LogBatch {
        LogBatch {
            waiting: HashMap::new(),
            in_row: HashMap::new(),
            kept,
        }
    }

    /// Take what the executors have queued.
    pub(crate) fn take(&mut self, log_rx: &mut UnboundedReceiver<LogChunk>) {
        let now = Instant::now();
        while let Ok(chunk) = log_rx.try_recv() {
            self.add(chunk, now);
        }
    }

    fn add(&mut self, chunk: LogChunk, now: Instant) {
        match self.waiting.entry(chunk.task_id) {
            Entry::Vacant(slot) => {
                slot.insert(Waiting {
                    fence: chunk.fence,
                    text: chunk.chunk,
                    reset: chunk.first,
                    since: now,
                });
            }
            Entry::Occupied(mut slot) => {
                let waiting = slot.get_mut();
                if chunk.fence < waiting.fence {
                    // An attempt that is over. Its row would refuse it.
                } else if chunk.fence > waiting.fence || chunk.first {
                    // A later attempt, or this one's whole log again (a
                    // sealed log): it takes the place of what waited, as it
                    // would have taken the place of what was written.
                    waiting.fence = chunk.fence;
                    waiting.text = chunk.chunk;
                    waiting.reset = chunk.first;
                } else {
                    waiting.text.push_str(&chunk.chunk);
                }
            }
        }
    }

    /// The tasks whose chunks have waited long enough.
    fn due(&self, now: Instant) -> Vec<String> {
        self.waiting
            .iter()
            .filter(|(task, w)| now.duration_since(w.since) >= self.hold(task, w))
            .map(|(task, _)| task.clone())
            .collect()
    }

    /// How long what waits for `task` waits: [`HOLD`], and a second more
    /// for each whole MiB of the log its append would rewrite, up to
    /// [`HOLD_MOST`]. A log begun again rewrites nothing that was there.
    fn hold(&self, task: &str, waiting: &Waiting) -> Duration {
        let held = |kept: Option<(i64, u64)>| match kept {
            Some((fence, bytes)) if fence == waiting.fence && !waiting.reset => bytes,
            _ => 0,
        };
        let rewritten = match &self.kept {
            None => held(self.in_row.get(task).copied()),
            Some(kept) if kept.store.open_in_row() => {
                held(kept.open.get(task).map(|open| (open.fence, open.bytes)))
            }
            Some(_) => 0,
        };
        (HOLD + Duration::from_secs(rewritten / REWRITTEN_PER_SECOND)).min(HOLD_MOST)
    }

    /// Write what is due, one append for each task. How many were made.
    pub(crate) async fn write_due(&mut self, pool: &db::Pool) -> usize {
        let due = self.due(Instant::now());
        self.write(pool, due).await
    }

    /// Write what waits for `tasks`, whose results are about to be recorded:
    /// a row that is no longer running takes no more. Fence-guarded like any
    /// append, so a stale attempt's chunk cannot reach a rerun's log.
    pub(crate) async fn write_for<'a>(
        &mut self,
        pool: &db::Pool,
        tasks: impl Iterator<Item = &'a str>,
    ) -> usize {
        let tasks: Vec<String> = tasks.map(str::to_string).collect();
        let named = tasks
            .iter()
            .filter(|task| self.waiting.contains_key(task.as_str()))
            .cloned()
            .collect();
        let made = self.write(pool, named).await;
        // Their attempts are over: the next is learnt again where it begins.
        for task in &tasks {
            self.in_row.remove(task);
        }
        if let Some(kept) = &mut self.kept {
            for task in &tasks {
                kept.open.remove(task);
            }
        }
        made
    }

    /// One append for each of `tasks`, made by the datastore together.
    /// Best-effort, as each chunk's append was: one that fails is logged and
    /// its chunks are gone. A task has one text waiting, so no two of the
    /// appends are to one row.
    async fn write(&mut self, pool: &db::Pool, tasks: Vec<String>) -> usize {
        let appends: Vec<db::LogAppend> = tasks
            .into_iter()
            .filter_map(|task| {
                let waiting = self.waiting.remove(&task)?;
                Some(db::LogAppend {
                    task_id: task,
                    fence: waiting.fence,
                    text: waiting.text,
                    reset: waiting.reset,
                })
            })
            .collect();
        let Some(kept) = &mut self.kept else {
            // What the rows now hold, counted once they hold it: an append
            // that failed rewrites nothing next time.
            let counted: Vec<(String, i64, u64, bool)> = appends
                .iter()
                .map(|a| (a.task_id.clone(), a.fence, a.text.len() as u64, a.reset))
                .collect();
            let made = db::append_task_outputs(pool, appends).await;
            if made == counted.len() {
                for (task, fence, len, reset) in counted {
                    self.in_row
                        .entry(task)
                        .and_modify(|(held, bytes)| {
                            if *held == fence && !reset {
                                *bytes += len;
                            } else {
                                (*held, *bytes) = (fence, len);
                            }
                        })
                        .or_insert((fence, len));
                }
            }
            return made;
        };
        let (resets, appends): (Vec<db::LogAppend>, Vec<db::LogAppend>) =
            appends.into_iter().partition(|a| a.reset);
        let mut made = 0;
        // A log begun again is begun in one transaction with the rows of its
        // closed segments going (`db::reset_task_log`): a reader sees the old
        // log or the new one, never the old segments before the new text. Its
        // text is the row's where the store keeps the open part there; a
        // file:// store's row is begun empty, for the notes the engine adds to
        // it, and its text goes to the new writing's open file. A reset the
        // row refused (its attempt is over) begins nothing, and what was kept
        // of the attempt stays. What the store kept of the writing before is
        // not removed here, on the loop: the reset recorded it, and the
        // store's GC is woken to take it (`log_sweep`).
        let mut begun = Vec::new();
        for append in resets {
            let text = match kept.store.open_in_row() {
                true => append.text.as_str(),
                false => "",
            };
            match db::reset_task_log(pool, &append.task_id, append.fence, text).await {
                Ok(reset) => {
                    made += 1;
                    if let Some(reset) = reset {
                        begun.push((append, reset));
                    }
                }
                Err(e) => {
                    tracing::warn!(task_id = %append.task_id, error = %e, "a log could not be begun again")
                }
            }
        }
        // An object store's open part is the row's `log`, as a whole log was:
        // the appends are made as they were, and a full one is closed after.
        // A file:// store's is a file, which `keep` adds to.
        let kept_now: Vec<db::LogAppend> = if kept.store.open_in_row() {
            let shadows = appends
                .iter()
                .map(|a| db::LogAppend {
                    task_id: a.task_id.clone(),
                    fence: a.fence,
                    text: a.text.clone(),
                    reset: a.reset,
                })
                .collect();
            made += db::append_task_outputs(pool, appends).await;
            shadows
        } else {
            made += appends.len();
            appends
        };
        for (append, reset) in &begun {
            if let Err(e) = kept.keep(pool, append, Some(reset)).await {
                tracing::warn!(task_id = %append.task_id, error = %format!("{e:#}"), "a log's text could not be kept in the log store");
            }
        }
        for append in &kept_now {
            if let Err(e) = kept.keep(pool, append, None).await {
                tracing::warn!(task_id = %append.task_id, error = %format!("{e:#}"), "a log's text could not be kept in the log store");
            }
        }
        if begun.iter().any(|(_, reset)| reset.swept) {
            if let Some(wake) = &kept.wake {
                wake.notify_one();
            }
        }
        // The runs of the logs begun again are told of once their text is
        // where a reader looks.
        let mut runs: Vec<String> = begun.into_iter().map(|(_, reset)| reset.run_id).collect();
        runs.sort_unstable();
        runs.dedup();
        if let Err(e) = db::notify_runs(pool, &runs).await {
            tracing::warn!(error = %e, runs = runs.len(), "telling of logs begun again failed");
        }
        made
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(task: &str, fence: i64, text: &str, first: bool) -> LogChunk {
        LogChunk {
            task_id: task.into(),
            fence,
            chunk: text.into(),
            first,
        }
    }

    /// A text is cut into segments of about the size at line ends, the rest
    /// left open; a line longer than a segment is cut within it, on a
    /// character's boundary.
    #[test]
    fn a_text_fills_segments_cut_at_line_ends() {
        let text = "aaaa\nbbbb\ncc\n";
        let (full, rest) = segments_of(text, 6);
        assert_eq!((full, rest), (vec!["aaaa\n", "bbbb\n"], "cc\n"));
        let long = format!("{}\nshort\n", "é".repeat(10));
        let (full, rest) = segments_of(&long, 7);
        assert!(full.iter().all(|p| p.len() <= 8), "{full:?}");
        assert_eq!(full.concat() + rest, long, "nothing lost or doubled");
        assert_eq!(segments_of("tiny", 100), (vec![], "tiny"));
    }

    /// A running task row of a fresh SQLite database, at `fence`.
    async fn running_task(fence: i64) -> (db::Pool, tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("e.db");
        let pool = db::init_pool(path.to_str().unwrap()).await.unwrap();
        let yaml = "name: t\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let dag = dagron_core::dag::DagGraph::from_yaml(yaml).unwrap();
        let run = db::create_run(&pool, &dag, yaml).await.unwrap();
        let (task, _, _, _, _, _) = db::task_log_rows(&pool, &run).await.unwrap().remove(0);
        sqlx::query("UPDATE task_runs SET status = 'running', version = ?1 WHERE id = ?2")
            .bind(fence)
            .bind(&task)
            .execute(&pool)
            .await
            .unwrap();
        (pool, dir, task)
    }

    /// The whole log as a reader puts it together.
    async fn read(pool: &db::Pool, store: &SegmentStore, task: &str, fence: i64) -> String {
        let segments = db::log_segments_of(pool, &[task.to_string()])
            .await
            .unwrap()
            .len() as i64;
        let row = db::read_row_log(pool, task, fence).await.unwrap();
        let (_, _, gen) = db::log_open_state(pool, task, fence)
            .await
            .unwrap()
            .unwrap_or_default();
        let rows = [dagron_artifact::log_segments::TaskRow {
            task_id: task,
            fence,
            gen,
            log: row.as_deref(),
            segments,
        }];
        let kept = dagron_artifact::log_segments::kept_logs(store, &rows)
            .await
            .unwrap();
        kept.get(task).cloned().or(row).unwrap_or_default()
    }

    /// A log written through the batch is kept as closed segments and
    /// an open part, in either kind of store, and read back whole, each line
    /// once; begun again, it is the new text alone; an engine that starts
    /// over goes on where the database and the store say.
    #[tokio::test]
    async fn a_log_is_kept_in_segments_and_read_back_whole() {
        for open_in_row in [false, true] {
            let (pool, _db, task) = running_task(2).await;
            let logs = tempfile::tempdir().unwrap();
            let store = match open_in_row {
                false => SegmentStore::Dir(logs.path().to_path_buf()),
                true => SegmentStore::DirOpenInRow(logs.path().to_path_buf()),
            };
            let mut batch = LogBatch::new(Some(Kept::new(store.clone(), 4096)));
            let mut whole = String::new();
            for n in 0..400 {
                let text = format!("line {n:04} {}\n", "x".repeat(30));
                whole.push_str(&text);
                batch.add(chunk(&task, 2, &text, n == 0), Instant::now() - HOLD);
                if n % 50 == 49 {
                    batch.write_due(&pool).await;
                }
            }
            batch.write_due(&pool).await;
            let segments = db::log_segments_of(&pool, std::slice::from_ref(&task))
                .await
                .unwrap();
            assert!(
                segments.len() >= 3,
                "{open_in_row}: closed segments: {}",
                segments.len()
            );
            assert_eq!(
                read(&pool, &store, &task, 2).await,
                whole,
                "{open_in_row}: whole, each line once"
            );
            if open_in_row {
                let row = db::read_row_log(&pool, &task, 2)
                    .await
                    .unwrap()
                    .unwrap_or_default();
                assert!(
                    row.len() < 4096 + 2000,
                    "the row holds the open part alone: {}",
                    row.len()
                );
            }

            // An engine that starts over goes on where the store says.
            let mut again = LogBatch::new(Some(Kept::new(store.clone(), 4096)));
            again.add(
                chunk(&task, 2, "after a restart\n", false),
                Instant::now() - HOLD,
            );
            again.write_due(&pool).await;
            whole.push_str("after a restart\n");
            assert_eq!(
                read(&pool, &store, &task, 2).await,
                whole,
                "{open_in_row}: after a restart"
            );

            // Begun again: the new text alone, and nothing read of the old;
            // the new is the log's next writing, under names of its own. The
            // old is kept in the store, the batch removing nothing, until the
            // store's GC takes it, as the reset recorded it to.
            let old = db::log_segments_of(&pool, std::slice::from_ref(&task))
                .await
                .unwrap();
            let (_, _, gen) = db::log_open_state(&pool, &task, 2).await.unwrap().unwrap();
            again.add(chunk(&task, 2, "masked\n", true), Instant::now() - HOLD);
            again.write_due(&pool).await;
            assert_eq!(
                read(&pool, &store, &task, 2).await,
                "masked\n",
                "{open_in_row}: begun again"
            );
            assert!(db::log_segments_of(&pool, std::slice::from_ref(&task))
                .await
                .unwrap()
                .is_empty());
            assert!(
                old.iter().all(|s| logs.path().join(&s.object_key).exists()),
                "{open_in_row}: the old writing's segments wait for the GC"
            );
            if !open_in_row {
                assert!(
                    std::fs::read_dir(
                        logs.path()
                            .join(std::path::Path::new(&old[0].object_key).parent().unwrap())
                    )
                    .unwrap()
                    .any(|e| e
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .ends_with(".open")),
                    "the old writing's open part waits for the GC too"
                );
            }
            let gc = crate::log_sweep::Sweeper::new(
                pool.clone(),
                store.clone(),
                Arc::new(dagron_core::metrics::Metrics::new()),
                Arc::new(tokio::sync::Notify::new()),
                Duration::from_secs(30),
            );
            gc.pass().await.unwrap();
            assert!(
                old.iter()
                    .all(|s| !logs.path().join(&s.object_key).exists()),
                "{open_in_row}: the old writing's segments are gone"
            );
            // And the rest of it: its open parts, and its directory.
            let writing = std::path::Path::new(&old[0].object_key).parent().unwrap();
            assert!(
                !logs.path().join(writing).exists(),
                "{open_in_row}: nothing of the old writing is left"
            );
            assert_eq!(
                read(&pool, &store, &task, 2).await,
                "masked\n",
                "{open_in_row}: the new writing is whole"
            );
            assert_eq!(
                db::log_open_state(&pool, &task, 2)
                    .await
                    .unwrap()
                    .unwrap()
                    .2,
                gen + 1,
                "{open_in_row}: the next writing"
            );
            if !open_in_row {
                let before = LogKey::new(task.as_str(), 2, gen);
                let now = LogKey::new(task.as_str(), 2, gen + 1);
                assert_eq!(
                    store.open_size(&before, old.len() as i64).await.unwrap(),
                    None
                );
                assert_eq!(store.open_size(&now, 0).await.unwrap(), Some(7));
            }

            // A reset the row refuses, its attempt being over, begins
            // nothing: what was kept of the attempt stays.
            sqlx::query("UPDATE task_runs SET status = 'succeeded' WHERE id = ?1")
                .bind(&task)
                .execute(&pool)
                .await
                .unwrap();
            again.add(chunk(&task, 2, "late seal\n", true), Instant::now() - HOLD);
            again.write_due(&pool).await;
            assert_eq!(
                read(&pool, &store, &task, 2).await,
                "masked\n",
                "{open_in_row}: a refused reset changes nothing"
            );
            sqlx::query("UPDATE task_runs SET status = 'running' WHERE id = ?1")
                .bind(&task)
                .execute(&pool)
                .await
                .unwrap();

            // A whole log in one write, as a seal is, or a log read back in
            // one piece, is segments of about the size, not one of its whole
            // size.
            let big: String = (0..1000)
                .map(|n| format!("big {n:04} {}\n", "y".repeat(30)))
                .collect();
            again.add(chunk(&task, 2, &big, true), Instant::now() - HOLD);
            again.write_due(&pool).await;
            let n = db::log_segments_of(&pool, std::slice::from_ref(&task))
                .await
                .unwrap()
                .len();
            assert!(
                n >= 9,
                "{open_in_row}: {} bytes in {n} segments of 4096",
                big.len()
            );
            assert_eq!(
                read(&pool, &store, &task, 2).await,
                big,
                "{open_in_row}: the one write, whole"
            );
        }
    }

    /// However many chunks a task sends between two writes, they wait as one
    /// text, in the order they came, and each task's are its own.
    #[test]
    fn a_task_s_chunks_wait_as_one() {
        let mut batch = LogBatch::default();
        let now = Instant::now();
        for n in 0..1000 {
            batch.add(chunk("a", 3, &format!("line {n}\n"), n == 0), now);
            if n % 400 == 0 {
                batch.add(chunk("b", 7, &format!("b{n}\n"), n == 0), now);
            }
        }
        assert_eq!(batch.waiting.len(), 2, "one text for each task");
        let a = &batch.waiting["a"];
        let whole: String = (0..1000).map(|n| format!("line {n}\n")).collect();
        assert_eq!(
            (a.fence, a.reset, a.text.as_str()),
            (3, true, whole.as_str())
        );
        assert_eq!(batch.waiting["b"].text, "b0\nb400\nb800\n");
    }

    /// Chunks that follow an attempt's first write are added to its row, not
    /// put in its place.
    #[test]
    fn what_follows_the_first_write_is_added() {
        let mut batch = LogBatch::default();
        let now = Instant::now();
        batch.add(chunk("a", 3, "second\n", false), now);
        batch.add(chunk("a", 3, "third\n", false), now);
        let a = &batch.waiting["a"];
        assert_eq!((a.reset, a.text.as_str()), (false, "second\nthird\n"));
    }

    /// An attempt's whole log again, which an executor sends once it has
    /// masked it as a whole, takes the place of the chunks that waited.
    #[test]
    fn a_whole_log_again_takes_the_place_of_what_waited() {
        let mut batch = LogBatch::default();
        let now = Instant::now();
        batch.add(chunk("a", 3, "to***\n", true), now);
        batch.add(chunk("a", 3, "ken\n", false), now);
        batch.add(chunk("a", 3, "***\n", true), now);
        let a = &batch.waiting["a"];
        assert_eq!((a.reset, a.text.as_str()), (true, "***\n"));
    }

    /// A later attempt's chunks take the place of an earlier one's that never
    /// got written, and a late chunk of the earlier one is dropped.
    #[test]
    fn a_later_attempt_takes_over_and_an_earlier_one_s_late_chunk_is_dropped() {
        let mut batch = LogBatch::default();
        let now = Instant::now();
        batch.add(chunk("a", 3, "first attempt\n", true), now);
        batch.add(chunk("a", 5, "second attempt\n", true), now);
        batch.add(chunk("a", 3, "late\n", false), now);
        batch.add(chunk("a", 5, "goes on\n", false), now);
        let a = &batch.waiting["a"];
        assert_eq!(
            (a.fence, a.reset, a.text.as_str()),
            (5, true, "second attempt\ngoes on\n")
        );
    }

    /// The chunks of a log an append would rewrite much of wait longer: a
    /// second more for each whole MiB, up to the most; another attempt's
    /// log, and one begun again, rewrite nothing.
    #[test]
    fn a_long_log_s_chunks_wait_longer() {
        let mut batch = LogBatch::default();
        let then = Instant::now();
        batch.in_row.insert("long".into(), (1, 4 << 20));
        batch.add(chunk("long", 1, "more\n", false), then);
        batch.add(chunk("short", 1, "more\n", false), then);
        assert_eq!(
            batch.due(then + HOLD),
            ["short"],
            "4 MiB to rewrite waits 4 s more"
        );
        let mut due = batch.due(then + HOLD + Duration::from_secs(4));
        due.sort();
        assert_eq!(due, ["long", "short"]);
        // The log of another attempt, or one begun again, is short again.
        batch.waiting.clear();
        batch.add(chunk("long", 2, "again\n", false), then);
        assert_eq!(batch.due(then + HOLD), ["long"]);
        batch.waiting.clear();
        batch.add(chunk("long", 1, "whole\n", true), then);
        assert_eq!(batch.due(then + HOLD), ["long"]);
        // And none waits more than the most.
        batch.waiting.clear();
        batch.in_row.insert("long".into(), (1, 1 << 30));
        batch.add(chunk("long", 1, "more\n", false), then);
        assert!(batch.due(then + HOLD_MOST - HOLD).is_empty());
        assert_eq!(batch.due(then + HOLD_MOST), ["long"]);
    }

    /// A task's chunks are due once the first of them has waited the hold,
    /// however much has gathered and however lately the last of them came.
    #[test]
    fn chunks_are_due_when_the_first_of_them_has_waited() {
        let mut batch = LogBatch::default();
        let then = Instant::now();
        batch.add(chunk("early", 1, "a line\n", true), then);
        batch.add(
            chunk("loud", 1, &"x".repeat(4 << 20), true),
            then + HOLD / 2,
        );
        assert!(batch.due(then + HOLD / 2).is_empty(), "size is no reason");
        // More chunks do not start the wait again.
        batch.add(chunk("early", 1, "another\n", false), then + HOLD / 2);
        assert_eq!(batch.due(then + HOLD), ["early"]);
        let mut due = batch.due(then + HOLD + HOLD / 2);
        due.sort();
        assert_eq!(due, ["early", "loud"]);
    }

    /// Against a real row: chunks wait, a result's task is written at once
    /// with one append, what follows is added, and a chunk of an attempt
    /// that is over reaches nothing.
    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn a_task_s_log_is_written_whole_with_one_append_for_each_write() {
        use crate::dag;

        let path = std::env::temp_dir().join(format!("m54_log_batch_{}.db", uuid::Uuid::new_v4()));
        let pool = db::init_pool(path.to_str().unwrap()).await.unwrap();
        let yaml = "name: q\ntasks:\n  - name: chatty\n    command: [\"true\"]\n";
        let run = db::create_run(&pool, &dag::DagGraph::from_yaml(yaml).unwrap(), yaml)
            .await
            .unwrap();
        db::advance_ready_tasks(&pool).await.unwrap();
        let claimed = db::claim_ready(&pool, "w", 10).await.unwrap();
        let (task, fence) = (claimed[0].id.clone(), claimed[0].version + 1);
        let log = || async {
            sqlx::query_scalar::<_, Option<String>>("SELECT log FROM task_runs WHERE run_id = ?")
                .bind(&run)
                .fetch_one(&pool)
                .await
                .unwrap()
        };

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let lines = |from: usize, to: usize| -> String {
            (from..to).map(|n| format!("line {n}\n")).collect()
        };
        for n in 0..500 {
            tx.send(chunk(&task, fence, &format!("line {n}\n"), n == 0))
                .unwrap();
        }
        let mut batch = LogBatch::default();
        batch.take(&mut rx);
        assert_eq!(batch.write_due(&pool).await, 0, "they have not waited");
        assert_eq!(log().await, None, "nothing is written while it waits");

        // The task's result is in: its log is written before the row closes.
        assert_eq!(batch.write_for(&pool, [task.as_str()].into_iter()).await, 1);
        assert_eq!(log().await.as_deref(), Some(lines(0, 500).as_str()));
        assert_eq!(batch.write_for(&pool, [task.as_str()].into_iter()).await, 0);

        // What follows is added to it, again with one append.
        for n in 500..700 {
            tx.send(chunk(&task, fence, &format!("line {n}\n"), false))
                .unwrap();
        }
        // And a chunk of an attempt that is over reaches nothing.
        tx.send(chunk(&task, fence - 1, "stale\n", false)).unwrap();
        batch.take(&mut rx);
        assert_eq!(batch.write_for(&pool, [task.as_str()].into_iter()).await, 1);
        assert_eq!(log().await.as_deref(), Some(lines(0, 700).as_str()));

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    /// One write makes an append for each task that has something waiting.
    /// Each task's text reaches its own row, each write adds to what the last
    /// one left, and a task with nothing waiting is no append.
    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn the_appends_of_one_write_are_each_task_s_own() {
        use crate::dag;

        let path = std::env::temp_dir().join(format!("m54_log_lanes_{}.db", uuid::Uuid::new_v4()));
        let pool = db::init_pool(path.to_str().unwrap()).await.unwrap();
        let tasks: String = (0..9)
            .map(|n| format!("  - name: t{n}\n    command: [\"true\"]\n"))
            .collect();
        let yaml = format!("name: lanes\ntasks:\n{tasks}");
        let run = db::create_run(&pool, &dag::DagGraph::from_yaml(&yaml).unwrap(), &yaml)
            .await
            .unwrap();
        db::advance_ready_tasks(&pool).await.unwrap();
        let claimed = db::claim_ready(&pool, "w", 16).await.unwrap();
        assert_eq!(claimed.len(), 9);
        let ids: Vec<String> = claimed.iter().map(|c| c.id.clone()).collect();

        let mut batch = LogBatch::default();
        let now = Instant::now();
        let mut wanted: HashMap<String, String> = HashMap::new();
        for round in 0..3 {
            for c in &claimed {
                let text = format!("{} in round {round}\n", c.name);
                batch.add(chunk(&c.id, c.version + 1, &text, round == 0), now);
                wanted.entry(c.id.clone()).or_default().push_str(&text);
            }
            assert_eq!(batch.write(&pool, ids.clone()).await, 9, "round {round}");
            assert!(batch.waiting.is_empty());
            assert_eq!(batch.write(&pool, ids.clone()).await, 0, "nothing waits");
        }
        let logs: Vec<(String, Option<String>)> =
            sqlx::query_as("SELECT id, log FROM task_runs WHERE run_id = ?")
                .bind(&run)
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(logs.len(), 9);
        for (id, log) in logs {
            assert_eq!(log.as_deref(), wanted.get(&id).map(String::as_str), "{id}");
        }

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }
}
