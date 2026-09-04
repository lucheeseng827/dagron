//! `StreamSource` — the built-in, zero-infra **streaming** ingestion source.
//!
//! Follows an append-only NDJSON file (or a named pipe) the way `tail -f`
//! does: every line is one workflow submission (JSON is a subset of YAML, so a
//! one-line JSON workflow spec parses with the same `DagGraph::from_yaml` path
//! as every other source). Producers are anything that can append a line —
//! `kafkacat`, `psql \copy`, an app log hook, a cron script, `curl | jq -c` —
//! which makes this the streaming on-ramp that needs no broker at all:
//!
//! ```text
//! producer ──append──▶ events.ndjson ──recv──▶ IngestActor ──▶ one run per line
//!                       ▲     │
//!            offset file ◀──ack┘        (at-least-once: the offset advances
//!                                        only after the run is durably created)
//! ```
//!
//! **Delivery semantics.** Exactly-once run creation under the ingest actor:
//! the line's byte offset is committed **in the same datastore transaction**
//! as the run it becomes (`pending_position` → `db::create_run_with_offset`),
//! and the actor hands the committed cursor back at startup
//! (`set_committed_position`, preferred over the offset file) — a crash-replay
//! of the file re-creates nothing. The offset *file* remains as a trailing
//! mirror (written on `ack`, atomic rename) for shell tooling and for running
//! this source outside the actor. A `nack` (a transient `create_run` failure)
//! redelivers the same line without advancing.
//! Poison lines follow the actor's dead-letter routing: a durable
//! `dead_letters` row plus a mirrored line in this source's `.dlq` file (the
//! file-based analog of a broker-native dead-letter queue).
//!
//! **Replay & drain.** The consumer cursor lives in the datastore
//! (`source_offsets`, source name = the configured `SOURCE` kind) with the
//! offset file as its trailing mirror: to replay the stream from the start,
//! clear **both** (`DELETE FROM source_offsets WHERE source_name='stream'` +
//! remove the file), or point the engine at a fresh datastore. With
//! `STREAM_FOLLOW=false` the source drains to the end of the file and
//! exhausts — the scheduler processes the backlog and exits — which turns the
//! same event file into a reproducible batch replay.
//!
//! **What this is not.** One file, one consumer, one host: there is no
//! partitioning, no consumer group rebalance, no broker-side retention or
//! cross-host failover. Managed broker connectors (Kafka, NATS, SQS, Redis)
//! and the CloudEvents webhook gateway are not in this build; this
//! source is the same programming model at laptop scale.
//!
//! | Env var | Default | Meaning |
//! |---|---|---|
//! | `STREAM_PATH` | — (required) | NDJSON file or FIFO to follow |
//! | `STREAM_FOLLOW` | `true` | keep waiting for growth (`false` = drain then exhaust) |
//! | `STREAM_POLL_MS` | `500` | poll interval while waiting at end-of-file |
//! | `STREAM_OFFSET_PATH` | `<STREAM_PATH>.offset` | committed-offset checkpoint file |
//! | `STREAM_DLQ_PATH` | `<STREAM_PATH>.dlq` | poison-line mirror (NDJSON) |

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};

use crate::source::{AckHandle, WorkflowMessage, WorkflowSource};

/// Configuration for [`StreamSource`] (see the module table for the env mapping).
#[derive(Clone, Debug)]
pub struct StreamConfig {
    pub path: PathBuf,
    /// Keep following the file for new lines (`tail -f`). `false` = emit the
    /// backlog then exhaust (`recv` → `None`), so the engine can drain and exit.
    pub follow: bool,
    /// How long to sleep at end-of-file before re-checking for growth.
    pub poll_interval: Duration,
    /// Where the committed byte offset is persisted on `ack`.
    pub offset_path: PathBuf,
    /// Where poison lines are mirrored by `dead_letter`.
    pub dlq_path: PathBuf,
}

impl StreamConfig {
    /// Derive the standard config from a stream path (`.offset` / `.dlq`
    /// siblings, follow on, 500 ms poll).
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let path: PathBuf = path.into();
        let sibling = |ext: &str| {
            let mut s = path.as_os_str().to_owned();
            s.push(ext);
            PathBuf::from(s)
        };
        Self {
            offset_path: sibling(".offset"),
            dlq_path: sibling(".dlq"),
            follow: true,
            poll_interval: Duration::from_millis(500),
            path,
        }
    }

    /// Read the `STREAM_*` env vars (module table). Errors if `STREAM_PATH` is
    /// unset — a streaming source with no stream is a misconfiguration, not a
    /// default.
    pub fn from_env() -> Result<Self> {
        let path = std::env::var("STREAM_PATH").context(
            "SOURCE=stream requires STREAM_PATH (the NDJSON file or named pipe to follow)",
        )?;
        let mut cfg = Self::new(path);
        if let Ok(v) = std::env::var("STREAM_FOLLOW") {
            cfg.follow = !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no");
        }
        if let Some(ms) = std::env::var("STREAM_POLL_MS").ok().and_then(|v| v.parse().ok()) {
            cfg.poll_interval = Duration::from_millis(ms);
        }
        if let Ok(p) = std::env::var("STREAM_OFFSET_PATH") {
            cfg.offset_path = PathBuf::from(p);
        }
        if let Ok(p) = std::env::var("STREAM_DLQ_PATH") {
            cfg.dlq_path = PathBuf::from(p);
        }
        Ok(cfg)
    }
}

/// Streaming file/FIFO follower — see the module docs for semantics.
pub struct StreamSource {
    cfg: StreamConfig,
    reader: Option<BufReader<tokio::fs::File>>,
    /// Absolute byte position the reader has consumed up to (committed + read).
    pos: u64,
    /// Whether the underlying file supports seeking (a regular file). FIFOs
    /// don't: offsets are meaningless across restarts there, so seek/truncate
    /// recovery is disabled and the stream is consumed live.
    seekable: bool,
    /// The line currently delivered but not yet acked, with the byte position
    /// just past it. `nack` keeps it here so the next `recv` redelivers.
    inflight: Option<(String, u64)>,
    /// The datastore-committed position handed over by the ingest actor
    /// (exactly-once). Authoritative over the offset *file* when present: the
    /// file trails it (ack fires after the run's transaction), so preferring
    /// the datastore cursor is what closes the crash-replay window.
    db_position: Option<u64>,
}

impl StreamSource {
    pub fn new(cfg: StreamConfig) -> Self {
        Self { cfg, reader: None, pos: 0, seekable: true, inflight: None, db_position: None }
    }

    pub fn from_env() -> Result<Self> {
        Ok(Self::new(StreamConfig::from_env()?))
    }

    /// The committed offset: the datastore cursor when the ingest actor handed
    /// one over (exactly-once), else the offset-file checkpoint (0 when
    /// absent/corrupt — "start of stream", the replay-from-scratch default).
    async fn committed_offset(&self) -> u64 {
        if let Some(pos) = self.db_position {
            return pos;
        }
        match tokio::fs::read_to_string(&self.cfg.offset_path).await {
            Ok(s) => s.trim().parse().unwrap_or(0),
            Err(_) => 0,
        }
    }

    /// Open (or reopen) the stream, positioned at the committed offset when the
    /// file is seekable. Blocks until the file exists (and, for a FIFO, until a
    /// writer connects) — "waiting for the stream" is the steady state of a
    /// streaming consumer, not an error.
    async fn open(&mut self) -> Result<()> {
        loop {
            match tokio::fs::File::open(&self.cfg.path).await {
                Ok(mut file) => {
                    let committed = self.committed_offset().await;
                    self.pos = 0;
                    self.seekable = true;
                    if committed > 0 {
                        // A checkpoint past the end of a seekable file means it
                        // was truncated/rotated since — restart from 0 rather
                        // than waiting forever at a phantom offset.
                        let len = file.metadata().await.map(|m| m.len()).unwrap_or(0);
                        if len < committed {
                            tracing::warn!(
                                path = %self.cfg.path.display(), committed, len,
                                "stream shorter than committed offset — assuming rotation, replaying from start"
                            );
                        } else {
                            match file.seek(std::io::SeekFrom::Start(committed)).await {
                                Ok(_) => self.pos = committed,
                                Err(e) => {
                                    // A FIFO: not seekable. Consume live from here.
                                    tracing::debug!(error = %e, "stream not seekable (named pipe) — consuming live");
                                    self.seekable = false;
                                }
                            }
                        }
                    } else if file.seek(std::io::SeekFrom::Start(0)).await.is_err() {
                        self.seekable = false;
                    }
                    self.reader = Some(BufReader::new(file));
                    return Ok(());
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    tracing::debug!(path = %self.cfg.path.display(), "stream file not found yet — waiting");
                    tokio::time::sleep(self.cfg.poll_interval).await;
                }
                Err(e) => {
                    return Err(e).context(format!(
                        "cannot open stream '{}'",
                        self.cfg.path.display()
                    ))
                }
            }
        }
    }

    /// Atomic (write + rename) offset-file mirror write.
    async fn write_offset_mirror(&self, next: &str) -> Result<()> {
        let tmp = self.cfg.offset_path.with_extension("offset.tmp");
        let mut f = tokio::fs::File::create(&tmp)
            .await
            .with_context(|| format!("create offset checkpoint '{}'", tmp.display()))?;
        f.write_all(next.as_bytes()).await?;
        f.flush().await?;
        tokio::fs::rename(&tmp, &self.cfg.offset_path).await?;
        Ok(())
    }

    /// Pull the next complete line (without the trailing newline), following
    /// growth per config. Returns `None` when `follow` is off and the file is
    /// fully consumed.
    async fn next_line(&mut self) -> Result<Option<String>> {
        if self.reader.is_none() {
            self.open().await?;
        }
        let mut buf = String::new();
        loop {
            let reader = self.reader.as_mut().expect("opened above");
            let n = reader.read_line(&mut buf).await?;
            self.pos += n as u64;

            if n > 0 && buf.ends_with('\n') {
                let line = buf.trim_end_matches(['\n', '\r']).to_string();
                if line.trim().is_empty() {
                    buf.clear(); // skip blank lines, keep the position advance
                    continue;
                }
                return Ok(Some(line));
            }

            // End of file with either nothing or a partial (unterminated) line
            // in `buf`. A partial line is a producer mid-append — wait for the
            // rest rather than delivering torn JSON.
            if !self.cfg.follow {
                if !buf.trim().is_empty() {
                    // Drain mode: the final line may legitimately lack a newline.
                    return Ok(Some(buf.trim_end_matches(['\n', '\r']).to_string()));
                }
                return Ok(None);
            }

            // Rotation guard (regular files only): the file shrank below what
            // we've consumed — reopen from the top of the new file.
            if self.seekable {
                if let Ok(meta) = tokio::fs::metadata(&self.cfg.path).await {
                    if meta.len() < self.pos {
                        tracing::warn!(
                            path = %self.cfg.path.display(), consumed = self.pos, len = meta.len(),
                            "stream truncated/rotated under the reader — reopening from start"
                        );
                        self.reader = None;
                        let _ = tokio::fs::remove_file(&self.cfg.offset_path).await;
                        self.open().await?;
                        buf.clear();
                        continue;
                    }
                }
            }
            tokio::time::sleep(self.cfg.poll_interval).await;
        }
    }
}

#[async_trait]
impl WorkflowSource for StreamSource {
    async fn recv(&mut self) -> Result<Option<WorkflowMessage>> {
        // Redeliver a nacked line before reading anything new — the streaming
        // analog of a broker redelivery, and what keeps this at-least-once.
        if let Some((line, next)) = &self.inflight {
            return Ok(Some(WorkflowMessage {
                payload: line.clone(),
                handle: AckHandle::String(next.to_string()),
            }));
        }
        match self.next_line().await? {
            Some(line) => {
                let next = self.pos;
                self.inflight = Some((line.clone(), next));
                Ok(Some(WorkflowMessage {
                    payload: line,
                    handle: AckHandle::String(next.to_string()),
                }))
            }
            None => Ok(None),
        }
    }

    /// Release the delivered line, then mirror the offset to the checkpoint
    /// file (write + rename, atomic). The in-flight slot clears FIRST and the
    /// mirror is best-effort: under the ingest actor the authoritative cursor
    /// already committed with the run's transaction, so a mirror-write failure
    /// must not leave the line in flight — that would redeliver it in-process
    /// and create a duplicate run.
    async fn ack(&mut self, handle: &AckHandle) -> Result<()> {
        self.inflight = None;
        if let AckHandle::String(next) = handle {
            if let Err(e) = self.write_offset_mirror(next).await {
                tracing::warn!(
                    error = %e,
                    path = %self.cfg.offset_path.display(),
                    "offset mirror write failed (datastore cursor already committed) — continuing"
                );
            }
        }
        Ok(())
    }

    /// Keep the line in flight so the next `recv` redelivers it.
    async fn nack(&mut self, _handle: &AckHandle) -> Result<()> {
        Ok(())
    }

    /// Exactly-once: the byte position just past the in-flight line, committed
    /// by the ingest actor in the same transaction as the run it becomes.
    fn pending_position(&self) -> Option<crate::source::PendingPosition> {
        self.inflight
            .as_ref()
            .map(|(_, next)| crate::source::PendingPosition::whole(next.to_string()))
    }

    /// Adopt the datastore-committed cursor (preferred over the offset file —
    /// see [`StreamSource::committed_offset`]). An unparsable value is ignored
    /// with a warning rather than wedging ingestion.
    async fn set_committed_position(&mut self, position: Option<String>) -> Result<()> {
        if let Some(p) = position {
            match p.trim().parse::<u64>() {
                Ok(n) => self.db_position = Some(n),
                Err(_) => {
                    tracing::warn!(position = %p, "unparsable committed stream position — falling back to the offset file");
                }
            }
        }
        Ok(())
    }

    /// Mirror a poison line into the `.dlq` NDJSON file — the file-based analog
    /// of a broker-native dead-letter queue. The durable `dead_letters` row is
    /// recorded by the ingest actor regardless; this file is what a shell
    /// redrive (`while read -r; do … done < events.ndjson.dlq`) consumes.
    async fn dead_letter(&mut self, payload: &str, error: &str) -> Result<()> {
        let entry = serde_json::json!({ "error": error, "payload": payload });
        let mut f = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.cfg.dlq_path)
            .await
            .with_context(|| format!("open stream DLQ '{}'", self.cfg.dlq_path.display()))?;
        f.write_all(format!("{entry}\n").as_bytes()).await?;
        f.flush().await?;
        Ok(())
    }
}

// ── ShardedStreamSource: one stream dir, many consumers ──────────────────────

/// Multi-consumer variant of [`StreamSource`]: `STREAM_PATH` is a **directory**
/// of append-only NDJSON shard files, and N engines split the shards through
/// per-partition range leases (`source_partitions`) — one consumer per shard
/// at a time, rebalanced at lease expiry when a consumer dies. Every line
/// still commits exactly-once: each shard's byte offset is namespaced
/// `<source>/<file>` in `source_offsets` and rides the run's transaction via
/// [`PendingPosition::substream`](crate::source::PendingPosition).
///
/// This is the broker-free consumer group: producers shard however they like
/// (one file per producer, per key range, per hour), consumers scale by
/// starting more engines. Extra knobs over the single-file source:
/// `STREAM_SUFFIX` (default `.ndjson` — which files are shards) and
/// `STREAM_MAX_PARTITIONS` (default unlimited — cap per consumer, so capacity
/// spreads instead of one engine hoarding every shard).
pub struct ShardedStreamSource {
    pool: dagron_core::db::Pool,
    /// Offsets/lease namespace — the configured `SOURCE` kind (`stream`).
    source_key: String,
    dir: PathBuf,
    suffix: String,
    follow: bool,
    poll_interval: Duration,
    max_partitions: i64,
    worker_id: String,
    /// Claimed shards, keyed by file name, polled round-robin.
    children: std::collections::BTreeMap<String, StreamSource>,
    rr: std::collections::VecDeque<String>,
    /// The shard whose line is currently in flight (routes ack/nack/DLQ).
    inflight_partition: Option<String>,
    last_scan: Option<std::time::Instant>,
    last_renew: Option<std::time::Instant>,
}

/// Partition lease horizon; renewed at a third of it, matching task leases.
const PARTITION_LEASE_SECS: i64 = 30;
const PARTITION_RENEW_EVERY: Duration = Duration::from_secs(10);
const PARTITION_SCAN_EVERY: Duration = Duration::from_secs(5);

impl ShardedStreamSource {
    pub fn new(pool: dagron_core::db::Pool, source_key: impl Into<String>, dir: PathBuf) -> Self {
        let suffix = std::env::var("STREAM_SUFFIX")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| ".ndjson".to_string());
        let max_partitions = std::env::var("STREAM_MAX_PARTITIONS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(i64::MAX)
            .max(1);
        let base = StreamConfig::new(&dir); // reuse env parsing for follow/poll
        let mut cfg_probe = base;
        if let Ok(v) = std::env::var("STREAM_FOLLOW") {
            cfg_probe.follow =
                !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no");
        }
        if let Some(ms) = std::env::var("STREAM_POLL_MS").ok().and_then(|v| v.parse().ok()) {
            cfg_probe.poll_interval = Duration::from_millis(ms);
        }
        Self {
            pool,
            source_key: source_key.into(),
            dir,
            suffix,
            follow: cfg_probe.follow,
            poll_interval: cfg_probe.poll_interval,
            max_partitions,
            worker_id: format!("consumer-{}", uuid::Uuid::new_v4()),
            children: Default::default(),
            rr: Default::default(),
            inflight_partition: None,
            last_scan: None,
            last_renew: None,
        }
    }

    /// Discover shards, register them, claim capacity, renew held leases, and
    /// reconcile children with what we actually hold.
    async fn upkeep(&mut self) -> Result<()> {
        let now = std::time::Instant::now();
        let scan_due = self.last_scan.map_or(true, |t| now.duration_since(t) >= PARTITION_SCAN_EVERY);
        let renew_due =
            self.last_renew.map_or(true, |t| now.duration_since(t) >= PARTITION_RENEW_EVERY);
        if !scan_due && !renew_due {
            return Ok(());
        }
        use dagron_core::db;
        if renew_due && !self.children.is_empty() {
            self.last_renew = Some(now);
            let renewed = db::renew_source_partitions(
                &self.pool,
                &self.source_key,
                &self.worker_id,
                PARTITION_LEASE_SECS,
            )
            .await?;
            if renewed < self.children.len() as u64 {
                tracing::warn!(
                    held = self.children.len(),
                    renewed,
                    "some shard leases were reclaimed — resyncing"
                );
            }
        }
        if scan_due {
            self.last_scan = Some(now);
            let mut found = Vec::new();
            let mut entries = tokio::fs::read_dir(&self.dir).await?;
            while let Some(e) = entries.next_entry().await? {
                let name = e.file_name().to_string_lossy().into_owned();
                if name.ends_with(&self.suffix) && e.file_type().await?.is_file() {
                    found.push(name);
                }
            }
            found.sort();
            db::register_source_partitions(&self.pool, &self.source_key, &found).await?;
            let capacity = self.max_partitions.saturating_sub(self.children.len() as i64);
            if capacity > 0 {
                db::claim_source_partitions(
                    &self.pool,
                    &self.source_key,
                    &self.worker_id,
                    capacity,
                    PARTITION_LEASE_SECS,
                )
                .await?;
            }
        }
        // Reconcile children with the datastore's view of what we hold.
        let held =
            dagron_core::db::held_source_partitions(&self.pool, &self.source_key, &self.worker_id)
                .await?;
        let held_set: std::collections::BTreeSet<&String> = held.iter().collect();
        let stale: Vec<String> = self
            .children
            .keys()
            .filter(|k| !held_set.contains(*k))
            .cloned()
            .collect();
        for k in stale {
            tracing::info!(shard = %k, "shard lease lost — handing it to its new consumer");
            self.children.remove(&k);
            self.rr.retain(|p| p != &k);
        }
        for k in held {
            if !self.children.contains_key(&k) {
                let mut cfg = StreamConfig::new(self.dir.join(&k));
                cfg.follow = false; // children never block; this source multiplexes
                cfg.poll_interval = self.poll_interval;
                let mut child = StreamSource::new(cfg);
                // Per-shard exactly-once cursor (namespaced offsets row).
                let key = format!("{}/{}", self.source_key, k);
                if let Some(pos) = dagron_core::db::source_offset(&self.pool, &key).await? {
                    let _ = child.set_committed_position(Some(pos)).await;
                }
                tracing::info!(shard = %k, "shard claimed — consuming");
                self.rr.push_back(k.clone());
                self.children.insert(k, child);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl WorkflowSource for ShardedStreamSource {
    async fn recv(&mut self) -> Result<Option<WorkflowMessage>> {
        loop {
            self.upkeep().await?;
            // One fair pass over the claimed shards.
            for _ in 0..self.rr.len() {
                let Some(partition) = self.rr.pop_front() else { break };
                self.rr.push_back(partition.clone());
                let Some(child) = self.children.get_mut(&partition) else { continue };
                if let Some(msg) = child.recv().await? {
                    self.inflight_partition = Some(partition);
                    return Ok(Some(msg));
                }
            }
            // Every claimed shard is dry.
            if !self.follow {
                return Ok(None); // drain mode: backlog processed
            }
            tokio::time::sleep(self.poll_interval).await;
        }
    }

    async fn ack(&mut self, handle: &AckHandle) -> Result<()> {
        if let Some(p) = self.inflight_partition.take() {
            if let Some(child) = self.children.get_mut(&p) {
                child.ack(handle).await?;
            }
        }
        Ok(())
    }

    async fn nack(&mut self, handle: &AckHandle) -> Result<()> {
        if let Some(p) = &self.inflight_partition {
            if let Some(child) = self.children.get_mut(p) {
                child.nack(handle).await?;
            }
        }
        Ok(())
    }

    async fn dead_letter(&mut self, payload: &str, error: &str) -> Result<()> {
        if let Some(p) = &self.inflight_partition {
            if let Some(child) = self.children.get_mut(p) {
                child.dead_letter(payload, error).await?;
            }
        }
        Ok(())
    }

    /// The shard's coordinate, namespaced by the shard file — so every shard
    /// gets its own exactly-once cursor row.
    fn pending_position(&self) -> Option<crate::source::PendingPosition> {
        let p = self.inflight_partition.as_ref()?;
        let mut pos = self.children.get(p)?.pending_position()?;
        pos.substream = Some(p.clone());
        Some(pos)
    }

    /// Per-shard cursors are read at claim time; the whole-source row does not
    /// apply to a sharded consumer.
    async fn set_committed_position(&mut self, _position: Option<String>) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("m54-stream-{}-{name}", uuid::Uuid::new_v4()))
    }

    fn drain_cfg(path: &PathBuf) -> StreamConfig {
        let mut cfg = StreamConfig::new(path.clone());
        cfg.follow = false;
        cfg.poll_interval = Duration::from_millis(10);
        cfg
    }

    async fn cleanup(cfg: &StreamConfig) {
        for p in [&cfg.path, &cfg.offset_path, &cfg.dlq_path] {
            let _ = tokio::fs::remove_file(p).await;
        }
    }

    /// Drain mode: every NDJSON line is delivered in order (blank lines
    /// skipped), then the source exhausts.
    #[tokio::test]
    async fn drains_lines_then_exhausts() {
        let path = temp("drain.ndjson");
        tokio::fs::write(&path, "{\"name\":\"a\"}\n\n{\"name\":\"b\"}\n").await.unwrap();
        let cfg = drain_cfg(&path);
        let mut src = StreamSource::new(cfg.clone());

        let m1 = src.recv().await.unwrap().unwrap();
        assert_eq!(m1.payload, "{\"name\":\"a\"}");
        src.ack(&m1.handle).await.unwrap();
        let m2 = src.recv().await.unwrap().unwrap();
        assert_eq!(m2.payload, "{\"name\":\"b\"}", "blank line skipped");
        src.ack(&m2.handle).await.unwrap();
        assert!(src.recv().await.unwrap().is_none(), "drained to exhaustion");

        cleanup(&cfg).await;
    }

    /// The offset checkpoint survives a restart: acked lines are not
    /// redelivered, the first un-acked line is.
    #[tokio::test]
    async fn committed_offset_resumes_after_restart() {
        let path = temp("resume.ndjson");
        tokio::fs::write(&path, "{\"name\":\"a\"}\n{\"name\":\"b\"}\n").await.unwrap();
        let cfg = drain_cfg(&path);

        {
            let mut src = StreamSource::new(cfg.clone());
            let m1 = src.recv().await.unwrap().unwrap();
            src.ack(&m1.handle).await.unwrap();
            // b is received but NOT acked — the crash window.
            let _m2 = src.recv().await.unwrap().unwrap();
        }

        // "Restart": a fresh source over the same paths redelivers exactly b.
        let mut src = StreamSource::new(cfg.clone());
        let m = src.recv().await.unwrap().unwrap();
        assert_eq!(m.payload, "{\"name\":\"b\"}", "at-least-once: unacked line redelivers");
        src.ack(&m.handle).await.unwrap();
        assert!(src.recv().await.unwrap().is_none());

        cleanup(&cfg).await;
    }

    /// `nack` redelivers the same line on the next `recv` without advancing.
    #[tokio::test]
    async fn nack_redelivers_same_line() {
        let path = temp("nack.ndjson");
        tokio::fs::write(&path, "{\"name\":\"a\"}\n{\"name\":\"b\"}\n").await.unwrap();
        let cfg = drain_cfg(&path);
        let mut src = StreamSource::new(cfg.clone());

        let m = src.recv().await.unwrap().unwrap();
        assert_eq!(m.payload, "{\"name\":\"a\"}");
        src.nack(&m.handle).await.unwrap();
        let again = src.recv().await.unwrap().unwrap();
        assert_eq!(again.payload, "{\"name\":\"a\"}", "nacked line redelivers");
        src.ack(&again.handle).await.unwrap();
        assert_eq!(src.recv().await.unwrap().unwrap().payload, "{\"name\":\"b\"}");

        cleanup(&cfg).await;
    }

    /// Follow mode picks up lines appended after the reader hit end-of-file.
    #[tokio::test]
    async fn follow_mode_sees_appended_lines() {
        let path = temp("follow.ndjson");
        tokio::fs::write(&path, "{\"name\":\"a\"}\n").await.unwrap();
        let mut cfg = StreamConfig::new(path.clone());
        cfg.poll_interval = Duration::from_millis(10);
        let mut src = StreamSource::new(cfg.clone());

        let m1 = src.recv().await.unwrap().unwrap();
        src.ack(&m1.handle).await.unwrap();

        // Append while the source is waiting at EOF.
        let appender = {
            let path = path.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let mut f = tokio::fs::OpenOptions::new().append(true).open(&path).await.unwrap();
                f.write_all(b"{\"name\":\"late\"}\n").await.unwrap();
            })
        };
        let m2 = tokio::time::timeout(Duration::from_secs(5), src.recv())
            .await
            .expect("follow must deliver the appended line")
            .unwrap()
            .unwrap();
        assert_eq!(m2.payload, "{\"name\":\"late\"}");
        appender.await.unwrap();

        cleanup(&cfg).await;
    }

    /// A partial (unterminated) line in follow mode is held until the newline
    /// arrives — torn JSON is never delivered.
    #[tokio::test]
    async fn partial_line_waits_for_newline_in_follow_mode() {
        let path = temp("partial.ndjson");
        tokio::fs::write(&path, "{\"name\":\"tr").await.unwrap();
        let mut cfg = StreamConfig::new(path.clone());
        cfg.poll_interval = Duration::from_millis(10);
        let mut src = StreamSource::new(cfg.clone());

        let completer = {
            let path = path.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let mut f = tokio::fs::OpenOptions::new().append(true).open(&path).await.unwrap();
                f.write_all(b"uncated\"}\n").await.unwrap();
            })
        };
        let m = tokio::time::timeout(Duration::from_secs(5), src.recv())
            .await
            .expect("completed line must deliver")
            .unwrap()
            .unwrap();
        assert_eq!(m.payload, "{\"name\":\"truncated\"}", "line assembled across appends");
        completer.await.unwrap();

        cleanup(&cfg).await;
    }

    /// Sharded dir + partition leases: two consumers split the shard files
    /// disjointly; per-shard cursors are exactly-once, so after the first
    /// consumer's lease lapses, the second consumer picks up its shard
    /// *without* re-reading the lines whose runs were already committed.
    #[tokio::test]
    async fn sharded_dir_splits_shards_and_resumes_exactly_once() {
        let dir = temp("shards");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join("p0.ndjson"), "{\"name\":\"p0-1\"}\n{\"name\":\"p0-2\"}\n")
            .await
            .unwrap();
        tokio::fs::write(dir.join("p1.ndjson"), "{\"name\":\"p1-1\"}\n").await.unwrap();
        let db_path = temp("shards-db");
        let pool =
            dagron_core::db::init_pool(db_path.to_str().unwrap()).await.unwrap();

        // Consumer A: capped to one shard, drain mode.
        let mut a = ShardedStreamSource::new(pool.clone(), "stream", dir.clone());
        a.max_partitions = 1;
        a.follow = false;
        a.poll_interval = Duration::from_millis(10);
        let mut a_lines = Vec::new();
        while let Some(msg) = a.recv().await.unwrap() {
            let pp = a.pending_position().expect("sharded coordinate");
            assert!(pp.substream.is_some(), "shard-namespaced cursor");
            // Commit the cursor the way the ingest actor does (same-tx path).
            dagron_core::db::record_dead_letter_with_offset(
                &pool,
                &msg.payload,
                "test-commit",
                &pp.offset_key("stream"),
                1,
                &pp.position,
            )
            .await
            .unwrap();
            a.ack(&msg.handle).await.unwrap();
            a_lines.push(msg.payload);
        }
        assert!(!a_lines.is_empty(), "A consumed its shard");
        let a_shard: std::collections::BTreeSet<String> =
            a_lines.iter().map(|l| l[9..11].to_string()).collect();
        assert_eq!(a_shard.len(), 1, "A stayed within one shard: {a_lines:?}");

        // A dies without releasing: expire its lease so B may rebalance.
        sqlx::query("UPDATE source_partitions SET lease_expires_at = ?")
            .bind((chrono::Utc::now() - chrono::TimeDelta::seconds(5)).to_rfc3339())
            .execute(&pool)
            .await
            .unwrap();

        // Consumer B: unlimited, drain mode — takes everything left over.
        let mut b = ShardedStreamSource::new(pool.clone(), "stream", dir.clone());
        b.follow = false;
        b.poll_interval = Duration::from_millis(10);
        let mut b_lines = Vec::new();
        while let Some(msg) = b.recv().await.unwrap() {
            b.ack(&msg.handle).await.unwrap();
            b_lines.push(msg.payload);
        }

        let mut all: Vec<String> = a_lines.iter().chain(b_lines.iter()).cloned().collect();
        all.sort();
        assert_eq!(
            all,
            vec!["{\"name\":\"p0-1\"}", "{\"name\":\"p0-2\"}", "{\"name\":\"p1-1\"}"],
            "every line consumed exactly once across both consumers"
        );

        pool.close().await;
        let _ = tokio::fs::remove_dir_all(&dir).await;
        let _ = std::fs::remove_file(&db_path);
    }

    /// `dead_letter` mirrors poison lines into the `.dlq` NDJSON file.
    #[tokio::test]
    async fn dead_letter_appends_to_dlq_file() {
        let path = temp("dlq.ndjson");
        tokio::fs::write(&path, "not json\n").await.unwrap();
        let cfg = drain_cfg(&path);
        let mut src = StreamSource::new(cfg.clone());

        let m = src.recv().await.unwrap().unwrap();
        src.dead_letter(&m.payload, "invalid workflow spec").await.unwrap();
        src.ack(&m.handle).await.unwrap();

        let dlq = tokio::fs::read_to_string(&cfg.dlq_path).await.unwrap();
        let entry: serde_json::Value = serde_json::from_str(dlq.lines().next().unwrap()).unwrap();
        assert_eq!(entry["payload"], "not json");
        assert_eq!(entry["error"], "invalid workflow spec");

        cleanup(&cfg).await;
    }
}
