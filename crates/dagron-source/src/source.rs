//! Workflow ingestion sources (v4).
//!
//! Where the `Executor` trait (dagron-executor) abstracts *how a task
//! runs*, [`WorkflowSource`] abstracts *where new workflows come from*. The
//! scheduler is no longer a one-shot "run this YAML and exit" process: a
//! [`WorkflowSource`] is a stream of workflow submissions that the
//! [`IngestActor`](crate::ingest::IngestActor) pulls, parses, and turns into runs
//! via [`db::create_run`](dagron_core::db::create_run) — for as long as the process
//! lives. That makes the scheduler a durable daemon that can sit in front of a
//! high-throughput queue (SQS / Kafka / Redis) and absorb a large influx, with
//! the queue itself buffering bursts and `MAX_INFLIGHT_RUNS` providing admission
//! backpressure.
//!
//! Three sources are always compiled (zero-infra):
//!
//! * [`FileSource`] — emits one bundled DAG file then drains; preserves the
//!   original single-run behaviour and is the default.
//! * [`StreamSource`](crate::stream::StreamSource) — follows an append-only
//!   NDJSON file or named pipe (`SOURCE=stream`), one workflow submission per
//!   line, at-least-once with a durable offset checkpoint — the built-in
//!   streaming on-ramp.
//! * [`ChannelSource`] — an in-process `mpsc` queue; the reference "generic
//!   queue" used in tests and for embedding the scheduler in another process.
//!
//! [`DirSource`] (`SOURCE=dir`, a watched directory) is always compiled too, and
//! the open MQTT adapter ([`MqttSource`](crate::mqtt::MqttSource), `SOURCE=mqtt`)
//! ships behind the `mqtt` feature so a stock build links no broker client.
//!
//! Managed broker connectors (Kafka, NATS, SQS, Redis), the CloudEvents
//! webhook gateway and the fleet plane (`SOURCE=fleet`) are **not in this
//! build**; they plug in through the [`SourceFactory`] seam below,
//! implementing this same trait. A custom backend can be wired in the identical
//! way (`dagron_engine::Seams::source_factory`).

use anyhow::{bail, Result};
use async_trait::async_trait;

/// A source's resumable coordinate for the message currently in flight — what
/// the ingest actor commits in the same transaction as the run it creates.
/// `substream` names the partition for multi-consumer sources (`None` = the
/// source has one cursor); the committed row is keyed
/// `<source>` or `<source>/<substream>` accordingly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingPosition {
    pub substream: Option<String>,
    pub position: String,
}

impl PendingPosition {
    /// A whole-source (unpartitioned) coordinate.
    pub fn whole(position: impl Into<String>) -> Self {
        Self { substream: None, position: position.into() }
    }

    /// The `source_offsets` key this coordinate commits under.
    pub fn offset_key(&self, source_name: &str) -> String {
        match &self.substream {
            Some(s) => format!("{source_name}/{s}"),
            None => source_name.to_string(),
        }
    }
}

/// One workflow submission pulled from a source. `payload` is the raw DAG spec
/// (YAML — which is a superset of JSON, so JSON payloads parse too). `handle` is
/// an opaque per-source token used to ack/nack the underlying message once the
/// run has been durably created (at-least-once delivery).
pub struct WorkflowMessage {
    pub payload: String,
    pub handle: AckHandle,
}

/// Opaque acknowledgement token. Sources with delivery semantics stash whatever
/// they need to ack or redeliver the message — the stream source's next byte
/// offset, an SQS receipt handle, the Redis processing-list payload, etc.
/// In-process / one-shot sources use [`AckHandle::None`].
pub enum AckHandle {
    None,
    /// Carried by sources with real delivery semantics (offset / receipt handle).
    String(String),
}

/// A stream of workflow submissions.
///
/// `recv` blocks until the next submission is available, or returns `Ok(None)`
/// when the source is *permanently* exhausted (e.g. a one-shot file). Streaming
/// queue backends never return `None` under normal operation — they block until
/// a message arrives. `ack`/`nack` default to no-ops for sources without
/// delivery semantics.
#[async_trait]
pub trait WorkflowSource: Send + 'static {
    async fn recv(&mut self) -> Result<Option<WorkflowMessage>>;

    /// Acknowledge that the run was durably created; remove the message so it is
    /// not redelivered.
    async fn ack(&mut self, _handle: &AckHandle) -> Result<()> {
        Ok(())
    }

    /// The message could not be turned into a run; ask the source to redeliver
    /// (or eventually dead-letter) it.
    async fn nack(&mut self, _handle: &AckHandle) -> Result<()> {
        Ok(())
    }

    /// Route a poison submission to this source's **broker-native** dead-letter
    /// destination (SQS DLQ, Kafka DLT topic, Redis DLQ list, NATS DLQ subject),
    /// so downstream consumers/alerting on the broker see it too. The durable
    /// `dead_letters` Postgres row is recorded by the ingest actor regardless;
    /// this is the broker-side mirror. Default: no-op (file/channel have no
    /// broker, and queue backends with no DLQ configured stay Postgres-only).
    async fn dead_letter(&mut self, _payload: &str, _error: &str) -> Result<()> {
        Ok(())
    }

    // ── Exactly-once (transactional offsets) ─────────────────────────────────
    // A source with a resumable coordinate (byte offset, partition offset,
    // replication LSN) opts into exactly-once run creation by implementing the
    // pair below. The ingest actor then commits the coordinate **in the same
    // datastore transaction** as the run it accounts for
    // (`db::create_run_with_offset`), and hands the committed cursor back at
    // startup — a crash between run-create and broker ack can redeliver the
    // message, but the source (repositioned past the committed cursor) never
    // re-creates its run. Sources without a coordinate (file/channel/webhook)
    // keep the defaults and stay at-least-once.

    /// The coordinate to commit atomically with the run created from the
    /// message most recently returned by `recv` — typically "the position just
    /// past it". `None` (default) = no transactional offset for this message.
    /// A partitioned source names the shard via
    /// [`PendingPosition::substream`], which namespaces the committed row.
    fn pending_position(&self) -> Option<PendingPosition> {
        None
    }

    /// Hand the source its durably-committed cursor before the first `recv`
    /// (`None` = nothing committed yet). A resuming source starts strictly
    /// after this position. Default: ignored.
    async fn set_committed_position(&mut self, _position: Option<String>) -> Result<()> {
        Ok(())
    }
}

// ── FileSource ────────────────────────────────────────────────────────────────

/// Emits a single DAG file once, then drains. Preserves the original
/// "run one workflow and exit" behaviour — the default source.
pub struct FileSource {
    path: String,
    emitted: bool,
}

impl FileSource {
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into(), emitted: false }
    }
}

#[async_trait]
impl WorkflowSource for FileSource {
    async fn recv(&mut self) -> Result<Option<WorkflowMessage>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        let payload = tokio::fs::read_to_string(&self.path)
            .await
            .map_err(|e| anyhow::anyhow!("cannot read DAG file '{}': {e}", self.path))?;
        Ok(Some(WorkflowMessage { payload, handle: AckHandle::None }))
    }
}

// ── DirSource ──────────────────────────────────────────────────────

/// A directory of YAML as live input: drop a file in and it runs, edit one and the
/// next scan picks the new version up.
///
/// [`FileSource`] emits a single spec at startup and drains, which is why pointing
/// the engine at `WORKFLOW_DIR` has until now produced "Is a directory" and why a
/// YAML dropped in later was never seen. That gap is the whole reason this exists:
/// "put a file in a folder" is the mental model operators already have from every
/// other tool on the box, and it is the on-ramp for people who will never wire up
/// GitOps.
///
/// **Polls, deliberately** — no inotify. File watching does not fire on the mounts
/// this source is for: Docker/Podman bind mounts from a Windows or macOS host, NFS
/// and SMB shares on a NAS, and most network filesystems. A watcher that silently
/// never triggers is worse than a scan that always works, and the scan costs one
/// `readdir` plus a `stat` per file every `DIR_POLL_MS`.
///
/// **Re-emits on change**, keyed on (modified time, length) rather than a content
/// hash: a spec is re-submitted when its file changes, and an untouched file is
/// never submitted twice. Two edits inside one filesystem timestamp granularity
/// with an identical length are the known blind spot; a hash would close it at the
/// cost of reading every file on every scan, which is the wrong trade for a
/// directory someone edits by hand.
///
/// **That key survives a restart.** It is committed to `source_offsets` in the same
/// transaction as the run it becomes (one row per file, keyed `<SOURCE>/<file>`,
/// the exactly-once machinery [`StreamSource`] already uses), and consulted the
/// first time each file is seen. Without it a container restart re-submits every
/// YAML in the directory as a fresh run — an inbox someone leaves ten workflows in
/// would run all ten again on every crash-loop iteration. A source built without a
/// datastore ([`build`], for embedders) keeps the in-memory-only behaviour.
///
/// Never returns `None`. Ingestion stops when the source is exhausted, and a watched
/// directory is never exhausted — it is idle.
pub struct DirSource {
    dir: std::path::PathBuf,
    poll: std::time::Duration,
    /// path -> fingerprint of the version already emitted (or already committed by
    /// an earlier process).
    seen: std::collections::HashMap<std::path::PathBuf, String>,
    /// Specs found by the last scan and not yet handed out: (path, fingerprint,
    /// payload).
    pending: std::collections::VecDeque<(std::path::PathBuf, String, String)>,
    /// The message handed out and not yet acked. `nack` leaves it here so the next
    /// `recv` redelivers it — nothing else would, because the scan already recorded
    /// its fingerprint as seen. The ingest actor nacks a submission it could not
    /// turn into a run *yet* (a workflow at its `max_active_runs` cap, a transient
    /// datastore error), and its log line promises redelivery; a no-op `nack` here
    /// would make that promise a silent drop until someone touched the file again.
    inflight: Option<(std::path::PathBuf, String, String)>,
    /// Warn once per unreadable path, not once per scan, so a file whose
    /// permissions are wrong does not produce a line every two seconds forever.
    warned: std::collections::HashSet<std::path::PathBuf>,
    /// Datastore + the `source_offsets` namespace to read committed fingerprints
    /// back from. The name must be the one the ingest actor commits under (it keys
    /// rows `<source_name>/<substream>`), so it is the configured `SOURCE` value.
    store: Option<(dagron_core::db::Pool, String)>,
}

impl DirSource {
    pub fn new(dir: impl Into<std::path::PathBuf>, poll: std::time::Duration) -> Self {
        Self {
            dir: dir.into(),
            poll,
            seen: Default::default(),
            pending: Default::default(),
            inflight: None,
            warned: Default::default(),
            store: None,
        }
    }

    /// Remember across restarts which version of each file already became a run.
    /// `source_name` must match the ingest actor's (the `SOURCE` value), or the
    /// key written with the run and the key read here would not be the same row.
    pub fn with_datastore(
        mut self,
        pool: dagron_core::db::Pool,
        source_name: impl Into<String>,
    ) -> Self {
        self.store = Some((pool, source_name.into()));
        self
    }

    /// `*.yaml` / `*.yml`, this directory only — no recursion. A workflow directory
    /// is a flat inbox; descending into it would sweep up vendored charts, `.git`
    /// and editor backups that happen to sit below it.
    fn is_spec(path: &std::path::Path) -> bool {
        matches!(
            path.extension().and_then(|e| e.to_str()).map(str::to_ascii_lowercase).as_deref(),
            Some("yaml") | Some("yml")
        )
    }

    /// The version identity of a file: modified time (ns since the epoch) and
    /// length. A string because that is what `source_offsets.position` stores, and
    /// it is compared for equality only — never ordered.
    fn fingerprint(modified: Option<std::time::SystemTime>, len: u64) -> String {
        let mtime = modified
            .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_nanos().to_string())
            .unwrap_or_else(|| "-".to_string());
        format!("{mtime}:{len}")
    }

    /// The `substream` this file's cursor commits under — its name, since the scan
    /// never descends. The ingest actor prefixes the source name.
    fn substream(path: &std::path::Path) -> String {
        path.file_name().unwrap_or(path.as_os_str()).to_string_lossy().into_owned()
    }

    /// Did an earlier process already turn *this* version of the file into a run
    /// (or a dead letter)? Both commit the fingerprint with the row, so a matching
    /// `source_offsets` position means "already ingested — do not re-submit".
    ///
    /// Costs one indexed read per file per process: it is only asked the first time
    /// a path is seen, after which the in-memory map answers.
    async fn already_ingested(&self, path: &std::path::Path, fingerprint: &str) -> bool {
        let Some((pool, source_name)) = self.store.as_ref() else {
            return false;
        };
        let key = format!("{source_name}/{}", Self::substream(path));
        match dagron_core::db::source_offset(pool, &key).await {
            Ok(committed) => committed.as_deref() == Some(fingerprint),
            // Treat an unreadable cursor as "not ingested": a duplicate run is
            // recoverable and visible, a workflow that silently never runs is not.
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e,
                    "could not read the committed position for this file — treating it as new");
                false
            }
        }
    }

    /// One pass: queue every spec that is new or has changed since it was emitted.
    async fn scan(&mut self) -> Result<()> {
        let mut entries = match tokio::fs::read_dir(&self.dir).await {
            Ok(e) => e,
            // A directory that does not exist yet is not an error: the mount may
            // arrive after the engine, and the next scan will find it.
            Err(e) => {
                if self.warned.insert(self.dir.clone()) {
                    tracing::warn!(dir = %self.dir.display(), error = %e,
                        "workflow directory unreadable — retrying every poll");
                }
                return Ok(());
            }
        };
        self.warned.remove(&self.dir);

        let mut found = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if !Self::is_spec(&path) {
                continue;
            }
            let Ok(meta) = entry.metadata().await else { continue };
            if !meta.is_file() {
                continue;
            }
            found.push((path, Self::fingerprint(meta.modified().ok(), meta.len())));
        }
        // Deterministic order, so the first scan of a seeded directory submits in
        // the order someone reading `ls` would expect.
        found.sort_by(|a, b| a.0.cmp(&b.0));

        for (path, fingerprint) in found {
            let previous = self.seen.get(&path).cloned();
            if previous.as_deref() == Some(fingerprint.as_str()) {
                continue;
            }
            // First sight this process — which includes every file in the directory
            // right after a restart. Ask the datastore whether this exact version
            // already became a run before re-running it.
            if previous.is_none() && self.already_ingested(&path, &fingerprint).await {
                self.seen.insert(path, fingerprint);
                continue;
            }
            match tokio::fs::read_to_string(&path).await {
                Ok(payload) => {
                    self.seen.insert(path.clone(), fingerprint.clone());
                    self.warned.remove(&path);
                    self.pending.push_back((path, fingerprint, payload));
                }
                Err(e) => {
                    // Half-written file, or one being copied in. Do not record it as
                    // seen: the next scan retries, which is what makes a large file
                    // dropped in with `cp` land correctly.
                    if self.warned.insert(path.clone()) {
                        tracing::warn!(path = %path.display(), error = %e,
                            "cannot read workflow file — will retry");
                    }
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl WorkflowSource for DirSource {
    async fn recv(&mut self) -> Result<Option<WorkflowMessage>> {
        loop {
            // A nacked file stays in flight and is handed back first — the scan
            // will not offer it again, its fingerprint is already recorded.
            if let Some((path, _, payload)) = self.inflight.as_ref() {
                tracing::debug!(path = %path.display(), "redelivering workflow file");
                return Ok(Some(WorkflowMessage {
                    payload: payload.clone(),
                    handle: AckHandle::None,
                }));
            }
            if let Some(item) = self.pending.pop_front() {
                tracing::info!(path = %item.0.display(), "workflow file picked up");
                let payload = item.2.clone();
                self.inflight = Some(item);
                return Ok(Some(WorkflowMessage { payload, handle: AckHandle::None }));
            }
            self.scan().await?;
            if self.pending.is_empty() {
                tokio::time::sleep(self.poll).await;
            }
        }
    }

    /// The run (or dead letter) is durable; release the file.
    async fn ack(&mut self, _handle: &AckHandle) -> Result<()> {
        self.inflight = None;
        Ok(())
    }

    /// Keep the file in flight so the next `recv` redelivers it.
    async fn nack(&mut self, _handle: &AckHandle) -> Result<()> {
        Ok(())
    }

    /// Exactly-once: the in-flight file's fingerprint, committed by the ingest
    /// actor in the same transaction as the run it becomes — which is what makes a
    /// restart skip it instead of running it again.
    fn pending_position(&self) -> Option<PendingPosition> {
        self.inflight.as_ref().map(|(path, fingerprint, _)| PendingPosition {
            substream: Some(Self::substream(path)),
            position: fingerprint.clone(),
        })
    }
}

// ── ChannelSource ─────────────────────────────────────────────────────────────

/// In-process `mpsc` queue — the zero-infra reference "queue" backend. Useful
/// for tests and for embedding the scheduler in a larger process that produces
/// workflow specs directly. `recv` returns `None` once the sender is dropped.
#[allow(dead_code)] // library/embedding + test surface; not selectable via SOURCE env
pub struct ChannelSource {
    rx: tokio::sync::mpsc::Receiver<String>,
}

#[allow(dead_code)]
impl ChannelSource {
    pub fn new(rx: tokio::sync::mpsc::Receiver<String>) -> Self {
        Self { rx }
    }
}

#[async_trait]
impl WorkflowSource for ChannelSource {
    async fn recv(&mut self) -> Result<Option<WorkflowMessage>> {
        Ok(self
            .rx
            .recv()
            .await
            .map(|payload| WorkflowMessage { payload, handle: AckHandle::None }))
    }
}

// ── Source selection ──────────────────────────────────────────────────────────

/// Extension hook for ingestion sources beyond the built-in file/channel — e.g.
/// queue backends (Redis/SQS/Kafka/NATS). A build wires its factory into
/// [`build_with`]; the default engine passes `None` and supports only the
/// built-in sources.
#[async_trait]
pub trait SourceFactory: Send + Sync {
    /// Build the source for `kind`, or `Ok(None)` if this factory does not handle
    /// it (so the caller falls through to the built-ins, then errors).
    async fn build(&self, kind: &str, file_path: &str)
        -> Result<Option<Box<dyn WorkflowSource>>>;
}

/// Build the configured ingestion source, consulting `extra` (if any) before the
/// built-ins. `kind` comes from `$SOURCE` (default `file`); `file_path` is the DAG
/// path used by [`FileSource`]. Queue backends read their own settings from env.
pub async fn build_with(
    kind: &str,
    file_path: &str,
    extra: Option<&dyn SourceFactory>,
) -> Result<Box<dyn WorkflowSource>> {
    build_inner(kind, file_path, None, extra).await
}

/// [`build_with`] plus a datastore pool, which unlocks the multi-consumer
/// built-ins (a `STREAM_PATH` directory becomes a
/// [`ShardedStreamSource`](crate::stream::ShardedStreamSource) splitting its
/// shard files across engines via per-partition leases). The engine calls
/// this; `build_with` remains for pool-less embedding.
pub async fn build_pooled(
    kind: &str,
    file_path: &str,
    pool: &dagron_core::db::Pool,
    extra: Option<&dyn SourceFactory>,
) -> Result<Box<dyn WorkflowSource>> {
    build_inner(kind, file_path, Some(pool), extra).await
}

async fn build_inner(
    kind: &str,
    file_path: &str,
    pool: Option<&dagron_core::db::Pool>,
    extra: Option<&dyn SourceFactory>,
) -> Result<Box<dyn WorkflowSource>> {
    if let Some(factory) = extra {
        if let Some(src) = factory.build(kind, file_path).await? {
            return Ok(src);
        }
    }
    Ok(match kind {
        "file" => Box::new(FileSource::new(file_path)),
        // A directory of YAML, watched. Reads WORKFLOW_DIR — the knob that already
        // names the engine's workflow directory and the path compose mounts — rather
        // than the positional DAG argument, so there is one answer to "which
        // directory" instead of two that can disagree.
        "dir" => {
            let dir = std::env::var("WORKFLOW_DIR").unwrap_or_else(|_| "/workflows".to_string());
            let poll = std::env::var("DIR_POLL_MS")
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .unwrap_or(2000)
                .max(100);
            tracing::info!(dir = %dir, poll_ms = poll, "watching workflow directory");
            let src = DirSource::new(dir, std::time::Duration::from_millis(poll));
            // Per-file cursors, so a restart does not re-run the whole directory.
            // `kind` is the configured `SOURCE`, which is also the ingest actor's
            // `source_name` — the two must agree or they name different rows.
            Box::new(match pool {
                Some(p) => src.with_datastore(p.clone(), kind),
                None => src,
            })
        }
        "stream" => {
            let cfg = crate::stream::StreamConfig::from_env()?;
            // Mode is fixed at startup, so decide it explicitly: STREAM_MODE=
            // file|sharded|auto (default auto: an existing dir → sharded, an
            // existing file/FIFO → file, and an ABSENT path is an error rather
            // than a silent guess — a shard dir created later would otherwise
            // be tailed as a single file forever).
            let mode = std::env::var("STREAM_MODE")
                .unwrap_or_else(|_| "auto".to_string())
                .trim()
                .to_ascii_lowercase();
            let sharded = match mode.as_str() {
                "file" => false,
                "sharded" => true,
                "auto" | "" => match tokio::fs::metadata(&cfg.path).await {
                    Ok(m) => m.is_dir(),
                    Err(_) => bail!(
                        "STREAM_PATH '{}' does not exist, so the stream mode cannot be \
                         inferred. Create the file/FIFO or shard directory first, or set \
                         STREAM_MODE=file (wait for a single stream file) or \
                         STREAM_MODE=sharded (a directory of shard files).",
                        cfg.path.display()
                    ),
                },
                other => bail!("unknown STREAM_MODE '{other}' (file | sharded | auto)"),
            };
            if sharded {
                let Some(pool) = pool else {
                    bail!(
                        "STREAM_PATH '{}' selects sharded streaming — sharded \
                         consumption needs the engine's datastore for partition leases; \
                         use source::build_pooled",
                        cfg.path.display()
                    );
                };
                Box::new(crate::stream::ShardedStreamSource::new(
                    pool.clone(),
                    kind,
                    cfg.path,
                ))
            } else {
                Box::new(crate::stream::StreamSource::new(cfg))
            }
        }
        // A broker subscription: plant floor, gateway, robot fleet. Only compiled
        // with `--features mqtt` (a stock build links no MQTT client); without
        // it the kind is a clear startup error naming the flag, never a silent
        // fall-through to the unknown-kind message.
        #[cfg(feature = "mqtt")]
        "mqtt" => {
            let cfg = crate::mqtt::MqttConfig::from_env()?;
            tracing::info!(url = %cfg.url, topic = %cfg.topic, client_id = %cfg.client_id,
                qos = ?cfg.qos, position_field = cfg.position_field.as_deref().unwrap_or("- (at-least-once)"),
                "subscribing to MQTT broker");
            let src = crate::mqtt::MqttSource::new(cfg);
            // Per-topic exactly-once cursors, read back from `source_offsets`
            // under `<kind>/<topic>` — `kind` is the configured `SOURCE`, which
            // is also the ingest actor's `source_name`; they must agree.
            Box::new(match pool {
                Some(p) => src.with_datastore(p.clone(), kind),
                None => src,
            })
        }
        #[cfg(not(feature = "mqtt"))]
        "mqtt" => bail!(
            "SOURCE=mqtt requires building with `--features mqtt` (the MQTT client is not \
             linked into this binary — `cargo build --features mqtt`, or the image built \
             with it). See docs/STREAMING.md."
        ),
        // The fleet plane: enrolment, fan-out and staged bundle rollout across
        // many units. Its unit-side source is an enterprise SourceFactory; in
        // this build the kind is a signpost to the single-unit path.
        "fleet" => bail!(
            "SOURCE=fleet joins a managed fleet plane (enrolment, fan-out, staged bundle \
             rollout), which is not in this build — \
             https://github.com/lucheeseng827/dagron#what-this-build-does-not-do. This build runs one \
             unit: use SOURCE=dir, SOURCE=stream or SOURCE=mqtt and GitOps sync \
             (docs/OPERATIONS.md); custom sources plug in via the SourceFactory seam \
             (dagron_engine::Seams)."
        ),
        // The managed connector suite: name each kind precisely so the error is
        // an accurate signpost, not a dead end — the open build streams via
        // SOURCE=stream, embeds via the SourceFactory seam, and the managed
        // brokers are not in this build.
        "redis" | "sqs" | "kafka" | "nats" | "events" => bail!(
            "ingestion connector '{kind}' is not bundled in this build. Managed broker \
             connectors (Kafka, NATS, SQS, Redis) and the CloudEvents webhook gateway are \
             not part of it — https://github.com/lucheeseng827/dagron#what-this-build-does-not-do. \
             This build streams out of the box with SOURCE=stream (follow an NDJSON event \
             file or named pipe, at-least-once with a durable offset checkpoint — see \
             docs/STREAMING.md), and custom backends plug in via the SourceFactory seam \
             (dagron_engine::Seams)."
        ),
        other => bail!(
            "unknown SOURCE '{other}'. Built-in kinds: 'file' (one-shot), 'dir' (watch \
             WORKFLOW_DIR for YAML), 'stream' (follow an NDJSON file/pipe), 'mqtt' \
             (subscribe to a broker topic; needs a build with --features mqtt). Additional \
             connectors register through the SourceFactory seam."
        ),
    })
}

/// Built-in-only source selection. Use [`build_with`] to register
/// additional backends.
pub async fn build(kind: &str, file_path: &str) -> Result<Box<dyn WorkflowSource>> {
    build_with(kind, file_path, None).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn channel_source_yields_then_drains() {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tx.send("name: a\ntasks: []".to_string()).await.unwrap();
        tx.send("name: b\ntasks: []".to_string()).await.unwrap();
        drop(tx);

        let mut src = ChannelSource::new(rx);
        assert_eq!(src.recv().await.unwrap().unwrap().payload, "name: a\ntasks: []");
        assert_eq!(src.recv().await.unwrap().unwrap().payload, "name: b\ntasks: []");
        assert!(src.recv().await.unwrap().is_none(), "drains to None once sender drops");
    }

    /// The enterprise connector kinds fail with a signpost (what it is, where it
    /// exists, what this build offers instead).
    /// `fleet` is the fleet plane's unit-side source; its clause names the
    /// single-unit path (dir / stream / mqtt + GitOps) instead of a broker.
    #[tokio::test]
    async fn enterprise_connector_kinds_error_with_signpost() {
        for kind in ["redis", "sqs", "kafka", "nats", "events", "fleet"] {
            let err = build_with(kind, "unused.yaml", None).await.err().unwrap_or_else(|| {
                panic!("SOURCE={kind} must error without a registered SourceFactory")
            });
            let msg = err.to_string();
            assert!(msg.contains("not in this build") || msg.contains("not bundled"),
                "{kind}: names the gap ({msg})");
            assert!(msg.contains("SOURCE=stream"), "{kind}: offers the built-in path ({msg})");
            assert!(msg.contains("SourceFactory"), "{kind}: offers the seam ({msg})");
        }
        let fleet = build_with("fleet", "unused.yaml", None)
            .await
            .err()
            .expect("SOURCE=fleet must error without a factory")
            .to_string();
        assert!(fleet.contains("SOURCE=fleet joins a managed fleet plane"), "{fleet}");
        assert!(fleet.contains("SOURCE=mqtt"), "the fleet clause offers the MQTT on-ramp: {fleet}");
        assert!(fleet.contains("This build runs one unit"), "{fleet}");
    }

    /// The unknown-kind message lists every built-in, including the
    /// feature-gated one, so a typo's fix is on the screen.
    #[tokio::test]
    async fn unknown_kind_lists_every_builtin() {
        let msg = build_with("mqtt-v5", "unused.yaml", None)
            .await
            .err()
            .expect("an unknown SOURCE must error")
            .to_string();
        assert!(msg.starts_with("unknown SOURCE 'mqtt-v5'"), "{msg}");
        for kind in ["'file'", "'dir'", "'stream'", "'mqtt'", "SourceFactory"] {
            assert!(msg.contains(kind), "lists {kind}: {msg}");
        }
    }

    /// Without the feature, `SOURCE=mqtt` names the build flag rather than
    /// falling through to "unknown SOURCE".
    #[cfg(not(feature = "mqtt"))]
    #[tokio::test]
    async fn mqtt_kind_without_the_feature_names_the_build_flag() {
        let msg = build_with("mqtt", "unused.yaml", None)
            .await
            .err()
            .expect("SOURCE=mqtt without the feature must error")
            .to_string();
        assert!(msg.contains("--features mqtt"), "{msg}");
        assert!(!msg.contains("unknown SOURCE"), "{msg}");
    }

    /// With the feature, `SOURCE=mqtt` builds without a reachable broker — the
    /// client dials on the first poll, so a broker that is down at boot is a
    /// `recv` retry, not a failed start — and honours the env knobs.
    #[cfg(feature = "mqtt")]
    #[tokio::test]
    async fn mqtt_kind_builds_lazily_without_a_broker() {
        let _g = crate::mqtt::test_support::env_lock();
        std::env::set_var("MQTT_URL", "mqtt://127.0.0.1:1");
        std::env::set_var("MQTT_TOPIC", "plant/+/jobs");
        let src = build_with("mqtt", "unused.yaml", None).await;
        assert!(src.is_ok(), "construction is lazy: {:?}", src.err());
        // A malformed knob is still a startup error.
        std::env::set_var("MQTT_QOS", "9");
        let err = build_with("mqtt", "unused.yaml", None)
            .await
            .err()
            .expect("an unreachable broker must surface an error")
            .to_string();
        assert!(err.contains("MQTT_QOS"), "{err}");
    }

    /// A registered factory is consulted before the built-ins — the seam the
    /// enterprise build (and any custom embedding) hooks.
    #[tokio::test]
    async fn source_factory_overrides_builtin_error() {
        struct Fixed;
        #[async_trait]
        impl SourceFactory for Fixed {
            async fn build(
                &self,
                kind: &str,
                _file_path: &str,
            ) -> Result<Option<Box<dyn WorkflowSource>>> {
                Ok(match kind {
                    "kafka" => {
                        let (_tx, rx) = tokio::sync::mpsc::channel(1);
                        Some(Box::new(ChannelSource::new(rx)) as Box<dyn WorkflowSource>)
                    }
                    _ => None,
                })
            }
        }
        assert!(build_with("kafka", "unused.yaml", Some(&Fixed)).await.is_ok());
        assert!(build_with("nats", "unused.yaml", Some(&Fixed)).await.is_err(), "unhandled kind still funnels");
    }

    #[tokio::test]
    async fn file_source_emits_once_then_none() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("module54-src-{}.yaml", uuid::Uuid::new_v4()));
        tokio::fs::write(&path, "name: one\ntasks: []").await.unwrap();

        let mut src = FileSource::new(path.to_string_lossy().to_string());
        assert!(src.recv().await.unwrap().is_some());
        assert!(src.recv().await.unwrap().is_none(), "one-shot file drains after first emit");

        tokio::fs::remove_file(&path).await.ok();
    }

    /// A fresh scan picks up every spec once, in filename order, and does not hand
    /// the same unchanged file out twice.
    #[tokio::test]
    async fn dir_source_emits_each_file_once() {
        let dir = std::env::temp_dir().join(format!("m54-dir-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join("b.yaml"), "name: b
tasks: []").await.unwrap();
        tokio::fs::write(dir.join("a.yml"), "name: a
tasks: []").await.unwrap();
        // Not a spec: must be ignored, or a README in the inbox becomes a dead letter.
        tokio::fs::write(dir.join("README.md"), "not a workflow").await.unwrap();

        // Acked between pulls, as the ingest actor does — an unacked message stays
        // in flight and redelivers (see `dir_source_nack_redelivers_the_same_file`).
        let mut src = DirSource::new(&dir, Duration::from_millis(100));
        let first = src.recv().await.unwrap().unwrap();
        src.ack(&first.handle).await.unwrap();
        let second = src.recv().await.unwrap().unwrap();
        src.ack(&second.handle).await.unwrap();
        assert!(first.payload.contains("name: a"), "filename order: a.yml before b.yaml");
        assert!(second.payload.contains("name: b"));

        // Nothing changed, so nothing more is due: recv() blocks rather than
        // re-emitting. A timeout is the assertion.
        let idle = tokio::time::timeout(Duration::from_millis(400), src.recv()).await;
        assert!(idle.is_err(), "an unchanged directory must not re-emit");

        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    /// The point of watching: a file that appears after startup is picked up, and an
    /// edited file is re-submitted.
    #[tokio::test]
    async fn dir_source_sees_added_and_changed_files() {
        let dir = std::env::temp_dir().join(format!("m54-dir-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let mut src = DirSource::new(&dir, Duration::from_millis(50));

        // Added after the source started — FileSource would never see this.
        let f = dir.join("later.yaml");
        tokio::fs::write(&f, "name: v1
tasks: []").await.unwrap();
        let got = tokio::time::timeout(Duration::from_secs(5), src.recv())
            .await
            .expect("a file added after startup must be picked up")
            .unwrap()
            .unwrap();
        assert!(got.payload.contains("name: v1"));
        src.ack(&got.handle).await.unwrap();

        // Edited: different length, so the (mtime, len) key changes even where the
        // filesystem timestamp is coarse.
        tokio::fs::write(&f, "name: v2-edited
tasks: []").await.unwrap();
        let got = tokio::time::timeout(Duration::from_secs(5), src.recv())
            .await
            .expect("an edited file must be re-submitted")
            .unwrap()
            .unwrap();
        assert!(got.payload.contains("name: v2-edited"));

        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    /// A nacked file is handed back. The ingest actor nacks when a run could not be
    /// created *yet* (a workflow at its `max_active_runs` cap) and logs "nacking for
    /// later redelivery"; nothing else re-offers the file, because the scan has
    /// already recorded its fingerprint.
    #[tokio::test]
    async fn dir_source_nack_redelivers_the_same_file() {
        let dir = std::env::temp_dir().join(format!("m54-dir-nack-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join("only.yaml"), "name: only\ntasks: []").await.unwrap();

        let mut src = DirSource::new(&dir, Duration::from_millis(50));
        let first = src.recv().await.unwrap().unwrap();
        assert!(first.payload.contains("name: only"));

        src.nack(&first.handle).await.unwrap();
        let again = tokio::time::timeout(Duration::from_millis(500), src.recv())
            .await
            .expect("a nacked file must redeliver, not be dropped")
            .unwrap()
            .unwrap();
        assert!(again.payload.contains("name: only"));

        // Acked: released, and an unchanged file is not offered a third time.
        src.ack(&again.handle).await.unwrap();
        let idle = tokio::time::timeout(Duration::from_millis(400), src.recv()).await;
        assert!(idle.is_err(), "an acked, unchanged file must not redeliver");

        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    /// A restart does not re-run the directory. The fingerprint committed with the
    /// run identifies the version already ingested, so a fresh source over the same
    /// directory skips it — and still picks the file up once it is edited.
    #[tokio::test]
    async fn dir_source_does_not_resubmit_after_a_restart() {
        let db_path = std::env::temp_dir().join(format!("m54-dir-{}.db", uuid::Uuid::new_v4()));
        let pool = dagron_core::db::init_pool(db_path.to_str().unwrap()).await.unwrap();
        let dir = std::env::temp_dir().join(format!("m54-dir-restart-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let spec = "name: restart-me\ntasks:\n  - name: a\n    command: [\"true\"]\n";
        let file = dir.join("wf.yaml");
        tokio::fs::write(&file, spec).await.unwrap();

        // First process: pick the file up and create its run exactly as the ingest
        // actor does — the cursor rides the run's transaction.
        let mut src = DirSource::new(&dir, Duration::from_millis(50)).with_datastore(pool.clone(), "dir");
        let msg = src.recv().await.unwrap().unwrap();
        let pos = src.pending_position().expect("a dir message carries its coordinate");
        let dag = dagron_core::dag::DagGraph::from_yaml(&msg.payload).unwrap();
        dagron_core::db::create_run_with_offset(
            &pool,
            &dag,
            &msg.payload,
            &pos.offset_key("dir"),
            &pos.position,
        )
        .await
        .unwrap();
        src.ack(&msg.handle).await.unwrap();
        drop(src);

        // Restart: same directory, same datastore, nothing new to run.
        let mut restarted =
            DirSource::new(&dir, Duration::from_millis(50)).with_datastore(pool.clone(), "dir");
        let idle = tokio::time::timeout(Duration::from_millis(500), restarted.recv()).await;
        assert!(idle.is_err(), "a restart must not re-submit an already-ingested file");

        // Edited after the restart: a different version, so it runs.
        tokio::fs::write(&file, "name: restart-me-v2\ntasks:\n  - name: a\n    command: [\"true\"]\n")
            .await
            .unwrap();
        let got = tokio::time::timeout(Duration::from_secs(5), restarted.recv())
            .await
            .expect("an edit after a restart must still be picked up")
            .unwrap()
            .unwrap();
        assert!(got.payload.contains("restart-me-v2"));

        tokio::fs::remove_dir_all(&dir).await.ok();
        tokio::fs::remove_file(&db_path).await.ok();
    }

    /// Without a datastore (the `build` path embedders use) the source still works —
    /// it just remembers in memory only.
    #[tokio::test]
    async fn dir_source_without_a_datastore_still_runs() {
        let dir = std::env::temp_dir().join(format!("m54-dir-nodb-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join("a.yaml"), "name: a\ntasks: []").await.unwrap();

        let mut src = DirSource::new(&dir, Duration::from_millis(50));
        let got = src.recv().await.unwrap().unwrap();
        assert!(got.payload.contains("name: a"));
        assert!(src.pending_position().is_some(), "the coordinate is still offered");

        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    /// A directory that does not exist yet is not fatal — the mount may arrive after
    /// the engine does.
    #[tokio::test]
    async fn dir_source_tolerates_a_missing_directory() {
        let dir = std::env::temp_dir().join(format!("m54-dir-absent-{}", uuid::Uuid::new_v4()));
        let mut src = DirSource::new(&dir, Duration::from_millis(50));

        let idle = tokio::time::timeout(Duration::from_millis(300), src.recv()).await;
        assert!(idle.is_err(), "a missing directory idles, it does not error");

        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join("x.yaml"), "name: x
tasks: []").await.unwrap();
        let got = tokio::time::timeout(Duration::from_secs(5), src.recv())
            .await
            .expect("the directory appearing later must start working")
            .unwrap()
            .unwrap();
        assert!(got.payload.contains("name: x"));

        tokio::fs::remove_dir_all(&dir).await.ok();
    }
}
