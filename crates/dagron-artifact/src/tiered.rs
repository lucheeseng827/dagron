//! Tiered artifact store — local tier always, deferred budgeted upload to a
//! remote tier, content-addressed dedup, and a durable NDJSON ledger. The
//! capture → curate → uplink primitive for units on a metered link.
//!
//! # Why a decorator, not a new backend
//!
//! A unit on a metered or intermittent link (a robot, a vehicle, a plant cell)
//! must never lose a capture because the uplink is down, and must never burn
//! its data plan the moment the link returns. [`TieredStore`] therefore answers
//! every write from the **local** tier immediately and moves bytes to the
//! **remote** tier only inside [`sync`](ArtifactStore::sync) /
//! [`drain_at`](TieredStore::drain_at), under a declared per-UTC-day byte
//! budget ([`UplinkBudget`]). Both tiers are plain [`ArtifactStore`]s, so the
//! existing local-FS and cloud backends — and the encrypting decorator —
//! compose without any new storage code.
//!
//! # Layout
//!
//! * artifacts: wherever the local tier puts them (`<base>/<run>/<task>/<name>`
//!   for [`LocalFsStore`](crate::LocalFsStore));
//! * ledger: `<base>/.tiered/ledger.ndjson` — depth 2 on purpose.
//!   [`LocalFsStore::list`](crate::LocalFsStore) reports only files at depth 3,
//!   so neither the scan below nor the key-rotation sweep ever mistakes the
//!   ledger (or its compaction temp file) for an artifact. `.tiered` is
//!   consequently a reserved `run_id`: `put` refuses it.
//!
//! # The ledger
//!
//! One JSON record per line, appended before the call that caused it returns;
//! a compaction rewrites the live state atomically (temp file + `rename`) once
//! the appended tail outgrows it. Record kinds:
//!
//! * `pending` — bytes landed locally with content hash `sha`; not yet remote;
//! * `done` — uploaded, with the remote locator;
//! * `uploaded_by_sha` — dedup alias: identical bytes were already uploaded
//!   under the canonical key `of`, so no transfer happened;
//! * `day` — the UTC-day counter (`day` = days since the epoch, `bytes`
//!   moved so far that day);
//! * `forget` — the entry was dropped (its local copy vanished before it could
//!   move, so nothing is left to uplink).
//!
//! Replay is last-write-wins per key. It tolerates a torn last line (a crash
//! mid-append) and skips malformed lines with a warning; either marks the
//! ledger for compaction on the next write. Losing a `done` record costs at
//! most one re-upload (remote puts are idempotent overwrites); losing a
//! `pending` record is repaired by the next scan. The ledger is a cache of
//! *what has moved*, never the only copy of anything, which is why it is not
//! fsync'd on every append (flash wear on the unit) — only at compaction.
//!
//! # Hashing and dedup
//!
//! The hash is SHA-256 of the bytes **this store receives**. Composed as
//! `EncryptedStore<TieredStore<…>>` those bytes are ciphertext under a fresh
//! per-object data key, so two identical plaintexts hash differently and dedup
//! is effectively off — the price of never letting a storage tier see
//! plaintext. Dedup is within this unit only (its own ledger).
//!
//! # Reads
//!
//! `get` / `get_stream` / `exists` try the local tier first and fall back to
//! the remote tier, resolving a dedup alias to its canonical key **only while
//! that canonical is still `done` with the same hash** — a re-put canonical
//! must never serve another key's bytes (the stale-alias guard). When both
//! tiers miss, the **local** error is returned unchanged, so the
//! `std::io::ErrorKind::NotFound` the management API maps to `404` survives. A
//! remote *failure* on that path (link down) is logged at `warn` and treated
//! as a miss: on a unit the honest answer to "is it here?" is still no.
//! `run_location` and `list` are the local tier's — tasks keep writing into
//! the shared run directory exactly as before.
//!
//! # What the scan uplinks
//!
//! `drain_at` first walks `local.list()` and ledgers every unledgered key, so
//! files a task wrote straight into its `DAGRON_ARTIFACTS` dir move too —
//! unless the tier is sealed by an outer [`EncryptedStore`](crate::EncryptedStore)
//! (`sealed_by_outer_encryption`), in which case those files are plaintext this
//! tier holds no key for, and they stay local rather than reach the remote tier
//! in the clear. With
//! the local-FS tier that walk reports files at exactly depth 3
//! (`<run>/<task>/<name>`): the engine's per-task checkpoint dir
//! `<run>/.checkpoints/<task>/…` sits one level deeper and is **not** seen,
//! while a file placed directly at `<run>/.checkpoints/<name>` is reported as
//! task `.checkpoints` and uplinked like any other artifact. A file is hashed
//! and uplinked in whatever state it is in at scan time — write it atomically
//! (temp + rename). An entry ledgered `done` is not re-hashed by later scans;
//! overwrite it through `put` to re-queue it.
//!
//! # Draining
//!
//! Oldest first. An entry that would exceed today's remaining budget stops the
//! drain (strict order — a large capture is never starved by a stream of small
//! ones); an entry larger than the *whole* daily budget can never move, so it
//! is skipped with a warning rather than blocking the queue forever. Dedup
//! aliases cost no budget. A remote failure aborts the drain with an error
//! after recording what already moved; the next drain resumes from the ledger.
//!
//! # Edition line
//!
//! This is the open, single-unit primitive: one local tier, one remote tier, a
//! byte budget, dedup within the unit. Managed transfer through the fleet plane
//! with resumable chunks and central dedup is not in this build
//! (<https://github.com/lucheeseng827/dagron#what-this-build-does-not-do>); the open
//! build drains straight to `DAGRON_ARTIFACT_URL` as described here.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context as TaskContext, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio::sync::{MappedMutexGuard, Mutex, MutexGuard};

use crate::{sanitize_component, ArtifactKey, ArtifactStore};

/// Directory under the local tier's base that holds the ledger — and therefore
/// a reserved `run_id` (see the module docs on depth).
pub const LEDGER_DIR: &str = ".tiered";
/// The ledger file name inside [`LEDGER_DIR`].
pub const LEDGER_FILE: &str = "ledger.ndjson";
const SECS_PER_DAY: u64 = 86_400;
/// Read granularity when hashing a local artifact (scan / stream paths).
const HASH_CHUNK: usize = 64 * 1024;
/// Compact once the appended tail outgrows twice the live state — and never
/// below this many appended records, where a rewrite costs more than it saves.
const COMPACT_MIN_APPENDED: usize = 256;

/// How many bytes a unit may move to the remote tier per UTC day.
///
/// `None` = unlimited. The day boundary is UTC (days since the Unix epoch), so
/// every unit in a fleet rolls over at the same instant regardless of local
/// time zone — the plan is metered that way too.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct UplinkBudget {
    /// Bytes per UTC day; `None` = no cap.
    pub bytes_per_day: Option<u64>,
}

impl UplinkBudget {
    /// No cap.
    pub const UNLIMITED: Self = Self { bytes_per_day: None };

    /// A cap of `bytes` per UTC day; `0` means unlimited.
    pub fn per_day(bytes: u64) -> Self {
        Self { bytes_per_day: (bytes > 0).then_some(bytes) }
    }

    /// Read `DAGRON_ARTIFACT_UPLINK_BYTES_PER_DAY` (unset / empty / `0` =
    /// unlimited). A malformed value is a hard error: silently meaning
    /// "unlimited" on a metered link is exactly the surprise this knob exists
    /// to prevent.
    pub fn from_env() -> Result<Self> {
        Self::parse(std::env::var("DAGRON_ARTIFACT_UPLINK_BYTES_PER_DAY").ok().as_deref())
    }

    /// Pure parser behind [`from_env`](Self::from_env).
    pub fn parse(raw: Option<&str>) -> Result<Self> {
        let Some(raw) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
            return Ok(Self::UNLIMITED);
        };
        let bytes: u64 = raw.parse().with_context(|| {
            format!(
                "DAGRON_ARTIFACT_UPLINK_BYTES_PER_DAY must be a byte count (0 = unlimited), got '{raw}'"
            )
        })?;
        Ok(Self::per_day(bytes))
    }

    fn fits(&self, used_today: u64, bytes: u64) -> bool {
        self.bytes_per_day.is_none_or(|cap| used_today.saturating_add(bytes) <= cap)
    }

    fn oversize(&self, bytes: u64) -> bool {
        self.bytes_per_day.is_some_and(|cap| bytes > cap)
    }
}

// ── ledger records (the on-disk NDJSON shapes) ───────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Record {
    Pending { key: ArtifactKey, sha: String, bytes: u64, at: u64 },
    Done { key: ArtifactKey, sha: String, bytes: u64, at: u64, remote: String },
    UploadedBySha { key: ArtifactKey, sha: String, bytes: u64, at: u64, of: ArtifactKey },
    Day { day: u64, bytes: u64 },
    Forget { key: ArtifactKey },
}

// ── in-memory ledger state ───────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
enum State {
    Pending,
    Done { remote: String },
    Alias { of: ArtifactKey },
}

#[derive(Debug, Clone)]
struct Entry {
    key: ArtifactKey,
    sha: String,
    bytes: u64,
    /// Unix seconds when the entry entered its current state (drain order).
    at: u64,
    /// Insertion order — the tie-break inside one second.
    seq: u64,
    state: State,
}

#[derive(Debug, Default)]
struct Ledger {
    /// Keyed by `ArtifactKey::rel_path()` — the same locator every tier uses.
    entries: BTreeMap<String, Entry>,
    /// Content hash → rel_path of a `done` canonical (the dedup index).
    by_sha: HashMap<String, String>,
    /// The UTC day (days since the epoch) `day_bytes` counts for.
    day: u64,
    day_bytes: u64,
    next_seq: u64,
    /// Records appended since the last compaction.
    appended: usize,
    /// Set when replay saw damage or an append failed: the next write rewrites
    /// the whole file instead of appending to a suspect tail.
    needs_compaction: bool,
}

impl Ledger {
    /// The single state-transition function — replay and live updates both go
    /// through here, so what is on disk and what is in memory can never drift.
    fn apply(&mut self, rec: &Record) {
        match rec {
            Record::Pending { key, sha, bytes, at } => {
                self.insert(key, sha, *bytes, *at, State::Pending);
            }
            Record::Done { key, sha, bytes, at, remote } => {
                self.insert(key, sha, *bytes, *at, State::Done { remote: remote.clone() });
                self.by_sha.insert(sha.clone(), key.rel_path());
            }
            Record::UploadedBySha { key, sha, bytes, at, of } => {
                self.insert(key, sha, *bytes, *at, State::Alias { of: of.clone() });
            }
            Record::Day { day, bytes } => {
                self.day = *day;
                self.day_bytes = *bytes;
            }
            Record::Forget { key } => {
                let rel = key.rel_path();
                self.detach(&rel);
                self.entries.remove(&rel);
            }
        }
    }

    fn insert(&mut self, key: &ArtifactKey, sha: &str, bytes: u64, at: u64, state: State) {
        let rel = key.rel_path();
        self.detach(&rel);
        let seq = self.next_seq;
        self.next_seq += 1;
        self.entries
            .insert(rel, Entry { key: key.clone(), sha: sha.to_string(), bytes, at, seq, state });
    }

    /// Drop the dedup index pointer if it names `rel` — its bytes are about to
    /// change or go away, so no alias may resolve to it any more.
    fn detach(&mut self, rel: &str) {
        if let Some(old) = self.entries.get(rel) {
            if self.by_sha.get(&old.sha).is_some_and(|r| r == rel) {
                self.by_sha.remove(&old.sha);
            }
        }
    }

    /// A `done` entry (other than `exclude_rel`) holding exactly these bytes.
    fn canonical_for(&self, sha: &str, exclude_rel: &str) -> Option<&Entry> {
        let rel = self.by_sha.get(sha)?;
        if rel == exclude_rel {
            return None;
        }
        let e = self.entries.get(rel)?;
        (matches!(e.state, State::Done { .. }) && e.sha == sha).then_some(e)
    }

    /// The stale-alias guard: an alias is only good while its canonical is
    /// still `done` with the same hash.
    fn alias_target_valid(&self, e: &Entry) -> bool {
        match &e.state {
            State::Alias { of } => self
                .entries
                .get(&of.rel_path())
                .is_some_and(|c| matches!(c.state, State::Done { .. }) && c.sha == e.sha),
            _ => false,
        }
    }

    /// Which key to ask the remote tier for. `None` = a stale alias: the remote
    /// never held bytes under this key and the canonical no longer matches.
    fn remote_key_for(&self, key: &ArtifactKey) -> Option<ArtifactKey> {
        match self.entries.get(&key.rel_path()) {
            Some(e) => match &e.state {
                State::Alias { of } if self.alias_target_valid(e) => Some(of.clone()),
                State::Alias { .. } => None,
                // done, pending (a prior life may have uploaded it), or the
                // remote may simply have it from elsewhere: ask.
                _ => Some(key.clone()),
            },
            None => Some(key.clone()),
        }
    }

    /// True when `key` already holds exactly these bytes in a state that needs
    /// no new record (pending, done, or a still-valid alias).
    fn is_settled(&self, key: &ArtifactKey, sha: &str) -> bool {
        self.entries.get(&key.rel_path()).is_some_and(|e| {
            e.sha == sha
                && match &e.state {
                    State::Pending | State::Done { .. } => true,
                    State::Alias { .. } => self.alias_target_valid(e),
                }
        })
    }

    fn pending_in_order(&self) -> Vec<Entry> {
        let mut v: Vec<Entry> =
            self.entries.values().filter(|e| e.state == State::Pending).cloned().collect();
        v.sort_by_key(|e| (e.at, e.seq));
        v
    }

    fn pending_count(&self) -> usize {
        self.entries.values().filter(|e| e.state == State::Pending).count()
    }

    fn stale_aliases(&self) -> Vec<Entry> {
        self.entries
            .values()
            .filter(|e| matches!(e.state, State::Alias { .. }) && !self.alias_target_valid(e))
            .cloned()
            .collect()
    }

    /// The live state as records, in replay order — what a compaction writes.
    fn snapshot(&self) -> Vec<Record> {
        let mut entries: Vec<&Entry> = self.entries.values().collect();
        entries.sort_by_key(|e| (e.at, e.seq));
        let mut out: Vec<Record> = entries
            .into_iter()
            .map(|e| match &e.state {
                State::Pending => Record::Pending {
                    key: e.key.clone(),
                    sha: e.sha.clone(),
                    bytes: e.bytes,
                    at: e.at,
                },
                State::Done { remote } => Record::Done {
                    key: e.key.clone(),
                    sha: e.sha.clone(),
                    bytes: e.bytes,
                    at: e.at,
                    remote: remote.clone(),
                },
                State::Alias { of } => Record::UploadedBySha {
                    key: e.key.clone(),
                    sha: e.sha.clone(),
                    bytes: e.bytes,
                    at: e.at,
                    of: of.clone(),
                },
            })
            .collect();
        out.push(Record::Day { day: self.day, bytes: self.day_bytes });
        out
    }
}

// ── ledger I/O ───────────────────────────────────────────────────────────────

async fn load_ledger(path: &Path) -> Result<Ledger> {
    let raw = match tokio::fs::read(path).await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Ledger::default()),
        Err(e) => {
            return Err(e).with_context(|| format!("read tier ledger {}", path.display()))
        }
    };
    let text = String::from_utf8_lossy(&raw);
    let ends_clean = text.ends_with('\n');
    let lines: Vec<&str> = text.lines().collect();
    let n = lines.len();
    let mut ledger = Ledger::default();
    for (i, line) in lines.iter().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Record>(line) {
            Ok(rec) => ledger.apply(&rec),
            Err(e) => {
                // The last line without a trailing newline is the signature of
                // a crash mid-append: expected, not damage worth a warning.
                if i + 1 == n && !ends_clean {
                    tracing::debug!(path = %path.display(), "tier ledger: torn last line ignored (crash mid-append)");
                } else {
                    tracing::warn!(path = %path.display(), line = i + 1, error = %e, "tier ledger: skipping malformed line");
                }
                ledger.needs_compaction = true;
            }
        }
    }
    Ok(ledger)
}

fn encode(records: &[Record]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    for r in records {
        buf.extend_from_slice(&serde_json::to_vec(r).context("encode tier ledger record")?);
        buf.push(b'\n');
    }
    Ok(buf)
}

async fn append_records(path: &Path, records: &[Record]) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let buf = encode(records)?;
    let mut f = tokio::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .await
        .with_context(|| format!("open tier ledger {}", path.display()))?;
    f.write_all(&buf).await?;
    f.flush().await?;
    Ok(())
}

/// Atomic rewrite: temp file (same dir, so `rename` is a same-filesystem
/// move), fsync, rename over the live ledger. A crash leaves either the old or
/// the new file, never a truncated one.
async fn rewrite_ledger(path: &Path, records: &[Record]) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp = path.with_file_name(format!("{LEDGER_FILE}.tmp"));
    let buf = encode(records)?;
    {
        let mut f = tokio::fs::File::create(&tmp)
            .await
            .with_context(|| format!("create {}", tmp.display()))?;
        f.write_all(&buf).await?;
        f.sync_all().await?;
    }
    tokio::fs::rename(&tmp, path)
        .await
        .with_context(|| format!("replace tier ledger {}", path.display()))?;
    Ok(())
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn now_secs(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// Whether a store error is a plain "no such object" (as opposed to a fault).
/// Local FS errors surface as `std::io::Error`; the cloud backend wraps
/// `object_store::Error::NotFound` (anyhow's `context` keeps both
/// downcastable).
fn is_not_found(e: &anyhow::Error) -> bool {
    if e.downcast_ref::<std::io::Error>().is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound)
    {
        return true;
    }
    #[cfg(feature = "cloud")]
    if matches!(e.downcast_ref::<object_store::Error>(), Some(object_store::Error::NotFound { .. }))
    {
        return true;
    }
    false
}

/// Log a remote-tier read that did not produce bytes: a miss is routine
/// (`debug`), a fault (link down, auth) deserves a `warn` even though the
/// caller still sees a miss.
fn log_remote_miss(key: &ArtifactKey, e: &anyhow::Error) {
    if is_not_found(e) {
        tracing::debug!(key = %key.rel_path(), "artifact missing from both tiers");
    } else {
        tracing::warn!(key = %key.rel_path(), error = %e, "remote artifact tier failed; treating as a miss");
    }
}

/// Tees every byte the local tier reads out of a `put_stream` into a SHA-256,
/// so the ledger gets the content hash in the same pass — the artifact is never
/// re-read to hash it. The state is shared behind a std mutex because the local
/// tier owns (and drops) the reader; the store finalises after the write.
struct HashingReader {
    inner: Box<dyn AsyncRead + Send + Unpin>,
    state: Arc<StdMutex<(Sha256, u64)>>,
}

impl AsyncRead for HashingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let res = Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(res, Poll::Ready(Ok(()))) {
            let fresh = &buf.filled()[before..];
            if !fresh.is_empty() {
                let mut g = self.state.lock().unwrap_or_else(|p| p.into_inner());
                g.0.update(fresh);
                g.1 += fresh.len() as u64;
            }
        }
        res
    }
}

// ── the store ────────────────────────────────────────────────────────────────

/// Local tier + remote tier + ledger + budget. See the module docs.
///
/// `L` is normally [`LocalFsStore`](crate::LocalFsStore) (the ledger lives
/// under its base dir); `R` is any [`ArtifactStore`] — the cloud backend, or a
/// type-erased `Box<dyn ArtifactStore>` (the blanket impls in the crate root
/// make both pointer types stores in their own right).
pub struct TieredStore<L, R> {
    local: L,
    remote: R,
    budget: UplinkBudget,
    ledger_path: PathBuf,
    /// Lazily loaded on first use so construction stays synchronous and
    /// infallible; a tokio mutex because it is held across ledger writes.
    ledger: Mutex<Option<Ledger>>,
    /// One drain at a time (the periodic loop and the on-demand route may
    /// overlap); the second caller waits rather than double-uploading.
    drain_lock: Mutex<()>,
    /// Set when an [`EncryptedStore`](crate::EncryptedStore) wraps this tier.
    /// Everything written through `put` is then already ciphertext, but a file a
    /// task wrote straight into the shared run directory is not — and the tier
    /// sits *inside* the decorator, so it has no key to seal one with. When this
    /// is set the scan stops adopting those files, because uplinking them would
    /// put plaintext in the remote tier under a configuration whose whole
    /// premise is that the remote tier holds none.
    seal_scan: bool,
}

impl<L: ArtifactStore, R: ArtifactStore> TieredStore<L, R> {
    /// Tier `local` under `remote`, keeping the ledger at
    /// `<local_base>/.tiered/ledger.ndjson`.
    pub fn new(local: L, remote: R, local_base: impl AsRef<Path>, budget: UplinkBudget) -> Self {
        Self {
            local,
            remote,
            budget,
            ledger_path: local_base.as_ref().join(LEDGER_DIR).join(LEDGER_FILE),
            ledger: Mutex::new(None),
            drain_lock: Mutex::new(()),
            seal_scan: false,
        }
    }

    /// Declare that an [`EncryptedStore`](crate::EncryptedStore) wraps this tier,
    /// which stops the scan from adopting task-written files. See [`Self::seal_scan`].
    #[must_use]
    pub fn sealed_by_outer_encryption(mut self, sealed: bool) -> Self {
        self.seal_scan = sealed;
        self
    }

    /// Where the ledger lives.
    pub fn ledger_path(&self) -> &Path {
        &self.ledger_path
    }

    /// The local tier.
    pub fn local(&self) -> &L {
        &self.local
    }

    /// The remote tier.
    pub fn remote(&self) -> &R {
        &self.remote
    }

    /// The configured budget.
    pub fn budget(&self) -> UplinkBudget {
        self.budget
    }

    /// How many ledgered artifacts still wait for the remote tier.
    pub async fn pending(&self) -> Result<usize> {
        Ok(self.ledger().await?.pending_count())
    }

    /// `true` for the reserved ledger `run_id`.
    fn is_reserved(key: &ArtifactKey) -> bool {
        sanitize_component(&key.run_id) == LEDGER_DIR
    }

    fn check_key(key: &ArtifactKey) -> Result<()> {
        if Self::is_reserved(key) {
            bail!("run_id '{LEDGER_DIR}' is reserved for the artifact tier ledger");
        }
        Ok(())
    }

    async fn ledger(&self) -> Result<MappedMutexGuard<'_, Ledger>> {
        let mut guard = self.ledger.lock().await;
        if guard.is_none() {
            *guard = Some(load_ledger(&self.ledger_path).await?);
        }
        Ok(MutexGuard::map(guard, |o| o.as_mut().expect("ledger loaded above")))
    }

    /// Apply `records` to the in-memory state and make them durable — by
    /// appending, or by a full atomic rewrite when the tail has outgrown the
    /// live state or the file is suspect. Memory is updated first so a failed
    /// write leaves us *ahead* of disk (self-healing on the next write / scan),
    /// never behind it.
    async fn persist(&self, ledger: &mut Ledger, records: Vec<Record>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        for r in &records {
            ledger.apply(r);
        }
        let threshold = COMPACT_MIN_APPENDED.max(2 * ledger.entries.len());
        let compact = ledger.needs_compaction || ledger.appended + records.len() > threshold;
        let written = if compact {
            rewrite_ledger(&self.ledger_path, &ledger.snapshot()).await
        } else {
            append_records(&self.ledger_path, &records).await
        };
        match written {
            Ok(()) => {
                ledger.appended = if compact { 0 } else { ledger.appended + records.len() };
                ledger.needs_compaction = false;
                Ok(())
            }
            Err(e) => {
                ledger.needs_compaction = true;
                Err(e)
            }
        }
    }

    async fn hash_local(&self, key: &ArtifactKey) -> Result<(String, u64)> {
        let mut r = self.local.get_stream(key).await?;
        let mut h = Sha256::new();
        let mut n = 0u64;
        let mut buf = vec![0u8; HASH_CHUNK];
        loop {
            let k = r.read(&mut buf).await?;
            if k == 0 {
                break;
            }
            h.update(&buf[..k]);
            n += k as u64;
        }
        Ok((hex::encode(h.finalize()), n))
    }

    /// Ledger a fresh local write. Same bytes already tracked ⇒ no record: a
    /// `done` entry stays done (no re-upload), a pending one stays queued.
    async fn ledger_put(&self, key: &ArtifactKey, sha: String, bytes: u64) -> Result<()> {
        let mut l = self.ledger().await?;
        if l.is_settled(key, &sha) {
            return Ok(());
        }
        let rec = Record::Pending { key: key.clone(), sha, bytes, at: now_secs(SystemTime::now()) };
        self.persist(&mut l, vec![rec]).await
    }

    /// The drain at an injected instant (`sync` passes `SystemTime::now()`):
    /// scan the local tier for unledgered keys, repair stale aliases, then move
    /// pending entries oldest-first under the UTC-day budget of `now`. Returns
    /// how many entries left `pending` (uploads + dedup aliases). Resume-safe:
    /// every transition is ledgered as it happens, so a drain cut short by a
    /// remote fault or a restart simply continues next time.
    pub async fn drain_at(&self, now: SystemTime) -> Result<usize> {
        let _one_at_a_time = self.drain_lock.lock().await;
        let now_secs = now_secs(now);
        let today = now_secs / SECS_PER_DAY;

        // 1. Scan: ledger every artifact the local tier holds that we have not
        //    seen (task-written files, a ledger lost to a torn write, …).
        let listed = self.local.list().await.context("scan the local artifact tier")?;
        let unledgered: Vec<ArtifactKey> = {
            let l = self.ledger().await?;
            listed
                .into_iter()
                .filter(|k| !Self::is_reserved(k) && !l.entries.contains_key(&k.rel_path()))
                .collect()
        };
        // Under an outer EncryptedStore these are plaintext and this tier holds
        // no key, so adopting them would uplink them in the clear.
        let unledgered = if self.seal_scan && !unledgered.is_empty() {
            tracing::warn!(
                files = unledgered.len(),
                "tier scan: artifacts are encrypted at rest, so task-written files are kept local \
                 and not uplinked; write them through the artifact store to have them move"
            );
            Vec::new()
        } else {
            unledgered
        };
        let mut records = Vec::new();
        let mut discovered = 0usize;
        for key in unledgered {
            match self.hash_local(&key).await {
                Ok((sha, bytes)) => {
                    discovered += 1;
                    records.push(Record::Pending { key, sha, bytes, at: now_secs });
                }
                Err(e) => tracing::warn!(key = %key.rel_path(), error = %e, "tier scan: cannot hash local artifact; skipped this sweep"),
            }
        }

        // 2. Stale aliases: their canonical was re-put or forgotten, so the
        //    remote no longer holds their bytes under any key. Re-queue under
        //    the alias's own key while the local copy exists; forget otherwise.
        let stale = self.ledger().await?.stale_aliases();
        let mut requeued = 0usize;
        for e in stale {
            if self.local.exists(&e.key).await.unwrap_or(false) {
                requeued += 1;
                records.push(Record::Pending { key: e.key, sha: e.sha, bytes: e.bytes, at: now_secs });
            } else {
                tracing::warn!(key = %e.key.rel_path(), "tier: dedup alias lost its canonical and has no local copy; forgotten");
                records.push(Record::Forget { key: e.key });
            }
        }
        {
            let mut l = self.ledger().await?;
            if l.day != today {
                records.push(Record::Day { day: today, bytes: 0 });
            }
            self.persist(&mut l, records).await?;
        }

        // 3. Drain, oldest first, under today's budget.
        let order = self.ledger().await?.pending_in_order();
        let total = order.len();
        let (mut uploaded, mut deduped, mut oversize, mut deferred) = (0usize, 0usize, 0usize, 0usize);
        let mut bytes_moved = 0u64;
        for (idx, want) in order.iter().enumerate() {
            let rel = want.key.rel_path();
            let upload = {
                let mut l = self.ledger().await?;
                let Some(cur) = l.entries.get(&rel) else { continue };
                if cur.state != State::Pending || cur.sha != want.sha {
                    continue; // re-put mid-drain: the next sweep takes it
                }
                let cur = cur.clone();
                if let Some(canon) = l.canonical_for(&cur.sha, &rel) {
                    let of = canon.key.clone();
                    tracing::debug!(key = %rel, of = %of.rel_path(), "tier: identical bytes already remote; aliasing");
                    self.persist(
                        &mut l,
                        vec![Record::UploadedBySha {
                            key: cur.key.clone(),
                            sha: cur.sha.clone(),
                            bytes: cur.bytes,
                            at: now_secs,
                            of,
                        }],
                    )
                    .await?;
                    deduped += 1;
                    continue;
                }
                if self.budget.oversize(cur.bytes) {
                    oversize += 1;
                    tracing::warn!(key = %rel, bytes = cur.bytes, budget = ?self.budget.bytes_per_day, "tier: artifact exceeds the whole daily uplink budget and can never move — raise DAGRON_ARTIFACT_UPLINK_BYTES_PER_DAY");
                    continue;
                }
                if !self.budget.fits(l.day_bytes, cur.bytes) {
                    deferred = total - idx;
                    tracing::debug!(key = %rel, bytes = cur.bytes, day_bytes = l.day_bytes, budget = ?self.budget.bytes_per_day, "tier: daily uplink budget reached; deferring");
                    break;
                }
                cur
            };

            // Upload outside the ledger lock so `put` is never blocked on the link.
            let stream = match self.local.get_stream(&upload.key).await {
                Ok(s) => s,
                Err(e) if is_not_found(&e) => {
                    tracing::warn!(key = %rel, "tier: local copy vanished before uplink; forgotten");
                    let mut l = self.ledger().await?;
                    self.persist(&mut l, vec![Record::Forget { key: upload.key.clone() }]).await?;
                    continue;
                }
                Err(e) => {
                    return Err(e).with_context(|| format!("read local artifact {rel} for uplink"))
                }
            };
            // Hash what is actually sent, not what the scan saw. The sha in a
            // pending entry was taken when the file was first noticed, and a
            // budget-deferred entry can wait days — during which a task may
            // rewrite the file it is still appending to (the shared artifact dir
            // invites exactly that). Recording the stale sha would put a wrong
            // hash in the dedup index, and a later key with those *original*
            // bytes would be aliased to this object and read back as the wrong
            // content. It would also bill the metered link for the old size.
            let state = Arc::new(StdMutex::new((Sha256::new(), 0u64)));
            let tap = HashingReader { inner: stream, state: Arc::clone(&state) };
            let remote = self
                .remote
                .put_stream(&upload.key, Box::new(tap))
                .await
                .with_context(|| format!("uplink artifact {rel} to the remote tier"))?;
            let (sent_sha, sent_bytes) = {
                let mut g = state.lock().unwrap_or_else(|p| p.into_inner());
                let (h, n) = std::mem::replace(&mut *g, (Sha256::new(), 0));
                (hex::encode(h.finalize()), n)
            };
            if sent_sha != upload.sha {
                tracing::warn!(
                    key = %rel,
                    scanned_bytes = upload.bytes,
                    sent_bytes,
                    "tier: local artifact changed between scan and uplink; recording what was sent"
                );
            }

            let mut l = self.ledger().await?;
            // A `put` that raced the upload changed the bytes: leave it pending.
            let still = l
                .entries
                .get(&rel)
                .is_some_and(|e| e.state == State::Pending && e.sha == upload.sha);
            if still {
                let day_bytes = l.day_bytes.saturating_add(sent_bytes);
                self.persist(
                    &mut l,
                    vec![
                        Record::Done {
                            key: upload.key.clone(),
                            sha: sent_sha.clone(),
                            bytes: sent_bytes,
                            at: now_secs,
                            remote,
                        },
                        Record::Day { day: today, bytes: day_bytes },
                    ],
                )
                .await?;
                uploaded += 1;
                bytes_moved += upload.bytes;
            }
        }

        let day_bytes = self.ledger().await?.day_bytes;
        let moved = uploaded + deduped;
        if moved + discovered + requeued + deferred + oversize == 0 {
            tracing::debug!(day_bytes, budget = ?self.budget.bytes_per_day, "artifact tier drain: nothing to do");
        } else {
            tracing::info!(
                discovered,
                requeued,
                uploaded,
                deduped,
                bytes = bytes_moved,
                deferred,
                oversize,
                day_bytes,
                budget = ?self.budget.bytes_per_day,
                "artifact tier drain"
            );
        }
        Ok(moved)
    }
}

#[async_trait]
impl<L: ArtifactStore, R: ArtifactStore> ArtifactStore for TieredStore<L, R> {
    /// Lands locally; the remote tier sees it at the next drain. Returns the
    /// local locator (that is where the bytes are).
    async fn put(&self, key: &ArtifactKey, bytes: &[u8]) -> Result<String> {
        Self::check_key(key)?;
        let loc = self.local.put(key, bytes).await?;
        let sha = hex::encode(Sha256::digest(bytes));
        self.ledger_put(key, sha, bytes.len() as u64).await?;
        Ok(loc)
    }

    async fn get(&self, key: &ArtifactKey) -> Result<Vec<u8>> {
        let local_err = match self.local.get(key).await {
            Ok(b) => return Ok(b),
            Err(e) if is_not_found(&e) => e,
            Err(e) => return Err(e),
        };
        let Some(remote_key) = self.ledger().await?.remote_key_for(key) else {
            tracing::debug!(key = %key.rel_path(), "tier: stale dedup alias; not served from the remote tier");
            return Err(local_err);
        };
        match self.remote.get(&remote_key).await {
            Ok(b) => Ok(b),
            Err(e) => {
                log_remote_miss(key, &e);
                Err(local_err)
            }
        }
    }

    async fn exists(&self, key: &ArtifactKey) -> Result<bool> {
        if self.local.exists(key).await? {
            return Ok(true);
        }
        let Some(remote_key) = self.ledger().await?.remote_key_for(key) else {
            return Ok(false);
        };
        match self.remote.exists(&remote_key).await {
            Ok(b) => Ok(b),
            Err(e) => {
                log_remote_miss(key, &e);
                Ok(false)
            }
        }
    }

    /// The local tier's — tasks keep sharing the run directory.
    fn run_location(&self, run_id: &str) -> Option<String> {
        self.local.run_location(run_id)
    }

    /// The local tier's listing (what a rotation sweep re-keys; every key it
    /// returns is served locally).
    async fn list(&self) -> Result<Vec<ArtifactKey>> {
        self.local.list().await
    }

    async fn put_stream(
        &self,
        key: &ArtifactKey,
        reader: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Result<String> {
        Self::check_key(key)?;
        let state = Arc::new(StdMutex::new((Sha256::new(), 0u64)));
        let tap = HashingReader { inner: reader, state: Arc::clone(&state) };
        let loc = self.local.put_stream(key, Box::new(tap)).await?;
        let (hasher, bytes) = {
            let mut g = state.lock().unwrap_or_else(|p| p.into_inner());
            std::mem::replace(&mut *g, (Sha256::new(), 0))
        };
        self.ledger_put(key, hex::encode(hasher.finalize()), bytes).await?;
        Ok(loc)
    }

    async fn get_stream(
        &self,
        key: &ArtifactKey,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>> {
        let local_err = match self.local.get_stream(key).await {
            Ok(r) => return Ok(r),
            Err(e) if is_not_found(&e) => e,
            Err(e) => return Err(e),
        };
        let Some(remote_key) = self.ledger().await?.remote_key_for(key) else {
            tracing::debug!(key = %key.rel_path(), "tier: stale dedup alias; not served from the remote tier");
            return Err(local_err);
        };
        match self.remote.get_stream(&remote_key).await {
            Ok(r) => Ok(r),
            Err(e) => {
                log_remote_miss(key, &e);
                Err(local_err)
            }
        }
    }

    /// [`drain_at`](Self::drain_at) now.
    async fn sync(&self) -> Result<usize> {
        self.drain_at(SystemTime::now()).await
    }
}

/// In-memory [`ArtifactStore`] test double for the remote tier: counts puts
/// (dedup proof) and can be switched "offline" (link-down proof). Its miss
/// error is deliberately *not* an `io::Error`, so tests can tell the local
/// NotFound apart from a remote miss.
#[cfg(test)]
pub(crate) mod testing {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;

    use anyhow::{bail, Result};
    use async_trait::async_trait;

    use crate::{ArtifactKey, ArtifactStore};

    #[derive(Default)]
    pub(crate) struct MemStore {
        objects: Mutex<BTreeMap<String, (ArtifactKey, Vec<u8>)>>,
        puts: AtomicUsize,
        offline: AtomicBool,
    }

    impl MemStore {
        pub(crate) fn new() -> Self {
            Self::default()
        }
        pub(crate) fn has(&self, key: &ArtifactKey) -> bool {
            self.objects.lock().unwrap().contains_key(&key.rel_path())
        }
        pub(crate) fn raw(&self, key: &ArtifactKey) -> Option<Vec<u8>> {
            self.objects.lock().unwrap().get(&key.rel_path()).map(|(_, b)| b.clone())
        }
        pub(crate) fn put_count(&self) -> usize {
            self.puts.load(Ordering::SeqCst)
        }
        pub(crate) fn set_offline(&self, offline: bool) {
            self.offline.store(offline, Ordering::SeqCst);
        }
        fn check_link(&self) -> Result<()> {
            if self.offline.load(Ordering::SeqCst) {
                bail!("remote tier offline");
            }
            Ok(())
        }
    }

    #[async_trait]
    impl ArtifactStore for MemStore {
        async fn put(&self, key: &ArtifactKey, bytes: &[u8]) -> Result<String> {
            self.check_link()?;
            self.objects.lock().unwrap().insert(key.rel_path(), (key.clone(), bytes.to_vec()));
            self.puts.fetch_add(1, Ordering::SeqCst);
            Ok(format!("mem://{}", key.rel_path()))
        }
        async fn get(&self, key: &ArtifactKey) -> Result<Vec<u8>> {
            self.check_link()?;
            // A miss must look like a real backend's miss — `std::io::ErrorKind::
            // NotFound` for the local FS, `object_store::Error::NotFound` for a
            // bucket — so `is_not_found` can tell it apart from a link failure.
            // A bare `anyhow!` here would make the double the only "remote" in
            // existence whose absence is indistinguishable from its being down.
            self.raw(key).ok_or_else(|| {
                anyhow::Error::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("mem: no object {}", key.rel_path()),
                ))
            })
        }
        async fn exists(&self, key: &ArtifactKey) -> Result<bool> {
            self.check_link()?;
            Ok(self.has(key))
        }
        async fn list(&self) -> Result<Vec<ArtifactKey>> {
            self.check_link()?;
            Ok(self.objects.lock().unwrap().values().map(|(k, _)| k.clone()).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::testing::MemStore;
    use super::*;
    use crate::{EncryptedStore, LocalFsStore};

    fn tmp(tag: &str) -> PathBuf {
        static N: AtomicUsize = AtomicUsize::new(0);
        std::env::temp_dir().join(format!(
            "dagron-tier-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ))
    }

    fn key(r: &str, t: &str, n: &str) -> ArtifactKey {
        ArtifactKey::new(r, t, n)
    }

    fn at_day(day: u64, plus_secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(day * SECS_PER_DAY + plus_secs)
    }

    fn store(
        base: &Path,
        remote: &Arc<MemStore>,
        budget: UplinkBudget,
    ) -> TieredStore<LocalFsStore, Arc<MemStore>> {
        TieredStore::new(LocalFsStore::new(base), Arc::clone(remote), base, budget)
    }

    fn local_path(base: &Path, k: &ArtifactKey) -> PathBuf {
        base.join(k.rel_path())
    }

    async fn read_all(mut r: Box<dyn AsyncRead + Send + Unpin>) -> Vec<u8> {
        let mut out = Vec::new();
        r.read_to_end(&mut out).await.unwrap();
        out
    }

    fn reader(bytes: Vec<u8>) -> Box<dyn AsyncRead + Send + Unpin> {
        Box::new(std::io::Cursor::new(bytes))
    }

    fn is_io_not_found(e: &anyhow::Error) -> bool {
        e.downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound)
    }

    fn ledger_lines(base: &Path) -> Vec<String> {
        std::fs::read_to_string(base.join(LEDGER_DIR).join(LEDGER_FILE))
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }

    // 1. put lands locally, not remotely, until sync.
    #[tokio::test]
    async fn put_lands_locally_not_remote_until_sync() {
        let base = tmp("put");
        let remote = Arc::new(MemStore::new());
        let t = store(&base, &remote, UplinkBudget::UNLIMITED);
        let k = key("run1", "capture", "window.bin");

        let loc = t.put(&k, b"hello").await.unwrap();
        assert!(loc.replace('\\', "/").ends_with("run1/capture/window.bin"), "local locator: {loc}");
        assert!(local_path(&base, &k).is_file(), "bytes are on the local tier");
        assert!(!remote.has(&k), "nothing moved before sync");
        assert_eq!(remote.put_count(), 0);
        assert_eq!(t.pending().await.unwrap(), 1);
        assert!(t.ledger_path().is_file(), "ledger written at put time");
        assert_eq!(t.get(&k).await.unwrap(), b"hello", "served locally");
        assert!(t.exists(&k).await.unwrap());

        // The ledger dir is invisible to the listing.
        let listed: Vec<String> = t.list().await.unwrap().iter().map(|k| k.rel_path()).collect();
        assert_eq!(listed, vec!["run1/capture/window.bin"]);

        let _ = std::fs::remove_dir_all(&base);
    }

    // 2. sync uploads; the remote tier serves reads once the local copy is gone.
    #[tokio::test]
    async fn sync_uploads_and_remote_serves_fallback() {
        let base = tmp("sync");
        let remote = Arc::new(MemStore::new());
        let t = store(&base, &remote, UplinkBudget::UNLIMITED);
        let k = key("run1", "capture", "window.bin");
        t.put(&k, b"hello").await.unwrap();

        assert_eq!(t.sync().await.unwrap(), 1);
        assert_eq!(remote.raw(&k).unwrap(), b"hello");
        assert_eq!(t.pending().await.unwrap(), 0);
        assert_eq!(t.sync().await.unwrap(), 0, "nothing left to move");

        std::fs::remove_file(local_path(&base, &k)).unwrap();
        assert_eq!(t.get(&k).await.unwrap(), b"hello", "remote fallback");
        assert_eq!(read_all(t.get_stream(&k).await.unwrap()).await, b"hello");
        assert!(t.exists(&k).await.unwrap(), "exists consults the remote tier");
        assert_eq!(t.sync().await.unwrap(), 0, "a done entry is never re-queued by the scan");

        let _ = std::fs::remove_dir_all(&base);
    }

    // 3. the per-day budget defers the rest to a later day; oversize never blocks.
    #[tokio::test]
    async fn per_day_budget_defers_until_a_later_day() {
        let base = tmp("budget");
        let remote = Arc::new(MemStore::new());
        let t = store(&base, &remote, UplinkBudget::per_day(100));
        let (a, b, c) = (key("r", "t", "a"), key("r", "t", "b"), key("r", "t", "c"));
        // Distinct bytes per key: identical content would be aliased by the
        // dedup path (test 5) and never spend budget, which is not the subject
        // here.
        for (k, fill) in [(&a, 7u8), (&b, 8u8), (&c, 9u8)] {
            t.put(k, &[fill; 60]).await.unwrap();
        }

        assert_eq!(t.drain_at(at_day(10, 0)).await.unwrap(), 1, "60 of 100 today");
        assert!(remote.has(&a) && !remote.has(&b) && !remote.has(&c), "oldest first");
        assert_eq!(t.drain_at(at_day(10, 3_600)).await.unwrap(), 0, "same day: over budget");
        assert_eq!(t.drain_at(at_day(10, SECS_PER_DAY - 1)).await.unwrap(), 0, "still the same UTC day");
        assert_eq!(t.drain_at(at_day(11, 0)).await.unwrap(), 1, "new day, new budget");
        assert!(remote.has(&b) && !remote.has(&c));
        assert_eq!(t.drain_at(at_day(12, 0)).await.unwrap(), 1);
        assert!(remote.has(&c));
        assert_eq!(t.pending().await.unwrap(), 0);

        // An artifact larger than the whole budget is skipped (warned), not a
        // head-of-line block: a smaller one queued behind it still moves.
        let big = key("r", "t", "big");
        let small = key("r", "t", "small");
        t.put(&big, &[1u8; 150]).await.unwrap();
        t.put(&small, &[2u8; 10]).await.unwrap();
        assert_eq!(t.drain_at(at_day(13, 0)).await.unwrap(), 1);
        assert!(remote.has(&small) && !remote.has(&big));
        assert_eq!(t.pending().await.unwrap(), 1, "the oversize one stays pending");

        let _ = std::fs::remove_dir_all(&base);
    }

    // 4. the ledger (entries + day counter) survives re-creating the store.
    #[tokio::test]
    async fn ledger_survives_recreate() {
        let base = tmp("recreate");
        let remote = Arc::new(MemStore::new());
        let (a, b, c) = (key("r", "t", "a"), key("r", "t", "b"), key("r", "t", "c"));
        {
            let t = store(&base, &remote, UplinkBudget::per_day(100));
            t.put(&a, b"first").await.unwrap();
            assert_eq!(t.drain_at(at_day(20, 0)).await.unwrap(), 1);
            t.put(&b, &[9u8; 60]).await.unwrap();
            assert_eq!(t.drain_at(at_day(21, 0)).await.unwrap(), 1, "60 bytes on day 21");
        }
        let t = store(&base, &remote, UplinkBudget::per_day(100));
        assert_eq!(t.pending().await.unwrap(), 0, "done entries replayed");
        assert_eq!(t.drain_at(at_day(21, 10)).await.unwrap(), 0, "nothing new; nothing re-uploaded");
        assert_eq!(remote.put_count(), 2);
        // Different bytes from `b`, or dedup would alias it home for free.
        t.put(&c, &[8u8; 60]).await.unwrap();
        assert_eq!(t.drain_at(at_day(21, 20)).await.unwrap(), 0, "day counter survived: 60+60 > 100");
        assert_eq!(t.drain_at(at_day(22, 0)).await.unwrap(), 1);
        std::fs::remove_file(local_path(&base, &a)).unwrap();
        assert_eq!(t.get(&a).await.unwrap(), b"first", "replayed done entry serves the remote copy");

        let _ = std::fs::remove_dir_all(&base);
    }

    // 5. identical bytes under two keys transfer once; the alias reads back.
    #[tokio::test]
    async fn dedup_records_alias_and_skips_transfer() {
        let base = tmp("dedup");
        let remote = Arc::new(MemStore::new());
        let t = store(&base, &remote, UplinkBudget::per_day(50));
        let (a, b, c) = (key("r1", "t", "a"), key("r2", "t", "b"), key("r1", "t", "c"));
        t.put(&a, b"same-bytes").await.unwrap();
        t.put(&b, b"same-bytes").await.unwrap();
        t.put(&c, b"other").await.unwrap();

        assert_eq!(t.drain_at(at_day(30, 0)).await.unwrap(), 3, "two uploads + one alias");
        assert_eq!(remote.put_count(), 2, "dedup skipped the transfer");
        assert!(remote.has(&a) && remote.has(&c) && !remote.has(&b));
        assert_eq!(t.pending().await.unwrap(), 0);
        let lines = ledger_lines(&base).join("\n");
        assert!(lines.contains("\"kind\":\"uploaded_by_sha\""), "alias record: {lines}");

        std::fs::remove_file(local_path(&base, &b)).unwrap();
        assert_eq!(t.get(&b).await.unwrap(), b"same-bytes", "alias resolves to the canonical");
        assert_eq!(read_all(t.get_stream(&b).await.unwrap()).await, b"same-bytes");
        assert!(t.exists(&b).await.unwrap());

        let _ = std::fs::remove_dir_all(&base);
    }

    // 6. run_location / list are the local tier's.
    #[tokio::test]
    async fn run_location_and_list_are_local() {
        let base = tmp("local");
        let remote = Arc::new(MemStore::new());
        let t = store(&base, &remote, UplinkBudget::UNLIMITED);
        assert_eq!(t.run_location("run-7"), LocalFsStore::new(&base).run_location("run-7"));
        t.put(&key("r", "t", "a"), b"x").await.unwrap();
        t.put(&key("r", "u", "b"), b"y").await.unwrap();
        assert_eq!(t.sync().await.unwrap(), 2);
        let mut got: Vec<String> = t.list().await.unwrap().iter().map(|k| k.rel_path()).collect();
        got.sort();
        assert_eq!(got, vec!["r/t/a", "r/u/b"], "no .tiered entry, even after a drain");
        let _ = std::fs::remove_dir_all(&base);
    }

    // 7. EncryptedStore<TieredStore<…>>: ciphertext on both tiers, sync forwarded,
    //    reads decrypt from either tier, dedup effectively off.
    #[tokio::test]
    async fn encrypted_composition() {
        let base = tmp("enc");
        let remote = Arc::new(MemStore::new());
        let enc = EncryptedStore::new(
            store(&base, &remote, UplinkBudget::UNLIMITED),
            Box::new(dagron_crypto::LocalKekProvider::new([5u8; 32])),
        );
        let k = key("run", "train", "ckpt.bin");
        let plaintext = b"MODEL-WEIGHTS-do-not-leak".to_vec();
        enc.put(&k, &plaintext).await.unwrap();

        let on_disk = std::fs::read(local_path(&base, &k)).unwrap();
        assert_ne!(on_disk, plaintext);
        assert_eq!(on_disk[0], 0x02, "envelope on the local tier");
        assert!(!remote.has(&k));

        assert_eq!(enc.sync().await.unwrap(), 1, "sync forwarded through the decorator");
        assert_eq!(remote.raw(&k).unwrap(), on_disk, "the remote tier holds the same ciphertext");
        assert!(
            !remote.raw(&k).unwrap().windows(plaintext.len()).any(|w| w == plaintext.as_slice()),
            "no plaintext on the remote tier"
        );
        assert_eq!(enc.get(&k).await.unwrap(), plaintext);
        std::fs::remove_file(local_path(&base, &k)).unwrap();
        assert_eq!(enc.get(&k).await.unwrap(), plaintext, "remote fallback decrypts");
        assert_eq!(read_all(enc.get_stream(&k).await.unwrap()).await, plaintext);
        assert!(enc.run_location("run").is_none(), "encryption still hides the plaintext dir");

        // Fresh data keys ⇒ identical plaintexts are different ciphertexts ⇒ no dedup.
        let before = remote.put_count();
        enc.put(&key("run", "train", "dup1"), b"same").await.unwrap();
        enc.put(&key("run", "train", "dup2"), b"same").await.unwrap();
        assert_eq!(enc.sync().await.unwrap(), 2);
        assert_eq!(remote.put_count(), before + 2, "dedup is off under encryption");

        let _ = std::fs::remove_dir_all(&base);
    }

    // Torn last line (crash mid-append) and a malformed middle line are
    // tolerated on replay, and the next write compacts them away.
    #[tokio::test]
    async fn torn_ledger_replay() {
        let base = tmp("torn");
        let remote = Arc::new(MemStore::new());
        let a = key("r", "t", "a");
        {
            let t = store(&base, &remote, UplinkBudget::UNLIMITED);
            t.put(&a, b"alpha").await.unwrap();
            assert_eq!(t.sync().await.unwrap(), 1);
        }
        let path = base.join(LEDGER_DIR).join(LEDGER_FILE);
        let mut text = std::fs::read_to_string(&path).unwrap();
        // A malformed line in the middle + a torn tail with no newline.
        text.insert_str(0, "this is not json\n");
        text.push_str(r#"{"kind":"pending","key":{"run_id":"r","ta"#);
        std::fs::write(&path, &text).unwrap();

        let t = store(&base, &remote, UplinkBudget::UNLIMITED);
        assert_eq!(t.pending().await.unwrap(), 0, "the done record was honoured");
        std::fs::remove_file(local_path(&base, &a)).unwrap();
        assert_eq!(t.get(&a).await.unwrap(), b"alpha", "replayed state serves the remote copy");

        // The first write after damage compacts: every line parses again.
        t.put(&key("r", "t", "b"), b"beta").await.unwrap();
        let lines = ledger_lines(&base);
        assert!(lines.iter().all(|l| serde_json::from_str::<Record>(l).is_ok()), "compacted: {lines:?}");
        assert!(std::fs::read_to_string(&path).unwrap().ends_with('\n'));
        assert_eq!(t.sync().await.unwrap(), 1);

        let t2 = store(&base, &remote, UplinkBudget::UNLIMITED);
        assert_eq!(t2.pending().await.unwrap(), 0);
        assert_eq!(t2.sync().await.unwrap(), 0);

        let _ = std::fs::remove_dir_all(&base);
    }

    // The stale-alias guard: once the canonical changes, an alias must not be
    // served from the remote tier — and self-heals while a local copy exists.
    #[tokio::test]
    async fn stale_alias_guard() {
        let base = tmp("stale");
        let remote = Arc::new(MemStore::new());
        let t = store(&base, &remote, UplinkBudget::UNLIMITED);
        let (a, b) = (key("r", "t", "a"), key("r", "t", "b"));
        t.put(&a, b"same").await.unwrap();
        t.put(&b, b"same").await.unwrap();
        assert_eq!(t.sync().await.unwrap(), 2);
        assert!(remote.has(&a) && !remote.has(&b), "b is an alias of a");

        // Canonical re-put (not yet synced), alias has no local copy any more:
        // the remote still holds the old bytes under `a`, but b must not be
        // served from it — a's next upload would silently change what b reads.
        t.put(&a, b"changed").await.unwrap();
        std::fs::remove_file(local_path(&base, &b)).unwrap();
        let err = t.get(&b).await.unwrap_err();
        assert!(is_io_not_found(&err), "stale alias is a local NotFound, got: {err:#}");
        assert!(!t.exists(&b).await.unwrap());

        // The drain forgets the alias (no copy anywhere) and re-uploads a.
        assert_eq!(t.sync().await.unwrap(), 1);
        assert_eq!(remote.raw(&a).unwrap(), b"changed");
        assert!(!remote.has(&b));
        let err = t.get(&b).await.unwrap_err();
        assert!(is_io_not_found(&err), "forgotten alias: {err:#}");

        // Self-heal: an alias whose local copy still exists is re-queued under
        // its own key when its canonical changes.
        let (c, d) = (key("r", "t", "c"), key("r", "t", "d"));
        t.put(&c, b"twin").await.unwrap();
        t.put(&d, b"twin").await.unwrap();
        assert_eq!(t.sync().await.unwrap(), 2);
        assert!(!remote.has(&d));
        t.put(&c, b"twin-v2").await.unwrap();
        assert_eq!(t.sync().await.unwrap(), 2, "c re-uploaded + d requeued and uploaded");
        assert_eq!(remote.raw(&c).unwrap(), b"twin-v2");
        assert_eq!(remote.raw(&d).unwrap(), b"twin");
        std::fs::remove_file(local_path(&base, &d)).unwrap();
        assert_eq!(t.get(&d).await.unwrap(), b"twin");

        let _ = std::fs::remove_dir_all(&base);
    }

    // Both tiers miss ⇒ the LOCAL io::Error NotFound comes back unchanged (the
    // management API maps exactly that to 404), also when the remote is down.
    #[tokio::test]
    async fn both_miss_preserves_local_not_found() {
        let base = tmp("miss");
        let remote = Arc::new(MemStore::new());
        let t = store(&base, &remote, UplinkBudget::UNLIMITED);
        let k = key("nope", "t", "n");

        let err = t.get(&k).await.unwrap_err();
        assert!(is_io_not_found(&err), "got: {err:#}");
        let err = match t.get_stream(&k).await {
            Ok(_) => panic!("get_stream of a missing key must fail"),
            Err(e) => e,
        };
        assert!(is_io_not_found(&err), "got: {err:#}");
        assert!(!t.exists(&k).await.unwrap());

        remote.set_offline(true);
        let err = t.get(&k).await.unwrap_err();
        assert!(is_io_not_found(&err), "remote fault is a miss, not a 500: {err:#}");
        assert!(!t.exists(&k).await.unwrap(), "exists never errors on a remote fault");

        let _ = std::fs::remove_dir_all(&base);
    }

    // put_stream hashes in the same pass; dedup works across put/put_stream.
    #[tokio::test]
    async fn put_stream_hashes_and_dedups() {
        let base = tmp("stream");
        let remote = Arc::new(MemStore::new());
        let t = store(&base, &remote, UplinkBudget::UNLIMITED);
        let data: Vec<u8> = (0..200_000).map(|i| (i % 251) as u8).collect();
        let (a, b) = (key("r", "t", "streamed"), key("r", "t", "whole"));
        t.put_stream(&a, reader(data.clone())).await.unwrap();
        t.put(&b, &data).await.unwrap();

        assert_eq!(t.sync().await.unwrap(), 2);
        assert_eq!(remote.put_count(), 1, "same hash from both write paths");
        assert_eq!(remote.raw(&a).unwrap(), data);
        std::fs::remove_file(local_path(&base, &b)).unwrap();
        assert_eq!(read_all(t.get_stream(&b).await.unwrap()).await, data);

        let _ = std::fs::remove_dir_all(&base);
    }

    // Files written straight into the run dir (tasks, DAGRON_ARTIFACTS) are
    // discovered by the scan; the depth rule for `.checkpoints` is as documented.
    #[tokio::test]
    async fn scan_picks_up_files_written_directly() {
        let base = tmp("scan");
        let remote = Arc::new(MemStore::new());
        let t = store(&base, &remote, UplinkBudget::UNLIMITED);
        let direct = key("r", "t", "direct.bin");
        let ck_pointer = key("r", ".checkpoints", "latest");
        std::fs::create_dir_all(base.join("r/t")).unwrap();
        std::fs::write(local_path(&base, &direct), b"task-written").unwrap();
        std::fs::create_dir_all(base.join("r/.checkpoints/t")).unwrap();
        std::fs::write(base.join("r/.checkpoints/latest"), b"pointer").unwrap();
        std::fs::write(base.join("r/.checkpoints/t/ckpt.bin"), b"deep").unwrap();

        assert_eq!(t.sync().await.unwrap(), 2, "depth-3 files only");
        assert_eq!(remote.raw(&direct).unwrap(), b"task-written");
        assert_eq!(remote.raw(&ck_pointer).unwrap(), b"pointer");
        assert!(!remote.has(&key("r", ".checkpoints", "ckpt.bin")));
        assert!(!remote.has(&key("r", "t", "ckpt.bin")));
        assert_eq!(t.sync().await.unwrap(), 0, "ledgered now; not re-hashed");

        let _ = std::fs::remove_dir_all(&base);
    }

    // Sealed by an outer EncryptedStore, the scan must not adopt task-written
    // plaintext: this tier has no key, so uplinking it would put cleartext in
    // the remote tier of a deployment configured for ciphertext at rest.
    #[tokio::test]
    async fn a_sealed_tier_keeps_task_written_plaintext_local() {
        let base = tmp("scan-sealed");
        let remote = Arc::new(MemStore::new());
        let t = store(&base, &remote, UplinkBudget::UNLIMITED).sealed_by_outer_encryption(true);
        let direct = key("r", "t", "direct.bin");
        std::fs::create_dir_all(base.join("r/t")).unwrap();
        std::fs::write(local_path(&base, &direct), b"task-written").unwrap();

        assert_eq!(t.sync().await.unwrap(), 0, "nothing adopted from the scan");
        assert!(!remote.has(&direct), "plaintext must not reach the remote tier");
        // The local copy is untouched — sealing withholds the uplink, not the file.
        assert_eq!(std::fs::read(local_path(&base, &direct)).unwrap(), b"task-written");

        // A put still moves: it comes through the store, so an outer decorator
        // has already sealed it by the time the tier sees the bytes.
        let via_put = key("r", "t", "via-put.bin");
        t.put(&via_put, b"ciphertext-by-then").await.unwrap();
        assert_eq!(t.sync().await.unwrap(), 1);
        assert_eq!(remote.raw(&via_put).unwrap(), b"ciphertext-by-then");

        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn reserved_run_id_is_refused() {
        let base = tmp("reserved");
        let remote = Arc::new(MemStore::new());
        let t = store(&base, &remote, UplinkBudget::UNLIMITED);
        let err = t.put(&key(".tiered", "x", "y"), b"z").await.unwrap_err();
        assert!(err.to_string().contains("reserved"), "{err}");
        let err = t.put_stream(&key(".tiered", "x", "y"), reader(b"z".to_vec())).await.unwrap_err();
        assert!(err.to_string().contains("reserved"), "{err}");
        assert!(!base.join(".tiered/x/y").exists());
        let _ = std::fs::remove_dir_all(&base);
    }

    // A remote fault aborts the drain with an error; what moved stays moved and
    // the next drain resumes.
    #[tokio::test]
    async fn upload_failure_is_an_error_and_resumes() {
        let base = tmp("resume");
        let remote = Arc::new(MemStore::new());
        let t = store(&base, &remote, UplinkBudget::UNLIMITED);
        let (a, b) = (key("r", "t", "a"), key("r", "t", "b"));
        t.put(&a, b"one").await.unwrap();
        t.put(&b, b"two").await.unwrap();

        remote.set_offline(true);
        let err = t.sync().await.unwrap_err();
        assert!(err.to_string().contains("uplink"), "{err:#}");
        assert_eq!(t.pending().await.unwrap(), 2);

        remote.set_offline(false);
        assert_eq!(t.sync().await.unwrap(), 2);
        assert_eq!(t.pending().await.unwrap(), 0);
        assert_eq!(remote.put_count(), 2, "no double upload");

        let _ = std::fs::remove_dir_all(&base);
    }

    // Re-putting the same key (rotation does this) re-queues it; the same
    // bytes again do not. Compaction bounds the file.
    #[tokio::test]
    async fn re_put_requeues_and_compaction_bounds_the_ledger() {
        let base = tmp("compact");
        let remote = Arc::new(MemStore::new());
        let t = store(&base, &remote, UplinkBudget::UNLIMITED);
        let k = key("r", "t", "k");
        t.put(&k, b"v1").await.unwrap();
        assert_eq!(t.sync().await.unwrap(), 1);
        t.put(&k, b"v1").await.unwrap();
        assert_eq!(t.pending().await.unwrap(), 0, "same bytes: still done");
        t.put(&k, b"v2").await.unwrap();
        assert_eq!(t.pending().await.unwrap(), 1, "new bytes: pending again");
        assert_eq!(t.sync().await.unwrap(), 1);
        assert_eq!(remote.raw(&k).unwrap(), b"v2");

        for i in 0..400u32 {
            t.put(&k, &i.to_le_bytes()).await.unwrap();
        }
        let lines = ledger_lines(&base);
        assert!(lines.len() < 300, "compacted tail, got {} lines", lines.len());
        assert_eq!(t.pending().await.unwrap(), 1);

        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn budget_parse() {
        assert_eq!(UplinkBudget::parse(None).unwrap(), UplinkBudget::UNLIMITED);
        assert_eq!(UplinkBudget::parse(Some("")).unwrap(), UplinkBudget::UNLIMITED);
        assert_eq!(UplinkBudget::parse(Some("0")).unwrap(), UplinkBudget::UNLIMITED);
        assert_eq!(UplinkBudget::parse(Some(" 1024 ")).unwrap().bytes_per_day, Some(1024));
        let err = UplinkBudget::parse(Some("lots")).unwrap_err().to_string();
        assert!(err.contains("DAGRON_ARTIFACT_UPLINK_BYTES_PER_DAY"), "{err}");
        assert!(UplinkBudget::per_day(10).fits(0, 10));
        assert!(!UplinkBudget::per_day(10).fits(1, 10));
        assert!(UplinkBudget::per_day(10).oversize(11));
        assert!(!UplinkBudget::UNLIMITED.oversize(u64::MAX));
        assert!(UplinkBudget::UNLIMITED.fits(u64::MAX, u64::MAX));
    }

    /// The real remote backend behind the same trait: an in-memory object store
    /// through `CloudStore`, so the cloud listing and locators are exercised.
    #[cfg(feature = "cloud")]
    #[tokio::test]
    async fn cloud_in_memory_remote() {
        use crate::cloud::CloudStore;
        let base = tmp("cloud");
        let backend = Arc::new(object_store::memory::InMemory::new());
        let remote = || CloudStore::with_store(backend.clone(), "s3://unit-uplink/edge", "edge");
        let t = TieredStore::new(LocalFsStore::new(&base), remote(), &base, UplinkBudget::UNLIMITED);
        let k = key("run", "capture", "window.mp4");
        t.put(&k, b"frames").await.unwrap();
        assert!(!remote().exists(&k).await.unwrap());

        assert_eq!(t.sync().await.unwrap(), 1);
        assert_eq!(remote().get(&k).await.unwrap(), b"frames");
        assert_eq!(
            remote().list().await.unwrap().iter().map(|k| k.rel_path()).collect::<Vec<_>>(),
            vec!["run/capture/window.mp4"]
        );
        let lines = ledger_lines(&base).join("\n");
        assert!(lines.contains("s3://unit-uplink/edge/run/capture/window.mp4"), "remote locator ledgered: {lines}");

        std::fs::remove_file(local_path(&base, &k)).unwrap();
        assert_eq!(t.get(&k).await.unwrap(), b"frames");
        assert_eq!(read_all(t.get_stream(&k).await.unwrap()).await, b"frames");
        let err = t.get(&key("run", "capture", "missing")).await.unwrap_err();
        assert!(is_io_not_found(&err), "cloud miss keeps the local NotFound: {err:#}");

        let _ = std::fs::remove_dir_all(&base);
    }
}
