//! Where a task's log is kept once it is out of the scheduling database
//! (docs/LOG_PIPELINE.md §3, §6).
//!
//! A task attempt's log is a run of **segments**: each closed one a zstd
//! object of about [`DEFAULT_SEGMENT_BYTES`] of text, named by the attempt
//! (`<task>/<fence>[.<gen>]/<seq>.seg.zst`) and recorded by a row of the
//! database's `log_segments`, and the one still being written, the **open
//! part**:
//!
//! - **`file://DIR`**: the closed segments are files under `DIR`, and so is
//!   the open part (`<seq>.open`, appended to as the task writes). The
//!   engine and dagron-api share `DIR` (a volume); nothing of the log goes
//!   through the database but a row for each closed segment.
//! - **`s3://` / `gs://` / `az://`**: the closed segments are objects; the open
//!   part is the task row's `log` column, as a whole log was before, never
//!   larger than a segment: a closed segment's text leaves it in the
//!   transaction that records the segment, so a reader sees each line once.
//!
//! An attempt's log is written again from its start where it is sealed (its
//! whole text once more, masked): each such writing is a **generation** of it
//! (the task row's `log_gen`), and its segments and open part are named by it
//! as well as by the attempt, so a reader of one generation never reads
//! another's bytes under the names it counted. A log begun before generations
//! were counted is generation 0, under the names it was written with.
//!
//! This module is the bytes: what holds them, and their compression. The
//! rows are dagron-core's (`db::log_segments_of` and the rest), and the
//! engine's log batch decides when a segment closes. A reader that follows a
//! log by its bytes (dagron-api's live relay) reads from a position:
//! [`SegmentStore::read_segment_from`], [`SegmentStore::read_open_from`].
//!
//! Everything kept of a task, every attempt and every writing, is under one
//! name of its own (`<task>/`), so the engine's GC removes what no row names
//! by that name ([`SegmentStore::remove_log`]): a task's writings below the
//! one its row is on, or the whole of a task whose row is gone.

use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};

/// A segment closes at about this much text (8 MiB, LOG_PIPELINE.md §3).
pub const DEFAULT_SEGMENT_BYTES: u64 = 8 * 1024 * 1024;
/// Where logs are kept: `file:///DIR`, or an object store's URL. Unset, a
/// log stays in the task row, as before.
pub const ENV_STORE: &str = "DAGRON_LOG_STORE";
/// How much text a segment holds before it closes.
pub const ENV_SEGMENT_BYTES: &str = "DAGRON_LOG_SEGMENT_BYTES";

/// A task attempt's log, by its task row's id, the attempt's fence, and which
/// writing of it this is (its generation: the task row's `log_gen`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogKey {
    pub task_id: String,
    pub fence: i64,
    pub gen: i64,
}

impl LogKey {
    pub fn new(task_id: impl Into<String>, fence: i64, gen: i64) -> Self {
        Self {
            task_id: task_id.into(),
            fence,
            gen,
        }
    }

    /// The closed segment `seq`'s name, relative to the store.
    pub fn segment(&self, seq: i64) -> String {
        format!("{}/{seq:06}.seg.zst", self.attempt())
    }

    /// The open part's name, relative to the store (`file://` alone).
    fn open(&self, seq: i64) -> String {
        format!("{}/{seq:06}.open", self.attempt())
    }

    /// Where this writing of the attempt's log is kept: `<task>/<fence>` for
    /// generation 0 (a log begun before generations were counted, under the
    /// names it has), `<task>/<fence>.<gen>` for any later one.
    fn attempt(&self) -> String {
        match self.gen {
            0 => format!("{}/{}", safe(&self.task_id), self.fence),
            gen => format!("{}/{}.{gen}", safe(&self.task_id), self.fence),
        }
    }
}

/// A task id as a path component: what is not a letter, digit, `-` or `_`
/// is `_` (a task row's id is a UUID).
fn safe(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Which writing of which attempt of a task's log a name under the task is:
/// its fence, and its generation. Ordered as they are begun: by attempt, and
/// within one by writing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Writing {
    pub fence: i64,
    pub gen: i64,
}

/// The writing a name under a task is ([`LogKey`]'s `<fence>` for
/// generation 0, `<fence>.<gen>` for a later one); `None` for any other
/// name, which is no writing's and is kept while the task has a row.
pub fn parse_writing(name: &str) -> Option<Writing> {
    let number = |part: &str| -> Option<i64> {
        if part.is_empty() || part.len() > 18 || !part.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        part.parse().ok()
    };
    match name.split_once('.') {
        None => Some(Writing {
            fence: number(name)?,
            gen: 0,
        }),
        Some((fence, gen)) => Some(Writing {
            fence: number(fence)?,
            gen: number(gen)?,
        }),
    }
}

/// What a removal took from a store: files and objects (a file:// store's
/// directories among them), and their bytes as stored.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Removed {
    pub entries: u64,
    pub bytes: u64,
}

/// What holds the segments ([`ENV_STORE`]).
#[derive(Clone)]
pub enum SegmentStore {
    /// Segments and the open part under a directory.
    Dir(PathBuf),
    /// Segments under a directory and the open part in the task row: what an
    /// object store does, with a directory for its objects. For tests.
    DirOpenInRow(PathBuf),
    /// Segments in an object store, the open part in the task row.
    #[cfg(feature = "cloud")]
    Cloud(std::sync::Arc<crate::cloud::CloudStore>),
}

impl std::fmt::Debug for SegmentStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SegmentStore::Dir(dir) => write!(f, "file://{}", dir.display()),
            SegmentStore::DirOpenInRow(dir) => {
                write!(f, "file://{} (open part in the row)", dir.display())
            }
            #[cfg(feature = "cloud")]
            SegmentStore::Cloud(_) => f.write_str("object store"),
        }
    }
}

/// The store [`ENV_STORE`] names and the size a segment closes at
/// ([`ENV_SEGMENT_BYTES`]), or `None` where it names none. A setting that
/// cannot be used stops the process: a log kept nowhere it can be read
/// again is no log.
pub fn from_env() -> Result<Option<(SegmentStore, u64)>> {
    let var = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
    let Some(url) = var(ENV_STORE) else {
        return Ok(None);
    };
    let bytes = match var(ENV_SEGMENT_BYTES) {
        None => DEFAULT_SEGMENT_BYTES,
        Some(v) => parse_segment_bytes(&v)?,
    };
    Ok(Some((SegmentStore::from_url(url.trim())?, bytes)))
}

/// [`ENV_SEGMENT_BYTES`]: 4 KiB to 1 GiB.
pub fn parse_segment_bytes(v: &str) -> Result<u64> {
    let n: u64 = v
        .trim()
        .parse()
        .map_err(|_| anyhow!("{ENV_SEGMENT_BYTES}={v:?} is not a number of bytes"))?;
    if !(4096..=1 << 30).contains(&n) {
        bail!("{ENV_SEGMENT_BYTES}={n} is outside 4096 to 1073741824");
    }
    Ok(n)
}

impl SegmentStore {
    /// `file:///DIR`, or an object store's `s3://`, `gs://` or `az://` URL
    /// (with this build's cloud features).
    pub fn from_url(url: &str) -> Result<SegmentStore> {
        if let Some(path) = url.strip_prefix("file://") {
            if path.is_empty() || !Path::new(path).is_absolute() {
                bail!("{ENV_STORE}={url:?}: a file:// store names an absolute directory");
            }
            return Ok(SegmentStore::Dir(PathBuf::from(path)));
        }
        #[cfg(feature = "cloud")]
        {
            let store = crate::cloud::CloudStore::from_url(url)
                .with_context(|| format!("{ENV_STORE}={url:?}"))?;
            Ok(SegmentStore::Cloud(std::sync::Arc::new(store)))
        }
        #[cfg(not(feature = "cloud"))]
        bail!(
            "{ENV_STORE}={url:?}: this build keeps logs in a file:// directory alone (it has no \
             object store support)"
        )
    }

    /// Whether the open part is the task row's `log` (an object store), not
    /// a file of this store's.
    pub fn open_in_row(&self) -> bool {
        !matches!(self, SegmentStore::Dir(_))
    }

    fn dir(&self) -> Option<&Path> {
        match self {
            SegmentStore::Dir(dir) | SegmentStore::DirOpenInRow(dir) => Some(dir),
            #[cfg(feature = "cloud")]
            SegmentStore::Cloud(_) => None,
        }
    }

    /// Keep closed segment `seq` of `key`, compressed: its object's name.
    pub async fn put_segment(&self, key: &LogKey, seq: i64, text: &str) -> Result<String> {
        let name = key.segment(seq);
        let bytes =
            zstd::stream::encode_all(text.as_bytes(), 3).context("compressing a log segment")?;
        match self {
            #[cfg(feature = "cloud")]
            SegmentStore::Cloud(store) => {
                use crate::ArtifactStore;
                store.put(&object_key(&name), &bytes).await?;
            }
            _ => {
                let path = self.dir().expect("a directory").join(&name);
                if let Some(parent) = path.parent() {
                    tokio::fs::create_dir_all(parent)
                        .await
                        .with_context(|| format!("making {}", parent.display()))?;
                }
                // Written whole and then renamed: a reader finds the segment
                // whole, or not at all.
                let partial = path.with_extension("zst.partial");
                tokio::fs::write(&partial, &bytes)
                    .await
                    .with_context(|| format!("writing {}", partial.display()))?;
                tokio::fs::rename(&partial, &path)
                    .await
                    .with_context(|| format!("keeping {}", path.display()))?;
            }
        }
        Ok(name)
    }

    /// A closed segment's text, by its object's name.
    pub async fn get_segment(&self, name: &str) -> Result<String> {
        let bytes = match self {
            #[cfg(feature = "cloud")]
            SegmentStore::Cloud(store) => {
                use crate::ArtifactStore;
                store.get(&object_key(name)).await?
            }
            _ => {
                let path = self.dir().expect("a directory").join(name);
                tokio::fs::read(&path)
                    .await
                    .with_context(|| format!("reading {}", path.display()))?
            }
        };
        let text =
            zstd::stream::decode_all(bytes.as_slice()).context("a log segment that is not zstd")?;
        Ok(String::from_utf8_lossy(&text).into_owned())
    }

    /// Remove what is kept of a task that no reader can name: with its row's
    /// writing `current`, every writing of the task below it (closed
    /// segments, open parts, what a write left half done, and their
    /// directories); with none (the row is gone), everything under the task.
    /// A name under the task that is no writing's is kept while it has a row.
    /// What is gone already is no error, so a removal can be made again.
    pub async fn remove_log(&self, task_id: &str, current: Option<Writing>) -> Result<Removed> {
        // A task's name is its id: an empty one would name the whole store.
        if safe(task_id).is_empty() {
            anyhow::bail!("no task's log is named by an empty id");
        }
        let doomed = |name: &str| match current {
            None => true,
            Some(current) => parse_writing(name).is_some_and(|w| w < current),
        };
        match self {
            #[cfg(feature = "cloud")]
            SegmentStore::Cloud(store) => {
                let (entries, bytes) = store
                    .remove_under(&format!("logs-{}", safe(task_id)), doomed)
                    .await?;
                Ok(Removed { entries, bytes })
            }
            _ => {
                let task = self.dir().expect("a directory").join(safe(task_id));
                if current.is_none() {
                    return remove_tree(&task).await;
                }
                let mut removed = Removed::default();
                let mut names = match tokio::fs::read_dir(&task).await {
                    Ok(names) => names,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(removed),
                    Err(e) => return Err(e).with_context(|| format!("reading {}", task.display())),
                };
                while let Some(entry) = names
                    .next_entry()
                    .await
                    .with_context(|| format!("reading {}", task.display()))?
                {
                    let is_dir = entry.file_type().await.is_ok_and(|t| t.is_dir());
                    if is_dir && doomed(&entry.file_name().to_string_lossy()) {
                        let gone = remove_tree(&entry.path()).await?;
                        removed.entries += gone.entries;
                        removed.bytes += gone.bytes;
                    }
                }
                Ok(removed)
            }
        }
    }

    /// An object store held in memory, where this build has object stores
    /// (`None` where it has none). For tests.
    pub fn in_memory() -> Option<SegmentStore> {
        #[cfg(feature = "cloud")]
        {
            let objects = std::sync::Arc::new(object_store::memory::InMemory::new());
            let store = crate::cloud::CloudStore::with_store(objects, "memory://logs", "logs");
            Some(SegmentStore::Cloud(std::sync::Arc::new(store)))
        }
        #[cfg(not(feature = "cloud"))]
        None
    }

    /// Add `text` to the open part `seq` of `key` (a `file://` store's).
    pub async fn append_open(&self, key: &LogKey, seq: i64, text: &str) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        let path = self.open_path(key, seq)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("making {}", parent.display()))?;
        }
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .with_context(|| format!("opening {}", path.display()))?;
        file.write_all(text.as_bytes())
            .await
            .with_context(|| format!("writing {}", path.display()))?;
        file.flush().await?;
        Ok(())
    }

    /// Make the open part `seq` of `key` `text`, whatever it held.
    pub async fn write_open(&self, key: &LogKey, seq: i64, text: &str) -> Result<()> {
        let path = self.open_path(key, seq)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("making {}", parent.display()))?;
        }
        // Written whole and then renamed: a reader finds the old part or
        // the whole new one, never a part of it.
        let partial = path.with_extension("open.partial");
        tokio::fs::write(&partial, text.as_bytes())
            .await
            .with_context(|| format!("writing {}", partial.display()))?;
        tokio::fs::rename(&partial, &path)
            .await
            .with_context(|| format!("keeping {}", path.display()))
    }

    /// The open part `seq` of `key`, as written so far; `None` where there
    /// is none (closed, or never begun).
    pub async fn read_open(&self, key: &LogKey, seq: i64) -> Result<Option<String>> {
        let path = self.open_path(key, seq)?;
        match tokio::fs::read(&path).await {
            Ok(bytes) => Ok(Some(String::from_utf8_lossy(&bytes).into_owned())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
        }
    }

    /// How many bytes the open part `seq` of `key` holds (0 where none).
    pub async fn open_len(&self, key: &LogKey, seq: i64) -> Result<u64> {
        let path = self.open_path(key, seq)?;
        match tokio::fs::metadata(&path).await {
            Ok(meta) => Ok(meta.len()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
        }
    }

    /// Remove the open part `seq` of `key`: closed, or replaced.
    pub async fn remove_open(&self, key: &LogKey, seq: i64) -> Result<()> {
        let path = self.open_path(key, seq)?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).with_context(|| format!("removing {}", path.display())),
        }
    }

    /// How many bytes the open part `seq` of `key` holds; `None` where there
    /// is none (closed and removed since it was counted, or never begun),
    /// which a reader that follows the log tells from an empty one.
    pub async fn open_size(&self, key: &LogKey, seq: i64) -> Result<Option<u64>> {
        let path = self.open_path(key, seq)?;
        match tokio::fs::metadata(&path).await {
            Ok(meta) => Ok(Some(meta.len())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
        }
    }

    /// At most `max` bytes of the open part `seq` of `key`, from byte `from`:
    /// the bytes as written, which may end inside a character the next write
    /// finishes (the caller keeps such an end back). `None` where there is no
    /// open part.
    pub async fn read_open_from(
        &self,
        key: &LogKey,
        seq: i64,
        from: u64,
        max: usize,
    ) -> Result<Option<Vec<u8>>> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        let path = self.open_path(key, seq)?;
        let mut file = match tokio::fs::File::open(&path).await {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("opening {}", path.display())),
        };
        file.seek(std::io::SeekFrom::Start(from))
            .await
            .with_context(|| format!("reading {}", path.display()))?;
        let mut bytes = Vec::new();
        file.take(max as u64)
            .read_to_end(&mut bytes)
            .await
            .with_context(|| format!("reading {}", path.display()))?;
        Ok(Some(bytes))
    }

    /// At most `max` bytes of closed segment `name`'s text, from byte `skip`
    /// of it. The object is fetched whole (a segment is one zstd frame), and
    /// decoded only as far as is read: what is held is the object and `max`,
    /// not the segment's text.
    pub async fn read_segment_from(&self, name: &str, skip: u64, max: usize) -> Result<Vec<u8>> {
        let compressed = match self {
            #[cfg(feature = "cloud")]
            SegmentStore::Cloud(store) => {
                use crate::ArtifactStore;
                store.get(&object_key(name)).await?
            }
            _ => {
                let path = self.dir().expect("a directory").join(name);
                tokio::fs::read(&path)
                    .await
                    .with_context(|| format!("reading {}", path.display()))?
            }
        };
        tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
            use std::io::Read;
            let mut text = zstd::stream::read::Decoder::new(compressed.as_slice())
                .context("a log segment that is not zstd")?;
            std::io::copy(&mut (&mut text).take(skip), &mut std::io::sink())
                .context("a log segment that is not zstd")?;
            let mut bytes = Vec::with_capacity(max.min(1 << 20));
            text.take(max as u64)
                .read_to_end(&mut bytes)
                .context("a log segment that is not zstd")?;
            Ok(bytes)
        })
        .await
        .context("decoding a log segment")?
    }

    fn open_path(&self, key: &LogKey, seq: i64) -> Result<PathBuf> {
        match self {
            SegmentStore::Dir(dir) => Ok(dir.join(key.open(seq))),
            _ => bail!("this store keeps the open part of a log in its task row, not a file"),
        }
    }

    /// A task attempt's whole log as kept: its closed segments (by name, in
    /// order), and then its open part: the next segment's file of a
    /// `file://` store, or `row` (the task row's `log`) of one that keeps it
    /// there, which a `file://` store's notes also land in. `None` where
    /// nothing of it is kept here, so that a reader falls back to the row.
    pub async fn assemble(
        &self,
        key: &LogKey,
        segments: &[String],
        row: Option<&str>,
    ) -> Result<Option<String>> {
        let mut text = String::new();
        for name in segments {
            // A segment the count names and the store has not: the attempt
            // was begun again since the count was read.
            match self.get_segment(name).await {
                Ok(segment) => text.push_str(&segment),
                Err(e) => return Err(anyhow::Error::new(Moved).context(format!("{e:#}"))),
            }
        }
        let open = match self {
            SegmentStore::Dir(_) => {
                let next = segments.len() as i64;
                let open = self.read_open(key, next).await?;
                // A close writes the next open part before it records its
                // segments, so the open part the count names is there,
                // unless the log moved on since the count was read: closed
                // twice, or begun again.
                if open.is_none() {
                    if !segments.is_empty() {
                        return Err(Moved.into());
                    }
                    // Nothing counted and no open part 0: never begun, or
                    // closed twice since the count. A kept segment 0 says
                    // which (a reset deletes the segments before the open
                    // parts).
                    let first = self.dir().expect("a directory").join(key.segment(0));
                    if tokio::fs::try_exists(&first).await.unwrap_or(false) {
                        return Err(Moved.into());
                    }
                }
                open
            }
            _ => None,
        };
        if segments.is_empty() && open.is_none() {
            return Ok(None);
        }
        if let Some(open) = open {
            text.push_str(&open);
        }
        if let Some(row) = row {
            text.push_str(row);
        }
        Ok(Some(text))
    }
}

/// What a reader met where a log moved on under it, between the count of its
/// segments it read with the row and its reading them: it reads the row
/// again ([`is_moved`]).
#[derive(Debug, Clone, Copy)]
pub struct Moved;

impl std::fmt::Display for Moved {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the log moved on while it was read: read it again")
    }
}

impl std::error::Error for Moved {}

/// Whether `e` is a log that moved on while it was read ([`Moved`]).
pub fn is_moved(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| cause.is::<Moved>())
}

/// How many times a reader reads a row and its log again when the log moved
/// on under it, before it says so.
pub const READS: usize = 3;

/// A task's row as a reader of logs has it: the attempt it is on, and its
/// `log` column.
#[derive(Debug, Clone, Copy)]
pub struct TaskRow<'a> {
    pub task_id: &'a str,
    /// The attempt its log is of, and which writing of it (`log_gen`).
    pub fence: i64,
    pub gen: i64,
    pub log: Option<&'a str>,
    /// How many closed segments the attempt has, read with the row: their
    /// names follow from it ([`LogKey::segment`]).
    pub segments: i64,
}

/// The logs of `tasks` that are kept in `store`, by task id: each task's
/// closed segments and then its open part. A task not kept here is not in
/// the map: its reader takes its row as before.
pub async fn kept_logs(
    store: &SegmentStore,
    tasks: &[TaskRow<'_>],
) -> Result<std::collections::HashMap<String, String>> {
    let mut out = std::collections::HashMap::new();
    for task in tasks {
        let key = LogKey::new(task.task_id, task.fence, task.gen);
        let names: Vec<String> = (0..task.segments).map(|seq| key.segment(seq)).collect();
        if names.is_empty() && store.open_in_row() {
            // Nothing closed, and the open part is the row: the row is it.
            continue;
        }
        if let Some(text) = store.assemble(&key, &names, task.log).await? {
            out.insert(task.task_id.to_string(), text);
        }
    }
    Ok(out)
}

/// Remove the directory `dir` and all under it, and count it: its files and
/// their bytes, its directories and itself. No directory is no error.
async fn remove_tree(dir: &Path) -> Result<Removed> {
    let mut removed = Removed::default();
    let mut todo = vec![dir.to_path_buf()];
    while let Some(next) = todo.pop() {
        let mut names = match tokio::fs::read_dir(&next).await {
            Ok(names) => names,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e).with_context(|| format!("reading {}", next.display())),
        };
        removed.entries += 1;
        while let Some(entry) = names
            .next_entry()
            .await
            .with_context(|| format!("reading {}", next.display()))?
        {
            match entry.file_type().await {
                Ok(kind) if kind.is_dir() => todo.push(entry.path()),
                _ => {
                    removed.entries += 1;
                    removed.bytes += entry.metadata().await.map_or(0, |m| m.len());
                }
            }
        }
    }
    match tokio::fs::remove_dir_all(dir).await {
        Ok(()) => Ok(removed),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(removed),
        Err(e) => Err(e).with_context(|| format!("removing {}", dir.display())),
    }
}

/// A segment's name as an object store key: under `logs-<task>/`, by the
/// name's own parts (`<task>/<fence>[.<gen>]/<seq>.seg.zst`).
#[cfg(feature = "cloud")]
fn object_key(name: &str) -> crate::ArtifactKey {
    let mut parts = name.splitn(3, '/');
    let (task, fence, file) = (
        parts.next().unwrap_or_default(),
        parts.next().unwrap_or_default(),
        parts.next().unwrap_or_default(),
    );
    crate::ArtifactKey {
        run_id: format!("logs-{task}"),
        task: fence.to_string(),
        name: file.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_store_is_named_by_its_url() {
        assert!(matches!(
            SegmentStore::from_url("file:///var/lib/dagron/logs").unwrap(),
            SegmentStore::Dir(_)
        ));
        assert!(SegmentStore::from_url("file://relative").is_err());
        assert!(SegmentStore::from_url("file://").is_err());
        assert_eq!(parse_segment_bytes("65536").unwrap(), 65536);
        assert!(parse_segment_bytes("100").is_err());
        assert!(parse_segment_bytes("lots").is_err());
    }

    #[tokio::test]
    async fn a_log_is_its_segments_and_then_its_open_part() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentStore::Dir(dir.path().to_path_buf());
        let key = LogKey::new("t-1", 2, 0);
        assert_eq!(
            store.assemble(&key, &[], None).await.unwrap(),
            None,
            "nothing kept"
        );

        let first = store.put_segment(&key, 0, "one\ntwo\n").await.unwrap();
        assert_eq!(first, "t-1/2/000000.seg.zst");
        store.append_open(&key, 1, "three\n").await.unwrap();
        store.append_open(&key, 1, "four\n").await.unwrap();
        assert_eq!(store.open_len(&key, 1).await.unwrap(), 11);
        let all = store
            .assemble(&key, std::slice::from_ref(&first), Some("a note\n"))
            .await
            .unwrap();
        assert_eq!(all.as_deref(), Some("one\ntwo\nthree\nfour\na note\n"));

        // On disk it is compressed, and whole.
        let raw = std::fs::read(dir.path().join(&first)).unwrap();
        assert_eq!(&raw[..4], &[0x28, 0xb5, 0x2f, 0xfd]);
        store.remove_open(&key, 1).await.unwrap();
        std::fs::remove_file(dir.path().join(&first)).unwrap();
        assert_eq!(store.assemble(&key, &[], None).await.unwrap(), None);
    }

    /// A reader that counted the segments before a close finds the open part
    /// it counted on; one that counted before two closes, or before the log
    /// was begun again, is told the log moved on, and reads again; and an
    /// open part is never read half-written.
    #[tokio::test]
    async fn a_reader_is_told_where_a_log_moved_on_under_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentStore::Dir(dir.path().to_path_buf());
        let key = LogKey::new("t-3", 1, 0);
        let row = |segments| TaskRow {
            task_id: "t-3",
            fence: 1,
            gen: 0,
            log: None,
            segments,
        };
        // Open part 0, then a close, as the engine makes it: the rest as
        // open part 1 first, then the segment (its row, counted after).
        store.write_open(&key, 0, "one\ntwo\n").await.unwrap();
        store.write_open(&key, 1, "two\n").await.unwrap();
        store.put_segment(&key, 0, "one\n").await.unwrap();
        // Counted before the close: the open part it counted on is there.
        let before = kept_logs(&store, &[row(0)]).await.unwrap();
        assert_eq!(before["t-3"], "one\ntwo\n");
        // Counted after it: the segment, and the new open part.
        let after = kept_logs(&store, &[row(1)]).await.unwrap();
        assert_eq!(after["t-3"], "one\ntwo\n");
        // A second close, as the engine makes it: the rest as open part 2,
        // segment 1, and open part 0 removed (1 stays until the next). A
        // reader that counted before the first close, nothing, is told, and
        // does not read a shorter log; one that counted after it still
        // reads the whole log.
        store.write_open(&key, 2, "").await.unwrap();
        store.put_segment(&key, 1, "two\n").await.unwrap();
        store.remove_open(&key, 0).await.unwrap();
        let stale = kept_logs(&store, &[row(0)]).await.unwrap_err();
        assert!(is_moved(&stale), "{stale:#}");
        let one_behind = kept_logs(&store, &[row(1)]).await.unwrap();
        assert_eq!(one_behind["t-3"], "one\ntwo\n");
        // A third close removes open part 1: the reader that counted one
        // segment is told now.
        store.remove_open(&key, 1).await.unwrap();
        let stale = kept_logs(&store, &[row(1)]).await.unwrap_err();
        assert!(is_moved(&stale), "{stale:#}");
        // Begun again: a reader that counted the old segments is told too.
        for seq in 0..2 {
            std::fs::remove_file(dir.path().join(key.segment(seq))).unwrap();
        }
        let gone = kept_logs(&store, &[row(2)]).await.unwrap_err();
        assert!(is_moved(&gone), "{gone:#}");
        // No half-written open part is left behind.
        assert!(std::fs::read_dir(dir.path().join("t-3/1"))
            .unwrap()
            .all(|e| !e
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".partial")));
    }

    #[tokio::test]
    async fn a_store_that_keeps_the_open_part_in_the_row_reads_it_from_there() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentStore::DirOpenInRow(dir.path().to_path_buf());
        let key = LogKey::new("t-2", 1, 0);
        assert!(store.open_in_row());
        assert!(
            store.append_open(&key, 0, "x").await.is_err(),
            "no open file"
        );
        let seg = store.put_segment(&key, 0, "closed\n").await.unwrap();
        let all = store.assemble(&key, &[seg], Some("open\n")).await.unwrap();
        assert_eq!(all.as_deref(), Some("closed\nopen\n"));
        assert_eq!(
            store.assemble(&key, &[], Some("row\n")).await.unwrap(),
            None,
            "the row is read as it was"
        );
    }

    /// A log written again from its start is kept under names of its own, so
    /// that a reader of the writing before reads its bytes or none; and a
    /// reader that follows a log reads it from a byte, of a segment or of the
    /// open part, without the bytes before it.
    #[tokio::test]
    async fn each_writing_of_a_log_has_its_own_names_and_is_read_from_a_byte() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentStore::Dir(dir.path().to_path_buf());
        let first = LogKey::new("t-4", 3, 0);
        let again = LogKey::new("t-4", 3, 2);
        assert_eq!(first.segment(0), "t-4/3/000000.seg.zst");
        assert_eq!(again.segment(0), "t-4/3.2/000000.seg.zst");
        store.write_open(&first, 0, "unmasked\n").await.unwrap();
        store.write_open(&again, 0, "***\n").await.unwrap();
        assert_eq!(
            store.read_open(&first, 0).await.unwrap().as_deref(),
            Some("unmasked\n")
        );

        let text = "é€🦀 one\ntwo\nthree\n";
        let name = store.put_segment(&again, 1, text).await.unwrap();
        let all = store.read_segment_from(&name, 0, 1 << 20).await.unwrap();
        assert_eq!(all, text.as_bytes());
        let from = text.find("two").unwrap();
        let rest = store
            .read_segment_from(&name, from as u64, 4)
            .await
            .unwrap();
        assert_eq!(rest, b"two\n", "four bytes from the byte asked for");
        let past = store.read_segment_from(&name, 1000, 10).await.unwrap();
        assert!(past.is_empty(), "past the end is nothing");

        store.append_open(&again, 2, "four\nfi").await.unwrap();
        assert_eq!(store.open_size(&again, 2).await.unwrap(), Some(7));
        assert_eq!(
            store.open_size(&again, 3).await.unwrap(),
            None,
            "none, not empty"
        );
        assert_eq!(
            store
                .read_open_from(&again, 2, 5, 100)
                .await
                .unwrap()
                .as_deref(),
            Some(&b"fi"[..])
        );
        assert_eq!(store.read_open_from(&again, 3, 0, 100).await.unwrap(), None);
        // A half-written character is read as the bytes there are.
        store.append_open(&again, 4, "é").await.unwrap();
        assert_eq!(
            store
                .read_open_from(&again, 4, 0, 1)
                .await
                .unwrap()
                .as_deref(),
            Some(&"é".as_bytes()[..1])
        );
    }

    fn writing(fence: i64, gen: i64) -> Writing {
        Writing { fence, gen }
    }

    #[test]
    fn a_writing_is_named_by_its_fence_and_gen() {
        assert_eq!(parse_writing("7"), Some(writing(7, 0)));
        assert_eq!(parse_writing("7.3"), Some(writing(7, 3)));
        for stray in ["x", "7.", ".3", "7.3.1", "-1", "", "1234567890123456789"] {
            assert_eq!(parse_writing(stray), None, "{stray:?}");
        }
        assert!(writing(2, 9) < writing(3, 0) && writing(3, 0) < writing(3, 1));
        let key = LogKey::new("t", 4, 2);
        let name = key.segment(0);
        assert_eq!(
            parse_writing(name.split('/').nth(1).unwrap()),
            Some(writing(4, 2))
        );
    }

    /// What a writing holds on disk, as a log batch and a crash leave it: a
    /// closed segment, an open part, and one of each half written.
    async fn a_writing_on_disk(store: &SegmentStore, dir: &Path, key: &LogKey) {
        store.put_segment(key, 0, "closed\n").await.unwrap();
        store.write_open(key, 1, "open\n").await.unwrap();
        let at = dir.join(key.segment(0));
        std::fs::write(at.with_extension("zst.partial"), b"half").unwrap();
        std::fs::write(at.with_file_name("000001.open.partial"), b"half").unwrap();
    }

    fn files_under(dir: &Path) -> u64 {
        std::fs::read_dir(dir).map_or(0, |names| {
            names
                .map(|e| e.unwrap())
                .map(|e| match e.file_type().unwrap().is_dir() {
                    true => files_under(&e.path()),
                    false => 1,
                })
                .sum()
        })
    }

    #[tokio::test]
    async fn a_task_s_old_writings_go_and_its_current_one_stays() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentStore::Dir(dir.path().to_path_buf());
        let task = "3f1c3a52-6a51-4f0e-9a43-1b1f0e4b8c10";
        let other = "9b2e7c41-0d1f-4c55-8f6e-2a3b4c5d6e7f";
        for (fence, gen) in [(2, 0), (2, 1), (3, 1)] {
            a_writing_on_disk(&store, dir.path(), &LogKey::new(task, fence, gen)).await;
        }
        a_writing_on_disk(&store, dir.path(), &LogKey::new(other, 1, 1)).await;
        let notes = dir.path().join(task).join("notes");
        std::fs::create_dir_all(&notes).unwrap();
        std::fs::write(notes.join("kept"), b"x").unwrap();
        let bytes = |fence: i64, gen: i64| -> u64 {
            let at = dir.path().join(task).join(match gen {
                0 => fence.to_string(),
                gen => format!("{fence}.{gen}"),
            });
            std::fs::read_dir(at)
                .unwrap()
                .map(|e| e.unwrap().metadata().unwrap().len())
                .sum()
        };
        let wanted = bytes(2, 0) + bytes(2, 1);

        let removed = store.remove_log(task, Some(writing(3, 1))).await.unwrap();
        assert_eq!(
            removed,
            Removed {
                entries: 2 * 4 + 2,
                bytes: wanted
            },
            "four files and their directory, twice"
        );
        assert!(!dir.path().join(task).join("2").exists());
        assert!(!dir.path().join(task).join("2.1").exists());
        assert_eq!(files_under(&dir.path().join(task).join("3.1")), 4);
        assert!(notes.join("kept").exists(), "no writing's: kept");
        assert_eq!(files_under(&dir.path().join(other)), 4, "another task's");
        assert_eq!(
            store.remove_log(task, Some(writing(3, 1))).await.unwrap(),
            Removed::default()
        );
    }

    #[tokio::test]
    async fn an_empty_id_names_no_task_and_removes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentStore::Dir(dir.path().to_path_buf());
        let kept = dir.path().join("a-task").join("2.1");
        std::fs::create_dir_all(&kept).unwrap();
        std::fs::write(kept.join("000000.open"), "a line\n").unwrap();
        assert!(store.remove_log("", None).await.is_err());
        assert!(kept.join("000000.open").exists(), "the store is as it was");
    }

    #[tokio::test]
    async fn a_deleted_task_s_whole_log_goes() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentStore::Dir(dir.path().to_path_buf());
        let task = "0d6f6a2b-5b0e-4d7e-b1c9-7c1a2e3f4a5b";
        a_writing_on_disk(&store, dir.path(), &LogKey::new(task, 4, 2)).await;
        std::fs::write(dir.path().join(task).join("stray"), b"abc").unwrap();
        let removed = store.remove_log(task, None).await.unwrap();
        assert_eq!(
            removed.entries,
            4 + 1 + 2,
            "files, the stray, and two directories"
        );
        assert!(removed.bytes >= 3 + 4 + 4);
        assert!(!dir.path().join(task).exists());
        assert_eq!(
            store.remove_log(task, None).await.unwrap(),
            Removed::default()
        );
        let none = SegmentStore::Dir(dir.path().join("never-made"));
        assert_eq!(
            none.remove_log(task, None).await.unwrap(),
            Removed::default()
        );
        assert_eq!(
            none.remove_log(task, Some(writing(1, 1))).await.unwrap(),
            Removed::default()
        );
    }

    #[tokio::test]
    async fn a_bare_fence_is_gen_zero_and_kept_while_current() {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentStore::Dir(dir.path().to_path_buf());
        let task = "6e5d4c3b-2a19-4807-a6b5-c4d3e2f1a0b9";
        a_writing_on_disk(&store, dir.path(), &LogKey::new(task, 5, 0)).await;
        let removed = store.remove_log(task, Some(writing(5, 0))).await.unwrap();
        assert_eq!(removed, Removed::default(), "the writing the row is on");
        assert_eq!(files_under(&dir.path().join(task).join("5")), 4);
        let removed = store.remove_log(task, Some(writing(5, 1))).await.unwrap();
        assert_eq!(removed.entries, 5);
        assert!(!dir.path().join(task).join("5").exists());
    }

    /// An object store's names of a task are taken by writing and whole, as
    /// a directory's are, and nothing beside them: not a task whose id
    /// begins as its does, an artifact, or another kind of object under the
    /// same prefix.
    #[cfg(feature = "cloud")]
    #[tokio::test]
    async fn an_object_store_drops_a_task_s_objects_by_writing_and_whole() {
        use object_store::{path::Path as ObjPath, ObjectStore};
        let objects = std::sync::Arc::new(object_store::memory::InMemory::new());
        let store = SegmentStore::Cloud(std::sync::Arc::new(crate::cloud::CloudStore::with_store(
            objects.clone(),
            "s3://b/logs",
            "logs",
        )));
        let task = "3f1c3a52-6a51-4f0e-9a43-1b1f0e4b8c10";
        for (fence, gen) in [(2, 0), (2, 1), (3, 1)] {
            let key = LogKey::new(task, fence, gen);
            store.put_segment(&key, 0, "one\n").await.unwrap();
            store.put_segment(&key, 1, "two\n").await.unwrap();
        }
        let beside = [
            format!("logs/logs-{task}x/1.1/000000.seg.zst"),
            format!("logs/run-1/{task}/out.bin"),
            "logs/ci/cache/entry".to_string(),
            format!("logs/logs-{task}/notes/kept"),
        ];
        for name in &beside {
            objects
                .put(&ObjPath::from(name.as_str()), "x".into())
                .await
                .unwrap();
        }
        let names = || async {
            let mut all: Vec<String> = Vec::new();
            let mut listed = objects.list(None);
            while let Some(meta) = futures_util::StreamExt::next(&mut listed).await {
                all.push(meta.unwrap().location.to_string());
            }
            all.sort();
            all
        };
        let one = names().await.len();

        let removed = store.remove_log(task, Some(writing(3, 1))).await.unwrap();
        assert_eq!(removed.entries, 4, "two writings of two segments");
        assert!(removed.bytes > 0);
        assert_eq!(names().await.len(), one - 4);
        assert!(store
            .get_segment(&LogKey::new(task, 3, 1).segment(1))
            .await
            .is_ok());
        assert!(store
            .get_segment(&LogKey::new(task, 2, 1).segment(0))
            .await
            .is_err());
        assert_eq!(
            store.remove_log(task, Some(writing(3, 1))).await.unwrap(),
            Removed::default()
        );

        let removed = store.remove_log(task, None).await.unwrap();
        assert_eq!(
            removed.entries, 3,
            "the writing the row was on, and the stray"
        );
        let mut wanted: Vec<String> = beside[..3].to_vec();
        wanted.sort();
        assert_eq!(names().await, wanted, "nothing beside the task's");
        assert_eq!(
            store.remove_log(task, None).await.unwrap(),
            Removed::default()
        );
    }

    /// Against a bucket, by hand: `DAGRON_TEST_LOG_STORE_URL` names one
    /// (`s3://bucket/prefix`, with `AWS_*` and the like for it), in a build
    /// with its backend. A writing's removal, and a task's; S3's bulk
    /// delete among them. Skipped where unset.
    #[tokio::test]
    async fn log_store_removal_against_a_real_bucket() {
        let Ok(url) = std::env::var("DAGRON_TEST_LOG_STORE_URL") else {
            eprintln!("DAGRON_TEST_LOG_STORE_URL unset - skipping a test against a bucket");
            return;
        };
        let store = SegmentStore::from_url(url.trim()).unwrap();
        let task = format!("removal-{}", std::process::id());
        for (fence, gen) in [(1, 1), (2, 1)] {
            let key = LogKey::new(task.as_str(), fence, gen);
            for seq in 0..3 {
                store.put_segment(&key, seq, "a line\n").await.unwrap();
            }
        }
        let removed = store.remove_log(&task, Some(writing(2, 1))).await.unwrap();
        assert_eq!(removed.entries, 3);
        assert!(store
            .get_segment(&LogKey::new(task.as_str(), 2, 1).segment(2))
            .await
            .is_ok());
        assert_eq!(store.remove_log(&task, None).await.unwrap().entries, 3);
        assert_eq!(
            store.remove_log(&task, None).await.unwrap(),
            Removed::default()
        );
    }
}
