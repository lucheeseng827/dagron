//! **dagron-ci-cache**: a task's path caches (`workspace.cache`), restored
//! before its steps and saved after they succeed, with the signed URLs the
//! engine gave it in `DAGRON_CACHE` (dagron-engine's `path_cache` module).
//!
//! - **The key** of a cache is its prefix and a hash of the files of the
//!   workspace its `key_files` globs name, read when it is restored, before
//!   any step has changed them. `key_files` empty: the prefix alone.
//! - **Restore** takes the entry offered under that key, or else the newest
//!   offered (the run's own ref first, then the ref it restores from), and
//!   unpacks it over the cache's paths. Nothing offered: the task starts cold.
//! - **Save**, where the run was given URLs to save with, packs the paths into
//!   one archive (tar, zstd) and writes it and a manifest, unless it restored
//!   the very key, which is then kept as it was.
//!
//! Nothing here fails the task. A cache that is not read or written says why
//! on the task's log, and the task runs on, cold or unsaved. A signed URL is
//! a credential: no message here prints one.

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// The variable the engine gives a task its caches in.
pub const ENV: &str = "DAGRON_CACHE";
/// What restore leaves for save: the key of each cache and whether it was
/// the one restored.
pub const STATE_ENV: &str = "DAGRON_CACHE_STATE";
/// A key file walk stops here: a workspace this large is not a checkout.
const MAX_WALK: usize = 200_000;
/// zstd's level: fast, and most of what a higher one gives.
const LEVEL: i32 = 3;

/// `DAGRON_CACHE`, as the engine writes it.
#[derive(Debug, Clone, Deserialize)]
pub struct Given {
    pub v: u32,
    pub max_bytes: u64,
    pub caches: Vec<Cache>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Cache {
    pub prefix: String,
    pub version: String,
    #[serde(default)]
    pub key_files: Vec<String>,
    pub paths: Vec<String>,
    #[serde(default)]
    pub restore: Vec<Offered>,
    #[serde(default)]
    pub save: Option<Save>,
}

/// A signed request: its method, URL and the headers it was signed with.
#[derive(Debug, Clone, Deserialize)]
pub struct Signed {
    pub method: String,
    pub url: String,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
}

/// An entry offered to restore.
#[derive(Debug, Clone, Deserialize)]
pub struct Offered {
    pub key: String,
    /// `own`, or the ref it is of.
    pub from: String,
    #[serde(flatten)]
    pub get: Signed,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Save {
    pub archive: Signed,
    pub manifest: Signed,
}

/// What restore left for save, a cache at a time.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct State {
    pub caches: Vec<Kept>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Kept {
    pub prefix: String,
    pub key: String,
    /// The entry restored was saved under this very key.
    pub hit: bool,
}

impl Given {
    pub fn parse(raw: &str) -> Result<Self, String> {
        let given: Given =
            serde_json::from_str(raw).map_err(|e| format!("{ENV} is not one: {e}"))?;
        if given.v != 1 {
            return Err(format!(
                "{ENV} is of version {}, and this dagron-ci-cache reads 1",
                given.v
            ));
        }
        Ok(given)
    }
}

/// Where the state file is: `DAGRON_CACHE_STATE`, or the temporary directory.
pub fn state_path() -> PathBuf {
    if let Some(p) = std::env::var_os(STATE_ENV) {
        return PathBuf::from(p);
    }
    let task = std::env::var("DAGRON_TASK_ID").unwrap_or_default();
    let task: String = task
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-')
        .collect();
    std::env::temp_dir().join(format!("dagron-cache-{task}.json"))
}

// ── the key ──────────────────────────────────────────────────────────────────

/// A cache's key: its prefix, and a hash of the files its globs name in
/// `workspace` (their paths and contents).
pub fn key_of(cache: &Cache, workspace: &Path) -> std::io::Result<String> {
    if cache.key_files.is_empty() {
        return Ok(cache.prefix.clone());
    }
    let files = matching(workspace, &cache.key_files)?;
    let mut h = Sha256::new();
    for rel in &files {
        let mut file = Sha256::new();
        std::io::copy(&mut File::open(workspace.join(rel))?, &mut file)?;
        h.update(rel.as_bytes());
        h.update([0u8]);
        h.update(file.finalize());
    }
    let hex: String = h
        .finalize()
        .iter()
        .take(16)
        .map(|b| format!("{b:02x}"))
        .collect();
    Ok(format!("{}-{hex}", cache.prefix))
}

/// The files under `root` (relative, `/`-separated, sorted) that any of
/// `globs` matches. Not into `.git`, and not through a symbolic link.
pub fn matching(root: &Path, globs: &[String]) -> std::io::Result<BTreeSet<String>> {
    let mut found = BTreeSet::new();
    let mut walked = 0usize;
    let mut stack = vec![PathBuf::new()];
    while let Some(rel) = stack.pop() {
        let dir = root.join(&rel);
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if rel.as_os_str().is_empty() => return Err(e),
            Err(_) => continue,
        };
        for entry in entries {
            let entry = entry?;
            walked += 1;
            if walked > MAX_WALK {
                return Err(std::io::Error::other(format!(
                    "the workspace holds more than {MAX_WALK} files: key_files are read in a checkout"
                )));
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let path = rel.join(name);
            let kind = entry.file_type()?;
            if kind.is_dir() {
                if name != ".git" {
                    stack.push(path);
                }
            } else if kind.is_file() {
                let text = path.to_string_lossy().replace('\\', "/");
                if globs.iter().any(|g| glob_match(g, &text)) {
                    found.insert(text);
                }
            }
        }
    }
    Ok(found)
}

/// Whether `path` matches `pattern`: `/`-separated, `**` any number of
/// directories, `*` and `?` within one name.
pub fn glob_match(pattern: &str, path: &str) -> bool {
    let pattern = pattern.trim_start_matches("./");
    let p: Vec<&str> = pattern.split('/').filter(|s| !s.is_empty()).collect();
    let s: Vec<&str> = path.split('/').collect();
    fn segs(p: &[&str], s: &[&str]) -> bool {
        match p.split_first() {
            None => s.is_empty(),
            Some((&"**", rest)) => (0..=s.len()).any(|i| segs(rest, &s[i..])),
            Some((first, rest)) => {
                !s.is_empty()
                    && name_match(first.as_bytes(), s[0].as_bytes())
                    && segs(rest, &s[1..])
            }
        }
    }
    fn name_match(p: &[u8], s: &[u8]) -> bool {
        match p.split_first() {
            None => s.is_empty(),
            Some((b'*', rest)) => (0..=s.len()).any(|i| name_match(rest, &s[i..])),
            Some((b'?', rest)) => !s.is_empty() && name_match(rest, &s[1..]),
            Some((c, rest)) => s.first() == Some(c) && name_match(rest, &s[1..]),
        }
    }
    segs(&p, &s)
}

/// A cache's path, as the task's file system has it: `~/…` in `home`, a
/// relative one in the workspace.
pub fn expand(path: &str, workspace: &Path, home: &Path) -> PathBuf {
    if path == "~" {
        home.to_path_buf()
    } else if let Some(rest) = path.strip_prefix("~/") {
        home.join(rest)
    } else {
        workspace.join(path)
    }
}

// ── the archive ──────────────────────────────────────────────────────────────

/// Pack `paths` into `out`: each under its index, `<i>/…`, symbolic links as
/// links. Returns how many of the paths there were to pack.
pub fn pack(paths: &[PathBuf], out: &File) -> std::io::Result<usize> {
    let encoder = zstd::stream::write::Encoder::new(out, LEVEL)?;
    let mut tar = tar::Builder::new(encoder);
    tar.follow_symlinks(false);
    let mut packed = 0;
    for (i, path) in paths.iter().enumerate() {
        let Ok(meta) = std::fs::symlink_metadata(path) else {
            continue;
        };
        if meta.is_dir() {
            tar.append_dir_all(i.to_string(), path)?;
        } else {
            tar.append_path_with_name(path, i.to_string())?;
        }
        packed += 1;
    }
    tar.into_inner()?.finish()?;
    Ok(packed)
}

/// Unpack an archive `pack` made over `paths`. An entry that names no path
/// of them, or climbs out of one, is refused.
pub fn unpack(archive: impl Read, paths: &[PathBuf]) -> std::io::Result<u64> {
    let mut tar = tar::Archive::new(zstd::stream::read::Decoder::new(archive)?);
    tar.set_preserve_mtime(true);
    tar.set_overwrite(true);
    let mut files = 0;
    for entry in tar.entries()? {
        let mut entry = entry?;
        let name = entry.path()?.into_owned();
        let mut parts = name.components();
        let index = match parts.next() {
            Some(Component::Normal(i)) => i.to_str().and_then(|i| i.parse::<usize>().ok()),
            _ => None,
        };
        let rest: PathBuf = parts.clone().collect();
        let Some(base) = index.and_then(|i| paths.get(i)) else {
            return Err(std::io::Error::other(format!(
                "{} names no path of the cache",
                name.display()
            )));
        };
        if !parts.all(|c| matches!(c, Component::Normal(_))) {
            return Err(std::io::Error::other(format!(
                "{} climbs out of its path",
                name.display()
            )));
        }
        let target = if rest.as_os_str().is_empty() {
            base.clone()
        } else {
            base.join(&rest)
        };
        // A cache's path itself is a directory or a file, never a link the
        // archive makes: an entry `0` that is a link, then `0/file`, would
        // write through it. `pack` writes a path as a directory or a file,
        // and a link that is there already is the image's own.
        let kind = entry.header().entry_type();
        if rest.as_os_str().is_empty() && !(kind.is_dir() || kind.is_file()) {
            return Err(std::io::Error::other(format!(
                "{} would make the cache's path {} a link",
                name.display(),
                base.display()
            )));
        }
        // Nothing is written through a link: not one this archive made (an
        // entry `0/link` and then `0/link/file`), and not one that was there.
        // `pack` keeps a link as a link and never what is under it, so an
        // archive it made has no entry beneath one.
        let mut under = base.clone();
        for part in rest.parent().into_iter().flat_map(Path::components) {
            under.push(part);
            if std::fs::symlink_metadata(&under).is_ok_and(|m| m.file_type().is_symlink()) {
                return Err(std::io::Error::other(format!(
                    "{} is under a symbolic link, {}",
                    name.display(),
                    under.display()
                )));
            }
        }
        if entry.header().entry_type().is_dir() {
            std::fs::create_dir_all(&target)?;
            continue;
        }
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // A link or a file there already is replaced, not written through.
        if std::fs::symlink_metadata(&target).is_ok_and(|m| !m.is_dir()) {
            std::fs::remove_file(&target)?;
        }
        entry.unpack(&target)?;
        files += 1;
    }
    Ok(files)
}

// ── the requests ─────────────────────────────────────────────────────────────

fn agent() -> ureq::Agent {
    ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(30))
        .timeout_read(Duration::from_secs(300))
        .timeout_write(Duration::from_secs(300))
        .build()
}

/// What a failed request says, without its URL (which is a credential).
fn said(e: ureq::Error) -> String {
    match e {
        ureq::Error::Status(code, response) => {
            let mut body = String::new();
            let _ = response.into_reader().take(300).read_to_string(&mut body);
            let body = body.split_whitespace().collect::<Vec<_>>().join(" ");
            format!(
                "the store answered {code}{}",
                if body.is_empty() {
                    String::new()
                } else {
                    format!(": {body}")
                }
            )
        }
        ureq::Error::Transport(t) => format!(
            "the store was not reached ({:?}{})",
            t.kind(),
            t.message().map(|m| format!(": {m}")).unwrap_or_default()
        ),
    }
}

fn request(agent: &ureq::Agent, signed: &Signed) -> ureq::Request {
    signed
        .headers
        .iter()
        .fold(agent.request(&signed.method, &signed.url), |r, (k, v)| {
            r.set(k, v)
        })
}

/// Fetch an entry into `into`, at most `max` bytes.
fn fetch(agent: &ureq::Agent, get: &Signed, into: &mut File, max: u64) -> Result<u64, String> {
    let response = request(agent, get).call().map_err(said)?;
    let got = std::io::copy(&mut response.into_reader().take(max + 1), into)
        .map_err(|e| format!("reading it: {e}"))?;
    if got > max {
        return Err(format!("it is larger than {max} bytes"));
    }
    into.flush()
        .and_then(|_| into.rewind())
        .map_err(|e| e.to_string())?;
    Ok(got)
}

fn put(agent: &ureq::Agent, signed: &Signed, body: impl Read, len: u64) -> Result<(), String> {
    request(agent, signed)
        .set("Content-Length", &len.to_string())
        .send(body)
        .map(|_| ())
        .map_err(said)
}

fn human(bytes: u64) -> String {
    match bytes {
        b if b >= 1 << 30 => format!("{:.1} GiB", b as f64 / (1u64 << 30) as f64),
        b if b >= 1 << 20 => format!("{:.1} MiB", b as f64 / (1u64 << 20) as f64),
        b if b >= 1 << 10 => format!("{:.1} KiB", b as f64 / 1024.0),
        b => format!("{b} B"),
    }
}

// ── restore and save ─────────────────────────────────────────────────────────

/// Where the task is: its workspace (where it starts) and its home.
pub struct Here {
    pub workspace: PathBuf,
    pub home: PathBuf,
}

impl Here {
    pub fn now() -> Self {
        Self {
            workspace: std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/")),
            home: std::env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/")),
        }
    }

    fn paths(&self, cache: &Cache) -> Vec<PathBuf> {
        cache
            .paths
            .iter()
            .map(|p| expand(p, &self.workspace, &self.home))
            .collect()
    }
}

/// Restore each cache; returns the state for save, and says on `log` what
/// became of each.
pub fn restore(given: &Given, here: &Here, log: &mut impl Write) -> State {
    let agent = agent();
    let mut state = State::default();
    for cache in &given.caches {
        let key = match key_of(cache, &here.workspace) {
            Ok(key) => key,
            Err(e) => {
                let _ = writeln!(
                    log,
                    "cache {}: not restored: its key files could not be read: {e}",
                    cache.prefix
                );
                continue;
            }
        };
        let chosen = cache
            .restore
            .iter()
            .find(|o| o.key == key)
            .or_else(|| cache.restore.first());
        let Some(offered) = chosen else {
            let _ = writeln!(
                log,
                "cache {}: nothing to restore under {key}: it starts cold",
                cache.prefix
            );
            state.caches.push(Kept {
                prefix: cache.prefix.clone(),
                key,
                hit: false,
            });
            continue;
        };
        let hit = offered.key == key;
        let restored = (|| -> Result<(u64, u64), String> {
            let mut file = tempfile().map_err(|e| format!("a temporary file: {e}"))?;
            let size = fetch(&agent, &offered.get, &mut file, given.max_bytes)?;
            let files =
                unpack(&file, &here.paths(cache)).map_err(|e| format!("unpacking it: {e}"))?;
            Ok((size, files))
        })();
        match restored {
            Ok((size, files)) => {
                let _ = writeln!(
                    log,
                    "cache {}: restored {} ({}, {files} files{}){}",
                    cache.prefix,
                    offered.key,
                    human(size),
                    if offered.from == "own" {
                        String::new()
                    } else {
                        format!(", from {}", offered.from)
                    },
                    if hit {
                        ""
                    } else {
                        ": not this key, the newest"
                    }
                );
                state.caches.push(Kept {
                    prefix: cache.prefix.clone(),
                    key,
                    hit,
                });
            }
            Err(e) => {
                let _ = writeln!(
                    log,
                    "cache {}: {} not restored: {e}",
                    cache.prefix, offered.key
                );
                state.caches.push(Kept {
                    prefix: cache.prefix.clone(),
                    key,
                    hit: false,
                });
            }
        }
    }
    state
}

/// Save each cache the run may write, unless restore found its very key.
pub fn save(given: &Given, state: &State, here: &Here, log: &mut impl Write) {
    let agent = agent();
    for cache in &given.caches {
        let Some(save) = &cache.save else {
            let _ = writeln!(
                log,
                "cache {}: not saved: this run reads caches and does not write them",
                cache.prefix
            );
            continue;
        };
        // The key restore read; else, now (a cache restore did not reach).
        let kept = state.caches.iter().find(|k| k.prefix == cache.prefix);
        if kept.is_some_and(|k| k.hit) {
            let _ = writeln!(
                log,
                "cache {}: {} was restored, and is kept as it was",
                cache.prefix,
                kept.unwrap().key
            );
            continue;
        }
        let key = match kept {
            Some(k) => k.key.clone(),
            None => match key_of(cache, &here.workspace) {
                Ok(key) => key,
                Err(e) => {
                    let _ = writeln!(
                        log,
                        "cache {}: not saved: its key files could not be read: {e}",
                        cache.prefix
                    );
                    continue;
                }
            },
        };
        let saved = (|| -> Result<Option<u64>, String> {
            let file = tempfile().map_err(|e| format!("a temporary file: {e}"))?;
            let packed = pack(&here.paths(cache), &file).map_err(|e| format!("packing it: {e}"))?;
            if packed == 0 {
                return Ok(None);
            }
            let size = file.metadata().map_err(|e| e.to_string())?.len();
            if size > given.max_bytes {
                return Err(format!(
                    "it is {}, more than the {} an entry may be",
                    human(size),
                    human(given.max_bytes)
                ));
            }
            let mut file = file;
            file.rewind().map_err(|e| e.to_string())?;
            put(&agent, &save.archive, file, size)?;
            let manifest = serde_json::json!({
                "key": key,
                "size": size,
                "prefix": cache.prefix,
                "version": cache.version,
                "paths": cache.paths,
            })
            .to_string();
            put(
                &agent,
                &save.manifest,
                manifest.as_bytes(),
                manifest.len() as u64,
            )?;
            Ok(Some(size))
        })();
        match saved {
            Ok(Some(size)) => {
                let _ = writeln!(log, "cache {}: saved {key} ({})", cache.prefix, human(size));
            }
            Ok(None) => {
                let _ = writeln!(
                    log,
                    "cache {}: not saved: none of its paths is there",
                    cache.prefix
                );
            }
            Err(e) => {
                let _ = writeln!(log, "cache {}: {key} not saved: {e}", cache.prefix);
            }
        }
    }
}

/// A temporary file, gone when closed (unlinked as it is made).
fn tempfile() -> std::io::Result<File> {
    let path = std::env::temp_dir().join(format!(
        "dagron-cache-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default()
    ));
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)?;
    let _ = std::fs::remove_file(&path);
    Ok(file)
}

#[cfg(test)]
mod tests;
