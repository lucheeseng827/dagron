//! A job's artifacts, through the engine's artifact service: what a GitLab
//! job's `artifacts:paths` hands on, and what the jobs it waits for handed
//! on to it.
//!
//! - **Before the steps**, each artifact `CI_ARTIFACT_DOWNLOAD` names (one to
//!   a line) is fetched and unpacked into the workspace. One that cannot be
//!   had fails the job, which then runs no step, as GitLab fails a job whose
//!   dependencies' artifacts are gone. One its job never uploaded (it
//!   failed, and uploads on success) is said, and passed over.
//! - **After the steps**, where `CI_ARTIFACT_UPLOAD_WHEN` says so of how
//!   they went, the workspace's files `CI_ARTIFACT_UPLOAD_PATHS` names (and
//!   `CI_ARTIFACT_UPLOAD_EXCLUDE` does not) are packed into a zip and
//!   uploaded as `CI_ARTIFACT_UPLOAD_NAME`. An upload that fails fails the
//!   job, as GitLab's does.
//!
//! The service is the one GitHub's `actions/upload-artifact` speaks to
//! (`ACTIONS_RESULTS_URL`, `ACTIONS_RUNTIME_TOKEN`): an artifact is the
//! run's, by name, and a zip, so that the console and a workflow's
//! `download-artifact` read it as they read any.
//!
//! A path is GitLab's: a file, a directory (and all under it), or a glob
//! (`**/*.log`, a directory it matches taken whole). Paths are of the
//! workspace, and nothing outside it is packed or unpacked.

// ureq's calls return its own Error, which is large; it is matched on where
// it is made, and goes no further than this module.
#![allow(clippy::result_large_err)]

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dagron_ci_expr::glob::Glob;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const SERVICE: &str = "github.actions.results.api.v1.ArtifactService";

/// How much of an upload goes in one block.
const BLOCK: usize = 8 * 1024 * 1024;

/// A file of a zip this writes and reads: one under 4 GiB, as a zip without
/// its 64-bit extension has them.
const MAX_ENTRY: u64 = u32::MAX as u64;

/// What the task's variables ask of artifacts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Asked {
    pub download: Vec<String>,
    pub upload: Option<Upload>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Upload {
    pub name: String,
    pub paths: Vec<String>,
    pub exclude: Vec<String>,
    /// `on_success`, `on_failure` or `always`.
    pub when: String,
    pub expire_days: Option<u64>,
}

impl Upload {
    /// Whether it is uploaded, of a job that exited with `code`.
    pub fn after(&self, code: i32) -> bool {
        match self.when.as_str() {
            "always" => true,
            "on_failure" => code != 0,
            _ => code == 0,
        }
    }
}

fn lines(value: Option<String>) -> Vec<String> {
    value
        .unwrap_or_default()
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect()
}

/// What the environment asks.
pub fn asked() -> Asked {
    let var = |name: &str| std::env::var(name).ok();
    let upload = var("CI_ARTIFACT_UPLOAD_NAME")
        .filter(|n| !n.is_empty())
        .map(|name| Upload {
            name,
            paths: lines(var("CI_ARTIFACT_UPLOAD_PATHS")),
            exclude: lines(var("CI_ARTIFACT_UPLOAD_EXCLUDE")),
            when: var("CI_ARTIFACT_UPLOAD_WHEN").unwrap_or_else(|| "on_success".into()),
            expire_days: var("CI_ARTIFACT_UPLOAD_EXPIRE_DAYS").and_then(|d| d.parse().ok()),
        });
    Asked {
        download: lines(var("CI_ARTIFACT_DOWNLOAD")),
        upload,
    }
}

/// The service, as the task was given it.
pub struct Service {
    base: String,
    bearer: String,
    agent: ureq::Agent,
}

impl Service {
    /// The service at `base`, as `token`.
    pub fn new(base: &str, token: &str) -> Service {
        Service {
            base: if base.ends_with('/') {
                base.to_string()
            } else {
                format!("{base}/")
            },
            bearer: format!("Bearer {token}"),
            agent: ureq::AgentBuilder::new()
                .timeout_connect(Duration::from_secs(15))
                .timeout_read(Duration::from_secs(120))
                .build(),
        }
    }

    pub fn from_env() -> Result<Service, String> {
        let base = std::env::var("ACTIONS_RESULTS_URL")
            .ok()
            .filter(|b| !b.is_empty());
        let token = std::env::var("ACTIONS_RUNTIME_TOKEN")
            .ok()
            .filter(|t| !t.is_empty());
        match (base, token) {
            (Some(base), Some(token)) => Ok(Service::new(&base, &token)),
            _ => Err(
                "the engine gave the job no artifact service (ACTIONS_RESULTS_URL): its operator \
                 sets DAGRON_ACTIONS_CACHE_LISTEN and DAGRON_ACTIONS_CACHE_URL"
                    .into(),
            ),
        }
    }

    fn call(&self, method: &str, body: Value) -> Result<Value, (u16, String)> {
        let url = format!("{}twirp/{SERVICE}/{method}", self.base);
        let text = body.to_string();
        match again(|| {
            self.agent
                .post(&url)
                .set("authorization", &self.bearer)
                .set("content-type", "application/json")
                .send_string(&text)
        }) {
            Ok(response) => {
                let text = response.into_string().map_err(|e| (0, e.to_string()))?;
                let said: Value =
                    serde_json::from_str(&text).map_err(|e| (0, format!("{method}: {e}")))?;
                // An answer that says it did not do what it was asked is no
                // answer to go on.
                if said.get("ok") == Some(&Value::Bool(false)) {
                    let why = said["msg"].as_str().unwrap_or("no reason given");
                    return Err((0, format!("{method}: the engine did not: {why}")));
                }
                Ok(said)
            }
            Err(ureq::Error::Status(code, response)) => {
                let said = response.into_string().unwrap_or_default();
                let why = serde_json::from_str::<Value>(&said)
                    .ok()
                    .and_then(|v| v["msg"].as_str().map(str::to_string))
                    .unwrap_or(said);
                Err((code, format!("{method}: the engine answered {code}: {why}")))
            }
            Err(other) => Err((
                0,
                format!(
                    "{method}: the engine could not be reached: {}",
                    spoken(other)
                ),
            )),
        }
    }

    /// Upload the zip at `zip` as `name`.
    pub fn upload(&self, name: &str, zip: &Path, expire_days: Option<u64>) -> Result<u64, String> {
        let mut create = json!({ "name": name, "version": 4 });
        if let Some(days) = expire_days {
            create["expires_at"] = json!(rfc3339_in(days));
        }
        let created = match self.call("CreateArtifact", create.clone()) {
            Ok(created) => created,
            // Uploaded by an earlier attempt of the same job: this one's
            // stands in its place, as upload-artifact's `overwrite` has it.
            Err((409, _)) => {
                self.call("DeleteArtifact", json!({ "name": name }))
                    .map_err(|(_, why)| why)?;
                self.call("CreateArtifact", create)
                    .map_err(|(_, why)| why)?
            }
            Err((_, why)) => return Err(why),
        };
        let url = created["signed_upload_url"]
            .as_str()
            .ok_or("CreateArtifact: the engine gave no upload URL")?
            .to_string();
        let mut file = File::open(zip).map_err(|e| format!("{}: {e}", zip.display()))?;
        let size = file.metadata().map_err(|e| e.to_string())?.len();
        let mut hasher = Sha256::new();
        let mut ids = Vec::new();
        let mut buf = vec![0u8; BLOCK];
        loop {
            let n = read_full(&mut file, &mut buf).map_err(|e| e.to_string())?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            let id = base64(format!("{:08}", ids.len()).as_bytes());
            let sep = if url.contains('?') { '&' } else { '?' };
            let block = format!("{url}{sep}comp=block&blockid={id}");
            again(|| {
                self.agent
                    .put(&block)
                    .set("x-ms-blob-type", "BlockBlob")
                    .send_bytes(&buf[..n])
            })
            .map_err(|e| format!("uploading the artifact: {}", spoken(e)))?;
            ids.push(id);
        }
        let list: String = ids
            .iter()
            .map(|id| format!("<Latest>{id}</Latest>"))
            .collect();
        let sep = if url.contains('?') { '&' } else { '?' };
        let blocklist = format!("{url}{sep}comp=blocklist");
        let xml =
            format!("<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList>{list}</BlockList>");
        again(|| self.agent.put(&blocklist).send_string(&xml))
            .map_err(|e| format!("uploading the artifact: {}", spoken(e)))?;
        let digest = hex(&hasher.finalize());
        self.call(
            "FinalizeArtifact",
            json!({ "name": name, "size": size.to_string(), "hash": format!("sha256:{digest}") }),
        )
        .map_err(|(_, why)| why)?;
        Ok(size)
    }

    /// Fetch the artifact `name` of the run into `into`, a file, and check
    /// it is the one the service listed. `false` where the run has none of
    /// that name: its job uploaded nothing.
    pub fn download(&self, name: &str, into: &Path) -> Result<bool, String> {
        let listed = self
            .call("ListArtifacts", json!({ "name_filter": name }))
            .map_err(|(_, why)| why)?;
        let Some(found) = listed["artifacts"]
            .as_array()
            .and_then(|all| all.iter().find(|a| a["name"] == name))
        else {
            return Ok(false);
        };
        let digest = found["digest"].as_str().map(str::to_string);
        let signed = self
            .call("GetSignedArtifactURL", json!({ "name": name }))
            .map_err(|(_, why)| why)?;
        let url = signed["signed_url"]
            .as_str()
            .ok_or("GetSignedArtifactURL: the engine gave no URL")?;
        let response = again(|| self.agent.get(url).call())
            .map_err(|e| format!("fetching {name}: {}", spoken(e)))?;
        let mut out = File::create(into).map_err(|e| format!("{}: {e}", into.display()))?;
        let mut reader = response.into_reader();
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = reader
                .read(&mut buf)
                .map_err(|e| format!("fetching {name}: {e}"))?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            out.write_all(&buf[..n]).map_err(|e| e.to_string())?;
        }
        if let Some(digest) = digest {
            let got = format!("sha256:{}", hex(&hasher.finalize()));
            if got != digest {
                return Err(format!(
                    "{name} is not what was uploaded: {got}, not {digest}"
                ));
            }
        }
        Ok(true)
    }
}

/// What went wrong, without the URL: a signed one is a credential, good for
/// as long as it is signed for.
fn spoken(e: ureq::Error) -> String {
    match e {
        ureq::Error::Status(code, response) => {
            let said: String = response
                .into_string()
                .unwrap_or_default()
                .chars()
                .take(500)
                .collect();
            format!("answered {code}: {said}")
        }
        ureq::Error::Transport(t) => format!(
            "could not be reached: {}{}",
            t.kind(),
            t.message().map(|m| format!(": {m}")).unwrap_or_default()
        ),
    }
}

/// `request`, made again where it failed for a moment: a connection that
/// failed or was lost, a 429, a 5xx. Three tries in all, a second and then
/// three apart, as upload-artifact tries again.
fn again<T>(mut request: impl FnMut() -> Result<T, ureq::Error>) -> Result<T, ureq::Error> {
    let mut wait = Duration::from_secs(1);
    for _ in 0..2 {
        match request() {
            Err(ureq::Error::Transport(_)) => {}
            Err(ureq::Error::Status(code, _)) if code == 429 || code >= 500 => {}
            done => return done,
        }
        std::thread::sleep(wait);
        wait *= 3;
    }
    request()
}

fn read_full(file: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match file.read(&mut buf[n..])? {
            0 => break,
            m => n += m,
        }
    }
    Ok(n)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn base64(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(A[(n >> 18) as usize & 63] as char);
        out.push(A[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            A[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            A[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// Now and `days` on, as RFC 3339.
fn rfc3339_in(days: u64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let at = now + days * 86_400;
    let (y, m, d) = civil((at / 86_400) as i64);
    let secs = at % 86_400;
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        secs / 3600,
        secs / 60 % 60,
        secs % 60
    )
}

/// Howard Hinnant's `civil_from_days`.
fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

// ---- which files ------------------------------------------------------------

fn has_glob(path: &str) -> bool {
    path.contains(['*', '?', '['])
}

/// The workspace's regular files `paths` name and `exclude` does not, by
/// their paths under it, in order. A link is passed over: what it points at
/// may be outside the workspace.
pub fn selected(
    workspace: &Path,
    paths: &[String],
    exclude: &[String],
) -> Result<Vec<String>, String> {
    let mut include_globs = Vec::new();
    let mut plain = Vec::new();
    for path in paths {
        let path = path
            .trim_start_matches("./")
            .trim_end_matches('/')
            .to_string();
        if path.is_empty() || path == "." {
            plain.push(String::new());
        } else if has_glob(&path) {
            include_globs.push(Glob::new(&path)?);
        } else {
            plain.push(path);
        }
    }
    let exclude: Vec<Glob> = exclude
        .iter()
        .map(|p| Glob::new(p.trim_start_matches("./").trim_end_matches('/')))
        .collect::<Result<_, _>>()?;
    let mut all = BTreeSet::new();
    walk(workspace, workspace, &mut all)?;
    let wanted = |name: &str| {
        let under = |prefix: &str| {
            prefix.is_empty() || name == prefix || name.starts_with(&format!("{prefix}/"))
        };
        let by_plain = plain.iter().any(|p| under(p));
        // A glob that matches a directory takes all under it.
        let by_glob = include_globs.iter().any(|g| {
            let parts: Vec<&str> = name.split('/').collect();
            (1..=parts.len()).any(|n| g.matches(&parts[..n].join("/")))
        });
        let excluded = exclude.iter().any(|g| {
            let parts: Vec<&str> = name.split('/').collect();
            (1..=parts.len()).any(|n| g.matches(&parts[..n].join("/")))
        });
        (by_plain || by_glob) && !excluded
    };
    Ok(all.into_iter().filter(|name| wanted(name)).collect())
}

fn walk(root: &Path, dir: &Path, out: &mut BTreeSet<String>) -> Result<(), String> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(format!("{}: {e}", dir.display())),
    };
    for entry in entries {
        let entry = entry.map_err(|e| format!("{}: {e}", dir.display()))?;
        let kind = entry.file_type().map_err(|e| e.to_string())?;
        let path = entry.path();
        if kind.is_dir() {
            walk(root, &path, out)?;
        } else if kind.is_file() {
            // A name a zip can carry as it is: UTF-8, with no `\` in it.
            if let Some(rel) = path.strip_prefix(root).ok().and_then(|r| r.to_str()) {
                if !rel.contains('\\') {
                    out.insert(rel.to_string());
                }
            }
        }
    }
    Ok(())
}

// ---- a zip --------------------------------------------------------------------

/// A writer that counts what it has written.
struct Counted<W> {
    inner: W,
    at: u64,
}

impl<W: Write> Write for Counted<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.at += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

struct Entry {
    name: String,
    crc: u32,
    compressed: u32,
    size: u32,
    offset: u32,
    /// Its permissions, as a zip made on Unix keeps them.
    mode: u32,
}

/// The most files a zip without its 64-bit extension holds.
const MAX_ENTRIES: usize = u16::MAX as usize;

/// The most an artifact unpacks to, all its files together.
const MAX_UNPACKED: u64 = 16 * 1024 * 1024 * 1024;

#[cfg(unix)]
fn mode_of(meta: &fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    meta.permissions().mode() & 0o777
}

#[cfg(not(unix))]
fn mode_of(_: &fs::Metadata) -> u32 {
    0o644
}

/// Pack `files` (paths under `workspace`) into a zip at `into`, each
/// deflated, with its permissions.
pub fn pack(workspace: &Path, files: &[String], into: &Path) -> Result<u64, String> {
    if files.len() > MAX_ENTRIES {
        return Err(format!(
            "an artifact is at most {MAX_ENTRIES} files, and its paths name {}",
            files.len()
        ));
    }
    let out = File::create(into).map_err(|e| format!("{}: {e}", into.display()))?;
    let mut w = Counted {
        inner: io::BufWriter::new(out),
        at: 0,
    };
    let mut entries = Vec::new();
    let io = |e: io::Error| e.to_string();
    for name in files {
        let path = workspace.join(name);
        let meta = fs::metadata(&path).map_err(|e| format!("{name}: {e}"))?;
        let len = meta.len();
        let mode = mode_of(&meta);
        if len >= MAX_ENTRY || w.at >= MAX_ENTRY {
            return Err(format!(
                "{name}: an artifact is packed to under 4 GiB, and this is past it"
            ));
        }
        let offset = w.at as u32;
        // Its header, with its sizes and CRC in a descriptor after it.
        w.write_all(&0x0403_4b50u32.to_le_bytes()).map_err(io)?;
        w.write_all(&20u16.to_le_bytes()).map_err(io)?;
        w.write_all(&0x0808u16.to_le_bytes()).map_err(io)?; // descriptor, UTF-8
        w.write_all(&8u16.to_le_bytes()).map_err(io)?; // deflate
        w.write_all(&[0, 0, 0x21, 0]).map_err(io)?; // 1980-01-01 00:00
        w.write_all(&[0u8; 12]).map_err(io)?;
        w.write_all(&(name.len() as u16).to_le_bytes())
            .map_err(io)?;
        w.write_all(&0u16.to_le_bytes()).map_err(io)?;
        w.write_all(name.as_bytes()).map_err(io)?;
        let start = w.at;
        let mut crc = crc32fast::Hasher::new();
        let mut file = File::open(&path).map_err(|e| format!("{name}: {e}"))?;
        let mut size = 0u64;
        {
            let mut deflate =
                flate2::write::DeflateEncoder::new(&mut w, flate2::Compression::default());
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let n = file.read(&mut buf).map_err(|e| format!("{name}: {e}"))?;
                if n == 0 {
                    break;
                }
                crc.update(&buf[..n]);
                size += n as u64;
                deflate.write_all(&buf[..n]).map_err(io)?;
            }
            deflate.finish().map_err(io)?;
        }
        let compressed = w.at - start;
        if compressed >= MAX_ENTRY || size >= MAX_ENTRY {
            return Err(format!(
                "{name}: an artifact is packed to under 4 GiB, and this is past it"
            ));
        }
        let crc = crc.finalize();
        w.write_all(&0x0807_4b50u32.to_le_bytes()).map_err(io)?;
        w.write_all(&crc.to_le_bytes()).map_err(io)?;
        w.write_all(&(compressed as u32).to_le_bytes())
            .map_err(io)?;
        w.write_all(&(size as u32).to_le_bytes()).map_err(io)?;
        entries.push(Entry {
            name: name.clone(),
            crc,
            compressed: compressed as u32,
            size: size as u32,
            offset,
            mode,
        });
    }
    let directory = w.at;
    for e in &entries {
        w.write_all(&0x0201_4b50u32.to_le_bytes()).map_err(io)?;
        w.write_all(&0x031eu16.to_le_bytes()).map_err(io)?; // made by: Unix, 3.0
        w.write_all(&20u16.to_le_bytes()).map_err(io)?;
        w.write_all(&0x0808u16.to_le_bytes()).map_err(io)?;
        w.write_all(&8u16.to_le_bytes()).map_err(io)?;
        w.write_all(&[0, 0, 0x21, 0]).map_err(io)?;
        w.write_all(&e.crc.to_le_bytes()).map_err(io)?;
        w.write_all(&e.compressed.to_le_bytes()).map_err(io)?;
        w.write_all(&e.size.to_le_bytes()).map_err(io)?;
        w.write_all(&(e.name.len() as u16).to_le_bytes())
            .map_err(io)?;
        w.write_all(&[0u8; 8]).map_err(io)?; // extra, comment, disk, internal
        w.write_all(&((0o100000 | e.mode) << 16).to_le_bytes())
            .map_err(io)?;
        w.write_all(&e.offset.to_le_bytes()).map_err(io)?;
        w.write_all(e.name.as_bytes()).map_err(io)?;
    }
    let size = w.at - directory;
    if entries.len() > u16::MAX as usize || w.at >= MAX_ENTRY {
        return Err("an artifact is at most 65535 files and under 4 GiB".into());
    }
    w.write_all(&0x0605_4b50u32.to_le_bytes()).map_err(io)?;
    w.write_all(&[0u8; 4]).map_err(io)?;
    w.write_all(&(entries.len() as u16).to_le_bytes())
        .map_err(io)?;
    w.write_all(&(entries.len() as u16).to_le_bytes())
        .map_err(io)?;
    w.write_all(&(size as u32).to_le_bytes()).map_err(io)?;
    w.write_all(&(directory as u32).to_le_bytes()).map_err(io)?;
    w.write_all(&0u16.to_le_bytes()).map_err(io)?;
    w.flush().map_err(io)?;
    Ok(w.at)
}

fn u16_at(b: &[u8], at: usize) -> Result<u16, String> {
    b.get(at..at + 2)
        .map(|s| u16::from_le_bytes([s[0], s[1]]))
        .ok_or_else(|| "the zip is cut short".to_string())
}

fn u32_at(b: &[u8], at: usize) -> Result<u32, String> {
    b.get(at..at + 4)
        .map(|s| u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
        .ok_or_else(|| "the zip is cut short".to_string())
}

/// A file made anew for an entry, never through a link put where it goes.
fn create(path: &Path) -> io::Result<File> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    options.open(path)
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
}

#[cfg(not(unix))]
fn set_mode(_: &Path, _: u32) -> io::Result<()> {
    Ok(())
}

/// Where an entry's name puts it under `root`, or why it may not be put.
fn inside(root: &Path, name: &str) -> Result<PathBuf, String> {
    let relative = Path::new(name);
    let mut out = root.to_path_buf();
    for part in relative.components() {
        match part {
            Component::Normal(p) => out.push(p),
            Component::CurDir => {}
            _ => return Err(format!("{name:?} is not a path under the workspace")),
        }
    }
    Ok(out)
}

/// Unpack the zip at `zip` into `root`: its files, stored or deflated, each
/// checked against its CRC. No entry goes outside `root`, nor through a
/// link in it.
pub fn unpack(zip: &Path, root: &Path) -> Result<usize, String> {
    let mut file = File::open(zip).map_err(|e| format!("{}: {e}", zip.display()))?;
    let len = file.metadata().map_err(|e| e.to_string())?.len();
    // The end of the central directory: in the last 64 KiB and 22 bytes.
    let tail_len = len.min(65_557) as usize;
    let mut tail = vec![0u8; tail_len];
    file.seek(SeekFrom::Start(len - tail_len as u64))
        .map_err(|e| e.to_string())?;
    file.read_exact(&mut tail).map_err(|e| e.to_string())?;
    let end = (0..tail_len.saturating_sub(21))
        .rev()
        .find(|&i| tail[i..i + 4] == [0x50, 0x4b, 0x05, 0x06])
        .ok_or("is not a zip: it has no central directory")?;
    let count = u16_at(&tail, end + 10)? as usize;
    let dir_size = u32_at(&tail, end + 12)? as u64;
    let dir_at = u32_at(&tail, end + 16)? as u64;
    if dir_at + dir_size > len {
        return Err("the zip's directory is past its end".into());
    }
    // A directory entry is 46 bytes and its name: one larger than every
    // entry's name at its longest is no directory of these entries.
    if dir_size > count as u64 * (46 + 3 * 65_535) {
        return Err("the zip's directory is larger than its entries".into());
    }
    let mut dir = vec![0u8; dir_size as usize];
    file.seek(SeekFrom::Start(dir_at))
        .map_err(|e| e.to_string())?;
    file.read_exact(&mut dir).map_err(|e| e.to_string())?;
    let mut at = 0;
    let mut unpacked = 0;
    let mut total = 0u64;
    let mut offsets = BTreeSet::new();
    for _ in 0..count {
        if u32_at(&dir, at)? != 0x0201_4b50 {
            return Err("the zip's directory is not one".into());
        }
        let made_on = u16_at(&dir, at + 4)? >> 8;
        let method = u16_at(&dir, at + 10)?;
        let crc = u32_at(&dir, at + 16)?;
        let compressed = u32_at(&dir, at + 20)? as u64;
        let size = u32_at(&dir, at + 24)? as u64;
        let name_len = u16_at(&dir, at + 28)? as usize;
        let extra_len = u16_at(&dir, at + 30)? as usize;
        let comment_len = u16_at(&dir, at + 32)? as usize;
        let external = u32_at(&dir, at + 38)?;
        let local = u32_at(&dir, at + 42)? as u64;
        let name = String::from_utf8_lossy(
            dir.get(at + 46..at + 46 + name_len)
                .ok_or("the zip is cut short")?,
        )
        .into_owned();
        at += 46 + name_len + extra_len + comment_len;
        if compressed == u64::from(u32::MAX) || size == u64::from(u32::MAX) {
            return Err(format!("{name}: a zip's 64-bit entries are not read"));
        }
        let target = inside(root, &name)?;
        let is_dir = name.ends_with('/');
        // Each directory on the way (and a directory entry's own) is a
        // directory, and none is a link.
        let mut on_the_way = root.to_path_buf();
        let parent = if is_dir {
            Some(Path::new(name.trim_end_matches('/')))
        } else {
            Path::new(&name).parent()
        };
        if let Some(parent) = parent {
            for part in parent.components() {
                on_the_way.push(part);
                match fs::symlink_metadata(&on_the_way) {
                    Ok(meta) if meta.file_type().is_symlink() => {
                        return Err(format!("{name}: {} is a link", on_the_way.display()))
                    }
                    Ok(meta) if !meta.is_dir() => {
                        return Err(format!("{name}: {} is no directory", on_the_way.display()))
                    }
                    Ok(_) => {}
                    Err(_) => fs::create_dir(&on_the_way).map_err(|e| format!("{name}: {e}"))?,
                }
            }
        }
        if is_dir {
            continue;
        }
        total = total.saturating_add(size);
        if total > MAX_UNPACKED || !offsets.insert(local) {
            return Err(format!(
                "{name}: the zip unpacks to more than an artifact may, or names one file's \
                 bytes twice"
            ));
        }
        if fs::symlink_metadata(&target).is_ok_and(|m| m.file_type().is_symlink()) {
            fs::remove_file(&target).map_err(|e| format!("{name}: {e}"))?;
        }
        let mut header = [0u8; 30];
        file.seek(SeekFrom::Start(local))
            .map_err(|e| e.to_string())?;
        file.read_exact(&mut header).map_err(|e| e.to_string())?;
        if u32_at(&header, 0)? != 0x0403_4b50 {
            return Err(format!("{name}: its local header is not one"));
        }
        let skip = u16_at(&header, 26)? as u64 + u16_at(&header, 28)? as u64;
        file.seek(SeekFrom::Start(local + 30 + skip))
            .map_err(|e| e.to_string())?;
        let data = (&mut file).take(compressed);
        let mut out = create(&target).map_err(|e| format!("{name}: {e}"))?;
        let mut check = crc32fast::Hasher::new();
        let mut written = 0u64;
        let mut copy = |reader: &mut dyn Read| -> Result<(), String> {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let n = reader.read(&mut buf).map_err(|e| format!("{name}: {e}"))?;
                if n == 0 {
                    return Ok(());
                }
                written += n as u64;
                if written > size {
                    return Err(format!("{name} is larger than its zip says"));
                }
                check.update(&buf[..n]);
                out.write_all(&buf[..n])
                    .map_err(|e| format!("{name}: {e}"))?;
            }
        };
        match method {
            0 => copy(&mut { data })?,
            8 => copy(&mut flate2::read::DeflateDecoder::new(data))?,
            other => return Err(format!("{name}: compression {other} is not read")),
        }
        if written != size || check.finalize() != crc {
            return Err(format!(
                "{name} is not what was packed: its size or CRC differs"
            ));
        }
        // Made on Unix: its permissions, as they were. A program built in
        // one job runs in the next.
        if made_on == 3 {
            let mode = (external >> 16) & 0o777;
            if mode != 0 {
                set_mode(&target, mode).map_err(|e| format!("{name}: {e}"))?;
            }
        }
        unpacked += 1;
    }
    Ok(unpacked)
}

// ---- around the steps ---------------------------------------------------------

/// Fetch and unpack each artifact `asked` names, into the workspace. What
/// went wrong, where something did. A job that uploaded none (it failed,
/// and uploads on success) has none to fetch, and is said to have none.
pub fn fetch(asked: &Asked, workspace: &Path, log: &mut impl Write) -> Result<(), String> {
    if asked.download.is_empty() {
        return Ok(());
    }
    let service = Service::from_env()?;
    // A directory of its own, made new and removed when it is dropped: no
    // step can have put a link where the zip goes.
    let dir = tempfile::tempdir().map_err(|e| format!("a directory for the artifacts: {e}"))?;
    for name in &asked.download {
        let zip = dir.path().join("download.zip");
        if !service.download(name, &zip)? {
            let _ = writeln!(
                log,
                "::dagron-ci::warning::artifacts: {name} uploaded none, so none of it is in the \
                 workspace"
            );
            continue;
        }
        let n = unpack(&zip, workspace)?;
        let _ = writeln!(
            log,
            "artifacts: those of {name} are in the workspace ({n} files)"
        );
        let _ = fs::remove_file(&zip);
    }
    Ok(())
}

/// Pack and upload what `upload` names. The files it took, and their size.
pub fn send(upload: &Upload, workspace: &Path, log: &mut impl Write) -> Result<(), String> {
    let files = selected(workspace, &upload.paths, &upload.exclude)?;
    if files.is_empty() {
        let _ = writeln!(
            log,
            "::dagron-ci::warning::artifacts: no file matches {}: nothing is uploaded",
            upload.paths.join(", ")
        );
        return Ok(());
    }
    let service = Service::from_env()?;
    let dir = tempfile::tempdir().map_err(|e| format!("a directory for the artifact: {e}"))?;
    let zip = dir.path().join("artifact.zip");
    pack(workspace, &files, &zip)?;
    let bytes = service.upload(&upload.name, &zip, upload.expire_days)?;
    let _ = writeln!(
        log,
        "artifacts: uploaded as {}: {} files, {bytes} bytes",
        upload.name,
        files.len()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        for (path, text) in [
            ("out/app", "binary"),
            ("out/lib/a.so", "lib"),
            ("logs/one.log", "1"),
            ("logs/deep/two.log", "2"),
            ("src/main.rs", "fn main() {}"),
            ("out/tmp/scratch", "x"),
        ] {
            let p = dir.path().join(path);
            fs::create_dir_all(p.parent().unwrap()).unwrap();
            fs::write(p, text).unwrap();
        }
        dir
    }

    #[test]
    fn paths_are_files_directories_and_globs_less_what_is_excluded() {
        let dir = tree();
        let got = selected(
            dir.path(),
            &["out/".into(), "**/*.log".into()],
            &["out/tmp".into()],
        )
        .unwrap();
        assert_eq!(
            got,
            [
                "logs/deep/two.log",
                "logs/one.log",
                "out/app",
                "out/lib/a.so"
            ]
        );
        assert!(selected(dir.path(), &["nothing/".into()], &[])
            .unwrap()
            .is_empty());
    }

    #[test]
    fn a_packed_zip_unpacks_to_the_same_files() {
        let dir = tree();
        let files = selected(dir.path(), &[".".into()], &[]).unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        let zip = elsewhere.path().join("packed.zip");
        #[cfg(unix)]
        set_mode(&dir.path().join("out/app"), 0o755).unwrap();
        pack(dir.path(), &files, &zip).unwrap();
        let bytes = fs::read(&zip).unwrap();
        assert_eq!(&bytes[..2], b"PK");
        let into = tempfile::tempdir().unwrap();
        assert_eq!(unpack(&zip, into.path()).unwrap(), files.len());
        for name in &files {
            assert_eq!(
                fs::read(dir.path().join(name)).unwrap(),
                fs::read(into.path().join(name)).unwrap(),
                "{name}"
            );
        }
        // A program stays one.
        #[cfg(unix)]
        {
            let mode = |p: &Path| mode_of(&fs::metadata(p).unwrap());
            assert_eq!(mode(&into.path().join("out/app")), 0o755);
            assert_eq!(mode(&into.path().join("src/main.rs")) & 0o111, 0);
        }
    }

    /// A directory entry is made as a file's directories are: never
    /// through a link the workspace has.
    #[cfg(unix)]
    #[test]
    fn a_directory_entry_is_not_made_through_a_link() {
        let out = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(out.path(), root.path().join("out")).unwrap();
        // A zip of one directory entry, `out/x/`, made by hand.
        let name = b"out/x/";
        let mut zip = Vec::new();
        zip.extend_from_slice(&0x0403_4b50u32.to_le_bytes());
        zip.extend_from_slice(&[20, 0, 0, 0, 0, 0, 0, 0, 0x21, 0]);
        zip.extend_from_slice(&[0u8; 12]);
        zip.extend_from_slice(&(name.len() as u16).to_le_bytes());
        zip.extend_from_slice(&[0, 0]);
        zip.extend_from_slice(name);
        let dir_at = zip.len() as u32;
        zip.extend_from_slice(&0x0201_4b50u32.to_le_bytes());
        zip.extend_from_slice(&[0x1e, 3, 20, 0, 0, 0, 0, 0, 0, 0, 0x21, 0]);
        zip.extend_from_slice(&[0u8; 12]);
        zip.extend_from_slice(&(name.len() as u16).to_le_bytes());
        zip.extend_from_slice(&[0u8; 8]);
        zip.extend_from_slice(&(0o40755u32 << 16).to_le_bytes());
        zip.extend_from_slice(&0u32.to_le_bytes());
        zip.extend_from_slice(name);
        let dir_size = zip.len() as u32 - dir_at;
        zip.extend_from_slice(&0x0605_4b50u32.to_le_bytes());
        zip.extend_from_slice(&[0, 0, 0, 0, 1, 0, 1, 0]);
        zip.extend_from_slice(&dir_size.to_le_bytes());
        zip.extend_from_slice(&dir_at.to_le_bytes());
        zip.extend_from_slice(&[0, 0]);
        let path = root.path().join("dir.zip");
        fs::write(&path, &zip).unwrap();
        let e = unpack(&path, root.path()).unwrap_err();
        assert!(e.contains("is a link"), "{e}");
        assert!(!out.path().join("x").exists(), "nothing made outside");
    }

    #[test]
    fn nothing_is_unpacked_outside_the_root() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("x"), "evil").unwrap();
        let zip = dir.path().join("evil.zip");
        // A zip of one entry, renamed to climb out once packed.
        pack(dir.path(), &["x".to_string()], &zip).unwrap();
        let mut bytes = fs::read(&zip).unwrap();
        let replace = |bytes: &mut Vec<u8>| {
            // Both copies of the one-byte name become `/`: an absolute path.
            for i in 0..bytes.len() {
                if bytes[i] == b'x' && i > 0 && bytes[i - 1] == 0 {
                    bytes[i] = b'/';
                }
            }
        };
        replace(&mut bytes);
        fs::write(&zip, &bytes).unwrap();
        let into = tempfile::tempdir().unwrap();
        assert!(unpack(&zip, into.path()).is_err());
        assert!(inside(into.path(), "../escape").is_err());
        assert!(inside(into.path(), "/etc/passwd").is_err());
        assert!(inside(into.path(), "a/./b").is_ok());
    }

    #[test]
    fn an_upload_is_made_as_its_job_went() {
        let up = |when: &str| Upload {
            name: "build".into(),
            paths: vec!["out/".into()],
            exclude: Vec::new(),
            when: when.into(),
            expire_days: None,
        };
        assert!(up("on_success").after(0) && !up("on_success").after(1));
        assert!(up("on_failure").after(2) && !up("on_failure").after(0));
        assert!(up("always").after(0) && up("always").after(1));
        assert_eq!(base64(b"00000000"), "MDAwMDAwMDA=");
        assert_eq!(civil(0), (1970, 1, 1));
        assert!(rfc3339_in(1).ends_with('Z'));
    }
}
