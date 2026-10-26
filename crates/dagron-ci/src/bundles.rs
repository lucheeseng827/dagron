//! Patch bundles: the run API's way to run a pipeline on a
//! change nobody pushed, a coding agent's work in progress or a developer's
//! before a push.
//!
//! A bundle is a tar of the files a change touched since a base commit of
//! the repository's, as they are in the change, and, at its top, a
//! `.dagron-deleted` naming the files it deletes, one a line. A plain tar or
//! one compressed with zstd. Only files and directories, at paths inside the
//! tree: no links, no `..`, nothing under `.git`.
//!
//! - `POST /ci/v1/bundles?repo=HOST/OWNER/NAME&base=SHA`, the tar as the body
//!   (up to [`MAX_INLINE`]): `201` `{id, repo, base, size_bytes, sha256,
//!   expires_at}`. Or, with a JSON body `{repo, base, size_bytes}`, for one
//!   up to [`MAX_BUNDLE`]: an upload link the store signs, where it signs
//!   them, the bundle checked when a run first names it.
//! - `POST /ci/v1/runs` with `bundle`: a run of the repository's pipelines on
//!   the base, with the bundle laid over it.
//!
//! A bundle is kept in CI_OBJECT_STORE_URL for [`KEPT_DAYS`] and is its
//! uploader's alone: only the caller who uploaded it may run it, on its own
//! repository. A run on one is untrusted (an agent's is an agent's), as a
//! change no one reviewed is, and the forge is told nothing of it: its base
//! is a commit the change is not. Its pipelines are the change's where it
//! changes them (`.dagron/ci`, a workflow file), else the base's.
//!
//! A job's checkout fetches the base and lays the patch over it, from a link
//! of an hour that the checkout credential door hands each attempt
//! (`POST /ci/v1/checkout-tokens` with `patch`), served here at
//! [`CONTENT_PATH`] on the URL jobs reach dagron-ci at (CI_URL_FOR_JOBS).

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::problem::problem;
use crate::server::AppState;

/// The largest bundle sent as a request's body.
pub const MAX_INLINE: usize = 32 * 1024 * 1024;
/// The largest bundle, uploaded through a link the store signs.
pub const MAX_BUNDLE: u64 = 256 * 1024 * 1024;
/// The most entries a bundle holds.
pub const MAX_ENTRIES: usize = 20_000;
/// How long a bundle is kept, used or not.
pub const KEPT_DAYS: i64 = 7;
/// The file at a bundle's top that names the files it deletes.
pub const DELETED: &str = ".dagron-deleted";
/// The most a zstd bundle expands to, its tar read as it is decompressed.
pub const MAX_EXPANDED: u64 = 4 * MAX_BUNDLE;
/// The most of a bundle's files a dispatch keeps in memory: what a
/// pipeline, a workflow or a GitLab include can be ([`kept`]), and its
/// `.dagron-deleted`, together.
pub const MAX_KEPT: u64 = 16 * 1024 * 1024;
/// How long a link to a bundle's content holds: an hour, as the checkout's
/// credential from the same door does, since a pod may wait that long to be
/// scheduled and its images pulled before its checkout starts.
pub const LINK_SECS: u64 = 3600;
/// How many bundles are sent to checkouts at once: each is held in memory,
/// compressed, while it is sent, so at most this many of MAX_BUNDLE.
pub const SENT_AT_ONCE: usize = 4;
/// How long a checkout's request waits for one of those before it is
/// answered 503, to ask again (the checkout does, a few times).
const SEND_WAIT: Duration = Duration::from_secs(30);
/// Where a bundle's content is served, `{id}` its id.
pub const CONTENT_PATH: &str = "/ci/v1/bundles/{id}/content";
/// The files of a bundle a dispatch reads, kept in memory as it reads the
/// pipelines: what a pipeline, a workflow or a GitLab include can be.
fn kept(path: &str) -> bool {
    path.starts_with(".dagron/")
        || path.starts_with(".github/")
        || path.ends_with(".yml")
        || path.ends_with(".yaml")
}

/// What a bundle changes, as a dispatch reads it: the files it keeps
/// ([`kept`]) and the paths it deletes.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Patch {
    files: BTreeMap<String, Vec<u8>>,
    deleted: BTreeSet<String>,
}

impl Patch {
    /// What the bundle says of `path`: `Some(Some(bytes))` where it changes
    /// it, `Some(None)` where it deletes it, `None` where it leaves it be.
    pub fn file(&self, path: &str) -> Option<Option<&[u8]>> {
        if let Some(bytes) = self.files.get(path) {
            return Some(Some(bytes));
        }
        let gone = self.deleted.contains(path)
            || self
                .deleted
                .iter()
                .any(|d| path.starts_with(d.as_str()) && path[d.len()..].starts_with('/'));
        gone.then_some(None)
    }

    /// The files it changes directly under `dir` (`.dagron/ci`).
    pub fn in_dir<'a>(&'a self, dir: &'a str) -> impl Iterator<Item = (&'a str, &'a [u8])> + 'a {
        self.files.iter().filter_map(move |(path, bytes)| {
            let rest = path.strip_prefix(dir)?.strip_prefix('/')?;
            (!rest.contains('/')).then_some((path.as_str(), bytes.as_slice()))
        })
    }
}

/// A path a bundle may name: relative, inside the tree, none of `.git`.
fn safe_path(path: &str) -> Result<String, String> {
    let path = path.trim_end_matches('/');
    let path = path.strip_prefix("./").unwrap_or(path);
    if path.is_empty()
        || path.starts_with('/')
        || path.contains('\\')
        || path.contains('\0')
        || path.chars().any(char::is_control)
    {
        return Err(format!("{path:?} is not a path inside the tree"));
    }
    for part in path.split('/') {
        if part.is_empty() || part == "." || part == ".." {
            return Err(format!("{path:?} is not a path inside the tree"));
        }
    }
    if path == ".git" || path.starts_with(".git/") {
        return Err(format!(
            "{path:?} is git's own: a bundle changes the tree, not its history"
        ));
    }
    Ok(path.to_string())
}

/// A reader that fails once more than `left` bytes come through it, rather
/// than ending as though the input did.
struct Capped<R> {
    inner: R,
    left: u64,
}

impl<R: Read> Read for Capped<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.left = self.left.checked_sub(n as u64).ok_or_else(|| {
            std::io::Error::other(format!(
                "the bundle expands to more than {MAX_EXPANDED} bytes"
            ))
        })?;
        Ok(n)
    }
}

/// The plain tar of `bytes`, read as it is decompressed: a tar as it is, or
/// a zstd one, which may expand to [`MAX_EXPANDED`] and no more.
fn tar_of(bytes: &[u8]) -> Result<Box<dyn Read + Send + '_>, String> {
    if bytes.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        let decoder = zstd::stream::read::Decoder::new(bytes)
            .map_err(|e| format!("the bundle is not zstd it can read: {e}"))?;
        Ok(Box::new(Capped {
            inner: decoder,
            left: MAX_EXPANDED,
        }))
    } else {
        Ok(Box::new(bytes))
    }
}

/// Read a bundle (plain or zstd), checking every entry: what it changes, the
/// files a dispatch reads kept. `Err` says why it is no bundle.
pub fn read(bytes: &[u8]) -> Result<Patch, String> {
    let mut archive = tar::Archive::new(tar_of(bytes)?);
    let mut patch = Patch::default();
    let mut entries = 0usize;
    // What is kept in memory, against MAX_KEPT, before it is read.
    let mut kept_bytes = 0u64;
    let mut deleted_list = None;
    for entry in archive
        .entries()
        .map_err(|e| format!("the bundle is not a tar: {e}"))?
    {
        let mut entry = entry.map_err(|e| format!("the bundle is not a tar: {e}"))?;
        entries += 1;
        if entries > MAX_ENTRIES {
            return Err(format!("the bundle holds more than {MAX_ENTRIES} entries"));
        }
        let raw = entry
            .path()
            .map_err(|e| format!("an entry's path cannot be read: {e}"))?
            .to_string_lossy()
            .to_string();
        let kind = entry.header().entry_type();
        let path = safe_path(&raw)?;
        if kind.is_dir() {
            continue;
        }
        if !kind.is_file() {
            return Err(format!(
                "{path:?} is not a file: a bundle carries files and directories, not links or devices"
            ));
        }
        if path == DELETED || kept(&path) {
            kept_bytes = kept_bytes.saturating_add(entry.size());
            if kept_bytes > MAX_KEPT {
                return Err(format!(
                    "the bundle's pipelines, workflows and {DELETED} come to more than {MAX_KEPT} bytes"
                ));
            }
        }
        if path == DELETED {
            let mut text = String::new();
            entry
                .read_to_string(&mut text)
                .map_err(|e| format!("{DELETED} is not text: {e}"))?;
            deleted_list = Some(text);
            continue;
        }
        if kept(&path) {
            let mut bytes = Vec::new();
            entry
                .read_to_end(&mut bytes)
                .map_err(|e| format!("{path:?} cannot be read: {e}"))?;
            patch.files.insert(path, bytes);
        }
    }
    if let Some(text) = deleted_list {
        // A path a line, as it is: a name may end in a space, and a `\r`
        // is refused below rather than taken off.
        for line in text.split('\n').filter(|l| !l.is_empty()) {
            let path = safe_path(line).map_err(|why| format!("{DELETED}: {why}"))?;
            if patch.files.contains_key(&path) {
                return Err(format!(
                    "{DELETED} deletes {path:?}, which the bundle also carries"
                ));
            }
            patch.deleted.insert(path);
        }
    }
    Ok(patch)
}

/// Signs and checks the links to a bundle's content, with a key made from
/// what no job holds: the engines' secret for the checkout credential door,
/// as the GitHub API door's links are.
pub struct Links {
    /// Where jobs reach dagron-ci (CI_URL_FOR_JOBS), without a trailing slash.
    public: String,
    key: [u8; 32],
}

impl Links {
    pub fn new(public: &str, secret: &[u8]) -> Links {
        let mut digest = Sha256::new();
        digest.update(b"dagron-ci bundle links\0");
        digest.update(secret);
        Links {
            public: public.trim_end_matches('/').to_string(),
            key: digest.finalize().into(),
        }
    }

    fn mac(&self, id: &str, expires: u64) -> Hmac<Sha256> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key).expect("any key length");
        mac.update(format!("{id}\n{expires}").as_bytes());
        mac
    }

    /// A link to bundle `id`'s content, good for [`LINK_SECS`].
    pub fn link(&self, id: &str) -> String {
        let expires = now() + LINK_SECS;
        let sig = hex::encode(self.mac(id, expires).finalize().into_bytes());
        format!(
            "{}{}?expires={expires}&sig={sig}",
            self.public,
            CONTENT_PATH.replace("{id}", id)
        )
    }

    fn verified(&self, id: &str, expires: u64, sig: &str) -> bool {
        expires >= now()
            && hex::decode(sig).is_ok_and(|sig| self.mac(id, expires).verify_slice(&sig).is_ok())
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

/// A bundle id as dagron-ci makes them, and nothing that could change a key.
pub fn is_bundle_id(id: &str) -> bool {
    uuid::Uuid::parse_str(id).is_ok() && id.len() == 36
}

#[derive(Debug, Deserialize)]
pub struct UploadQuery {
    repo: Option<String>,
    base: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Announced {
    repo: String,
    base: String,
    size_bytes: u64,
}

/// `POST /ci/v1/bundles`.
pub async fn upload(
    State(st): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<UploadQuery>,
    body: Bytes,
) -> Response {
    let Some(store) = st.payloads.clone().filter(|_| st.bundle_links.is_some()) else {
        return problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "bundles are not taken here",
            "this dagron-ci keeps no bundles: it needs CI_OBJECT_STORE_URL, CI_URL_FOR_JOBS and \
             the checkout credential door's secret (CI_CHECKOUT_TOKEN_SECRET_FILE)"
                .to_string(),
        );
    };
    let json = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/json"));
    let (repo, base, announced) = if json {
        match serde_json::from_slice::<Announced>(&body) {
            Ok(a) => (a.repo, a.base, Some(a.size_bytes)),
            Err(e) => {
                return problem(
                    StatusCode::BAD_REQUEST,
                    "unreadable request",
                    format!("the body is not {{repo, base, size_bytes}}: {e}"),
                )
            }
        }
    } else {
        match (query.repo, query.base) {
            (Some(repo), Some(base)) => (repo, base, None),
            _ => {
                return problem(
                    StatusCode::BAD_REQUEST,
                    "invalid bundle",
                    "name the repository and the base commit: ?repo=HOST/OWNER/NAME&base=SHA"
                        .to_string(),
                )
            }
        }
    };
    if !crate::event::is_commit_id(&base) {
        return problem(
            StatusCode::BAD_REQUEST,
            "invalid bundle",
            format!("{base:?} is not a commit's full id"),
        );
    }
    let (caller, repo) = match crate::api::caller_of(&st, &headers, &repo).await {
        Ok(found) => found,
        Err(response) => return response,
    };
    let id = uuid::Uuid::now_v7().to_string();
    let name = crate::payloads::bundle_name(&id);
    let row = crate::db::Bundle {
        id: id.clone(),
        repo_id: repo.id.clone(),
        base_sha: base.clone(),
        principal: caller.clone(),
        object_name: name.clone(),
        size_bytes: None,
        sha256: None,
        ready: false,
        expires_at: String::new(),
    };
    if let Some(size) = announced {
        if size > MAX_BUNDLE {
            return problem(
                StatusCode::PAYLOAD_TOO_LARGE,
                "bundle too large",
                format!("a bundle is at most {MAX_BUNDLE} bytes"),
            );
        }
        let link = match store
            .presign_bundle_put(&name, Duration::from_secs(15 * 60))
            .await
        {
            Ok(Some(link)) => link,
            Ok(None) => {
                return problem(
                    StatusCode::CONFLICT,
                    "no upload link",
                    format!(
                        "CI_OBJECT_STORE_URL signs no upload links: send the bundle as the body, \
                         up to {MAX_INLINE} bytes"
                    ),
                )
            }
            Err(e) => {
                return problem(
                    StatusCode::BAD_GATEWAY,
                    "the store failed",
                    format!("{e:#}"),
                )
            }
        };
        let expires_at = match st.db.insert_bundle(&row, KEPT_DAYS).await {
            Ok(at) => at,
            Err(e) => {
                return problem(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "the database failed",
                    e.to_string(),
                )
            }
        };
        tracing::info!(bundle = %id, repo = %repo.full_name, %base, size, "a bundle's upload link given");
        return (
            StatusCode::CREATED,
            axum::Json(serde_json::json!({
                "id": id,
                "repo": format!("{}/{}", repo.forge_host, repo.full_name),
                "base": base,
                "expires_at": expires_at,
                "upload": { "method": link.method, "url": link.url, "headers": link.headers },
            })),
        )
            .into_response();
    }
    if let Err(why) = read(&body) {
        return problem(StatusCode::UNPROCESSABLE_ENTITY, "invalid bundle", why);
    }
    let stored = match compressed(&body) {
        Ok(stored) => stored,
        Err(why) => return problem(StatusCode::UNPROCESSABLE_ENTITY, "invalid bundle", why),
    };
    let sha256 = hex::encode(Sha256::digest(&body));
    if let Err(e) = store.put_bundle(&name, &stored).await {
        return problem(
            StatusCode::BAD_GATEWAY,
            "the store failed",
            format!("{e:#}"),
        );
    }
    let row = crate::db::Bundle {
        size_bytes: Some(body.len() as i64),
        sha256: Some(sha256.clone()),
        ready: true,
        ..row
    };
    let expires_at = match st.db.insert_bundle(&row, KEPT_DAYS).await {
        Ok(at) => at,
        Err(e) => {
            let _ = store.delete_bundle(&name).await;
            return problem(
                StatusCode::SERVICE_UNAVAILABLE,
                "the database failed",
                e.to_string(),
            );
        }
    };
    tracing::info!(bundle = %id, repo = %repo.full_name, %base, size = body.len(), "a bundle is kept");
    (
        StatusCode::CREATED,
        axum::Json(serde_json::json!({
            "id": id,
            "repo": format!("{}/{}", repo.forge_host, repo.full_name),
            "base": base,
            "size_bytes": body.len(),
            "sha256": sha256,
            "expires_at": expires_at,
        })),
    )
        .into_response()
}

/// A bundle as the store keeps it: compressed with zstd.
fn compressed(bytes: &[u8]) -> Result<Vec<u8>, String> {
    if bytes.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        return Ok(bytes.to_vec());
    }
    zstd::stream::encode_all(bytes, 3).map_err(|e| format!("the bundle could not be kept: {e}"))
}

/// Check a bundle uploaded through a link, as a run first names it: its
/// bytes are what a bundle is, and its row is then ready. `Err` says why
/// not, and the bundle is then gone.
pub async fn made_ready(st: &AppState, bundle: &crate::db::Bundle) -> Result<(), String> {
    let store = st
        .payloads
        .as_ref()
        .ok_or("this dagron-ci keeps no bundles")?;
    let bytes = store
        .get_bundle(&bundle.object_name)
        .await
        .map_err(|e| format!("the bundle was never uploaded, or cannot be read: {e:#}"))?;
    if bytes.len() as u64 > MAX_BUNDLE {
        let _ = store.delete_bundle(&bundle.object_name).await;
        return Err(format!("the bundle is over {MAX_BUNDLE} bytes"));
    }
    if let Err(why) = read(&bytes) {
        let _ = store.delete_bundle(&bundle.object_name).await;
        let _ = st.db.delete_bundle(&bundle.id).await;
        return Err(why);
    }
    let sha256 = hex::encode(Sha256::digest(&bytes));
    st.db
        .mark_bundle_ready(&bundle.id, bytes.len() as i64, &sha256)
        .await
        .map_err(|e| format!("the database could not be written: {e}"))
}

#[derive(Debug, Deserialize)]
pub struct ContentQuery {
    expires: u64,
    sig: String,
}

/// `GET /ci/v1/bundles/{id}/content?expires=&sig=`: the bundle as a plain
/// tar, for a link the checkout credential door gave.
pub async fn content(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<ContentQuery>,
) -> Response {
    let (Some(links), Some(store)) = (st.bundle_links.as_ref(), st.payloads.as_ref()) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if !is_bundle_id(&id) || !links.verified(&id, q.expires, &q.sig) {
        return problem(
            StatusCode::FORBIDDEN,
            "Forbidden",
            "the link is not one dagron-ci gave, or it has expired".to_string(),
        );
    }
    let bundle = match st.db.bundle(&id).await {
        Ok(Some(b)) if b.ready => b,
        Ok(_) => {
            return problem(
                StatusCode::GONE,
                "Gone",
                format!("bundle {id} is no longer kept"),
            )
        }
        Err(e) => {
            return problem(
                StatusCode::SERVICE_UNAVAILABLE,
                "the database failed",
                e.to_string(),
            )
        }
    };
    let permit = match send_permit(&st.bundle_sends, SEND_WAIT).await {
        Ok(permit) => permit,
        Err(busy) => return busy,
    };
    let bytes = match store.get_bundle(&bundle.object_name).await {
        Ok(bytes) => bytes,
        Err(e) => return problem(StatusCode::GONE, "Gone", format!("{e:#}")),
    };
    // Decompressed as it is sent, a chunk at a time: the tar is never whole
    // in memory, however far it expands.
    let (tx, rx) = tokio::sync::mpsc::channel::<std::io::Result<Bytes>>(4);
    tokio::task::spawn_blocking(move || {
        // Held until the bundle is sent, or the checkout goes away.
        let _permit = permit;
        let mut tar = match tar_of(&bytes) {
            Ok(tar) => tar,
            Err(why) => {
                let _ = tx.blocking_send(Err(std::io::Error::other(why)));
                return;
            }
        };
        let mut chunk = vec![0u8; 64 * 1024];
        loop {
            match tar.read(&mut chunk) {
                Ok(0) => return,
                Ok(n) => {
                    if tx
                        .blocking_send(Ok(Bytes::copy_from_slice(&chunk[..n])))
                        .is_err()
                    {
                        return;
                    }
                }
                Err(e) => {
                    let _ = tx.blocking_send(Err(e));
                    return;
                }
            }
        }
    });
    let body = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|chunk| (chunk, rx))
    });
    (
        [(axum::http::header::CONTENT_TYPE, "application/x-tar")],
        axum::body::Body::from_stream(body),
    )
        .into_response()
}

/// A place among the bundles sent at once, waited for up to `wait`; else a
/// 503 that says when to ask again.
async fn send_permit(
    sends: &Arc<tokio::sync::Semaphore>,
    wait: Duration,
) -> Result<tokio::sync::OwnedSemaphorePermit, Response> {
    match tokio::time::timeout(wait, sends.clone().acquire_owned()).await {
        Ok(Ok(permit)) => Ok(permit),
        _ => {
            let mut busy = problem(
                StatusCode::SERVICE_UNAVAILABLE,
                "Busy",
                format!("{SENT_AT_ONCE} bundles are being sent already: ask again"),
            );
            busy.headers_mut().insert(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("5"),
            );
            Err(busy)
        }
    }
}

/// Delete the bundles past [`KEPT_DAYS`]: their objects first, then their
/// rows. The hourly retention pass makes it, whether or not a bundle was
/// uploaded since.
pub async fn sweep(db: &crate::db::Db, store: &crate::payloads::Payloads) {
    match db.expired_bundles(500).await {
        Ok(gone) => {
            for (id, name) in gone {
                if let Err(e) = store.delete_bundle(&name).await {
                    tracing::warn!(bundle = %id, error = %format!("{e:#}"), "an expired bundle could not be deleted");
                    continue;
                }
                if let Err(e) = db.delete_bundle(&id).await {
                    tracing::warn!(bundle = %id, error = %e, "an expired bundle's row could not be deleted");
                }
            }
        }
        Err(e) => tracing::warn!(error = %e, "the expired bundles could not be read"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tar_of_entries(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut b = tar::Builder::new(Vec::new());
        for (path, bytes) in entries {
            let mut h = tar::Header::new_gnu();
            h.set_size(bytes.len() as u64);
            h.set_mode(0o644);
            h.set_cksum();
            b.append_data(&mut h, path, *bytes).unwrap();
        }
        b.into_inner().unwrap()
    }

    #[test]
    fn a_bundle_says_what_it_changes_and_deletes() {
        let tar = tar_of_entries(&[
            (".dagron/ci/ci.yaml", b"name: ci\n"),
            ("src/lib.rs", b"pub fn x() {}\n"),
            (DELETED, b"docs/old.md\nold\n"),
        ]);
        for bytes in [
            tar.clone(),
            zstd::stream::encode_all(tar.as_slice(), 3).unwrap(),
        ] {
            let patch = read(&bytes).unwrap();
            assert_eq!(
                patch.file(".dagron/ci/ci.yaml"),
                Some(Some(&b"name: ci\n"[..]))
            );
            assert_eq!(patch.file("docs/old.md"), Some(None));
            assert_eq!(
                patch.file("old/inner.yaml"),
                Some(None),
                "under a deleted directory"
            );
            assert_eq!(
                patch.file("src/lib.rs"),
                None,
                "not kept: no dispatch reads it"
            );
            assert_eq!(patch.file("README.md"), None);
            assert_eq!(patch.in_dir(".dagron/ci").count(), 1);
        }
    }

    #[test]
    fn a_bundle_names_nothing_outside_the_tree() {
        for bad in ["../x", "/etc/passwd", ".git/config", "a/../../b", "a//b"] {
            let mut b = tar::Builder::new(Vec::new());
            let mut h = tar::Header::new_gnu();
            h.set_size(1);
            h.set_mode(0o644);
            // Written around the builder's own check, as a hostile tar is.
            let name = h.as_old_mut().name.as_mut();
            name[..bad.len()].copy_from_slice(bad.as_bytes());
            h.set_cksum();
            b.append(&h, &b"x"[..]).unwrap();
            let tar = b.into_inner().unwrap();
            assert!(read(&tar).is_err(), "{bad} was taken");
        }
        let tar = tar_of_entries(&[(DELETED, b"../outside\n")]);
        assert!(read(&tar).unwrap_err().contains(DELETED));
        // A name is read as it is: one ending in a space is that file, and
        // not another; a `\r` in one is refused, not taken off.
        let tar = tar_of_entries(&[(DELETED, b"notes \n")]);
        assert!(read(&tar).unwrap().deleted.contains("notes "));
        let tar = tar_of_entries(&[(DELETED, b"notes\r\n")]);
        assert!(read(&tar).unwrap_err().contains(DELETED));
        assert!(read(b"not a tar at all, but long enough to be read as one's header?").is_err());
    }

    #[test]
    fn a_bundle_keeps_its_pipelines_in_memory_up_to_a_bound() {
        let big = vec![b'#'; MAX_KEPT as usize + 1];
        let tar = tar_of_entries(&[(".dagron/ci/huge.yaml", &big)]);
        let zstd = zstd::stream::encode_all(tar.as_slice(), 3).unwrap();
        assert!(
            read(&zstd).unwrap_err().contains("more than"),
            "a pipeline past MAX_KEPT was kept"
        );
        // A file no dispatch reads is checked, and not kept.
        let tar = tar_of_entries(&[("data/huge.bin", &big)]);
        assert!(read(&tar).unwrap().files.is_empty());
    }

    #[test]
    fn a_bundle_expands_no_further_than_its_bound() {
        let mut capped = Capped {
            inner: &[0u8; 100][..],
            left: 10,
        };
        let mut out = Vec::new();
        let err = capped.read_to_end(&mut out).unwrap_err();
        assert!(err.to_string().contains("expands to more than"), "{err}");
        let mut whole = Capped {
            inner: &[0u8; 10][..],
            left: 10,
        };
        out.clear();
        assert_eq!(whole.read_to_end(&mut out).unwrap(), 10);
    }

    #[tokio::test]
    async fn a_bundle_waits_for_its_place_among_those_sent_and_is_told_to_ask_again() {
        let sends = Arc::new(tokio::sync::Semaphore::new(1));
        let held = send_permit(&sends, Duration::from_millis(10))
            .await
            .unwrap();
        let busy = send_permit(&sends, Duration::from_millis(10))
            .await
            .unwrap_err();
        assert_eq!(busy.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(busy.headers()[axum::http::header::RETRY_AFTER], "5");
        drop(held);
        assert!(send_permit(&sends, Duration::from_millis(10)).await.is_ok());
    }

    #[tokio::test]
    async fn a_bundle_past_its_time_is_swept_with_its_object() {
        let (db, _dir) = crate::db::tests::sqlite().await;
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let store_dir = tempfile::tempdir().unwrap();
        let store =
            crate::payloads::Payloads::from_url(&format!("file://{}", store_dir.path().display()))
                .unwrap();
        let row = |id: &str| crate::db::Bundle {
            id: id.to_string(),
            repo_id: repo.id.clone(),
            base_sha: "4b4708e725f29891d22b4c96af9f0a403ff51c7c".into(),
            principal: "u-1".into(),
            object_name: crate::payloads::bundle_name(id),
            size_bytes: Some(3),
            sha256: None,
            ready: true,
            expires_at: String::new(),
        };
        let (old, new) = (
            uuid::Uuid::now_v7().to_string(),
            uuid::Uuid::now_v7().to_string(),
        );
        for id in [&old, &new] {
            store
                .put_bundle(&crate::payloads::bundle_name(id), b"tar")
                .await
                .unwrap();
            db.insert_bundle(&row(id), KEPT_DAYS).await.unwrap();
        }
        crate::db::tests::exec(
            &db,
            &format!(
                "UPDATE ci_bundles SET expires_at = '2000-01-01T00:00:00Z' WHERE id = '{old}'"
            ),
        )
        .await;
        sweep(&db, &store).await;
        assert!(
            db.bundle(&old).await.unwrap().is_none(),
            "the old bundle's row is gone"
        );
        assert!(store
            .get_bundle(&crate::payloads::bundle_name(&old))
            .await
            .is_err());
        assert!(
            db.bundle(&new).await.unwrap().is_some(),
            "a bundle in its time stays"
        );
        assert!(store
            .get_bundle(&crate::payloads::bundle_name(&new))
            .await
            .is_ok());
    }

    #[test]
    fn a_bundle_carries_no_links() {
        let mut b = tar::Builder::new(Vec::new());
        let mut h = tar::Header::new_gnu();
        h.set_entry_type(tar::EntryType::Symlink);
        h.set_size(0);
        b.append_link(&mut h, "evil", "/etc/passwd").unwrap();
        let tar = b.into_inner().unwrap();
        assert!(read(&tar).unwrap_err().contains("not a file"));
    }

    #[test]
    fn a_link_holds_for_its_bundle_and_its_time_alone() {
        let links = Links::new("http://ci.example:8090/", b"secret");
        let id = uuid::Uuid::now_v7().to_string();
        let link = links.link(&id);
        assert!(link.starts_with(&format!(
            "http://ci.example:8090/ci/v1/bundles/{id}/content?"
        )));
        let url = reqwest::Url::parse(&link).unwrap();
        let q: BTreeMap<String, String> = url.query_pairs().into_owned().collect();
        let expires: u64 = q["expires"].parse().unwrap();
        assert!(links.verified(&id, expires, &q["sig"]));
        assert!(!links.verified(&uuid::Uuid::now_v7().to_string(), expires, &q["sig"]));
        assert!(!links.verified(&id, expires + 1, &q["sig"]));
        assert!(!Links::new("http://ci.example:8090", b"other").verified(&id, expires, &q["sig"]));
        assert!(!links.verified(
            &id,
            now() - 1,
            &hex::encode(links.mac(&id, now() - 1).finalize().into_bytes())
        ));
    }
}
