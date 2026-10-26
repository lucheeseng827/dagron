//! dagron-ci's object store (`CI_OBJECT_STORE_URL`): an object store, `s3://`,
//! `gs://`, `az://`, or a directory, `file:///path`. It keeps a webhook's
//! payload too large for its event's row, and the segments its records are
//! written to before they leave the database ([`crate::records`]).
//!
//! A payload of at most [`crate::db::PAYLOAD_INLINE_MAX`] is kept in its
//! event's row, as it always was. A larger one is put in the store first, and
//! the event's row names it (`ci_events.payload_ref`): the event is recorded
//! only once its payload is kept, and a payload whose event was not recorded
//! (a redelivery, a write that failed) is taken out again. Without a store, a
//! larger payload is not kept, and its event is recorded without it.
//!
//! A payload goes with its event: where events are dropped
//! (`CI_RECORD_DAYS`), the payloads they name are deleted after them
//! ([`crate::retention`]).
//!
//! A payload is named `payloads/<day>/<id>.json`, the day it was put (UTC)
//! and an id of its own, and never by anything a forge sent. A segment is
//! named `segments/<table>/<day>.<scope>.<part>.jsonl.zst`.

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use dagron_artifact::{ArtifactKey, ArtifactStore};

/// The first part of every payload's name in the store.
const PREFIX: &str = "payloads";

/// The store payloads are kept in.
#[derive(Clone)]
pub struct Payloads {
    store: Arc<dyn ArtifactStore>,
    /// The store's URL, as the operator gave it, for what dagron-ci says of
    /// itself. It holds no credential: those come from the environment.
    url: String,
}

impl std::fmt::Debug for Payloads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Payloads").field("url", &self.url).finish()
    }
}

impl Payloads {
    /// The store `url` names: `file:///path` for a directory, else the
    /// object store of `s3://`, `gs://` or `az://`, with credentials from the
    /// environment as the engine's stores read them.
    pub fn from_url(url: &str) -> Result<Payloads> {
        let url = url.trim();
        let store: Arc<dyn ArtifactStore> = if let Some(path) = url.strip_prefix("file://") {
            if !path.starts_with('/') {
                bail!("CI_OBJECT_STORE_URL={url:?}: a file:// URL names an absolute path");
            }
            Arc::new(dagron_artifact::LocalFsStore::new(path))
        } else {
            Arc::new(
                dagron_artifact::cloud::CloudStore::from_url(url)
                    .with_context(|| format!("CI_OBJECT_STORE_URL={url:?}"))?,
            )
        };
        Ok(Payloads {
            store,
            url: url.to_string(),
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// Keep `bytes`, and say what the event's row names them by.
    pub async fn put(&self, bytes: &[u8]) -> Result<String> {
        let day = today();
        let name = format!("{PREFIX}/{day}/{}.json", uuid::Uuid::now_v7());
        let key = key_of(&name).context("a payload's own name")?;
        self.store
            .put(&key, bytes)
            .await
            .context("keeping a payload in CI_OBJECT_STORE_URL")?;
        Ok(name)
    }

    /// The payload an event's row names.
    pub async fn get(&self, name: &str) -> Result<Vec<u8>> {
        let key = key_of(name)?;
        self.store
            .get(&key)
            .await
            .with_context(|| format!("reading the payload {name} from CI_OBJECT_STORE_URL"))
    }

    /// Delete the payload `name`. One that is not there is deleted already.
    pub async fn delete(&self, name: &str) -> Result<()> {
        let key = key_of(name)?;
        self.store
            .delete(&key)
            .await
            .map(drop)
            .with_context(|| format!("deleting the payload {name} from CI_OBJECT_STORE_URL"))
    }

    /// Keep a segment of `table`'s rows of `day`, and say its name.
    pub async fn put_segment(
        &self,
        table: &str,
        day: &str,
        scope: &str,
        part: i32,
        bytes: &[u8],
    ) -> Result<String> {
        let name = format!("{SEGMENTS}/{table}/{day}.{scope}.{part}.jsonl.zst");
        let key = segment_key(&name)?;
        self.store
            .put(&key, bytes)
            .await
            .with_context(|| format!("writing the segment {name} to CI_OBJECT_STORE_URL"))?;
        Ok(name)
    }

    pub async fn get_segment(&self, name: &str) -> Result<Vec<u8>> {
        let key = segment_key(name)?;
        self.store
            .get(&key)
            .await
            .with_context(|| format!("reading the segment {name} from CI_OBJECT_STORE_URL"))
    }

    /// Delete a segment. One that is not there is deleted already.
    pub async fn delete_segment(&self, name: &str) -> Result<()> {
        let key = segment_key(name)?;
        self.store
            .delete(&key)
            .await
            .map(drop)
            .with_context(|| format!("deleting the segment {name} from CI_OBJECT_STORE_URL"))
    }
}

/// The first part of every bundle's name in the store.
const BUNDLES: &str = "bundles";

/// The name bundle `id` is kept under: `bundles/<day>/<id>.tar.zst`.
pub fn bundle_name(id: &str) -> String {
    format!("{BUNDLES}/{}/{id}.tar.zst", today())
}

/// The store's key for a bundle's name, and nothing else.
fn bundle_key(name: &str) -> Result<ArtifactKey> {
    let mut parts = name.split('/');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(BUNDLES), Some(day), Some(file), None)
            if crate::db::Day::parse(day).is_some()
                && file
                    .strip_suffix(".tar.zst")
                    .is_some_and(crate::bundles::is_bundle_id) =>
        {
            Ok(ArtifactKey::new(BUNDLES, day, file))
        }
        _ => bail!("{name:?} is not the name of a bundle dagron-ci kept"),
    }
}

impl Payloads {
    /// Keep a bundle under `name` ([`bundle_name`]).
    pub async fn put_bundle(&self, name: &str, bytes: &[u8]) -> Result<()> {
        let key = bundle_key(name)?;
        self.store
            .put(&key, bytes)
            .await
            .map(drop)
            .with_context(|| format!("keeping the bundle {name} in CI_OBJECT_STORE_URL"))
    }

    pub async fn get_bundle(&self, name: &str) -> Result<Vec<u8>> {
        let key = bundle_key(name)?;
        self.store
            .get(&key)
            .await
            .with_context(|| format!("reading the bundle {name} from CI_OBJECT_STORE_URL"))
    }

    /// Delete a bundle. One that is not there is deleted already.
    pub async fn delete_bundle(&self, name: &str) -> Result<()> {
        let key = bundle_key(name)?;
        self.store
            .delete(&key)
            .await
            .map(drop)
            .with_context(|| format!("deleting the bundle {name} from CI_OBJECT_STORE_URL"))
    }

    /// A link the store signs for one upload of a bundle under `name`,
    /// good for `expires_in`; `None` from a store that signs none (a
    /// directory).
    pub async fn presign_bundle_put(
        &self,
        name: &str,
        expires_in: std::time::Duration,
    ) -> Result<Option<dagron_artifact::Presigned>> {
        let key = bundle_key(name)?;
        self.store
            .presign(&key, dagron_artifact::Transfer::Put, expires_in)
            .await
            .with_context(|| format!("signing an upload of the bundle {name}"))
    }
}

/// The first part of every segment's name in the store.
const SEGMENTS: &str = "segments";

/// The store's key for a segment's name, and nothing else.
fn segment_key(name: &str) -> Result<ArtifactKey> {
    let mut parts = name.split('/');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(SEGMENTS), Some(table), Some(file), None)
            if crate::db::RECORD_TABLES.contains(&table)
                && file.strip_suffix(".jsonl.zst").is_some_and(|stem| {
                    let mut bits = stem.splitn(3, '.');
                    matches!(
                        (bits.next(), bits.next(), bits.next()),
                        (Some(day), Some(scope), Some(part))
                            if crate::db::Day::parse(day).is_some()
                                && !scope.is_empty()
                                && scope.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                                && part.parse::<u32>().is_ok()
                    )
                }) =>
        {
            Ok(ArtifactKey::new(SEGMENTS, table, file))
        }
        _ => bail!("{name:?} is not the name of a segment dagron-ci wrote"),
    }
}

/// Today's date, UTC: the day a payload is kept under.
pub(crate) fn today() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let (y, m, d) = crate::cron::civil((secs / 86_400) as i64);
    format!("{y:04}-{m:02}-{d:02}")
}

/// The store's key for a payload's name: `payloads/<day>/<id>.json`, and
/// nothing else, so a row that names something else reaches nothing.
fn key_of(name: &str) -> Result<ArtifactKey> {
    let mut parts = name.split('/');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(PREFIX), Some(day), Some(file), None)
            if day.len() == 10
                && day.bytes().all(|b| b.is_ascii_digit() || b == b'-')
                && file.strip_suffix(".json").is_some_and(|id| {
                    !id.is_empty() && id.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-')
                }) =>
        {
            Ok(ArtifactKey::new(PREFIX, day, file))
        }
        _ => bail!("{name:?} is not the name of a payload dagron-ci kept"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_payload_is_kept_read_and_deleted_under_a_name_of_its_own() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("file://{}", dir.path().display());
        let payloads = Payloads::from_url(&url).unwrap();
        let name = payloads.put(b"{\"big\": true}").await.unwrap();
        let day = today();
        assert!(name.starts_with(&format!("payloads/{day}/")), "{name}");
        assert!(name.ends_with(".json"), "{name}");
        assert_eq!(payloads.get(&name).await.unwrap(), b"{\"big\": true}");
        assert!(dir.path().join(&name).is_file(), "kept as its name says");
        payloads.delete(&name).await.unwrap();
        assert!(payloads.get(&name).await.is_err());
        // Deleted already: as good as deleted.
        payloads.delete(&name).await.unwrap();
    }

    #[test]
    fn only_a_name_dagron_ci_gives_reaches_the_store() {
        assert!(key_of("payloads/2026-10-19/0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b.json").is_ok());
        for name in [
            "",
            "payloads",
            "payloads/2026-10-19",
            "payloads/2026-10-19/../x.json",
            "payloads/../../etc/passwd.json",
            "other/2026-10-19/a.json",
            "payloads/2026-10-19/a/b.json",
            "payloads/2026-10-19/.json",
            "payloads/2026-10-19/abc.txt",
            "payloads/2026/10/19.json",
        ] {
            assert!(key_of(name).is_err(), "{name:?}");
        }
    }

    #[test]
    fn a_store_url_is_a_directory_or_an_object_store() {
        assert!(Payloads::from_url("file://relative/dir").is_err());
        assert!(Payloads::from_url("ftp://somewhere").is_err());
        assert!(Payloads::from_url("file:///tmp/payloads").is_ok());
    }
}
