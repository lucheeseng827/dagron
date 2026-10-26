//! `dagron backup`: a database's dumps, to and from a backup target.
//!
//! The dump itself is `pg_dump`'s (the chart's backup CronJob runs it beside
//! this, `docs/BACKUP_RECOVERY.md`); this moves the files it makes to where
//! they are kept, keeps the newest of them, and fetches one back for a
//! restore. A target is a URL, and any of them works the same:
//!
//! | target | where |
//! |---|---|
//! | `s3://bucket/prefix` | S3, or MinIO, Ceph and R2 through `AWS_ENDPOINT_URL` |
//! | `gs://bucket/prefix` | Google Cloud Storage |
//! | `az://container/prefix` | Azure Blob Storage (`AZURE_STORAGE_ACCOUNT_NAME`) |
//! | `file:///dir` | a directory: a volume, an NFS mount |
//!
//! Each cloud's credentials come from its own environment, as for the GC
//! archive ([`crate::objstore`]), workload identity included: what the pod's
//! service account is, the store is reached as.
//!
//! ```text
//! dagron backup put <file|dir> <target> [--keep N]   send a dump (each file of a dir)
//! dagron backup list <target>                         newest first
//! dagron backup get <target> <dir> [--name NAME]      fetch the newest (or NAME)
//! dagron backup prune <target> --keep N               keep the newest N
//! ```
//!
//! A target's dumps are the objects directly under its prefix, newest first by
//! when they were written, then by name: a dump named for its time (the
//! chart's are `dagron-<UTC>.dump`) sorts the same either way.

use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use futures_util::StreamExt;
use object_store::{path::Path, ObjectMeta, ObjectStore};
use tokio::io::AsyncWriteExt;

use crate::objstore;

const USAGE: &str = "usage:
  dagron backup put <file|dir> <target> [--keep N]
  dagron backup list <target>
  dagron backup get <target> <dir> [--name NAME]
  dagron backup prune <target> --keep N
a target is s3://bucket/prefix, gs://bucket/prefix, az://container/prefix or file:///dir";

/// `dagron backup …`, `args` being what follows `backup`.
pub async fn run_cli(args: &[String]) -> Result<()> {
    let (positional, keep, name) = parse_flags(args)?;
    match positional.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        ["put", from, target] => {
            let (store, prefix) = objstore::from_url(target)?;
            for path in files(FsPath::new(from))? {
                let size = put(&store, &prefix, &path).await?;
                println!("{target}/{}  {size} bytes", file_name(&path)?);
            }
            if let Some(keep) = keep {
                prune(&store, &prefix, keep, target).await?;
            }
            Ok(())
        }
        ["list", target] => {
            let (store, prefix) = objstore::from_url(target)?;
            for meta in dumps(&store, &prefix).await? {
                println!(
                    "{}  {:>12}  {}",
                    meta.last_modified.to_rfc3339(),
                    meta.size,
                    meta.location.filename().unwrap_or_default()
                );
            }
            Ok(())
        }
        ["get", target, dir] => {
            let (store, prefix) = objstore::from_url(target)?;
            let path = get(&store, &prefix, name.as_deref(), FsPath::new(dir)).await?;
            println!("{}", path.display());
            Ok(())
        }
        ["prune", target] => {
            let Some(keep) = keep else {
                bail!("prune needs --keep N: how many of the newest dumps to keep");
            };
            let (store, prefix) = objstore::from_url(target)?;
            prune(&store, &prefix, keep, target).await
        }
        _ => bail!("{USAGE}"),
    }
}

/// The words, `--keep N` (at least 1: a target is never emptied by
/// retention), and `--name NAME`.
fn parse_flags(args: &[String]) -> Result<(Vec<String>, Option<usize>, Option<String>)> {
    let (mut positional, mut keep, mut name) = (Vec::new(), None, None);
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--keep" => {
                let n = it.next().context("--keep needs a number")?;
                let n: usize = n.parse().with_context(|| format!("--keep {n}: not a number"))?;
                if n == 0 {
                    bail!("--keep 0 would delete every dump: keep at least one");
                }
                keep = Some(n);
            }
            "--name" => name = Some(it.next().context("--name needs a dump's name")?.clone()),
            "-h" | "--help" => bail!("{USAGE}"),
            _ => positional.push(arg.clone()),
        }
    }
    Ok((positional, keep, name))
}

/// The files `from` names: itself, or each regular file directly in it (a
/// dump the CronJob made in its scratch volume), in name order.
fn files(from: &FsPath) -> Result<Vec<PathBuf>> {
    let meta = std::fs::metadata(from).with_context(|| format!("{}", from.display()))?;
    if meta.is_file() {
        return Ok(vec![from.to_path_buf()]);
    }
    let mut out = Vec::new();
    for entry in std::fs::read_dir(from).with_context(|| format!("{}", from.display()))? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            out.push(entry.path());
        }
    }
    if out.is_empty() {
        bail!("{}: no dump to send", from.display());
    }
    out.sort();
    Ok(out)
}

fn file_name(path: &FsPath) -> Result<&str> {
    path.file_name()
        .and_then(|n| n.to_str())
        .with_context(|| format!("{}: no file name", path.display()))
}

/// Send `path` to `prefix/<its name>`, streamed (a dump is as large as the
/// database), and check the target holds as many bytes as were sent.
async fn put(store: &Arc<dyn ObjectStore>, prefix: &Path, path: &FsPath) -> Result<u64> {
    let location = prefix.child(file_name(path)?);
    let mut file = tokio::fs::File::open(path).await.with_context(|| format!("{}", path.display()))?;
    let mut writer = object_store::buffered::BufWriter::new(Arc::clone(store), location.clone());
    // A dump past one part is a multipart upload: one that fails is aborted,
    // or its parts stay in the bucket, billed, where list and prune never see
    // them.
    let sent = match tokio::io::copy(&mut file, &mut writer).await {
        Ok(sent) => sent,
        Err(e) => {
            let _ = writer.abort().await;
            return Err(e).with_context(|| format!("sending {}", path.display()));
        }
    };
    if let Err(e) = writer.shutdown().await {
        let _ = writer.abort().await;
        return Err(e).with_context(|| format!("finishing {location}"));
    }
    let held = store.head(&location).await.with_context(|| format!("{location}"))?.size as u64;
    if held != sent {
        bail!("{location}: {sent} bytes sent, {held} held");
    }
    Ok(sent)
}

/// The dumps directly under `prefix`, newest first.
async fn dumps(store: &Arc<dyn ObjectStore>, prefix: &Path) -> Result<Vec<ObjectMeta>> {
    let prefix = (!prefix.as_ref().is_empty()).then_some(prefix);
    let listed = store.list_with_delimiter(prefix).await.context("listing the target")?;
    let mut out = listed.objects;
    sort_newest_first(&mut out);
    Ok(out)
}

fn sort_newest_first(dumps: &mut [ObjectMeta]) {
    dumps.sort_by(|a, b| {
        b.last_modified.cmp(&a.last_modified).then_with(|| b.location.cmp(&a.location))
    });
}

/// Fetch the newest dump, or the one named, into `dir`: written beside its
/// name and renamed to it once whole, so a half-fetched dump is never one a
/// restore could take for a dump.
async fn get(
    store: &Arc<dyn ObjectStore>,
    prefix: &Path,
    name: Option<&str>,
    dir: &FsPath,
) -> Result<PathBuf> {
    let all = dumps(store, prefix).await?;
    let meta = match name {
        Some(name) => all
            .iter()
            .find(|m| m.location.filename() == Some(name))
            .with_context(|| format!("no dump named {name}"))?,
        None => all.first().context("the target holds no dump")?,
    };
    let name = meta.location.filename().context("a dump with no name")?.to_string();
    tokio::fs::create_dir_all(dir).await.with_context(|| format!("{}", dir.display()))?;
    let path = dir.join(&name);
    let partial = dir.join(format!(".{name}.partial"));
    let mut out = tokio::fs::File::create(&partial).await.with_context(|| format!("{}", partial.display()))?;
    let mut stream = store.get(&meta.location).await.with_context(|| format!("{}", meta.location))?.into_stream();
    let mut fetched = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.with_context(|| format!("fetching {}", meta.location))?;
        fetched += chunk.len() as u64;
        out.write_all(&chunk).await?;
    }
    out.sync_all().await?;
    drop(out);
    if fetched != meta.size as u64 {
        let _ = tokio::fs::remove_file(&partial).await;
        bail!("{}: {} bytes listed, {fetched} fetched", meta.location, meta.size);
    }
    tokio::fs::rename(&partial, &path).await?;
    Ok(path)
}

/// Keep the newest `keep` dumps under `prefix`; delete the rest, oldest last
/// standing. Says what it deleted.
async fn prune(store: &Arc<dyn ObjectStore>, prefix: &Path, keep: usize, target: &str) -> Result<()> {
    for meta in dumps(store, prefix).await?.into_iter().skip(keep) {
        store.delete(&meta.location).await.with_context(|| format!("deleting {}", meta.location))?;
        println!("pruned {target}/{}", meta.location.filename().unwrap_or_default());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    fn scratch() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("dagron-backup-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn args(words: &[&str]) -> Vec<String> {
        words.iter().map(|w| w.to_string()).collect()
    }

    #[tokio::test]
    async fn a_dump_goes_whole_and_comes_back_whole() {
        let store = store();
        let prefix = Path::from("cells/a");
        let dir = scratch();
        let dump = dir.join("dagron-20261019T020000Z.dump");
        // Larger than a multipart part, so the streamed path is the one used.
        let body: Vec<u8> = (0..(6 * 1024 * 1024)).map(|i| (i % 251) as u8).collect();
        std::fs::write(&dump, &body).unwrap();
        assert_eq!(put(&store, &prefix, &dump).await.unwrap(), body.len() as u64);

        let back = scratch();
        let got = get(&store, &prefix, None, &back).await.unwrap();
        assert_eq!(got, back.join("dagron-20261019T020000Z.dump"));
        assert_eq!(std::fs::read(&got).unwrap(), body);
        assert!(
            std::fs::read_dir(&back).unwrap().count() == 1,
            "no partial file is left beside it"
        );
    }

    #[tokio::test]
    async fn the_newest_are_kept_and_listed_first() {
        let store = store();
        let prefix = Path::from("p");
        let dir = scratch();
        for name in ["dagron-1.dump", "dagron-2.dump", "dagron-3.dump"] {
            std::fs::write(dir.join(name), name).unwrap();
            put(&store, &prefix, &dir.join(name)).await.unwrap();
            // Each written later than the last, as the CronJob's are.
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        // Not a dump of this target: one level down.
        store.put(&Path::from("p/older/dagron-0.dump"), "x".into()).await.unwrap();
        let names = |d: Vec<ObjectMeta>| -> Vec<String> {
            d.iter().map(|m| m.location.filename().unwrap().to_string()).collect()
        };
        assert_eq!(
            names(dumps(&store, &prefix).await.unwrap()),
            ["dagron-3.dump", "dagron-2.dump", "dagron-1.dump"]
        );
        prune(&store, &prefix, 2, "mem://p").await.unwrap();
        assert_eq!(names(dumps(&store, &prefix).await.unwrap()), ["dagron-3.dump", "dagron-2.dump"]);
        assert!(store.head(&Path::from("p/older/dagron-0.dump")).await.is_ok(), "below the prefix is left");

        let back = scratch();
        let named = get(&store, &prefix, Some("dagron-2.dump"), &back).await.unwrap();
        assert_eq!(std::fs::read_to_string(named).unwrap(), "dagron-2.dump");
        assert!(get(&store, &prefix, Some("dagron-1.dump"), &back).await.is_err(), "pruned");
    }

    #[tokio::test]
    async fn a_directory_sends_each_of_its_files() {
        let dir = scratch();
        std::fs::write(dir.join("b.dump"), "b").unwrap();
        std::fs::write(dir.join("a.dump"), "a").unwrap();
        std::fs::create_dir(dir.join("sub")).unwrap();
        let found = files(&dir).unwrap();
        assert_eq!(found, [dir.join("a.dump"), dir.join("b.dump")], "files only, in name order");
        assert!(files(&scratch()).is_err(), "an empty directory is no dump");
    }

    #[test]
    fn flags_parse_and_keep_is_never_zero() {
        let (pos, keep, name) = parse_flags(&args(&["get", "s3://b/p", "/tmp", "--name", "x.dump"])).unwrap();
        assert_eq!((pos.len(), keep, name.as_deref()), (3, None, Some("x.dump")));
        let (_, keep, _) = parse_flags(&args(&["prune", "gs://b", "--keep", "14"])).unwrap();
        assert_eq!(keep, Some(14));
        assert!(parse_flags(&args(&["prune", "gs://b", "--keep", "0"])).is_err());
        assert!(parse_flags(&args(&["prune", "gs://b", "--keep"])).is_err());
        assert!(parse_flags(&args(&["prune", "gs://b", "--keep", "many"])).is_err());
    }

    #[tokio::test]
    async fn a_file_target_is_a_directory() {
        let dir = scratch();
        let url = format!("file://{}", dir.join("backups").display());
        let dump = dir.join("dagron-x.dump");
        std::fs::write(&dump, "dump").unwrap();
        run_cli(&args(&["put", dump.to_str().unwrap(), &url, "--keep", "3"])).await.unwrap();
        assert_eq!(std::fs::read_to_string(dir.join("backups/dagron-x.dump")).unwrap(), "dump");
        assert!(objstore::from_url("file://relative/dir").is_err(), "a file URL is absolute");
    }
}
