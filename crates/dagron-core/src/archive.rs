//! Durable writes to a local archive sink.
//!
//! One function, and it lives here rather than in the GC because two processes
//! now write archive documents: the engine's retention sweep, and dagron-api's
//! per-run archive route. Both treat a successful write as *purge permission* —
//! the run is deleted from the hot store immediately afterwards — so the fsync
//! chain below is the whole correctness story, and having one copy of it is
//! worth more than the module it costs.
//!
//! The cloud sinks (`GC_ARCHIVE_URL=s3|gs|az://…`) are not here: each caller
//! already builds its own `object_store` handle behind its own cargo features,
//! and a completed PUT is atomic, so there is no shared subtlety to hoist.

use std::path::{Path, PathBuf};

/// Atomically write one run's archive document: tmp file → fsync → rename to
/// `run-<id>.json`, then fsync the directory.
///
/// Overwriting an existing archive — a crash between the archive and the purge,
/// or a re-archive of the same run — is the idempotent re-do, not an error.
///
/// The tmp file carries a per-write nonce rather than the run id alone, because
/// a shared tmp name is not a lock. Two writers can now be mid-archive on the
/// *same* run (an operator hits the route while the retention window sweeps it
/// up), and `File::create` truncates: the second writer would empty the first's
/// file somewhere between its `to_writer` and its `sync_all`, so the first
/// renames a short document into place and returns `Ok`. That `Ok` is purge
/// permission, so the run leaves the hot store against a truncated archive —
/// the one way this module can lose data. A private tmp per write keeps each
/// writer's bytes to itself. The two renames still race, but rename is atomic
/// and both documents describe the same run, so whichever lands last is the
/// idempotent re-do above.
///
/// The parent-directory fsync is not optional: a rename's directory-entry
/// update is not durable until the directory itself is synced, so without it a
/// crash after a *verified* archive-and-purge could revert the entry and lose
/// the run entirely. Failure propagates so the caller keeps the run in the hot
/// store.
pub fn write_document(
    dir: &Path,
    run_id: &str,
    doc: &serde_json::Value,
) -> std::io::Result<PathBuf> {
    std::fs::create_dir_all(dir)?;
    let final_path = dir.join(format!("run-{run_id}.json"));
    let tmp_path = dir.join(format!(".run-{run_id}.{}.tmp", uuid::Uuid::new_v4()));
    match write_then_rename(&tmp_path, &final_path, dir, doc) {
        Ok(()) => Ok(final_path),
        Err(e) => {
            // The nonce that makes the tmp private also means no later write
            // reuses (and so cleans up) this name — unlink it rather than
            // litter the sink with one orphan per failed archive.
            let _ = std::fs::remove_file(&tmp_path);
            Err(e)
        }
    }
}

/// The fsync chain itself, split out so [`write_document`] has one error path to
/// hang tmp-file cleanup on.
fn write_then_rename(
    tmp_path: &Path,
    final_path: &Path,
    dir: &Path,
    doc: &serde_json::Value,
) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(tmp_path)?;
    serde_json::to_writer(&mut f, doc).map_err(std::io::Error::other)?;
    f.flush()?;
    f.sync_all()?;
    std::fs::rename(tmp_path, final_path)?;
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}

/// The object name a run's document is written under, in either sink. Shared so
/// a reader and a writer cannot drift on the naming.
pub fn document_name(run_id: &str) -> String {
    format!("run-{run_id}.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch() -> PathBuf {
        let d = std::env::temp_dir().join(format!("dagron-archive-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// The regression this module's tmp nonce exists for: many writers archiving
    /// the *same* run id at once must each publish a whole document. With one
    /// shared tmp path a writer truncates a peer's in-flight file, and the
    /// peer's `rename` then publishes the short version — which its caller reads
    /// as permission to purge the run. The document is padded so the write is
    /// wide enough for the interleaving to actually land.
    #[test]
    fn concurrent_writers_of_one_run_never_publish_a_partial_document() {
        let dir = scratch();
        let doc = serde_json::json!({"run": {"id": "r1", "pad": "x".repeat(512 * 1024)}});

        std::thread::scope(|s| {
            for _ in 0..8 {
                s.spawn(|| {
                    let p = write_document(&dir, "r1", &doc).expect("write");
                    // Read back through this writer's own returned path: the
                    // moment it returns Ok, the caller purges the hot store.
                    let back: serde_json::Value =
                        serde_json::from_slice(&std::fs::read(&p).unwrap()).expect("whole json");
                    assert_eq!(back["run"]["id"], "r1");
                    assert_eq!(back["run"]["pad"].as_str().unwrap().len(), 512 * 1024);
                });
            }
        });

        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "tmp files left behind: {leftovers:?}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Re-archiving a run is the documented idempotent re-do, and the sink is
    /// left with exactly one document plus no tmp residue.
    #[test]
    fn rewriting_the_same_run_overwrites_in_place() {
        let dir = scratch();
        write_document(&dir, "r2", &serde_json::json!({"v": 1})).unwrap();
        let p = write_document(&dir, "r2", &serde_json::json!({"v": 2})).unwrap();

        assert_eq!(p, dir.join(document_name("r2")));
        let back: serde_json::Value = serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
        assert_eq!(back["v"], 2);
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }
}
