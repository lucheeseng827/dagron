//! A life for dagron-ci's records: a day's rows written to the
//! object store before they leave the database, and read back from there.
//!
//! Where `CI_RECORD_DAYS` says how long records are kept in the database and
//! `CI_OBJECT_STORE_URL` names a store, a day past it is written out before
//! it is dropped ([`crate::retention`]): each of [`RECORD_TABLES`] a segment
//! of that day's rows (more than one where they are many), JSON a row a line,
//! compressed with zstd, an event's stored payload in its row. Each segment
//! is read back and its checksum and its count matched, and only then do
//! the day's rows leave the database. A day that does not verify stays, and
//! is written again at the next pass.
//!
//! A segment holds the rows of one retention scope. There is one, the
//! instance's ([`SCOPE`]), until a repository or an organisation has a
//! policy of its own. It is kept `CI_SEGMENT_DAYS` after its day, or
//! for good where that is unset.
//!
//! What dagron-ci posted for an event is read from the database while its
//! day is there, and from its day's segments after
//! ([`report_log_from_segments`]), the day and the event an id is of found
//! in `ci_archived`, which the drop writes as the rows go.

use std::io::Read;

use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};

use crate::db::{Day, Db, Segment, RECORD_TABLES};
use crate::payloads::Payloads;

/// The one retention scope there is yet.
pub const SCOPE: &str = "instance";

/// The most rows a segment holds; a day with more has more segments.
pub const PART_ROWS: usize = 50_000;

/// What writing a day out did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Written {
    /// Segments written, and verified, by this pass.
    pub segments: usize,
    pub rows: usize,
}

/// Write `day`'s records to `store`, a table at a time, and read each
/// segment back. `Ok` once every table's rows of the day are in segments
/// that were verified: those written by a pass before and still holding
/// every row are kept as they are.
pub async fn write_day(db: &Db, store: &Payloads, day: Day) -> Result<Written> {
    let mut written = Written::default();
    let before = db.segments_of_day(day).await?;
    for table in RECORD_TABLES {
        let mut rows = db
            .records_of_day(table, day)
            .await
            .with_context(|| format!("reading {table}'s rows of {}", day.date()))?;
        let digest = crate::db::rows_digest(&rows);
        let kept: Vec<&Segment> = before.iter().filter(|s| s.table == table).collect();
        // Kept only where the rows are still those it holds: an event's and
        // a run's rows change in place, as many as they were.
        if !kept.is_empty() && kept.iter().all(|s| s.verified && s.rows_sha256 == digest) {
            continue;
        }
        let parts = rows.len().div_ceil(PART_ROWS);
        // Parts past those this writing makes hold rows no longer there.
        for stale in kept.iter().filter(|s| s.part as usize >= parts) {
            store.delete_segment(&stale.object_key).await?;
            db.forget_segment(stale).await?;
        }
        if rows.is_empty() {
            continue;
        }
        if table == "ci_events" {
            rows = with_payloads(store, rows).await?;
        }
        for (part, chunk) in rows.chunks(PART_ROWS).enumerate() {
            let segment = write_segment(db, store, table, day, part as i32, chunk, &digest).await?;
            written.segments += 1;
            written.rows += segment.row_count as usize;
        }
    }
    Ok(written)
}

/// An event's row with the payload it kept in the store, as
/// `payload_stored`: a segment holds what its day's rows did, and the store's
/// copy goes with the row.
async fn with_payloads(store: &Payloads, rows: Vec<String>) -> Result<Vec<String>> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut value: serde_json::Value =
            serde_json::from_str(&row).context("an event's row as JSON")?;
        if let Some(name) = value
            .get("payload_ref")
            .and_then(|v| v.as_str())
            .map(str::to_string)
        {
            let bytes = store.get(&name).await?;
            value["payload_stored"] = serde_json::json!(String::from_utf8_lossy(&bytes));
        }
        out.push(value.to_string());
    }
    Ok(out)
}

async fn write_segment(
    db: &Db,
    store: &Payloads,
    table: &str,
    day: Day,
    part: i32,
    rows: &[String],
    rows_sha256: &str,
) -> Result<Segment> {
    let mut lines = rows.join("\n");
    lines.push('\n');
    let bytes = zstd::encode_all(lines.as_bytes(), 3).context("compressing a segment")?;
    let sha256 = hex::encode(Sha256::digest(&bytes));
    let object_key = store
        .put_segment(table, &day.date(), SCOPE, part, &bytes)
        .await?;
    let segment = Segment {
        table: table.to_string(),
        day: day.date(),
        scope: SCOPE.to_string(),
        part,
        object_key,
        row_count: rows.len() as i64,
        size_bytes: bytes.len() as i64,
        sha256,
        rows_sha256: rows_sha256.to_string(),
        verified: false,
    };
    db.put_segment(&segment).await?;
    // Read back whole: what the store gives is what was written, every row.
    let back = store.get_segment(&segment.object_key).await?;
    if hex::encode(Sha256::digest(&back)) != segment.sha256 {
        bail!(
            "{}: read back, its checksum does not match what was written",
            segment.object_key
        );
    }
    let count = decode(&back)?.lines().count();
    if count != rows.len() {
        bail!(
            "{}: read back, it holds {count} rows of {}",
            segment.object_key,
            rows.len()
        );
    }
    db.verify_segment(&segment).await?;
    Ok(Segment {
        verified: true,
        ..segment
    })
}

fn decode(bytes: &[u8]) -> Result<String> {
    let mut text = String::new();
    zstd::stream::Decoder::new(bytes)
        .context("a segment's frame")?
        .read_to_string(&mut text)
        .context("decompressing a segment")?;
    Ok(text)
}

/// What dagron-ci posted for the event `event_id`, read back from the
/// segments of `day`, its day ([`Db::archived`]), oldest first.
pub async fn report_log_from_segments(
    db: &Db,
    store: &Payloads,
    event_id: &str,
    day: Day,
) -> Result<Vec<serde_json::Value>> {
    let mut rows = Vec::new();
    for segment in db.segments_of_day(day).await? {
        if segment.table != "ci_report_log" || !segment.verified {
            continue;
        }
        let text = decode(&store.get_segment(&segment.object_key).await?)?;
        for line in text.lines().filter(|l| !l.is_empty()) {
            let row: serde_json::Value = serde_json::from_str(line).context("a report log row")?;
            if row.get("event_id").and_then(|v| v.as_str()) == Some(event_id) {
                rows.push(row);
            }
        }
    }
    rows.sort_by_key(|r| r.get("id").and_then(|v| v.as_i64()).unwrap_or(0));
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::db::tests::{drop_schema, event, postgres, sqlite};
    use crate::db::{Recorded, ReportLog, SubmittedRun};
    use crate::metrics::Metrics;
    use crate::retention::Retention;

    /// Run `sql` with one text parameter on either backend.
    async fn exec(db: &Db, sql: &str, args: &[&str]) {
        match db {
            Db::Postgres(p) => {
                let mut q = sqlx::query(sql);
                for a in args {
                    q = q.bind(*a);
                }
                q.execute(p).await.unwrap();
            }
            Db::Sqlite(p) => {
                let sql = sql.replace('$', "?");
                let mut q = sqlx::query(&sql);
                for a in args {
                    q = q.bind(*a);
                }
                q.execute(p).await.unwrap();
            }
        }
    }

    async fn count(db: &Db, sql: &str) -> i64 {
        match db {
            Db::Postgres(p) => sqlx::query_scalar(sql).fetch_one(p).await.unwrap(),
            Db::Sqlite(p) => sqlx::query_scalar(sql).fetch_one(p).await.unwrap(),
        }
    }

    fn files_under(dir: &std::path::Path) -> Vec<String> {
        let mut out = Vec::new();
        let mut todo = vec![dir.to_path_buf()];
        while let Some(d) = todo.pop() {
            for entry in std::fs::read_dir(&d).into_iter().flatten().flatten() {
                let p = entry.path();
                if p.is_dir() {
                    todo.push(p)
                } else {
                    out.push(
                        p.strip_prefix(dir)
                            .unwrap()
                            .to_string_lossy()
                            .replace('\\', "/"),
                    )
                }
            }
        }
        out.sort();
        out
    }

    /// A day past CI_RECORD_DAYS is written to the store, a segment a table
    /// read back and verified, before it leaves the database; what was posted
    /// for its event is read back from there; a second pass writes nothing;
    /// and a segment goes once CI_SEGMENT_DAYS have passed.
    async fn the_records_contract(db: &Db) {
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let store = Payloads::from_url(&format!("file://{}", dir.path().display())).unwrap();
        let big = format!("{{\"padding\": \"{}\"}}", "x".repeat(70_000));
        let name = store.put(big.as_bytes()).await.unwrap();
        let Recorded::Event(made) = db
            .record_delivery_stored(&repo, "d-old", &event(), &name)
            .await
            .unwrap()
        else {
            panic!("a new delivery")
        };
        db.record_delivery(&repo, "d-new", &event(), b"{}")
            .await
            .unwrap();

        // Three weeks ago, as its id says too (the id is a UUIDv7 of when
        // the event was made).
        let today = Day::today();
        let old_day = today.minus(20);
        let old_secs = (old_day.0 * 86_400 + 12 * 3600) as u64;
        let old = uuid::Uuid::new_v7(uuid::Timestamp::from_unix(uuid::NoContext, old_secs, 0))
            .to_string();
        db.ensure_event_partitions(&[old_day]).await.unwrap();
        let at = format!("{}T12:00:00.000Z", old_day.date());
        let cast = match db {
            Db::Postgres(_) => "::timestamptz",
            Db::Sqlite(_) => "",
        };
        exec(
            db,
            &format!("UPDATE ci_events SET id = $1, received_at = $2{cast} WHERE id = $3"),
            &[&old, &at, &made],
        )
        .await;
        exec(
            db,
            &format!("UPDATE ci_deliveries SET received_at = $1{cast} WHERE delivery_id = 'd-old'"),
            &[&at],
        )
        .await;
        let run = SubmittedRun {
            run_id: "run-old".into(),
            event_id: old.clone(),
            repo_id: repo.id.clone(),
            cell: "default".into(),
            pipeline: ".dagron/ci/ci.yaml".into(),
            idempotency_key: "key-old".into(),
            on_failed: false,
        };
        assert!(db.record_run(&run).await.unwrap());
        let posted = |context: &str, outcome: &'static str| ReportLog {
            event_id: old.clone(),
            run_id: Some("run-old".into()),
            repo_id: repo.id.clone(),
            sha: "a".repeat(40),
            context: context.into(),
            state: "success".into(),
            description: Some("passed".into()),
            target_url: None,
            outcome,
            detail: (outcome != "posted").then(|| "a refusal".to_string()),
        };
        db.log_reports(&[
            posted("dagron-ci/ci/build (push)", "posted"),
            posted("dagron-ci/ci/test (push)", "refused"),
        ])
        .await
        .unwrap();
        assert_eq!(db.report_log_of(&old).await.unwrap().len(), 2);

        let metrics = Arc::new(Metrics::default());
        let pass = Retention::new(
            db.clone(),
            Some(14),
            Some(30),
            Some(store.clone()),
            metrics.clone(),
        );
        pass.pass(today).await.unwrap();
        // Its run is still followed: the day is written out, and kept.
        assert_eq!(db.segments_of_day(old_day).await.unwrap().len(), 4);
        assert_eq!(count(db, "SELECT count(*) FROM ci_runs").await, 1, "kept");
        // A post is logged after it was written, and its run ends: the next
        // pass writes the day again before it drops it, so that the post is
        // in a segment too.
        db.log_reports(&[posted("dagron-ci/ci/lint (push)", "posted")])
            .await
            .unwrap();
        exec(
            db,
            "UPDATE ci_runs SET report_due_at = NULL WHERE run_id = $1",
            &["run-old"],
        )
        .await;
        pass.pass(today).await.unwrap();

        // Its segments, one a table that had rows that day, each verified.
        let segments = db.segments_of_day(old_day).await.unwrap();
        let shape: Vec<(String, i64, bool)> = segments
            .iter()
            .map(|s| (s.table.clone(), s.row_count, s.verified))
            .collect();
        assert_eq!(
            shape,
            [
                ("ci_deliveries".to_string(), 1, true),
                ("ci_events".to_string(), 1, true),
                ("ci_report_log".to_string(), 3, true),
                ("ci_runs".to_string(), 1, true),
            ]
        );
        let files = files_under(dir.path());
        for s in &segments {
            assert!(
                files.contains(&s.object_key),
                "{} in {files:?}",
                s.object_key
            );
        }
        // Gone from the database, and its payload from the store: the
        // segment holds what the row named.
        assert_eq!(count(db, "SELECT count(*) FROM ci_events").await, 1);
        assert_eq!(count(db, "SELECT count(*) FROM ci_runs").await, 0);
        assert_eq!(count(db, "SELECT count(*) FROM ci_report_log").await, 0);
        assert_eq!(
            count(
                db,
                "SELECT count(*) FROM ci_deliveries WHERE delivery_id = 'd-old'"
            )
            .await,
            0
        );
        assert!(!files.contains(&name), "the payload went with its row");
        let events = segments.iter().find(|s| s.table == "ci_events").unwrap();
        let text = decode(&store.get_segment(&events.object_key).await.unwrap()).unwrap();
        let row: serde_json::Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
        assert_eq!(row["id"], old.as_str());
        assert_eq!(row["payload_stored"], big.as_str());
        // The run's row changed in place after the first writing (its run
        // ended), as many rows as before: its segment holds it as it is now.
        let runs = segments.iter().find(|s| s.table == "ci_runs").unwrap();
        let text = decode(&store.get_segment(&runs.object_key).await.unwrap()).unwrap();
        let run: serde_json::Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
        assert_eq!(run["run_id"], "run-old");
        assert!(run["report_due_at"].is_null(), "{run}");

        // What was posted for it, read back from its day's segments.
        // Its event and its run are found by their ids, with their day and
        // their repository's cell.
        assert_eq!(
            db.archived(&old).await.unwrap(),
            Some((old.clone(), old_day, "default".to_string()))
        );
        assert_eq!(
            db.archived("run-old").await.unwrap(),
            Some((old.clone(), old_day, "default".to_string()))
        );
        assert_eq!(
            db.archived(&uuid::Uuid::now_v7().to_string())
                .await
                .unwrap(),
            None
        );
        let back = report_log_from_segments(db, &store, &old, old_day)
            .await
            .unwrap();
        let said: Vec<(&str, &str)> = back
            .iter()
            .map(|r| {
                (
                    r["context"].as_str().unwrap(),
                    r["outcome"].as_str().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            said,
            [
                ("dagron-ci/ci/build (push)", "posted"),
                ("dagron-ci/ci/test (push)", "refused"),
                ("dagron-ci/ci/lint (push)", "posted")
            ]
        );
        assert!(
            report_log_from_segments(db, &store, &uuid::Uuid::now_v7().to_string(), old_day)
                .await
                .unwrap()
                .is_empty()
        );

        // Nothing is written twice, and a segment is kept while its days
        // last.
        let written = db.segments_of_day(old_day).await.unwrap();
        pass.pass(today).await.unwrap();
        assert_eq!(db.segments_of_day(old_day).await.unwrap(), written);
        assert!(metrics
            .render(None, &[])
            .contains("dagron_ci_segments_written_total 6\n"));

        // A day after its own is all CI_SEGMENT_DAYS=1 keeps it.
        Retention::new(
            db.clone(),
            Some(14),
            Some(1),
            Some(store.clone()),
            metrics.clone(),
        )
        .pass(today)
        .await
        .unwrap();
        assert!(db.segments_of_day(old_day).await.unwrap().is_empty());
        // Its ids are forgotten at the next pass, with no segment left of it.
        Retention::new(
            db.clone(),
            Some(14),
            Some(15),
            Some(store.clone()),
            metrics.clone(),
        )
        .pass(today)
        .await
        .unwrap();
        assert_eq!(db.archived(&old).await.unwrap(), None);
        assert!(
            files_under(dir.path())
                .iter()
                .all(|f| !f.starts_with("segments/")),
            "{:?}",
            files_under(dir.path())
        );
    }

    #[tokio::test]
    async fn a_day_is_written_out_read_back_and_dropped() {
        let (db, _dir) = sqlite().await;
        the_records_contract(&db).await;
    }

    #[tokio::test]
    async fn a_day_is_written_out_read_back_and_dropped_on_postgres() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        the_records_contract(&db).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    /// A day whose segment cannot be written stays in the database.
    #[tokio::test]
    async fn a_day_not_written_is_not_dropped() {
        let (db, _dir) = sqlite().await;
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        db.record_delivery(&repo, "d-old", &event(), b"{}")
            .await
            .unwrap();
        let old = Day::today().minus(20);
        exec(
            &db,
            "UPDATE ci_events SET received_at = $1",
            &[&format!("{}T12:00:00.000Z", old.date())],
        )
        .await;
        let dir = tempfile::tempdir().unwrap();
        let not_a_dir = dir.path().join("a-file");
        std::fs::write(&not_a_dir, b"").unwrap();
        let store = Payloads::from_url(&format!("file://{}", not_a_dir.display())).unwrap();
        Retention::new(db.clone(), Some(14), None, Some(store), Default::default())
            .pass(Day::today())
            .await
            .unwrap();
        assert_eq!(
            count(&db, "SELECT count(*) FROM ci_events").await,
            1,
            "kept"
        );
    }
}
