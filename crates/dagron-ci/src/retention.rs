//! How long dagron-ci keeps its records.
//!
//! On Postgres, `ci_events` is partitioned by day (UTC). Each pass makes the
//! partitions of today and the [`AHEAD`] days after it where there are none,
//! so that an event always has its day's to go in; one received where there
//! is none lands in the default partition, and is kept as any other.
//!
//! Records leave the database only where the operator says how long they
//! are kept there (`CI_RECORD_DAYS`). Then each pass takes the days that are
//! over that, oldest first: where there is a store (`CI_OBJECT_STORE_URL`),
//! a day is written to it first, and read back ([`crate::records`]); a day
//! that is not written stays, and the pass stops there. Then the day is
//! dropped: its events (a partition dropped whole on Postgres, rows deleted
//! on SQLite), their runs' rows, their run API requests and their report
//! log, and its deliveries; and the payloads its events kept in the store
//! are deleted after them. Unset, nothing is dropped.
//!
//! Fleet mode's rows go by the same horizon: a job that ended before it,
//! and a runner cleared up after before it, are deleted, and are not
//! written to the store; what is still open is kept. They are what its
//! showback counts ([`crate::fleet::usage`]), which reads no further back.
//!
//! A segment written is deleted `CI_SEGMENT_DAYS` after its day, or kept for
//! good where that is unset; each pass sets every segment's time from it,
//! so a change of it holds for those written before.
//!
//! Who caused a run is also on the run's own record in the engine's
//! database, which this drops nothing of.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::db::{Day, Db};
use crate::metrics::Metrics;
use crate::payloads::Payloads;

/// How many days after today have their partitions made ahead.
pub const AHEAD: i64 = 7;

/// How often a pass is made. A day is dropped within this of its time.
pub const EVERY: Duration = Duration::from_secs(3600);

/// What a pass is given.
pub struct Retention {
    db: Db,
    /// `CI_RECORD_DAYS`: how many whole days before today records are kept
    /// in the database, or `None` to keep every one.
    keep_days: Option<u32>,
    /// `CI_SEGMENT_DAYS`: how long a segment is kept after its day.
    segment_days: Option<u32>,
    store: Option<Payloads>,
    metrics: Arc<Metrics>,
}

impl Retention {
    pub fn new(
        db: Db,
        keep_days: Option<u32>,
        segment_days: Option<u32>,
        store: Option<Payloads>,
        metrics: Arc<Metrics>,
    ) -> Retention {
        Retention {
            db,
            keep_days,
            segment_days,
            store,
            metrics,
        }
    }

    /// One pass, as of `today`.
    pub async fn pass(&self, today: Day) -> anyhow::Result<()> {
        let days: Vec<Day> = (0..=AHEAD).map(|n| Day(today.0 + n)).collect();
        let made = self.db.ensure_event_partitions(&days).await?;
        if !made.is_empty() {
            tracing::info!(partitions = %made.join(", "), "made ci_events' partitions ahead");
        }
        let result = self.leave(today).await;
        // Whatever the days did, the oldest record says how far behind they
        // are, and segments past their time go.
        if let Ok(age) = self.db.oldest_record_age_secs().await {
            self.metrics.set_oldest_record_age(age.unwrap_or(0.0));
        }
        if let Some(store) = &self.store {
            // The patch bundles past their time.
            crate::bundles::sweep(&self.db, store).await;
            self.db.set_segment_expiry(self.segment_days).await?;
            // Done at the end, after the deletes below, in the next pass.
            self.db.forget_unarchived().await?;
            for segment in self.db.expired_segments().await? {
                match store.delete_segment(&segment.object_key).await {
                    Ok(()) => {
                        self.db.forget_segment(&segment).await?;
                        tracing::info!(segment = %segment.object_key, "deleted a segment past CI_SEGMENT_DAYS");
                    }
                    Err(e) => {
                        tracing::warn!(segment = %segment.object_key, error = %format!("{e:#}"), "a segment past CI_SEGMENT_DAYS was not deleted")
                    }
                }
            }
        }
        result
    }

    /// The days past CI_RECORD_DAYS, written out where there is a store,
    /// and dropped.
    async fn leave(&self, today: Day) -> anyhow::Result<()> {
        let Some(keep) = self.keep_days else {
            return Ok(());
        };
        // A day of the horizon's is kept: `keep` whole days before today are.
        let horizon = today.minus(keep);
        let (jobs, runners) = self.db.drop_fleet_before(&horizon.start()).await?;
        if jobs + runners > 0 {
            tracing::info!(
                before = %horizon.date(), jobs, runners,
                "dropped fleet mode's rows past CI_RECORD_DAYS"
            );
        }
        for day in self.db.days_before(horizon).await? {
            let mut written = None;
            if let Some(store) = &self.store {
                match crate::records::write_day(&self.db, store, day).await {
                    Ok(wrote) => {
                        self.metrics.segments_written(wrote.segments as u64);
                        if wrote.segments > 0 {
                            tracing::info!(day = %day.date(), segments = wrote.segments, rows = wrote.rows, "wrote a day's records to CI_OBJECT_STORE_URL");
                        }
                        // What its verified segments hold, which the drop
                        // checks the day's rows against.
                        let mut held = std::collections::BTreeMap::new();
                        for segment in self.db.segments_of_day(day).await? {
                            if segment.verified {
                                held.insert(segment.table, segment.rows_sha256);
                            }
                        }
                        written = Some(held);
                    }
                    // Not written, not dropped: the next pass tries again.
                    Err(e) => {
                        tracing::warn!(day = %day.date(), error = %format!("{e:#}"), "a day's records could not be written out: they stay in the database");
                        return Ok(());
                    }
                }
            }
            let Some(dropped) = self.db.drop_day(day, written.as_ref()).await? else {
                continue;
            };
            self.metrics.records_dropped(dropped.events);
            tracing::info!(
                day = %day.date(),
                partitions = %dropped.partitions.join(", "),
                events = dropped.events,
                runs = dropped.runs,
                payloads = dropped.payloads.len(),
                "dropped a day past CI_RECORD_DAYS"
            );
            // The rows are gone: the payloads they named go after them. One
            // not deleted is left in the store, and said; nothing names it.
            for name in &dropped.payloads {
                let deleted = match &self.store {
                    Some(store) => store.delete(name).await,
                    None => Err(anyhow::anyhow!("CI_OBJECT_STORE_URL is not set")),
                };
                if let Err(e) = deleted {
                    tracing::warn!(payload = %name, error = %format!("{e:#}"), "a dropped event's payload was not deleted from the store");
                }
            }
        }
        Ok(())
    }
}

/// A pass at once, and then every [`EVERY`], until `stop`.
pub fn spawn(
    retention: Arc<Retention>,
    mut stop: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while !*stop.borrow() {
            if let Err(e) = retention.pass(Day::today()).await {
                tracing::warn!(error = %format!("{e:#}"), "keeping ci_events' days: the pass failed, and is made again later");
            }
            tokio::select! {
                _ = tokio::time::sleep(EVERY) => {}
                _ = stop.changed() => {}
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::tests::{event, sqlite};
    use crate::db::Recorded;

    /// Fleet mode's rows go with the horizon, and not before it, and what is
    /// open is kept however old.
    #[tokio::test]
    async fn a_pass_drops_the_fleet_s_rows_past_the_horizon_and_keeps_what_is_open() {
        let (db, _dir) = sqlite().await;
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let job = |id: u64, action| crate::github::Job {
            forge_host: "github.com".into(),
            full_name: "acme/widgets".into(),
            action,
            id,
            run_id: 7,
            run_attempt: 1,
            name: "build".into(),
            workflow: None,
            labels: vec!["dagron".into()],
            head_sha: "4b4708e725f29891d22b4c96af9f0a403ff51c7c".into(),
            runner_id: None,
            runner_name: None,
            conclusion: None,
        };
        use crate::github::JobAction;
        db.fleet_job(&repo, &job(1, JobAction::Completed), "dagron")
            .await
            .unwrap();
        db.fleet_job(&repo, &job(2, JobAction::Queued), "dagron")
            .await
            .unwrap();
        let Db::Sqlite(pool) = &db else {
            unreachable!()
        };
        sqlx::query("UPDATE ci_fleet_jobs SET finished_at = ?1, queued_at = ?1")
            .bind(Day::today().minus(20).start())
            .execute(pool)
            .await
            .unwrap();
        let ids = || async {
            sqlx::query_scalar::<_, String>("SELECT job_id FROM ci_fleet_jobs ORDER BY job_id")
                .fetch_all(pool)
                .await
                .unwrap()
        };
        let pass =
            |keep| Retention::new(db.clone(), keep, None, None, Arc::new(Metrics::default()));
        pass(None).pass(Day::today()).await.unwrap();
        assert_eq!(ids().await, ["1", "2"], "nothing goes with no horizon");
        pass(Some(30)).pass(Day::today()).await.unwrap();
        assert_eq!(ids().await, ["1", "2"], "inside the horizon");
        pass(Some(7)).pass(Day::today()).await.unwrap();
        assert_eq!(ids().await, ["2"], "the one that waits is kept");
    }

    /// A pass drops nothing unless a horizon is set; set, it drops the days
    /// past it, and deletes the payloads their events kept in the store.
    #[tokio::test]
    async fn a_pass_drops_the_days_past_the_horizon_and_their_payloads() {
        let (db, _dir) = sqlite().await;
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let store = tempfile::tempdir().unwrap();
        let payloads = Payloads::from_url(&format!("file://{}", store.path().display())).unwrap();
        let name = payloads.put(b"{\"big\": true}").await.unwrap();
        let Recorded::Event(old) = db
            .record_delivery_stored(&repo, "d-old", &event(), &name)
            .await
            .unwrap()
        else {
            panic!("a new delivery")
        };
        db.record_delivery(&repo, "d-new", &event(), b"{}")
            .await
            .unwrap();
        let Db::Sqlite(pool) = &db else {
            unreachable!()
        };
        sqlx::query("UPDATE ci_events SET received_at = ?1 WHERE id = ?2")
            .bind(Day::today().minus(20).start())
            .bind(&old)
            .execute(pool)
            .await
            .unwrap();
        let count = || async {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM ci_events")
                .fetch_one(pool)
                .await
                .unwrap()
        };

        Retention::new(
            db.clone(),
            None,
            None,
            Some(payloads.clone()),
            Default::default(),
        )
        .pass(Day::today())
        .await
        .unwrap();
        assert_eq!(count().await, 2, "no horizon: every event is kept");
        assert!(store.path().join(&name).is_file());

        Retention::new(
            db.clone(),
            Some(14),
            None,
            Some(payloads.clone()),
            Default::default(),
        )
        .pass(Day::today())
        .await
        .unwrap();
        assert_eq!(count().await, 1, "the day past fourteen is dropped");
        assert!(
            !store.path().join(&name).exists(),
            "and its payload with it"
        );
    }
}
