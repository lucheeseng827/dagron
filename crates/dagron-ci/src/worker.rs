//! Event workers: claim the oldest due event under a lease, hand it to the
//! [`Handler`], and leave it as the handler says.
//!
//! A worker that stops mid-event (killed, its replica gone) loses nothing:
//! its lease lapses, and the next claim, by any worker in any replica, takes
//! the event over. That claim counts as an attempt, so an event that keeps
//! killing its workers is given up once attempts run out, instead of taking
//! the fleet down one worker at a time.
//!
//! A worker idles until the receiver queues an event (`wake`) or, at the
//! latest, [`IDLE_POLL`] passes: another replica's receiver, a retry coming
//! due, and a lapsed lease are noticed by polling.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{watch, Notify};
use tokio::task::JoinSet;

use crate::db::{Db, Finish};
use crate::dispatch::Handler;
use crate::metrics::Metrics;

/// The longest an idle worker waits before looking again.
pub const IDLE_POLL: Duration = Duration::from_secs(2);

/// How long a worker waits after the database fails it, before trying again.
const DB_RETRY: Duration = Duration::from_secs(1);

/// The bounds of a handler's wait (`Finish::Wait`).
const MIN_WAIT: Duration = Duration::from_secs(1);
const MAX_WAIT: Duration = Duration::from_secs(3600);

#[derive(Clone, Copy, Debug)]
pub struct Settings {
    pub workers: usize,
    pub lease: Duration,
    pub max_attempts: i64,
}

/// The wait before attempt `attempts + 1`: 5 s, doubling, at most 10 min.
pub fn backoff(attempts: i64) -> Duration {
    let doublings = attempts.clamp(1, 8) - 1;
    Duration::from_secs((5_u64 << doublings).min(600))
}

/// Start `settings.workers` workers. They run until `stop` turns true, finish
/// the event in hand, and return.
pub fn spawn<H: Handler>(
    db: Db,
    handler: Arc<H>,
    settings: Settings,
    metrics: Arc<Metrics>,
    wake: Arc<Notify>,
    stop: watch::Receiver<bool>,
) -> JoinSet<()> {
    let mut set = JoinSet::new();
    let replica = uuid::Uuid::now_v7().simple().to_string();
    for n in 0..settings.workers {
        let name = format!("{}-{n}", &replica[replica.len() - 12..]);
        set.spawn(work(
            name,
            db.clone(),
            handler.clone(),
            settings,
            metrics.clone(),
            wake.clone(),
            stop.clone(),
        ));
    }
    set
}

async fn work<H: Handler>(
    name: String,
    db: Db,
    handler: Arc<H>,
    settings: Settings,
    metrics: Arc<Metrics>,
    wake: Arc<Notify>,
    mut stop: watch::Receiver<bool>,
) {
    while !*stop.borrow() {
        match db.claim_event(&name, settings.lease).await {
            Ok(Some(ev)) => {
                let finish = if ev.attempts > settings.max_attempts {
                    // Earlier claims all lapsed: each worker that took it stopped.
                    Finish::Failed(format!(
                        "given up after {} attempts, each of whose workers stopped before finishing",
                        settings.max_attempts
                    ))
                } else {
                    match handler.handle(&ev).await {
                        Finish::Retry { reason, .. } if ev.attempts >= settings.max_attempts => {
                            Finish::Failed(format!(
                                "given up after {} attempts; the last: {reason}",
                                ev.attempts
                            ))
                        }
                        Finish::Retry { reason, .. } => Finish::Retry {
                            reason,
                            after: backoff(ev.attempts),
                        },
                        // A wait spends no attempt, so it never runs them out.
                        Finish::Wait { reason, after } => Finish::Wait {
                            reason,
                            after: after.clamp(MIN_WAIT, MAX_WAIT),
                        },
                        other => other,
                    }
                };
                match db.finish_event(&ev.id, &name, &finish).await {
                    Ok(true) => {
                        metrics.event_finished(&finish);
                        log(&ev.id, &ev.repo.full_name, ev.attempts, &finish);
                    }
                    Ok(false) => tracing::warn!(
                        event_id = %ev.id,
                        "the lease lapsed before the event was finished; another worker has it"
                    ),
                    // The lease lapses, and the event is tried again.
                    Err(e) => {
                        tracing::error!(event_id = %ev.id, error = %e, "could not finish an event")
                    }
                }
            }
            Ok(None) => {
                tokio::select! {
                    _ = wake.notified() => {}
                    _ = tokio::time::sleep(IDLE_POLL) => {}
                    _ = stop.changed() => {}
                }
            }
            Err(e) => {
                tracing::warn!(worker = %name, error = %e, "claiming an event failed");
                tokio::select! {
                    _ = tokio::time::sleep(DB_RETRY) => {}
                    _ = stop.changed() => {}
                }
            }
        }
    }
}

fn log(id: &str, repo: &str, attempts: i64, finish: &Finish) {
    match finish {
        Finish::Done(note) => tracing::info!(event_id = %id, repo, note, "event done"),
        Finish::Skipped(why) => tracing::info!(event_id = %id, repo, reason = why, "event skipped"),
        Finish::Failed(why) => {
            tracing::warn!(event_id = %id, repo, attempts, reason = why, "event failed")
        }
        Finish::Retry { reason, after } => tracing::warn!(
            event_id = %id, repo, attempts, reason, retry_in_secs = after.as_secs(),
            "event attempt failed; retrying"
        ),
        Finish::Wait { reason, after } => tracing::info!(
            event_id = %id, repo, reason, retry_in_secs = after.as_secs(),
            "event waits"
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::sync::Mutex;

    use super::*;
    use crate::db::tests::{event, sqlite};
    use crate::db::Claimed;

    /// A handler that answers from a script, and remembers what it was given.
    struct Scripted {
        answers: Mutex<Vec<Finish>>,
        seen: Mutex<Vec<(String, i64)>>,
    }

    impl Handler for Scripted {
        fn handle(&self, ev: &Claimed) -> impl Future<Output = Finish> + Send {
            self.seen.lock().unwrap().push((ev.id.clone(), ev.attempts));
            let mut answers = self.answers.lock().unwrap();
            let answer = if answers.len() > 1 {
                answers.remove(0)
            } else {
                answers[0].clone()
            };
            async move { answer }
        }
    }

    fn settings(workers: usize, max_attempts: i64) -> Settings {
        Settings {
            workers,
            lease: Duration::from_secs(60),
            max_attempts,
        }
    }

    async fn state_of(db: &Db) -> BTreeMap<String, (String, Option<String>)> {
        db.list_events("", 100)
            .await
            .unwrap()
            .into_iter()
            .map(|e| (e.id, (e.state, e.reason)))
            .collect()
    }

    async fn until(db: &Db, done: impl Fn(&BTreeMap<String, (String, Option<String>)>) -> bool) {
        for _ in 0..200 {
            if done(&state_of(db).await) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("events never settled: {:?}", state_of(db).await);
    }

    #[test]
    fn backoff_doubles_from_five_seconds_to_ten_minutes() {
        let secs: Vec<u64> = (1..=10).map(|n| backoff(n).as_secs()).collect();
        assert_eq!(secs, [5, 10, 20, 40, 80, 160, 320, 600, 600, 600]);
        assert_eq!(backoff(0).as_secs(), 5);
    }

    /// Every queued event is handled once, by a pool sharing the queue, and
    /// the receiver's wake starts a worker without waiting for the poll.
    #[tokio::test]
    async fn a_pool_handles_each_event_once() {
        let (db, _dir) = sqlite().await;
        let repo = db
            .add_repo(
                "github",
                "github.com",
                "acme/widgets",
                Some("wf"),
                "default",
            )
            .await
            .unwrap()
            .unwrap();
        for n in 0..12 {
            db.record_delivery(&repo, &format!("d-{n}"), &event(), b"{}")
                .await
                .unwrap();
        }
        let handler = Arc::new(Scripted {
            answers: Mutex::new(vec![Finish::Done("ok".into())]),
            seen: Mutex::default(),
        });
        let (stop_tx, stop) = watch::channel(false);
        let wake = Arc::new(Notify::new());
        let mut set = spawn(
            db.clone(),
            handler.clone(),
            settings(4, 3),
            Arc::new(Metrics::default()),
            wake.clone(),
            stop,
        );
        until(&db, |s| s.values().all(|(state, _)| state == "done")).await;

        // One more, woken.
        db.record_delivery(&repo, "d-late", &event(), b"{}")
            .await
            .unwrap();
        wake.notify_one();
        until(&db, |s| {
            s.len() == 13 && s.values().all(|(state, _)| state == "done")
        })
        .await;

        stop_tx.send(true).unwrap();
        while set.join_next().await.is_some() {}
        let seen = handler.seen.lock().unwrap();
        let mut ids: Vec<_> = seen.iter().map(|(id, _)| id.clone()).collect();
        ids.sort();
        ids.dedup();
        assert_eq!((seen.len(), ids.len()), (13, 13), "each event once");
    }

    /// A retry comes back after its backoff; once attempts run out the event is
    /// left failed, with the last reason.
    #[tokio::test]
    async fn retries_end_in_failure_with_the_reason() {
        let (db, _dir) = sqlite().await;
        let repo = db
            .add_repo(
                "github",
                "github.com",
                "acme/widgets",
                Some("wf"),
                "default",
            )
            .await
            .unwrap()
            .unwrap();
        db.record_delivery(&repo, "d-1", &event(), b"{}")
            .await
            .unwrap();
        let retry = Finish::Retry {
            reason: "dagron-api answered 503".into(),
            after: Duration::ZERO,
        };
        let handler = Arc::new(Scripted {
            answers: Mutex::new(vec![retry]),
            seen: Mutex::default(),
        });
        let metrics = Arc::new(Metrics::default());
        let (stop_tx, stop) = watch::channel(false);
        let wake = Arc::new(Notify::new());
        let mut set = spawn(
            db.clone(),
            handler.clone(),
            settings(1, 3),
            metrics.clone(),
            wake.clone(),
            stop,
        );

        // Each retry waits its backoff (5 s, then 10 s): once an attempt is
        // finished, make the next one due at once.
        for attempt in 1..=2 {
            // Handled first, and only then looked at. The event reads "new"
            // before an attempt is claimed as well as after it is finished,
            // and a look from before, judged once the handler had run, would
            // put the override below under the attempt, to be lost to the
            // backoff its finish writes.
            until(&db, |_| handler.seen.lock().unwrap().len() == attempt).await;
            until(&db, |s| s.values().all(|(state, _)| state == "new")).await;
            match &db {
                Db::Sqlite(p) => {
                    sqlx::query("UPDATE ci_events SET next_attempt_at = '2000-01-01T00:00:00Z'")
                        .execute(p)
                        .await
                        .unwrap()
                }
                Db::Postgres(_) => unreachable!(),
            };
            wake.notify_one();
        }
        until(&db, |s| s.values().all(|(state, _)| state == "failed")).await;
        stop_tx.send(true).unwrap();
        while set.join_next().await.is_some() {}

        let (_, (_, reason)) = state_of(&db).await.into_iter().next().unwrap();
        assert_eq!(
            reason.as_deref(),
            Some("given up after 3 attempts; the last: dagron-api answered 503")
        );
        let attempts: Vec<i64> = handler
            .seen
            .lock()
            .unwrap()
            .iter()
            .map(|(_, a)| *a)
            .collect();
        assert_eq!(attempts, [1, 2, 3]);
        let text = metrics.render(None, &[]);
        assert!(
            text.contains("dagron_ci_events_finished_total{outcome=\"retried\"} 2"),
            "{text}"
        );
        assert!(
            text.contains("dagron_ci_events_finished_total{outcome=\"failed\"} 1"),
            "{text}"
        );
    }

    /// An event whose every claim lapsed (each worker stopped mid-event) is not
    /// handed to the handler again past the limit: it is failed.
    #[tokio::test]
    async fn an_event_that_outlives_its_workers_is_given_up() {
        let (db, _dir) = sqlite().await;
        let repo = db
            .add_repo(
                "github",
                "github.com",
                "acme/widgets",
                Some("wf"),
                "default",
            )
            .await
            .unwrap()
            .unwrap();
        db.record_delivery(&repo, "d-1", &event(), b"{}")
            .await
            .unwrap();
        for _ in 0..2 {
            // A worker claims it and dies: its lease lapses.
            let ev = db
                .claim_event("doomed", Duration::from_secs(60))
                .await
                .unwrap()
                .unwrap();
            match &db {
                Db::Sqlite(p) => sqlx::query(
                    "UPDATE ci_events SET claim_expires_at = '2000-01-01T00:00:00Z' WHERE id = ?1",
                )
                .bind(&ev.id)
                .execute(p)
                .await
                .unwrap(),
                Db::Postgres(_) => unreachable!(),
            };
        }
        let handler = Arc::new(Scripted {
            answers: Mutex::new(vec![Finish::Done("ok".into())]),
            seen: Mutex::default(),
        });
        let (stop_tx, stop) = watch::channel(false);
        let mut set = spawn(
            db.clone(),
            handler.clone(),
            settings(1, 2),
            Arc::new(Metrics::default()),
            Arc::new(Notify::new()),
            stop,
        );
        until(&db, |s| s.values().all(|(state, _)| state == "failed")).await;
        stop_tx.send(true).unwrap();
        while set.join_next().await.is_some() {}
        assert!(
            handler.seen.lock().unwrap().is_empty(),
            "the handler never saw it"
        );
        let (_, (_, reason)) = state_of(&db).await.into_iter().next().unwrap();
        assert!(reason.unwrap().contains("given up after 2 attempts"));
    }
}
