//! Quota back-pressure in the result loop.
//!
//! A worker result can say the executor refused to start the task for lack of
//! capacity (`TaskResult::backpressure`): on Kubernetes, the apiserver would
//! not admit the pod because the namespace is at its `ResourceQuota`. Nothing
//! ran, so that is not a failed attempt. [`settle`] hands the claim back — the
//! row returns to `ready` with a future `scheduled_at` and the attempt the
//! claim consumed is given back — and the task is dispatched again once its
//! backoff elapses.
//!
//! The schedule and the transition live in `dagron_core::quota_wait` and
//! `db::requeue_quota_wait`. This module is what the reconcile loop does around
//! them: decide whether the task waits at all, count the refusal, say so in the
//! log, and fail the task once it has waited too long.
//!
//! The wait is bounded by `DAGRON_QUOTA_WAIT_MAX_SECS`, on a clock kept on the
//! task row, so for a task at rest in the queue a restart or a hand-over to
//! another scheduler continues it rather than restarting it. A task whose probe
//! was in flight when its engine stopped starts a new wait when its lease is
//! recovered, and, as with any lease recovery, that dispatch's attempt is not
//! given back. Past the bound the task fails for good, whatever its
//! `max_attempts`: a bound multiplied by a retry budget is a number nobody set.
//! `0` turns the wait off, and a refusal is a failed attempt.
//!
//! Not feature-gated. `Backpressure` is a marker any executor backend may
//! return, and the summary counts waiters whichever replica left them. The
//! summary is the reconcile loop's `quota_summary` sweep, once a minute where
//! `SWEEP_INTERVALS` does not say otherwise.

use anyhow::Result;
use dagron_core::quota_wait::{clip_detail, QuotaRequeue};
use tracing::{info, warn};

use crate::metrics::Metrics;
use crate::worker::TaskResult;
use crate::{dag, db, Seams};

/// Default for `DAGRON_QUOTA_WAIT_MAX_SECS`: six hours.
///
/// Long on purpose. A deep queue draining through a small namespace is the case
/// the wait exists for, and a short bound fails exactly that. A namespace that
/// is stuck rather than busy is what the WARN at [`WARN_AFTER_SECS`] is for.
pub(crate) const MAX_WAIT_SECS_DEFAULT: i64 = 21_600;

/// Past this a wait is reported at WARN rather than INFO, per task and in the
/// summary. The task is still waiting as designed; ten minutes is where a queue
/// draining slowly and a namespace that will never have room start to look the
/// same, and only an operator can tell which.
const WARN_AFTER_SECS: i64 = 600;

/// `DAGRON_QUOTA_WAIT_MAX_SECS` as the reconcile loop reads it: the seconds a
/// task may wait on quota, `0` = never wait. Unset or unparseable = the default.
pub(crate) fn max_wait_from_env() -> i64 {
    parse_max_wait(std::env::var("DAGRON_QUOTA_WAIT_MAX_SECS").ok().as_deref())
}

/// The pure half of [`max_wait_from_env`]. Parsed unsigned, so a negative value
/// is a typo that gets the default rather than a second spelling of "off", then
/// clamped into the `i64` the datastore compares against.
fn parse_max_wait(raw: Option<&str>) -> i64 {
    raw.and_then(|v| v.trim().parse::<u64>().ok())
        .map_or(MAX_WAIT_SECS_DEFAULT, |secs| i64::try_from(secs).unwrap_or(i64::MAX))
}

/// What became of one back-pressure result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Settled {
    /// Back in the queue with its attempt given back. Nothing went terminal.
    Requeued,
    /// Waited past the bound and was failed here. Terminal: dependents moved.
    Expired,
    /// The claim was already gone. Nothing was written and nothing counted.
    Stale,
    /// This task does not wait. Nothing was touched; the caller takes the
    /// ordinary failure path with the claim it still holds.
    NotApplicable,
}

/// Does a refused task wait, or take the ordinary failure path?
///
/// Two cases do not wait. A bound of `0` is the operator's off switch. And with
/// gang co-scheduling on, a gang member must not be handed back alone: both
/// gang-aware claim paths take a gang all-or-nothing, so a lone `ready` member
/// is a row neither would ever claim. It takes the ordinary failure path, which
/// fails the member and cancels its siblings. With gangs off a member row is an
/// ordinary task and waits like one.
pub(crate) fn applies(max_wait_secs: i64, gangs_on: bool, spec: Option<&dag::TaskSpec>) -> bool {
    max_wait_secs > 0 && !(gangs_on && spec.is_some_and(|s| s.gang_member.is_some()))
}

/// Settle one back-pressure result: hand the claim back, or — at the bound —
/// fail the task under the same fence.
///
/// Called before the failure branch numbers the attempt, classifies the text or
/// consults a retry budget, because a refusal is none of the things those
/// record: a wait leaves no fault, no retry and no attempt behind.
///
/// Counting follows what was written, as everywhere else in the loop. A requeue
/// that landed is one refusal. A stale fence changed nothing and counts
/// nothing. An expiry is the failure it looks like, so it goes through the same
/// funnel as every other terminal transition, and only if the mark held.
pub(crate) async fn settle(
    pool: &db::Pool,
    metrics: &Metrics,
    seams: &Seams,
    max_wait_secs: i64,
    gangs_on: bool,
    result: &TaskResult,
) -> Result<Settled> {
    let detail = result.output.as_deref().unwrap_or_default();

    if !applies(max_wait_secs, gangs_on, result.spec.as_deref()) {
        // The worker logged this refusal at DEBUG, as back-pressure. Here it is
        // about to become a failed attempt, so the reason is repeated where it
        // will be seen.
        warn!(
            task_id = %result.task_id,
            max_wait_secs,
            gang_member = result.spec.as_ref().is_some_and(|s| s.gang_member.is_some()),
            refusal = %detail,
            "pod refused for namespace quota and this task does not wait — taking the failure path"
        );
        return Ok(Settled::NotApplicable);
    }

    match db::requeue_quota_wait(
        pool,
        &result.task_id,
        &result.worker_id,
        result.fence,
        detail,
        max_wait_secs,
    )
    .await?
    {
        QuotaRequeue::Requeued { waits, waited_secs, retry_in_secs } => {
            metrics.inc_quota_refusal();
            // Said on the 1st, 2nd, 4th, 8th… refusal. A task that waits an
            // hour is refused about sixty times, and sixty copies of one line
            // per waiter is how a log stops being read; the summary line keeps
            // the minute-by-minute picture.
            if u64::try_from(waits).is_ok_and(u64::is_power_of_two) {
                if waited_secs >= WARN_AFTER_SECS {
                    warn!(
                        task_id = %result.task_id,
                        waits,
                        waited_secs,
                        retry_in_secs,
                        refusal = %detail,
                        "task still waiting on namespace quota — returned to the queue, no \
                         attempt consumed"
                    );
                } else {
                    info!(
                        task_id = %result.task_id,
                        waits,
                        waited_secs,
                        retry_in_secs,
                        refusal = %detail,
                        "task waiting on namespace quota — returned to the queue, no attempt \
                         consumed"
                    );
                }
            }
            Ok(Settled::Requeued)
        }
        QuotaRequeue::Expired { waits, waited_secs } => {
            let reason = format!(
                "gave up waiting for namespace quota after {waited_secs}s ({waits} refusals); \
                 the command never started. Last refusal: {}. Raise the quota, lower this \
                 task's resources, or raise DAGRON_QUOTA_WAIT_MAX_SECS.",
                clip_detail(detail)
            );
            warn!(
                task_id = %result.task_id,
                waits,
                waited_secs,
                max_wait_secs,
                "gave up waiting on namespace quota — failing the task"
            );
            // The datastore wrote nothing at the bound, so the claim is still
            // this result's and the failure is fenced on it like any other.
            crate::note_failure(pool, &result.task_id, result.fence, &reason).await;
            if db::mark_task_failed(
                pool,
                &result.task_id,
                &result.worker_id,
                result.fence,
                Some(reason),
            )
            .await?
            {
                crate::task_finished(metrics, seams, false).await;
                metrics.inc_quota_wait_expired();
                Ok(Settled::Expired)
            } else {
                Ok(Settled::Stale)
            }
        }
        // The datastore already said so in the log.
        QuotaRequeue::Stale => Ok(Settled::Stale),
    }
}

/// One line saying how many tasks are waiting on quota and for how long, and
/// nothing at all when none is. The in-band twin of the
/// `scheduler_tasks_waiting_on_quota` gauge, for fleets nobody scrapes and for
/// the lean build, which has no `/metrics`.
///
/// Best-effort: it reports, so a failed read is a line in the log and never a
/// failed tick.
pub(crate) async fn log_summary(pool: &db::Pool) {
    match db::quota_wait_stats(pool).await {
        Ok(stats) if stats.waiting > 0 => {
            let oldest_wait_secs = stats.oldest_age_secs(chrono::Utc::now());
            if oldest_wait_secs >= WARN_AFTER_SECS {
                warn!(
                    waiting = stats.waiting,
                    oldest_wait_secs,
                    "tasks are waiting on namespace quota — raise the quota, lower their \
                     resources, or cap how many run at once with POOLS"
                );
            } else {
                info!(
                    waiting = stats.waiting,
                    oldest_wait_secs,
                    "tasks are waiting on namespace quota"
                );
            }
        }
        Ok(_) => {}
        Err(e) => warn!(error = %e, "quota-wait summary failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(json: &str) -> dag::TaskSpec {
        serde_json::from_str(json).expect("a task spec")
    }

    /// The whole decision, as a table. The gang rows are the ones that matter:
    /// the same member row waits or fails depending only on whether this engine
    /// claims gangs as a unit.
    #[test]
    fn a_refused_task_waits_unless_the_bound_is_zero_or_it_is_a_member_of_a_live_gang() {
        let plain = spec(r#"{"name":"a","command":["true"]}"#);
        let member = spec(
            r#"{"name":"a-0","command":["true"],"gang_member":{"id":"g","rank":0,"size":2}}"#,
        );
        assert!(member.gang_member.is_some(), "the fixture is a gang member");

        assert!(applies(MAX_WAIT_SECS_DEFAULT, false, None), "no stored spec is an ordinary task");
        assert!(applies(MAX_WAIT_SECS_DEFAULT, true, None));
        assert!(applies(MAX_WAIT_SECS_DEFAULT, false, Some(&plain)));
        assert!(applies(MAX_WAIT_SECS_DEFAULT, true, Some(&plain)), "gangs on, but not a member");
        assert!(applies(1, false, Some(&plain)), "any bound above zero waits");

        assert!(
            applies(MAX_WAIT_SECS_DEFAULT, false, Some(&member)),
            "with gangs off a member row is claimed like any other, so it can wait"
        );
        assert!(
            !applies(MAX_WAIT_SECS_DEFAULT, true, Some(&member)),
            "with gangs on a lone ready member is never claimed again, so it must not wait"
        );

        for gangs_on in [false, true] {
            for spec in [None, Some(&plain), Some(&member)] {
                assert!(!applies(0, gangs_on, spec), "a zero bound is the off switch");
            }
        }
    }

    #[test]
    fn the_bound_defaults_to_six_hours_and_zero_turns_the_wait_off() {
        assert_eq!(parse_max_wait(None), 21_600, "unset → six hours");
        assert_eq!(parse_max_wait(Some("0")), 0, "0 is the off switch, not a missing value");
        assert!(!applies(parse_max_wait(Some("0")), false, None));
        assert_eq!(parse_max_wait(Some("900")), 900);
        assert_eq!(parse_max_wait(Some(" 900\n")), 900, "whitespace tolerated");
        for typo in ["", "six hours", "-1", "1.5", "900s"] {
            assert_eq!(
                parse_max_wait(Some(typo)),
                MAX_WAIT_SECS_DEFAULT,
                "{typo:?} is a typo, and a typo must not turn the wait off"
            );
        }
        assert_eq!(
            parse_max_wait(Some("18446744073709551615")),
            i64::MAX,
            "a value past i64 is clamped, not wrapped negative"
        );
    }

    /// `settle` against a real datastore: what lands on the row, and what is
    /// and is not counted for it.
    #[cfg(feature = "sqlite")]
    mod store {
        use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
        use std::sync::Arc;

        use super::super::*;

        const WORKER: &str = "w1";

        /// The executor's refusal text, as it arrives in `TaskResult::output`.
        const REFUSAL: &str = "not started: pod refused by namespace ResourceQuota 'tenant-quota' \
            (requested: limits.cpu=550m, used: limits.cpu=3750m, limited: limits.cpu=4)";

        #[derive(Default)]
        struct CountingMeter {
            ok: AtomicUsize,
            bad: AtomicUsize,
        }

        #[async_trait::async_trait]
        impl crate::hooks::Meter for CountingMeter {
            async fn on_task_completed(&self, success: bool) {
                if success {
                    self.ok.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.bad.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        impl CountingMeter {
            /// (successes, failures) metered so far.
            fn seen(&self) -> (usize, usize) {
                (self.ok.load(Ordering::Relaxed), self.bad.load(Ordering::Relaxed))
            }
        }

        fn harness() -> (Metrics, Arc<CountingMeter>, Seams) {
            let meter = Arc::new(CountingMeter::default());
            let seams = Seams { meter: meter.clone(), ..Default::default() };
            (Metrics::new(), meter, seams)
        }

        /// Every counter a wait could wrongly move, in one tuple:
        /// (refusals, expired, failed, retried, faults of any class).
        fn counted(m: &Metrics) -> (u64, u64, u64, u64, u64) {
            let n = |c: &AtomicU64| c.load(Ordering::Relaxed);
            (
                n(&m.quota_refusals),
                n(&m.quota_wait_expired),
                n(&m.tasks_failed),
                n(&m.tasks_retried),
                m.task_faults.iter().map(n).sum(),
            )
        }

        /// A one-task run in a fresh SQLite file, advanced to `ready`:
        /// (pool, path, task_id).
        async fn one_ready_task() -> (db::Pool, std::path::PathBuf, String) {
            let path = std::env::temp_dir()
                .join(format!("m54_quota_wait_{}.db", uuid::Uuid::new_v4()));
            let pool = db::init_pool(path.to_str().unwrap()).await.unwrap();
            let yaml = "name: q\ntasks:\n  - name: pod\n    command: [\"true\"]\n";
            let dag = dag::DagGraph::from_yaml(yaml).unwrap();
            let run_id = db::create_run(&pool, &dag, yaml).await.unwrap();
            db::advance_ready_tasks(&pool).await.unwrap();
            let id: String = sqlx::query_scalar("SELECT id FROM task_runs WHERE run_id = ?")
                .bind(&run_id)
                .fetch_one(&pool)
                .await
                .unwrap();
            (pool, path, id)
        }

        /// The loop's next dispatch of the task, refused: its backoff elapses,
        /// it is claimed, and the worker hands back a back-pressure result
        /// carrying that claim's fence.
        async fn refused_dispatch(pool: &db::Pool, id: &str) -> TaskResult {
            sqlx::query("UPDATE task_runs SET scheduled_at = NULL WHERE id = ?")
                .bind(id)
                .execute(pool)
                .await
                .unwrap();
            let claimed = db::claim_ready(pool, WORKER, 10).await.unwrap();
            assert_eq!(claimed.len(), 1, "the task is claimable once it is due");
            let task = &claimed[0];
            TaskResult {
                task_id: task.id.clone(),
                worker_id: WORKER.to_string(),
                success: false,
                timed_out: false,
                backpressure: true,
                output: Some(REFUSAL.to_string()),
                attempt: task.attempt,
                max_attempts: 1,
                retry_delay_secs: 5,
                retry_max_delay_secs: None,
                retry_on_timeout: true,
                fence: task.version + 1,
                finished: std::time::Instant::now(),
                spec: None,
                stopped: dagron_executor::worker::Stopped::No,
                teardown: false,
                confined: false,
            }
        }

        /// (status, attempt, claimed_by, output) of the task row.
        async fn row(pool: &db::Pool, id: &str) -> (String, i64, Option<String>, Option<String>) {
            sqlx::query_as("SELECT status, attempt, claimed_by, output FROM task_runs WHERE id = ?")
                .bind(id)
                .fetch_one(pool)
                .await
                .unwrap()
        }

        /// The point of the whole change, from the loop's side: a refused task
        /// with `max_attempts: 1` is back in the queue with its one attempt
        /// unspent, and nothing says it failed, retried or faulted.
        #[tokio::test]
        async fn a_refusal_goes_back_to_the_queue_and_is_counted_only_as_a_refusal() {
            let (pool, path, id) = one_ready_task().await;
            let (metrics, meter, seams) = harness();

            let result = refused_dispatch(&pool, &id).await;
            assert_eq!(row(&pool, &id).await.1, 1, "the claim consumed an attempt");
            assert_eq!(
                settle(&pool, &metrics, &seams, 3600, false, &result).await.unwrap(),
                Settled::Requeued
            );
            let (status, attempt, claimed_by, _) = row(&pool, &id).await;
            assert_eq!((status.as_str(), attempt, claimed_by), ("ready", 0, None));
            assert_eq!(counted(&metrics), (1, 0, 0, 0, 0), "one refusal and nothing else");
            assert_eq!(meter.seen(), (0, 0), "nothing finished, so nothing is metered");

            // Refused again on the next dispatch: still no attempt spent.
            let result = refused_dispatch(&pool, &id).await;
            assert_eq!(
                settle(&pool, &metrics, &seams, 3600, false, &result).await.unwrap(),
                Settled::Requeued
            );
            assert_eq!(row(&pool, &id).await.1, 0);
            assert_eq!(counted(&metrics), (2, 0, 0, 0, 0));
            assert_eq!(meter.seen(), (0, 0));

            pool.close().await;
            let _ = std::fs::remove_file(&path);
        }

        /// Past the bound the task fails for good, with a reason an operator
        /// can act on, and is counted and metered exactly once as the failure
        /// it is.
        #[tokio::test]
        async fn at_the_bound_the_task_fails_once_with_the_reason() {
            let (pool, path, id) = one_ready_task().await;
            let (metrics, meter, seams) = harness();

            let first = refused_dispatch(&pool, &id).await;
            assert_eq!(
                settle(&pool, &metrics, &seams, 600, false, &first).await.unwrap(),
                Settled::Requeued
            );
            // The episode began fifteen minutes ago, against a ten-minute bound.
            let since = (chrono::Utc::now() - chrono::TimeDelta::seconds(900)).to_rfc3339();
            sqlx::query("UPDATE task_runs SET quota_wait_since = ? WHERE id = ?")
                .bind(&since)
                .bind(&id)
                .execute(&pool)
                .await
                .unwrap();

            let last = refused_dispatch(&pool, &id).await;
            assert_eq!(
                settle(&pool, &metrics, &seams, 600, false, &last).await.unwrap(),
                Settled::Expired
            );

            let (status, _, claimed_by, output) = row(&pool, &id).await;
            assert_eq!((status.as_str(), claimed_by), ("failed", None));
            let output = output.expect("the reason is the task's output");
            assert!(
                output.starts_with("gave up waiting for namespace quota after 90"),
                "measured from the first refusal: {output}"
            );
            assert!(
                output.contains("s (2 refusals); the command never started. "),
                "the refusal that reached the bound is counted: {output}"
            );
            assert!(
                output.ends_with(&format!(
                    "Last refusal: {REFUSAL}. Raise the quota, lower this task's resources, \
                     or raise DAGRON_QUOTA_WAIT_MAX_SECS."
                )),
                "{output}"
            );
            assert_eq!(
                counted(&metrics),
                (1, 1, 1, 0, 0),
                "the first refusal, then one expiry that is also one failure"
            );
            assert_eq!(meter.seen(), (0, 1), "metered once, as a failure");

            // The same result drained a second time finds the claim gone.
            assert_eq!(
                settle(&pool, &metrics, &seams, 600, false, &last).await.unwrap(),
                Settled::Stale
            );
            assert_eq!(counted(&metrics), (1, 1, 1, 0, 0));
            assert_eq!(meter.seen(), (0, 1));

            pool.close().await;
            let _ = std::fs::remove_file(&path);
        }

        /// A result whose claim was taken from under it — here, a fence that is
        /// not the row's — neither gives an attempt back nor counts anything.
        #[tokio::test]
        async fn a_result_whose_claim_is_gone_settles_nothing() {
            let (pool, path, id) = one_ready_task().await;
            let (metrics, meter, seams) = harness();

            let mut result = refused_dispatch(&pool, &id).await;
            result.fence += 1;
            assert_eq!(
                settle(&pool, &metrics, &seams, 3600, false, &result).await.unwrap(),
                Settled::Stale
            );
            let (status, attempt, claimed_by, _) = row(&pool, &id).await;
            assert_eq!(
                (status.as_str(), attempt, claimed_by.as_deref()),
                ("running", 1, Some(WORKER)),
                "the row belongs to the claim that holds it"
            );
            assert_eq!(counted(&metrics), (0, 0, 0, 0, 0));
            assert_eq!(meter.seen(), (0, 0));

            pool.close().await;
            let _ = std::fs::remove_file(&path);
        }

        /// The off switch has to stop short of the datastore, which answers a
        /// zero bound with "expired": the row must still be the caller's to
        /// fail or retry the ordinary way.
        #[tokio::test]
        async fn with_the_wait_turned_off_the_row_is_left_for_the_failure_path() {
            let (pool, path, id) = one_ready_task().await;
            let (metrics, meter, seams) = harness();

            let result = refused_dispatch(&pool, &id).await;
            assert_eq!(
                settle(&pool, &metrics, &seams, 0, false, &result).await.unwrap(),
                Settled::NotApplicable
            );
            let (status, attempt, claimed_by, output) = row(&pool, &id).await;
            assert_eq!(
                (status.as_str(), attempt, claimed_by.as_deref(), output),
                ("running", 1, Some(WORKER), None),
                "untouched, and still claimed"
            );
            assert_eq!(counted(&metrics), (0, 0, 0, 0, 0));
            assert_eq!(meter.seen(), (0, 0));

            pool.close().await;
            let _ = std::fs::remove_file(&path);
        }
    }
}
