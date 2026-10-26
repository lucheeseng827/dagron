//! A run's end-of-run work, through the transactional outbox.
//!
//! Every path that ends a run writes its `run.completed` event in the
//! transaction that ends it. The work its end owes the world — the run sink,
//! OpenLineage's terminal event, the `notify.git` commit status and
//! `notify.webhook` / `notify.slack`, and CDEvents ([`crate::cdevents`]) — is
//! done from that event, each part by a
//! worker of its own draining its own sink of the outbox
//! ([`dagron_core::db::OutboxSink`]). None is done in the reconcile loop, so a
//! forge or a lineage backend that is down or slow holds back nothing but its
//! own part: runs end, and dispatch goes on, as fast as when it is up. And
//! none is lost to a crash between a run's end and its work, as it was where
//! the loop did it after committing the end.
//!
//! A part is retried with backoff while its backend fails, and given up after
//! [`MAX_ATTEMPTS`] (its row `dead`, with why). Delivery is at least once: a
//! worker that dies mid-part leaves its lease to lapse, and the part is done
//! again. Each part is safe to repeat — a commit status, a lineage event of
//! the same run, a run sink that keeps a ledger of each run — but for a
//! notification, which an operator may then read twice.
//!
//! An end is done only while it is still the run's: a run cancelled and rerun
//! before the cancel's end was done ends again, and that end is the one owed.
//! An end older than [`STALE_SECS`] when its part first reaches it is let go:
//! that is a part that was switched off, now on again, and not a slow one.

use std::sync::Arc;
use std::time::Duration;

use dagron_core::db::{self, OutboxSink};
use dagron_core::models::OutboxEvent;
use tokio::sync::Notify;
use tracing::{debug, info, warn};

use crate::hooks::RunSink;

/// The run sink's part ([`RunSink`]).
pub(crate) const RUN_SINK: OutboxSink<'static> = OutboxSink {
    name: "engine.run_sink",
    event_types: &["run.completed"],
    adopts_legacy: false,
};
/// OpenLineage's terminal event.
pub(crate) const LINEAGE: OutboxSink<'static> = OutboxSink {
    name: "engine.lineage",
    event_types: &["run.completed"],
    adopts_legacy: false,
};
/// The `notify.git` commit status.
pub(crate) const FORGE: OutboxSink<'static> = OutboxSink {
    name: "engine.forge",
    event_types: &["run.completed"],
    adopts_legacy: false,
};
/// `notify.webhook` / `notify.slack`, and the global notification defaults.
pub(crate) const NOTIFY: OutboxSink<'static> = OutboxSink {
    name: "engine.notify",
    event_types: &["run.completed"],
    adopts_legacy: false,
};

/// The run's CDEvents ([`crate::cdevents`]).
#[cfg(feature = "ops")]
pub(crate) const CDEVENTS: OutboxSink<'static> = OutboxSink {
    name: "engine.cdevents",
    event_types: &["run.completed"],
    adopts_legacy: false,
};

/// Ends a worker claims at once, and does side by side.
const BATCH: i64 = 16;
/// The lease on a claimed end: longer than any part's own timeout.
const LEASE_SECS: i64 = 120;
/// How long a worker with nothing to do waits before it looks again, unless
/// the loop tells it a run has ended first.
const IDLE: Duration = Duration::from_secs(2);
/// Tries of one part of one end before it is given up.
pub(crate) const MAX_ATTEMPTS: i64 = 8;
/// The first retry's delay; each after doubles it, to [`MAX_BACKOFF_SECS`].
const BACKOFF_SECS: i64 = 5;
const MAX_BACKOFF_SECS: i64 = 3600;
/// An end this old when its part first reaches it is not done.
pub(crate) const STALE_SECS: i64 = 24 * 3600;
/// The longest a lineage post may take.
const LINEAGE_TIMEOUT: Duration = Duration::from_secs(15);
/// The longest any part may take, its own timeouts within it: well inside
/// [`LEASE_SECS`], so that a hung part is a failed try, retried, and not
/// one another replica starts again while it still runs.
pub(crate) const PART_TIMEOUT: Duration = Duration::from_secs(60);

/// One part of the end-of-run work, with what it needs.
#[derive(Clone)]
pub(crate) enum Part {
    RunSink(Arc<dyn RunSink>),
    Lineage(Arc<dagron_lineage::OpenLineageClient>),
    Forge(Arc<dagron_forge::ForgeClient>),
    Notify,
    #[cfg(feature = "ops")]
    CdEvents(Arc<crate::cdevents::CdEvents>),
}

impl Part {
    /// The lease on a claimed end of this part: past the longest it may
    /// take ([`Part::timeout`]).
    fn lease_secs(&self) -> i64 {
        match self {
            #[cfg(feature = "ops")]
            Part::CdEvents(_) => crate::cdevents::LEASE_SECS,
            _ => LEASE_SECS,
        }
    }

    /// The longest this part of an end may take. CDEvents keeps its own
    /// budget within this, by the number of events a run has.
    fn timeout(&self) -> Duration {
        match self {
            #[cfg(feature = "ops")]
            Part::CdEvents(_) => crate::cdevents::MAX_BUDGET,
            _ => PART_TIMEOUT,
        }
    }

    pub(crate) fn sink(&self) -> &'static OutboxSink<'static> {
        match self {
            Part::RunSink(_) => &RUN_SINK,
            Part::Lineage(_) => &LINEAGE,
            Part::Forge(_) => &FORGE,
            Part::Notify => &NOTIFY,
            #[cfg(feature = "ops")]
            Part::CdEvents(_) => &CDEVENTS,
        }
    }

    /// Do this part of `run_id`'s end in `status`, of the outbox row `end`.
    /// `Err` is retried.
    async fn run(
        &self,
        pool: &db::Pool,
        end: &str,
        run_id: &str,
        status: &str,
    ) -> Result<(), String> {
        #[cfg(not(feature = "ops"))]
        let _ = end;
        match self {
            Part::RunSink(sink) => {
                sink.on_run_completed(run_id, status).await;
                Ok(())
            }
            Part::Lineage(ol) => {
                let job = db::workflow_name_for_run(pool, run_id)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| run_id.to_string());
                let datasets = super::run_lineage_datasets(pool, run_id).await;
                let outcome = dagron_lineage::RunOutcome::from_run_status(status);
                match tokio::time::timeout(
                    LINEAGE_TIMEOUT,
                    ol.emit_run_completed(run_id, &job, outcome, &datasets),
                )
                .await
                {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(format!("OpenLineage emit failed: {e:#}")),
                    Err(_) => Err(format!(
                        "OpenLineage emit took longer than {}s",
                        LINEAGE_TIMEOUT.as_secs()
                    )),
                }
            }
            Part::Forge(forge) => super::forge_status(forge, pool, run_id, status).await,
            Part::Notify => {
                crate::notify::notify_run_event(pool, run_id, status).await;
                Ok(())
            }
            #[cfg(feature = "ops")]
            Part::CdEvents(cd) => cd.send_run_end(pool, end, run_id).await,
        }
    }
}

/// The parts this engine does: the run sink where its build has one, lineage,
/// the forge and CDEvents where they are configured, and notifications always.
pub(crate) fn parts(
    run_sink: Arc<dyn RunSink>,
    lineage: Option<dagron_lineage::OpenLineageClient>,
    forge: Option<Arc<dagron_forge::ForgeClient>>,
) -> Vec<Part> {
    let mut parts = Vec::new();
    if run_sink.delivers() {
        parts.push(Part::RunSink(run_sink));
    }
    if let Some(ol) = lineage {
        parts.push(Part::Lineage(Arc::new(ol)));
    }
    if let Some(forge) = forge {
        parts.push(Part::Forge(forge));
    }
    parts.push(Part::Notify);
    #[cfg(feature = "ops")]
    if let Some(cd) = crate::cdevents::CdEvents::from_env() {
        info!("CDEvents enabled (DAGRON_CDEVENTS_URL)");
        parts.push(Part::CdEvents(Arc::new(cd)));
    }
    parts
}

/// Say that every end written from now on is owed each of `parts`, before
/// any run of this engine can end.
pub(crate) async fn register(pool: &db::Pool, parts: &[Part]) -> anyhow::Result<()> {
    for part in parts {
        db::register_outbox_sink(pool, part.sink()).await?;
    }
    Ok(())
}

/// A worker for each of `parts`, until the process ends. `ended` is how the
/// reconcile loop says that a run has ended, so that its end is done at once
/// rather than at the next look.
pub(crate) fn spawn(pool: db::Pool, parts: Vec<Part>, ended: Arc<Notify>) {
    for part in parts {
        let (pool, ended) = (pool.clone(), Arc::clone(&ended));
        info!(sink = part.sink().name, "end-of-run worker started");
        tokio::spawn(async move { work(pool, part, ended).await });
    }
}

async fn work(pool: db::Pool, part: Part, ended: Arc<Notify>) {
    let sink = part.sink();
    loop {
        // Asked for before the claim, so that an end committed while it runs
        // wakes the wait after it.
        let woken = ended.notified();
        tokio::pin!(woken);
        woken.as_mut().enable();
        let n = match drain_once(&pool, &part).await {
            Ok(n) => n,
            Err(e) => {
                warn!(error = %e, sink = sink.name, "end-of-run drain failed");
                0
            }
        };
        if n == 0 {
            tokio::select! {
                _ = &mut woken => {}
                _ = tokio::time::sleep(IDLE) => {}
            }
        }
    }
}

/// Do each of `parts` for `run_id`'s end in `status`, once, each spawned:
/// a run a release before the outbox left its end to the loop for
/// (`completion_pending_at`), whose event is no part's.
pub(crate) fn spawn_once(pool: &db::Pool, parts: &[Part], run_id: &str, status: &str) {
    for part in parts {
        let (pool, part) = (pool.clone(), part.clone());
        let (run_id, status) = (run_id.to_string(), status.to_string());
        tokio::spawn(async move {
            let end = format!("before-10.7/{run_id}");
            if let Err(e) = part.run(&pool, &end, &run_id, &status).await {
                warn!(sink = part.sink().name, %run_id, error = %e, "an end-of-run part failed");
            }
        });
    }
}

/// Claim a batch of `part`'s ends and do them side by side. Returns how many
/// were claimed.
pub(crate) async fn drain_once(pool: &db::Pool, part: &Part) -> anyhow::Result<usize> {
    let sink = part.sink();
    // The claim that carries when each event was written, which is what an
    // end is too old by.
    let rows = db::claim_outbox_batch_for_uplink(pool, sink, BATCH, part.lease_secs()).await?;
    let n = rows.len();
    let mut done = tokio::task::JoinSet::new();
    for row in rows {
        let (pool, part) = (pool.clone(), part.clone());
        done.spawn(async move { deliver(&pool, &part, row).await });
    }
    while let Some(result) = done.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!(error = %e, sink = sink.name, "recording an end-of-run part failed")
            }
            Err(e) => warn!(error = %e, sink = sink.name, "an end-of-run part panicked"),
        }
    }
    Ok(n)
}

/// What an end's part is to do, by its event and what the run is now.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Owed {
    /// Do it, for this run in this status.
    Do { run_id: String, status: String },
    /// Nothing, and why.
    Nothing(&'static str),
}

/// Whether the end `ev` is still owed: its run is in the status it ended in,
/// and it is not older than [`STALE_SECS`] (`age_secs`, where its time
/// parsed). `now` is the run's status as read now, `None` for a run gone.
pub(crate) fn owed(ev: &OutboxEvent, age_secs: Option<i64>, now: Option<&str>) -> Owed {
    #[derive(serde::Deserialize)]
    struct Ended {
        run_id: String,
        status: String,
    }
    let Ok(ended) = serde_json::from_str::<Ended>(&ev.payload) else {
        return Owed::Nothing("its payload names no run and status");
    };
    if age_secs.is_some_and(|a| a > STALE_SECS) {
        return Owed::Nothing("older than a day");
    }
    match now {
        None => Owed::Nothing("its run is gone"),
        Some(s) if s != ended.status => Owed::Nothing("its run has been rerun since"),
        Some(_) => Owed::Do {
            run_id: ended.run_id,
            status: ended.status,
        },
    }
}

async fn deliver(pool: &db::Pool, part: &Part, row: db::OutboxUplinkRow) -> anyhow::Result<()> {
    let sink = part.sink().name;
    let age = chrono::DateTime::parse_from_rfc3339(&row.created_at)
        .ok()
        .map(|at| (chrono::Utc::now() - at.with_timezone(&chrono::Utc)).num_seconds());
    let ev = row.event;
    let status = match db::run_status(pool, &ev.run_id).await {
        Ok(s) => s.map(|s| s.to_string()),
        // Not knowing is no reason to give the end up: as a failed try.
        Err(e) => return fail(pool, sink, &ev, &format!("reading the run: {e:#}")).await,
    };
    let (run_id, st) = match owed(&ev, age, status.as_deref()) {
        Owed::Do { run_id, status } => (run_id, status),
        Owed::Nothing(why) => {
            debug!(sink, event_id = %ev.id, run_id = %ev.run_id, why, "an end that is no longer owed");
            db::mark_outbox_delivered(pool, sink, &ev.id).await?;
            return Ok(());
        }
    };
    let timeout = part.timeout();
    match tokio::time::timeout(timeout, part.run(pool, &ev.id, &run_id, &st)).await {
        Ok(Ok(())) => db::mark_outbox_delivered(pool, sink, &ev.id).await?,
        Ok(Err(e)) => fail(pool, sink, &ev, &e).await?,
        Err(_) => {
            let e = format!("took longer than {}s", timeout.as_secs());
            fail(pool, sink, &ev, &e).await?
        }
    }
    Ok(())
}

/// One failed try of `ev`'s part: again after a backoff, or given up.
async fn fail(pool: &db::Pool, sink: &str, ev: &OutboxEvent, error: &str) -> anyhow::Result<()> {
    let attempt = ev.attempts + 1;
    if attempt >= MAX_ATTEMPTS {
        warn!(sink, run_id = %ev.run_id, attempts = attempt, %error, "an end-of-run part is given up");
        db::mark_outbox_dead(pool, sink, &ev.id, error).await?;
        return Ok(());
    }
    let delay = backoff_secs(ev.attempts);
    let retry_at = (chrono::Utc::now() + chrono::TimeDelta::seconds(delay)).to_rfc3339();
    warn!(sink, run_id = %ev.run_id, attempt, retry_in_secs = delay, %error, "an end-of-run part failed; it is tried again");
    db::mark_outbox_failed(pool, sink, &ev.id, error, &retry_at).await?;
    Ok(())
}

/// [`BACKOFF_SECS`] doubled `attempts` times, to [`MAX_BACKOFF_SECS`].
pub(crate) fn backoff_secs(attempts: i64) -> i64 {
    let shift = attempts.clamp(0, 16) as u32;
    BACKOFF_SECS
        .saturating_mul(1i64 << shift)
        .min(MAX_BACKOFF_SECS)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(payload: &str) -> OutboxEvent {
        OutboxEvent {
            id: "e".into(),
            run_id: "r".into(),
            event_type: "run.completed".into(),
            payload: payload.into(),
            attempts: 0,
            created_at: "2026-10-19T00:00:00+00:00".into(),
        }
    }

    #[test]
    fn an_end_is_owed_while_it_is_still_the_runs() {
        let ev = event(r#"{"run_id":"r","status":"cancelled"}"#);
        assert_eq!(
            owed(&ev, Some(5), Some("cancelled")),
            Owed::Do {
                run_id: "r".into(),
                status: "cancelled".into()
            }
        );
        assert_eq!(
            owed(&ev, None, Some("cancelled")),
            Owed::Do {
                run_id: "r".into(),
                status: "cancelled".into()
            },
            "an unparseable time is no reason to let it go"
        );
        assert_eq!(
            owed(&ev, Some(5), Some("running")),
            Owed::Nothing("its run has been rerun since")
        );
        assert_eq!(
            owed(&ev, Some(5), Some("succeeded")),
            Owed::Nothing("its run has been rerun since")
        );
        assert_eq!(owed(&ev, Some(5), None), Owed::Nothing("its run is gone"));
        assert_eq!(
            owed(&ev, Some(STALE_SECS + 1), Some("cancelled")),
            Owed::Nothing("older than a day")
        );
        assert_eq!(
            owed(&event("{}"), Some(5), Some("cancelled")),
            Owed::Nothing("its payload names no run and status")
        );
    }

    /// A forge that answers every request `201` and records each body's
    /// `state`, in arrival order.
    #[cfg(feature = "ops")]
    async fn stub_forge() -> (String, Arc<std::sync::Mutex<Vec<String>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let states = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = states.clone();
        tokio::spawn(async move {
            while let Ok((mut conn, _)) = listener.accept().await {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                while !buf.ends_with(b"}") {
                    match conn.read(&mut chunk).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    }
                }
                let text = String::from_utf8_lossy(&buf);
                if let Some(at) = text.find("\"state\":\"") {
                    let state = text[at + 9..].split('"').next().unwrap_or("").to_string();
                    seen.lock().unwrap().push(state);
                }
                let _ = conn
                    .write_all(
                        b"HTTP/1.1 201 Created\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}",
                    )
                    .await;
            }
        });
        (base, states)
    }

    #[cfg(feature = "ops")]
    const NOTIFY_GIT: &str = "name: ci\n\
        parameters: { commit_sha: 4b4708e725f29891d22b4c96af9f0a403ff51c7c }\n\
        notify:\n  git: { provider: github, repo: acme/etl, sha: \"{{ commit_sha }}\" }\n\
        tasks:\n- name: a\n  command: [\"true\"]\n";

    #[cfg(feature = "ops")]
    async fn temp_pool() -> (db::Pool, std::path::PathBuf) {
        let path =
            std::env::temp_dir().join(format!("module54_run_end_{}.db", uuid::Uuid::new_v4()));
        (db::init_pool(path.to_str().unwrap()).await.unwrap(), path)
    }

    /// The status of `sink`'s row of each end of `run_id`, and its tries.
    #[cfg(feature = "ops")]
    async fn rows(pool: &db::Pool, sink: &str, run_id: &str) -> Vec<(String, i64)> {
        sqlx::query_as(
            "SELECT d.status, d.attempts FROM outbox_deliveries d JOIN event_outbox e ON e.id = d.outbox_id
             WHERE d.sink = ? AND e.run_id = ? ORDER BY e.created_at",
        )
        .bind(sink)
        .bind(run_id)
        .fetch_all(pool)
        .await
        .unwrap()
    }

    /// A cancelled run's commit status is posted by the forge's part, from its
    /// event, once; and an end superseded before its part reached it (the run
    /// rerun) is never posted.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn an_end_is_posted_by_its_part_once_and_a_superseded_one_never() {
        let (pool, path) = temp_pool().await;
        let (base, states) = stub_forge().await;
        let forge = Part::Forge(Arc::new(dagron_forge::ForgeClient::github("token", base)));
        register(&pool, std::slice::from_ref(&forge)).await.unwrap();
        let dag = crate::dag::DagGraph::from_yaml(NOTIFY_GIT).unwrap();

        let run = db::create_run(&pool, &dag, NOTIFY_GIT).await.unwrap();
        assert!(db::cancel_run(&pool, &run).await.unwrap().is_some());
        assert_eq!(drain_once(&pool, &forge).await.unwrap(), 1);
        assert_eq!(
            *states.lock().unwrap(),
            ["error"],
            "a cancelled run's check"
        );
        assert_eq!(drain_once(&pool, &forge).await.unwrap(), 0, "once");
        assert_eq!(
            rows(&pool, FORGE.name, &run).await,
            [("delivered".to_string(), 0)]
        );

        states.lock().unwrap().clear();
        let rerun = db::create_run(&pool, &dag, NOTIFY_GIT).await.unwrap();
        assert!(db::cancel_run(&pool, &rerun).await.unwrap().is_some());
        assert!(db::rerun_from_failed(&pool, &rerun)
            .await
            .unwrap()
            .is_some());
        assert_eq!(drain_once(&pool, &forge).await.unwrap(), 1);
        assert!(
            states.lock().unwrap().is_empty(),
            "the superseded cancel is not posted"
        );
        assert_eq!(
            rows(&pool, FORGE.name, &rerun).await,
            [("delivered".to_string(), 0)]
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A forge that is down costs its own part a try, again after a backoff,
    /// and nothing else: the run has ended, and the notification part of the
    /// same end is done.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn a_forge_that_is_down_is_tried_again_and_holds_back_no_other_part() {
        let (pool, path) = temp_pool().await;
        // A port nothing listens on.
        let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", closed.local_addr().unwrap());
        drop(closed);
        let forge = Part::Forge(Arc::new(dagron_forge::ForgeClient::github("token", base)));
        let parts = [forge.clone(), Part::Notify];
        register(&pool, &parts).await.unwrap();
        let dag = crate::dag::DagGraph::from_yaml(NOTIFY_GIT).unwrap();

        let run = db::create_run(&pool, &dag, NOTIFY_GIT).await.unwrap();
        assert!(db::cancel_run(&pool, &run).await.unwrap().is_some());
        assert_eq!(
            db::run_status(&pool, &run).await.unwrap(),
            Some(crate::models::RunStatus::Cancelled)
        );

        assert_eq!(drain_once(&pool, &forge).await.unwrap(), 1);
        assert_eq!(
            rows(&pool, FORGE.name, &run).await,
            [("pending".to_string(), 1)]
        );
        let (error, next): (Option<String>, String) = sqlx::query_as(
            "SELECT last_error, next_attempt_at FROM outbox_deliveries WHERE sink = ?",
        )
        .bind(FORGE.name)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(error
            .unwrap_or_default()
            .contains("forge commit status post failed"));
        assert!(
            next > chrono::Utc::now().to_rfc3339(),
            "again after a backoff: {next}"
        );
        assert_eq!(drain_once(&pool, &forge).await.unwrap(), 0, "not before");

        assert_eq!(drain_once(&pool, &Part::Notify).await.unwrap(), 1);
        assert_eq!(
            rows(&pool, NOTIFY.name, &run).await,
            [("delivered".to_string(), 0)]
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// An end whose part has failed its last try is given up, with why.
    #[tokio::test]
    #[cfg(feature = "ops")]
    async fn an_end_whose_part_fails_its_last_try_is_given_up() {
        let (pool, path) = temp_pool().await;
        let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", closed.local_addr().unwrap());
        drop(closed);
        let forge = Part::Forge(Arc::new(dagron_forge::ForgeClient::github("token", base)));
        register(&pool, std::slice::from_ref(&forge)).await.unwrap();
        let dag = crate::dag::DagGraph::from_yaml(NOTIFY_GIT).unwrap();
        let run = db::create_run(&pool, &dag, NOTIFY_GIT).await.unwrap();
        assert!(db::cancel_run(&pool, &run).await.unwrap().is_some());
        assert_eq!(drain_once(&pool, &forge).await.unwrap(), 1, "enrolled");

        // As though it had been tried all but once, and was due now.
        sqlx::query("UPDATE outbox_deliveries SET attempts = ?, next_attempt_at = '2020-01-01T00:00:00+00:00'")
            .bind(MAX_ATTEMPTS - 1)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(drain_once(&pool, &forge).await.unwrap(), 1);
        assert_eq!(
            rows(&pool, FORGE.name, &run).await,
            [("dead".to_string(), MAX_ATTEMPTS)]
        );

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn backoff_doubles_to_an_hour() {
        assert_eq!(backoff_secs(0), 5);
        assert_eq!(backoff_secs(1), 10);
        assert_eq!(backoff_secs(6), 320);
        assert_eq!(backoff_secs(10), 3600);
        assert_eq!(backoff_secs(i64::MAX), 3600);
        assert_eq!(backoff_secs(-3), 5);
    }
}
