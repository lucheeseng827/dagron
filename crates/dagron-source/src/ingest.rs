//! Queue-ingestion actor (v4).
//!
//! A `ractor` actor that turns a [`WorkflowSource`] stream into workflow runs.
//! It is the ingestion counterpart to the `WorkerPool` (dagron-executor):
//! where the worker actors pull *tasks* and execute them, the `IngestActor`
//! pulls *workflows* and persists them with `dagron_core::db::create_run`.
//!
//! It drives itself with a self-`cast` loop (`pre_start` kicks the first `Poll`;
//! each `handle` re-`cast`s the next), so a single submission is processed at a
//! time and run creation is naturally serialized.
//!
//! **Backpressure / influx absorption.** Before consuming a message, the actor
//! checks [`db::count_active_runs`] against `max_inflight_runs`. While at the
//! cap it sleeps briefly and re-polls *without* taking a message — so under a
//! large influx the messages pile up in the queue (SQS/Kafka/Redis), not in the
//! scheduler, and admission proceeds only as the reconcile loop drains runs to
//! terminal. The queue is the buffer; this counter is the valve.
//!
//! **Dead-letter routing (v4).** A submission can fail two ways: it doesn't parse
//! into a DAG (a *validation* failure — deterministic, so redelivering the same
//! bytes can never help), or `create_run` fails (possibly transient — a DB blip).
//! A parse failure is parked in the dead-letter store immediately; a `create_run`
//! failure is nacked for redelivery and only dead-lettered once it has failed
//! `max_validation_attempts` times (tracked per-payload in memory). Either way,
//! once a message is dead-lettered it is **acked** so it leaves the broker
//! instead of nack-looping forever. The poison message becomes a durable
//! `dead_letters` row an operator can inspect, redrive, or discard.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use ractor::{Actor, ActorProcessingErr, ActorRef};
use tracing::{info, warn};

use dagron_core::dag::DagGraph;
use dagron_core::db;
use dagron_core::metrics::Metrics;
use crate::source::{WorkflowMessage, WorkflowSource};

/// Sole message: advance the ingestion loop by one step.
pub enum IngestMsg {
    Poll,
}

/// Spawn arguments for [`IngestActor`].
pub struct IngestArgs {
    pub pool: db::Pool,
    pub source: Box<dyn WorkflowSource>,
    /// Admission cap — the actor will not create a new run while
    /// `count_active_runs() >= max_inflight_runs`. `0` (or less) disables the
    /// cap entirely; see [`at_inflight_cap`].
    pub max_inflight_runs: i64,
    /// Set true once the source is permanently exhausted (one-shot file). The
    /// reconcile loop reads this to decide when draining is complete.
    pub exhausted: Arc<AtomicBool>,
    /// Process counters; the actor bumps `runs_created` on each persisted run.
    pub metrics: Arc<Metrics>,
    /// Name of the configured source (e.g. `file`/`redis`), stored on each
    /// dead-letter row so an operator knows which broker produced the poison.
    pub source_name: String,
    /// How many times a `create_run` failure for the same payload is retried
    /// (nacked) before the submission is dead-lettered. `1` = dead-letter on the
    /// first transient failure. Parse failures dead-letter immediately regardless.
    pub max_validation_attempts: i64,
}

/// Whether the admission valve is closed: `active` runs against a `cap` of which
/// **`0` (or less) means "no cap"**, the contract the chart documents and the
/// engine's `POST /runs` gate already implements.
fn at_inflight_cap(active: i64, cap: i64) -> bool {
    cap > 0 && active >= cap
}

pub struct IngestState {
    pool: db::Pool,
    source: Box<dyn WorkflowSource>,
    max_inflight_runs: i64,
    exhausted: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    source_name: String,
    max_validation_attempts: i64,
    /// Per-payload `create_run` failure counter, so a transient failure is
    /// retried but a persistently-failing payload is eventually dead-lettered.
    /// Cleared when a payload finally succeeds or is dead-lettered, so it only
    /// ever holds the handful of currently-failing submissions.
    failures: HashMap<String, i64>,
}

/// How long to wait before re-checking admission while at the in-flight cap.
const THROTTLE: Duration = Duration::from_millis(250);
/// Backoff after a transient source error before retrying.
const ERROR_BACKOFF: Duration = Duration::from_secs(1);

pub struct IngestActor;

#[async_trait]
impl Actor for IngestActor {
    type Msg = IngestMsg;
    type State = IngestState;
    type Arguments = IngestArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<IngestMsg>,
        mut args: IngestArgs,
    ) -> Result<IngestState, ActorProcessingErr> {
        // Exactly-once resume: hand the source its durably-committed cursor
        // (written in the same transaction as the runs it accounts for) before
        // the first recv, so a redelivered position is never re-run. Failures
        // here degrade to at-least-once (the source's own checkpoint), never
        // block ingestion.
        match db::source_offset(&args.pool, &args.source_name).await {
            Ok(committed) => {
                if let Err(e) = args.source.set_committed_position(committed).await {
                    warn!(error = %e, "source rejected committed position — resuming from its own checkpoint");
                }
            }
            Err(e) => {
                warn!(error = %e, "committed-position lookup failed — resuming from the source's own checkpoint");
            }
        }
        myself.cast(IngestMsg::Poll)?;
        Ok(IngestState {
            pool: args.pool,
            source: args.source,
            max_inflight_runs: args.max_inflight_runs,
            exhausted: args.exhausted,
            metrics: args.metrics,
            source_name: args.source_name,
            max_validation_attempts: args.max_validation_attempts.max(1),
            failures: HashMap::new(),
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<IngestMsg>,
        msg: IngestMsg,
        state: &mut IngestState,
    ) -> Result<(), ActorProcessingErr> {
        let IngestMsg::Poll = msg;

        // ── Admission control: hold at the in-flight cap ───────────────────────
        // A transient DB error must not kill the actor — it is the admission
        // valve for all ingestion. Back off and re-poll, mirroring the
        // source.recv error path below.
        let active = match db::count_active_runs(&state.pool).await {
            Ok(n) => n,
            Err(e) => {
                warn!(error = %e, "count_active_runs failed — retrying after backoff");
                // SAFETY: ractor runs actors on tokio tasks, so sleeping here
                // yields cooperatively rather than blocking a thread.
                tokio::time::sleep(ERROR_BACKOFF).await;
                myself.cast(IngestMsg::Poll)?;
                return Ok(());
            }
        };
        // `max_inflight_runs <= 0` disables the cap, matching the chart docs and
        // the `POST /runs` gate in the engine's api.rs. Without this guard a
        // configured `0` compares `active >= 0` — always true — and the actor
        // throttles forever, admitting nothing at all.
        if at_inflight_cap(active, state.max_inflight_runs) {
            // SAFETY: ractor runs actors on tokio tasks; this sleep yields.
            tokio::time::sleep(THROTTLE).await;
            myself.cast(IngestMsg::Poll)?;
            return Ok(());
        }

        // ── Pull one submission and turn it into a run ─────────────────────────
        match state.source.recv().await {
            Ok(Some(message)) => {
                // Exactly-once: a source with a resumable coordinate commits it
                // in the same transaction as the run (or the dead letter) —
                // `None` keeps the plain at-least-once path. Partitioned
                // sources namespace their cursor row per shard.
                let position = state
                    .source
                    .pending_position()
                    .map(|pp| (pp.offset_key(&state.source_name), pp.position));
                match DagGraph::from_yaml(&message.payload) {
                    Ok(dag) => match create_run_at(&state.pool, &dag, &message.payload, &position)
                        .await
                    {
                        Ok(run_id) => {
                            state.metrics.inc_runs_created();
                            state.failures.remove(&message.payload); // clear any prior transient failures
                            info!(
                                %run_id,
                                name = %dag.spec.name,
                                tasks = dag.spec.tasks.len(),
                                "run created from queue"
                            );
                            // The run is already durably persisted; an ack
                            // failure must not kill the actor (that would both
                            // stop all ingestion and risk a duplicate run on
                            // redelivery). Log and keep going.
                            if let Err(e) = state.source.ack(&message.handle).await {
                                warn!(error = %e, %run_id, "ack failed — run persisted, message may redeliver");
                            }
                        }
                        // create_run can fail transiently (a DB blip), so retry
                        // via nack up to the threshold before giving up.
                        Err(e) => {
                            // Per-workflow concurrency cap (#21) is a "try later",
                            // not a poison: requeue without counting toward the
                            // dead-letter threshold, so a valid workflow submitted
                            // while at capacity is never dead-lettered — it
                            // redelivers and starts once a run slot frees.
                            if e.downcast_ref::<dagron_core::models::MaxActiveRunsReached>()
                                .is_some()
                            {
                                info!(error = %e, "at max_active_runs — nacking for later redelivery");
                                if let Err(e) = state.source.nack(&message.handle).await {
                                    warn!(error = %e, "nack failed — message redelivers after timeout");
                                }
                                // Same pacing as the in-flight valve above: on a
                                // source that redelivers promptly this would
                                // otherwise spin recv→refuse→nack until a slot frees.
                                tokio::time::sleep(THROTTLE).await;
                            } else {
                                // Copy the count out before the await below: the
                                // map entry is a live borrow of `state`, and
                                // holding it across a suspension point both
                                // conflicts with reading the setting and makes
                                // the future non-Send.
                                let count = {
                                    let c = state
                                        .failures
                                        .entry(message.payload.clone())
                                        .or_insert(0);
                                    *c += 1;
                                    *c
                                };
                                let max_attempts = effective_max_attempts(
                                    state.pool.clone(),
                                    state.max_validation_attempts,
                                )
                                .await;
                                if count >= max_attempts {
                                    let failures = count;
                                    state.failures.remove(&message.payload);
                                    dead_letter(
                                        state,
                                        &message,
                                        &e.to_string(),
                                        failures,
                                        &position,
                                    )
                                    .await;
                                } else {
                                    warn!(error = %e, attempt = count, "create_run failed — nacking for redelivery");
                                    if let Err(e) = state.source.nack(&message.handle).await {
                                        warn!(error = %e, "nack failed — message redelivers after timeout");
                                    }
                                }
                            }
                        }
                    },
                    // A parse failure is deterministic — redelivering the same
                    // bytes can never succeed, so dead-letter it immediately.
                    Err(e) => {
                        dead_letter(
                            state,
                            &message,
                            &format!("invalid workflow spec: {e}"),
                            1,
                            &position,
                        )
                        .await;
                    }
                }
                myself.cast(IngestMsg::Poll)?;
            }
            Ok(None) => {
                info!("workflow source exhausted — ingestion stopping");
                state.exhausted.store(true, Ordering::SeqCst);
                myself.stop(Some("source exhausted".to_string()));
            }
            Err(e) => {
                // `{:#}` (not `%e`): print the whole anyhow chain. A source's
                // outermost context ("peek logical replication changes") names
                // the operation but not the cause, and the driver error under
                // it is the only thing that tells an operator whether the feed
                // is misconfigured, unauthorized, or fighting for a lock.
                warn!(error = format!("{e:#}"), "source recv error — retrying after backoff");
                tokio::time::sleep(ERROR_BACKOFF).await;
                myself.cast(IngestMsg::Poll)?;
            }
        }

        Ok(())
    }
}

/// Create the run, committing the source's coordinate in the same transaction
/// when one is pending (exactly-once); without a coordinate this is plain
/// [`db::create_run`] (at-least-once).
async fn create_run_at(
    pool: &db::Pool,
    dag: &DagGraph,
    payload: &str,
    position: &Option<(String, String)>,
) -> anyhow::Result<String> {
    match position {
        Some((key, pos)) => db::create_run_with_offset(pool, dag, payload, key, pos).await,
        None => db::create_run(pool, dag, payload).await,
    }
}

/// Attempts before a submission is parked, as configured *now*.
///
/// Read on the failure path rather than cached at startup, so changing the
/// policy in the console takes effect without restarting the engine. That costs
/// one indexed row read per ingestion failure — and ingestion failures are, by
/// construction, the uncommon case. Caching it would mean the console showing a
/// number the running process is not actually using, which is worse than the
/// read.
///
/// Falls back to the value the process started with (`DEAD_LETTER_MAX_ATTEMPTS`)
/// whenever the setting is unset, malformed, or the table is absent — a
/// standalone SQLite engine with no dagron-api alongside it has no `ui_settings`
/// table at all, and has to keep working exactly as before.
/// Takes the pool by value rather than `&IngestState`: the state owns a
/// `Box<dyn WorkflowSource>`, which is not `Sync`, so borrowing any part of it
/// across an await would make the actor's whole future non-`Send`. The pool is
/// an `Arc` internally, so cloning it costs a refcount.
async fn effective_max_attempts(pool: db::Pool, fallback: i64) -> i64 {
    #[derive(serde::Deserialize)]
    struct Stored {
        max_attempts: i64,
    }
    match db::ui_setting(&pool, "dead_letters").await {
        Ok(Some(raw)) => serde_json::from_str::<Stored>(&raw)
            .map(|s| s.max_attempts.max(1))
            .unwrap_or(fallback),
        _ => fallback,
    }
}

/// Park a poison submission in the dead-letter store — advancing the source's
/// committed coordinate in the same transaction when one is pending, so a
/// restart never re-parks the same event — then ack it off the source so it
/// stops redelivering. A failure to persist or ack must not kill the actor
/// (that would stall all ingestion); the worst case on a persist failure is the
/// message redelivers and is retried, so log and carry on.
async fn dead_letter(
    state: &mut IngestState,
    message: &WorkflowMessage,
    error: &str,
    failures: i64,
    position: &Option<(String, String)>,
) {
    let recorded = match position {
        Some((key, pos)) => {
            db::record_dead_letter_with_offset(&state.pool, &message.payload, error, key, failures, pos)
                .await
        }
        None => {
            db::record_dead_letter(&state.pool, &message.payload, error, &state.source_name, failures)
                .await
        }
    };
    match recorded {
        Ok(id) => {
            state.metrics.inc_dead_letters();
            warn!(dead_letter_id = %id, failures, %error, "submission dead-lettered");
            // Mirror to the broker's native DLQ (SQS DLQ / Kafka DLT / Redis DLQ
            // list / NATS DLQ subject) if one is configured. Best-effort: the
            // durable Postgres row above is the source of truth, so a broker
            // publish failure must not stall ingestion — log and continue to ack.
            if let Err(e) = state.source.dead_letter(&message.payload, error).await {
                warn!(error = %e, "broker dead-letter routing failed (Postgres row recorded)");
            }
            // The poison is now durably recorded; drop it from the broker.
            if let Err(e) = state.source.ack(&message.handle).await {
                warn!(error = %e, "ack of dead-lettered message failed — it may redeliver");
            }
        }
        Err(e) => {
            // Couldn't park it — nack so it isn't silently lost; it'll be retried.
            warn!(error = %e, "failed to record dead letter — nacking for redelivery");
            if let Err(e) = state.source.nack(&message.handle).await {
                warn!(error = %e, "nack failed — message redelivers after timeout");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::ChannelSource;

    /// The admission valve's contract. A positive cap closes at or above itself;
    /// `0` disables it, so the actor keeps admitting no matter how many runs are
    /// active — the case that used to throttle forever because `active >= 0`
    /// always holds.
    #[test]
    fn zero_cap_never_closes_the_valve() {
        assert!(!at_inflight_cap(0, 64), "empty, well under the cap");
        assert!(!at_inflight_cap(63, 64), "one slot left");
        assert!(at_inflight_cap(64, 64), "at the cap");
        assert!(at_inflight_cap(65, 64), "over the cap");
        assert!(at_inflight_cap(1, 1), "a cap of one still caps");
        assert!(!at_inflight_cap(0, 0), "0 disables — not 'admit nothing'");
        assert!(!at_inflight_cap(10_000, 0), "0 disables at any depth");
        assert!(!at_inflight_cap(10_000, -1), "a negative cap is off too");
    }

    /// End-to-end ingest routing: an unparseable submission is dead-lettered (and
    /// acked off the source), while a valid one alongside it still becomes a run.
    #[tokio::test]
    async fn invalid_payload_is_dead_lettered_valid_one_runs() {
        let path = std::env::temp_dir().join(format!("m54-ingest-{}.db", uuid::Uuid::new_v4()));
        let pool = db::init_pool(path.to_str().unwrap()).await.unwrap();

        let (tx, rx) = tokio::sync::mpsc::channel(8);
        // Parses as YAML but not as a DagSpec → DagGraph::from_yaml errors.
        tx.send("just a string, not a dag".to_string()).await.unwrap();
        tx.send(
            "name: ok\ntasks:\n  - name: a\n    command: [\"true\"]\n".to_string(),
        )
        .await
        .unwrap();
        drop(tx); // sender closed → recv eventually yields None → actor stops

        let metrics = Arc::new(Metrics::new());
        let (_actor, handle) = IngestActor::spawn(
            None,
            IngestActor,
            IngestArgs {
                pool: pool.clone(),
                source: Box::new(ChannelSource::new(rx)),
                max_inflight_runs: 64,
                exhausted: Arc::new(AtomicBool::new(false)),
                metrics: Arc::clone(&metrics),
                source_name: "channel".to_string(),
                max_validation_attempts: 3,
            },
        )
        .await
        .unwrap();
        handle.await.unwrap(); // runs until the source drains, then stops

        let dead: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM dead_letters")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(dead, 1, "the invalid payload is dead-lettered");
        let runs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM workflow_runs")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(runs, 1, "the valid payload still became a run");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A positioned source for the exactly-once contract: every event carries a
    /// monotonically increasing coordinate; a fresh instance replays everything
    /// (a "restarted broker consumer" whose own acks were lost) until the actor
    /// hands it the datastore-committed cursor.
    struct CursorSource {
        events: Vec<(String, u64)>,
        committed: u64,
        idx: usize,
    }

    impl CursorSource {
        fn new(events: Vec<(String, u64)>) -> Self {
            Self { events, committed: 0, idx: 0 }
        }
    }

    #[async_trait]
    impl crate::source::WorkflowSource for CursorSource {
        async fn recv(&mut self) -> anyhow::Result<Option<WorkflowMessage>> {
            while let Some((_, pos)) = self.events.get(self.idx) {
                if *pos > self.committed {
                    let (payload, _) = &self.events[self.idx];
                    return Ok(Some(WorkflowMessage {
                        payload: payload.clone(),
                        handle: crate::source::AckHandle::None,
                    }));
                }
                self.idx += 1; // already committed — the replayed prefix
            }
            Ok(None) // drained
        }
        async fn ack(&mut self, _handle: &crate::source::AckHandle) -> anyhow::Result<()> {
            self.idx += 1;
            Ok(())
        }
        fn pending_position(&self) -> Option<crate::source::PendingPosition> {
            self.events
                .get(self.idx)
                .map(|(_, pos)| crate::source::PendingPosition::whole(pos.to_string()))
        }
        async fn set_committed_position(&mut self, position: Option<String>) -> anyhow::Result<()> {
            if let Some(p) = position {
                self.committed = p.parse().unwrap_or(0);
            }
            Ok(())
        }
    }

    /// Exactly-once: run + cursor commit atomically, so a full replay of the
    /// stream (the crash-redelivery case) creates zero duplicate runs and zero
    /// duplicate dead letters — the position table repositions the source past
    /// everything it already accounted for, poison included.
    #[tokio::test]
    async fn transactional_offsets_survive_full_replay_without_duplicates() {
        let path = std::env::temp_dir().join(format!("m54-eo-{}.db", uuid::Uuid::new_v4()));
        let pool = db::init_pool(path.to_str().unwrap()).await.unwrap();
        let events = || {
            vec![
                ("name: a\ntasks:\n  - name: t\n    command: [\"true\"]\n".to_string(), 1),
                ("torn line, not a workflow".to_string(), 2),
                ("name: b\ntasks:\n  - name: t\n    command: [\"true\"]\n".to_string(), 3),
            ]
        };
        let run_actor = |src: CursorSource| {
            let pool = pool.clone();
            async move {
                let (_a, handle) = IngestActor::spawn(
                    None,
                    IngestActor,
                    IngestArgs {
                        pool,
                        source: Box::new(src),
                        max_inflight_runs: 64,
                        exhausted: Arc::new(AtomicBool::new(false)),
                        metrics: Arc::new(Metrics::new()),
                        source_name: "cursor-test".to_string(),
                        max_validation_attempts: 3,
                    },
                )
                .await
                .unwrap();
                handle.await.unwrap();
            }
        };

        // First pass: both runs land, the poison parks, the cursor reaches 3.
        run_actor(CursorSource::new(events())).await;
        let runs: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM workflow_runs").fetch_one(&pool).await.unwrap();
        let dead: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM dead_letters").fetch_one(&pool).await.unwrap();
        assert_eq!((runs, dead), (2, 1));
        assert_eq!(
            db::source_offset(&pool, "cursor-test").await.unwrap().as_deref(),
            Some("3"),
            "cursor committed with the work it accounts for"
        );

        // "Crash" replay: a fresh source re-offers the entire stream. The actor
        // hands it the committed cursor; nothing is re-created or re-parked.
        run_actor(CursorSource::new(events())).await;
        let runs2: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM workflow_runs").fetch_one(&pool).await.unwrap();
        let dead2: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM dead_letters").fetch_one(&pool).await.unwrap();
        assert_eq!((runs2, dead2), (2, 1), "full replay creates no duplicates — exactly-once");

        pool.close().await;
        let _ = std::fs::remove_file(&path);
    }
}
