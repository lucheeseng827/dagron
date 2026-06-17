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
    /// `count_active_runs() >= max_inflight_runs`.
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
        args: IngestArgs,
    ) -> Result<IngestState, ActorProcessingErr> {
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
        if active >= state.max_inflight_runs {
            // SAFETY: ractor runs actors on tokio tasks; this sleep yields.
            tokio::time::sleep(THROTTLE).await;
            myself.cast(IngestMsg::Poll)?;
            return Ok(());
        }

        // ── Pull one submission and turn it into a run ─────────────────────────
        match state.source.recv().await {
            Ok(Some(message)) => {
                match DagGraph::from_yaml(&message.payload) {
                    Ok(dag) => match db::create_run(&state.pool, &dag, &message.payload).await {
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
                            let count =
                                state.failures.entry(message.payload.clone()).or_insert(0);
                            *count += 1;
                            if *count >= state.max_validation_attempts {
                                let failures = *count;
                                state.failures.remove(&message.payload);
                                dead_letter(state, &message, &e.to_string(), failures).await;
                            } else {
                                warn!(error = %e, attempt = *count, "create_run failed — nacking for redelivery");
                                if let Err(e) = state.source.nack(&message.handle).await {
                                    warn!(error = %e, "nack failed — message redelivers after timeout");
                                }
                            }
                        }
                    },
                    // A parse failure is deterministic — redelivering the same
                    // bytes can never succeed, so dead-letter it immediately.
                    Err(e) => {
                        dead_letter(state, &message, &format!("invalid workflow spec: {e}"), 1).await;
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
                warn!(error = %e, "source recv error — retrying after backoff");
                tokio::time::sleep(ERROR_BACKOFF).await;
                myself.cast(IngestMsg::Poll)?;
            }
        }

        Ok(())
    }
}

/// Park a poison submission in the dead-letter store, then ack it off the source
/// so it stops redelivering. A failure to persist or ack must not kill the actor
/// (that would stall all ingestion); the worst case on a persist failure is the
/// message redelivers and is retried, so log and carry on.
async fn dead_letter(state: &mut IngestState, message: &WorkflowMessage, error: &str, failures: i64) {
    match db::record_dead_letter(&state.pool, &message.payload, error, &state.source_name, failures)
        .await
    {
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
}
