//! dagron core — the foundation shared by the engine, the API gateway and the
//! operator.
//!
//! * [`archive`] — durable writes to a local archive sink, shared by the
//!   engine's retention sweep and the API's per-run archive route.
//! * [`attempt_log`] — how much of a superseded attempt's output to keep, so a
//!   loop's log shows every iteration without multiplying an unbounded value.
//! * [`dag`] — the DAG model, YAML parsing + validation, the run graph.
//! * [`expand`] — matrix / call-task expansion into leaf tasks.
//! * [`fault`] — the fault-attribution taxonomy: what a failure *was*, and
//!   whether another attempt is worth anything.
//! * [`models`] — datastore row types + status enums shared across the API.
//! * [`db`] — the datastore facade (one backend compiled in: `sqlite` | `postgres`).
//! * [`metrics`] — the process metrics registry rendered at `GET /metrics`.
//!
//! Nothing here knows *how* a task runs (see `dagron-executor`) or *where* a
//! workflow submission comes from (see `dagron-source`).

/// Serializes every test that writes process environment variables against
/// every test that reads them.
///
/// `std::env::set_var` is process-global and `cargo test` runs a crate's tests
/// on threads of one process, so a test that lowers `DAGRON_MAX_TASKS_PER_RUN`
/// to prove a refusal silently imposes that ceiling on whatever else is mid-run
/// — which is how a correct feature test fails with a budget error it never
/// asked for. One lock rather than one per key: over-serializing a handful of
/// tests costs milliseconds, and a per-key lock has to be right about which
/// keys a call path reads, which is exactly the thing nobody rechecks.
#[cfg(test)]
pub(crate) fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

pub mod archive;
/// Retention policy for superseded attempts' output (`task_attempts`).
pub mod attempt_log;
pub mod clock;
pub mod dag;
pub mod db;
pub mod expand;
pub mod fault;
pub mod isolation;
/// Reading a verdict out of a vendor's JSON status response (`defer.http`).
pub mod jsonpred;
pub mod metrics;
pub mod models;
