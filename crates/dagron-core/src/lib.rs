//! dagron core — the foundation shared by the engine, the API gateway and the
//! operator.
//!
//! * [`archive`] — durable writes to a local archive sink, shared by the
//!   engine's retention sweep and the API's per-run archive route.
//! * [`attempt_log`] — how much of a superseded attempt's output to keep, so a
//!   loop's log shows every iteration without multiplying an unbounded value.
//! * [`cloudevents`] — the CloudEvents 1.0 envelope every outbound webhook
//!   carries, in either of the HTTP binding's content modes.
//! * [`dag`] — the DAG model, YAML parsing + validation, the run graph.
//! * [`expand`] — matrix / call-task expansion into leaf tasks.
//! * [`fault`] — the fault-attribution taxonomy: what a failure *was*, and
//!   whether another attempt is worth anything.
//! * [`quota_wait`] — the back-off and the bound for a task whose pod a full
//!   Kubernetes namespace quota refused: when it probes again, when it stops.
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
/// A task's way into its run's artifacts over dagron-api's API
/// (`artifact_api: true`), and the credential it is given for it.
pub mod artifact_api;
/// Retention policy for superseded attempts' output (`task_attempts`).
pub mod attempt_log;
pub mod clock;
pub mod cloudevents;
pub mod confine;
/// A process's connections against its database's, login's and server's
/// limits.
pub mod connections;
pub mod dag;
pub mod db;
/// A Docker daemon of a task's own, beside it (`docker_daemon: true`).
pub mod docker_daemon;
pub mod envpolicy;
pub mod expand;
pub mod fault;
pub mod forge_token;
/// The `/metrics` datastore gauges in two tiers, each cached on its own.
#[cfg(feature = "ops")]
pub mod gauges;
pub mod isolation;
/// Reading a verdict out of a vendor's JSON status response (`defer.http`).
pub mod jsonpred;
pub mod metrics;
pub mod models;
pub mod oidc;
/// A task that asks to be privileged, on a dedicated pool alone.
pub mod privileged;
pub mod protected;
pub mod quota_wait;
/// The remote runner protocol's wire types.
pub mod runner;
/// Which stores a task may read its secrets from, and reading them.
pub mod secrets;
/// The containers a task has beside it while it runs (`services:`).
pub mod service;
/// The reconcile loop's sweeps, by name, scope and default cadence.
pub mod sweep;
/// The model tokens a task reports it spent; and, in the enterprise build,
/// the meter and the budgets held on it (`budget: { tokens }`).
pub mod tokens;
/// The directory a task starts in, made for each attempt (`workspace:`).
pub mod workspace;
