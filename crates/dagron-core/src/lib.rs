//! dagron core — the foundation shared by the engine, the API gateway and the
//! operator.
//!
//! * [`archive`] — durable writes to a local archive sink, shared by the
//!   engine's retention sweep and the API's per-run archive route.
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

pub mod archive;
pub mod clock;
pub mod dag;
pub mod db;
pub mod expand;
pub mod fault;
pub mod metrics;
pub mod models;
