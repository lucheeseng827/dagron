//! dagron core — the foundation shared by the engine, the API gateway and the
//! operator.
//!
//! * [`dag`] — the DAG model, YAML parsing + validation, the run graph.
//! * [`expand`] — matrix / call-task expansion into leaf tasks.
//! * [`models`] — datastore row types + status enums shared across the API.
//! * [`db`] — the datastore facade (one backend compiled in: `sqlite` | `postgres`).
//! * [`metrics`] — the process metrics registry rendered at `GET /metrics`.
//!
//! Nothing here knows *how* a task runs (see `dagron-executor`) or *where* a
//! workflow submission comes from (see `dagron-source`).

pub mod dag;
pub mod db;
pub mod expand;
pub mod metrics;
pub mod models;
