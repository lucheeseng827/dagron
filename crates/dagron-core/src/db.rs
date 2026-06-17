//! Datastore facade.
//!
//! The datastore *is* the scheduler: every query here is a state-machine
//! transition on `task_runs`, and the lease + version + status contract is the
//! whole correctness story. Exactly one backend is compiled in, selected by
//! Cargo feature:
//!
//! * `sqlite` (default) — zero-infra single-node path; optimistic CAS claim.
//! * `postgres` — v2 horizontal scale; `FOR UPDATE SKIP LOCKED` claim +
//!   `LISTEN/NOTIFY` event-driven wake for N coordination-free workers.
//!
//! Both backends expose the identical API — `Pool`, `Waker`, and the `create_run`
//! / `claim_ready` / `mark_task_*` / `is_run_complete` family — so `main.rs` and
//! the reconcile loop are backend-agnostic. Switching backends is a feature flag
//! plus a connection string, exactly as the design intends.

#[cfg(all(feature = "sqlite", feature = "postgres"))]
compile_error!("enable only one DB backend: `sqlite` or `postgres`, not both");

#[cfg(not(any(feature = "sqlite", feature = "postgres")))]
compile_error!("enable a DB backend: build with `--features sqlite` (default) or `--features postgres`");

#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "sqlite")]
pub use sqlite::*;

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "postgres")]
pub use postgres::*;
