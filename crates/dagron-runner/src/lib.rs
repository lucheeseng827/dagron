//! `dagron-runner`: dagron's remote runner.
//!
//! It dials dagron-api over HTTPS (`/runner/v1`), takes the tasks of the
//! runner classes it serves, runs them on dagron-executor's executors (a
//! process here, a container on this host's Docker, or a pod: the runner as
//! an in-cluster controller), streams their logs and says how each came out.
//! It holds no database credentials and owns no state: one that dies loses
//! what it was running, which dagron-api takes back once its session lapses.
//!
//! - [`config`]: what it is set with.
//! - [`client`]: the protocol, as it calls it.
//! - [`host`]: its executor, and a task made ready for it.
//! - [`runner`]: the run loop.
//! - [`scale`]: sizing runners by the work there is.

pub mod client;
pub mod config;
pub mod host;
pub mod runner;
pub mod scale;
