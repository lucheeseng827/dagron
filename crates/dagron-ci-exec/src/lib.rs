//! dagron-ci-exec: the step runner of a dagron-ci task.
//!
//! dagron-ci compiles a task's `steps:` into environment variables, the step
//! protocol ([`protocol`]), and a command that runs them. That command runs
//! this binary when the image provides one that speaks the protocol (at
//! `/dagron/bin/dagron-ci-exec`, or on `PATH`), and a POSIX `sh` script of
//! its own when it does not. The two implement one protocol, held to the same
//! output by a conformance test; the binary adds what a script cannot do
//! everywhere:
//!
//! - per-step timeouts without `timeout(1)` in the image;
//! - a cancelled task's signal passed on to the running step;
//! - what a step leaves running, killed when the task ends;
//! - a result ([`result`]): each step's outcome, logged as the last line and
//!   written to `CI_RESULT_FILE` when it is set.
//!
//! Both write the step markers to stderr, so a task's output, its stdout, is
//! its steps' stdout alone.
//!
//! **Step protocol 2** is for the steps of a job that came from a GitHub
//! workflow ([`workflow`]): each has a shell, a directory and a condition
//! of its own, and expressions that are read as the job runs. Only this
//! binary speaks it; there is no shell fallback for it. Version 3 adds what
//! a job is beside its steps, and version 4 a step that uses a local
//! action, whose steps this binary reads from the workspace and runs.
//!
//! The binary has one more job, which is no step's: [`gate`], what the
//! Kubernetes executor runs before a task that asked for no network, to see
//! that its pod has none.

/// A job's artifacts, through the engine's artifact service.
pub mod artifacts;
pub mod container;
pub mod gate;
/// `hashFiles`, of the job's workspace.
pub mod hash_files;
/// GitHub's workflow commands as a job prints them: masks and annotations.
#[cfg(unix)]
pub mod commands;
pub mod job;
pub mod plan;
pub mod protocol;
pub mod report;
pub mod result;
#[cfg(unix)]
pub mod run;
#[cfg(unix)]
pub mod workflow;

pub use protocol::{read, Job, Step, PROTOCOL, PROTOCOLS};
