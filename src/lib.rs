// SPDX-License-Identifier: Apache-2.0
//! # dagron
//!
//! A small, durable DAG workflow runner. The public surface is two pluggable
//! contracts plus zero-infra reference implementations:
//!
//! * [`Executor`] — *how a task runs*. Ships [`LocalExecutor`] (subprocess).
//! * [`WorkflowSource`] — *where workflows come from*. Ships [`FileSource`] and
//!   [`ChannelSource`].
//!
//! [`run_dag`] is the in-memory reference scheduler: dependency-driven concurrent
//! execution with retries and downstream skip-on-failure.
//!
//! ```no_run
//! use std::sync::Arc;
//! use dagron::{run_dag, LocalExecutor};
//!
//! # async fn demo() -> anyhow::Result<()> {
//! let yaml = r#"
//! name: hello
//! tasks:
//!   - { name: greet, command: ["echo", "hello"] }
//! "#;
//! let report = run_dag(yaml, Arc::new(LocalExecutor)).await?;
//! assert!(report.succeeded);
//! # Ok(())
//! # }
//! ```
#![forbid(unsafe_code)]

pub mod dag;
pub mod executor;
pub mod runner;
pub mod source;

pub use dag::{DagGraph, DagSpec, TaskSpec};
pub use executor::{ExecContext, ExecOutput, Executor, LocalExecutor};
pub use runner::{run_dag, RunReport, TaskReport, TaskState};
pub use source::{AckHandle, ChannelSource, FileSource, WorkflowMessage, WorkflowSource};
