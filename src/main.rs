// SPDX-License-Identifier: Apache-2.0
//! `dagron` CLI — run a DAG locally with the subprocess executor.
//!
//! ```text
//! dagron run <file.yaml>
//! ```

use std::sync::Arc;

use anyhow::{bail, Result};
use dagron::{run_dag, FileSource, LocalExecutor, RunReport, TaskState, WorkflowSource};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("run") => {
            let path = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("usage: dagron run <file.yaml>"))?;
            let mut source = FileSource::new(path);
            let executor = Arc::new(LocalExecutor);

            let mut any_failed = false;
            while let Some(msg) = source.recv().await? {
                let report = run_dag(&msg.payload, executor.clone()).await?;
                print_report(&report);
                if report.succeeded {
                    source.ack(&msg.handle).await?;
                } else {
                    any_failed = true;
                    source.nack(&msg.handle).await?;
                }
            }
            if any_failed {
                std::process::exit(1);
            }
            Ok(())
        }
        Some("--help") | Some("-h") | None => {
            println!("dagron — a small, durable DAG workflow runner\n\nUSAGE:\n    dagron run <file.yaml>");
            Ok(())
        }
        Some(other) => bail!("unknown command '{other}'. usage: dagron run <file.yaml>"),
    }
}

fn print_report(report: &RunReport) {
    println!(
        "\nDAG '{}': {}",
        report.dag,
        if report.succeeded {
            "SUCCEEDED"
        } else {
            "FAILED"
        }
    );
    for t in &report.tasks {
        let marker = match t.state {
            TaskState::Succeeded => "✓",
            TaskState::Failed => "✗",
            TaskState::Skipped => "–",
        };
        println!(
            "  {marker} {:<24} {} (attempts: {})",
            t.name, t.state, t.attempts
        );
    }
}
