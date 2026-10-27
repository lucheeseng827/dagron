//! DuckDB, through its own CLI.
//!
//! DuckDB is here as an *engine*, not as a store: it is what reads and writes
//! Parquet for a store that cannot, above all Postgres on-prem, which it attaches
//! with its `postgres` extension. So it gets the two modes a copy needs, `exec` and
//! `script`, and nothing that keeps rows.
//!
//! The CLI rather than the library: linking DuckDB means compiling its C++ into
//! every build of this step, for an engine most of its tasks never touch. The CLI
//! is one static binary that a copy task's image carries, like the vendor clients
//! this step otherwise replaces, and its version moves without a rebuild here.
//!
//! * **The script goes on stdin**, never on the command line, where every process
//!   listing would show it.
//! * **`-bail`**: the first failing statement ends the run, with a non-zero exit.
//!   Closing the process then rolls back whatever transaction the script had open,
//!   in DuckDB and in a Postgres it attached.
//! * **stdout is read and dropped.** The CLI prints a table for every statement
//!   with a result (a `COPY`'s row count, say), and `exec`/`script` write nothing
//!   to stdout: that is the contract, and stdout is the task's uncapped `output`.
//! * **stderr is kept, bounded**, because it carries the error.
//! * **The password, if set, becomes `PGPASSWORD`.** The only credential DuckDB
//!   needs here is the attached Postgres's, and libpq reads it from there. The rest
//!   of the connection is `PGHOST`, `PGPORT`, `PGDATABASE` and `PGUSER`, which the
//!   task sets and the CLI inherits. The SQL names no host and holds no secret.
//! * **The deadline kills it.** `SQL_TIMEOUT_SECS` bounds the run, and a CLI still
//!   running at it is killed, which ends its statement.

use std::process::Stdio;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use dagron_step_sql::{Mode, Settings};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

/// The most of the CLI's stderr kept for the error message.
const MAX_STDERR_BYTES: usize = 8 * 1024;

pub async fn run(settings: &Settings, statement: &str, mode: Mode) -> Result<()> {
    if matches!(mode, Mode::Scalar | Mode::Rows) {
        bail!(
            "SQL_ENGINE=duckdb runs `exec` and `script` only. DuckDB is here as the engine that \
             reads and writes Parquet for a copy, so nothing it runs is kept: rows and scalars \
             are not built for it."
        );
    }
    let bin = std::env::var("SQL_DUCKDB")
        .ok()
        .map(|b| b.trim().to_string())
        .filter(|b| !b.is_empty())
        .unwrap_or_else(|| "duckdb".into());

    let mut cmd = tokio::process::Command::new(&bin);
    cmd.arg("-bail")
        .arg(&settings.dsn)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(pw) = &settings.password {
        cmd.env("PGPASSWORD", pw);
    }
    let mut child = cmd.spawn().with_context(|| {
        format!(
            "starting the DuckDB CLI ('{bin}'). The copy task's image needs the `duckdb` binary on \
             PATH, or SQL_DUCKDB naming where it is."
        )
    })?;

    let mut stdin = child.stdin.take().expect("stdin was piped");
    let stdout = child.stdout.take().expect("stdout was piped");
    let stderr = child.stderr.take().expect("stderr was piped");
    // Both pipes are drained while the script runs: a CLI blocked writing a full
    // pipe would otherwise look exactly like a slow statement until the deadline.
    let out = tokio::spawn(drain(stdout, 0));
    let err = tokio::spawn(drain(stderr, MAX_STDERR_BYTES));

    let script = format!("{}\n", statement.trim_end());
    // Moved in and dropped: end-of-input is the pipe closing, and `shutdown` on a
    // child's stdin does not close it. A CLI never told the script has ended waits
    // for the rest of it until the deadline.
    let feed = async move {
        stdin.write_all(script.as_bytes()).await?;
        drop(stdin);
        Ok::<(), std::io::Error>(())
    };
    let deadline = Duration::from_secs(settings.timeout_secs);
    let waited = tokio::time::timeout(deadline, async {
        // A CLI that exits early (a parse error on line one) closes stdin before the
        // rest of a long script is written. That is not this step's failure; its
        // exit status says what happened.
        let _ = feed.await;
        child.wait().await
    })
    .await;

    let status = match waited {
        Ok(status) => status.context("waiting for the DuckDB CLI")?,
        Err(_) => {
            let _ = child.kill().await;
            // Not awaited: a process the CLI started could hold the pipes open past
            // the kill, and the deadline has already passed.
            out.abort();
            err.abort();
            bail!(
                "the DuckDB script was still running at {} ({}s), so the CLI was killed and its \
                 open transaction rolled back. Raise {} if it is meant to run this long.",
                settings.var("TIMEOUT_SECS"),
                settings.timeout_secs,
                settings.var("TIMEOUT_SECS")
            );
        }
    };
    let (dropped, _) = out.await.unwrap_or_default();
    let (_, stderr) = err.await.unwrap_or_default();
    if !status.success() {
        let code = status.code().map(|c| c.to_string()).unwrap_or_else(|| "a signal".into());
        bail!(
            "the DuckDB script failed (exit {code}). It stops at the first failing statement, and \
             any transaction it had open was rolled back. DuckDB said: {}",
            String::from_utf8_lossy(&stderr).trim()
        );
    }
    tracing::info!(stdout_bytes_dropped = dropped, "DuckDB script executed");
    Ok(())
}

/// Read a pipe to its end, keeping at most `keep` bytes of it. Returns how many
/// bytes there were and the kept prefix.
async fn drain(mut pipe: impl AsyncRead + Unpin, keep: usize) -> (u64, Vec<u8>) {
    let mut kept = Vec::new();
    let mut total = 0u64;
    let mut buf = [0u8; 8192];
    loop {
        match pipe.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                total += n as u64;
                let room = keep.saturating_sub(kept.len());
                kept.extend_from_slice(&buf[..n.min(room)]);
            }
        }
    }
    (total, kept)
}
