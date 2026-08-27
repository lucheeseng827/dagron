//! dagron-step-mcp — call an MCP tool as a workflow task.
//!
//! Invoked as a task `command`. Spawns the MCP server named by
//! `DAGRON_MCP_STEP_SERVER`, calls `DAGRON_MCP_STEP_TOOL` with
//! `DAGRON_MCP_STEP_ARGS` (or `DAGRON_MCP_STEP_ARGS_FILE`, `-` = stdin), and
//! writes the result to `DAGRON_MCP_STEP_OUTPUT` (or stdout). Exits non-zero on
//! failure so the engine retries the task. See [`dagron_step_mcp`].

use std::io::Read;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use dagron_step_mcp::{parse_timeout_secs, run_step, StepConfig};

#[tokio::main]
async fn main() -> Result<()> {
    dagron_logging::init("dagron-step-mcp");

    // Establish the deadline *before* the first blocking read. Arguments can
    // arrive on stdin or a FIFO (`DAGRON_MCP_STEP_ARGS_FILE=-`), which can block
    // indefinitely, and a timeout that only started at the tool call would never
    // bound that. The whole step — argument read included — shares one deadline.
    let timeout = Duration::from_secs(parse_timeout_secs(
        std::env::var("DAGRON_MCP_STEP_TIMEOUT_SECS").ok(),
    )?);
    let deadline = tokio::time::Instant::now() + timeout;

    // The read is blocking, so it runs on a blocking thread and is bounded by the
    // shared deadline. On expiry we abandon it and fail; the thread ends when the
    // process does.
    let arguments =
        tokio::time::timeout_at(deadline, tokio::task::spawn_blocking(resolve_arguments))
            .await
            .with_context(|| {
                format!("reading the tool arguments did not finish within {timeout:?}")
            })?
            .context("the argument-reading task panicked")?
            .context("resolving the tool arguments")?;

    let cfg = StepConfig::from_env(&arguments)?;

    // Idempotency, the same guard the LLM step carries: if a prior attempt wrote
    // the output and then died before exiting 0, reuse it rather than calling
    // the tool again. That matters more here than for a completion — an MCP tool
    // may well not be read-only, and a retry that re-sends the same write is a
    // duplicate side effect, not a duplicate charge. Stdout mode has no prior
    // artifact to check, so only file output gets this. The write below is
    // atomic, so a non-empty file is always a *complete* prior result.
    if let Some(path) = &cfg.output {
        if std::fs::metadata(path).map(|m| m.len() > 0).unwrap_or(false) {
            tracing::info!(path, "output already present from a prior attempt; not calling again");
            return Ok(());
        }
    }

    let result = run_step(&cfg, deadline).await.context("MCP step failed")?;

    // A tool that reports `isError` failed. Fail the task with its text so the
    // engine's retry and the run's failure summary both see the reason.
    //
    // Deliberately *before* any write: writing the error to the output file
    // would satisfy the idempotency check above, and the retry this failure is
    // meant to trigger would skip the call and succeed on a stale error.
    if result.is_error {
        bail!("the MCP tool {:?} reported an error: {}", cfg.tool, result.text);
    }

    match &cfg.output {
        Some(path) => {
            write_atomically(path, &result.text)?;
            tracing::info!(path, bytes = result.text.len(), tool = %cfg.tool, "MCP step wrote result");
        }
        None => println!("{}", result.text),
    }
    Ok(())
}

/// Write `contents` to `path` atomically: to a sibling temp file, then rename
/// it into place.
///
/// The output file is this step's idempotency marker — a non-empty one makes a
/// retry skip the call (see `main`). A direct `fs::write` interrupted mid-way
/// leaves a *partial* non-empty file, which a retry would then accept as a
/// finished side effect. Writing to a sibling and renaming means `path` only
/// ever appears complete: a crash leaves the temp (or nothing), never half of
/// `path`. The temp name is deterministic, so a killed attempt's leftover is
/// overwritten by the next rather than accumulating.
fn write_atomically(path: &str, contents: &str) -> Result<()> {
    let dest = std::path::Path::new(path);
    let dir = match dest.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        // No parent component (a bare filename) resolves against the cwd.
        _ => std::path::PathBuf::from("."),
    };
    let name = dest.file_name().and_then(|n| n.to_str()).unwrap_or("output");
    // A sibling in the same directory, so it is on the same filesystem and the
    // rename is atomic (a cross-filesystem rename is a copy, which is not).
    let tmp = dir.join(format!(".{name}.partial"));
    std::fs::write(&tmp, contents)
        .with_context(|| format!("writing output to the temporary file {}", tmp.display()))?;
    std::fs::rename(&tmp, dest)
        .with_context(|| format!("renaming {} into place at {path}", tmp.display()))?;
    Ok(())
}

/// The tool arguments, from `DAGRON_MCP_STEP_ARGS_FILE` if set (`-` = stdin),
/// else `DAGRON_MCP_STEP_ARGS`, else empty.
///
/// The file form exists because arguments are frequently produced by an upstream
/// task, and an artifact path is how a task hands something to the next one; a
/// large JSON body also has no business being an environment variable.
fn resolve_arguments() -> Result<String> {
    match std::env::var("DAGRON_MCP_STEP_ARGS_FILE").ok().filter(|p| !p.trim().is_empty()) {
        Some(path) if path.trim() == "-" => {
            let mut buf = String::new();
            std::io::stdin().read_to_string(&mut buf).context("reading arguments from stdin")?;
            Ok(buf)
        }
        Some(path) => std::fs::read_to_string(path.trim())
            .with_context(|| format!("reading arguments from {}", path.trim())),
        None => Ok(std::env::var("DAGRON_MCP_STEP_ARGS").unwrap_or_default()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The output file must only ever be observed complete, because a non-empty
    /// one is the retry's "already done" marker. The temp file is the write; the
    /// rename is the commit.
    #[test]
    fn write_atomically_commits_the_whole_file_and_leaves_no_temp() {
        let dir = std::env::temp_dir().join(format!("dagron-step-mcp-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let out = dir.join("result.txt");

        write_atomically(out.to_str().unwrap(), "the whole result").unwrap();
        assert_eq!(std::fs::read_to_string(&out).unwrap(), "the whole result");
        // The deterministic temp sibling must not survive a successful write.
        assert!(!dir.join(".result.txt.partial").exists(), "the temp file was not renamed away");

        // A second write (a retry that ran anyway) replaces the contents wholesale.
        write_atomically(out.to_str().unwrap(), "replaced").unwrap();
        assert_eq!(std::fs::read_to_string(&out).unwrap(), "replaced");

        std::fs::remove_dir_all(&dir).ok();
    }
}
