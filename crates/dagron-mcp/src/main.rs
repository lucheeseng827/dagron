//! dagron-mcp — Model Context Protocol server over stdio.
//!
//! Reads newline-delimited JSON-RPC from stdin, dispatches via
//! [`dagron_mcp::handle_with_progress`], and writes responses to stdout. **Logs go
//! to stderr** — stdout is the protocol channel and must carry only JSON-RPC.
//! Config: `DAGRON_API_URL`, `DAGRON_MCP_TOKEN`.
//!
//! Every line written to stdout goes through one queue and one writer task. A
//! `notifications/progress` is produced while a call is still being handled, so
//! it cannot be written by the loop that writes the reply; sharing the queue
//! keeps the two from interleaving mid-line and keeps a notification ahead of
//! the reply it belongs to.

use dagron_mcp::{handle_with_progress, progress_notification, DagronClient, ProgressSink};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

/// Queues a progress notification for the stdout writer.
struct QueueSink(UnboundedSender<Value>);

impl ProgressSink for QueueSink {
    fn send(&self, token: &Value, progress: u64, total: u64) {
        // The writer only goes away when stdout failed, and the main loop
        // reports that; a lost notification is not a second error.
        let _ = self.0.send(progress_notification(token, progress, total));
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let client = DagronClient::from_env()?;
    let (out, mut queue) = unbounded_channel::<Value>();
    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(msg) = queue.recv().await {
            let mut line = serde_json::to_string(&msg)?;
            line.push('\n');
            stdout.write_all(line.as_bytes()).await?;
            stdout.flush().await?;
        }
        anyhow::Ok(())
    });
    let sink = QueueSink(out.clone());
    let mut lines = BufReader::new(tokio::io::stdin()).lines();

    tracing::info!("dagron-mcp server started (stdio)");
    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let reply = match serde_json::from_str::<Value>(line) {
            Ok(msg) => handle_with_progress(&client, &msg, &sink).await,
            Err(e) => {
                tracing::warn!(error = %e, "malformed JSON-RPC line");
                // Reply with a JSON-RPC parse error so the client isn't left waiting.
                Some(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": { "code": -32700, "message": "parse error" }
                }))
            }
        };
        if let Some(reply) = reply {
            if out.send(reply).is_err() {
                // The writer stopped, which only a stdout error does: report it.
                break;
            }
        }
    }
    drop(sink);
    drop(out);
    writer.await?
}
