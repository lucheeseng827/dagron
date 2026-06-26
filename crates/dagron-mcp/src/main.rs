//! dagron-mcp — Model Context Protocol server over stdio.
//!
//! Reads newline-delimited JSON-RPC from stdin, dispatches via [`dagron_mcp::handle`],
//! and writes responses to stdout. **Logs go to stderr** — stdout is the protocol
//! channel and must carry only JSON-RPC. Config: `DAGRON_API_URL`, `DAGRON_MCP_TOKEN`.

use dagron_mcp::{handle, DagronClient};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let client = DagronClient::from_env();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();

    tracing::info!("dagron-mcp server started (stdio)");
    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let msg: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "malformed JSON-RPC line");
                // Reply with a JSON-RPC parse error so the client isn't left waiting.
                let err_resp = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": { "code": -32700, "message": "parse error" }
                });
                let mut out = serde_json::to_string(&err_resp)?;
                out.push('\n');
                stdout.write_all(out.as_bytes()).await?;
                stdout.flush().await?;
                continue;
            }
        };
        if let Some(resp) = handle(&client, &msg).await {
            let mut out = serde_json::to_string(&resp)?;
            out.push('\n');
            stdout.write_all(out.as_bytes()).await?;
            stdout.flush().await?;
        }
    }
    Ok(())
}
