# dagron MCP server (`mancube/dagron-mcp`)

**The open-source dagron MCP server — drive workflows from an AI agent over the [Model Context Protocol](https://modelcontextprotocol.io) (JSON-RPC on stdio).**

- **Image:** `mancube/dagron-mcp` — a Rust binary on **distroless/cc** (no shell, no package manager), runs as **nonroot** (uid 65532).
- **Arch:** `linux/amd64`, `linux/arm64`
- **Runtime:** stdio JSON-RPC (protocol `2024-11-05`) · **no ports** (launched by an MCP client as a subprocess)
- **Talks to:** `dagron-api` (the auth + management API), never the engine directly
- **Website:** dagron.dev · **Source / full docs:** github.com/lucheeseng827/dagron · Apache-2.0

## Tools

Forty-two tools over the same JWT-gated API the browser uses:

- **Drive** — `dagron_list_runs`, `dagron_get_run`, `dagron_submit_run` (with `parameters` + `Idempotency-Key`), `dagron_cancel_run`, `dagron_wait_run`
- **Author** — `dagron_create_workflow`, `dagron_update_workflow`, `dagron_delete_workflow`, `dagron_set_workflow_state`, `dagron_run_workflow`, `dagron_list_workflows`, `dagron_get_workflow`, `dagron_list_workflow_runs`, `dagron_list_workflow_versions`
- **Recover** — `dagron_rerun_run`, `dagron_resubmit_run`, `dagron_retry_task`, `dagron_clear_task`, `dagron_redrive_dead_letter`, `dagron_delete_dead_letter`, `dagron_triage_run`, `dagron_clear_triage`
- **Gates** — `dagron_list_approvals`, `dagron_approve_task`, `dagron_reject_task`
- **Inspect** — `dagron_get_run_logs`, `dagron_get_task_logs`, `dagron_get_run_graph`, `dagron_get_run_spec`, `dagron_get_artifact`, `dagron_artifact_exists`, `dagron_put_artifact`
- **Observe** — `dagron_get_metrics`, `dagron_get_metrics_timeseries`, `dagron_get_health`, `dagron_search`, `dagron_list_dead_letters`, `dagron_get_run_events`
- **Lineage & archive** — `dagron_list_datasets`, `dagron_get_dataset_events`, `dagron_list_archived_runs`, `dagron_get_archived_run`

Eighteen of them change cluster state. Set `DAGRON_MCP_READONLY=1` to hide and
refuse all eighteen, leaving the 24 read tools.

## Tags

| Tag | Notes |
|---|---|
| `latest` | newest release |
| `0.9.1` | pinned version (= current `latest`) |
| `0.9` | floating minor — newest `0.9.x` |

Pin in production — pick the newest published tag rather than copying a version
from this page, which ages.

> The 42-tool catalogue above ships in **0.9.0 and later**. `0.7.0` and `0.8.x`
> carry the earlier nine-tool surface.

## Run

Launched by an MCP client over stdio — protocol on stdout, logs on stderr:

```bash
docker run -i --rm \
  -e DAGRON_API_URL=http://host.docker.internal:8080 \
  -e DAGRON_MCP_TOKEN=<session-jwt> \
  mancube/dagron-mcp:latest
```

MCP client (`mcpServers`) entry:

```json
{
  "mcpServers": {
    "dagron": {
      "command": "docker",
      "args": ["run", "-i", "--rm",
               "-e", "DAGRON_API_URL", "-e", "DAGRON_MCP_TOKEN",
               "mancube/dagron-mcp:latest"],
      "env": {
        "DAGRON_API_URL": "http://host.docker.internal:8080",
        "DAGRON_MCP_TOKEN": "<session-jwt>"
      }
    }
  }
}
```

## Configuration

| Env | Purpose |
|---|---|
| `DAGRON_API_URL` | `dagron-api` base URL (default `http://localhost:8080`) |
| `DAGRON_MCP_TOKEN` | session JWT sent as `Authorization: Bearer …` |
| `DAGRON_MCP_READONLY` | `1` hides **and** refuses every write tool |
| `DAGRON_MCP_MAX_ARTIFACT_BYTES` | largest artifact returned inline (default `262144`) |
| `DAGRON_MCP_ALLOW_PLAINTEXT_TOKEN` | `1` permits the token over plaintext `http://` to a remote host (refused otherwise) |

> Mint a dedicated, least-privilege token for the agent — the agent's prompt is untrusted input that decides which tools get called. Point `DAGRON_API_URL` at the **`dagron-api` gateway**, never the engine's internal ops API.
