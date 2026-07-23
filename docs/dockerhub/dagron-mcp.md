# dagron MCP server (`mancube/dagron-mcp`)

**The open-source dagron MCP server — drive workflows from an AI agent over the [Model Context Protocol](https://modelcontextprotocol.io) (JSON-RPC on stdio).**

- **Image:** `mancube/dagron-mcp` — a Rust binary on **distroless/cc** (no shell, no package manager), runs as **nonroot** (uid 65532).
- **Arch:** `linux/amd64`, `linux/arm64`
- **Runtime:** stdio JSON-RPC (protocol `2024-11-05`) · **no ports** (launched by an MCP client as a subprocess)
- **Talks to:** `dagron-api` (the auth + management API), never the engine directly
- **Website:** dagron.dev · **Source / full docs:** github.com/lucheeseng827/dagron · Apache-2.0

## Tools

Eight tools over the same JWT-gated API the browser uses:

- **Drive** — `dagron_list_runs`, `dagron_get_run`, `dagron_submit_run`, `dagron_cancel_run`, `dagron_get_task_logs`
- **Observe** — `dagron_get_metrics`, `dagron_list_dead_letters`, `dagron_get_run_events`

## Tags

| Tag | Notes |
|---|---|
| `latest` | newest release |
| `0.4.3` | pinned version (= current `latest`) |

Pin in production: `mancube/dagron-mcp:0.4.3`.

## Run

Launched by an MCP client over stdio — protocol on stdout, logs on stderr:

```bash
docker run -i --rm \
  -e DAGRON_API_URL=http://host.docker.internal:8080 \
  -e DAGRON_MCP_TOKEN=<session-jwt> \
  mancube/dagron-mcp:0.4.3
```

MCP client (`mcpServers`) entry:

```json
{
  "mcpServers": {
    "dagron": {
      "command": "docker",
      "args": ["run", "-i", "--rm",
               "-e", "DAGRON_API_URL", "-e", "DAGRON_MCP_TOKEN",
               "mancube/dagron-mcp:0.4.3"],
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

> Mint a dedicated, least-privilege token for the agent — the agent's prompt is untrusted input that decides which tools get called. Point `DAGRON_API_URL` at the **`dagron-api` gateway**, never the engine's internal ops API.
