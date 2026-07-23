# dagron MCP server

`dagron-mcp` exposes the dagron management API as [Model Context Protocol](https://modelcontextprotocol.io)
tools over stdio, so an AI agent (Claude Desktop, an IDE, or any MCP client) can
drive workflows — submit, list, inspect, cancel runs and read task logs — and
observe the cluster's live state.

The MCP engine is the **agent-facing seam** to the Dagron cluster: it speaks the
same JWT-gated `dagron-api` the browser uses, so all access controls, validation,
and observability already enforced on the UI edge apply identically to agents.
See the [agent event-call sequence](ARCHITECTURE.md#58-mcp-agent-event-call--submit--bounded-sse-event-poll)
and the [system-context diagram](ARCHITECTURE.md#1-system-context) in
[`docs/ARCHITECTURE.md`](ARCHITECTURE.md).

## Tools

### Drive workflows

| Tool | Arguments | dagron-api |
|---|---|---|
| `dagron_list_runs` | — | `GET /api/runs` |
| `dagron_get_run` | `run_id` | `GET /api/runs/{id}` |
| `dagron_submit_run` | `yaml` | `POST /api/runs` |
| `dagron_cancel_run` | `run_id` | `POST /api/runs/{id}/cancel` |
| `dagron_get_task_logs` | `run_id`, `task_id` | `GET /api/runs/{id}/tasks/{tid}/logs` |

### Observe cluster internals

So the agent can reason about what the engine is doing, not just send commands:

| Tool | Arguments | dagron-api |
|---|---|---|
| `dagron_get_metrics` | — | `GET /api/metrics` (runs/tasks by status + dead-letter total) |
| `dagron_list_dead_letters` | `limit` (1..=500, default 100) | `GET /api/dead-letters?limit=` |
| `dagron_get_run_events` | `run_id`, `wait_ms` (100..=10000, default 2000) | bounded read of `GET /api/runs/{id}/stream` (SSE) |

`dagron_get_run_events` opens the same per-run SSE channel the browser uses,
collects events emitted within `wait_ms`, parses the SSE frames into JSON, and
returns them as a single tool response. The window is hard-capped at 10 s
wall-clock and 256 KiB so a JSON-RPC call always returns promptly — agents
poll the tool in a loop instead of holding a long-lived stream.

## Configuration

| Env | Purpose |
|---|---|
| `DAGRON_API_URL` | dagron-api base URL (default `http://localhost:8080`) |
| `DAGRON_MCP_TOKEN` | optional session JWT, sent as `Authorization: Bearer …` |

## Run

```sh
cargo run -p dagron-mcp        # speaks JSON-RPC over stdio
```

Register it with an MCP client (example `mcpServers` entry):

```json
{
  "mcpServers": {
    "dagron": {
      "command": "dagron-mcp",
      "env": {
        "DAGRON_API_URL": "http://localhost:8080",
        "DAGRON_MCP_TOKEN": "<session-jwt>"
      }
    }
  }
}
```

The transport is newline-delimited JSON-RPC 2.0 on stdio (protocol `2024-11-05`);
logs go to **stderr** so stdout carries only protocol messages.

## Security best practices

`dagron-mcp` runs as a per-agent stdio subprocess that holds a bearer token for
`dagron-api`. Treat the MCP server like any other client of your management API
— the agent's prompt is *untrusted input* that influences which tools get
called. The defaults are safe; the practices below cover the parts that depend
on how you deploy it.

### Use a dedicated, least-privilege session token

- Don't reuse a human's session JWT in `DAGRON_MCP_TOKEN`. Mint a separate
  account for the agent, scope it to only the workflows/projects it needs to
  reach, and rotate it on a schedule.
- Treat the token like any secret: pass it via the MCP client's `env` block (or
  a secret manager), never embed it in `command`/`args`, prompts, or repo files.
- If `DAGRON_MCP_TOKEN` is unset, the server makes unauthenticated calls — only
  acceptable when `DAGRON_API_URL` points at a private, network-isolated edge.

### Pin and isolate the network edge

- Set `DAGRON_API_URL` to the **public UI gateway** (`dagron-api`), never the
  engine's internal `api.rs` ops API. The ops API is unauthenticated and meant
  to be cluster-private — exposing it via MCP would bypass auth entirely.
- Use HTTPS in production. The crate builds reqwest with the `rustls-tls`
  feature, so TLS works out of the box; just point `DAGRON_API_URL` at `https://…`.
- If your agent runs on an end-user device, keep `dagron-api` behind a VPN or
  mTLS edge — the bearer token alone is one breach away from full API access.

### Sandbox what `dagron_submit_run` can launch

- `dagron_submit_run` accepts arbitrary DAG YAML and the engine launches each
  task as a subprocess (or Docker/Kube container) per its `Executor`. Run
  agent-driven workloads under an executor that **isolates** them: a dedicated
  Kubernetes namespace with a restricted PodSecurityPolicy/Pod Security
  Standard, a Docker daemon with locked-down capabilities, or a separate
  dagron cluster entirely. Never point an agent at a scheduler whose tasks
  execute on a shared host filesystem.
- Set per-task `timeout_secs` defaults and submission quotas on the
  agent's account so a runaway prompt can't fan out unbounded work.

### Defend against prompt-injection escalation

- Any text the agent reads — workflow YAML, task logs, dead-letter payloads —
  can carry instructions aimed at the LLM. Tools like `dagron_get_task_logs`
  and `dagron_list_dead_letters` return that text verbatim. If the agent will
  act on it, treat results as data, not directives.
- The MCP server validates path-segment ids (`run_id`, `task_id`) against
  `[A-Za-z0-9_-]+` before any HTTP call so a crafted id can't reshape the
  request path or smuggle query/header content. Don't disable that check.
- The Bearer token never appears in MCP responses — only `dagron-api`'s body
  is forwarded. Keep it that way if you fork the client.

### Bound and observe the agent's reach

- `dagron_get_run_events`'s window is hard-capped (10 s / 256 KiB) so an agent
  can't pin a connection or exhaust memory by polling a chatty stream.
- `dagron_list_dead_letters`'s `limit` is clamped to `1..=500` locally before
  the request, so a malformed argument fails fast as a tool error rather than
  reaching the server.
- Enable `dagron-api`'s access log on the edge; every MCP call appears as an
  HTTP request from the agent's JWT subject, giving you a single audit trail
  for human and agent traffic.
- Log to stderr only (the default). `tracing-subscriber` is wired to stderr
  precisely so stdout stays a clean JSON-RPC channel — a sensitive log line
  written to stdout would corrupt protocol framing *and* leak through MCP.

> This makes dagron's durable engine drivable by agents — the foundation for the
> agentic durable-execution step types on the roadmap (LLM / tool / approval steps).
