# Docker Hub overviews for the dagron images + chart

Source for the **"full description"** shown on each public Docker Hub repo (paste the matching file
into the repo's description, or sync it). Keep these in step with releases.

## Where it fits

`dagron` is a **workflow/DAG orchestrator**: operators drive it from a console,
the control plane parses a DAG and reconciles task state, and it dispatches each
ready task to a worker. It owns the schedule → dispatch → track loop — it is the
thing that runs your tasks, not the tasks themselves. The four artifacts split
along that path.

```
   CLIENTS              CONSOLE + API           ENGINE                EXECUTORS
   (people, CI)         (front of the stack)    (control plane)       (run the tasks)

 ┌──────────────┐     ┌────────────────┐
 │ Browser      │───▶ │ dagron-frontend│──┐
 │ (operator)   │     │ Next.js console│  │ proxies /api/*
 └──────────────┘     └────────────────┘  ▼
                      ┌────────────────┐      ┌───────────────┐     ┌──────────────────┐
 ┌──────────────┐     │ dagron-api     │      │ dagron-engine │───▶ │ local subprocess │
 │ Scripts / CI │───▶ │ auth + mgmt    │      │ parse · sched │───▶ │ docker container │
 │ (REST calls) │     │ REST + JWT     │      │ retry · deps  │───▶ │ k8s pod / task   │
 └──────────────┘     └───────┬────────┘      └───────┬───────┘     └──────────────────┘
                              │                       │
                              │   ┌───────────────┐   │
                              └─▶ │ Postgres      │ ◀─┘
                                  │ runs · DAG    │
                                  │ state (shared)│
                                  └───────────────┘
```

- **Front** — `dagron-frontend` (Next.js) is the only browser-facing image; it
  proxies `/api/*` to `dagron-api`. Scripts/CI hit `dagron-api` directly.
- **Control plane** — `dagron-api` authenticates (HttpOnly JWT) and serves the
  workflow/run/schedule REST; `dagron-engine` reconciles the DAG
  (`pending → ready → running → done`), claims work with `SKIP LOCKED`, and dispatches it.
- **Executors** — the engine runs each task on `local` (subprocess), `docker`
  (one container per task), or `kubernetes` (one pod per task); it drives the
  workers, it is not one.
- **Shared state** — `dagron-api` and `dagron-engine` don't call each other; they
  coordinate through one Postgres. The `mancube/dagron` Helm/OCI chart deploys all
  three images plus an optional throwaway Postgres.

| Docker Hub repo | File | What |
|---|---|---|
| [`mancube/dagron-engine`](https://hub.docker.com/r/mancube/dagron-engine) | [`dagron-engine.md`](./dagron-engine.md) | the workflow/DAG engine |
| [`mancube/dagron-api`](https://hub.docker.com/r/mancube/dagron-api) | [`dagron-api.md`](./dagron-api.md) | auth + management API |
| [`mancube/dagron-frontend`](https://hub.docker.com/r/mancube/dagron-frontend) | [`dagron-frontend.md`](./dagron-frontend.md) | Next.js operator console |
| [`mancube/dagron-mcp`](https://hub.docker.com/r/mancube/dagron-mcp) | [`dagron-mcp.md`](./dagron-mcp.md) | MCP server (drive dagron from an AI agent) |
| `oci://registry-1.docker.io/mancube/dagron` | [`dagron-chart.md`](./dagron-chart.md) | Helm chart (the full stack) |

All four images are published **`linux/amd64` + `linux/arm64`** at `0.4.3` + `latest`.
