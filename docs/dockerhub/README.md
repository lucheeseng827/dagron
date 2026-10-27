# Docker Hub overviews for the dagron images + chart

Source for the **"full description"** shown on each public Docker Hub repo (paste the matching file
into the repo's description, or sync it). Keep these in step with releases.

## Where it fits

`dagron` is a **workflow/DAG orchestrator**: operators drive it from a console,
the control plane parses a DAG and reconciles task state, and it dispatches each
ready task to a worker. It owns the schedule → dispatch → track loop — it is the
thing that runs your tasks, not the tasks themselves. The artifacts below split
along that path.

```
   CLIENTS              CONSOLE + API           ENGINE                EXECUTORS
   (people, CI)         (front of the stack)    (control plane)       (run the tasks)

 ┌──────────────┐
 │ Browser      │───┐
 │ (operator)   │   │  console at /, API at /api — one origin
 └──────────────┘   │
                      ▼
                      ┌────────────────┐      ┌───────────────┐     ┌──────────────────┐
 ┌──────────────┐     │ dagron-api     │      │ dagron-engine │───▶ │ local subprocess │
 │ Scripts / CI │───▶ │ console + auth │      │ parse · sched │───▶ │ docker container │
 │ (REST calls) │     │ REST + JWT     │      │ retry · deps  │───▶ │ k8s pod / task   │
 └──────────────┘     └───────┬────────┘      └───────┬───────┘     └──────────────────┘
                              │                       │
                              │   ┌───────────────┐   │
                              └─▶ │ Postgres      │ ◀─┘
                                  │ runs · DAG    │
                                  │ state (shared)│
                                  └───────────────┘
```

- **Front** — `dagron-api` is the only browser-facing image: it serves the
  console at `/` and the REST API under `/api`, same origin, no proxy.
  Scripts/CI hit the same port. (`dagron-frontend` carried the console until
  0.8.1 and is discontinued — see [`dagron-frontend.md`](./dagron-frontend.md).)
- **Control plane** — `dagron-api` authenticates (HttpOnly JWT) and serves the
  workflow/run/schedule REST; `dagron-engine` reconciles the DAG
  (`pending → ready → running → done`), claims work with `SKIP LOCKED`, and dispatches it.
- **Executors** — the engine runs each task on `local` (subprocess), `docker`
  (one container per task), or `kubernetes` (one pod per task); it drives the
  workers, it is not one.
- **Shared state** — `dagron-api` and `dagron-engine` don't call each other; they
  coordinate through one Postgres. The `mancube/dagron` Helm/OCI chart deploys
  both images plus an optional throwaway Postgres.

| Docker Hub repo | File | What |
|---|---|---|
| [`mancube/dagron-engine`](https://hub.docker.com/r/mancube/dagron-engine) | [`dagron-engine.md`](./dagron-engine.md) | the workflow/DAG engine |
| [`mancube/dagron-api`](https://hub.docker.com/r/mancube/dagron-api) | [`dagron-api.md`](./dagron-api.md) | auth + management API |
| [`mancube/dagron-frontend`](https://hub.docker.com/r/mancube/dagron-frontend) | [`dagron-frontend.md`](./dagron-frontend.md) | ⛔ discontinued after 0.8.1 — the console ships in `dagron-api` |
| [`mancube/dagron-mcp`](https://hub.docker.com/r/mancube/dagron-mcp) | [`dagron-mcp.md`](./dagron-mcp.md) | MCP server (drive dagron from an AI agent) |
| [`mancube/dagron-gitops`](https://hub.docker.com/r/mancube/dagron-gitops) | [`dagron-gitops.md`](./dagron-gitops.md) | GitOps worker (repo → workflow definitions; the only image with `git`) |
| [`mancube/dagron-step-mcp`](https://hub.docker.com/r/mancube/dagron-step-mcp) | [`dagron-step-mcp.md`](./dagron-step-mcp.md) | MCP-tool *step* (drive an agent's tools from a DAG) — a task binary, usually `COPY --from`'d into your task's image |
| [`mancube/dagron-step-spark`](https://hub.docker.com/r/mancube/dagron-step-spark) | [`dagron-step-spark.md`](./dagron-step-spark.md) | Spark *step* (submit a job, hand back its id, park on it) — a task binary |
| [`mancube/dagron-step-sql`](https://hub.docker.com/r/mancube/dagron-step-sql) | [`dagron-step-sql.md`](./dagron-step-sql.md) | SQL *step* (one statement against an analytical store, bounded result) — a task binary |
| [`mancube/dagron-ci`](https://hub.docker.com/r/mancube/dagron-ci) | [`dagron-ci.md`](./dagron-ci.md) | the CI service (a forge's events in, runs in dagron-api, commit statuses back); from 0.11.0 |
| [`mancube/dagron-ci-exec`](https://hub.docker.com/r/mancube/dagron-ci-exec) | [`dagron-ci-exec.md`](./dagron-ci-exec.md) | a CI job's step runner, put into each job by an init container (`DAGRON_CI_EXEC_IMAGE`); from 0.11.0 |
| [`mancube/dagron-runner`](https://hub.docker.com/r/mancube/dagron-runner) | [`dagron-runner.md`](./dagron-runner.md) | the remote runner: runs a class of tasks away from the engine, dialling dagron-api; from 0.11.0 |
| [`mancube/dagron-engine-localdev`](https://hub.docker.com/r/mancube/dagron-engine-localdev) | [`dagron-engine-localdev.md`](./dagron-engine-localdev.md) | the engine on debian-slim — has a shell, so `EXECUTOR=local` tasks resolve |
| `oci://registry-1.docker.io/mancube/dagron` | [`dagron-chart.md`](./dagron-chart.md) | Helm chart (the full stack) |

All eleven current images and the chart are published **`linux/amd64` + `linux/arm64`** at
the release version, the floating minor and `latest` — `0.11.0`, `0.11` and
`latest` as of 0.11.0. (`dagron-frontend` stopped at 0.8.1.)

A new repository is created by its first push, and Docker Hub makes it
**private** by default. That is why the three step images, first pushed with
0.9.3, could not be pulled until their repositories were made public on
2026-10-03, and why `dagron-ci`, `dagron-ci-exec` and `dagron-runner`, first
pushed with 0.11.0, were made public on 2026-10-27. When a release adds an image, make its repository public before
announcing it, then publish its description with the script below.

Publish them with [`../../scripts/publish-dockerhub-descriptions.py`](../../scripts/publish-dockerhub-descriptions.py)
rather than pasting: it carries UTF-8 through (hand-pasting once replaced every em
dash on the live pages with `U+FFFD`) and reads each description back to prove
what the page actually says.
