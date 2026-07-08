# dagron engine (`mancube/dagron-engine`)

**The dagron workflow/DAG engine — runs DAGs of tasks with retries and dependencies, on a local, Docker, or Kubernetes executor, from one Rust binary.**

dagron is a lightweight workflow scheduler in Rust. The engine parses a YAML DAG, reconciles `pending → ready → running → done`, claims work with optimistic CAS / `FOR UPDATE SKIP LOCKED`, dispatches it to a bounded worker pool, and decrements downstream dependencies — the control-plane half of the dagron stack.

- **Image:** `mancube/dagron-engine` — Rust binary on **distroless/cc** (glibc), runs as **nonroot**, no shell.
- **Arch:** `linux/amd64`, `linux/arm64`
- **Binary inside:** `/usr/local/bin/dagron` (entrypoint)
- **Datastore:** Postgres (`DATABASE_URL`)
- **Source / full docs:** github.com/lucheeseng827/dagron · Apache-2.0

## Tags

| Tag | Notes |
|---|---|
| `latest` | newest release |
| `0.3.0` | pinned version (= current `latest`) |

Pin in production: `mancube/dagron-engine:0.3.0`.

## Run

```bash
docker run \
  -e DATABASE_URL=postgres://dagron:dagron@postgres:5432/workflow \
  -e API_ADDR=0.0.0.0:8787 \
  -e EXECUTOR=local \
  mancube/dagron-engine:0.3.0
```

## Configuration (env)

| Var | Meaning |
|---|---|
| `DATABASE_URL` | Postgres connection string (required). |
| `API_ADDR` | bind for the resident ops/management API (e.g. `0.0.0.0:8787`). |
| `EXECUTOR` | `local` (subprocesses) · `docker` (one container per task) · `kubernetes` (one pod per task). |
| `WORKER_COUNT` | tasks in flight per engine (bounded concurrency). |
| `MAX_INFLIGHT_RUNS` | admission cap; past it `POST /runs` returns `429`. |
| `K8S_IMAGE` / `K8S_NAMESPACE` | task image + namespace for the kubernetes executor. |
| `RUST_LOG` | log level (`info`). |

The engine is built with `FEATURES=postgres,ops,kubernetes`. It needs a Postgres datastore — run it with the `dagron-api` (auth/UI gateway) and `dagron-frontend` (console) for the full stack, or deploy everything with the Helm chart (`oci://registry-1.docker.io/mancube/dagron`).
