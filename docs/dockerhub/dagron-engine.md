# dagron engine (`mancube/dagron-engine`)

**The dagron workflow/DAG engine — runs DAGs of tasks with retries and dependencies, on a local, Docker, or Kubernetes executor, from one Rust binary.**

dagron is a lightweight workflow scheduler in Rust. The engine parses a YAML DAG, reconciles `pending → ready → running → done`, claims work with optimistic CAS / `FOR UPDATE SKIP LOCKED`, dispatches it to a bounded worker pool, and decrements downstream dependencies — the control-plane half of the dagron stack.

- **Image:** `mancube/dagron-engine` — Rust binary on **distroless/cc** (glibc), runs as **nonroot**, no shell.
- **Arch:** `linux/amd64`, `linux/arm64`
- **Binary inside:** `/usr/local/bin/dagron` (entrypoint)
- **Datastore:** Postgres (`DATABASE_URL`)
- **Website:** dagron.dev · **Source / full docs:** github.com/lucheeseng827/dagron · Apache-2.0

## Tags

| Tag | Notes |
|---|---|
| `latest` | newest release |
| `0.11.1` | pinned version (= current `latest`) |
| `0.11` | floating minor — newest `0.11.x` |

Pin in production: `mancube/dagron-engine:0.11.1`.

## Run

```bash
docker run \
  -e DATABASE_URL=postgres://dagron:dagron@postgres:5432/workflow \
  -e API_ADDR=0.0.0.0:8787 \
  -e EXECUTOR=local \
  mancube/dagron-engine:0.11.1
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

The engine is built with `FEATURES=postgres,ops,kubernetes`: Postgres, the ops API and the Kubernetes executor. **From 0.11.0** it adds `otel,archive-s3,archive-gcs,archive-azure`: OpenTelemetry (a trace context for each task, and OTLP span export once `OTEL_EXPORTER_OTLP_ENDPOINT` names a collector), and S3, Google Cloud Storage and Azure Blob for archived runs (`GC_ARCHIVE_URL`). Tags through 0.10.x do not have them: there `GC_ARCHIVE_URL` with a bucket is a startup error. An image built with them says so as it starts, on its `configuration resolved` line (`features=…`); one that prints no `features=` there is an earlier one. It needs a Postgres datastore — run it with `dagron-api` (auth, REST API and the console) for the full stack, or deploy everything with the Helm chart (`oci://registry-1.docker.io/mancube/dagron`).
