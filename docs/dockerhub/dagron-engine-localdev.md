# dagron engine, local-dev build (`mancube/dagron-engine-localdev`)

**The same dagron engine binary as `mancube/dagron-engine`, on debian-slim instead of distroless — so a task whose command is `echo` or `sh -c ...` actually resolves. For trying dagron out, not for production.**

With `EXECUTOR=local` the engine runs each task as a **subprocess inside its own container**, so the task's command has to exist *in the engine image*. The production image is distroless: no shell, no coreutils, and `command: ["echo", "hello"]` — the first thing anyone writes — fails to resolve. This image exists so that first workflow runs.

Reach for the distroless [`mancube/dagron-engine`](https://hub.docker.com/r/mancube/dagron-engine) everywhere else. With the `docker` or `kubernetes` executor each task runs in its own image, so the engine needs no shell and the smaller, shell-less build is the right one.

- **Image:** `mancube/dagron-engine-localdev` — same Rust binary on **debian:bookworm-slim**, runs as **nonroot** (uid 65532), has `/bin/sh` + coreutils.
- **Arch:** `linux/amd64`, `linux/arm64`
- **Binary inside:** `/usr/local/bin/dagron` (entrypoint) · **Bundled examples:** `/etc/dagron/examples/`
- **Datastore:** Postgres (`DATABASE_URL`)
- **Website:** dagron.dev · **Source / full docs:** github.com/lucheeseng827/dagron · Apache-2.0

## Tags

| Tag | Notes |
|---|---|
| `latest` | newest release |
| `0.7.0` | pinned version (= current `latest`) |
| `0.7` | floating minor — newest `0.7.x` |

## Run

The quickest path is the bundled compose file, which already wires this image to Postgres, the API, and the console:

```bash
curl -fsSLO https://raw.githubusercontent.com/lucheeseng827/dagron/main/compose.quickstart.yaml
docker compose -f compose.quickstart.yaml up
```

Standalone, note that **the DAG path is a positional argument, not an environment variable** — the entrypoint is the bare binary, so it goes in `command:`:

```bash
docker run \
  -e DATABASE_URL=postgres://dagron:dagron@postgres:5432/workflow \
  -e API_ADDR=0.0.0.0:8787 \
  -e EXECUTOR=local \
  -v dagron-workflows:/workflows \
  mancube/dagron-engine-localdev:0.7.0 /workflows/simple_dag.yaml
```

Setting `DAG_PATH` in the environment does nothing: the binary never reads it, falls back to a built-in default that does not exist in the image, and logs `cannot read DAG file` on every boot.

`/workflows` is `WORKFLOW_DIR`, seeded with the bundled examples on first start when empty. Under the default `file` source it is a seed target, not an inbox: one YAML is emitted **once** at startup and then drained, so a file dropped in later is never picked up.

Set `SOURCE=dir` to make it a live inbox instead — every `*.yaml`/`*.yml` in the directory runs, a file added later runs when the next scan finds it (`DIR_POLL_MS`, default 2 s), and an edited file is re-submitted when the edit changes its modified time or length — the scan keys on that pair, so a same-length rewrite within the filesystem's timestamp granularity is not seen. Note that the engine seeds the bundled examples into an empty `WORKFLOW_DIR`, so a first start with `SOURCE=dir` and an empty volume runs all of them. Either way, further workflows can also go in through the console or `POST /api/runs`.

## Configuration (env)

Identical to [`mancube/dagron-engine`](https://hub.docker.com/r/mancube/dagron-engine); the two images differ only in base layer. Built with `FEATURES=postgres,ops,kubernetes`.

| Var | Meaning |
|---|---|
| `DATABASE_URL` | Postgres connection string (required). |
| `API_ADDR` | bind for the resident ops/management API (e.g. `0.0.0.0:8787`). |
| `EXECUTOR` | `local` (subprocesses — what this image is for) · `docker` · `kubernetes`. |
| `WORKER_COUNT` | tasks in flight per engine (bounded concurrency). |
| `MAX_INFLIGHT_RUNS` | admission cap; past it `POST /runs` returns `429`. |
| `RUST_LOG` | log level (`info`). |

Run it with `dagron-api` (auth/UI gateway) and `dagron-frontend` (console) for the full stack. For a real deployment use the Helm chart (`oci://registry-1.docker.io/mancube/dagron`), which deploys the distroless engine.
