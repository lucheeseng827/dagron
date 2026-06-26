# dagron SDK examples

Runnable scripts that drive a live `dagron-api` with the dagron SDKs. They're the
fastest way to see the SDK work end-to-end: bring up the stack, run one file, watch
a real run go green.

The SDKs themselves (and their reference docs) live in [`../../sdks`](../../sdks) —
Python: [`sdks/python/README.md`](../../sdks/python/README.md), TypeScript:
[`sdks/typescript`](../../sdks/typescript). These examples *use* them.

## 1. Start a dagron to talk to

From the module root (`module_54/`):

```bash
podman compose up --build      # or: docker compose up --build
```

That brings up Postgres + engine + `dagron-api` (:8080) + the web UI (:3000) and
seeds an admin user. The examples default to that stack and its seeded admin
(`admin@local` / `dagron-admin`).

## 2. Run an example

### Python

No install needed — the scripts add the in-tree SDK to `sys.path` if `dagron`
isn't already installed (see `python/_bootstrap.py`). Standard library only.

```bash
cd examples/sdk/python
python 01_quickstart.py
```

| Script | Shows |
|---|---|
| `01_quickstart.py` | Build a DAG, submit an ad-hoc run, wait, read task logs. |
| `02_workflow_and_schedule.py` | Save a reusable workflow, attach a cron schedule, trigger it. Cleans up after itself. |
| `03_stream_run.py` | Follow a run's task transitions live over Server-Sent Events. |
| `04_rerun_failed.py` | Fail a run mid-graph, then cascade-`rerun` to recover the failure frontier. |

To use the published package instead of the in-tree copy:

```bash
pip install -e ../../../sdks/python    # or, once released: pip install dagron-sdk
```

### TypeScript / JavaScript

Node 18+ (global `fetch`), zero dependencies. The example imports the SDK by
relative path:

```bash
cd examples/sdk/typescript
node 01_quickstart.mjs
```

| Script | Shows |
|---|---|
| `01_quickstart.mjs` | Build a DAG, log in, submit a run, poll it to a terminal state. |

## 3. Point them at a real deployment

Every script reads its connection details from the environment, so the same files
work against a remote dagron:

| Env var | Default | Meaning |
|---|---|---|
| `DAGRON_API_URL` | `http://localhost:8080` | gateway base URL |
| `DAGRON_TOKEN` | _(unset)_ | session JWT — skips login when set |
| `DAGRON_EMAIL` | `admin@local` | login email (when no token) |
| `DAGRON_PASSWORD` | `dagron-admin` | login password (when no token) |

```bash
DAGRON_API_URL=https://dagron.example.com \
DAGRON_EMAIL=me@example.com DAGRON_PASSWORD=••• \
python 01_quickstart.py
```

## Notes

- The DAGs use plain `echo`/`sh` commands so they run on the engine's **local
  executor** with no container images. To exercise the Docker executor instead,
  bring the stack up with `compose.docker-executor.yaml` and give tasks an `image`.
- A `params=` override on `rerun` (fix-forward rerun) is an **Enterprise** feature;
  the OSS engine rejects it, so `04_rerun_failed.py` uses a plain cascade rerun.
