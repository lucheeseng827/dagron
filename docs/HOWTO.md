# dagron how-to guide

Task-oriented recipes: start a workflow (CLI + REST), chain one workflow from
another, monitor runs, and wire secrets/environment variables. Commands assume
the UI stack from [`compose.yaml`](../compose.yaml) (`docker compose up`), which
serves the authenticated API gateway on `http://localhost:8080` and the web UI on
`http://localhost:3000`.

- **Engine** — the `dagron` binary: scheduler daemon + task runner.
- **dagron-api** (`/api/...`) — the authenticated gateway the UI and REST clients
  use. Postgres-only. Everything below hits this unless noted.

---

## Workflow YAML in 30 seconds

A workflow is a name plus a list of tasks; each task lists its upstream
dependencies. This is the diamond used throughout:

```yaml
name: hello
tasks:
  - name: a
    command: ["echo", "task a"]
  - name: b
    command: ["echo", "task b"]
    depends_on: [a]
  - name: c
    command: ["echo", "task c"]
    depends_on: [a]
  - name: d
    command: ["echo", "done"]
    depends_on: [b, c]
```

Common task fields: `command` (argv array), `depends_on`, `docker_image`,
`env`, `max_attempts` / `retry_delay_secs`, `timeout_secs`, `runner_class`,
`type: approval` (human gate), `repeat`, and `workflow_ref` (call another
workflow — see below). DAG-level fields include `parameters`, `environment`,
`task_defaults`, and `notify`.

Validate a spec offline before running it:

```console
dagron validate path/to/workflow.yaml
dagron validate examples/ --json      # lint a whole directory
```

---

## 1. Start a workflow via the CLI

The `dagron` binary has **no `submit` subcommand** — it is a scheduler daemon.
You start a workflow by handing the engine a YAML file to ingest, or by posting
to the API (section 2). The two CLI paths:

**Quickstart (`dagron dev`)** — ingests the file once and stays resident with the
management API on `127.0.0.1:8787` (SQLite `workflow.db` by default):

```console
dagron dev path/to/workflow.yaml
```

**Explicit file + datastore** — positional `<dag-file> [db-target]`; the second
arg is a SQLite file path or a Postgres URL:

```console
# run against a local SQLite file
dagron path/to/workflow.yaml workflow.db

# run against Postgres
dagron path/to/workflow.yaml postgres://dagron:dagron@localhost:5432/workflow
```

The file is ingested by the built-in `file` source. Whether the process stays up
(serving the API, cron, GC) or exits after draining is controlled by the
`API_ADDR`, `CRON_CONFIG`, `DB_SCHEDULES`, and `GC_RETENTION_SECS` env vars — set
any of them to keep it resident (the `docs/CONFIG.md` reference in the source tree
lists every env var). To submit repeatedly from the shell, use the REST call below.

---

## 2. Start a workflow via REST

Two calls: log in (get a session cookie), then `POST /api/runs` with the YAML as
a JSON string field named `yaml`.

```console
# 1. log in — stores the dagron_session cookie in cookies.txt
curl -sS -c cookies.txt -X POST http://localhost:8080/api/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"admin@local","password":"dagron-admin"}'

# 2. submit a workflow (YAML embedded as the "yaml" string field)
curl -sS -b cookies.txt -X POST http://localhost:8080/api/runs \
  -H 'Content-Type: application/json' \
  -d "$(jq -Rn --arg y "$(cat workflow.yaml)" '{yaml:$y}')"
# -> 201 {"run_id":"d90c9ce3-..."}
```

The server parses + cycle-checks the DAG, expands any `workflow_ref` calls, and
creates the run. `POST /api/login` also returns `{"token":"<jwt>"}` if you prefer
an `Authorization: Bearer` header over the cookie.

---

## 3. One workflow triggering another (`workflow_ref`)

A task can call a **saved** workflow instead of running a command — the child's
tasks are inlined into the parent DAG (namespaced `<task>.<subtask>`), with the
parent task's upstreams/downstreams rewired around the sub-DAG.

**Step 1 — save the child workflow** (`workflow_ref` resolves by the saved
workflow's `name`, so it must exist first):

```console
curl -sS -b cookies.txt -X POST http://localhost:8080/api/workflows \
  -H 'Content-Type: application/json' \
  -d "$(jq -Rn --arg s "$(cat etl.yaml)" '{name:"etl", spec:$s}')"
```

**Step 2 — reference it from a parent task** via `workflow_ref` (a task is either
a `command` leaf or a `workflow_ref` call, never both):

```yaml
name: nightly
tasks:
  - name: prepare
    command: ["sh", "-c", "echo prepare"]
  - name: run-etl
    workflow_ref: etl          # calls the saved `etl` workflow
    depends_on: [prepare]
  - name: notify
    command: ["sh", "-c", "echo done"]
    depends_on: [run-etl]      # waits for the whole etl sub-DAG
```

Submit `nightly` as in section 2. Refs may nest (up to 32 deep); an unknown name
or a cycle is rejected with `400`. Runnable pair:
[`examples/ui/04_chained_parent.yaml`](../examples/ui/04_chained_parent.yaml) +
[`examples/ui/03_etl.yaml`](../examples/ui/03_etl.yaml).

> Note: `workflow_ref` (save-and-call) is distinct from the engine's inline
> `templates:` sub-DAG mechanism ([`examples/templates/`](../examples/templates/)).

---

## 4. Monitor a workflow

All monitoring is REST (or the web UI at `:3000`). Using the `run_id` from the
submit call:

```console
# list recent runs (filter by ?status= &name= &trigger= &limit= &offset=)
curl -sS -b cookies.txt http://localhost:8080/api/runs | jq '.[].status'

# one run: status + every task's status/attempt/output
curl -sS -b cookies.txt http://localhost:8080/api/runs/$RUN_ID

# the DAG as nodes + edges (what the UI graph draws)
curl -sS -b cookies.txt http://localhost:8080/api/runs/$RUN_ID/graph

# one task's logs (poll ?offset=next_offset until "eof": true)
curl -sS -b cookies.txt "http://localhost:8080/api/runs/$RUN_ID/tasks/$TASK_ID/logs?offset=0"

# block until the run finishes (?timeout_secs=, default 30, max 600)
curl -sS -b cookies.txt "http://localhost:8080/api/runs/$RUN_ID/wait?timeout_secs=120"
```

**Live updates (SSE):** `GET /api/runs/$RUN_ID/stream` streams an event every time
the run changes. The payload is a **refetch signal**, not the data — each message
is `{"run_id":"..."}` (plus an `event: resync` on broadcast lag); on receipt,
re-`GET` the run/graph for current state. `GET /api/events/stream` is the same
signal across all runs.

```console
curl -sS -N -b cookies.txt http://localhost:8080/api/events/stream
```

**Fleet health:**

```console
# JSON status counts + a timeseries
curl -sS -b cookies.txt http://localhost:8080/api/metrics

# tasks that exhausted retries — inspect, redrive, or delete
curl -sS -b cookies.txt "http://localhost:8080/api/dead-letters?limit=50"
curl -sS -b cookies.txt -X POST http://localhost:8080/api/dead-letters/$ID/redrive
```

Prometheus scrape metrics (`text/plain; version=0.0.4`) are exposed by the
**engine** management API at `/metrics` (when `API_ADDR` is set), not by
dagron-api. The engine API also serves Swagger UI at `/docs`.

---

## 5. Secrets & environment variables

Two layers: plain **variables** (substituted into the spec) and encrypted
**secrets** (decrypted only at task dispatch). Both live in a named
**environment**; a workflow opts in with `environment: <name>`.

### Enable secret storage

Secrets are encrypted at rest (AES-256-GCM). Set one shared key on **both**
dagron-api (encrypts on write) and the engine (decrypts at dispatch) —
`DAGRON_ENV_SECRET_KEY` (32 bytes of base64 used verbatim, or any passphrase,
hashed). Without it, writing a secret returns `503`. The Helm chart wires this
via `envSecrets.key` / `envSecrets.existingSecret`; compose sets it already.

```console
export DAGRON_ENV_SECRET_KEY="$(openssl rand -base64 32)"   # same value for api + engine
```

### Create an environment + variables + secrets

```console
# environment with plain variables
curl -sS -b cookies.txt -X POST http://localhost:8080/api/environments \
  -H 'Content-Type: application/json' \
  -d '{"name":"prod","variables":{"AWS_REGION":"ap-southeast-1"}}'
# -> {"id":"<env-id>", ...}

# add an encrypted secret (write-only; value never returned; 503 if no key)
curl -sS -b cookies.txt -X PUT \
  http://localhost:8080/api/environments/$ENV_ID/secrets/prod_api_token \
  -H 'Content-Type: application/json' \
  -d '{"value":"s3cr3t-token"}'
```

`GET /api/environments` lists variables in full but secrets only by name
(`secret_names`, `secrets_configured`) — values never leave the server.

### Consume them in a workflow

- Opt in at DAG level: `environment: prod`. Its variables become `{{ env.NAME }}`.
- Per-task literal var: `env: [{name, value}]`.
- Per-task var from a secret: `env: [{name, value_from: {secret: <name>}}]`.

```yaml
name: deploy
environment: prod                     # vars → {{ env.* }}, secrets resolvable
tasks:
  - name: push
    command: ["sh", "-c", "deploy.sh"]
    env:
      - name: REGION
        value: "{{ env.AWS_REGION }}"  # from the environment's variables
      - name: API_TOKEN
        value_from:
          secret: prod_api_token       # decrypted at dispatch, injected as env
```

At dispatch the engine resolves `value_from` from the environment's secret store
first, then falls back to a `DAGRON_SECRET_<NAME>` env var / the secrets directory
on the engine host. For knobs the chart doesn't model, `engine.extraEnv` /
`dagronApi.extraEnv` pass raw env vars straight to the containers.

---

## See also

- [`../examples/`](../examples/) — runnable workflow specs.
- [`ARCHITECTURE.md`](ARCHITECTURE.md) — component design, the task state machine,
  and event/call sequences.
- [`../README.md`](../README.md) — install (OCI Helm chart / images) and the
  feature tour.
- The engine's Swagger UI (`/docs` on the engine API) — the live endpoint
  catalogue for both HTTP surfaces.
