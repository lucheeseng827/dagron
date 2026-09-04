# dagron how-to guide

Task-oriented recipes: start a workflow (CLI + REST), chain one workflow from
another, monitor runs, and wire secrets/environment variables. Commands assume
the UI stack from [`compose.yaml`](../compose.yaml) (`docker compose up`), which
serves the authenticated API gateway on `http://localhost:8080` and the web UI on
`http://localhost:8080`.

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

`dev` is a subcommand token, so the positionals shift right —
`dagron dev [dag-file] [db-target]`. A single argument after `dev` is the
**workflow**, never the datastore: `dagron dev my.db` tries to ingest `my.db`
as YAML and still writes to `workflow.db`. To pick the datastore, pass both
(`dagron dev my.yaml my.db`). The startup line names the datastore in use —
read it first when runs land somewhere you didn't expect.

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

All monitoring is REST (or the web UI at `:8080`, same port). Using the `run_id` from the
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

# the WHOLE run's logs, merged and attributed — the call for "it failed and I
# don't know which task did it". Add a filter to narrow it (docs/API.md#log-filter):
curl -sS -b cookies.txt "http://localhost:8080/api/runs/$RUN_ID/logs?level=error&context=1" \
  | jq -r '.lines[] | "\(.task)  \(.text)"'

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

## 5. API tokens for CI and scripts

`/api/login` mints a **session** token: short-lived, and meant for a browser.
Anything automated that used it had to keep a user's password around to get a
fresh one — a secret that cannot be revoked without locking the human out, and
that looks exactly like the human in the logs.

A personal access token is the credential for that job. It is named, revocable
on its own, and optionally expiring.

```bash
# Create one (needs a password login — see below for why).
curl -s -X POST localhost:8080/api/tokens -b cookies.txt \
  -H 'content-type: application/json' \
  -d '{"name":"nightly-ci","expires_in_days":90}'
# -> 201 {"id":"…","name":"nightly-ci","prefix":"dgp_D-5E8bxXpY",
#         "token":"dgp_D-5E8bxXpY…","expires_at":"2026-10-24T…"}
```

**Copy the `token` now.** Only its SHA-256 is stored, so there is no endpoint
that can show it again — a database dump yields no working credential, and
neither does a support request.

Use it anywhere the session bearer would go, with no cookie jar and no password:

```bash
curl -s localhost:8080/api/runs \
  -H "Authorization: Bearer $DAGRON_TOKEN"
```

```bash
# List your tokens — prefix, last use, expiry. Never the secret.
curl -s localhost:8080/api/tokens -b cookies.txt

# Revoke one. Effective immediately; the row is kept so an audit can still see
# that the token existed and when it was stopped.
curl -s -X DELETE localhost:8080/api/tokens/<id> -b cookies.txt
```

`last_used_at` (to the minute) is how you tell whether a token is still wired
into something before revoking it.

**Creating and revoking tokens requires a password session, not a token.** A
token that could mint tokens would replace itself faster than you could revoke
it, and revocation would stop being a control. Requests to `/api/tokens`
carrying a `dgp_` bearer get `403` saying so.

**A token carries its owner's permissions**, read live — there is no per-token
scoping yet. Demoting a user to `viewer` takes effect on their existing tokens
at once rather than whenever the token is next replaced, but a token belonging
to an admin *is* an admin. Give automation its own user.

## 6. Secrets & environment variables

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

## 7. Long scripts: where the code lives

A task's `command:` is argv. There is **no `script:` field and no file-include
directive** — a spec never pulls in another file. So "my script is 400 lines"
is really *where does the code live so the executor can reach it*.

Only three things cross into the task process: `command:` (argv), `env:` (every
backend), and `docker_image:` (container backends). `input:` does **not** — it is
stored on the task row, never passed to the process. Two constraints decide the
rest: **no executor mounts host paths** (the Docker backend uses a default
`HostConfig` with no binds; the Kubernetes backend declares no volumes), and
`DAGRON_ARTIFACTS` is a host directory, so it can pass files between *host* tasks
but cannot carry a script into a container.

| | approach | when |
| --- | --- | --- |
| 1 | **bake it into the image** — `command: ["/app/bin/transform.sh", "--shard", "3"]` | docker/k8s; the default answer. The script's version is the image tag, so a pinned re-run runs the code that ran then. |
| 2 | **absolute path on the engine host** — `command: ["bash", "/opt/dagron/scripts/x.sh"]` | `EXECUTOR=local` only. |
| 3 | **fetch when the run starts** into `$DAGRON_ARTIFACTS` (host) or inside each container | the script changes faster than you rebuild images. |
| 4 | **the body in an env var** — `command: ["sh", "-c", 'eval "$SCRIPT"']` | tens of lines, no build pipeline. |

Two traps worth stating outright:

- For **2**, use absolute paths — the executor sets no working directory, so a
  relative path resolves against the *engine process's* cwd, not the workflow's
  location. And every engine replica needs the file: any replica may claim any
  task, so a script on one box fails whenever another wins the claim.
- For **4**, the body travels as data rather than argv, which removes the
  shell-quoting hazard, but it is still inlined — re-parsed every run, diffed on
  every GitOps sync, and submitted through an API that caps bodies at 1 MiB.

```yaml
# pattern 4 — runnable as-is
tasks:
  - name: transform
    command: ["sh", "-c", 'eval "$SCRIPT"']
    env:
      - name: SHARD
        value: "3"
      - name: SCRIPT
        value: |
          set -eu
          echo "transform starting (shard=${SHARD})"
```

Things that look like answers but aren't: **YAML anchors** only dedupe within one
document; **`templates:`** is DAG reuse, not code reuse (it dedupes *steps*, and
won't shorten a `command:`); **GitOps sync** stores files that have a `tasks:`
key and ships nothing else to your executors. To keep the script in its own repo
file, assemble the spec in CI or an SDK step and submit the result.

Worked examples for all four, plus what to validate:
[`../examples/scripts/`](../examples/scripts/README.md).

---

## 8. Tasks that run longer than 25 seconds

**A task with no `timeout_secs` is killed after 25 seconds.** This is the first
thing most people hit, because the default is short and nothing about the spec
hints at it — a `sleep 60`, a slow query, a model download, all die identically:

```text
run:  failed
task: failed   attempt 1
      command timed out after 25s
```

Fix it on the task:

```yaml
tasks:
  - name: slow
    command: ["sh", "-c", "sleep 60; echo done"]
    timeout_secs: 120        # default 25
```

or once for the whole spec:

```yaml
task_defaults:
  timeout_secs: 3600
```

**Read the failure carefully — it says `timed out`, not a non-zero exit.** A
task killed by its deadline reports `command timed out after Ns`; a task whose
command failed reports its own output. They are different problems and the
second one is not fixed by a bigger number.

### Why it fails on attempt 1

`max_attempts` defaults to **1**, which is one total attempt and therefore no
retries, for any failure. So a timeout shows a single attempt even though a
retry looks like it should have happened. The field counts attempts, not
retries: set it to `2` for one retry, `3` for two, and so on.

Separately, `retry_on_timeout` (default **`true`**) decides whether a *deadline
kill specifically* consumes retries. Set it `false` when a timeout is unlikely
to succeed on re-run — that fails at once instead of burning the remaining
budget (Airflow #9232). It is timeout-only: a non-zero exit always follows the
normal attempts rule.

### Is there a maximum?

**Not one you will hit.** `timeout_secs` and `run_timeout_secs` are both `u64`
seconds — `86400` (a day) and `604800` (a week) are legal. The only validation
is the lower end: `timeout_secs must be >= 1 when provided`. `run_timeout_secs`
is clamped to `i64::MAX` seconds (~292 billion years) when it is persisted, so
there is a ceiling in principle but none that constrains any real schedule.

The task lease is not a ceiling either. It is 30 s
(`TASK_LEASE_EXTEND_SECS`), but a running task's worker heartbeats it every 10 s,
so an hours-long task is never reclaimed out from under itself. A task outliving
its lease period is normal and expected. (Heartbeats can be turned off with
`TASK_LEASE_HEARTBEAT=false`; that trades this for a hard rule that every task
must finish inside one lease window, which only suits a fleet of genuinely short
tasks.)

### The three budgets, and which one to reach for

| Field | Scope | On expiry |
| --- | --- | --- |
| `timeout_secs` | one task | task killed → `failed` (retries per above) |
| `run_timeout_secs` | whole run | run `failed`, remaining tasks cancelled |
| `deadline: { in: 45m }` | whole run | **alert only** — the run keeps going |

Per-task timeouts do not bound a run: a long DAG of individually-bounded tasks
can still exceed the total duration you had in mind, so `run_timeout_secs` is
the actual safety net.
Reach for `deadline:` when "this is late" should page someone rather than
destroy three hours of work.

```yaml
name: nightly-train
run_timeout_secs: 90000        # whole-run budget
deadline: { in: 6h }           # soft SLA — alerts, does not cancel
task_defaults:
  timeout_secs: 3600
tasks:
  - name: extract
    command: ["extract"]
    timeout_secs: 1800
  - name: train
    command: ["python", "train.py"]
    timeout_secs: 14400        # 4 h
    max_attempts: 5            # preemption = a retry that resumes
```

Pair a long timeout with checkpointing rather than relying on it alone: a
four-hour task that retries from zero is worse than one that fails fast.
`DAGRON_RESUME_FROM` hands the last checkpoint back on retry — see
[`AI_WORKLOADS.md`](AI_WORKLOADS.md).

### A different `timeout_secs` that *is* capped

The synchronous-wait query parameter shares the name and is unrelated to how
long a task may run:

```console
curl -X POST 'localhost:8787/runs?wait=true&timeout_secs=30'   # capped at 600
curl 'localhost:8787/runs/$RUN/wait?timeout_secs=600'          # capped at 600
```

It is **clamped to 1–600 s** and controls only how long the HTTP call blocks.
Against a workflow longer than ten minutes the call returns `finished: false`
with the live status so you re-poll — that is the documented contract, not an
error, and not a sign the run died.

---

## See also

- [`../examples/`](../examples/) — runnable workflow specs.
- [`../examples/scripts/`](../examples/scripts/README.md) — the four places a
  long script can live, and which reach which executor (§7).
- [`STREAMING.md`](STREAMING.md) — events → workflows: the built-in stream
  source, delivery semantics, and five case studies
  ([`../examples/streaming/`](../examples/streaming/)).
- [`AI_WORKLOADS.md`](AI_WORKLOADS.md) — long/checkpointed tasks, the resume
  contract, GPU routing, and five case studies
  ([`../examples/ai/`](../examples/ai/)).
- [`ARCHITECTURE.md`](ARCHITECTURE.md) — component design, the task state machine,
  and event/call sequences.
- [`../README.md`](../README.md) — install (OCI Helm chart / images) and the
  feature tour.
- The engine's Swagger UI (`/docs` on the engine API) — the live endpoint
  catalogue for both HTTP surfaces.
