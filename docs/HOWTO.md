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

Common task fields: `command` (argv array), `depends_on`, `docker_image`
(and `image_pull_policy`: `if_not_present`, `always` or `never`),
`env`, `max_attempts` / `retry_delay_secs`, `timeout_secs`, `runner_class`,
`type: approval` (human gate), `repeat`, `workspace` (a directory made for each
attempt — §9), `services` (containers beside the task while it runs — §10),
and `workflow_ref` (call another workflow — see below). DAG-level fields include `parameters`, `environment`,
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

**An agent's token is not its owner.** For a coding agent, mint one with
`agent`:

```bash
curl -s -X POST localhost:8080/api/tokens -b cookies.txt \
  -H 'content-type: application/json' \
  -d '{"name":"fixer","expires_in_days":30,"agent":"fixer"}'
```

It presents as `agent:fixer`, with your groups less `admin` and `ci:trigger`.
It reads what you may read, and reruns or cancels the runs recorded as its
own; any other change is a `403`. Its runs come from dagron-ci's run API,
where an operator who has them run (`CI_AGENT_RUNS=untrusted`) gets them
confined, as untrusted runs are. The name `fixer` stays yours: nobody else
can mint a token as `agent:fixer`, even after you revoke yours.

**Roles.** There are two, in every build. A user in the **`viewer`** group is read-only:
every mutating request (`POST`/`PUT`/`PATCH`/`DELETE`, by session or by token) is refused
with `403 viewer role is read-only`, except logging in and out. **Git repository
configuration is admin-only**: connecting a repo, deleting one, and setting or removing its
credential need the `admin` group (`403` otherwise); listing repos and *sync now* stay open
to any non-viewer. Everyone else is an ordinary user with the rights they have always had,
including over workflows. Per-team ownership, custom roles and SSO group mapping are
Enterprise.

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
on the engine host (for a `trusted` run: "Who may use an environment", below).
For knobs the chart doesn't model, `engine.extraEnv` / `dagronApi.extraEnv` pass
raw env vars straight to the containers.

### A task's own environment

A task may name an environment of its own. Its `value_from` secrets are then
resolved from that one, in place of the workflow's:

```yaml
name: release
environment: build                    # variables for the whole workflow
tasks:
  - name: test
    command: ["make", "test"]
  - name: publish
    environment: registry             # this task's secrets come from here
    command: ["make", "publish"]
    env:
      - name: TOKEN
        value_from: { secret: registry_token }
```

`{{ env.NAME }}` variables are still the workflow's own environment's: they are
put in when the run is made, before there are tasks. An environment's name is
taken as written, on a workflow and on a task. It is never filled in from a
parameter, so a caller does not choose which one is read.

### Who may use an environment

An environment has a **policy**: which runs may use it. A run is admitted when
it passes every part that is set.

| Part | Admits | Left out |
|---|---|---|
| `allowed_trust` | runs of the trust classes named: `trusted`, `untrusted`, `agent` | Not optional. `[]` admits no run: the environment is closed |
| `allowed_refs` | a run made for one of these refs: `refs/heads/main`, `refs/tags/v*` | any ref, and a run made for none |
| `allowed_pipelines` | a workflow of one of these names: `release`, `deploy-*` | any |
| `allowed_identity` | `["agent"]`: an agent's task in the environment may hold an identity (`identity:`, OIDC tokens for a cloud), as a trusted run's may. `agent` alone may be named, and only where `allowed_trust` admits it (below) | none: an identity is a trusted run's alone |
| `approvers` | a **protected environment**: who must approve a run that is not trusted before a task of it uses the environment, `email` or `group:<name>` (below) | none |

In a pattern, `*` is any run of characters up to a `/`, `**` is any run of
characters, and `?` is one character.

**An environment with no policy admits `trusted` runs and no others.** Every
run an installation makes for itself is `trusted`: one a person submits, a
schedule's, a dataset's. A run a CI service makes says its own class, from the
event it verified. So nothing that is not `trusted` reads an environment until
an admin writes a policy that names its class.

```console
# only main and the release tags of the pipeline "release" read this environment
curl -sS -b cookies.txt -X PUT http://localhost:8080/api/environments/$ENV_ID/policy \
  -H 'Content-Type: application/json' \
  -d '{"allowed_trust":["trusted"],"allowed_refs":["refs/heads/main","refs/tags/v*"],"allowed_pipelines":["release"]}'
```

Only an admin writes a policy. `PUT` replaces the whole of it, and `DELETE` on
the same path takes it away. `GET /api/environments` shows it as `policy`.

It is asked twice:

- **When a run is made.** A run that names an environment that does not admit
  it is refused (`403`, with the reason), before its variables are put into
  anything that is stored.
- **When a task's secrets are resolved.** A task that may not have them fails,
  with the reason as its output, and is not started. This is the check that
  holds for a run however it was made. It comes before a task's `cache:` is
  looked in, so an output that an earlier run printed with a secret is not
  served to a run that may not read the secret.

**An agent's run, by policy (`allowed_identity`, `approvers`).** A run an
agent asked for (`agent`, dagron-ci's `CI_AGENT_RUNS=untrusted`) is confined
as a fork's is, and two parts of a policy give it more, in that environment
alone:

- **An identity.** With `"allowed_identity": ["agent"]` (and `agent` in
  `allowed_trust`), an agent's task that names the environment and asks for
  an identity is given one. Its tokens say `"trust": "agent"`, so a cloud's
  trust policy can still tell it from a trusted run's. It is asked as the
  task is dispatched and again by the issuer as each token is asked for, so
  taking `agent` out of the policy stops it at once. An untrusted run is
  never given one.
- **A protected environment.** With `"approvers": ["group:release"]`, each
  task of a run that is not trusted that names the environment waits on a
  gate first: one the engine puts before the task as the run is made,
  `<task>.environment`, which asks when the task would otherwise start (it
  depends on what the task depended on). It is a gate as any other: listed
  in `GET /api/approvals`, decided by its approvers alone, and rejected
  after a day with no answer, and then the task does not run. A trusted
  run is not asked. The agent cannot approve it: an agent's token decides
  nothing.

```console
curl -sS -b cookies.txt -X PUT http://localhost:8080/api/environments/$ENV_ID/policy \
  -H 'Content-Type: application/json' \
  -d '{"allowed_trust":["trusted","agent"],"allowed_identity":["agent"],"approvers":["group:release"]}'
```

What a policy is, and what it is not:

- **A run's class and its ref are a CI service's word**, from an event the
  forge signed. The run's author chooses neither. Those two parts are what
  holds a pull request off an environment.
- **A run that a `type: workflow` task starts is of the class of the run
  that called it**, and for the same ref. Calling a sub-workflow gets a run
  no more than it had.
- **A pipeline's name is its author's to write.** `allowed_pipelines` narrows
  beside the other two, and is no boundary alone.
- **A run asked for through dagron-ci's run API is for the ref its caller
  names, and of a commit that is on that ref.** dagron-ci asks the forge,
  and makes no run of a commit the ref never had: any commit of a fork is
  reachable in the repository it forks. On a forge whose history it cannot
  ask, it takes the ref's own commit alone.
- **A run a person submits has no ref**, and nor has a schedule's. A policy
  that names refs does not admit them. Leave `allowed_refs` out for an
  environment that people's own runs are to use.
- **It speaks for an environment, not for a secret on the engine's host.**
  `DAGRON_SECRET_<NAME>` and the secrets directory belong to no environment.
  They are read by `trusted` runs alone: a run of another class has what an
  environment that admits it holds, and nothing else.
- **A person who may submit runs can read what a `trusted` run can.** Against
  them a policy guards a mistake. It is not a boundary.

### Runs that are not trusted

A run's **trust class** is on its record: `trusted`, `untrusted` or `agent`.
Every run an installation makes for itself is `trusted`. A CI service says
`untrusted` of the run it makes for a pull request from a fork, whose
workflow is its author's to write. An engine holds such a run to less. It
goes by the record, and by nothing the workflow says of itself.

**It runs its own containers, and the engine does nothing else on its word.**
A run that is not trusted may not use:

| Key | Why not |
|---|---|
| `notify` | the engine would post where the workflow names, with what it holds |
| `on_datasets`, and a task's `produces` | they start other runs |
| a task's `type: approval`, `workflow` or `wait` | a person's decision, a workflow registered here, a watch the engine keeps |
| a task's `defer` | the engine would call what the task names |
| a task's `service_account` | an identity of this installation's |
| a task's `gang` | it holds workers for tasks other than its own |
| a task's `repeat` | how often a loop goes round is its author's to write |
| a task's `with_output_of` | how many tasks it becomes is decided as the run goes, by what a task printed. A fan-out written in the workflow (`with_items`) is counted with the run's tasks |

A run that names one is not made (`403`, with the reason), and a task that
names one is failed where it would have been dispatched. Some keys such a
run is not given, and does not fail for having written:

- **`cache`.** An output kept under a workflow's and a task's name is every
  run's of those names. Such a run is served none and leaves none.
- **`pool`** and a **`priority`** above the default. It draws no slot a
  trusted task then waits for, and stands ahead of nothing. Its tasks draw
  from the pool `untrusted` instead, which you may cap:
  `POOLS=untrusted:4` runs four of them at once on an engine.
- **A slot of another workflow's.** `max_active_runs` counts by a
  workflow's name and its `concurrency_key`, which the author writes. Such
  a run's cap is counted among such runs, so it waits for none of yours
  and none of yours waits for it.
- **`retry_budgets`**, and the attempts a failure's class gives. The class
  is read off what the task printed, so its task has its `max_attempts`.

**An engine runs such work only where its operator has said it may.**
`DAGRON_UNTRUSTED_ISOLATION_FLOOR` is a floor for those runs' tasks, written
as `DAGRON_TASK_ISOLATION_FLOOR` is and applied over it. An engine with none
fails every task of such a run, with the reason. An engine that is given
one starts only where it can hold such a task:

- never with `EXECUTOR=local`, where a task is the engine's own process;
- on Docker, with `DAGRON_DOCKER_NETWORK` set, since a container joins the
  daemon's default network otherwise;
- on Kubernetes, with what the pod may reach said in the floor:
  `network=none`, or `network=allow:<name>`.

A task of such a run is given no service-account token, whatever the floor
says. On Docker it is given none of `DAGRON_DOCKER_MOUNTS` and not
`DAGRON_DOCKER_WORKDIR`.

**What it may spend is yours to say**, in `DAGRON_UNTRUSTED_LIMITS`. None
of it can be turned off.

| Limit | Default | What it holds |
|---|---|---|
| `timeout` | `3600` | a task's longest, in seconds. A task that names less has its own, and one that names none has this |
| `attempts` | `3` | the most a task is tried |
| `tasks` | `256` | the most a run may have. A run with more has none of them run. Such a run is the size it was made: what would grow it as it goes is refused |
| `services` | `4` | the most a task may have beside it (§10), from 0 to 8. A task with more is not run |
| `memory`, `pids`, `cpus` | `4g`, `2048`, `2` | each container made for a task, its services' among them, on Docker: a task with four services may use five times this. On Kubernetes a pod's limits are its own `resources` and its namespace's: a `LimitRange` there is yours to write |

**The images it may name are yours to list** (`DAGRON_UNTRUSTED_IMAGES`).
A task names its own image, and an executor runs what its host can: on
Docker a private image is one the host already holds, and a task that named
it would read it. A pattern's `*` stops at a `/` and `**` does not. An
image is matched as the task wrote it, so `alpine:3.20` and
`docker.io/library/alpine:3.20` are two names here. Unset, any image. A
service's image is asked the same: it is as much its author's.

```bash
# Kubernetes: a sandbox runtime, an allowlist of yours, and no privileges
DAGRON_UNTRUSTED_ISOLATION_FLOOR="runtime_class=gvisor,network=allow:forks,drop_all_capabilities=true,no_new_privileges=true"

# Docker: a network of yours, an allowlist of yours (an internal network,
# dagron-task-allow-forks), no privileges, and limits
DAGRON_DOCKER_NETWORK=dagron-jobs
DAGRON_TASK_NETWORK_ALLOWLISTS=forks
DAGRON_UNTRUSTED_ISOLATION_FLOOR="network=allow:forks,drop_all_capabilities=true,no_new_privileges=true"
DAGRON_UNTRUSTED_LIMITS="memory=4g,pids=2048,cpus=2,timeout=1800,attempts=2,tasks=64,services=2"
DAGRON_UNTRUSTED_IMAGES="docker.io/library/*,ghcr.io/acme/ci-*"
POOLS="untrusted:4"
```

Its secrets are its environments' policies' to allow, as above: none, until
an admin opens an environment to its class.

What this does not hold:

- **A pod's size.** On Kubernetes a task's `resources` and its workspace's
  size are its author's to write, and its namespace's to cap.
- **The host's kernel.** A container shares it unless the floor names a
  sandbox runtime (`runtime_class`, on Kubernetes).
- **Who may run it again.** A retry, a rerun and a clear keep the run and
  its class, and a resubmit makes a new run of the same class, for the same
  commit. Anyone who is not a viewer may ask for one.

### Where a secret's plaintext can be seen

Encrypted at rest, decrypted only in the engine's memory at dispatch, and never in
the API, run JSON or the engine log. After that it depends on the executor:

- **Kubernetes** (default `DAGRON_TASK_SECRET_ENV=secret`): the value is placed in a
  per-task Kubernetes Secret and the pod references it with `secretKeyRef`, so it is
  **not** in the Pod spec: `kubectl get pod -o yaml`, `describe` and API-server audit
  logs show a reference. The Secret is owned by the pod and deleted with it. Anyone who
  can `get secrets` in the task namespace can still read it while the task runs, and
  encrypting Secrets in etcd is the cluster's job (an `EncryptionConfiguration`, or a
  KMS provider), not dagron's. If the engine cannot create Secrets the task fails naming
  the missing permission; `DAGRON_TASK_SECRET_ENV=inline` restores the old behaviour
  (plaintext `env[].value` in the Pod spec, readable by anyone who can `get pods`).
- **Docker executor**: the value is passed as a container environment variable, so
  `docker inspect` shows it to anyone with access to the Docker socket.
- **Local executor**: it is in the subprocess environment, readable by the same OS user
  (`/proc/<pid>/environ`).

In every case the task itself sees the plaintext (it has to), and the output redactor
masks it in stored output.

---

## 7. Long scripts: where the code lives

A task's `command:` is argv. There is **no `script:` field and no file-include
directive** — a spec never pulls in another file. So "my script is 400 lines"
is really *where does the code live so the executor can reach it*.

Only three things cross into the task process: `command:` (argv), `env:` (every
backend), and `docker_image:` (container backends). `input:` does **not** — it is
stored on the task row, never passed to the process. Two constraints decide the
rest: **no workflow can mount a host path** (the Docker backend mounts only what
its operator lists in `DAGRON_DOCKER_MOUNTS`, and a task's `workspace:` is an
empty volume, never a host directory), and `DAGRON_ARTIFACTS` is a host
directory, so it can pass files between *host* tasks but cannot carry a script
into a container.

| | approach | when |
| --- | --- | --- |
| 1 | **bake it into the image** — `command: ["/app/bin/transform.sh", "--shard", "3"]` | docker/k8s; the default answer. The script's version is the image tag, so a pinned re-run runs the code that ran then. |
| 2 | **absolute path on the engine host** — `command: ["bash", "/opt/dagron/scripts/x.sh"]` | `EXECUTOR=local` only. |
| 3 | **fetch when the run starts** into `$DAGRON_ARTIFACTS` (host) or inside each container | the script changes faster than you rebuild images. |
| 4 | **the body in an env var** — `command: ["sh", "-c", 'eval "$SCRIPT"']` | tens of lines, no build pipeline. |

Two traps worth stating outright:

- For **2**, use absolute paths — a relative path resolves against the task's
  working directory: the engine process's own, unless the operator set
  `DAGRON_LOCAL_WORKDIR` or the task has a `workspace:` (§9), never the
  workflow's location. And every engine replica needs the file: any replica may
  claim any task, so a script on one box fails whenever another wins the claim.
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

## 9. A directory per attempt: `workspace:`

A task that writes files for its own later commands — a build's `target/`, a
download it then unpacks — wants a directory of its own, rather than the
image's copy-on-write layer or the node's root disk. `workspace:` asks for one:

```yaml
tasks:
  - name: build
    docker_image: rust:1.86-bookworm
    command: ["sh", "-c", "cargo build --release && cargo test --release"]
    workspace: { size: 20Gi }    # or {} for no particular size
```

The task starts in it, it is empty at the start of every attempt (but for
its checkout, below), and it is removed after the attempt:

| Executor | The workspace |
| --- | --- |
| Kubernetes | an `emptyDir` at `/workspace` with `size` as its `sizeLimit`; from `DAGRON_WORKSPACE_PVC_FROM` up, a generic ephemeral volume of that size |
| Docker | a volume at `/workspace` made for the attempt, removed after it; the operator's `DAGRON_DOCKER_WORKDIR` instead, where set |
| Local | a new temporary directory; `DAGRON_LOCAL_WORKDIR` instead, where set |

`size` binds on Kubernetes only. Where the operator set `DAGRON_CI_EXEC_IMAGE`,
the Kubernetes and Docker executors also install `dagron-ci-exec`, the step
runner of dagron-ci's jobs, read-only at `/dagron/bin` in each workspace task.
Only command tasks take a workspace, and `size` is checked at validation: a
number and a unit, `Ki`…`Ei` or `k`…`E`.

**A checkout.** `checkout:` starts the workspace with a commit in it, fetched
before the task's command runs:

```yaml
parameters: { sha: "" }
tasks:
  - name: test
    docker_image: rust:1.86-bookworm
    command: ["cargo", "test"]
    workspace:
      checkout: { repository: https://github.com/acme/widgets.git, commit: "{{ sha }}" }
```

`repository` is an `https://` URL (`http://` for one fetched with no
credential), and `commit` a full commit id, 40 or 64 hex digits: both may
name the run's parameters, and are checked once those are filled in, so a
run whose parameters do not make them valid is refused. The engine fetches
that one commit by its id, at depth 1, and checks it out detached.
`depth:` says how many commits deep it comes instead: `depth: 0` is all of
the repository's history, every branch's and the tags, for a task that
asks git what changed (`git merge-base`, `git describe`). On
Kubernetes and Docker a container from the operator's
`DAGRON_CHECKOUT_IMAGE` (any image with `git` and `sh`) fetches it, and a
task with a checkout fails where the engine has none; the local executor
runs `git` itself. A fetch that fails fails the attempt, with git's words.

**A forge token.** `forge_token: { permissions: { contents: write } }` on a
task with a checkout gives it a token for the checkout's repository, as
`GITHUB_TOKEN`: the engine asks the token service
(`DAGRON_CHECKOUT_TOKEN_URL`) for it as each attempt starts, with those
permissions, by a GitHub App's names (`contents`, `pull_requests`,
`statuses`, …, each `read` or `write`), and has it revoked once the attempt
is over. The service decides what it gives: dagron-ci's gives a run that is
not trusted read alone. It is masked in the task's log, and on Kubernetes it
is in the task's Secret. dagron-ci writes it for a workflow's job, and a
pipeline of `.dagron/ci` names it as dagron does.

**Tools.** `tools: [node]` installs, with `dagron-ci-exec`, the Node.js a
GitHub workflow's JavaScript action runs on, at `/dagron/bin/node`: one
build that runs in an image of any libc. It is some 130 MB, so only a task
that names it is given it; dagron-ci names it for a job that uses an
action. The image `DAGRON_CI_EXEC_IMAGE` names holds it beside the step
runner.

**Actions.** `actions:` names repositories the task's steps use as
actions, each fetched before the task's command runs, by the same image as
a checkout, into a directory of its own under `/dagron/actions` that the
task reads and cannot change:

```yaml
    workspace:
      actions:
        - { repository: https://github.com/octo/hello-action.git, ref: v1, path: octo/hello-action/v1 }
```

`ref` is a tag, a branch or a commit's full id, fetched as it stands when
the attempt starts, alone (depth 1); the attempt's log says which commit it
was. `path` is where under `/dagron/actions`, and the task is told that
directory in `DAGRON_ACTIONS_DIR` (on the local executor, a directory made
for the attempt). No credential is sent with an action's fetch but where
it says `credential: true`: then the engine asks its token service
(`DAGRON_CHECKOUT_TOKEN_URL`) for one for that repository, as for a
checkout, and sends it to that repository's URL alone. dagron-ci's service
gives one for an action of the run's repository's own owner's, in a trusted
run. A task that asked for no network (`isolation: { network: none }`)
cannot have an action on Kubernetes. dagron-ci writes this for a workflow's
actions; a task written by hand may name its own.

A public repository needs nothing more. For a private one, the operator
names a token service, `DAGRON_CHECKOUT_TOKEN_URL`, which gives each attempt
a credential of its own and revokes it after: dagron-ci's is one, minting a
GitHub App's installation token for that repository alone, and keeping a
row for each. The service is reached over TLS, or on the engine's own
machine: the engine starts on no other. The credential reaches the fetch
and nothing else: not the task's environment, nor the repository's config,
nor its log.

---

## 10. A container beside a task: `services:`

A test wants a database, and a data task wants a proxy to one. Started from
the task's own command, such a thing has to be in the task's image, and is
the task's to wait for and to stop. `services:` gives the task a container
of its own for each instead:

```yaml
tasks:
  - name: test
    docker_image: rust:1.86-bookworm
    command: ["cargo", "test"]
    env:
      - { name: DATABASE_URL, value: "postgres://postgres:pw@db:5432/app" }
    services:
      - name: db
        image: postgres:16-alpine
        env:
          - { name: POSTGRES_PASSWORD, value: pw }
          - { name: POSTGRES_DB, value: app }
        ready: pg_isready -h localhost -U postgres -d app
```

Each service is started before the task and seen ready, in the order they
are written, and is removed when the task ends, however it ends. Every
attempt has services of its own.

| Key | |
|---|---|
| `name` | required: lowercase letters, digits and `-`, beginning with a letter. The task reaches the service by it |
| `image` | required. May name the run's parameters. Fetched as the task's `image_pull_policy` says |
| `args` | what the image's entrypoint is given, in place of the image's own command |
| `env` | a list of names and values, as a task's. Plain values: a `value_from` is refused, and no secret is given to a service |
| `ready` | a command run in the service's own container until it exits 0: one line for `sh -c`, or a list of a program and its arguments for an image with no shell. Left out, a service is ready once it has started |
| `ready_timeout_secs` | how long `ready` has to succeed: 60 unless set, at most 600 |
| `resources` | `requests` and `limits`, as a task's. Kubernetes only |

- **Where the task finds one.** At its name, on the port the image listens
  on: `db:5432`. The task and its services share one network, as a pod's
  containers do, so `localhost:5432` is the same place, and two services of
  one task cannot listen on one port.
- **A service that is not ready fails the attempt**, and the task never
  starts: one whose `ready` has not succeeded in the time it has, or that
  stops first. The reason names the service and has the last lines it wrote.
  A service's output is kept nowhere else.
- **A service is held as its task is**: the task's `isolation` (its network
  and its uid among it) and, in a run that is not trusted, the same limits
  and the same list of images. Under `drop_all_capabilities`, an image that
  starts as root and then changes user does not start: Postgres's own does
  that, unless it is given a uid to run as.
- **A task has at most eight**, and only a task that runs a command has any.

| Executor | A service |
| --- | --- |
| Kubernetes | a sidecar container of the task's pod (an init container that restarts), with `ready` as its startup probe. Needs Kubernetes 1.29 or later: a task with services is refused on an older cluster |
| Docker | a container made for the attempt. The task's container and the other services' are made in the first service's network, which is on `DAGRON_DOCKER_NETWORK` where that is set |
| Local | refused: a task there is a process of the engine's own. The task is failed once, with the reason, and is not tried again |

What differs between the two that run them:

- **A service that stops while its task runs** is started again on
  Kubernetes, and not on Docker. The task is stopped for neither.
- **On Docker, the first service is the one the others reach the network
  through.** If it stops, the task and the other services have no network
  left.
- **A service's name is its task's alone, and its port is not.** On Docker
  a service is on `DAGRON_DOCKER_NETWORK`, and on Kubernetes in a pod with
  an address of its own: whatever else is on that network reaches the
  service's port at that address, as it would reach a port the task itself
  listened on. Keep tasks that should not meet off one network, or give a
  service a password.
- **On Podman, a task that asked for no network** (`isolation.network:
  none`) reaches its services at `localhost` and not at their names. Docker
  and Kubernetes give it both.
- **`ready` with no `sh` in the image** is written as a list:
  `ready: [/bin/grpc_health_probe, "-addr=:50051"]`.

### A run's artifacts from a container: `artifact_api:`

A task on the local executor sees its run's artifacts in a directory
(`DAGRON_ARTIFACTS`). One in a container, a pod or on a remote runner does
not, unless the operator mounts it. `artifact_api: true` gives it the same
artifacts over dagron-api's API instead: `DAGRON_ARTIFACTS_API`, and a
credential of the attempt's own, `DAGRON_ARTIFACTS_TOKEN`.

```yaml
tasks:
  - name: build
    docker_image: docker.io/curlimages/curl:8.10.1
    artifact_api: true
    command:
      - sh
      - -c
      - |
        echo hello > out.txt
        # The credential on stdin, not on a command line.
        printf 'Authorization: Bearer %s\n' "$DAGRON_ARTIFACTS_TOKEN" |
          curl -fsS -H @- -T out.txt "$DAGRON_ARTIFACTS_API/build/out.txt"
```

- **While it runs**, it reads any artifact of its run (`GET
  $DAGRON_ARTIFACTS_API/<task>/<name>`) and writes its own (`PUT`, under its
  own task's name: another's is `403`). The bytes are dagron-api's store's,
  the one an approval's `binds` is checked against ([API.md](API.md#a-tasks-artifacts)).
- **It is the engine operator's to offer** (`DAGRON_ARTIFACT_API_URL`, where
  tasks reach dagron-api, `https://`); on an engine that offers none the task
  is failed, with the reason. A remote runner gives its tasks
  `$DAGRON_RUNNER_URL/task/v1/artifacts` (`DAGRON_RUNNER_ARTIFACTS_URL`).
- **A run that is not trusted may ask for it**: it reaches its own run's
  artifacts, and writes its own task's alone.

### A Docker daemon of the task's own: `docker_daemon:`

A task that runs `docker build` or `docker run` wants a daemon to send it
to. `docker_daemon: true` gives it one of its own, beside it as a service
named `docker`, and `DOCKER_HOST=tcp://127.0.0.1:2375`, which the Docker
command line reads:

```yaml
tasks:
  - name: image
    docker_image: docker.io/library/docker:27-cli
    docker_daemon: true
    command: ["docker", "build", "-t", "app:test", "."]
```

- **It is the engine operator's to offer** (`DAGRON_DOCKER_DAEMON=dind`,
  [CONFIG.md](CONFIG.md)), since a daemon in a container runs privileged
  (or, on Sysbox, below).
  On an engine that offers none, and on one whose executor is not Docker,
  the task is failed once, with the reason, and is not tried again. An
  engine that offers one runs no work that is not trusted.
- **A run that is not trusted may not ask for one**: it is refused when the
  run is made, as `identity` is.
- **It is one of the task's services**: started and seen ready
  (`docker info`) before the task starts, removed with the attempt, and
  with what it built and ran. A task with it has at most seven services of
  its own, and none named `docker`. A `DOCKER_HOST` the task sets is kept.
- **It listens on the loopback address alone**, without TLS, which the
  task and its services share: nothing else on the network reaches it, as
  something could reach a service's port. The task reaches it there with a
  network or with none.
- **It has what the task has, at the same paths**: the operator's mounts,
  and the workspace an init container made (`DAGRON_CI_EXEC_IMAGE`). So
  `docker run -v "$PWD:/src"` from the task finds the task's files. Not an
  operator's mount at `/var/run`, `/run` or `/var/lib/docker`, where a
  daemon keeps its own: the host daemon's socket is not given to it. A
  task with a workspace on an engine with no `DAGRON_CI_EXEC_IMAGE`, and no
  `DAGRON_DOCKER_WORKDIR`, is refused: its daemon would have an empty
  directory where the task has its files.
- **It is not held to the task's `isolation`**, nor to the engine's
  floor: its container is privileged and runs as root, whatever the task's
  envelope says, and the task can start a privileged container through
  it. The task's own container is held as it was.
- **Unprivileged, on Sysbox.** Where the operator names a runtime for it
  (`DAGRON_DOCKER_DAEMON_RUNTIME=sysbox-runc`), the daemon's container is
  run on that runtime and is not privileged: its root, and the root of
  whatever it runs, is no user of the engine's host. It works as before,
  `docker build` and `docker run` and the workspace among it.
- **It is the task's first service**: the task and its other services
  join its network, which on Sysbox it has to own to make its own devices
  in it.

### A privileged task, on a pool of its own: `privileged:`

A job that runs a cluster inside itself (kind, k3s), a daemon it starts
itself, or anything that mounts or makes devices needs its own container
privileged: root on its host, with every device and capability.
`privileged: true` asks for that, and `runner_class` sends it to the pool
that gives it:

```yaml
tasks:
  - name: e2e
    docker_image: docker.io/library/docker:27-dind
    runner_class: dind
    privileged: true
    command: ["sh", "-c", "dockerd-entrypoint.sh & sleep 5 && docker run --rm hello-world"]
```

- **It is the engine operator's to allow** (`DAGRON_TASK_PRIVILEGED=allow`,
  [CONFIG.md](CONFIG.md)), on a **dedicated pool**: an engine that allows
  it claims the runner classes named for such work and not `default`, runs
  no work that is not trusted, and, on Kubernetes, sends its task pods to
  nodes of their own in a namespace whose Pod Security admits a privileged
  pod, which it asks of the API server as it starts. An engine that is
  none of that does not start. Label that namespace
  `pod-security.kubernetes.io/enforce=privileged`, and keep it for the
  pool's tasks: the namespaces of other engines keep a stricter level.
- **Elsewhere the task is failed once**, with the reason, and is not tried
  again; a remote runner is given none.
- **A run that is not trusted may not ask for it**: it is refused when the
  run is made.
- **The task's own container alone** is privileged: what installs its
  tools and fetches its checkout, its services and its steps are not.
- **It is held to none of what contradicts it**: a task held to dropped
  capabilities, no new privileges, a runtime class or seccomp's default
  profile, by its own `isolation` or by the floor, is refused. A read-only
  root, a uid and a network are kept.

---

## 11. One task's word to the next: `env_from:`

A task decides something, and a task after it acts on what was decided.
`env_from:` gives the later task a variable that holds what the earlier one
came to: its status, or the last line it printed.

```yaml
tasks:
  - name: decide
    command: ["sh", "-c", "./pick-target.sh"]     # prints the target last
  - name: deploy
    depends_on: [decide]
    env_from:
      - { name: TARGET, task: decide, read: last_line }
    command: ["./deploy.sh"]                       # reads $TARGET
  - name: report
    depends_on: [deploy]
    trigger_rule: all_done
    env_from:
      - { name: DEPLOYED, task: deploy, read: status }
    command: ["sh", "-c", "echo \"the deploy $DEPLOYED\""]
```

- **`read: last_line`** is the last line the task printed that is not
  empty, without its line ending. Why the last line: what is kept of a task
  on Docker and Kubernetes is every line of both its streams, and the last
  is the one a task can be sure is its own last word. A task that hands on
  more than a word prints one line of JSON last. A task that printed
  nothing hands on an empty value.
- **`read: status`** is the task's status as the run has it: `succeeded`,
  `failed`, `skipped` or `cancelled`. A task that runs whatever became of
  the one before it (`trigger_rule: all_done`) reads which it was.
- **It is read when the task is claimed.** A task that is run again reads
  what the earlier one came to then.
- **The task it names is one this task depends on**, so that what is read
  is there to read. Anything else is refused when the run is asked for.
- **It names one task.** A task that was fanned out is several, each with
  a name of its own (`build.0`, `build.1`): name the one that is read. What
  they all printed is iterated with `with_output_of:`
  ([`LOOPS.md`](LOOPS.md)).
- **It is a value, and never a template.** The variable holds the line as
  it was printed. Nothing in it is filled in, and it is given to no
  `command:`: the command reads the variable.
- **A line is at most 32 KiB, and a task is given 256 KiB together.** A
  line longer, or values that are more together, fail the task that asked
  for them, once and with the reason, and the task is not run without them.
- **What a task printed is kept with its secrets redacted**, and so is the
  line it hands on.

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
