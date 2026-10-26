# dagron-runner

dagron's remote runner. It runs a dagron task somewhere an engine is
not: a Mac mini, a Windows host, a GPU box, a team's own cluster, a machine
behind NAT. It dials dagron-api over HTTPS and nothing dials it, so it needs
no open port. It holds no database credentials and owns no state.

```
dagron-runner            ──HTTPS──▶  dagron-api  ──▶  Postgres  ◀──  engine
  a process, a container,             /runner/v1        task_runs       retries, repeat,
  or a pod per task                                     runner_*        defer, cache, …
```

- **It takes the tasks of the classes it serves.** A task's `runner_class` is
  its label: a runner registered for `gpu` takes the ready tasks of class
  `gpu`. dagron-api claims them with the engine's own claim: the same
  priorities, pools and fencing.
- **It runs them as an engine does**, on dagron-executor's executors and its
  worker pool: the same redaction of secrets, caps on logs and output,
  timeouts, and teardown of work whose claim is gone.
- **It says how each came out, and an engine applies it** as it applies a
  result of its own workers: retries and their backoff, `repeat:`, `defer:`,
  the cache, `produces:`, dead letters.
- **A runner that dies loses only what it ran.** Its session's lease lapses,
  and an engine takes its tasks back to `ready`, where another runner takes
  them.

## Running one

1. **An admin mints a join token** for the labels the runner will serve:

   ```bash
   curl -b cookies.txt -X POST "$API/api/runners/join-tokens" \
     -H 'Content-Type: application/json' \
     -d '{"labels": ["gpu"], "max_uses": 1, "ttl_secs": 86400}'
   # → {"id": "…", "token": "dgrj_…", …}   (shown once)
   ```

2. **The runner starts with it**, registers, and keeps the credential it is
   given (`dgr_…`) for its next start:

   ```bash
   echo "dgrj_…" > /etc/dagron-runner/join-token
   DAGRON_RUNNER_URL=https://dagron.example.com \
   DAGRON_RUNNER_JOIN_TOKEN_FILE=/etc/dagron-runner/join-token \
   DAGRON_RUNNER_EXECUTOR=docker \
   DAGRON_RUNNER_CAPACITY=2 \
     dagron-runner
   ```

   Or register once and hand the credential to many processes (each opens a
   session of its own): `dagron-runner register` writes it to
   `DAGRON_RUNNER_TOKEN_FILE` and exits. That is how a Deployment of runners
   shares one: a Secret mounted at `DAGRON_RUNNER_TOKEN_FILE`.

3. **The engines leave the class to it.** An engine whose `RUNNER_CLASSES`
   lists `gpu` would race the runners for those tasks, and one with no
   `RUNNER_CLASSES` serves every class: give each engine the classes it
   serves, and the runners theirs.

A workflow sends a task to it by its class:

```yaml
tasks:
  - name: train
    runner_class: gpu
    docker_image: pytorch/pytorch:2.4.0-cuda12.4-cudnn9-runtime
    command: ["python", "train.py"]
```

## Settings

| Variable | Default | |
|---|---|---|
| `DAGRON_RUNNER_URL` | — | Where dagron-api is: `https://`, or `http://` to this machine |
| `DAGRON_RUNNER_JOIN_TOKEN_FILE` | — | The one-time join token, for the first start |
| `DAGRON_RUNNER_TOKEN_FILE` | `$DAGRON_RUNNER_STATE_DIR/credential` | The runner's credential: written by the first start, or given |
| `DAGRON_RUNNER_STATE_DIR` | `~/.dagron-runner` | Where the credential is kept, where no file is named |
| `DAGRON_RUNNER_NAME` | the host's name | How it is listed |
| `DAGRON_RUNNER_LABELS` | all it was registered for | The labels (runner classes) this process serves, comma-separated |
| `DAGRON_RUNNER_CAPACITY` | `1` | Tasks at once, 1 to 256 |
| `DAGRON_RUNNER_EXECUTOR` | `docker` | `local`, `docker` or `k8s` |
| `DAGRON_RUNNER_SECRETS` | `server` | `server`, or `home`: see [Secrets](#secrets) |
| `DAGRON_RUNNER_DRAIN_TIMEOUT_SECS` | — (as long as the tasks run) | How long a drain waits before it stops the tasks |
| `DAGRON_RUNNER_INSECURE` | — | `allow`: plain `http://` to another host. The credential, and every secret under `server`, would cross the network in the clear |
| `DAGRON_RUNNER_OIDC_REQUEST_URL` | `$DAGRON_RUNNER_URL/oidc/token` | Where a task that asks for an identity trades its request credential for tokens, as the task's network reaches dagron-api: `https://`, or `http://` where `DAGRON_RUNNER_INSECURE` allows it |
| `DAGRON_LOG_TASK_CAP_BYTES` | `67108864` (64 MiB) | As the engine's: how much of a task's log is sent as it runs, and of its output. No more than dagron-api's own, which keeps a runner's log up to twice its cap and 1 MiB and refuses a segment past that |
| `DAGRON_LOG_TAIL_BYTES` | a quarter of the cap | As the engine's: how much of the end of a log past its cap is sent once the task is over, under a line that says what was left out; no more than the cap; `0`, none |

The executor is set as the engine's is, by the same variables: `DOCKER_IMAGE`
(the default image), `DAGRON_DOCKER_MOUNTS`, `DAGRON_DOCKER_WORKDIR`,
`DAGRON_DOCKER_NETWORK`, `DAGRON_DOCKER_LOG_DRIVER`, `DAGRON_CI_EXEC_IMAGE`,
`DAGRON_CHECKOUT_IMAGE`; `K8S_IMAGE` and `K8S_NAMESPACE` on Kubernetes;
`DAGRON_LOCAL_WORKDIR`, `DAGRON_LOCAL_COMMAND_ALLOWLIST` and
`DAGRON_LOCAL_ENV_PASSTHROUGH` on the local executor; and
`DAGRON_INSTALLATION`, which labels the containers and pods it makes. See
[`docs/CONFIG.md`](../../docs/CONFIG.md).

A task's log goes up as it is written: what it writes waits a second, then
goes up a segment (up to 1 MiB) at each quarter-second tick while any waits,
and
what waits of it when the task is over, the end of a log past its cap
among it, goes up before its result, tried again for about four seconds
where an answer is lost; past that the result is said without it. Upgrade
dagron-api before its runners: an older one keeps a runner's log up to its
cap alone, and refuses the segments that carry a log's end.

## Executors

| `DAGRON_RUNNER_EXECUTOR` | A task is | For |
|---|---|---|
| `local` | a process on this host | a Mac or a Windows host, bare metal, a GPU box whose tools are installed. Nothing is isolated |
| `docker` | a container on this host's Docker | a Linux host with Docker; `services:` and a Docker daemon beside the task (`DAGRON_DOCKER_DAEMON`) |
| `k8s` | a pod in `K8S_NAMESPACE` | the runner as an in-cluster controller: one runner pod starts each task as a pod of its own. Built with `--features kubernetes` |

**Isolation is the host's to hold.** A task's `isolation:` is raised to this
host's floor (`DAGRON_TASK_ISOLATION_FLOOR`, read as the engine reads it), and
a task this executor cannot hold to it is failed, never run believing itself
sandboxed. A floor this executor cannot hold stops the runner as it starts.

## Secrets

- **`server`** (the default): dagron-api reads the task's secrets from its
  environment's store, by that environment's policy, and sends their values
  with the task. dagron-api has no store of its own to read, so a secret its
  environment does not hold fails the task.
- **`home`** ("secrets stay home", 7.5): dagron-api sends each secret's name
  alone, and never reads or sends a value. The runner resolves it from its own
  store, as the engine reads its own: `DAGRON_SECRET_<NAME>`, then the file
  `$DAGRON_SECRETS_DIR/<name>`. Who may use them is still the run's
  environment's policy, asked by dagron-api before the task is given out.

Either way the values are masked in the task's log and output before they
leave the runner.

## Drain

`SIGTERM` (or Ctrl-C) drains: the runner takes nothing more, lets what runs
finish, closes its session and exits 0. A second signal stops the running
tasks now; each is said `cancelled`. An admin can drain a runner from
dagron-api (`POST /api/runners/{id}/drain`), and it does the same once its
next heartbeat says so. A runner that is drained exits: what supervises it
starts it again, as a new session, for an upgrade say.

## Autoscaling

`dagron-runner scale` (built with `--features kubernetes`) sizes Deployments
of runners by the work there is for their labels: enough replicas for the
tasks that are ready and those that run, each running
`DAGRON_SCALE_PER_REPLICA` at once, up at once, down once the work has asked
for fewer for `DAGRON_SCALE_DOWN_AFTER_SECS`.

| Variable | Default | |
|---|---|---|
| `DAGRON_SCALE_TARGETS` | — | `label=deployment` or `label=namespace/deployment`, comma-separated |
| `DAGRON_SCALE_MIN`, `DAGRON_SCALE_MAX` | `0`, `10` | Replicas, each target |
| `DAGRON_SCALE_PER_REPLICA` | `1` | Tasks one replica runs at once: its `DAGRON_RUNNER_CAPACITY` |
| `DAGRON_SCALE_INTERVAL_SECS` | `15` | How often the queue is read |
| `DAGRON_SCALE_DOWN_AFTER_SECS` | `300` | How long the work must ask for fewer |

It reads `GET /runner/v1/queue` with a runner credential
(`DAGRON_RUNNER_TOKEN_FILE`) registered for the labels it scales; it opens no
session and runs nothing. A replica scaled away is sent SIGTERM and drains:
give its pod a `terminationGracePeriodSeconds` as long as its longest task.
The enterprise build sizes a cloud's VM pools the same way.

## The protocol

`/runner/v1`, version 0. The wire types are
[`dagron_core::runner`](../dagron-core/src/runner.rs).

| Method and path | |
|---|---|
| `GET /runner/v1` | The protocols dagron-api speaks, and the oldest runner release it takes |
| `POST /runner/v1/register` | A join token exchanged for a credential, `runner:<id>` |
| `POST /runner/v1/sessions` | A process opens a session: its labels, capacity, release, protocols and secrets mode. `426` where there is no protocol in common, or the runner is older than `DAGRON_RUNNER_MIN_VERSION` |
| `POST /runner/v1/sessions/{id}/claim?wait=30` | Long-polls for up to its free capacity of tasks |
| `POST /runner/v1/sessions/{id}/heartbeat` | Renews the session, and every task it holds, in one statement; says which tasks to stop, and whether to drain. `410` for a session that is gone |
| `POST /runner/v1/tasks/{id}/logs/{seq}` | A segment of a task's log, numbered 0, 1, 2, … through the attempt; segment 0, and a seal (`reset`), replace it. One whose answer was lost is sent again with the same number and text, and written once |
| `POST /runner/v1/tasks/{id}/result` | How a task came out, kept for an engine to apply. `409` where the fence is not the task's any more |
| `DELETE /runner/v1/sessions/{id}` | Drain: no more tasks; the session ends once what it runs is over |
| `GET /runner/v1/queue?labels=` | The work there is for some labels (the autoscaler's) |

Errors are RFC 9457 problem documents.

## What protocol 0 does not carry

These are refused, and the task failed saying so, rather than run without
them: a task of a run that is not trusted (a fork's pull request, an
agent's); a `workflow` or `wait` task, which an engine handles itself; a
gang's member; a task with a `forge_token`, whose credential an engine
mints. A private repository's checkout is fetched with no credential.

A task with an `identity` (OIDC) is carried: dagron-api mints its request
credential as an engine does, and the runner gives the task
`DAGRON_OIDC_REQUEST_URL`, `_TOKEN` and `_AUDIENCE`, the URL being where the
task reaches dagron-api's `/oidc/token`
(`DAGRON_RUNNER_OIDC_REQUEST_URL`, else `$DAGRON_RUNNER_URL/oidc/token`). A
dagron-api that is no issuer fails the task saying so. A task with `cache:` always runs (its result is memoized as
usual). Artifacts and path caches are not given to a remote task.

## Building it

```bash
cargo build --release -p dagron-runner                       # local and Docker executors
cargo build --release -p dagron-runner --features kubernetes # and Kubernetes, and `scale`
```

Release assets carry it for Linux x86_64 and arm64 (static, musl), macOS
arm64 and Windows x86_64. An image: [`Dockerfile`](Dockerfile).
`scripts/runner-e2e.sh` runs it end to end against an engine, dagron-api and
Postgres.
