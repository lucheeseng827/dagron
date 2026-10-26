# Running CI jobs: an operator's notes

How the things that run CI jobs are set up, bounded, watched and repaired.
It gathers what an operator needs from the settings in
[`crates/dagron-ci/README.md`](../crates/dagron-ci/README.md),
[`CONFIG.md`](CONFIG.md), [`METRICS.md`](METRICS.md) and
[`OPERATIONS.md`](OPERATIONS.md), and points at each for the detail.

It gives settings and their defaults. **It gives no figure for capacity or
timing**: none is claimed for dagron-ci, and how much a deployment carries
is its own to measure.

## Two kinds of job

| | A pipeline's job | A fleet job |
|---|---|---|
| Whose pipeline | a file in `.dagron/ci`, or a workflow file dagron-ci reads | a workflow of GitHub Actions' own |
| Who decides what runs, and shows it | dagron-ci and the engine | GitHub Actions |
| What runs it | the engine, as a container or a pod of the job's image | GitHub's own runner, which dagron-ci starts as a dagron run |
| Where its result is said | a status or a check run that dagron-ci posts | GitHub's own checks: dagron-ci posts nothing for it |
| On | by connecting a repository | `CI_GITHUB_FLEET_FILE` |

A pipeline's job runs where an engine runs it, or on a **remote runner**
that dials in to dagron-api (`dagron-runner`, [below](#remote-runners)): a
Mac, a Windows host, a GPU box, a team's own cluster.

## A pipeline's jobs

### Where they run

A job is a task of an ordinary dagron run: one container, one workspace,
retried as a unit. The engine's executor decides what a container is.

| `EXECUTOR` | A job is | For CI |
|---|---|---|
| `docker` | a container on the engine's host | what `compose.ci.yaml` uses |
| `kubernetes` | a pod in the engine's namespace | what the chart uses |
| `local`, the default | a process on the engine's machine | a job with `services:` is refused, and nothing is isolated |

- **The image** is the job's `docker_image`, else the pipeline's
  `task_defaults.docker_image`, else the engine's default (`DOCKER_IMAGE`,
  `K8S_IMAGE`).
- **Pulls.** On Docker the engine pulls an image the daemon lacks before
  each attempt, **with no credentials**: a private image has to be on the
  host already. On Kubernetes the kubelet pulls. A pull that fails fails
  the attempt, with what the runtime said.
- **Two images are the engine's to be told of.** `DAGRON_CHECKOUT_IMAGE`
  fetches a job's commit: without it, on Docker and Kubernetes, a job with
  a checkout fails. `DAGRON_CI_EXEC_IMAGE` gives each job the step runner.
  On the chart both go in `engine.extraEnv`.

### The step runner

A job's steps are run by `dagron-ci-exec` where the job has one, and by a
shell script where it has not
([its README](../crates/dagron-ci-exec/README.md)).

- **Where it comes from**: `/dagron/bin` in the job, which the engine
  fills from `DAGRON_CI_EXEC_IMAGE` before the job starts, or the image's
  own copy on its `PATH`.
- **What the shell does without it**: the same steps, markers and exit
  code. It keeps a step's time limit only where the image has
  `timeout(1)`, passes no cancellation on to a running step, and reads no
  test report and no step summary.
- **A job from a GitHub workflow file has no fallback.** Without the step
  runner it says so and exits 125. Set `DAGRON_CI_EXEC_IMAGE` on an engine
  that runs them.

### Workspaces and checkouts

A job with steps starts in a directory made for that attempt and removed
after it, with the event's commit checked out: one commit, at depth 1,
detached, unless the pipeline asks for more history.

| Executor | The workspace |
|---|---|
| Kubernetes | an `emptyDir` at `/workspace`, bounded by the job's `workspace.size`; a volume of its own from `DAGRON_WORKSPACE_PVC_FROM` up |
| Docker | a volume made for the attempt; the operator's `DAGRON_DOCKER_WORKDIR` where set |
| Local | a new temporary directory |

- **A job's caches are kept where the operator gives the engine a store**
  ([Caches](#caches)). There is no artifact store of CI's own.
  dagron-api has routes for artifacts
  ([`API.md`](API.md#artifacts-encrypted-at-rest--g-c2)), and nothing of
  CI's puts a job's files there.
- **A private repository's checkout** needs the GitHub App, and an engine
  that can ask dagron-ci for the credential (`DAGRON_CHECKOUT_TOKEN_URL`
  and its secret). On the chart that holds only where dagron-ci shares the
  engine's pod, which is the default placement.

### How much runs at once

Each bound is a setting of the engine's, of the pipeline's, or of
dagron-ci's. Their defaults are in [`CONFIG.md`](CONFIG.md).

| Bound | Set by | What it holds |
|---|---|---|
| `WORKER_COUNT` | the engine | tasks running at once on one engine |
| `MAX_INFLIGHT_RUNS` | the engine | runs in flight; past it a submit is told to come back |
| `POOLS`, and a job's `pool` | the engine, then the pipeline | tasks of one pool at once |
| `RUNNER_CLASSES`, and a job's `runner_class` | the engine, then the pipeline | which engine takes which jobs |
| A job's `priority` | the pipeline | which ready job is taken first |
| `concurrency: { key }` | the pipeline | one run of a key at a time: a newer one waits |
| A namespace's `ResourceQuota` | the cluster | a pod it refuses waits, and is tried again (`DAGRON_QUOTA_WAIT_MAX_SECS`) |
| `CI_EVENT_WORKERS` | dagron-ci | events being turned into runs at once |
| `DAGRON_UNTRUSTED_LIMITS`, pool `untrusted` | the engine | what a fork's run may use ([security](CI_SECURITY.md)) |

- An event that finds its concurrency key held, or GitHub's rate limit
  spent, waits and spends no attempt.
- A job's `resources` are its pod's, on Kubernetes, and `workspace.size`
  is a bound there alone.

### Time limits

- **A job**: its `timeout_secs`, else the pipeline's
  `task_defaults.timeout_secs`, else `CI_DEFAULT_TIMEOUT_SECS` (an hour).
  `DAGRON_MAX_TASK_TIMEOUT_SECS` is a ceiling on them all.
- **A step**: its own `timeout_secs`. Past it the step is stopped and the
  job fails with exit code 124. Where a shell runs the steps in the step
  runner's place, that holds only in an image that has `timeout(1)`.
- **A run**: `run_timeout_secs`, where the pipeline sets one.
- **A stop**: a job that is cancelled, or out of time, has
  `CI_STEP_GRACE_SECS` (10) to end before it is killed.

## Remote runners

A job whose `runner_class` (a pipeline's own, or its `task_defaults`') is
served by a `dagron-runner` runs there. The runner dials
dagron-api over HTTPS and holds no database credentials; nothing dials it.
The reference is the [runner's README](../crates/dagron-runner/README.md);
this is what an operator does with it.

### Setting it up

1. **Choose the class**, say `mac-arm64`, and **leave it to the runners**:
   no engine's `RUNNER_CLASSES` lists it, and every engine has a
   `RUNNER_CLASSES` (one with none serves every class, this one too).
2. **Mint a join token** for it, as an admin, signed in:
   `POST /api/runners/join-tokens {"labels": ["mac-arm64"], "max_uses": 1}`.
   It is shown once, and lives a day unless `ttl_secs` says otherwise.
3. **Start the runner** with it (`DAGRON_RUNNER_URL`,
   `DAGRON_RUNNER_JOIN_TOKEN_FILE`, `DAGRON_RUNNER_EXECUTOR`,
   `DAGRON_RUNNER_CAPACITY`). It registers once, keeps its credential, and
   from then on is `runner:<id>`. Many processes may share one credential:
   each opens a session of its own. `deploy/runner/` has a systemd unit and
   a Kubernetes manifest.

| Executor | A job is | |
|---|---|---|
| `docker` (the default) | a container on the runner's host | the step runner and the checkout image as the engine's: `DAGRON_CI_EXEC_IMAGE`, `DAGRON_CHECKOUT_IMAGE` |
| `k8s` | a pod in the runner's namespace | the runner as an in-cluster controller |
| `local` | a process on the runner's host | a Mac or a Windows host: nothing is isolated |

### What runs there, and what does not

- **Retries, `repeat:`, `defer:`, the cache and dead letters** are the
  engine's: a runner says how a job came out, and an engine applies it as
  it applies its own workers' results.
- **Secrets**: by default dagron-api reads them from the run's environment,
  by its policy, and sends them with the job. With
  `DAGRON_RUNNER_SECRETS=home` it sends their names alone, and the runner
  reads them from its own host (`DAGRON_SECRET_<NAME>`,
  `DAGRON_SECRETS_DIR`): the control plane never sees them.
- **Isolation** is the runner's host's: its `DAGRON_TASK_ISOLATION_FLOOR`,
  and a job its executor cannot hold to it is failed.
- **An identity** (OIDC): dagron-api mints the job's request credential as
  an engine does, and the runner gives it the job with where to use it,
  `$DAGRON_RUNNER_URL/oidc/token` (or `DAGRON_RUNNER_OIDC_REQUEST_URL`,
  where the job's network reaches dagron-api by another name). A
  dagron-api that is no issuer fails the job saying so. See
  [A job's identity](#a-jobs-identity).
- **Its run's artifacts** (`artifact_api: true`): dagron-api mints the job's
  credential as an engine does, and the runner gives it the job as
  `DAGRON_ARTIFACTS_TOKEN`, with `$DAGRON_RUNNER_URL/task/v1/artifacts` (or
  `DAGRON_RUNNER_ARTIFACTS_URL`) as `DAGRON_ARTIFACTS_API`. The credential is
  the attempt's own, good while it runs: it reads any artifact of the run,
  and writes the job's own task's alone.
- **Refused, and the job failed saying so**: a job of a run that is not
  trusted (a fork's pull request); one with a `forge_token`. A private
  repository's checkout is fetched with no credential, so it fails there.

### Its life

- **A session a process.** The runner heartbeats it every third of
  `DAGRON_SESSION_LEASE_SECS` (90 s on dagron-api), one statement for every
  job it holds; a job row is not written by it.
- **A runner that dies** (`kill -9`, a host lost, a network cut) stops
  heartbeating; once its session's lease has lapsed an engine's sweep takes
  its jobs back to `ready`, and another runner takes them. A word it says
  late is refused (409).
- **Cancel** reaches the job at the session's next heartbeat.
- **Drain**: SIGTERM, or `POST /api/runners/{id}/drain`. No new jobs; the
  running ones finish; the runner exits. Give a service manager or a pod as
  long to stop as the longest job (`TimeoutStopSec`,
  `terminationGracePeriodSeconds`).
- **Revoke**: `POST /api/runners/{id}/revoke`. Its credential is refused,
  its sessions end at once, and its jobs are taken back.
- **What is out there**: `GET /api/runners` lists each runner and its
  sessions; `GET /api/runners/queue` the ready and running jobs of each
  label, and the capacity that serves it.

### Sizing them

`dagron-runner scale` sizes Kubernetes Deployments of runners by their
label's ready and running jobs (`DAGRON_SCALE_TARGETS`,
`DAGRON_SCALE_MIN`/`_MAX`/`_PER_REPLICA`); the enterprise build sizes a
cloud's VM pools the same way. A runner scaled away drains.

## Fleet mode

A repository keeps GitHub Actions and its workflow files, and changes a
job's `runs-on` to one of the operator's labels; a GitLab project, a job's
`tags`. The reference is
[fleet mode](../crates/dagron-ci/README.md#fleet-mode); this is what an
operator does with it.

### Setting it up

1. **Write the label map**, a file dagron-ci reads at start
   (`CI_GITHUB_FLEET_FILE`):

   ```yaml
   labels:
     dagron:
       image: ghcr.io/actions/actions-runner:2.321.0
     dagron-large:
       image: ghcr.io/actions/actions-runner:2.321.0
       runner_class: large      # the engine's class the runner asks for
       pool: ci-large           # the engine's pool it waits in
       priority: 5
       timeout_secs: 7200       # how long a runner may be out
   ```

   A label takes `image`, `runner_class`, `pool`, `priority`,
   `timeout_secs` (six hours unless said), `with` (the labels a job may
   name beside it: `self-hosted`, `linux`, `x64` unless said) and
   `command`; where its runners are registered (`scope`, `runner_group`);
   how many may be out (`max_runners`, `max_per_repository`); what a
   fork's job is given (`untrusted`: `refuse` unless said, `floor` or
   `serve`, and `untrusted_runner_class`); a Docker daemon beside the
   runner (`docker`); runners kept ready (`warm`, `warm_for_secs`); and
   whether a job whose runner died is run again (`retry`). Any other key is
   refused. So is a label GitHub gives every self-hosted runner, such as
   `linux`. There is no key for a runner's size: that is the runner
   class's, and the engine's settings for task pods.
2. **Give the App** the repository permissions *Administration: Read and
   write* and *Actions: Read* (*Read and write* to run a job again whose
   runner died), and, for an organization's runners, the organization
   permission *Self-hosted runners: Read and write*; and send it the
   *Workflow job* event. dagron-ci hears of jobs by delivery, so the
   webhook has to reach it.
3. **Have a cell that keeps secrets**: `CI_CELLS`, `CI_API_TOKEN_FILE`,
   and `DAGRON_ENV_SECRET_KEY` on that cell's dagron-api. A runner's
   configuration is kept there as a secret while the runner is out
   (`CI_FLEET_ENVIRONMENT`).
4. **For `untrusted: floor`**, have the cell record what started a run
   from dagron-ci's token (its user in the `ci:trigger` group), and its
   engines hold a run that is not trusted to a floor
   (`DAGRON_UNTRUSTED_ISOLATION_FLOOR`). A fork's job's runner keeps its
   configuration in `CI_FLEET_UNTRUSTED_ENVIRONMENT`, which dagron-ci makes
   and gives a policy where it may; an admin's token, or an admin's
   `PUT /api/environments/{id}/policy` with
   `{"allowed_trust": ["untrusted"]}`, does it otherwise.
5. **Connect the repository.**

**For GitLab** the file is `CI_GITLAB_FLEET_FILE`, of the same keys (no
`runner_group`, and `with` none unless said), and a label's runner is
GitLab's own (`gitlab-runner run-single`). It needs a token of
`CI_GITLAB_TOKENS_DIR`'s for the host, with `api` (or `read_api`,
`create_runner` and `manage_runner`), of an Owner of each project (of the
group, for a group's runners: GitLab makes a runner through its API for
an Owner alone), and the project's webhook
sending *Job events*. On the Docker executor, a label of the
`gitlab/gitlab-runner` image says `command: [run-single, --max-builds,
"1"]`: that image's entrypoint is `gitlab-runner` itself.

dagron-ci does not start with a fleet file and no cell, no token for it,
or no App. With no webhook secret it does start, and says as it starts
that it takes no delivery: a replica without the secret still starts the
runners for the jobs another replica took, and where no replica has the
secret the fleet hears of no job.

**On the chart** the map is a value, `ci.fleet.labels`, with the same
keys:

```yaml
ci:
  fleet:
    labels:
      dagron:
        image: ghcr.io/actions/actions-runner:2.321.0
```

The chart writes it to a ConfigMap, mounts that in dagron-ci's container
and names the file to it. dagron-ci reads the map once, as it starts, so
the pod is annotated with the map's checksum and a map that changed
starts it again: the engine's pod, where dagron-ci shares it. The render
fails on what dagron-ci would not start on (no cell and token, no App, a
map it would refuse), and with no `ci.webhookSecret` too, since a
release's replicas are all set alike. A file of your own, named by
`CI_GITHUB_FLEET_FILE` in `ci.extraEnv`, is still read where the image
has one; the chart mounts none but its own, and none of GitLab's.
`ci.fleet.prepull.enabled` pulls each label's image onto each node a
runner may start on (its `nodeSelector` and `tolerations`), with an image
of `sh` in it, before any runner does.

### A runner's life

- **A job that asks for a label is written down**, and dagron-ci looks at
  the repository every five seconds while a job waits or a runner is out.
- **It keeps as many runners out as jobs wait**, for each label, its
  `warm` ones besides, and starts at most eight for a repository at a
  look, as far as the label's quota lets it. A runner is not one job's:
  GitHub gives it whichever waiting job asks for its labels.
- **A fork's job** is told apart by its workflow run's head, asked of
  GitHub once a run, and is given what its label says: no runner, a floor
  runner, or one as any job's.
- **A runner is registered just in time**, for the one repository or its
  organization, and started as a run of one task in the label's image. It
  takes one job and exits, and GitHub forgets it.
- **When its job ends** its registration is removed where GitHub still has
  one, and its secret is deleted. Its run is asked to stop only if it is
  still running thirty seconds on.
- **A spare runner**, one out with no job waiting for it, is taken back
  after five seconds: its registration first, so that GitHub can give it
  no job, and then its run.
- **One that could not be started** says why on its row, its job waits,
  and none other of its label and trust is started for a minute.
- **A runner is out no longer than its label's `timeout_secs`**, and one
  kept warm is started anew after half an hour, or a quarter of that time.
- **A job whose runner died** (its run failed, and the job with it) is run
  again through GitHub's API, once, when its workflow run is over.
- **A label keeps runners warm** only where it says `untrusted: serve`, or
  names a `runner_group` restricted to selected workflows: a warm runner
  is an idle trusted one, which GitHub would give a fork's job.

### When it goes wrong

`/metrics` has the jobs that wait and the runners out, by label
(`dagron_ci_fleet_jobs_waiting`, `dagron_ci_fleet_runners`), and
`dagron-ci fleet usage` what the runners did. The rest of its state is two
tables of dagron-ci's, `ci_fleet_jobs` (a job's `trust` and `retry`, with
`retry_note`) and `ci_fleet_runners` (a runner's `state` and `reason`), and
lines of dagron-ci's log that begin `fleet:`.

| What is seen | Why | What to do |
|---|---|---|
| A job waits, and no runner comes | the job names a label of the file beside one it does not go `with`, or two labels of the file | make the job's `runs-on` the label, and what goes with it |
| `GitHub will not mint an installation token for this repository's runners` | the App lacks *Administration: Read and write*, or is not installed there | give it, and have the installation accept it |
| `fleet: a runner could not be started; its job waits` | the cell refused the run, or keeps no secret | read the runner's `reason`; set `DAGRON_ENV_SECRET_KEY`; look at the engine's limits on runs |
| A job waits, and no runner comes, and its row's `trust` is `untrusted` or empty | it is a fork's job, and its label refuses one; or GitHub could not be asked (the App lacks *Actions: Read*) | say `untrusted: floor` or `serve`, or give the App the permission |
| A job waits, and `fleet_jobs_waiting` holds while `fleet_runners` stays at the label's `max_runners` | the label's quota is full | raise it, or wait |
| `the environment "dagron-ci-fleet-untrusted" … admits no run that is not trusted` | an admin gave it a policy, or dagron-ci's token may not write one | give it `{"allowed_trust": ["untrusted"]}` |
| A runner is out with no job | GitHub never delivered its job's end | GitHub is asked of the job a minute on, and the runner is taken back when GitHub says it is over |
| A runner's reason says GitHub gave it a fork's job | a fork's job was taken by a runner started as a trusted job's, and was stopped | restrict the label's organization runner group to selected workflows, or have forks' workflows approved |
| A job's runner died, and the job was not run again | its row's `retry_note` says why: its label's time was out, its workflow run went on for an hour, a job of its name was run again once already, or the App lacks *Actions: Read and write* | re-run it on GitHub |

## What to watch

**dagron-ci** serves `/metrics`
([the series](../crates/dagron-ci/README.md#metrics)):

- `dagron_ci_events_pending` and
  `dagron_ci_events_oldest_pending_age_seconds`: events not yet turned
  into runs. An age that grows is workers that cannot keep up, or a cell
  that does not answer.
- `dagron_ci_events_finished_total{outcome}`: `failed` is an event given
  up on, `retried` one tried again, `waited` one that found its key held.
- `dagron_ci_webhook_deliveries_total{outcome}`: `unauthorized` is a
  wrong webhook secret, `unknown_repo` a repository nobody connected.
- `dagron_ci_commit_statuses_total{outcome}`: `refused` is a status GitHub
  will not take, `rate_limited` one waiting on GitHub's limit.
- `dagron_ci_checkout_tokens_total{outcome}`: `refused` and `failed` are
  checkouts that will fail.
- `/readyz` answers when the database does.

**The engine** serves its own ([`METRICS.md`](METRICS.md)): what waits for
a worker (`scheduler_queue_depth`, and by class
`scheduler_ready_oldest_age_seconds`, where a class no engine serves only
ever grows), what waits on a quota (`scheduler_tasks_waiting_on_quota`),
and how tasks end. Nothing there is labelled by a job's name, and a job's
own use of memory and CPU is the cluster's exporters' to measure.

From a terminal, `dagron-ci events` lists the newest events with why each
ended as it did, and `dagron-ci repo list` shows a repository the sweep
cannot read.

## When something is wrong

The quickstart has [the first table](CI_QUICKSTART.md#when-nothing-happens)
to read: no status on a commit, a token GitHub will not mint, a job that
fails at its checkout. Beside it:

| What is seen | Why | What to do |
|---|---|---|
| A job fails before its first step, at its checkout | the engine has no `DAGRON_CHECKOUT_IMAGE`; or the repository is private and the App is not installed on it; or, on the chart, dagron-ci has a Deployment of its own | set the image; install the App; keep dagron-ci in the engine's pod |
| A job's steps run, and its report or summary is not there | the job has no step runner, and a shell ran its steps | set `DAGRON_CI_EXEC_IMAGE` |
| A job is stopped sooner than its `timeout_secs` | the engine's ceiling on every task's time, or a deadline on its pods | `DAGRON_MAX_TASK_TIMEOUT_SECS`, `DAGRON_TASK_ACTIVE_DEADLINE_SECS` |
| Jobs sit ready | no engine serves their `runner_class`, their pool is full, or the namespace's quota is | `RUNNER_CLASSES`, `POOLS`; raise the quota or lower `resources` |
| A status stays `pending` | its job was skipped, and a skipped job waits for its run; or its event waits behind a concurrency key | read the run; `dagron-ci events` |
| No status at all | no App and no token, or GitHub refused: why is on the run's row (`ci_runs.report_error`) | give dagron-ci the App, or a token that may write statuses |
| A job ran twice | an engine stopped while it held the job, and another took it over | make a job safe to run twice: delivery is at least once |
| A job's log has a hole, or stops | it reached its cap (`DAGRON_LOG_TASK_CAP_BYTES`), or, where the job has secrets, what it wrote did, by 64 KiB, before it was masked (`… counted before masking, passed …: what the task writes from here is held back, …`): it is held there while the job runs, and given its end once the job is over, under a line that says how much was left out (`[dagron] … of this attempt's output (… bytes and … lines, counted before masking) is left out here; what follows is its end`), unless the job was cancelled, whose log ends where it was held; or it was not followed to its end (a follow that was cut, or on Docker a container that could not be attached to), and what the runtime kept of the rest had lost lines | the cap and `DAGRON_LOG_TAIL_BYTES`, for more of its beginning and its end; on Docker, `DAGRON_DOCKER_LOG_DRIVER=json-file`; on Kubernetes, the kubelet's `containerLogMaxSize` |

## Restarts, drains and upgrades

- **dagron-ci may stop at any time.** An event it held goes back when its
  lease lapses, and is taken again, which counts as an attempt. A run is
  not submitted twice: each submit carries a key made of the commit and
  the pipeline. Runs already submitted go on in the engine, and their
  statuses are posted when dagron-ci is back.
- **More than one dagron-ci may run** on one Postgres: they share the
  events, the reporter's work, the sweep and the fleet through claims,
  each held under a lease.
- **An engine may stop too.** Another takes its tasks over once their
  leases lapse, and a task it held may run a second time. To empty one
  first, `DAGRON_PRESSURE_FILE` has it claim nothing new while what it
  holds finishes, and `DAGRON_ADMISSION_FILE` refuses new runs.
- **What a kill does, shown.** The load harness's durability demo
  (`loadtest/ci/run.sh durable`, [its README](../loadtest/ci/README.md))
  kills an engine, every engine, dagron-ci or dagron-api with SIGKILL
  during pipelines, starts it again, and checks what was written down.
  On one machine every pipeline finished and every job's last status was
  posted once; a job its engine held when it died ran again from its
  start once the lease lapsed, 24 to 30 s after the kill at the default
  lease; and nothing a forge sent twice made a second run. Its jobs run
  on the local executor, and die with their engine. On Docker a job's
  container outlives its engine: a test that kills the engine under
  one shows the attempt before's container stopped before the next
  starts, so never two run at once. A dagron-ci killed while it looks at
  a run leaves that run claimed for its reporter's lease, 60 s: the
  commit is told late, and the pending statuses meanwhile are not
  posted, though its last ones are.
- **dagron-api away**: events are retried, later and later, up to
  `CI_EVENT_MAX_ATTEMPTS`; statuses are looked at again every half
  minute; the run, rerun and cancel doors answer 503.
- **A cancel asks.** A job that is running is told to stop, has its grace,
  and is removed with what it started: a container with its volumes, a
  pod with its Secret. Its run reads `running` until that is done.
- **Upgrade the engines before dagron-api, or with it**
  ([`UPGRADING.md`](UPGRADING.md)). dagron-ci applies its own migrations,
  in its own schema, when it starts.
- **Settings are read once.** A rotated secret needs a restart. On the
  chart's default placement, a change to dagron-ci's settings restarts the
  engine's pod, which holds both.

## On Kubernetes

- **dagron-ci is a container in the engine's pod** unless
  `ci.placement=deployment`. It then has the engine's replicas and its
  node placement, and the pod is ready when both are. With a Deployment
  of its own it checks out public repositories alone.
- **One path of dagron-ci's is on the ingress**: `/hooks/github`. Its run
  API and its checkout door stay inside the cluster.
- **A job's pod** is made in the release's namespace, beside the engine's.
  It is given no service account's token unless it asked and the operator
  allows that account. What it may be asked to hold is the engine's
  `DAGRON_TASK_*` settings and its isolation floor
  ([`CONFIG.md`](CONFIG.md), [`HARDENING.md`](HARDENING.md)).
- **The engine watches its task pods** with one watch
  (`DAGRON_TASK_POD_WATCH`), and falls back to asking for each where the
  watch goes quiet.
- **A floor of `network=none`** refuses every job with a checkout, which
  is every job of a pipeline: a floor for CI names an allowlist that
  reaches the forge.
- **`services:`** need Kubernetes 1.29 or later.

## Caches

A job's `workspace.cache` ([`CI_PIPELINES.md`](CI_PIPELINES.md)) is kept in
the engine's cache store: an object store that signs URLs.

| The engine's setting | Unset | |
|---|---|---|
| `DAGRON_CACHE_URL` | every job starts cold | `s3://bucket/prefix`, `gs://bucket/prefix` or `az://container/prefix`, with the store's credentials in the standard `AWS_*`, `GOOGLE_*` or `AZURE_*` variables; an S3-compatible store by `AWS_ENDPOINT_URL`. The published engine images have all three |
| `DAGRON_CACHE_QUOTA_BYTES` | 10 GiB | what one repository's entries hold together; past it, the least recently read go |
| `DAGRON_CACHE_UNUSED_DAYS` | 7 | an entry not read for this long goes |
| `DAGRON_CACHE_MAX_ENTRY_BYTES` | 4 GiB | an entry larger than this is not saved |
| `DAGRON_CACHE_URL_SECS` | 6 hours | how long a URL is signed for, for a job with no timeout; a job with one is given its timeout and 15 minutes |
| `DAGRON_CACHE_SWEEP_SECS` | 30 | how often the engine makes saved entries ready, and evicts |

- **The engine moves no cache's bytes.** As it starts a job with caches, it
  signs URLs for it: to read the newest entries of its repository and ref,
  and of the branch a pull request merges into; and, for a trusted run
  alone, to write one more. A store that cannot sign a URL (a local
  directory) is refused when the engine starts.
- **The job reaches the store itself**, from its own container, with
  `dagron-ci-cache`, which the tools image (`DAGRON_CI_EXEC_IMAGE`) installs
  beside the step runner. A job's network must reach the store's host: on
  Kubernetes, a job with `network: none`, or an allowlist without the store,
  starts cold. TLS is checked against the public roots the tool carries: a
  store behind a private certificate authority is not reached yet.
- **An entry is its repository's own**: scoped by the forge host, the
  repository and the ref of the run's record, never by what a pipeline says.
  A run with no repository in its record (a run of dagron's own) has no
  cache.
- **The index is in the engine's database** (`path_caches`). The sweep makes
  an entry ready once its job has succeeded and its manifest is in the store;
  drops what a job that did not succeed left; and evicts, objects and rows.
  It runs on every engine at once safely.
- The URLs are in the job's environment (`DAGRON_CACHE`), as the rest of its
  variables are, for as long as they are signed for. Each names one object.

### A workflow's `actions/cache`

A workflow job's `actions/cache` (v4.2 or later), and a setup action's
`cache:`, which uses it, speak GitHub's cache service. The engine is one,
over the same store and index, where the operator turns it on.

| The engine's setting | Unset | |
|---|---|---|
| `DAGRON_ACTIONS_CACHE_LISTEN` | no service: every `actions/cache` misses, and saves nothing | the address the service listens on, `0.0.0.0:8090` |
| `DAGRON_ACTIONS_CACHE_URL` | — | the service's URL as a job's network reaches this engine, over `https://` |
| `DAGRON_ACTIONS_CACHE_INSECURE` | the URL is `https://` | `allow` lets it be plain `http://`, on a closed network where a job's cache credential may cross in the clear |
| `DAGRON_ACTIONS_CACHE_DIR` | the temporary directory's `dagron-actions-cache` | where uploads wait until they are finalized: room for the largest entry, times the saves at once |

- **It needs the cache store** (`DAGRON_CACHE_URL`): the engine does not
  start with the one and not the other. Entries are path caches (prefix
  `actions`), with their quota, eviction and scopes: a job restores from
  its own ref, then from the branch a pull request merges into; one that
  is not trusted saves nothing.
- **A job is told where it is** as the engine starts it: a job with a
  workspace, of a run with a repository, is given `ACTIONS_RESULTS_URL`,
  `ACTIONS_CACHE_SERVICE_V2` and `ACTIONS_RUNTIME_TOKEN`, a credential of
  its own for as long as it may run, masked in the log.
- **A restore reads the store directly**, at a URL the engine signs. **A
  save is uploaded to the engine**, which the action's upload needs: the
  engine keeps it in `DAGRON_ACTIONS_CACHE_DIR` and puts it in the store as
  the action finalizes it.
- **Each engine has its own URL.** An upload is kept by the engine it was
  made to, and the credential is that engine's, signed with a key it makes
  as it starts. A job that outlives its engine's restart finds the service
  refuses it: its `actions/cache` warns, and the job goes on.
- **The URL is a host's root**: `https://cache.example`, with no path. The
  artifact actions keep only its origin.

### A workflow's Docker actions

The same service runs a workflow's Docker actions, and its steps that use
an image (`uses: docker://image`): the step runner asks for each, with the
job's credential, and the engine's executor starts it beside the job.

- **On the Docker executor**, a container of the image, on the job's
  network (the first service's, where the job has services), with the
  job's workspace at `/workspace` and `/github/workspace`, as the job's
  user, held to what the job is held to (its limits and hardening, a run
  that is not trusted to the images `DAGRON_UNTRUSTED_IMAGES` lists), and
  labelled as the job is: the sweep finds one left over as it finds the
  job's. It is removed when it ends, and every one still there is removed
  when the job ends.
- **On the Kubernetes executor**, an ephemeral container added to the
  job's own pod, with the pod's workspace volume at `/workspace` and
  `/github/workspace`, the job's container's security context and image
  pull policy, and the pod's network, security context and NetworkPolicy.
  It goes with the pod. The engine needs `get` and `patch` on
  `pods/ephemeralcontainers`, which the chart's Role grants.
  - Kubernetes holds an ephemeral container to no limit, and cannot stop
    one alone: a run that is not trusted runs none, and its step fails,
    saying so; one past its time is said so, and runs on until the job
    ends, which ends it.
  - The job has ended when its own container has, whatever of its steps'
    is still there.
- **On the local executor**, such a step fails, and says so.
- **An action whose image is a Dockerfile** has it built on the Docker
  executor's daemon first, for a trusted run, where the operator says so
  (`DAGRON_DOCKER_BUILD_ACTIONS=allow`): the step runner sends the
  Dockerfile's directory to the same service (`POST
  /dagron/v1/steps/images`, at most 64 MiB), and the image, named
  `dagron-ci-action:<digest>` for what it was built from, is kept on the
  host for the next step that uses it, labelled `dagron.ci.step-image`.
  The engine reuses one it built itself, by the image's id, and builds
  over one another put under the name; a restarted engine builds each
  once again. Remove those when the host needs the room; one that is gone
  is built again. Unset, or on another executor, such a step fails, and
  says so.
- A job whose workspace is the operator's (`DAGRON_DOCKER_WORKDIR`, or a
  mount at `/workspace`) has none for such a container to share, and such
  a step fails.
- An image the host lacks is pulled as the job's image is, under the job's
  `image_pull_policy`.

### A job's Docker daemon

A workflow's job that runs `docker build` has a daemon on a hosted runner
and none here, unless three say so: the repository (`github.docker: true`
in `.dagron/ci.yaml`), dagron-ci's operator, who lists the repository, and
the engine's, who offers a daemon:

```sh
# dagron-ci: the repositories whose jobs may have one (owner/* or owner/repo)
CI_GITHUB_DOCKER=acme/widgets,acme-images/*
# the engine
DAGRON_DOCKER_DAEMON=dind
# DAGRON_DOCKER_DAEMON_IMAGE=docker.io/library/docker@sha256:…
```

Each job of a trusted run is then given a daemon of its own: a privileged
container of that image beside it (unprivileged on Sysbox, below), which it
reaches as `DOCKER_HOST`, with
the job's workspace at the job's own path, removed when the job ends with
whatever it built.

- **A daemon's container is root on the engine's host**, and can read what
  the host's other containers hold. List only the repositories whose
  writers you would give that, and run such jobs on engines of their own
  (a runner class) where you can. An engine that offers a daemon does not
  start where it also runs work that is not trusted.
- **Or run it unprivileged, on Sysbox.** Install
  [Sysbox](https://github.com/nestybox/sysbox) on the engine's host and set
  `DAGRON_DOCKER_DAEMON_RUNTIME=sysbox-runc`: each daemon then runs in a
  user namespace of its own, without `--privileged`, and its root is no
  user of the host's. The engine checks at start that the host's Docker
  has the runtime. Sysbox needs Docker (not Podman) on a Linux host with a
  recent kernel; see its documentation for which.
- A repository not listed asks for nothing: its jobs run as before, and a
  `docker` step fails for want of a daemon. One listed, on an engine that
  offers none, has each job failed with the reason.
- A job with a service of its own named `docker` (a `docker:dind` of the
  workflow's) keeps that one and is given none. One with eight services
  has no room for it, and the workflow is refused, naming the job.
- On the Docker executor only: an engine that sets this with another
  executor does not start. A run that is not trusted is given none.
- A pipeline of `.dagron/ci` asks for one with the task key
  `docker_daemon: true`, which `CI_GITHUB_DOCKER` does not cover: on an
  engine that offers a daemon, any trusted pipeline may.

### A workflow's artifacts

The same service is GitHub's artifact service, which
`actions/upload-artifact` and `actions/download-artifact` v4 speak, and
which a GitLab job's `artifacts:` go through (the step runner packs and
fetches them, around the job's scripts). It is on wherever the cache
service is.

| The engine's setting | Unset | |
|---|---|---|
| `DAGRON_ACTIONS_ARTIFACT_RETENTION_DAYS` | 90 | the longest an artifact is kept; a job's `retention-days` may ask for less |
| `DAGRON_ACTIONS_ARTIFACT_MAX_BYTES` | 5 GiB | the largest an artifact may be |

- **An artifact is its run's, by its name.** Any job of the run lists and
  downloads it. No job of another run sees it, and a run that is not
  trusted uploads to its own run alone, as a fork's workflow does on
  GitHub. A name already in the run is refused; `overwrite: true` deletes
  the old one first.
- **The bytes are in the cache store** (`DAGRON_CACHE_URL`), under
  `workflow-artifacts/<run>/`. An upload goes to the engine, as a cache's
  does, and is put in the store with its SHA-256, which must be the one the
  job gives. A download reads the store directly, at a URL the engine signs,
  and checks the SHA-256.
- **The index is in the engine's database** (`workflow_artifacts`). Each
  engine deletes what has expired, with its bytes, every hour; an upload
  never finished goes a day after it began.
- **`actions/upload-artifact/merge`** merges a run's artifacts into one,
  and deletes them where it is asked: every call it makes is the
  service's.
- **Another run's artifacts** (`download-artifact`'s `run-id` and
  `github-token`) are asked of GitHub's REST API, at the job's
  `GITHUB_API_URL`. Where the operator names one (`CI_GITHUB_API_FOR_JOBS`,
  a URL jobs reach dagron-ci at, ending in `/ci/v1/github-api`), that is
  dagron-ci's door, which answers GitHub's two calls itself for a run of
  dagron's: the run's artifacts listed as GitHub lists them, and a
  redirect to a link of its own, good for five minutes, that streams one's
  zip from the run's cell. It answers them for a token dagron-ci gave a job
  of the same repository, in a trusted run, and not yet revoked (the job's
  `GITHUB_TOKEN`), and for no other; a run that is not dagron's is
  GitHub's. Every other call is passed on to GitHub's API
  (`CI_GITHUB_API_URL`) as it came, credential and all, and one that
  carries no credential is refused. Unset, a job reaches GitHub's own API,
  where a dagron run's id names nothing.
- **In the console**, a run's page lists its workflow artifacts (name,
  size, when it expires) and downloads each one's zip, which dagron-api
  streams from the same store (`GET /api/runs/{run_id}/workflow-artifacts`
  and `…/{name}`, [API.md](API.md)). dagron-api is given the store as the
  engine is (`DAGRON_CACHE_URL` and the store's own variables). The
  console's other artifacts are dagron's own (`DAGRON_ARTIFACT_DIR`).

#### Signed provenance

Where the engine is given a key, each artifact is made its run's with its
[SLSA v1 provenance](https://slsa.dev/spec/v1.0/provenance), signed: what
made the zip, said by the engine and not by the job.

| The engine's setting | Unset | |
|---|---|---|
| `DAGRON_PROVENANCE_KEY` | artifacts are kept without provenance | an ECDSA P-256 private key, PEM, PKCS#8 |
| `DAGRON_PROVENANCE_SIGN_CMD` + `DAGRON_PROVENANCE_PUBLIC_KEY` | — | enterprise, in place of the key: a KMS key signs, through a command given the SHA-256 to sign |
| `DAGRON_PROVENANCE_BUILDER_ID` | `https://dagron.dev/builders/dagron-engine` | the `builder.id` a verifier's policy names |

- **What it says**: the zip's SHA-256 (the subject, `<name>.zip`); the
  repository, ref and commit the run was started for, its trigger and
  trust class (from the run's record); the workflow, the job and its
  attempt; the builder and the engine's version. The commit is the one
  resolved dependency.
- **Its shape is cosign's**: an in-toto Statement v1 in a DSSE envelope,
  ECDSA P-256 over SHA-256, so a build output is checked with the tool
  anyone checking one has:

  ```sh
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out provenance.pem
  openssl pkey -pubout -in provenance.pem -out provenance.pub   # publish this
  # the engine: DAGRON_PROVENANCE_KEY=/etc/dagron/provenance.pem

  curl -fsSo dist.zip "$API/api/runs/$RUN/workflow-artifacts/dist"
  curl -fsSo dist.intoto.json "$API/api/runs/$RUN/workflow-artifacts/dist/provenance"
  cosign verify-blob-attestation --key provenance.pub --type slsaprovenance1 \
    --signature dist.intoto.json --insecure-ignore-tlog dist.zip
  ```

  `--insecure-ignore-tlog`, because nothing is written to Sigstore's public
  log: an installation that cannot reach it signs all the same.
- **A KMS key** (enterprise): the command is given the SHA-256 of what is
  signed, base64, on stdin, and writes the DER signature, base64, on
  stdout. With AWS KMS (an `ECC_NIST_P256` key):

  ```sh
  #!/bin/sh
  d=$(mktemp) && base64 -d > "$d" && aws kms sign --key-id alias/dagron-provenance \
    --message "fileb://$d" --message-type DIGEST --signing-algorithm ECDSA_SHA_256 \
    --query Signature --output text; s=$?; rm -f "$d"; exit $s
  ```

  `DAGRON_PROVENANCE_PUBLIC_KEY` is the key's public half (`aws kms
  get-public-key`, as PEM); each signature is checked against it before it
  is kept, so a command that signs with another key fails at the engine,
  not at a verifier.
- **Signed or not kept**: an artifact whose provenance could not be signed
  (the KMS down, say) is refused, and the job's upload fails saying so. A
  key that cannot be read stops the engine as it starts.
- **Read it** at `GET /api/runs/{run_id}/workflow-artifacts/{name}/provenance`;
  a listing says `provenance: true` of an artifact that has one. An
  artifact finalized by an engine given no key has none.
- **What it does not prove**: that the build is reproducible, or that the
  job's steps were what its repository says. The key vouches for the
  engine, and the engine for what it ran.

## A job's identity

A job's `identity:` ([`CI_SECURITY.md`](CI_SECURITY.md#a-jobs-identity))
needs an issuer, which dagron-api is, and the engine told where jobs reach
it.

| Setting | Of | Unset | |
|---|---|---|---|
| `DAGRON_OIDC_ISSUER` | dagron-api | no issuer: a job that asks for an identity is refused | the issuer's `https://` URL, which a cloud fetches `/.well-known/openid-configuration` and `/.well-known/jwks` from, at that URL's path. The cloud must reach it |
| `DAGRON_OIDC_KEYS` | dagron-api | — | a PEM file of an RSA private key (2048 bits or more, PKCS#8 or PKCS#1), or a directory of them (`*.pem`) |
| `DAGRON_OIDC_SIGN_CMD` | dagron-api | the newest key is a private key | enterprise: the newest key is a KMS's, its public half the file that sorts last, and this command signs with it |
| `DAGRON_OIDC_TOKEN_SECS` | dagron-api | `600` | how long a token lasts, 60 to 3600 |
| `DAGRON_OIDC_REQUEST_URL` | engine | a job that asks for an identity is refused | where a job asks for a token: dagron-api's `/oidc/token`, as the job's network reaches it, over `https://` |
| `DAGRON_OIDC_REQUEST_INSECURE` | engine | the request URL is `https://` | `allow` lets the request URL be plain `http://`, on a closed network where a job's request credential may cross in the clear |

- **Make a key** with `openssl genpkey -algorithm RSA -pkeyopt
  rsa_keygen_bits:2048 -out 2026-10.pem`, and give every replica of
  dagron-api the same directory (a Secret, on Kubernetes).
- **Rotate** by adding a key whose file name sorts after the others: it
  signs from the next start, and the old key is still published, so tokens
  it signed verify until they expire. Remove the old key's file once they
  have (`DAGRON_OIDC_TOKEN_SECS` after every replica has restarted).
- **A KMS key** (enterprise): put its public half (`aws kms get-public-key`,
  as PEM) in the directory, sorting last, and set `DAGRON_OIDC_SIGN_CMD`.
  The command is given the SHA-256 of a token's signing input, base64, on
  stdin, and writes the RSASSA-PKCS1-v1_5 signature, base64, on stdout
  (`aws kms sign --message-type DIGEST --signing-algorithm
  RSASSA_PKCS1_V1_5_SHA_256`, with an `RSA_2048` key or larger). Each
  signature is checked against the published key before a token is given.
  Rotating onto the KMS is the same as onto a file: the private keys of
  the files before it are still published until their tokens expire.
- **A remote runner's job** is given its request credential by dagron-api,
  which mints it as the engine does, and the runner's URL for
  `/oidc/token` (`DAGRON_RUNNER_OIDC_REQUEST_URL`, else
  `$DAGRON_RUNNER_URL/oidc/token`).
- **A workflow's jobs** that `permissions` give `id-token: write` are given
  GitHub's `ACTIONS_ID_TOKEN_REQUEST_URL` and `_TOKEN` for the same
  credential, where the operator says (`CI_GITHUB_OIDC`): see dagron-ci.
- **A job reaches `/oidc/token`** with no session: the request credential is
  what lets it in. Give jobs that route of dagron-api and no other, as an
  ingress path or a door of its own; a job that is not trusted, which is
  given no credential, can then reach nothing of the API.
- dagron-api logs each token it gives (the run, the job, the audience),
  never the token.

## What is not here

- **Nodes for the engines.** A cluster's own autoscaler is the cluster's;
  remote runners are sized by `dagron-runner scale`.
- **An artifact store of CI's own.** A cache store behind a private
  certificate authority; GitHub's own cache service for a workflow's jobs
  (`actions/cache` as an action, which `dagron-import` rewrites as a
  `workspace.cache`).
- **Credentials for an image** on Docker.
- **A metric, a command or a route for the fleet's runners**; runners
  registered for an organization; a runner kept warm; a fleet job run
  again because its runner died; Docker inside a fleet job; a size for a
  runner by label.
- **A following log in the console**, which polls a job's log as it grows.
  dagron-api's stream and `dagron-ci logs --follow` follow it as it is
  written.
- **A procedure that drains a node of CI jobs.** The pieces above are
  there, and nothing joins them.
- **Dropping old rows** of dagron-ci's own.
- **Sizing advice.** No figure is claimed, so none is given.
