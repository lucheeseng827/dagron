# Deferred external jobs (`defer:`)

> Hand a task's work to a system dagron does not own — Spark, a warehouse, any
> submit-then-poll API — and hold no worker while it runs.
> Companion to [`CONFIG.md`](CONFIG.md) (env reference) and
> [`AI_WORKLOADS.md`](AI_WORKLOADS.md) (long and checkpointed tasks that run
> *here* rather than elsewhere).

## The problem

A task that runs a six-hour Spark job by blocking on `spark-submit` holds a
worker slot for six hours. That is affordable once and ruinous at a few hundred,
and it puts the job's fate in one process: kill the engine and the job is
orphaned, while the row is reclaimed by lease recovery and **submitted again**.

`defer:` splits the two things that were conflated. The task's `command` is the
*submit*, and nothing else. The job is a row.

## How it works

```yaml
tasks:
  - name: nightly_rollup
    command: ["dagron-step-spark", "submit", "--app", "s3://jobs/rollup.py"]
    timeout_secs: 300          # bounds the SUBMIT, not the job
    max_attempts: 3
    pool: spark                # at most N concurrent REMOTE JOBS — see below
    env:
      - { name: DATABRICKS_TOKEN, value_from: { secret: DATABRICKS_TOKEN } }
    defer:
      kind: databricks
      poll_secs: 30
      max_wait_secs: 21600     # 6h ceiling on the remote job itself
      http:
        url: "https://dbc.example.com/api/2.1/jobs/runs/get?run_id={{ handle }}"
        headers:
          - { name: Authorization, value_from: { secret: DATABRICKS_TOKEN } }
        succeed_when: "state.result_state == SUCCESS"
        fail_when:    "state.result_state in [FAILED, TIMEDOUT, CANCELED]"
        error_from:   "state.state_message"
```

1. The command runs as any command task does — lease, retries, `timeout_secs`,
   fault classification.
2. On success it prints its remote job's identity:

   ```
   dagron::handle=spark-rollup-20260914
   ```

   The **last** such line wins, so a step may log freely before it.
3. The engine parks the row: claim dropped, lease NULLed, `status` still
   `running`, the handle on the row. No worker is held.
4. A reconcile sweep polls it every `poll_secs` until it resolves.

A submit that exits 0 but prints no handle **fails**. Succeeding it would
advance dependents on work that has not happened.

### Three timeouts, three jobs

| Setting | Bounds |
|---|---|
| `timeout_secs` | the submit command |
| `defer.max_wait_secs` | the remote job |
| `run_timeout_secs` | the whole run |

Conflating the first two is the mistake the split exists to prevent: the
executor's default `timeout_secs` is 25 seconds, which is right for a submit and
absurd for a Spark run.

## Why it survives a crash

`recover_expired_leases` reclaims rows `WHERE lease_expires_at IS NOT NULL`. A
parked row has a NULL lease, so it is **provably outside** the set that sweep can
touch.

Kill every scheduler mid-job. The row is untouched. Any replica's next sweep
resumes the poll, because the handle is durable state rather than an in-memory
registration. Nothing resubmits, because nothing re-ran.

### Adoption, and the epoch

The engine injects two variables into a deferred task:

| Variable | Meaning |
|---|---|
| `DAGRON_TASK_ID` | the task row's id — stable across lease recovery |
| `DAGRON_EXTERNAL_EPOCH` | submission generation |

A step should name its remote job `dagron-<DAGRON_TASK_ID>-<DAGRON_EXTERNAL_EPOCH>`.
That makes the one genuinely dangerous window safe: if the engine dies *after*
the remote system accepted the job but *before* the park committed, the retried
submit uses the **same name**, the remote system answers AlreadyExists, and the
step adopts the running job instead of starting a second one.

`attempt` cannot serve this purpose — it increments on every claim, including
lease recovery, so a name built from it changes at exactly the moment adoption is
needed.

The epoch is bumped only where a row that already submitted is deliberately
re-armed for a *fresh* job: a resolved remote failure, `rerun_from_failed`, and
clearing a task with its downstream. So a retry gets a new job; a recovery adopts
the old one.

## `pool:` means something different here

For a normal task, `pool:` rations worker slots and a parked task releases its
slot. For a deferred task it rations **concurrent remote jobs**, and a parked row
keeps its slot for the life of the job.

That asymmetry is deliberate. The other four park shapes wait on something free —
a timer, an endpoint, a dataset cursor, a child run. A deferred row waits on a job
burning someone's cluster the whole time. If it released its slot, `pool: spark`
with capacity 4 would cap *submissions*, which take seconds and bound nothing.

So `POOLS=spark:4` means at most four Spark jobs at once — a bound on how much
is burning *simultaneously*. Worth setting, and not a bound on the total; for
that, see the ceiling below.

`POOLS` is per-scheduler configuration, not datastore state, so keep it identical
across replicas and through a rolling update: a replica running without it
claims uncapped. (One over-admission was seen during a rollout — six jobs
against a cap of two — and this is the suspected cause, not a proven one; it did
not reproduce on a settled deployment.)

## `budget.external_cost` — a ceiling on the whole run

`pool:` caps what runs at once. It says nothing about how many jobs a run submits
over its life, and the two failures are different: one exhausts the cluster now,
the other arrives on the invoice.

```yaml
name: nightly-rollup
budget:
  external_cost: 40            # refuse the run if the declared total exceeds this
tasks:
  - name: rollup
    command: ["dagron-step-spark", "submit", "--app", "s3://jobs/rollup.py"]
    with_items: "{{ params.regions }}"
    defer:
      kind: spark-k8s
      cost: 4                  # what one submission of THIS task is worth
```

`defer.cost` defaults to `1`, so a spec that declares no unit costs gets a plain
count of submissions: `external_cost: 40` admits at most 40 deferred tasks. Give
one task a `cost:` and the ceiling becomes a weighted sum, so the 200-node Spark
job counts for more than the one-row query.

**The check is at run creation, not mid-run.** By then dagron has expanded
`with_items`, templates and gangs, so the sum is exact and a run that would break
its ceiling is refused before a single remote job is submitted — rather than
killed halfway through with cluster-hours already spent. The refusal names the
ceiling, the planned total and how many deferred rows produced it:

```
workflow 'nightly-rollup' would submit 12 deferred task(s) of declared cost 48,
over its budget.external_cost of 40. These are the costs the spec itself
declares (`defer.cost`, default 1) — raise the ceiling if you meant it, or
lower the fan-out
```

Over the API that is a **400**, alongside the `budget.tasks` refusal.

**It counts rows, not spec entries.** A `gang: { size: 8 }` deferred task is one
line in the YAML and eight rows in the database, and every one of them submits —
so it costs `8 × cost`. Budgeting the page would budget nothing.

### What the number means is entirely yours

The engine never learns what a cost unit is. It is not dollars, not GPU-hours,
not anything dagron can verify — it is a number you wrote down so the engine can
add it up. Pick a unit that makes the arithmetic mean something to you (node-hours
of the job you expect, whole dollars, "one unit per executor") and stay consistent
inside a workflow; nothing compares costs across workflows.

That is the honest shape of the feature, and it is why it can exist at all. The
sum is **exact** — it is arithmetic on numbers a human declared — and it never
claims to be a measurement. There is deliberately no `spend:` field capping what a
run *actually* costs, because nothing in this engine meters currency, and a field
that promised that would be a promise with no measurement behind it.

## `defer.http` — the built-in transport

One adapter, no vendor code in dagron. Every system worth polling answers "is it
done?" with a JSON document containing a state field, so the poller is a GET plus
a predicate — which means a vendor API change is a YAML edit by the person it
affects, on their schedule, rather than a dagron release on ours.

| Field | Meaning |
|---|---|
| `url` | status endpoint; `{{ handle }}` expands to the remote job id |
| `headers` | same shape as a task's `env:`, so `value_from: { secret: … }` works |
| `succeed_when` | predicate that means "finished, successfully" |
| `fail_when` | predicate that means "finished, badly" (optional — see below) |
| `error_from` | dotted path to the vendor's own error text |

### The predicate grammar

```text
path == VALUE          status.applicationState.state == COMPLETED
path != VALUE          state.life_cycle_state != RUNNING
path in [A, B]         state.result_state in [FAILED, TIMEDOUT]
```

Paths are dotted and walk objects and arrays alike (`items.0.state`). Values
quote when they contain a space or comma. Numbers and booleans compare as
written.

Not expressible: wildcards, filters, recursive descent, arithmetic, boolean
connectives. Each is a step toward a query language nobody asked this project to
maintain; a response that needs one needs a step binary, which is a seam that
already exists.

**Every field is parsed when the DAG is submitted.** A typo in `succeed_when` is
otherwise a workflow that validates cleanly, starts a six-hour job, and only then
discovers it cannot read the answer.

### Three rules worth knowing

- **A missing path is `false` for every form, `!=` included.** A vendor that has
  not written `state.result_state` yet is not a vendor reporting failure, so an
  absent path means *undecided* and the job keeps polling. The next poll costs
  seconds; a wrong verdict costs the job.
- **`fail_when` is checked first.** If your two predicates overlap, one is wrong
  — and the mistakes are not equally expensive. A false failure costs a retry; a
  false success advances every dependent on a job that produced nothing. The
  overlap is logged.
- **Omitting `fail_when` is legal and usually wrong.** Without it a failed job is
  not noticed as failed — it is only *bounded*, and only if something bounds it.
  With `max_wait_secs` set, the task fails that many seconds late with a timeout
  message instead of the vendor's reason. With `run_timeout_secs` set instead,
  the run's own deadline cancels it. **With neither set, nothing bounds it**:
  `max_wait_secs` defaults to none on purpose, so a failed or unresolvable job
  stays parked and polling indefinitely. Watch `scheduler_external_parked` for a
  row that is not moving, and prefer writing `fail_when`.

### Network policy — the opposite of `wait.url`

`WAIT_URL_DENY_PRIVATE` is **off** by default. That is right for an
unauthenticated readiness probe whose main use is `http://svc.default.svc/ready`.

It is wrong here, because a `defer.http` poll carries a **bearer token**. The
same permissiveness would let a workflow author aim an operator's credential at
any address the scheduler can reach, `169.254.169.254` included. So the default
inverts:

| Variable | Default | Meaning |
|---|---|---|
| `DEFER_HTTP_DENY_PRIVATE` | **on** | refuse hosts resolving to non-global addresses |
| `DEFER_HTTP_ALLOW_HOSTS` | unset | comma-separated hosts exempt from that — how you poll an in-cluster Spark operator |

The filter lives inside the resolver, so the addresses checked are the addresses
dialled and a DNS rebind has no window. Redirects are refused outright: a 3xx
target is invisible to the check.

**Polling the in-cluster Kubernetes API needs two engine settings, not one.** The
apiserver's certificate is signed by the cluster CA, which the engine's trust
store does not contain, so on top of the allowlist:

```yaml
env:
  - { name: DEFER_HTTP_ALLOW_HOSTS, value: kubernetes.default.svc }
  - { name: SSL_CERT_FILE,          value: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt }
```

Without `SSL_CERT_FILE` the poll fails TLS verification on every sweep and the
row stays parked, re-parking as "not a verdict" until `max_wait_secs`. (Verified
on kind: with both set, a `SparkApplication` resolved two seconds after Spark
finished.) `SSL_CERT_FILE` replaces the engine's trusted roots for every HTTPS
client in the process, so an engine that also polls a public vendor needs a
bundle holding both, or a second engine.

### What is not a verdict

A non-2xx, an unparseable body, a connection reset, a timeout — none of these say
anything about the job. All re-park and try again at `poll_secs`. Failing a
healthy six-hour job because its vendor answered 503 once is the outcome this
rule exists to prevent.

Whatever `error_from` extracts is truncated before it reaches the task's output,
and every credential the headers resolved is masked out of anything the poller
writes back — including a vendor error envelope that echoes the `Authorization`
header.

## Cancelling a run reaches the cluster

Cancelling a run used to be pure SQL: it flipped rows terminal and cleared
leases. Nothing reached the system actually running the work, so "we cancelled
your run" meant "we stopped watching your cluster bill".

Now a terminated task that still holds a handle **owes a teardown**, and a sweep
settles it by calling `ExternalPoller::cancel`.

The debt is row state rather than something the cancel stamps, and that is
deliberate: the cancel path most callers use is inlined SQL in `dagron-api`,
which holds no seams — a teardown the cancel *performed* would be one the SDK
and the MCP server never triggered. A cancel that merely leaves evidence is one
every caller performs for free.

The predicate is per task: holds an `external_handle`, is not parked. A job that
finished on its own owes nothing, because resolving or failing it already
cleared the handle — otherwise every completed job would get a redundant vendor
cancel.

### It is best-effort, and says so

A job we cannot reach is a job we cannot stop. So teardown gives up — after
three failed attempts, or an hour from when the task went terminal, whichever
comes first — and when it does:

- the handle is cleared, so the row stops sweeping;
- `scheduler_external_orphans_total` increments;
- a warning names the **kind and the handle**, so it can be stopped by hand.

That counter is the point. The alternative to a visible leak is a silent one.

**`gc_old_runs` will not collect a run that still owes a teardown**, for the same
reason: deleting it would drop the only record of a job running on someone's
cluster. The debt settles itself within the give-up window, so collection is
deferred by that and no longer.

An engine with no poller for a kind hands the row back **without** consuming an
attempt — a `RUNNER_CLASSES` pool runs the same binary with different seams, so
the replica that cannot do the work must not spend the budget of the one that
can. The wall-clock bound is what guarantees termination regardless of fleet
shape.

### `defer.http` cancels only if you say how

The poll is a GET: it can tell you a job finished, and nothing in it can stop
one. So a `defer.http` block **without** `cancel:` leaves the remote job running
when its run is cancelled or `max_wait_secs` elapses — counted
(`scheduler_external_orphans_total`) and logged by handle, but only after the
three-attempt / one-hour give-up above, and the job bills the whole time.

Add `cancel:` and the same sweep sends it. It reuses the block's `headers`, the
same guarded client and the same header redaction as the poll:

```yaml
defer:
  kind: spark-k8s
  http:
    url: "https://kubernetes.default.svc/apis/sparkoperator.k8s.io/v1beta2/namespaces/data/sparkapplications/{{ handle }}"
    headers: [{ name: Authorization, value_from: { secret: K8S_TOKEN } }]
    succeed_when: "status.applicationState.state == COMPLETED"
    cancel:                     # DELETE is the default method
      url: "https://kubernetes.default.svc/apis/sparkoperator.k8s.io/v1beta2/namespaces/data/sparkapplications/{{ handle }}"
```

```yaml
    cancel:                     # a vendor that cancels with a POST
      url: "https://dbc.example/api/2.1/jobs/runs/cancel"
      method: POST
      body: '{"run_id": {{ handle }}}'
```

| Field | Meaning |
|---|---|
| `url` | `http`/`https` only, checked at submit; `{{ handle }}` is the remote job id |
| `method` | `DELETE` (default), `POST`, `PUT` or `PATCH` |
| `body` | optional, sent as `application/json`; `{{ handle }}` expands here too |

A 2xx counts as torn down, and so do **404 and 410**: the job is already gone,
which is the outcome wanted, and retrying would burn the budget to report an
orphan that was never running. Any other status is a failed attempt, and three
of them orphan the job as above.

**With `cancel:` set, `max_wait_secs` stops the job too**: the task still fails
at the ceiling, but keeps its handle so the same sweep tears the job down, and
its message says so. Without `cancel:` it fails with "was NOT cancelled".

**The credential now needs write access.** A token that only polls needs `get`;
one that cancels needs `delete` too. On Kubernetes that is a Role granting
`get`/`delete` on `sparkapplications` to the ServiceAccount whose token the
`headers` carry — the read-only poll token from the quickstart is no longer
enough.

A registered `ExternalPoller` gets first refusal: if its `cancel` claims the
kind, the `cancel:` block is not sent. To stop remote work in a way a single
HTTP request cannot express, register one whose `cancel` issues the vendor's own
calls (`CancelJobRun`, `batches.delete`, a CR delete). The seam exists for
exactly this.

## The two step binaries

`defer:` needs something to do the submitting. Two ship with dagron.

### `dagron-step-spark`

```yaml
- name: rollup
  command: ["dagron-step-spark", "submit", "--app", "s3://jobs/rollup.py"]
  timeout_secs: 300
  env:
    - { name: SPARK_BACKEND,   value: k8s }
    - { name: SPARK_NAMESPACE, value: data }
  defer: { kind: spark-k8s, poll_secs: 30, http: { … } }
```

On `k8s` the driver pod runs as the operator's default ServiceAccount — the
namespace's `default`, which usually cannot create the executor pods the driver
asks for, so a job that needs executors dies with `Forbidden`. Set
`SPARK_DRIVER_SERVICE_ACCOUNT` to an account that can (`spark.driver.serviceAccount`
on the CR). **Do not reach for `SPARK_CONF_*`:** that mapping lowercases every
key and Spark's are case-sensitive, so `spark.kubernetes.authenticate.driver.serviceAccountName`
cannot be written as an environment variable — it goes out as `…serviceaccountname`
and is ignored.

| `SPARK_BACKEND` | What it does |
|---|---|
| `k8s` (the configuration default — **but only a `--features k8s` build links the client**, which the published image does; a source build with default features refuses it and names both `--features k8s` and `SPARK_BACKEND=rest`) | creates a `SparkApplication` CR. Vendor-free, no account, and **AlreadyExists is the adoption path** — idempotency from the API's own semantics rather than anything dagron invented |
| `rest` | POSTs a body you supply to a URL you supply, reading the job id out by dotted path (`SPARK_REST_URL`, `SPARK_REST_BODY`, `SPARK_REST_HANDLE_PATH`). One adapter for Databricks, EMR Serverless, Dataproc, Livy, Kyuubi — put `{{ name }}` where the vendor's idempotency token goes |
| `spark-submit` | spawns the CLI. Needs a version-matched Spark distribution *in the task's image* — which is the dependency problem dagron positions against, so it never touches the control plane |

The job is named `dagron-<DAGRON_TASK_ID>-<DAGRON_EXTERNAL_EPOCH>`, and on `k8s`
that name **is** the idempotency. A retry after a crash reuses it, the apiserver
answers AlreadyExists, and the step **adopts** the running job — but only if it
is non-terminal, because adopting a finished job parks the task on a corpse it
can never leave.

**How strong that is depends on the backend, so pick deliberately.** The name is
reused on all three; only one of them enforces anything:

| Backend | A crash between "accepted" and "parked" |
|---|---|
| `k8s` | **Adopts.** `AlreadyExists` is the apiserver's own guarantee on the object name, so a duplicate is impossible |
| `rest` | **Only as strong as your body.** `{{ name }}` goes wherever you put it; unless that is a field the vendor treats as an idempotency key (Databricks `idempotency_token` and its equivalents), the retry submits a **second job**. The step cannot verify this without knowing the vendor's schema — which is the coupling `rest` exists to avoid |
| `spark-submit` | **None.** `--name` is `spark.app.name`, a label. Spark offers no submission idempotency and this step does no pre-submit lookup, so a retried submit starts a second application |

Use `k8s` where a crash during submit must not double-spend a cluster.

Naming a vendor as the backend (`SPARK_BACKEND=livy`) is refused with a pointer
to `rest`, rather than silently accepted: it works there today, and a named
backend per vendor is a permanent surface for a request shape you can already
write.

### `dagron-step-sql`

```yaml
- name: rollup
  command: ["dagron-step-sql"]
  env:
    - { name: SQL_ENGINE,    value: clickhouse }
    - { name: SQL_DSN,       value: "http://user@clickhouse:8123/?database=analytics" }
    - { name: SQL_PASSWORD,  value_from: { secret: CLICKHOUSE_PASSWORD } }
    - { name: SQL_STATEMENT, value: "INSERT INTO marts.daily SELECT …" }
```

Named after what people search for (`clickhouse`, `starrocks`, `doris`,
`postgres`, `redshift`, `mysql`), implemented against what actually varies —
three wire protocols. **Trino is deliberately not in that list**: its
`/v1/statement` protocol needs a `nextUri` loop this step does not implement,
so it is refused rather than routed through the ClickHouse transport. Maintenance is bounded by protocol count, not vendor count.
Airflow ran the per-store-operator experiment and reversed it.

**The output contract is the reason to use this over `sh -c "clickhouse-client …"`:**

| `SQL_MODE` | Where the result goes |
|---|---|
| `exec` (default) | nowhere — nothing to stdout, and what reaches the log differs by transport, so it is not a contract to gate on |
| `scalar` | one value to stdout, bounded to 1 KiB, for a downstream `when:` gate. Exactly one row and one column, both enforced — a multi-row result is refused, not silently reduced to its first |
| `rows` | NDJSON into `$DAGRON_ARTIFACTS`. **Never** stdout |

Rows never reach stdout because the engine appends every stdout line to the
task's `output` column with no cap anywhere on that path — so `SELECT *` to
stdout is an unbounded write into the datastore. `SQL_MAX_ROWS` and
`SQL_MAX_BYTES` are checked **before each row is read into the step**, and
therefore before it is written: a check applied afterwards is a report, not a
budget. Both transports stream, so a `SELECT *` over a huge table is refused
with the limit named rather than OOM-killing the step before the refusal can
print. And `rows` mode
**refuses** when `$DAGRON_ARTIFACTS` is unset rather than falling back — stdout
is the unbounded write, and the container's filesystem disappears with the task.

**Put the password in `SQL_PASSWORD`, not in the DSN.** The redactor masks task
env vars by *name* (SECRET, TOKEN, PASSWORD…), so `SQL_DSN=clickhouse://u:pw@h`
is masked nowhere — not in a log, not in an error. A DSN carrying an inline
password is refused, naming the fix. (Note the redactor's 4-character minimum: a
shorter password is never masked by anything.)

Both crates default to their lean backend set — the published images carry every
backend, because a source build vendoring the binary should not be made to carry
a Kubernetes client or a wire driver it will never speak.

## Operating it

| Variable | Default | Meaning |
|---|---|---|
| `EXTERNAL_POLL_SECS` | 30 | re-poll cadence after a poll that did not resolve. An operator knob rather than the author's `poll_secs`, because the thing it protects — the vendor's rate limit — belongs to whoever runs the engine |
| `DAGRON_DEFER_ENDPOINT` | unset | endpoint pinned onto the row at park time, so a poll after a workflow edit still talks to where the job actually went. Never a credential |
| `DEFER_HTTP_DENY_PRIVATE` | on | refuse `defer.http` hosts resolving to non-global addresses |
| `DEFER_HTTP_ALLOW_HOSTS` | unset | hosts exempt from that block |

A `defer.kind` that no registered poller owns falls through to `defer.http` when
the task declares one. If neither resolves it, the row stays **parked**, with one
warning per kind. An engine rebuilt without the backend that owns a running job should not
tear that job's task down on the next tick; cancel the run to release it.

**One tick's polls run concurrently.** Up to 32 parked jobs are claimed per
tick and polled at the same time, not one after another — polled serially, a
batch pointed at a black-holed vendor would hold the reconcile tick for 32 × the
request timeout, stalling claim, dispatch and run reaping behind it. The
per-request timeout bounds one poll; it never bounded the tick.

The consequence worth knowing is on the other side of the wire: a vendor can see
up to 32 simultaneous status requests from one scheduler, where before it saw
one at a time. If that trips a concurrency limit, the 429 is **not** a verdict —
the row re-parks and is retried `EXTERNAL_POLL_SECS` later, so the job is never
failed for it. Lower `EXTERNAL_POLL_SECS` protects the *rate*; nothing today
lowers the fan-out below the batch, and a vendor that needs that is worth an
issue.

### Failure modes

| Mode | What happens |
|---|---|
| Remote job fails | the remote error text becomes the task's output, so `retry_budgets:` and fault classification act on it unchanged |
| Vendor 429 / 5xx / connection reset | **not a verdict.** Re-parked with backoff. A rate limit says nothing about the job |
| `max_wait_secs` elapses | task fails; with `defer.http.cancel` set the job is then torn down, without it the message says explicitly that the remote job was **not** cancelled by this engine |
| Submit finishes on a reclaimed lease | the row belongs to a newer attempt; the job it started is orphaned and logged as such |
| Mixed-version schedulers | a scheduler predating `defer:` silently ignores the field and succeeds the task while the job runs. **Roll schedulers before publishing `defer:` specs** |

## Writing a backend

Built-in kinds aside, a backend registers through the `ExternalPoller` seam:

```rust
#[async_trait]
pub trait ExternalPoller: Send + Sync {
    /// Ok(None) = not my kind; fall through to the built-ins.
    async fn poll(&self, ctx: &PollCtx<'_>) -> anyhow::Result<Option<Verdict>>;

    /// Tear down a job whose run was cancelled. DEFAULTED — an implementation
    /// that only resolves jobs may omit it entirely, and the row is then
    /// counted as an orphan rather than torn down.
    async fn cancel(&self, _ctx: &PollCtx<'_>) -> anyhow::Result<Option<()>> {
        Ok(None)
    }
}

pub enum Verdict {
    Running,
    Succeeded { output: String },
    Failed { reason: String },
}
```

Hand it to the engine as `Seams { external_poller: Some(Arc::new(MyPoller)), ..Default::default() }`.

The `Result` is not decoration. A transport failure is **not** a verdict: return
`Err` and the sweep re-parks. Encoding it as `Failed` kills a healthy job because
its vendor rate-limited you; encoding it as `Running` swallows the rate limit.

## Limits of this build — external jobs

Two things here are not in this build. The full list, and what to do about it,
is [what this build does not do](https://github.com/lucheeseng827/dagron#what-this-build-does-not-do) in the README.

**Named connections** — one access-controlled, audited registry of compute and
warehouse endpoints, with credentials the workflow never names and a record of
which run used which endpoint.

**Cost attribution** (`budget: { external_cost_attribution: true }`) — pulling a
vendor's actual invoice back and reconciling each line to the run, workflow and
team that caused it. Asking for it here is a validation error, not a silent
no-op, because `DagSpec` accepts unknown keys: without the refusal a workflow
written against a build that has attribution would validate clean, run, and
account nothing.

The ceiling above is **not** the gated part and never will be — `external_cost`
is arithmetic on your own declared numbers, and a guardrail you set for yourself
prices nothing. What attribution adds is the other direction: numbers dagron did
not get from you.

This build carries the endpoint with the run, which is what a single team with a
single cluster needs:

```yaml
environment: prod                # named env: variables + AES-256-GCM secrets
tasks:
  - name: submit
    env:
      - { name: SPARK_API,   value: "{{ env.SPARK_API }}" }
      - { name: SPARK_TOKEN, value_from: { secret: SPARK_TOKEN } }
    defer: { kind: spark-k8s }
```

Secrets resolve at dispatch and are masked in task output ([`CONFIG.md`](CONFIG.md)).
Endpoint resolution also plugs in through the `ExternalPoller` seam above.
