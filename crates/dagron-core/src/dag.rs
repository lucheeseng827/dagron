use anyhow::{bail, Result};
use petgraph::{algo::is_cyclic_directed, graph::DiGraph};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TaskSpec {
    pub name: String,
    /// Shell/argv to run for a **leaf** task. Empty for a **call** task (one that
    /// invokes a `template` instead of running a container). Exactly one of
    /// `command` / `template` must be set; the template expander
    /// ([`crate::expand`]) rewrites every call task into leaf tasks before the
    /// graph is built, so a persisted/dispatched task always has a `command`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    #[serde(default)]
    pub depends_on: Vec<String>,
    pub input: Option<serde_json::Value>,

    // ── Sub-workflow / templating ────────────────────────────────────────────
    // These fields are consumed by the template expander and never persist on a
    // leaf task (skip_serializing_if keeps the stored TaskSpec JSON clean).
    /// Name of the `template` (a reusable sub-DAG declared in `DagSpec.templates`)
    /// this task calls. Makes the workflow call another workflow inline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    /// Arguments passed to whatever this task calls — values may reference the
    /// caller's scope via `{{ name }}`.
    ///
    /// Two callees, one field, because it is one idea:
    ///
    ///   * with `template:` they fill the template's parameters, inline, and
    ///     are consumed by the expander.
    ///   * with `type: workflow` they become the **child run's** parameters
    ///     (#23). Unlike the template case these *survive* expansion, because
    ///     the child run does not exist until the engine dispatches the trigger.
    ///     Without them a trigger can only hand the child constants, so every
    ///     run of a child workflow is identical — which is what made a repeating
    ///     trigger unable to tell one conversation from another.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub arguments: BTreeMap<String, String>,
    /// Fan-out: expand the call once per item. `{{ item }}` (and `{{ item.key }}`
    /// for object items) substitutes within the expansion — the map/`withItems`
    /// pattern.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub with_items: Option<Vec<serde_json::Value>>,
    /// Fan-out from a parameter holding a JSON array string (the `withParam`
    /// pattern) — e.g. `with_param: "{{ shards }}"`. Resolved like `with_items`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub with_param: Option<String>,
    /// Fan-out over an **upstream task's output**, resolved at run time rather
    /// than at expansion — `with_output_of: list-partitions`.
    ///
    /// The other two fan-outs are decided by the expander before anything runs,
    /// which is what lets `budget:` refuse a blow-up at submit. This one cannot
    /// be: at expansion time the producer has not run, so there is no list. The
    /// task is created as **one** row that never executes; when its
    /// dependencies are satisfied it parks (`status = 'running'`, no lease —
    /// the same shape as every other park), and a reconcile sweep reads the
    /// producer's trimmed stdout, parses a JSON array, and inserts one
    /// instance row per element. The parked row then stands as the join point
    /// its dependents were already wired to, so nothing is re-parented
    /// mid-run.
    ///
    /// The named task must be in `depends_on` — the same rule a runtime
    /// `when:` output reference carries, and for the same reason: an output
    /// you are not guaranteed to have is not an input.
    ///
    /// An empty array is **not** an error here, unlike `with_items: []`. At
    /// expansion an empty list is an authoring mistake; at run time "there was
    /// nothing to process" is a result, so the barrier simply succeeds with
    /// zero instances.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub with_output_of: Option<String>,
    /// Conditional guard, e.g. `"{{ depth }} > 0"`. When it evaluates false the
    /// task (and any sub-DAG it would expand to) is skipped. This is what lets a
    /// recursive template terminate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<String>,
    /// Human-readable label template for fan-out instances, e.g.
    /// `instance_key: "{{ item.region }}"`. When set on a `with_items` /
    /// `with_param` / `with_output_of` task, each expanded instance is named
    /// `<task>.<rendered-label>` instead of `<task>.<index>` — a readable
    /// display name for fan-out instances. Consumed at
    /// expansion; never persists on a leaf. Labels are sanitized to
    /// `[A-Za-z0-9_-]` and must be unique within the fan-out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_key: Option<String>,
    /// When this task runs relative to its dependencies' outcomes.
    /// One of `all_success` (default),
    /// `all_done`, `one_failed`, `all_failed`, `none_failed`. `None` = the
    /// default `all_success`. Lets a task be a cleanup join (`all_done`) or a
    /// failure handler (`one_failed`) instead of being skipped when a
    /// dependency fails.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_rule: Option<String>,
    /// Lifecycle hook: `on_exit` runs this task once
    /// every non-hook task is terminal (a finalizer/notifier); `on_failure` runs
    /// it only when the run is failing. Sugar over trigger rules — the task is
    /// auto-wired to depend on every non-hook task with the matching rule
    /// (`all_done` / `one_failed`), so it needs no explicit `depends_on`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hook: Option<String>,
    /// When true, this task failing does **not** fail the run (an optional /
    /// best-effort step). The task still shows as
    /// `failed` and still skips its `all_success` dependents; use a downstream
    /// `trigger_rule` if they should proceed regardless.
    #[serde(default, skip_serializing_if = "is_false")]
    pub allow_failure: bool,
    /// Task kind. `type: approval` makes this a **human approval gate**
    /// (fast-win #19): when its dependencies are
    /// satisfied it parks in `awaiting_approval` instead of running a command, and
    /// waits for an operator to approve (→ succeeds) or reject (→ fails, skipping
    /// `all_success` downstream) via the API, or for `approval_timeout_secs` to
    /// auto-resolve it. `None`/`"task"` = an ordinary command task.
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub task_type: Option<String>,
    /// For a `type: workflow` task (#23): the name of the **registered workflow**
    /// to trigger. The engine submits that workflow as a child run when this task
    /// is reached and parks the task until the child run is terminal — succeeding
    /// with it (child succeeded) or failing with it (child failed/cancelled).
    /// Required for (and only valid on) a `type: workflow` task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow: Option<String>,
    /// For a `type: wait` task (#27): a deferrable time sensor. The task parks
    /// with **no worker slot held** until the deadline, then succeeds. See
    /// [`WaitSpec`]. Required for (and only valid on) a `type: wait` task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait: Option<WaitSpec>,
    /// For a `type: approval` task: seconds to wait before the timeout default is
    /// applied. `None` = wait indefinitely for a human.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_timeout_secs: Option<u64>,
    /// What an expired approval defaults to — `"approve"` or `"reject"` (default
    /// `"reject"`: absent a human decision, a gate fails safe).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_on_timeout: Option<String>,
    /// How many times this task may be attempted before it is marked failed.
    /// 1 = no retries (default). Must be ≥ 1.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// Base delay in seconds between retries. Actual delay = retry_delay_secs * 2^(attempt-1).
    /// 0 = immediate retry.
    #[serde(default)]
    pub retry_delay_secs: u64,
    /// Upper bound in seconds on the exponential retry backoff. Without it the
    /// delay doubles unbounded (up to 2^10 doublings); with it the computed
    /// delay is clamped to `min(delay, retry_max_delay_secs)` — the
    /// retry-backoff ceiling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_max_delay_secs: Option<u64>,
    /// Whether a task killed by its `timeout_secs` deadline is retried. Defaults
    /// to `true` (a timeout is a failure like any other). Set `false` when a
    /// deadline kill is unlikely to succeed on re-run (Airflow #9232) — the task
    /// then fails immediately on timeout instead of burning the rest of its
    /// `max_attempts`. Timeout-only: non-zero exits and backend errors still
    /// retry. Falls back to [`TaskDefaults::retry_on_timeout`], then `true`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_on_timeout: Option<bool>,
    /// Per-fault-class attempt budgets — how many attempts this task gets
    /// *given what broke*.
    ///
    /// `max_attempts` spends the same budget on every failure, which is why
    /// teams wrap schedulers in bespoke retry bash: an ECC error and a NaN loss
    /// draw from the same three attempts, so infra faults give up too early and
    /// application faults burn GPU-hours proving a determinism nobody doubted.
    ///
    /// ```yaml
    /// retry_budgets:
    ///   gpu-ecc: 8          # the node broke; try elsewhere, liberally
    ///   fabric-ib: 8
    ///   nan-loss: 0         # never again — the next attempt diverges too
    ///   checkpoint-corrupt: 0
    /// ```
    ///
    /// Keys are [`crate::fault::FaultClass`] strings and are validated at parse
    /// (an unknown key is a typo that would silently never fire, so it is a
    /// hard error rather than a shrug). A class with no entry falls back to its
    /// disposition default, then to `max_attempts` — see
    /// [`crate::models::effective_budget`]. `0` means the attempt that just ran
    /// was the last one.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub retry_budgets: std::collections::BTreeMap<String, u32>,
    /// Per-task subprocess timeout in seconds. Falls back to the 25 s hard limit when absent.
    pub timeout_secs: Option<u64>,
    /// Docker image for this task. Used by DockerExecutor; ignored by LocalExecutor.
    /// If absent, DockerExecutor falls back to its configured default image.
    pub docker_image: Option<String>,
    /// Environment variables injected into the task container. Honoured by the
    /// Local (subprocess), Docker, and Kubernetes executors. This is how a
    /// parameterised task image (e.g. the load-test ETL image) is told what to do
    /// — object size, sleep/CPU/mem profile, S3 bucket, DB DSN, etc.
    #[serde(default)]
    pub env: Vec<EnvVar>,
    /// Per-task CPU/memory `requests`/`limits` applied to the task **pod** so the
    /// Kubernetes scheduler packs pods realistically (pod headroom, eviction, and
    /// OOMKill become observable). Ignored by the Local and Docker executors.
    pub resources: Option<ResourceRequirements>,
    /// ServiceAccount for the task pod — the IRSA seam. Annotating this SA with an
    /// `eks.amazonaws.com/role-arn` lets task pods assume an IAM role and reach S3
    /// (extract/load) without static credentials. Kubernetes executor only.
    pub service_account: Option<String>,
    /// Which **runner class** (pool of scheduler replicas) may claim this task.
    /// Schedulers started with `RUNNER_CLASSES=a,b` claim only tasks in those
    /// classes; unset schedulers claim everything. Falls back to the DAG-level
    /// [`DagSpec::runner_class`], then to `"default"`. Lowercase
    /// `[a-z0-9_-]`, max 64 chars.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_class: Option<String>,
    /// Dispatch **priority** (Airflow `priority_weight` / Argo Kueue analog).
    /// Among the tasks that are `ready` at the same moment, a scheduler claims
    /// higher-priority tasks first (`ORDER BY priority DESC, scheduled_at`), so a
    /// latency-sensitive branch jumps a deep backlog of low-priority work. Any
    /// signed integer; `0` is the default. Falls back to the DAG-level
    /// [`TaskDefaults::priority`] when a task leaves it at `0`. Priority breaks
    /// ties only — it never lets a task run before its dependencies, and it
    /// persists on the row so a retry / lease recovery keeps its place.
    #[serde(default)]
    pub priority: i64,
    /// Named **concurrency pool** this task draws a slot from (parity fast-win
    /// #21 — Airflow pools / Argo Kueue). A scheduler claims a pooled task only
    /// while fewer than the pool's configured capacity are already running in it
    /// (capacities come from the `POOLS` env, e.g. `POOLS=etl:4`); an over-budget
    /// task simply waits in `ready` until a slot frees — no run is dropped. A
    /// pool with no configured capacity is unlimited. Falls back to
    /// [`TaskDefaults::pool`]. `None` = unpooled (unlimited). Lowercase
    /// `[a-z0-9_-]`, max 64 chars.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    /// Result **memoization** (parity fast-win #22 — Argo memoization / Prefect
    /// task caching). When set, a successful run stores its output keyed by
    /// `(workflow, task, resolved cache key)`; a later task with the same key
    /// reuses that output and skips execution entirely. The key templates
    /// resolve at expansion, so `{{ scheduled_time }}` / `{{ params.* }}` make a
    /// backfill reproducible. `None` = always run. See [`CacheSpec`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<CacheSpec>,
    /// Loop operator: re-run this task until `until` evaluates true (the
    /// poll-until-done pattern). See [`RepeatSpec`]. Evaluated by the engine
    /// each time the task *succeeds*; failures still follow the normal
    /// retry/failure path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repeat: Option<RepeatSpec>,
    /// Hand this task's work to a system dagron does not own (Spark, a
    /// warehouse, any submit-then-poll API) and park the row rather than hold a
    /// worker while it runs. The `command` becomes the submit and prints the
    /// remote handle; a reconcile sweep polls it. See [`DeferSpec`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub defer: Option<DeferSpec>,
    /// **Datasets** this task updates when it succeeds (Airflow `outlets` /
    /// Dagster asset materializations). Each URI is upserted into the `datasets`
    /// registry and appended to the `dataset_events` lineage ledger with the
    /// producing run/task — which is what dataset wait-sensors
    /// (`wait: { dataset: … }`) and dataset-triggered workflows
    /// ([`DagSpec::on_datasets`]) key off. URIs template at expansion
    /// (`{{ params.* }}` / `{{ item }}`), so a fan-out can produce per-shard
    /// datasets. Purely declarative — the engine records the update; moving the
    /// actual bytes is the task's job. Empty = this task produces nothing.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub produces: Vec<String>,
    /// Gang / co-scheduling: expand this task into `size` member instances
    /// (`<name>.0` … `<name>.N-1`) that a gang-aware scheduler claims
    /// **all-or-nothing** (distributed training: N ranks together or none).
    /// See [`GangSpec`]. Leaf command tasks only; incompatible with retries,
    /// `repeat`, approval gates, and template calls.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gang: Option<GangSpec>,
    /// Engine-internal (populated at run creation on each expanded member —
    /// not for workflow authors): which gang this row belongs to, its rank,
    /// and the gang size. Dispatch injects these as `DAGRON_GANG_ID` /
    /// `DAGRON_GANG_RANK` / `DAGRON_GANG_SIZE` for rendezvous.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gang_member: Option<GangMember>,
    /// **Trust envelope** for this task — the privileges it runs under, declared
    /// per task rather than per scheduler process. See
    /// [`crate::isolation::IsolationSpec`].
    ///
    /// The engine raises whatever is declared here to the operator's
    /// `DAGRON_TASK_ISOLATION_FLOOR` before dispatch, so a workflow author may
    /// harden a task beyond the floor but never below it — which is what makes
    /// the field safe to expose to authors who are not the operator. The
    /// *effective* envelope is what the Kubernetes executor applies and what a
    /// run attestation records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub isolation: Option<crate::isolation::IsolationSpec>,
}

/// `gang:` — all-or-nothing co-scheduling for one task (see [`TaskSpec::gang`]).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GangSpec {
    /// Number of member instances (≥ 2); each runs the same command with its
    /// rank in `DAGRON_GANG_RANK`.
    pub size: u32,
}

/// Engine-stamped gang membership of one expanded member row.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GangMember {
    pub id: String,
    pub rank: u32,
    pub size: u32,
}

/// `repeat:` — run a task repeatedly until a condition on its own output holds.
///
/// After each successful execution the engine evaluates `until` with
/// `{{ output }}` (the task's stdout, trimmed) and `{{ attempt }}` (the
/// 1-based iteration count) bound; the same expression grammar as `when:`
/// (one binary comparison or a bare truthy value). True → the task succeeds
/// and the DAG proceeds. False → the task is re-queued after `delay_secs`,
/// up to `max_iterations` total executions, after which it **fails** (a
/// condition that never came true is an error, not a success).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RepeatSpec {
    /// Condition ending the loop, e.g. `"{{ output }} == done"`.
    pub until: String,
    /// Total execution budget (≥ 1). Bounded on purpose — an unbounded loop
    /// wedges a run forever.
    pub max_iterations: u32,
    /// Seconds to wait between iterations (default 0 = immediate).
    #[serde(default)]
    pub delay_secs: u64,
}

/// What `repeat:` decides after one successful iteration.
///
/// A value rather than inline control flow because **two paths now ask**: an
/// executor reporting a finished command, and the reconcile sweep resolving a
/// sub-workflow trigger whose child run has gone terminal. Those paths share no
/// machinery at all — one holds a worker claim and a fence, the other holds a
/// parked row nobody owns — so the only thing that can be shared is the
/// decision itself. Two copies of a loop operator is how the two come to
/// disagree about when a loop is over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepeatDecision {
    /// `until` holds — this really was the task's success.
    Done,
    /// Not yet, and iterations remain. Re-queue after `delay_secs`.
    Again { delay_secs: u64 },
    /// Give up. `reason` is what the task's output should record.
    Fail { reason: String },
}

impl RepeatSpec {
    /// Decide what happens after the iteration numbered `iteration` (1-based)
    /// produced `output`.
    ///
    /// `until` sees `{{ output }}` (trimmed) and `{{ attempt }}` (the iteration
    /// number), the same two bindings it has always seen. Running out of
    /// iterations is a **failure**, not a success: a condition that never came
    /// true is an error, and reporting it as success would hand the next task a
    /// result the loop never actually reached.
    pub fn decide(&self, output: &str, iteration: i64) -> RepeatDecision {
        let mut ctx = BTreeMap::new();
        ctx.insert("output".to_string(), output.trim().to_string());
        ctx.insert("attempt".to_string(), iteration.to_string());
        match crate::expand::eval_when(&crate::expand::substitute(&self.until, &ctx)) {
            Ok(true) => RepeatDecision::Done,
            Ok(false) if iteration < i64::from(self.max_iterations) => {
                RepeatDecision::Again { delay_secs: self.delay_secs }
            }
            Ok(false) => RepeatDecision::Fail {
                reason: format!(
                    "repeat.until '{}' not satisfied after {} iterations; last output:\n{}",
                    self.until, iteration, output
                ),
            },
            Err(e) => RepeatDecision::Fail {
                reason: format!("repeat.until '{}' failed to evaluate: {e}", self.until),
            },
        }
    }
}

/// `defer:` — hand this task's work to a system dagron does not own, then park
/// the row instead of holding a worker while that work runs.
///
/// The task's `command` is the **submit**, and nothing else. It runs exactly as
/// any command task does — lease, `max_attempts`, `timeout_secs`, fault
/// classification — and on success prints the remote job's identity as
/// [`HANDLE_PREFIX`]`<handle>` on its last matching stdout line. The engine
/// then parks the row: claim dropped, lease NULLed, `status` still `running`,
/// handle on the row. A reconcile sweep polls it to a verdict.
///
/// **`timeout_secs` therefore bounds the submit, not the job.** The job's own
/// ceiling is [`DeferSpec::max_wait_secs`]. Conflating them is the mistake this
/// split exists to prevent: a 25-second default deadline (the executor's
/// `DEFAULT_TASK_TIMEOUT_SECS`) is right for a submit and absurd for a
/// six-hour Spark run.
///
/// Why the park is safe across a crash: `recover_expired_leases` filters
/// `lease_expires_at IS NOT NULL`, so a parked row is provably outside the set
/// it can reclaim. Every scheduler can die and the row is untouched; any
/// replica's next sweep resumes the poll. Nothing resubmits, because nothing
/// re-ran.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeferSpec {
    /// Which poller resolves this task — a built-in kind, or one a registered
    /// `ExternalPoller` claims. Routed on, never parsed for meaning.
    pub kind: String,
    /// Seconds between polls (default [`DEFAULT_DEFER_POLL_SECS`]). The floor
    /// is 1: a zero would hot-loop the sweep against someone's API.
    #[serde(default = "default_defer_poll_secs")]
    pub poll_secs: u64,
    /// What one submission of this task costs, **in whatever unit the author
    /// chose**. Default 1, so a run that declares no unit costs bounds a plain
    /// count of submissions.
    ///
    /// The engine never learns what this means. It is not dollars, not
    /// GPU-hours, not anything the engine can verify — it is a number the
    /// author wrote down so that [`RunBudget::external_cost`] can add it up.
    /// A 200-node Spark job and a one-row query both cost 1 until someone says
    /// otherwise, which is exactly the judgement the engine has no way to make.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<u64>,
    /// Ceiling on how long the *remote job* may take before the task is failed,
    /// independent of `timeout_secs` (which bounds the submit) and of
    /// `run_timeout_secs` (which bounds the whole run).
    ///
    /// `None` means the run's own deadline is the only bound. That is a
    /// deliberate default rather than a number, because the right ceiling for a
    /// remote job is a property of the job and guessing one would fail exactly
    /// the long workloads the feature exists for. The
    /// `scheduler_external_parked` gauge is how an operator notices a row that
    /// has no ceiling and is not moving.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_wait_secs: Option<u64>,
    /// Poll this job over HTTP: the built-in transport, and the one that
    /// reaches a Databricks run, an EMR Serverless job, a Dataproc batch, a
    /// Livy or Kyuubi session, a YARN application or a SparkApplication CR
    /// without a line of vendor code in dagron. See [`DeferHttpSpec`].
    ///
    /// Consulted only when no registered `ExternalPoller` claims the row's
    /// `kind` — a poller installed for a kind is the more specific thing, and
    /// gets first refusal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http: Option<DeferHttpSpec>,
    /// Named connection this task defers through — an org-owned,
    /// access-controlled registry of compute and warehouse endpoints. Not in
    /// the open build; validation refuses it with a signpost naming the open
    /// path (`environment:` variables plus `value_from: { secret: … }`).
    ///
    /// Declared here rather than left unknown on purpose: `DagSpec` carries no
    /// `deny_unknown_fields`, so an unrecognised key is *silently dropped* by
    /// serde. A workflow written against the closed build would otherwise
    /// validate clean here, run, and submit with whatever the task's own env
    /// happened to carry, with no diagnostic anywhere.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection: Option<String>,
}

/// `defer.http` — poll a remote job by GETting a status endpoint and reading a
/// verdict out of the JSON it returns.
///
/// This is the whole vendor story, and deliberately so: one adapter reaches
/// Databricks (`runs/get`), EMR Serverless (`GetJobRun`), Dataproc
/// (`batches.get`), Livy, Kyuubi, YARN and a SparkApplication CR, because every
/// one of them answers "is it done?" with a JSON document containing a state
/// field. Vendor API drift then becomes a YAML edit by the person it affects,
/// on their schedule, rather than a dagron release on ours.
///
/// The URL may contain `{{ handle }}`, which the poller replaces with the
/// remote job's identity — the thing the submit printed and the engine parked
/// on. It survives expansion untouched because `expand::substitute` leaves
/// unknown placeholders verbatim, the same mechanism that carries
/// `{{ output }}` into `repeat.until`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeferHttpSpec {
    /// Status endpoint. `http`/`https` only, and `{{ handle }}` expands to the
    /// remote job id.
    pub url: String,
    /// Headers to send. The same shape as a task's `env:`, so a credential
    /// rides as `value_from: { secret: NAME }` and is resolved at poll time
    /// rather than stored in the spec — and is masked in anything the poller
    /// writes back to the task.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<EnvVar>,
    /// The job finished successfully when this holds, e.g.
    /// `status.applicationState.state == COMPLETED`. See
    /// [`crate::jsonpred`] for the grammar.
    pub succeed_when: String,
    /// …and failed when this does. Optional, because some APIs report failure
    /// as "reached a terminal state that is not success" and the author would
    /// rather write one predicate than two. Without it a job that fails is
    /// bounded by `max_wait_secs` rather than noticed, which is worth saying
    /// out loud: prefer writing it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fail_when: Option<String>,
    /// Dotted path to the vendor's own error text, lifted into the task's
    /// failure reason so `retry_budgets:` and fault classification see what the
    /// remote system actually said. Truncated before it reaches the task's
    /// output — a stack trace in a status document is still a status document.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_from: Option<String>,
    /// How to stop the remote job when the run is cancelled or `max_wait_secs`
    /// elapses. Without it the built-in transport cannot tear anything down —
    /// it only ever GETs — and the job keeps running, and costing money, after
    /// dagron has stopped watching. Sent with this block's `headers`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel: Option<DeferHttpCancel>,
}

/// `defer.http.cancel` — the one request that stops the remote job: a `DELETE`
/// on a SparkApplication, a `POST /runs/cancel` on Databricks. Same story as
/// [`DeferHttpSpec`]: dagron carries the transport, the author carries the vendor.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeferHttpCancel {
    /// `http`/`https` only; `{{ handle }}` expands to the remote job id.
    pub url: String,
    /// `DELETE` when omitted. `POST`, `PUT` and `PATCH` are the other verbs a
    /// vendor's cancel is ever spelled with; nothing else is accepted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    /// Sent as `application/json`; `{{ handle }}` expands here too.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
}

impl DeferHttpCancel {
    /// The verb to send, upper-cased. Validation has already refused anything
    /// outside [`CANCEL_METHODS`], so callers can pass it straight to reqwest.
    pub fn method(&self) -> String {
        self.method.as_deref().map_or("DELETE".into(), |m| m.trim().to_ascii_uppercase())
    }
}

/// The verbs `defer.http.cancel.method` may name.
pub const CANCEL_METHODS: [&str; 4] = ["DELETE", "POST", "PUT", "PATCH"];

/// The `{{ handle }}` placeholder a `defer.http` URL may carry.
pub const HANDLE_PLACEHOLDER: &str = "{{ handle }}";

/// How much of `error_from`'s extraction reaches the task's output.
///
/// A status document's error field is sometimes a sentence and sometimes a
/// paged stack trace, and the task's `output` column is appended to with no cap
/// anywhere on the write path — so the bound has to be applied here, before the
/// text is handed over, rather than checked afterwards.
pub const MAX_EXTERNAL_ERROR_BYTES: usize = 2048;

/// Default seconds between polls of a deferred task's remote job.
///
/// Thirty, matching `wait: { url: … }`'s `WAIT_POLL_SECS` default rather than
/// inventing a second cadence. Fast enough that a short job does not sit
/// resolved-but-unnoticed for minutes; slow enough that a few hundred parked
/// rows do not become a rate-limit incident against one vendor endpoint.
pub const DEFAULT_DEFER_POLL_SECS: u64 = 30;

fn default_defer_poll_secs() -> u64 {
    DEFAULT_DEFER_POLL_SECS
}

/// What a deferred task's submit prints to hand the engine its remote handle.
///
/// The **last** matching line wins, so a step may log freely before it: a step
/// that emits progress and then the handle is the normal shape, and a first-
/// match rule would make any earlier mention of the prefix in a log line — a
/// retried submit echoing its previous output, say — silently become the
/// handle.
pub const HANDLE_PREFIX: &str = "dagron::handle=";

/// The remote handle a deferred submit declared, if it declared one.
///
/// Trimmed, and empty is `None`: a step that printed the prefix with nothing
/// after it has not named a job, and parking on an empty handle would produce
/// a row no sweep can ever resolve.
pub fn parse_handle(output: &str) -> Option<String> {
    output
        .lines()
        .rev()
        .find_map(|l| l.trim().strip_prefix(HANDLE_PREFIX))
        .map(str::trim)
        .filter(|h| !h.is_empty())
        .map(str::to_string)
}

/// The largest `repeat.delay_secs` accepted (one year). Validation rejects a
/// larger delay at submit, and [`delayed_retry_at`] clamps to it — a backoff
/// past this is a typo, and a truly enormous one would otherwise panic
/// `chrono::TimeDelta::seconds`.
pub const MAX_REPEAT_DELAY_SECS: u64 = 365 * 24 * 60 * 60;

/// An RFC-3339 timestamp `delay_secs` from now, computed without panicking.
///
/// `chrono::TimeDelta::seconds` panics past ~`i64::MAX / 1000`, and even a valid
/// but enormous delta can overflow `DateTime` addition. `delay_secs` originates
/// in a user's `repeat.delay_secs`, so both are reachable from a spec. The value
/// is clamped to [`MAX_REPEAT_DELAY_SECS`] (validation already rejects larger
/// ones at submit; this guards any row that predates that check) and the
/// addition is checked, so the worst case is a slightly-too-soon retry rather
/// than a crashed reconcile tick.
pub fn delayed_retry_at(delay_secs: u64) -> String {
    let secs = delay_secs.min(MAX_REPEAT_DELAY_SECS) as i64;
    let now = chrono::Utc::now();
    chrono::TimeDelta::try_seconds(secs)
        .and_then(|d| now.checked_add_signed(d))
        .unwrap_or(now)
        .to_rfc3339()
}

/// `wait:` — a deferrable time sensor for a `type: wait` task (fast-win #27 —
/// Airflow deferrable time sensors / Argo suspend-with-duration). Exactly one of
/// `for` (a relative duration like `30s`/`5m`/`2h`, anchored when the task is
/// reached) or `until` (an absolute RFC-3339 instant) must be set. The task
/// holds no worker slot while it waits; the reconcile loop resolves it (success)
/// once the deadline passes.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WaitSpec {
    /// Relative duration to wait, anchored when the task is reached.
    #[serde(default, rename = "for", skip_serializing_if = "Option::is_none")]
    pub wait_for: Option<String>,
    /// Absolute RFC-3339 instant to wait until.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<String>,
    /// HTTP(S) endpoint to poll — an HTTP sensor. The task parks and the engine
    /// GETs this URL on a fixed interval (`WAIT_POLL_SECS`, default 15 s),
    /// succeeding when it returns a 2xx (Airflow HttpSensor). Bounded by the
    /// run's `run_timeout_secs`. Exactly one of `for` / `until` / `url` /
    /// `dataset` is set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Dataset to wait on — a **dataset sensor** (Dagster asset sensor /
    /// Airflow dataset-condition). The task parks holding no worker slot and
    /// succeeds when the named dataset records an update **after** the park
    /// (a `produces:` task succeeded, or — feature-gated — an external dataset
    /// event was posted). Updates already in the ledger at park time do not
    /// count: the sensor waits for *fresh* data, not any data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset: Option<String>,
}

/// `cache:` — result memoization for a task (fast-win #22). A successful run
/// records its output under `(workflow, task, key)`; a later task with the same
/// resolved `key` reuses that output and does not execute. `key` is a template
/// resolved at expansion (so it can reference `{{ params.* }}` /
/// `{{ scheduled_time }}`), making repeated/backfilled runs hit the cache.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CacheSpec {
    /// Cache key template. Two task runs whose resolved keys match share a result.
    pub key: String,
    /// Maximum age (seconds) of a cached entry; an older entry misses and the
    /// task re-runs. `None` = the entry never expires.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age_secs: Option<u64>,
}

/// A single environment variable for a task container. Either a literal `value`
/// or a `value_from` secret reference resolved at dispatch (never persisted
/// resolved). Omitting `value` defaults it to empty (used with `value_from`).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EnvVar {
    pub name: String,
    #[serde(default)]
    pub value: String,
    /// Resolve this variable's value from a secret at dispatch instead of storing
    /// it inline — so a credential never lands in the workflow spec or the
    /// datastore. See [`SecretRef`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_from: Option<SecretRef>,
}

/// A reference to an external secret (`value_from: { secret: NAME }`). The
/// resolver (in `dagron-executor`) reads `DAGRON_SECRET_<NAME>` from the engine
/// process environment, or a file `<DAGRON_SECRETS_DIR>/<NAME>` (the SOPS /
/// External-Secrets-Operator mount convention) — whichever is configured.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SecretRef {
    pub secret: String,
}

/// Kubernetes-style resource requests/limits (e.g. `cpu: "250m"`, `memory:
/// "512Mi"`). Both maps are optional; whatever is present is copied verbatim onto
/// the task pod container's `resources` block. `gpu:` is accelerator sugar —
/// see [`GpuRequest`].
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ResourceRequirements {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub requests: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub limits: BTreeMap<String, String>,
    /// GPU sugar: `resources: { gpu: { count: 1 } }` instead of hand-writing
    /// the vendor's extended-resource key into `limits`. Expanded by
    /// [`ResourceRequirements::effective_limits`]; an explicit `limits` entry
    /// for the same key wins, so specs that already spell it out are unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu: Option<GpuRequest>,
}

/// Accelerator request for a task (`resources.gpu`). Kubernetes schedules GPUs
/// as *extended resources* — an opaque counted key in the container's `limits`
/// (requests are implied equal for extended resources) — so this expands to
/// `limits["<resource>"] = "<count>"`. Combine with `runner_class` (e.g.
/// `spot-gpu` vs `ondemand-gpu` pools) to route the task to schedulers fronting
/// the right accelerator capacity.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GpuRequest {
    /// Number of devices (≥ 1). Fractional/MIG slicing is the device plugin's
    /// concern, not dagron's — name the sliced resource via `resource` instead.
    pub count: u32,
    /// Extended-resource key advertising the accelerator. Default
    /// `nvidia.com/gpu`; set e.g. `amd.com/gpu`, `google.com/tpu`, or a MIG
    /// profile key like `nvidia.com/mig-1g.5gb`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource: Option<String>,
}

/// The default accelerator extended-resource key (`resources.gpu.resource`).
pub const DEFAULT_GPU_RESOURCE: &str = "nvidia.com/gpu";

impl ResourceRequirements {
    /// `limits` with the `gpu:` sugar folded in. An explicit `limits` entry for
    /// the same key wins over the sugar (a spec that spells both is taken at
    /// its word); with no `gpu:` this is exactly `limits`.
    pub fn effective_limits(&self) -> BTreeMap<String, String> {
        let mut limits = self.limits.clone();
        if let Some(gpu) = &self.gpu {
            let key = gpu.resource.as_deref().unwrap_or(DEFAULT_GPU_RESOURCE);
            limits
                .entry(key.to_string())
                .or_insert_with(|| gpu.count.to_string());
        }
        limits
    }
}

fn default_max_attempts() -> u32 {
    1
}

/// `skip_serializing_if` helper for a `bool` field defaulting to `false`.
fn is_false(b: &bool) -> bool {
    !*b
}

/// Valid `hook:` values.
pub const HOOK_KINDS: &[&str] = &["on_exit", "on_failure"];

/// Valid `type:` values. `task` (the default) is an ordinary command task;
/// `approval` is a human approval gate (#19); `workflow` triggers a registered
/// sub-workflow and waits for it (#23); `wait` is a deferrable time sensor that
/// parks with no worker until a deadline (#27).
pub const TASK_KINDS: &[&str] = &["task", "approval", "workflow", "wait"];

/// Valid `approval_on_timeout:` values.
pub const APPROVAL_TIMEOUT_ACTIONS: &[&str] = &["approve", "reject"];

impl TaskSpec {
    /// Whether this task is a `type: approval` human gate (#19).
    pub fn is_approval(&self) -> bool {
        self.task_type.as_deref() == Some("approval")
    }
    /// Whether this task is a `type: workflow` sub-workflow trigger (#23).
    pub fn is_workflow(&self) -> bool {
        self.task_type.as_deref() == Some("workflow")
    }
    /// Whether this task is a `type: wait` deferrable time sensor (#27).
    pub fn is_wait(&self) -> bool {
        self.task_type.as_deref() == Some("wait")
    }
}

/// The runner class tasks belong to when neither the task nor the DAG names one.
/// Schedulers with no `RUNNER_CLASSES` restriction claim every class, so a
/// deployment that never segments its runners behaves exactly as before.
pub const DEFAULT_RUNNER_CLASS: &str = "default";

/// Validate a `runner_class` name: lowercase `[a-z0-9_-]`, 1–64 chars, and not
/// the reserved `"other"`. Strict on purpose — the name becomes a claim-path
/// SQL filter value, a Helm pool name, and (k8s) part of label values, so one
/// conservative charset serves all three; `"other"` is the metrics tail bucket
/// (`scheduler_ready_*_by_class`), so a real class by that name would collide
/// with the aggregated series.
pub fn validate_runner_class(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 64 {
        bail!("runner_class must be 1-64 characters, got {} ('{}')", name.len(), name);
    }
    if !name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_') {
        bail!("runner_class '{name}' may only contain [a-z0-9_-]");
    }
    if name == "other" {
        bail!("runner_class 'other' is reserved (it is the metrics tail bucket)");
    }
    Ok(())
}

/// Validate a workflow `tag`: `[A-Za-z0-9_.-]`, 1–64 chars — URL-safe (it becomes
/// a `?tag=` filter value) and label-friendly (mixed case + dots allowed).
pub fn validate_tag(tag: &str) -> Result<()> {
    if tag.is_empty() || tag.len() > 64 {
        bail!("tag must be 1-64 characters, got {} ('{}')", tag.len(), tag);
    }
    if !tag.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.') {
        bail!("tag '{tag}' may only contain [A-Za-z0-9_.-]");
    }
    Ok(())
}

/// Validate a `pool` name: lowercase `[a-z0-9_-]`, 1–64 chars — the same
/// conservative charset as [`validate_runner_class`]. Strict on purpose: the
/// name becomes a claim-path SQL filter value and is matched against the
/// comma-delimited "exhausted pools" set in the SQLite claim, so a comma (or
/// other delimiter) in a name must be impossible.
pub fn validate_pool(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 64 {
        bail!("pool must be 1-64 characters, got {} ('{}')", name.len(), name);
    }
    if !name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_') {
        bail!("pool '{name}' may only contain [a-z0-9_-]");
    }
    Ok(())
}

/// Validate a **dataset URI** (`produces:` / `on_datasets:` / `wait.dataset`):
/// 1–512 chars, no whitespace or control characters. Deliberately loose beyond
/// that — a dataset name is an opaque identity (`s3://lake/orders`,
/// `postgres://warehouse/public.orders`, `dataset://daily-report`), matched by
/// exact string equality like Airflow dataset URIs; dagron never dereferences
/// it. Whitespace is banned so a URI can never be mistaken for two, and the
/// length cap keeps the registry's key sane.
pub fn validate_dataset_uri(uri: &str) -> Result<()> {
    if uri.is_empty() || uri.len() > 512 {
        bail!("dataset URI must be 1-512 characters, got {} ('{uri}')", uri.len());
    }
    if uri.chars().any(|c| c.is_whitespace() || c.is_control()) {
        bail!("dataset URI '{uri}' must not contain whitespace or control characters");
    }
    Ok(())
}

/// Extract a raw spec's dataset subscriptions — `(on_datasets, mode)` — without
/// full template expansion or graph validation. The engine's dataset-trigger
/// sweep calls this per registered workflow to keep `dataset_triggers` rows in
/// sync cheaply; the full [`DagGraph::from_yaml_with_params`] pipeline (and its
/// validation) still runs at fire time. `None` = the spec doesn't parse or
/// subscribes to nothing. Mode defaults to `"any"`.
pub fn dataset_subscriptions(yaml: &str) -> Option<(Vec<String>, String)> {
    let spec: DagSpec = serde_yaml::from_str(yaml).ok()?;
    if spec.on_datasets.is_empty() {
        return None;
    }
    let mode = spec.datasets_mode.unwrap_or_else(|| "any".to_string());
    Some((spec.on_datasets, mode))
}

/// A soft SLA deadline (`deadline:` block). See [`DagSpec::deadline`].
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeadlineSpec {
    /// Duration after the run starts, e.g. `"45m"`, `"2h"`, `"90s"`, or a bare
    /// number of seconds. Parsed by [`parse_duration_secs`].
    #[serde(rename = "in")]
    pub within: String,
}

/// Parse a duration like `"45m"` / `"2h"` / `"90s"` / `"1d"` (or a bare number of
/// seconds) into seconds. Errors on a malformed or zero duration.
pub fn parse_duration_secs(s: &str) -> Result<u64> {
    let s = s.trim();
    if s.is_empty() {
        bail!("empty duration");
    }
    let (num, mult) = match s.chars().last().unwrap() {
        's' => (&s[..s.len() - 1], 1u64),
        'm' => (&s[..s.len() - 1], 60),
        'h' => (&s[..s.len() - 1], 3600),
        'd' => (&s[..s.len() - 1], 86_400),
        _ => (s, 1),
    };
    let n: u64 = num
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid duration '{s}' (use e.g. 45m, 2h, 90s, or seconds)"))?;
    if n == 0 {
        bail!("duration '{s}' must be greater than zero");
    }
    // Reject overflow rather than saturating to u64::MAX, so a fat-fingered value
    // fails validation instead of silently becoming "forever".
    n.checked_mul(mult)
        .ok_or_else(|| anyhow::anyhow!("duration '{s}' is too large"))
}

/// A reusable sub-DAG that tasks can `template:`-call. Declared under
/// `DagSpec.templates`; its own `parameters` provide defaults that a caller's
/// `arguments` override.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TemplateSpec {
    pub name: String,
    /// Default parameter values for the template, overridable per call via
    /// `arguments`. Referenced inside the template's tasks as `{{ name }}`.
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
    pub tasks: Vec<TaskSpec>,
}

/// A run's declared resource ceiling (`budget:` on the spec).
///
/// **Every field here is arithmetic on numbers already known at run creation,
/// and that is a statement about the data rather than about ambition.**
/// `AGENT_SCHEDULER_PLAN.md` §G-S3 asks for a "spend/token/task budget per
/// run". A task count is exact and knowable at creation — dagron expands
/// `with_items` fan-out and templates at submit, so the number of rows a run
/// will insert is already decided before anything runs.
///
/// Spend is not, and this struct still refuses to pretend otherwise. Nothing in
/// the engine meters currency or model tokens against a run, so a `spend:`
/// field — a cap on what a run *actually costs* — would be a promise with no
/// measurement behind it, which is worse than no field. That decision stands.
///
/// [`RunBudget::external_cost`] is **not** that field, and the distinction is
/// the whole reason it can exist. It caps a sum of costs the **author
/// declared** on their own tasks ([`DeferSpec::cost`], default 1), so the
/// engine's arithmetic is exact and the only estimate in the system is one a
/// human wrote down deliberately. Declared, never measured. Reconciling a
/// vendor's real invoice back to the run that caused it is a different
/// capability — see `external_cost_attribution`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct RunBudget {
    /// Maximum tasks this run may create. `None` = no cap. Must be >= 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tasks: Option<u32>,
    /// Ceiling on the **declared** cost of this run's external work: the sum of
    /// [`DeferSpec::cost`] over every deferred task the expanded graph will
    /// create. `None` = no cap. Must be >= 1.
    ///
    /// Checked at run creation, like `tasks` and for the same reason — after
    /// expansion the sum is exact, so a run that would break its ceiling is
    /// refused before a single remote job is submitted rather than killed
    /// halfway through with cluster-hours already spent.
    ///
    /// This is the bound that `tasks` cannot express. A run of 1000 `echo`
    /// tasks and a run of 1000 Spark submits are the same number to `tasks:`
    /// and wildly different to whoever pays for the cluster.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_cost: Option<u64>,
    /// Reconcile each external job's **actual** vendor cost back to the run,
    /// workflow and team that launched it. Not in this build — validation
    /// refuses it with a signpost naming the open path.
    ///
    /// Declared in the open struct on purpose. `DagSpec` carries no
    /// `deny_unknown_fields`, so serde would silently drop an unrecognised key:
    /// a workflow written against a build that has attribution would validate
    /// clean here, run, and quietly account nothing. A refusal is louder than a
    /// drop.
    #[serde(default, skip_serializing_if = "is_false")]
    pub external_cost_attribution: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DagSpec {
    pub name: String,
    /// Top-level workflow parameters (defaults). Referenced as `{{ name }}` in any
    /// task field and overridable when this workflow is itself called as a template.
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
    /// Reusable sub-DAGs callable via a task's `template:` field. Expanded inline
    /// into the main `tasks` graph at run-creation time (see [`crate::expand`]).
    #[serde(default)]
    pub templates: Vec<TemplateSpec>,
    /// Labels for organizing and filtering workflows (parity fast-win #26 —
    /// Airflow #16432 colored tags / #24464 folder view, Dagster #14530). Purely
    /// organizational — the engine ignores them; the workflow registry surfaces
    /// them on `GET /api/workflows` and filters with `?tag=`. Each tag is
    /// `[A-Za-z0-9_.-]`, ≤64 chars.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// Run-level wall-clock budget in seconds. When the run has been `running` longer than
    /// this, the engine's deadline sweep marks it `failed` and cancels its
    /// remaining tasks. `None` = no run-level deadline (per-task `timeout_secs`
    /// still applies). Must be ≥ 1 when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_timeout_secs: Option<u64>,
    /// Maximum number of **concurrently active runs** of this workflow (matched
    /// by name). When this many runs of the workflow are already `running`,
    /// `create_run` refuses to start another with a `MaxActiveRunsReached` error
    /// (parity fast-win #21 — Argo #12757 workflow concurrency control / Prefect
    /// deployment concurrency limits): the API returns 429, a queue source
    /// requeues the submission, and schedule/backfill fires are held back
    /// (backfill retries when a slot frees). `None`/`0` = unlimited. Enforced at
    /// run creation only — it caps concurrent runs, not per-task concurrency.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_active_runs: Option<u32>,
    /// Per-run resource budget (G-AG3). A run that would exceed it is refused at
    /// creation with a [`crate::models::TaskBudgetExceeded`] error, before
    /// anything executes.
    ///
    /// This exists because a scheduler called by an agent is an unbounded
    /// amplifier: one tool call can fan out to hundreds of tasks, and the first
    /// place anyone notices is the invoice. `budget:` is the author's ceiling on
    /// how big one run of this workflow is allowed to get.
    ///
    /// Distinct from [`Self::max_active_runs`], which caps how many runs of a
    /// workflow are in flight; this caps how big *one* of them may be. Also
    /// distinct from `DAGRON_MAX_TASKS_PER_RUN`, which is the operator's
    /// process-wide ceiling against an OOM: that one protects the engine from
    /// every workflow, this one lets a workflow's author say what *this* one is
    /// supposed to cost.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget: Option<RunBudget>,
    /// Soft SLA deadline. Unlike `run_timeout_secs`
    /// (which cancels), exceeding this only **emits an alert** — a
    /// `run.deadline_exceeded` outbox event + a metric — and leaves the run
    /// running. `None` = no deadline alert.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<DeadlineSpec>,
    /// Post-run notifications. Today: a `git` commit-status target so a run's
    /// result shows up as a check on the commit that triggered it (forge
    /// feedback). String fields accept `{{ param }}` templates, resolved against
    /// `parameters` when the notification fires.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notify: Option<NotifySpec>,
    /// Name of the task whose output becomes the *run's* result.
    /// When set, a succeeding run copies that task's output
    /// into `workflow_runs.output`, so a caller waiting on the run
    /// (`POST /runs?wait=true` / `GET /runs/{id}/wait`) gets a single return value
    /// — dagron as a durable function. The named task must exist and not be a
    /// hook. `None` = the run has no distinguished result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_from: Option<String>,
    /// Workflow-level default **runner class** applied to every task that does
    /// not set its own [`TaskSpec::runner_class`] — so an ETL workflow routes
    /// wholesale to the ETL runner pool with one line. `None` = `"default"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_class: Option<String>,
    /// Named **environment** (variable set + secrets, managed via the UI/API)
    /// this workflow runs against. Its variables become `{{ env.NAME }}`
    /// template references (merged under the workflow's own `parameters` at run
    /// creation), and its secrets are resolvable via
    /// `value_from: {secret: NAME}` at dispatch — so one spec runs against
    /// staging or prod by changing a single line.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment: Option<String>,
    /// Workflow-wide task defaults (the DRY block): every field set here is
    /// applied to each task that doesn't override it, so retries/timeouts/
    /// images/env don't have to be repeated on every task. See [`TaskDefaults`]
    /// for the exact merge rules.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_defaults: Option<TaskDefaults>,
    /// **Dataset triggers** (Airflow dataset scheduling / Dagster asset
    /// sensors): fire a run of this *registered* workflow when one of these
    /// datasets records a new update (a task with a matching `produces:`
    /// succeeded, or — where the feature is on — an external event was
    /// posted). Subscribe to one dataset, or to several and compose them with
    /// [`DagSpec::datasets_mode`]. Fires coalesce: updates that arrive while a
    /// fire is being processed produce one run, not one per event. Empty = not
    /// dataset-triggered.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on_datasets: Vec<String>,
    /// How multiple [`DagSpec::on_datasets`] entries combine:
    /// `"any"` — fire when any subscribed dataset updates (default); `"all"` —
    /// fire only once *every* subscribed dataset has updated since the last
    /// fire (the Airflow AND-of-datasets semantics, e.g. "refresh the join
    /// once both upstream tables landed"). Meaningless with a single dataset.
    ///
    /// `all` is the only construct with correct fan-in semantics. The
    /// alternative — trigger on one upstream and `wait: { dataset: … }` on the
    /// other — stamps the sensor's cursor when the task *parks*, so an upstream
    /// that refreshed before this run started does not satisfy it and the run
    /// hangs to its `run_timeout_secs` waiting for that dataset's next update.
    /// Reach for `all` whenever the arrival order of the upstreams is not
    /// guaranteed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datasets_mode: Option<String>,
    pub tasks: Vec<TaskSpec>,
}

/// `task_defaults:` — declared once, merged into every task (including tasks
/// inside `templates`). Merge rules, per field:
///
/// * Optional task fields (`timeout_secs`, `docker_image`, `runner_class`,
///   `retry_max_delay_secs`): the default applies only when the task leaves
///   the field unset.
/// * `max_attempts` / `retry_delay_secs`: the default applies when the task
///   uses the field's built-in default (1 / 0) — i.e. a task wins by writing
///   any explicit non-default value.
/// * `env`: default vars are **prepended**; a task var with the same name
///   shadows the default (last write wins at the executor).
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct TaskDefaults {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_delay_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_max_delay_secs: Option<u64>,
    /// DAG-wide default for [`TaskSpec::retry_on_timeout`]; applies to any task
    /// that does not set its own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_on_timeout: Option<bool>,
    /// DAG-wide [`TaskSpec::retry_budgets`]. Applies **per class**, not
    /// wholesale: a task that names `nan-loss` keeps its own number and still
    /// inherits the default's `gpu-ecc`. Written the other way — all-or-nothing
    /// — a task overriding one class would silently lose every other budget the
    /// workflow declared, which is precisely the failure this feature exists to
    /// stop.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub retry_budgets: std::collections::BTreeMap<String, u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docker_image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_class: Option<String>,
    /// DAG-wide dispatch priority default; applies to any task that leaves its
    /// own [`TaskSpec::priority`] at the built-in `0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<i64>,
    /// DAG-wide default concurrency [`TaskSpec::pool`] for tasks that set none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<EnvVar>,
}

/// Post-run notification targets (`notify:` block).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NotifySpec {
    /// Post a commit status / PR check to a Git forge on run finalization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git: Option<GitNotify>,
    /// POST a JSON event to an arbitrary HTTP endpoint on run finalization
    /// and/or soft-deadline breach.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook: Option<WebhookNotify>,
    /// Post a message to a Slack incoming webhook.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slack: Option<SlackNotify>,
}

/// A `notify.webhook` target. The engine POSTs
/// `{ "event", "run_id", "workflow", "status", "at" }` as JSON. `url` is
/// `{{ param }}`-templated like the git target's fields.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebhookNotify {
    pub url: String,
    /// Events that fire it: any of `succeeded`, `failed`, `cancelled`,
    /// `deadline_exceeded`. Empty (the default) = all of them.
    #[serde(default)]
    pub on: Vec<String>,
}

/// A `notify.slack` incoming-webhook target (the channel is fixed by the
/// webhook itself). `webhook_url` is `{{ param }}`-templated.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SlackNotify {
    pub webhook_url: String,
    /// Events that fire it. Empty (the default) = `failed` +
    /// `deadline_exceeded` only — chat channels want incidents, not every green
    /// run; list events explicitly (e.g. `[succeeded, failed]`) to widen it.
    #[serde(default)]
    pub on: Vec<String>,
}

/// A `notify.git` commit-status target. String fields are `{{ param }}`-templated.
///
/// Besides the workflow's own parameters, four `run.*` names resolve in these
/// fields, because the things an author most wants in a check are things only
/// the engine knows: `{{ run.id }}`, `{{ run.workflow }}`, `{{ run.status }}`
/// and `{{ run.images }}` (the distinct task images, first-appearance order).
/// The dot keeps them clear of ordinary parameter names; where one collides,
/// the engine's value wins, so a check cannot be made to report whatever a
/// caller put in a parameter.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GitNotify {
    /// `github` or `gitlab`.
    pub provider: String,
    /// GitHub `owner/repo`, or GitLab project path/id.
    pub repo: String,
    /// Commit SHA the status attaches to — usually `"{{ commit_sha }}"` from a
    /// parameter the CI caller supplies.
    pub sha: String,
    /// Status context/name shown on the commit (default `dagron`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    /// Optional link back to the run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_url: Option<String>,
    /// The one line of text the check shows. Absent (the default) keeps the
    /// engine's own "dagron run succeeded" wording.
    ///
    /// Worth setting when the run *produced* something the reviewer is looking
    /// for. A workflow whose image is built from a recipe is the case this was
    /// added for: `description: "built {{ run.images }}"` turns the check on
    /// the pull request that changed the recipe into the answer to what came
    /// out of it, instead of a pass/fail that says nothing about which image.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

pub struct DagGraph {
    pub spec: DagSpec,
    graph: DiGraph<String, ()>,
    node_index: HashMap<String, petgraph::graph::NodeIndex>,
}

impl DagGraph {
    /// Parse a workflow YAML, expand any `template:` calls into a flat leaf-only
    /// DAG (sub-workflows: recursion, fan-out, parameters), then build
    /// and validate the graph. This is the single entry point every submit path
    /// uses, so sub-workflow support is uniform across the API, cron, and ingest.
    pub fn from_yaml(yaml: &str) -> Result<Self> {
        let spec: DagSpec = serde_yaml::from_str(yaml)?;
        let spec = crate::expand::expand(spec)?;
        Self::from_spec(spec)
    }

    /// [`from_yaml`](Self::from_yaml) with parameter overrides merged in before
    /// expansion. This is how time-originated submits (cron, DB schedules,
    /// backfill) inject the fire's nominal time as `{{ scheduled_time }}`
    /// (RFC-3339) so tasks can reference their logical date — the
    /// data-interval idiom. Overrides win over declared defaults; keys
    /// the spec never references are harmless (unknown `{{ … }}` stays verbatim,
    /// unreferenced parameters are simply unused).
    pub fn from_yaml_with_params(
        yaml: &str,
        overrides: &BTreeMap<String, String>,
    ) -> Result<Self> {
        let mut spec: DagSpec = serde_yaml::from_str(yaml)?;
        for (k, v) in overrides {
            spec.parameters.insert(k.clone(), v.clone());
        }
        let spec = crate::expand::expand(spec)?;
        Self::from_spec(spec)
    }

    /// Build the graph from an already-expanded (leaf-only) [`DagSpec`].
    pub fn from_spec(spec: DagSpec) -> Result<Self> {
        let mut graph = DiGraph::new();
        let mut node_index = HashMap::new();

        if spec.run_timeout_secs == Some(0) {
            bail!("invalid run_timeout_secs=0 in DAG '{}'; expected >= 1 (or omit)", spec.name);
        }
        // The task budget is checked here rather than at dispatch, because here
        // it can be exact. `from_spec` runs *after* expansion, so `spec.tasks`
        // is the final list — templates inlined, `with_items` fanned out. A run
        // that would break its budget is therefore refused before a single task
        // exists, instead of being killed halfway through with work already
        // spent. Enforcing later would cost more and know less.
        if let Some(budget) = &spec.budget {
            match budget.tasks {
                Some(0) => {
                    bail!("invalid budget.tasks=0 in DAG '{}'; expected >= 1 (or omit)", spec.name)
                }
                Some(max) => {
                    // Count expanded task ROWS, not spec entries — the same
                    // gang-aware count `DagGraph::task_row_count` uses for
                    // admission. A `gang:` task is one spec that becomes `size`
                    // rows, so counting it as one would admit a run of N gangs
                    // against a budget of N while landing N × size tasks in the
                    // datastore, exactly the amplification the budget exists to
                    // cap.
                    let planned: u64 = spec
                        .tasks
                        .iter()
                        .map(|t| t.gang.as_ref().map(|g| g.size as u64).unwrap_or(1))
                        .sum();
                    if planned > u64::from(max) {
                        return Err(anyhow::Error::new(crate::models::TaskBudgetExceeded {
                            name: spec.name.clone(),
                            max,
                            planned,
                        }));
                    }
                }
                None => {}
            }

            // The external ceiling, checked here for the same reason and in the
            // same place as the task count: after expansion the sum is exact,
            // so a run that would break it is refused before one remote job is
            // submitted rather than killed halfway with cluster-hours spent.
            match budget.external_cost {
                Some(0) => bail!(
                    "invalid budget.external_cost=0 in DAG '{}'; expected >= 1 (or omit). \
                     A run that may spend nothing externally is a run with no `defer:` \
                     tasks — just omit them.",
                    spec.name
                ),
                Some(max) => {
                    // Gang-aware for the same reason the task count is: one
                    // `gang:` spec becomes `size` rows, and each of them submits.
                    // Counting the spec once would admit N gangs against a
                    // ceiling of N while launching N × size remote jobs, which
                    // is precisely the amplification this exists to cap.
                    let (planned, deferred_tasks) = spec
                        .tasks
                        .iter()
                        .filter(|t| t.defer.is_some())
                        .fold((0u64, 0u64), |(cost, n), t| {
                            let rows = t.gang.as_ref().map(|g| g.size as u64).unwrap_or(1);
                            let unit = t.defer.as_ref().and_then(|d| d.cost).unwrap_or(1);
                            (cost.saturating_add(unit.saturating_mul(rows)), n + rows)
                        });
                    if planned > max {
                        return Err(anyhow::Error::new(crate::models::ExternalBudgetExceeded {
                            name: spec.name.clone(),
                            max,
                            planned,
                            deferred_tasks,
                        }));
                    }
                }
                None => {}
            }

            // Cost attribution — reconciling a vendor's ACTUAL invoice back to
            // the run that caused it — is the gated capability. The ceiling
            // above is not, and will not become one: a guardrail an author sets
            // for themselves, over compute they already pay for, rations
            // nothing that belongs to anyone else. Refuse the attribution flag
            // with a signpost rather than letting serde drop it silently, which
            // is what would otherwise happen: `DagSpec` has no
            // `deny_unknown_fields`.
            if !cfg!(feature = "enterprise") && budget.external_cost_attribution {
                bail!(
                    "DAG '{}' requests external cost attribution: the ledger that reconciles \
                     each external job's actual vendor cost back to the run, workflow and team \
                     that launched it is not in this build — \
                     https://github.com/lucheeseng827/dagron#what-this-build-does-not-do. \
                     This build enforces the ceiling you declare yourself: set \
                     `budget: {{ external_cost: N }}` with `defer.cost` per task, and a run \
                     whose declared total exceeds N is refused before it submits anything. \
                     `pool:` caps concurrent remote jobs, `run_timeout_secs` bounds the run, \
                     and a cancelled run tears its remote jobs down \
                     (docs/EXTERNAL_JOBS.md). Reconciled cost reporting plugs in through the \
                     seams on dagron_engine::Seams.",
                    spec.name,
                );
            }
        }
        if let Some(d) = &spec.deadline {
            parse_duration_secs(&d.within)
                .map_err(|e| anyhow::anyhow!("invalid deadline in DAG '{}': {e}", spec.name))?;
        }
        if let Some(class) = &spec.runner_class {
            validate_runner_class(class)
                .map_err(|e| anyhow::anyhow!("invalid runner_class in DAG '{}': {e}", spec.name))?;
        }
        for tag in &spec.tags {
            validate_tag(tag)
                .map_err(|e| anyhow::anyhow!("invalid tag in DAG '{}': {e}", spec.name))?;
        }

        // Dataset triggers (`on_datasets:`): valid, deduplicated URIs, and a
        // well-formed `datasets_mode` when one is given.
        //
        // Multi-dataset composition used to be refused here with a signpost. It
        // is open now, and the reason is the fallback that signpost named: "keep
        // exactly one `on_datasets` entry … or split consumers into one workflow
        // per upstream dataset". In the canonical two-upstream mart that advice
        // is not merely lesser, it is WRONG — `park_wait_dataset` stamps its
        // cursor at park time, so a sensor only resolves on an update that lands
        // *after* it parks. An upstream that refreshed before the run started
        // never satisfies it, so the run waits for tomorrow's load and hangs to
        // its `run_timeout_secs`. `datasets_mode: all` does not have that race.
        //
        // A gate whose documented alternative loses data is a gate on
        // correctness, and correctness is the one thing this project does not
        // paywall. The composition itself was already implemented and tested in
        // the open tree on both backends (`claim_due_dataset_triggers` handles
        // `mode="all"`), so this was a refusal with nothing behind it.
        {
            let mut seen = std::collections::HashSet::new();
            for uri in &spec.on_datasets {
                validate_dataset_uri(uri).map_err(|e| {
                    anyhow::anyhow!("invalid on_datasets entry in DAG '{}': {e}", spec.name)
                })?;
                if !seen.insert(uri.as_str()) {
                    bail!("duplicate on_datasets entry '{uri}' in DAG '{}'", spec.name);
                }
            }
            if let Some(mode) = spec.datasets_mode.as_deref() {
                if !matches!(mode, "any" | "all") {
                    bail!(
                        "invalid datasets_mode '{mode}' in DAG '{}'; expected 'any' or 'all'",
                        spec.name
                    );
                }
                if spec.on_datasets.is_empty() {
                    bail!(
                        "datasets_mode set but on_datasets is empty in DAG '{}'",
                        spec.name
                    );
                }
            }
        }

        for task in &spec.tasks {
            if node_index.contains_key(&task.name) {
                bail!("duplicate task name '{}' in DAG '{}'", task.name, spec.name);
            }
            // A `retry_budgets` key that does not match a fault class *exactly*
            // never matches anything at runtime — the lookup is by
            // `FaultClass::as_str()`, so a key stored as `GPU_ECC` is a policy
            // the author wrote that silently does nothing, on the path that
            // decides whether to spend another thousand GPU-hours.
            //
            // Checking `FaultClass::parse(key) != Unknown` is *not* enough, and
            // that was the original bug here: `parse` is deliberately tolerant
            // (it lowercases and maps `_` to `-`) so it can read rows written by
            // other builds. Using a tolerant reader as a strict validator
            // accepts `GPU_ECC`, `Gpu-Ecc` and `canceled`, stores them verbatim,
            // and the runtime lookup then misses. Compare against the canonical
            // spelling instead — which also makes the `unknown` bucket fall out
            // for free, since `Unknown.as_str()` is `"unknown"`.
            for key in task.retry_budgets.keys() {
                let canonical = crate::fault::FaultClass::parse(key).as_str();
                if key != canonical {
                    let hint = if crate::fault::FaultClass::parse(key)
                        != crate::fault::FaultClass::Unknown
                    {
                        format!(" (did you mean '{canonical}'?)")
                    } else {
                        String::new()
                    };
                    bail!(
                        "unknown retry_budgets fault class '{}' on task '{}' in DAG '{}'{}; \
                         expected one of: {}",
                        key,
                        task.name,
                        spec.name,
                        hint,
                        crate::fault::FAULT_CLASS_NAMES.join(", ")
                    );
                }
            }
            if task.max_attempts == 0 {
                bail!(
                    "invalid max_attempts=0 for task '{}' in DAG '{}'; expected >= 1",
                    task.name,
                    spec.name
                );
            }
            if let Some(rule) = &task.trigger_rule {
                if !crate::models::TRIGGER_RULES.contains(&rule.as_str()) {
                    bail!(
                        "invalid trigger_rule '{}' for task '{}' in DAG '{}'; expected one of {:?}",
                        rule,
                        task.name,
                        spec.name,
                        crate::models::TRIGGER_RULES
                    );
                }
            }
            if let Some(hook) = &task.hook {
                if !HOOK_KINDS.contains(&hook.as_str()) {
                    bail!(
                        "invalid hook '{}' for task '{}' in DAG '{}'; expected one of {:?}",
                        hook,
                        task.name,
                        spec.name,
                        HOOK_KINDS
                    );
                }
            }
            // Approval-gate validation (#19).
            if let Some(kind) = &task.task_type {
                if !TASK_KINDS.contains(&kind.as_str()) {
                    bail!(
                        "invalid type '{}' for task '{}' in DAG '{}'; expected one of {:?}",
                        kind, task.name, spec.name, TASK_KINDS
                    );
                }
            }
            if let Some(action) = &task.approval_on_timeout {
                if !APPROVAL_TIMEOUT_ACTIONS.contains(&action.as_str()) {
                    bail!(
                        "invalid approval_on_timeout '{}' for task '{}' in DAG '{}'; expected one of {:?}",
                        action, task.name, spec.name, APPROVAL_TIMEOUT_ACTIONS
                    );
                }
            }
            if task.is_approval() && task.hook.is_some() {
                bail!("task '{}' cannot be both an approval gate and a hook in DAG '{}'", task.name, spec.name);
            }
            // Sub-workflow trigger (#23): needs a target workflow name, has no
            // command, and can't double as a hook. The `workflow:` field is only
            // meaningful on a `type: workflow` task.
            if task.is_workflow() {
                match task.workflow.as_deref() {
                    Some(w) if !w.trim().is_empty() => {}
                    _ => bail!(
                        "task '{}' is type: workflow but names no `workflow:` to trigger in DAG '{}'",
                        task.name, spec.name
                    ),
                }
                if !task.command.is_empty() {
                    bail!("task '{}' (type: workflow) must not set a command in DAG '{}'", task.name, spec.name);
                }
                if task.hook.is_some() {
                    bail!("task '{}' cannot be both a sub-workflow trigger and a hook in DAG '{}'", task.name, spec.name);
                }
            } else if task.workflow.is_some() {
                bail!(
                    "task '{}' sets `workflow:` but is not `type: workflow` in DAG '{}'",
                    task.name, spec.name
                );
            }
            // Deferrable wait sensor (#27): needs exactly one of wait.for /
            // wait.until / wait.url / wait.dataset, no command, no hook.
            // `wait:` is only for a type: wait task.
            if task.is_wait() {
                match &task.wait {
                    Some(w) => {
                        // Exactly one of for / until / url / dataset must be set.
                        let set = w.wait_for.is_some() as u8
                            + w.until.is_some() as u8
                            + w.url.is_some() as u8
                            + w.dataset.is_some() as u8;
                        if set != 1 {
                            bail!("task '{}' (type: wait) needs exactly one of wait.for / wait.until / wait.url / wait.dataset in DAG '{}'", task.name, spec.name);
                        }
                        if let Some(f) = &w.wait_for {
                            parse_duration_secs(f).map_err(|e| {
                                anyhow::anyhow!("invalid wait.for for task '{}' in DAG '{}': {e}", task.name, spec.name)
                            })?;
                        }
                        if let Some(u) = &w.until {
                            chrono::DateTime::parse_from_rfc3339(u).map_err(|e| {
                                anyhow::anyhow!("invalid wait.until (expected RFC-3339) for task '{}' in DAG '{}': {e}", task.name, spec.name)
                            })?;
                        }
                        if let Some(url) = &w.url {
                            let url = url.trim();
                            if !(url.starts_with("http://") || url.starts_with("https://")) {
                                bail!("invalid wait.url for task '{}' in DAG '{}': must be an http(s) URL", task.name, spec.name);
                            }
                        }
                        if let Some(ds) = &w.dataset {
                            validate_dataset_uri(ds).map_err(|e| {
                                anyhow::anyhow!("invalid wait.dataset for task '{}' in DAG '{}': {e}", task.name, spec.name)
                            })?;
                        }
                    }
                    None => bail!("task '{}' is type: wait but has no `wait:` block in DAG '{}'", task.name, spec.name),
                }
                if !task.command.is_empty() {
                    bail!("task '{}' (type: wait) must not set a command in DAG '{}'", task.name, spec.name);
                }
                if task.hook.is_some() {
                    bail!("task '{}' cannot be both a wait sensor and a hook in DAG '{}'", task.name, spec.name);
                }
            } else if task.wait.is_some() {
                bail!("task '{}' sets `wait:` but is not `type: wait` in DAG '{}'", task.name, spec.name);
            }
            // `produces:` — dataset updates are recorded wherever a producer task
            // can succeed, which is now three places: the worker result, a
            // memoization cache hit, and the external-job sweep that resolves a
            // `defer:` task. Approval gates, sub-workflow triggers and wait
            // sensors take none of those, so only command tasks may declare a
            // `produces:` and it is never silently dropped.
            //
            // A deferred task IS a command task, so it passes this guard — and
            // that is correct now rather than by accident: `resolve_external`
            // records through the same `record_produces` the other two paths
            // use. It was refused outright until that wiring existed.
            if !task.produces.is_empty() {
                if task.is_approval() || task.is_workflow() || task.is_wait() {
                    bail!(
                        "task '{}' (type: {}) cannot declare `produces:` in DAG '{}' — only command tasks record dataset updates",
                        task.name,
                        task.task_type.as_deref().unwrap_or("task"),
                        spec.name
                    );
                }
                let mut seen = std::collections::HashSet::new();
                for uri in &task.produces {
                    validate_dataset_uri(uri).map_err(|e| {
                        anyhow::anyhow!("invalid produces entry for task '{}' in DAG '{}': {e}", task.name, spec.name)
                    })?;
                    if !seen.insert(uri.as_str()) {
                        bail!("duplicate produces entry '{uri}' for task '{}' in DAG '{}'", task.name, spec.name);
                    }
                }
            }
            if let Some(class) = &task.runner_class {
                validate_runner_class(class).map_err(|e| {
                    anyhow::anyhow!(
                        "invalid runner_class for task '{}' in DAG '{}': {e}",
                        task.name,
                        spec.name
                    )
                })?;
            }
            if let Some(p) = &task.pool {
                validate_pool(p).map_err(|e| {
                    anyhow::anyhow!("invalid pool for task '{}' in DAG '{}': {e}", task.name, spec.name)
                })?;
            }
            if let Some(c) = &task.cache {
                if c.key.trim().is_empty() {
                    bail!("empty cache.key for task '{}' in DAG '{}'", task.name, spec.name);
                }
            }
            if let Some(iso) = &task.isolation {
                iso.validate().map_err(|e| {
                    anyhow::anyhow!("invalid isolation for task '{}' in DAG '{}': {e}", task.name, spec.name)
                })?;
            }
            // `resources.gpu` accelerator sugar: zero devices is a spec bug,
            // not a request.
            if let Some(gpu) = task.resources.as_ref().and_then(|r| r.gpu.as_ref()) {
                if gpu.count == 0 {
                    bail!(
                        "invalid resources.gpu.count=0 for task '{}' in DAG '{}'; expected >= 1 (or omit gpu)",
                        task.name,
                        spec.name
                    );
                }
                if gpu.resource.as_deref().is_some_and(|r| r.trim().is_empty()) {
                    bail!(
                        "empty resources.gpu.resource for task '{}' in DAG '{}'; omit it for the default ({})",
                        task.name,
                        spec.name,
                        DEFAULT_GPU_RESOURCE
                    );
                }
            }
            // `gang:` co-scheduling validation — leaf command tasks with
            // die-together (single-attempt) semantics only in v1.
            if let Some(gang) = &task.gang {
                if gang.size < 2 {
                    bail!(
                        "invalid gang.size={} for task '{}' in DAG '{}'; expected >= 2 (or omit gang)",
                        gang.size,
                        task.name,
                        spec.name
                    );
                }
                if task.max_attempts > 1 {
                    bail!(
                        "task '{}' cannot combine `gang` with retries (max_attempts > 1) in DAG '{}': a gang retries as a unit via run-level rerun, not per member",
                        task.name,
                        spec.name
                    );
                }
                if task.repeat.is_some() || task.is_approval() || task.template.is_some() {
                    bail!(
                        "task '{}' cannot combine `gang` with repeat/approval/template in DAG '{}'",
                        task.name,
                        spec.name
                    );
                }
                // A gang expands into member rows that each run a command; the
                // commandless kinds (sub-workflow trigger, wait sensor) resolve
                // through reconcile sweeps instead and have no meaning per-rank.
                // They are exempt from the command check, so reject them here.
                if task.is_workflow() || task.is_wait() {
                    bail!(
                        "task '{}' cannot combine `gang` with a sub-workflow trigger or wait sensor in DAG '{}'",
                        task.name,
                        spec.name
                    );
                }
                if spec.result_from.as_deref() == Some(task.name.as_str()) {
                    bail!(
                        "result_from cannot name gang task '{}' in DAG '{}' (members are '{}.<rank>')",
                        task.name,
                        spec.name,
                        task.name
                    );
                }
            }
            // `repeat:` loop-operator validation.
            if let Some(rep) = &task.repeat {
                if rep.until.trim().is_empty() {
                    bail!("task '{}' repeat.until is empty in DAG '{}'", task.name, spec.name);
                }
                if rep.max_iterations == 0 {
                    bail!(
                        "invalid repeat.max_iterations=0 for task '{}' in DAG '{}'; expected >= 1",
                        task.name,
                        spec.name
                    );
                }
                if rep.delay_secs > MAX_REPEAT_DELAY_SECS {
                    bail!(
                        "invalid repeat.delay_secs={} for task '{}' in DAG '{}'; expected <= {} (one year)",
                        rep.delay_secs,
                        task.name,
                        spec.name,
                        MAX_REPEAT_DELAY_SECS
                    );
                }
                // `repeat:` is only meaningful where something evaluates it
                // after a success. Two paths do: an executor reporting a
                // finished command, and the sub-workflow sweep resolving a
                // trigger whose child run went terminal — the second is what
                // makes a loop of child runs possible at all.
                //
                // The other two parked kinds have no iteration to speak of. An
                // approval is resolved by a person, and re-asking them until
                // they give the answer a condition wants is not a loop, it is
                // pestering. A wait sensor's whole job is to resolve once at a
                // deadline; `repeat` on it would mean "wait again", which is
                // what a longer `for:` already says.
                //
                // The rejection is kept rather than narrowed away because it
                // replaced a *silent* no-op: before the sweep learned `repeat`,
                // a loop on any of these succeeded once and said nothing, which
                // reads as a working workflow.
                if !matches!(task.task_type.as_deref(), None | Some("task") | Some("workflow")) {
                    bail!(
                        "task '{}' cannot combine `repeat` with `type: {}` in DAG '{}' \
                         — `repeat` applies to command tasks and sub-workflow triggers",
                        task.name,
                        task.task_type.as_deref().unwrap_or("task"),
                        spec.name
                    );
                }
            }
            // `defer:` — the external-job park shape.
            if let Some(def) = &task.defer {
                if def.kind.trim().is_empty() {
                    bail!("task '{}' defer.kind is empty in DAG '{}'", task.name, spec.name);
                }
                if def.poll_secs == 0 {
                    bail!(
                        "invalid defer.poll_secs=0 for task '{}' in DAG '{}'; expected >= 1 \
                         (a zero interval hot-loops the sweep against the remote API)",
                        task.name,
                        spec.name
                    );
                }
                if def.poll_secs > MAX_REPEAT_DELAY_SECS {
                    bail!(
                        "invalid defer.poll_secs={} for task '{}' in DAG '{}'; expected <= {} (one year)",
                        def.poll_secs,
                        task.name,
                        spec.name,
                        MAX_REPEAT_DELAY_SECS
                    );
                }
                // A ceiling below one poll interval can only ever be breached
                // before the first poll — the task would fail without the
                // remote job ever being looked at once.
                if let Some(max) = def.max_wait_secs {
                    if max < def.poll_secs {
                        bail!(
                            "invalid defer.max_wait_secs={} for task '{}' in DAG '{}'; must be >= \
                             defer.poll_secs ({}) — a shorter ceiling fails the task before its \
                             remote job is polled even once",
                            max,
                            task.name,
                            spec.name,
                            def.poll_secs
                        );
                    }
                }
                // Both are loop operators over one row, and they disagree about
                // what ends the loop: `repeat` re-runs the command on success,
                // `defer` parks on success and waits for a remote verdict. A row
                // carrying both would re-submit the job every time the poll said
                // "still running".
                if task.repeat.is_some() {
                    bail!(
                        "task '{}' cannot combine `defer` with `repeat` in DAG '{}' — both are \
                         loop operators: `repeat` re-runs the command after each success, `defer` \
                         parks on success until the remote job finishes. Poll with `defer` alone \
                         (defer.poll_secs), or drop `defer` and poll by re-running the command",
                        task.name,
                        spec.name
                    );
                }
                // The submit *is* the command, so the command-less kinds have
                // nothing to defer, and a sub-workflow trigger already parks on
                // its own child run.
                if !matches!(task.task_type.as_deref(), None | Some("task")) {
                    bail!(
                        "task '{}' cannot combine `defer` with `type: {}` in DAG '{}' — `defer` \
                         applies to command tasks: the command is the submit that names the \
                         remote job",
                        task.name,
                        task.task_type.as_deref().unwrap_or("task"),
                        spec.name
                    );
                }
                // A gang expands into N member rows running one command
                // all-or-nothing; a deferred member would park N rows on N
                // remote jobs with no all-or-nothing left anywhere. `gang`
                // already refuses `repeat` for the same reason.
                if task.gang.is_some() {
                    bail!(
                        "task '{}' cannot combine `defer` with `gang` in DAG '{}': a gang is N \
                         co-scheduled members of one command, and deferring makes each member \
                         park on its own remote job — the all-or-nothing the gang exists for is \
                         gone. Defer a single task that submits the parallel job instead",
                        task.name,
                        spec.name
                    );
                }
                // `defer.http:` — the built-in transport. Everything here is
                // checked at SUBMIT, which is the whole point: a typo in
                // `succeed_when` is otherwise a workflow that validates, starts
                // a six-hour job, and only then discovers it cannot read the
                // answer. That is the most expensive possible moment to find a
                // typo, and the cheapest possible check to run.
                if let Some(h) = &def.http {
                    let url = h.url.trim();
                    if url.is_empty() {
                        bail!("task '{}' defer.http.url is empty in DAG '{}'", task.name, spec.name);
                    }
                    // Scheme, not reachability. `{{ handle }}` and any
                    // `{{ param }}` that survived expansion are still in the
                    // string here, so anything stricter would reject URLs that
                    // are fine by the time they are fetched.
                    if !(url.starts_with("http://") || url.starts_with("https://")) {
                        bail!(
                            "task '{}' defer.http.url must be http(s) in DAG '{}' — got '{}'",
                            task.name,
                            spec.name,
                            url
                        );
                    }
                    for hdr in &h.headers {
                        if hdr.name.trim().is_empty() {
                            bail!(
                                "task '{}' has a defer.http header with no name in DAG '{}'",
                                task.name,
                                spec.name
                            );
                        }
                    }
                    let parse_pred = |field: &str, raw: &str| -> Result<()> {
                        crate::jsonpred::Predicate::parse(raw).map(|_| ()).map_err(|e| {
                            anyhow::anyhow!(
                                "task '{}' defer.http.{} in DAG '{}': {}",
                                task.name,
                                field,
                                spec.name,
                                e
                            )
                        })
                    };
                    if let Some(c) = &h.cancel {
                        let curl = c.url.trim();
                        if !(curl.starts_with("http://") || curl.starts_with("https://")) {
                            bail!(
                                "task '{}' defer.http.cancel.url must be http(s) in DAG '{}' — got '{}'",
                                task.name,
                                spec.name,
                                curl
                            );
                        }
                        let m = c.method();
                        if !CANCEL_METHODS.contains(&m.as_str()) {
                            bail!(
                                "task '{}' defer.http.cancel.method in DAG '{}' must be one of {} — got '{}'",
                                task.name,
                                spec.name,
                                CANCEL_METHODS.join(", "),
                                m
                            );
                        }
                    }
                    parse_pred("succeed_when", &h.succeed_when)?;
                    if let Some(f) = &h.fail_when {
                        parse_pred("fail_when", f)?;
                    }
                    if let Some(e) = &h.error_from {
                        crate::jsonpred::Path::parse(e).map_err(|err| {
                            anyhow::anyhow!(
                                "task '{}' defer.http.error_from in DAG '{}': {}",
                                task.name,
                                spec.name,
                                err
                            )
                        })?;
                    }
                }
                // `defer.connection:` — the governed endpoint registry.
                if !cfg!(feature = "enterprise") {
                    if let Some(conn) = def.connection.as_deref() {
                        bail!(
                            "task '{}' in DAG '{}' defers on connection '{}': named connections — \
                             one access-controlled, audited registry of compute and warehouse \
                             endpoints, with credentials the workflow never names and a record of \
                             which run used which endpoint — are not in this build — \
                             https://github.com/lucheeseng827/dagron#what-this-build-does-not-do. \
                             This build carries the endpoint with the run: put the host in the \
                             run's `environment:` variables and the credential in the task's env, \
                             resolved at dispatch and masked in task output (docs/CONFIG.md) — \
                             `env: [{{ name: SPARK_API, value: \"{{{{ env.SPARK_API }}}}\" }}, \
                             {{ name: SPARK_TOKEN, value_from: {{ secret: SPARK_TOKEN }} }}]`. \
                             One team with one cluster needs nothing else \
                             (docs/EXTERNAL_JOBS.md). Endpoint resolution plugs in via the \
                             ExternalPoller seam (dagron_engine::Seams).",
                            task.name,
                            spec.name,
                            conn
                        );
                    }
                }
            }
            // `arguments` has exactly two callees, and after expansion only one
            // of them can still be here: a template's are consumed inline, so
            // anything left belongs to a `type: workflow` trigger. Arguments
            // with nothing to pass them to are silently ignored otherwise, which
            // is the failure mode of a parameter that looks configured and is
            // not.
            if !task.arguments.is_empty() && !task.is_workflow() {
                bail!(
                    "task '{}' sets `arguments` with no `template` or `type: workflow` to pass them to in DAG '{}'",
                    task.name,
                    spec.name
                );
            }
            // After expansion every task must be a runnable leaf. A surviving
            // `template` or an empty `command` means expansion missed something.
            if task.template.is_some() {
                bail!(
                    "task '{}' still references template '{}' after expansion in DAG '{}'",
                    task.name,
                    task.template.as_deref().unwrap_or(""),
                    spec.name
                );
            }
            // A command is required for an ordinary task; an approval gate (waits
            // for a human), a sub-workflow trigger (runs a child workflow, #23),
            // and a wait sensor (defers on a timer, #27) have none, so they are exempt.
            if task.command.is_empty() && !task.is_approval() && !task.is_workflow() && !task.is_wait() {
                bail!(
                    "task '{}' has no command in DAG '{}' (a leaf task needs a command)",
                    task.name,
                    spec.name
                );
            }
            let idx = graph.add_node(task.name.clone());
            node_index.insert(task.name.clone(), idx);
        }

        // A hook task is a finalizer: nothing may depend on it (it is auto-wired
        // to depend on everything else). Catch a hand-written `depends_on: [hook]`.
        let hook_names: std::collections::HashSet<&str> =
            spec.tasks.iter().filter(|t| t.hook.is_some()).map(|t| t.name.as_str()).collect();
        for task in &spec.tasks {
            for dep in &task.depends_on {
                let &from = node_index
                    .get(dep)
                    .ok_or_else(|| anyhow::anyhow!("unknown dependency '{dep}' in task '{}'", task.name))?;
                if hook_names.contains(dep.as_str()) {
                    bail!("task '{}' cannot depend on hook task '{dep}'", task.name);
                }
                let &to = node_index.get(&task.name).unwrap();
                graph.add_edge(from, to, ());
            }
        }

        if is_cyclic_directed(&graph) {
            bail!("DAG '{}' contains a cycle", spec.name);
        }

        // A runtime `when` (the only `when:` form surviving expansion) may only
        // reference tasks it depends on — an output the gate is guaranteed to
        // have when readiness is evaluated.
        for task in &spec.tasks {
            if let Some(cond) = &task.when {
                for referenced in crate::expand::when_output_refs(cond) {
                    if !task.depends_on.contains(&referenced) {
                        bail!(
                            "task '{}' when references '{{{{ tasks.{referenced}.output }}}}' but does \
                             not depend on '{referenced}' in DAG '{}' — add it to depends_on",
                            task.name,
                            spec.name
                        );
                    }
                }
            }
        }

        // A runtime fan-out reads an upstream task's output, so it carries the
        // same rule as a runtime `when:` — and needs it more: `when:` only
        // gates a task, while this decides how many of it there are. Reading
        // the output of a task that has not necessarily run would make the
        // instance count depend on scheduling order.
        //
        // Only the *reference* rules live here. The shape rules (one fan-out
        // source, not on a call, not on a gang, command tasks only) are in
        // `expand.rs`, because this validator runs on the **expanded** graph:
        // by the time it sees a task that set both `with_items` and
        // `with_output_of`, the first has already fanned it out and is gone
        // from the leaf, so the conflict is invisible here.
        for task in &spec.tasks {
            let Some(producer) = &task.with_output_of else { continue };
            // Both checks are instance-aware, because this validator runs on
            // the EXPANDED graph: a producer that was itself fanned out no
            // longer exists under its authored name — expansion replaced
            // `regions` with `regions.0`, `regions.1`, … and rewired this
            // task's `depends_on` onto them. Comparing the authored name
            // against node names and dependency names directly is what used to
            // refuse a chained fan-out at submit.
            // The cheap check first, and the order is load-bearing rather than
            // stylistic. Dependencies were already proven to be real nodes
            // above ("unknown dependency"), so a non-empty result here proves
            // the producer exists — the scan below is only ever needed to
            // explain a failure. Reversed, every `with_output_of` task pays an
            // O(nodes) scan on the happy path, and the expanded graph runs to
            // `DAGRON_MAX_TASKS_PER_RUN` (100k), on the synchronous submit
            // path.
            if crate::expand::fanout_producer_rows(producer, &task.depends_on).is_empty() {
                // Nothing matched. Distinguish a typo from a real task this
                // one simply does not depend on, so the message names the
                // actual mistake — this is the error path, so the scan is free.
                let prefix = format!("{producer}.");
                let known = node_index.contains_key(producer)
                    || node_index.keys().any(|k| k.starts_with(&prefix));
                if !known {
                    bail!(
                        "task '{}' with_output_of names unknown task '{producer}' in DAG '{}'",
                        task.name,
                        spec.name
                    );
                }
                bail!(
                    "task '{}' fans out over '{producer}' output but does not depend on \
                     '{producer}' in DAG '{}' — add it to depends_on",
                    task.name,
                    spec.name
                );
            }
            if producer == &task.name {
                bail!(
                    "task '{}' cannot fan out over its own output in DAG '{}'",
                    task.name,
                    spec.name
                );
            }
        }

        // `result_from` must name a real, non-hook task (a hook is a finalizer, not
        // a result-bearing leaf) so the run's result is always well-defined.
        if let Some(rf) = &spec.result_from {
            if !node_index.contains_key(rf) {
                bail!("result_from names unknown task '{rf}' in DAG '{}'", spec.name);
            }
            if hook_names.contains(rf.as_str()) {
                bail!("result_from cannot name hook task '{rf}' in DAG '{}'", spec.name);
            }
        }

        Ok(Self { spec, graph, node_index })
    }

    /// Number of incoming edges (direct dependencies) for a task.
    pub fn dep_count(&self, task_name: &str) -> usize {
        let idx = self.node_index[task_name];
        self.graph
            .edges_directed(idx, petgraph::Direction::Incoming)
            .count()
    }

    pub fn task_spec(&self, task_name: &str) -> Option<&TaskSpec> {
        self.spec.tasks.iter().find(|t| t.name == task_name)
    }

    /// How many task ROWS `create_run` will insert for this graph.
    ///
    /// Not `spec.tasks.len()`: matrix and `template:` expansion has already
    /// happened by the time a `DagGraph` exists, but a `gang:` task is still one
    /// spec that becomes `size` rows (`<name>.<rank>`). Admission control counts
    /// what lands in the datastore, so it has to count the same way — otherwise a
    /// run of ten 64-member gangs is admitted as ten tasks and arrives as 640.
    pub fn task_row_count(&self) -> i64 {
        self.spec
            .tasks
            .iter()
            .map(|t| t.gang.as_ref().map(|g| g.size as i64).unwrap_or(1))
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runner-class validation: charset/length rules plus the reserved `other`
    /// (the metrics tail bucket) — a spec-level `other` would collide with the
    /// aggregated `runner_class="other"` series.
    #[test]
    fn runner_class_validation_rules() {
        // `resources.gpu` sugar folds into effective limits; explicit keys win.
        let gpu_yaml = r#"
name: gpu_sugar
tasks:
  - name: train
    command: ["python", "train.py"]
    resources: { gpu: { count: 2 } }
  - name: mig
    command: ["python", "infer.py"]
    resources:
      limits: { "nvidia.com/mig-1g.5gb": "4" }
      gpu: { count: 1, resource: "nvidia.com/mig-1g.5gb" }
"#;
        let g = DagGraph::from_yaml(gpu_yaml).expect("gpu sugar parses");
        let train = g.task_spec("train").unwrap().resources.as_ref().unwrap();
        assert_eq!(train.effective_limits().get("nvidia.com/gpu"), Some(&"2".to_string()));
        let mig = g.task_spec("mig").unwrap().resources.as_ref().unwrap();
        assert_eq!(
            mig.effective_limits().get("nvidia.com/mig-1g.5gb"),
            Some(&"4".to_string()),
            "an explicit limits entry outranks the sugar"
        );
        assert!(
            DagGraph::from_yaml(
                "name: g0\ntasks:\n  - name: t\n    command: [\"true\"]\n    resources: { gpu: { count: 0 } }\n"
            )
            .is_err(),
            "gpu.count=0 is rejected"
        );

        // `gang:` validation: size >= 2, leaf single-attempt tasks only.
        assert!(DagGraph::from_yaml(
            "name: g\ntasks:\n  - name: t\n    command: [\"true\"]\n    gang: { size: 4 }\n"
        )
        .is_ok());
        for bad in [
            "name: g\ntasks:\n  - name: t\n    command: [\"true\"]\n    gang: { size: 1 }\n",
            "name: g\ntasks:\n  - name: t\n    command: [\"true\"]\n    gang: { size: 2 }\n    max_attempts: 3\n",
            "name: g\ntasks:\n  - name: t\n    command: [\"true\"]\n    gang: { size: 2 }\n    repeat: { until: \"{{ output }} == done\", max_iterations: 3 }\n",
            "name: g\nresult_from: t\ntasks:\n  - name: t\n    command: [\"true\"]\n    gang: { size: 2 }\n",
        ] {
            assert!(DagGraph::from_yaml(bad).is_err(), "must reject: {bad}");
        }

        assert!(validate_runner_class("etl").is_ok());
        assert!(validate_runner_class("ml_training-2").is_ok());
        assert!(validate_runner_class("").is_err());
        assert!(validate_runner_class(&"x".repeat(65)).is_err());
        assert!(validate_runner_class("ETL").is_err());
        assert!(validate_runner_class("a,b").is_err());
        assert!(validate_runner_class("other").is_err(), "'other' is reserved");
        let err = DagGraph::from_yaml(
            "name: w\nrunner_class: other\ntasks:\n  - { name: a, command: [\"true\"] }\n",
        )
        .err()
        .expect("spec-level 'other' must be rejected")
        .to_string();
        assert!(err.contains("reserved"), "spec-level 'other' rejected: {err}");
    }

    #[test]
    fn workflow_tags_parse_and_validate() {
        // Valid tags round-trip onto the spec.
        let dag = DagGraph::from_yaml(
            "name: w\ntags: [etl, prod, team.data]\ntasks:\n  - { name: a, command: [\"true\"] }\n",
        )
        .unwrap();
        assert_eq!(dag.spec.tags, vec!["etl", "prod", "team.data"]);

        // Charset is enforced (a comma would break the URL/filter contract).
        assert!(validate_tag("etl").is_ok());
        assert!(validate_tag("team.data-1").is_ok());
        assert!(validate_tag("").is_err());
        assert!(validate_tag(&"x".repeat(65)).is_err());
        assert!(validate_tag("a b").is_err());
        assert!(validate_tag("a,b").is_err());

        // An invalid tag fails validation at spec load.
        let err = DagGraph::from_yaml(
            "name: w\ntags: [\"bad tag\"]\ntasks:\n  - { name: a, command: [\"true\"] }\n",
        )
        .err()
        .expect("invalid tag must be rejected")
        .to_string();
        assert!(err.contains("tag"), "invalid tag rejected: {err}");
    }

    #[test]
    fn subworkflow_trigger_validation() {
        // A valid trigger: type: workflow with a target and no command.
        let dag = DagGraph::from_yaml(
            "name: p\ntasks:\n  - { name: t, type: workflow, workflow: child }\n",
        )
        .unwrap();
        let t = dag.spec.tasks.iter().find(|t| t.name == "t").unwrap();
        assert!(t.is_workflow());
        assert_eq!(t.workflow.as_deref(), Some("child"));

        // type: workflow without a target is rejected.
        let err = DagGraph::from_yaml("name: p\ntasks:\n  - { name: t, type: workflow }\n")
            .err()
            .expect("missing workflow target must be rejected")
            .to_string();
        assert!(err.contains("workflow"), "target required: {err}");

        // `workflow:` on a non-workflow task is rejected.
        let err = DagGraph::from_yaml(
            "name: p\ntasks:\n  - { name: t, command: [\"true\"], workflow: child }\n",
        )
        .err()
        .expect("workflow field on an ordinary task must be rejected")
        .to_string();
        assert!(err.contains("workflow"), "misplaced workflow field: {err}");

        // A trigger must not also carry a command.
        let err = DagGraph::from_yaml(
            "name: p\ntasks:\n  - { name: t, type: workflow, workflow: c, command: [\"true\"] }\n",
        )
        .err()
        .expect("a trigger with a command must be rejected")
        .to_string();
        assert!(err.contains("command"), "trigger command rejected: {err}");
    }

    #[test]
    fn wait_sensor_validation() {
        // Valid: `for` and `until`.
        let dag = DagGraph::from_yaml(
            "name: p\ntasks:\n  - { name: w, type: wait, wait: { for: 30s } }\n",
        )
        .unwrap();
        assert!(dag.spec.tasks[0].is_wait());
        DagGraph::from_yaml(
            "name: p\ntasks:\n  - { name: w, type: wait, wait: { until: \"2030-01-01T00:00:00Z\" } }\n",
        )
        .unwrap();

        // Neither / both / missing block are rejected.
        assert!(DagGraph::from_yaml("name: p\ntasks:\n  - { name: w, type: wait, wait: {} }\n").is_err());
        assert!(DagGraph::from_yaml(
            "name: p\ntasks:\n  - { name: w, type: wait, wait: { for: 1s, until: \"2030-01-01T00:00:00Z\" } }\n"
        )
        .is_err());
        assert!(DagGraph::from_yaml("name: p\ntasks:\n  - { name: w, type: wait }\n").is_err());

        // Valid: `url` HTTP sensor (#27 follow-on).
        let dag = DagGraph::from_yaml(
            "name: p\ntasks:\n  - { name: w, type: wait, wait: { url: \"https://example.com/ready\" } }\n",
        )
        .unwrap();
        assert_eq!(
            dag.spec.tasks[0].wait.as_ref().unwrap().url.as_deref(),
            Some("https://example.com/ready")
        );

        // A non-http(s) `url` is rejected.
        let err = DagGraph::from_yaml(
            "name: p\ntasks:\n  - { name: w, type: wait, wait: { url: \"ftp://example.com\" } }\n",
        )
        .err()
        .expect("non-http url must be rejected")
        .to_string();
        assert!(err.contains("http"), "url scheme rejected: {err}");

        // `url` combined with `for`/`until` violates exactly-one.
        assert!(DagGraph::from_yaml(
            "name: p\ntasks:\n  - { name: w, type: wait, wait: { url: \"https://x/y\", for: 1s } }\n"
        )
        .is_err());

        // `wait:` on a non-wait task, and a wait task with a command, are rejected.
        assert!(DagGraph::from_yaml(
            "name: p\ntasks:\n  - { name: w, command: [\"true\"], wait: { for: 1s } }\n"
        )
        .is_err());
        assert!(DagGraph::from_yaml(
            "name: p\ntasks:\n  - { name: w, type: wait, wait: { for: 1s }, command: [\"true\"] }\n"
        )
        .is_err());
    }

    /// Dataset spec surface: `produces:` on tasks, the `wait.dataset` sensor,
    /// and `on_datasets:` triggers — plus the composition feature gate.
    #[test]
    fn dataset_spec_validation() {
        // Valid: produces on a command task; templates at expansion elsewhere.
        let dag = DagGraph::from_yaml(
            "name: p\ntasks:\n  - { name: t, command: [\"true\"], produces: [\"s3://lake/orders\"] }\n",
        )
        .unwrap();
        assert_eq!(dag.spec.tasks[0].produces, vec!["s3://lake/orders"]);

        // Invalid URIs and duplicates are rejected.
        assert!(DagGraph::from_yaml(
            "name: p\ntasks:\n  - { name: t, command: [\"true\"], produces: [\"has space\"] }\n"
        )
        .is_err());
        assert!(DagGraph::from_yaml(
            "name: p\ntasks:\n  - { name: t, command: [\"true\"], produces: [\"a://b\", \"a://b\"] }\n"
        )
        .is_err());

        // produces on a non-command task (its success bypasses the worker
        // result path) is rejected rather than silently dropped.
        assert!(DagGraph::from_yaml(
            "name: p\ntasks:\n  - { name: w, type: wait, wait: { for: 1s }, produces: [\"a://b\"] }\n"
        )
        .is_err());

        // Dataset sensor: valid alone, counted in the exactly-one-of rule.
        let dag = DagGraph::from_yaml(
            "name: p\ntasks:\n  - { name: w, type: wait, wait: { dataset: \"s3://lake/orders\" } }\n",
        )
        .unwrap();
        assert_eq!(
            dag.spec.tasks[0].wait.as_ref().unwrap().dataset.as_deref(),
            Some("s3://lake/orders")
        );
        assert!(DagGraph::from_yaml(
            "name: p\ntasks:\n  - { name: w, type: wait, wait: { dataset: \"a://b\", for: 1s } }\n"
        )
        .is_err());

        // Single-dataset trigger: fine in every edition.
        let dag = DagGraph::from_yaml(
            "name: p\non_datasets: [\"s3://lake/orders\"]\ntasks:\n  - { name: t, command: [\"true\"] }\n",
        )
        .unwrap();
        assert_eq!(dag.spec.on_datasets, vec!["s3://lake/orders"]);

        // Duplicate subscriptions and a bad mode are rejected everywhere.
        assert!(DagGraph::from_yaml(
            "name: p\non_datasets: [\"a://b\", \"a://b\"]\ntasks:\n  - { name: t, command: [\"true\"] }\n"
        )
        .is_err());
        assert!(DagGraph::from_yaml(
            "name: p\non_datasets: [\"a://b\"]\ndatasets_mode: sometimes\ntasks:\n  - { name: t, command: [\"true\"] }\n"
        )
        .is_err());
        // datasets_mode without subscriptions is meaningless.
        assert!(DagGraph::from_yaml(
            "name: p\ndatasets_mode: any\ntasks:\n  - { name: t, command: [\"true\"] }\n"
        )
        .is_err());

        // Composition (multi-dataset + datasets_mode) used to be refused here
        // in the open build. It is accepted in EVERY build now, and this
        // assertion is deliberately not `cfg`-split: the gate is gone, not
        // moved, so an open build and an enterprise build must agree.
        //
        // Restoring the gate would put a race on the paid line. The fallback it
        // used to recommend — one upstream on the trigger, the other on a
        // `wait: { dataset: … }` sensor — stamps the sensor's cursor at park
        // time, so an upstream that landed before the run started never
        // satisfies it. `all` is the only construct that gets that case right.
        let multi = "name: p\non_datasets: [\"a://b\", \"a://c\"]\ndatasets_mode: all\ntasks:\n  - { name: t, command: [\"true\"] }\n";
        let dag = DagGraph::from_yaml(multi).expect("multi-dataset composition is open");
        assert_eq!(dag.spec.on_datasets.len(), 2);
        assert_eq!(dag.spec.datasets_mode.as_deref(), Some("all"));
        // `any` over several datasets is open too — the gate covered both.
        let any = "name: p\non_datasets: [\"a://b\", \"a://c\"]\ntasks:\n  - { name: t, command: [\"true\"] }\n";
        assert!(DagGraph::from_yaml(any).is_ok(), "multi-dataset any-of is open");
    }

    /// The sweep-side subscription extraction reads raw specs without expansion.
    #[test]
    fn dataset_subscriptions_extraction() {
        assert_eq!(
            dataset_subscriptions("name: p\non_datasets: [\"a://b\"]\ntasks: []\n"),
            Some((vec!["a://b".to_string()], "any".to_string()))
        );
        assert_eq!(
            dataset_subscriptions(
                "name: p\non_datasets: [\"a://b\", \"a://c\"]\ndatasets_mode: all\ntasks: []\n"
            ),
            Some((vec!["a://b".to_string(), "a://c".to_string()], "all".to_string()))
        );
        // No subscriptions, or an unparseable spec → None.
        assert_eq!(dataset_subscriptions("name: p\ntasks: []\n"), None);
        assert_eq!(dataset_subscriptions("{{ not yaml"), None);
    }

    #[test]
    fn run_timeout_zero_is_rejected() {
        let err = DagGraph::from_yaml(
            "name: w\nrun_timeout_secs: 0\ntasks:\n  - { name: a, command: [\"true\"] }\n",
        )
        .err()
        .expect("run_timeout_secs=0 must be rejected")
        .to_string();
        assert!(err.contains("run_timeout_secs=0"), "got: {err}");
    }

    /// A runtime fan-out reads an upstream task's output, so the producer has
    /// to be a dependency — not as a nicety, but because otherwise the number
    /// of instances depends on scheduling order. Same rule a runtime `when:`
    /// output reference already carries; this one needs it more, because
    /// `when:` only decides whether a task runs and this decides how many of
    /// it there are.
    #[test]
    fn a_runtime_fan_out_must_depend_on_the_task_it_reads() {
        let ok = DagGraph::from_yaml(
            "name: w\ntasks:\n\
             \x20 - { name: list, command: [\"ls\"] }\n\
             \x20 - { name: use, command: [\"x\"], depends_on: [list], with_output_of: list }\n",
        );
        assert!(ok.is_ok(), "a dependency is a valid producer: {:?}", ok.err());

        let err = DagGraph::from_yaml(
            "name: w\ntasks:\n\
             \x20 - { name: list, command: [\"ls\"] }\n\
             \x20 - { name: use, command: [\"x\"], with_output_of: list }\n",
        )
        .err()
        .expect("a producer that is not a dependency must be refused")
        .to_string();
        assert!(err.contains("depends_on"), "the error says how to fix it: {err}");

        let err = DagGraph::from_yaml(
            "name: w\ntasks:\n\
             \x20 - { name: use, command: [\"x\"], with_output_of: ghost }\n",
        )
        .err()
        .expect("an unknown producer must be refused")
        .to_string();
        assert!(err.contains("unknown task 'ghost'"), "got: {err}");
    }

    /// The shapes a runtime fan-out cannot take. Each of these would otherwise
    /// be a spec that submits cleanly and then does something other than what
    /// it says — the failure mode every guard in this file exists for.
    #[test]
    fn a_runtime_fan_out_is_refused_where_it_could_not_work() {
        let cases: [(&str, &str, &str); 4] = [
            // Two fan-out sources: one resolved at submit, one mid-run.
            (
                "one source",
                "  - { name: list, command: [\"ls\"] }\n  - { name: use, command: [\"x\"], depends_on: [list], with_output_of: list, with_items: [1, 2] }",
                "one source",
            ),
            // A call is replaced by the template's tasks; its own fields go
            // with it, so there would be no row to fan out.
            (
                "template call",
                "  - { name: list, command: [\"ls\"] }\n  - { name: use, template: t, depends_on: [list], with_output_of: list }",
                "template",
            ),
            // A gang's size has to be known before it is claimed.
            (
                "gang",
                "  - { name: list, command: [\"ls\"] }\n  - { name: use, command: [\"x\"], depends_on: [list], with_output_of: list, gang: { size: 2 } }",
                "gang",
            ),
            // Nothing to substitute `{{ item }}` into.
            (
                "wait sensor",
                "  - { name: list, command: [\"ls\"] }\n  - { name: use, type: wait, wait: { for: 5m }, depends_on: [list], with_output_of: list }",
                "with_output_of",
            ),
        ];
        for (what, tasks, needle) in cases {
            let yaml = format!(
                "name: w\ntemplates:\n  - name: t\n    tasks:\n      - {{ name: inner, command: [\"true\"] }}\ntasks:\n{tasks}\n"
            );
            let err = DagGraph::from_yaml(&yaml)
                .err()
                .unwrap_or_else(|| panic!("with_output_of on a {what} must be rejected"))
                .to_string();
            assert!(err.contains(needle), "{what}: the error should name it, got: {err}");
        }
    }

    /// `repeat:` is evaluated only where an executor's result comes back. Every
    /// other task kind succeeds through a sweep that never consults it, so a
    /// `repeat:` there used to be a silent no-op — the workflow read as a loop
    /// and ran once. Saying no out loud is the only honest answer until the
    /// sweep paths learn the operator.
    #[test]
    fn repeat_is_rejected_on_task_kinds_that_never_evaluate_it() {
        let cases = [
            ("wait", "  - { name: a, type: wait, wait: { for: 5m }, repeat: { until: \"{{ output }} == done\", max_iterations: 3 } }"),
            ("approval", "  - { name: a, type: approval, repeat: { until: \"{{ output }} == done\", max_iterations: 3 } }"),
        ];
        for (kind, task) in cases {
            let err = DagGraph::from_yaml(&format!("name: w\ntasks:\n{task}\n"))
                .err()
                .unwrap_or_else(|| panic!("repeat on type: {kind} must be rejected"))
                .to_string();
            assert!(
                err.contains("repeat") && err.contains(kind),
                "the error should name both the operator and the kind, got: {err}"
            );
        }
    }

    #[test]
    fn a_satisfied_condition_ends_the_loop() {
        let rep = RepeatSpec {
            until: "{{ output }} == done".into(),
            max_iterations: 5,
            delay_secs: 0,
        };
        assert_eq!(rep.decide("done", 1), RepeatDecision::Done);
        // Trimmed before comparison — a command's output almost always ends in
        // a newline, and a loop that never matched because of one would be
        // maddening to debug.
        assert_eq!(rep.decide("done\n", 3), RepeatDecision::Done);
    }

    #[test]
    fn an_unsatisfied_condition_asks_for_another_iteration() {
        let rep = RepeatSpec {
            until: "{{ output }} == done".into(),
            max_iterations: 5,
            delay_secs: 7,
        };
        assert_eq!(rep.decide("continue", 1), RepeatDecision::Again { delay_secs: 7 });
        assert_eq!(rep.decide("continue", 4), RepeatDecision::Again { delay_secs: 7 });
    }

    /// Running out of iterations is a **failure**. A condition that never came
    /// true is an error, and calling it success hands the next task a result
    /// the loop never reached.
    #[test]
    fn exhausting_the_iterations_fails_and_says_what_it_last_saw() {
        let rep = RepeatSpec {
            until: "{{ output }} == done".into(),
            max_iterations: 5,
            delay_secs: 0,
        };
        let RepeatDecision::Fail { reason } = rep.decide("still going", 5) else {
            panic!("the last iteration must fail, not repeat forever");
        };
        assert!(reason.contains("not satisfied after 5 iterations"), "got: {reason}");
        assert!(reason.contains("still going"), "the last output has to be in the reason");
    }

    /// A condition the grammar cannot evaluate fails the task rather than
    /// looping until the budget runs out — the spec is wrong, and thirty-nine
    /// more attempts will not make it right.
    #[test]
    fn an_unevaluable_condition_fails_immediately() {
        let rep = RepeatSpec {
            until: "{{ output }} >< done".into(),
            max_iterations: 40,
            delay_secs: 0,
        };
        let d = rep.decide("anything", 1);
        assert!(
            matches!(&d, RepeatDecision::Fail { reason } if reason.contains("failed to evaluate")),
            "got: {d:?}"
        );
    }

    /// A trigger's `arguments` survive expansion — they are the child run's
    /// parameters, and the child does not exist until dispatch.
    #[test]
    fn sub_workflow_arguments_survive_expansion_and_resolve_the_callers_scope() {
        let g = DagGraph::from_yaml(
            "name: p\nparameters: { conversation: c-42 }\ntasks:\n  - { name: turn, type: workflow, workflow: agent-turn, arguments: { conversation: \"{{ conversation }}\", fixed: literal } }\n",
        )
        .expect("arguments on a trigger are allowed");
        let args = &g.spec.tasks[0].arguments;
        assert_eq!(
            args.get("conversation").map(String::as_str),
            Some("c-42"),
            "the caller's scope resolves, or a trigger could only pass constants"
        );
        assert_eq!(args.get("fixed").map(String::as_str), Some("literal"));
    }

    /// A template's arguments are consumed inline, so nothing survives on the
    /// leaf — the two callees share a field, not a lifetime.
    #[test]
    fn template_arguments_do_not_survive_expansion() {
        let g = DagGraph::from_yaml(
            "name: p\ntemplates:\n  - name: say\n    parameters: { who: world }\n    tasks:\n      - { name: hello, command: [\"echo\", \"{{ who }}\"] }\ntasks:\n  - { name: greet, template: say, arguments: { who: dagron } }\n",
        )
        .expect("a template call expands");
        assert!(
            g.spec.tasks.iter().all(|t| t.arguments.is_empty()),
            "a template's arguments are consumed, not carried"
        );
    }

    // ── budget.external_cost ────────────────────────────────────────────────

    const DEFER: &str = "defer: { kind: spark-k8s }";

    /// The ceiling bounds a plain count of submissions when nobody declares a
    /// unit cost — `defer.cost` defaults to 1.
    #[test]
    fn the_external_ceiling_counts_submissions_by_default() {
        let yaml = format!(
            "name: p\nbudget: {{ external_cost: 2 }}\ntasks:\n  \
             - {{ name: a, command: [x], {DEFER} }}\n  \
             - {{ name: b, command: [x], {DEFER} }}\n"
        );
        DagGraph::from_yaml(&yaml).expect("two submits against a ceiling of two is admitted");

        let over = format!(
            "name: p\nbudget: {{ external_cost: 2 }}\ntasks:\n  \
             - {{ name: a, command: [x], {DEFER} }}\n  \
             - {{ name: b, command: [x], {DEFER} }}\n  \
             - {{ name: c, command: [x], {DEFER} }}\n"
        );
        let err = DagGraph::from_yaml(&over).err().expect("three is over");
        let b = err
            .downcast_ref::<crate::models::ExternalBudgetExceeded>()
            .expect("typed, so the API can answer 400 rather than calling it a parse error");
        assert_eq!(b.planned, 3);
        assert_eq!(b.deferred_tasks, 3);
        assert_eq!(b.max, 2);
    }

    /// A declared unit cost is what makes the field a *cost* rather than a
    /// count: two expensive jobs can exceed a ceiling fifty cheap ones fit under.
    #[test]
    fn a_declared_unit_cost_is_summed_not_counted() {
        let yaml = "name: p\nbudget: { external_cost: 100 }\ntasks:\n  \
             - { name: big,      command: [x], defer: { kind: spark-k8s, cost: 60 } }\n  \
             - { name: also_big, command: [x], defer: { kind: spark-k8s, cost: 60 } }\n";
        let err = DagGraph::from_yaml(yaml).err().expect("120 > 100");
        let b = err.downcast_ref::<crate::models::ExternalBudgetExceeded>().expect("typed");
        assert_eq!(b.planned, 120, "summed, not counted");
        assert_eq!(b.deferred_tasks, 2, "and it says how many jobs made up that sum");
    }

    /// Only deferred tasks count. A run of a thousand `echo`s spends nothing
    /// externally and must not be refused by a ceiling on external work.
    #[test]
    fn ordinary_tasks_do_not_consume_the_external_ceiling() {
        let yaml = "name: p\nbudget: { external_cost: 1 }\ntasks:\n  \
             - { name: a, command: [x] }\n  \
             - { name: b, command: [x] }\n  \
             - { name: c, command: [x], defer: { kind: spark-k8s } }\n";
        DagGraph::from_yaml(yaml).expect("local work is free of this ceiling");
    }

    /// The amplification the ceiling exists to stop.
    ///
    /// One `gang:` spec becomes `size` rows and each one submits. Counting the
    /// spec once would admit a run against a ceiling of 2 while launching 20
    /// remote jobs — which is the whole failure mode, not an edge case. The
    /// task-count budget is gang-aware for exactly this reason and this must
    /// match it.
    #[test]
    fn a_gang_costs_its_size_not_one() {
        let yaml = "name: p\nbudget: { external_cost: 2 }\ntasks:\n  \
             - { name: fan, command: [x], gang: { size: 10 }, defer: { kind: spark-k8s } }\n";
        let err = DagGraph::from_yaml(yaml).err().expect("ten submits is over a ceiling of two");
        let b = err.downcast_ref::<crate::models::ExternalBudgetExceeded>().expect("typed");
        assert_eq!(b.planned, 10, "size rows, not one spec");
        assert_eq!(b.deferred_tasks, 10);
    }

    /// And the unit cost multiplies across the gang, rather than applying once.
    #[test]
    fn a_gangs_unit_cost_multiplies_across_its_rows() {
        let yaml = "name: p\nbudget: { external_cost: 100 }\ntasks:\n  \
             - { name: fan, command: [x], gang: { size: 4 }, defer: { kind: spark-k8s, cost: 30 } }\n";
        let err = DagGraph::from_yaml(yaml).err().expect("4 x 30 = 120 > 100");
        let b = err.downcast_ref::<crate::models::ExternalBudgetExceeded>().expect("typed");
        assert_eq!(b.planned, 120);
    }

    #[test]
    fn an_external_ceiling_of_zero_is_refused_as_meaningless() {
        let err = DagGraph::from_yaml("name: p\nbudget: { external_cost: 0 }\ntasks:\n  - { name: a, command: [x] }\n")
            .err()
            .expect("zero is not a ceiling, it is a contradiction")
            .to_string();
        assert!(err.contains("external_cost=0"), "{err}");
    }

    /// Cost *attribution* — reconciling a real vendor invoice — is the gated
    /// capability. The declared ceiling above is not, and the signpost has to
    /// say so: otherwise the refusal reads as "budgets are Enterprise", which is
    /// the opposite of the rule that a guardrail you set for yourself is never
    /// the thing sold.
    #[cfg(not(feature = "enterprise"))]
    #[test]
    fn attribution_is_gated_and_the_signpost_names_the_open_ceiling() {
        let err = DagGraph::from_yaml(
            "name: p\nbudget: { external_cost_attribution: true }\ntasks:\n  - { name: a, command: [x] }\n",
        )
        .err()
        .expect("attribution is not in this build")
        .to_string();
        assert!(err.contains("not in this build"), "names the gap: {err}");
        assert!(err.contains("#what-this-build-does-not-do"), "links the anchor: {err}");
        assert!(err.contains("external_cost"), "names the OPEN ceiling, so the refusal is not read as 'budgets are paid': {err}");
        assert!(err.contains("`pool:`"), "names the other open bound: {err}");
        assert!(err.contains("docs/EXTERNAL_JOBS.md"), "names the open doc: {err}");
        assert!(err.contains("dagron_engine::Seams"), "names the seam: {err}");
    }

    /// An enterprise build accepts it — the gate is a gate, not a removal.
    #[cfg(feature = "enterprise")]
    #[test]
    fn attribution_is_accepted_where_it_ships() {
        DagGraph::from_yaml(
            "name: p\nbudget: { external_cost_attribution: true }\ntasks:\n  - { name: a, command: [x] }\n",
        )
        .expect("an enterprise build accepts attribution");
    }

    /// `arguments` with nothing to pass them to is a mistake, and a silent one
    /// if it is allowed through: the values look configured and go nowhere.
    #[test]
    fn arguments_with_no_callee_are_rejected() {
        let err = DagGraph::from_yaml(
            "name: p\ntasks:\n  - { name: a, command: [\"true\"], arguments: { x: \"1\" } }\n",
        )
        .err()
        .expect("arguments on a plain command task must be rejected")
        .to_string();
        assert!(err.contains("`arguments`"), "got: {err}");
    }

    /// The durable agent loop: a sub-workflow trigger that repeats. Each
    /// iteration is a child run, which is what makes a conversation
    /// inspectable turn by turn.
    #[test]
    fn repeat_is_allowed_on_a_sub_workflow_trigger() {
        let g = DagGraph::from_yaml(
            "name: conversation\ntasks:\n  - { name: turn, type: workflow, workflow: agent-turn, repeat: { until: \"{{ output }} == done\", max_iterations: 40 } }\n",
        )
        .expect("a loop over child runs is the whole point");
        assert!(g.spec.tasks[0].repeat.is_some());
        assert!(g.spec.tasks[0].is_workflow());
    }

    #[test]
    fn repeat_is_still_allowed_on_an_ordinary_command_task() {
        DagGraph::from_yaml(
            "name: w\ntasks:\n  - { name: a, command: [\"true\"], repeat: { until: \"{{ output }} == done\", max_iterations: 3 } }\n",
        )
        .expect("the common case must keep working");
    }

    #[test]
    fn a_run_over_its_task_budget_is_refused_before_anything_runs() {
        // 3 tasks, budget of 2. The refusal happens at graph construction, which
        // is after expansion and before a single row is written.
        let err = DagGraph::from_yaml(
            "name: w\nbudget: { tasks: 2 }\ntasks:\n  - { name: a, command: [\"true\"] }\n  - { name: b, command: [\"true\"] }\n  - { name: c, command: [\"true\"] }\n",
        )
        .err()
        .expect("over budget must be refused");
        let b = err
            .downcast_ref::<crate::models::TaskBudgetExceeded>()
            .expect("a budget refusal must be typed, not a generic parse error");
        assert_eq!((b.max, b.planned), (2, 3));
    }

    /// The count is taken *after* expansion, which is the only place it is
    /// honest: a two-line spec with `with_items` is one task on the page and
    /// many in the database, and budgeting the page would budget nothing.
    #[test]
    fn the_budget_counts_expanded_fan_out_not_authored_tasks() {
        let yaml = "name: w\nbudget: { tasks: 2 }\ntasks:\n  - { name: a, command: [\"echo\", \"{{ item }}\"], with_items: [1, 2, 3] }\n";
        let err = DagGraph::from_yaml(yaml).err().expect("fan-out of 3 breaks a budget of 2");
        let b = err.downcast_ref::<crate::models::TaskBudgetExceeded>().expect("typed");
        assert_eq!(b.planned, 3, "the budget sees the tasks that would actually exist");
    }

    /// A `gang:` task is one line in the spec but `size` rows in the datastore,
    /// exactly like `with_items`. The budget must count the rows — otherwise a
    /// single gang spec of `size` sails past a budget of 1 and lands `size`
    /// tasks. This is the case `task_row_count` already counts for admission.
    #[test]
    fn the_budget_counts_gang_members_not_the_gang_spec() {
        let yaml = "name: w\nbudget: { tasks: 2 }\ntasks:\n  - { name: a, command: [\"true\"], gang: { size: 4 } }\n";
        let err = DagGraph::from_yaml(yaml).err().expect("a 4-member gang breaks a budget of 2");
        let b = err.downcast_ref::<crate::models::TaskBudgetExceeded>().expect("typed");
        assert_eq!(b.planned, 4, "the budget sees the gang members that would actually exist");
    }

    #[test]
    fn a_run_within_its_budget_is_built_normally() {
        let g = DagGraph::from_yaml(
            "name: w\nbudget: { tasks: 5 }\ntasks:\n  - { name: a, command: [\"true\"] }\n",
        )
        .expect("under budget");
        assert_eq!(g.spec.tasks.len(), 1);
    }

    /// A budget of zero admits nothing, so it is a typo rather than a policy.
    /// Rejecting it matches how `run_timeout_secs: 0` is treated.
    #[test]
    fn a_zero_task_budget_is_rejected_as_a_mistake() {
        let err = DagGraph::from_yaml(
            "name: w\nbudget: { tasks: 0 }\ntasks:\n  - { name: a, command: [\"true\"] }\n",
        )
        .err()
        .expect("budget.tasks=0 must be rejected")
        .to_string();
        assert!(err.contains("budget.tasks=0"), "got: {err}");
    }

    /// No `budget:` is the overwhelmingly common case and must stay free of any
    /// new behaviour at all.
    #[test]
    fn a_spec_without_a_budget_is_unaffected() {
        let g = DagGraph::from_yaml("name: w\ntasks:\n  - { name: a, command: [\"true\"] }\n")
            .expect("no budget declared");
        assert!(g.spec.budget.is_none());
    }

    #[test]
    fn run_timeout_survives_expansion() {
        let g = DagGraph::from_yaml(
            "name: w\nrun_timeout_secs: 90\ntasks:\n  - { name: a, command: [\"true\"] }\n",
        )
        .unwrap();
        assert_eq!(g.spec.run_timeout_secs, Some(90));
    }

    #[test]
    fn params_override_injects_scheduled_time() {
        // A time-originated submit (cron/schedule/backfill) merges overrides in;
        // declared defaults lose, and {{ scheduled_time }} resolves in any field.
        let yaml = "name: w\nparameters: { scheduled_time: \"unset\", keep: \"k\" }\ntasks:\n  - { name: a, command: [\"echo\", \"{{ scheduled_time }}\", \"{{ keep }}\"] }\n";
        let mut overrides = BTreeMap::new();
        overrides.insert("scheduled_time".to_string(), "2026-07-07T00:00:00+00:00".to_string());
        let g = DagGraph::from_yaml_with_params(yaml, &overrides).unwrap();
        assert_eq!(
            g.task_spec("a").unwrap().command,
            vec!["echo", "2026-07-07T00:00:00+00:00", "k"]
        );
    }

    #[test]
    fn duration_parser_units_and_errors() {
        assert_eq!(parse_duration_secs("90s").unwrap(), 90);
        assert_eq!(parse_duration_secs("45m").unwrap(), 2700);
        assert_eq!(parse_duration_secs("2h").unwrap(), 7200);
        assert_eq!(parse_duration_secs("1d").unwrap(), 86_400);
        assert_eq!(parse_duration_secs("120").unwrap(), 120); // bare = seconds
        assert!(parse_duration_secs("0").is_err());
        assert!(parse_duration_secs("").is_err());
        assert!(parse_duration_secs("abc").is_err());
    }

    #[test]
    fn retry_max_delay_survives_expansion() {
        let yaml = "name: w\ntasks:\n  - { name: a, command: [\"true\"], max_attempts: 5, retry_delay_secs: 3, retry_max_delay_secs: 10 }\n";
        let g = DagGraph::from_yaml(yaml).unwrap();
        assert_eq!(g.task_spec("a").unwrap().retry_max_delay_secs, Some(10));
    }

    #[test]
    fn notify_git_survives_expansion_and_resolves_from_params() {
        let yaml = "name: ci\nparameters: { commit_sha: abc123 }\n\
                    notify:\n  git:\n    provider: github\n    repo: acme/etl\n    sha: \"{{ commit_sha }}\"\n    context: dagron/ci\n\
                    tasks:\n  - { name: a, command: [\"true\"] }\n";

        // (1) The block survives parse + expand (the run's stored graph keeps it).
        let expanded = DagGraph::from_yaml(yaml).unwrap();
        assert!(expanded.spec.notify.and_then(|n| n.git).is_some());

        // (2) The engine reads the *original* YAML (params intact) at finalize and
        // resolves the templated sha against them — mirror that path here.
        let raw: DagSpec = serde_yaml::from_str(yaml).unwrap();
        let git = raw.notify.as_ref().and_then(|n| n.git.as_ref()).unwrap();
        assert_eq!(git.provider, "github");
        assert_eq!(crate::expand::substitute(&git.sha, &raw.parameters), "abc123");
    }

    #[test]
    fn result_from_survives_expansion_and_is_validated() {
        // (1) A valid result_from survives parse + expand.
        let ok = DagGraph::from_yaml(
            "name: w\nresult_from: b\ntasks:\n  - { name: a, command: [\"true\"] }\n  - { name: b, command: [\"true\"], depends_on: [\"a\"] }\n",
        )
        .unwrap();
        assert_eq!(ok.spec.result_from.as_deref(), Some("b"));

        // (2) result_from naming an unknown task is rejected.
        let err = DagGraph::from_yaml(
            "name: w\nresult_from: nope\ntasks:\n  - { name: a, command: [\"true\"] }\n",
        )
        .err()
        .expect("unknown result_from must be rejected")
        .to_string();
        assert!(err.contains("result_from names unknown task 'nope'"), "got: {err}");

        // (3) result_from naming a hook task is rejected (a hook isn't a result leaf).
        let err = DagGraph::from_yaml(
            "name: w\nresult_from: fin\ntasks:\n  - { name: a, command: [\"true\"] }\n  - { name: fin, command: [\"true\"], hook: on_exit }\n",
        )
        .err()
        .expect("hook result_from must be rejected")
        .to_string();
        assert!(err.contains("result_from cannot name hook task 'fin'"), "got: {err}");
    }

    #[test]
    fn approval_task_is_validated_and_needs_no_command() {
        // An approval gate parses without a command and carries its timeout knobs.
        let g = DagGraph::from_yaml(
            "name: w\ntasks:\n  - { name: build, command: [\"make\"] }\n  - { name: gate, type: approval, depends_on: [build], approval_timeout_secs: 3600, approval_on_timeout: approve }\n  - { name: deploy, command: [\"ship\"], depends_on: [gate] }\n",
        )
        .unwrap();
        let gate = g.task_spec("gate").unwrap();
        assert!(gate.is_approval());
        assert_eq!(gate.approval_timeout_secs, Some(3600));
        assert_eq!(gate.approval_on_timeout.as_deref(), Some("approve"));

        // An unknown type is rejected.
        let err = DagGraph::from_yaml(
            "name: w\ntasks:\n  - { name: a, type: wizardry, command: [\"true\"] }\n",
        )
        .err()
        .expect("bad type rejected")
        .to_string();
        assert!(err.contains("invalid type 'wizardry'"), "got: {err}");

        // An invalid approval_on_timeout is rejected.
        let err = DagGraph::from_yaml(
            "name: w\ntasks:\n  - { name: a, type: approval, approval_on_timeout: maybe }\n",
        )
        .err()
        .expect("bad on_timeout rejected")
        .to_string();
        assert!(err.contains("invalid approval_on_timeout 'maybe'"), "got: {err}");

        // A non-approval task still requires a command (rejected in expansion).
        let err = DagGraph::from_yaml("name: w\ntasks:\n  - { name: a }\n")
            .err()
            .expect("command-less non-approval task rejected")
            .to_string();
        assert!(
            err.contains("must set exactly one of `command`"),
            "got: {err}"
        );
    }

    #[test]
    fn retry_budgets_parse_and_survive_expansion() {
        let yaml = "\
name: w
tasks:
  - name: train
    command: [\"true\"]
    max_attempts: 3
    retry_budgets:
      gpu-ecc: 8
      nan-loss: 0
";
        let g = DagGraph::from_yaml(yaml).unwrap();
        let t = g.task_spec("train").unwrap();
        assert_eq!(t.retry_budgets.get("gpu-ecc"), Some(&8));
        assert_eq!(t.retry_budgets.get("nan-loss"), Some(&0));
        assert_eq!(t.max_attempts, 3);
    }

    #[test]
    fn a_misspelled_fault_class_is_a_parse_error_not_a_silent_no_op() {
        // The failure mode this guards: `gpu_ecc_error` parses to Unknown,
        // matches nothing at runtime, and the author believes they set a policy
        // on the code path that decides whether to spend another thousand
        // GPU-hours.
        let yaml = "\
name: w
tasks:
  - name: train
    command: [\"true\"]
    retry_budgets:
      gpu_ecc_error: 8
";
        let err = match DagGraph::from_yaml(yaml) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected a parse error for a misspelled fault class"),
        };
        assert!(err.contains("unknown retry_budgets fault class"), "{err}");
        // The error lists the legal alternatives rather than making the author grep.
        assert!(err.contains("gpu-ecc"), "{err}");
    }

    #[test]
    fn a_non_canonical_spelling_is_rejected_rather_than_silently_never_firing() {
        // The subtle half of the typo guard. `FaultClass::parse` is tolerant on
        // purpose so it can read rows written by other builds — so `GPU_ECC`
        // *parses*, passes a naive validity check, is stored verbatim, and then
        // never matches the runtime lookup, which is by the canonical
        // `as_str()`. The author's policy silently does nothing.
        for bad in ["GPU_ECC", "Gpu-Ecc", "gpu_ecc", "canceled"] {
            let yaml = format!(
                "name: w\ntasks:\n  - {{ name: t, command: [\"true\"], retry_budgets: {{ {bad}: 8 }} }}\n"
            );
            let err = match DagGraph::from_yaml(&yaml) {
                Err(e) => e.to_string(),
                Ok(_) => panic!("expected '{bad}' to be rejected"),
            };
            assert!(err.contains("unknown retry_budgets fault class"), "{bad}: {err}");
        }
        // And the error points at the canonical spelling rather than making the
        // author diff two strings by eye.
        let yaml = "name: w\ntasks:\n  - { name: t, command: [\"true\"], retry_budgets: { GPU_ECC: 8 } }\n";
        let err = match DagGraph::from_yaml(yaml) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected rejection"),
        };
        assert!(err.contains("did you mean 'gpu-ecc'?"), "{err}");
    }

    #[test]
    fn the_literal_unknown_class_is_a_legal_budget_key() {
        // "we looked and could not tell" is a real bucket an operator may want
        // to cap, and it round-trips through parse() to Unknown like a typo
        // would — so it needs an explicit exemption from the typo check.
        let yaml = "name: w\ntasks:\n  - { name: t, command: [\"true\"], retry_budgets: { unknown: 2 } }\n";
        let g = DagGraph::from_yaml(yaml).unwrap();
        assert_eq!(g.task_spec("t").unwrap().retry_budgets.get("unknown"), Some(&2));
    }

    #[test]
    fn task_defaults_merge_retry_budgets_per_class_not_wholesale() {
        // The task names one class; it must keep the workflow's other classes.
        let yaml = "\
name: w
task_defaults:
  retry_budgets:
    gpu-ecc: 8
    fabric-ib: 6
tasks:
  - name: a
    command: [\"true\"]
    retry_budgets:
      gpu-ecc: 2
  - name: b
    command: [\"true\"]
";
        let g = DagGraph::from_yaml(yaml).unwrap();
        let a = g.task_spec("a").unwrap();
        assert_eq!(a.retry_budgets.get("gpu-ecc"), Some(&2), "task wins its own class");
        assert_eq!(a.retry_budgets.get("fabric-ib"), Some(&6), "and inherits the rest");
        let b = g.task_spec("b").unwrap();
        assert_eq!(b.retry_budgets.get("gpu-ecc"), Some(&8));
        assert_eq!(b.retry_budgets.get("fabric-ib"), Some(&6));
    }

    /// `defer:` accepts the shapes it should and refuses the ones that would
    /// park a row nothing can resolve.
    #[test]
    fn defer_validation() {
        // Minimal valid: a command task with a kind. poll_secs defaults.
        let g = DagGraph::from_yaml(
            "name: p\ntasks:\n  - { name: s, command: [submit], defer: { kind: spark-k8s } }\n",
        )
        .unwrap();
        let d = g.task_spec("s").unwrap().defer.clone().unwrap();
        assert_eq!(d.kind, "spark-k8s");
        assert_eq!(d.poll_secs, DEFAULT_DEFER_POLL_SECS, "poll_secs defaults");
        assert!(d.max_wait_secs.is_none(), "no ceiling by default — the run's deadline bounds it");

        let err = |y: &str| {
            DagGraph::from_yaml(y).err().expect("must be refused").to_string()
        };

        // An empty kind routes to no poller.
        assert!(err("name: p\ntasks:\n  - { name: s, command: [x], defer: { kind: \"  \" } }\n")
            .contains("defer.kind is empty"));

        // A zero interval would hot-loop the sweep against someone's API.
        let zero = err("name: p\ntasks:\n  - { name: s, command: [x], defer: { kind: k, poll_secs: 0 } }\n");
        assert!(zero.contains("defer.poll_secs=0"), "{zero}");
        assert!(zero.contains("hot-loops"), "says why, not just that: {zero}");

        // A ceiling under one interval fails the task before the first poll.
        let tight = err(
            "name: p\ntasks:\n  - { name: s, command: [x], defer: { kind: k, poll_secs: 60, max_wait_secs: 30 } }\n",
        );
        assert!(tight.contains("max_wait_secs=30"), "{tight}");
        assert!(tight.contains("polled even once"), "{tight}");
        // Equal is fine: exactly one poll is a legitimate budget.
        DagGraph::from_yaml(
            "name: p\ntasks:\n  - { name: s, command: [x], defer: { kind: k, poll_secs: 60, max_wait_secs: 60 } }\n",
        )
        .unwrap();

        // Two loop operators on one row would re-submit on every "still running".
        let both = err(
            "name: p\ntasks:\n  - { name: s, command: [x], defer: { kind: k }, repeat: { until: \"{{ output }} == done\", max_iterations: 3 } }\n",
        );
        assert!(both.contains("cannot combine `defer` with `repeat`"), "{both}");
        assert!(both.contains("both are loop operators"), "names the conflict: {both}");

        // The command-less kinds have no submit to defer.
        for (kind, extra) in [
            ("approval", ""),
            ("wait", ", wait: { for: 5s }"),
            ("workflow", ", workflow: other"),
        ] {
            let msg = err(&format!(
                "name: p\ntasks:\n  - {{ name: s, type: {kind}{extra}, defer: {{ kind: k }} }}\n"
            ));
            assert!(
                msg.contains("cannot combine `defer` with `type:"),
                "type: {kind} must be refused: {msg}"
            );
        }

        // A command-less leaf never reaches this check — expansion's
        // "exactly one of `command` (leaf) or `template` (call)" rule refuses it
        // first — so the reachable command-less shapes are the typed kinds above
        // and the template call, which `expand` refuses by name.
        let call = err(
            "name: p\ntemplates:\n  - { name: t, tasks: [{ name: inner, command: [x] }] }\n\
             tasks:\n  - { name: s, template: t, defer: { kind: k } }\n",
        );
        assert!(call.contains("cannot set `defer:` on a `template:` call"), "{call}");
        assert!(call.contains("would be dropped"), "names the silent failure: {call}");

        // A deferred gang member parks N rows on N jobs — the all-or-nothing is gone.
        let gang = err(
            "name: p\ntasks:\n  - { name: s, command: [x], gang: { size: 4 }, defer: { kind: k } }\n",
        );
        assert!(gang.contains("cannot combine `defer` with `gang`"), "{gang}");

        // `defer:` + `produces:` is accepted: a deferred task succeeds in the
        // sweep, and the sweep records dataset updates through the same path
        // the worker result does.
        DagGraph::from_yaml(
            "name: p\ntasks:\n  - { name: s, command: [x], defer: { kind: k }, produces: [\"s3://b/o\"] }\n",
        )
        .expect("a deferred task may declare produces:");
    }

    /// `defer.connection:` is a signpost in the open build: it names what was
    /// attempted, links the gap list, names a WORKING open path, and names the
    /// seam. The enterprise build accepts the same spec.
    #[test]
    fn defer_connection_is_gated_with_a_signpost() {
        let spec = "name: p\ntasks:\n  - { name: s, command: [x], defer: { kind: k, connection: prod-spark } }\n";
        let parsed = DagGraph::from_yaml(spec);

        if cfg!(feature = "enterprise") {
            let g = parsed.expect("the enterprise build accepts a named connection");
            assert_eq!(
                g.task_spec("s").unwrap().defer.as_ref().unwrap().connection.as_deref(),
                Some("prod-spark")
            );
            return;
        }

        let msg = parsed.err().expect("the open build refuses a named connection").to_string();
        assert!(msg.contains("prod-spark"), "names what was attempted: {msg}");
        assert!(msg.contains("not in this build"), "names the gap: {msg}");
        assert!(
            msg.contains("#what-this-build-does-not-do"),
            "links the anchor: {msg}"
        );
        assert!(msg.contains("environment:"), "names the open endpoint path: {msg}");
        assert!(msg.contains("value_from"), "names the open credential path: {msg}");
        assert!(msg.contains("dagron_engine::Seams"), "names the seam: {msg}");
    }

    /// The handle is read from the LAST matching line, so a step may log before
    /// it — and an empty handle is no handle, because parking on one produces a
    /// row no sweep can ever resolve.
    #[test]
    fn parse_handle_takes_the_last_line_and_rejects_empty() {
        assert_eq!(parse_handle("dagron::handle=abc").as_deref(), Some("abc"));
        assert_eq!(
            parse_handle("submitting...\ndagron::handle=run-1\n").as_deref(),
            Some("run-1"),
            "a step may log before the handle"
        );
        assert_eq!(
            parse_handle("dagron::handle=stale\nretrying\ndagron::handle=fresh").as_deref(),
            Some("fresh"),
            "last wins — an echoed earlier attempt must not become the handle"
        );
        assert_eq!(parse_handle("  dagron::handle=  spaced  ").as_deref(), Some("spaced"));
        assert_eq!(parse_handle("dagron::handle=").as_deref(), None, "empty is no handle");
        assert_eq!(parse_handle("dagron::handle=   ").as_deref(), None);
        assert_eq!(parse_handle("no handle here").as_deref(), None);
    }

    /// `defer.http` is checked at SUBMIT — the point being that a typo in a
    /// predicate must not survive until four hours into a poll.
    #[test]
    fn defer_http_validation() {
        let ok = DagGraph::from_yaml(
            "name: p\ntasks:\n  - name: s\n    command: [submit]\n    defer:\n      kind: spark-k8s\n      http:\n        url: \"https://api.example/jobs/{{ handle }}\"\n        headers: [{ name: Authorization, value_from: { secret: TOK } }]\n        succeed_when: \"status.applicationState.state == COMPLETED\"\n        fail_when: \"status.applicationState.state in [FAILED, SUBMISSION_FAILED]\"\n        error_from: \"status.applicationState.errorMessage\"\n",
        )
        .unwrap();
        let h = ok.task_spec("s").unwrap().defer.as_ref().unwrap().http.clone().unwrap();
        assert!(h.url.contains(HANDLE_PLACEHOLDER), "{{{{ handle }}}} survives expansion");
        assert_eq!(h.headers[0].value_from.as_ref().unwrap().secret, "TOK");

        let err = |y: &str| DagGraph::from_yaml(y).err().expect("must be refused").to_string();
        let spec = |http: &str| {
            format!("name: p\ntasks:\n  - name: s\n    command: [x]\n    defer:\n      kind: k\n      http:\n{http}")
        };

        // Scheme, not reachability — the URL still holds unexpanded templates.
        let scheme = err(&spec("        url: \"ftp://h/j\"\n        succeed_when: \"a == b\"\n"));
        assert!(scheme.contains("must be http(s)"), "{scheme}");
        assert!(err(&spec("        url: \"  \"\n        succeed_when: \"a == b\"\n")).contains("url is empty"));

        // Every predicate field is parsed, and the diagnostic names which one.
        let bad_succeed = err(&spec("        url: \"https://h/j\"\n        succeed_when: \"status.phase\"\n"));
        assert!(bad_succeed.contains("defer.http.succeed_when"), "{bad_succeed}");
        assert!(bad_succeed.contains("expected `path == VALUE`"), "{bad_succeed}");

        let bad_fail = err(&spec(
            "        url: \"https://h/j\"\n        succeed_when: \"a == b\"\n        fail_when: \"a in b\"\n",
        ));
        assert!(bad_fail.contains("defer.http.fail_when"), "{bad_fail}");
        assert!(bad_fail.contains("bracketed list"), "{bad_fail}");

        let bad_path = err(&spec(
            "        url: \"https://h/j\"\n        succeed_when: \"a == b\"\n        error_from: \"a..b\"\n",
        ));
        assert!(bad_path.contains("defer.http.error_from"), "{bad_path}");
        assert!(bad_path.contains("empty segment"), "{bad_path}");

        let no_name = err(&spec(
            "        url: \"https://h/j\"\n        succeed_when: \"a == b\"\n        headers: [{ name: \"\", value: v }]\n",
        ));
        assert!(no_name.contains("header with no name"), "{no_name}");
    }

    /// `defer.http.cancel` is checked at SUBMIT like the rest of the block, and
    /// its verb defaults to DELETE.
    #[test]
    fn defer_http_cancel_validation() {
        let spec = |cancel: &str| {
            format!(
                "name: p\ntasks:\n  - name: s\n    command: [x]\n    defer:\n      kind: k\n      http:\n        url: \"https://h/j/{{{{ handle }}}}\"\n        succeed_when: \"a == b\"\n        cancel:\n{cancel}"
            )
        };
        let ok = DagGraph::from_yaml(&spec("          url: \"https://h/j/{{ handle }}\"\n")).unwrap();
        let c = ok.task_spec("s").unwrap().defer.as_ref().unwrap().http.clone().unwrap().cancel.unwrap();
        assert_eq!(c.method(), "DELETE", "DELETE is the default");
        assert!(c.url.contains(HANDLE_PLACEHOLDER), "{{{{ handle }}}} survives expansion");
        assert!(c.body.is_none());

        let post = DagGraph::from_yaml(&spec(
            "          url: \"https://h/cancel\"\n          method: post\n          body: '{\"run_id\": \"{{ handle }}\"}'\n",
        ))
        .unwrap();
        let c = post.task_spec("s").unwrap().defer.as_ref().unwrap().http.clone().unwrap().cancel.unwrap();
        assert_eq!(c.method(), "POST", "case-insensitive");
        assert!(c.body.unwrap().contains("{{ handle }}"));

        let err = |y: &str| DagGraph::from_yaml(y).err().expect("must be refused").to_string();
        let scheme = err(&spec("          url: \"ftp://h/j\"\n"));
        assert!(scheme.contains("defer.http.cancel.url must be http(s)"), "{scheme}");
        let verb = err(&spec("          url: \"https://h/j\"\n          method: GET\n"));
        assert!(verb.contains("defer.http.cancel.method"), "{verb}");
        assert!(verb.contains("DELETE, POST, PUT, PATCH"), "{verb}");
    }

    /// The http block templates per fan-out instance, but `{{ handle }}` is
    /// runtime state and must reach the poller unsubstituted.
    #[test]
    fn defer_http_templates_per_instance_but_leaves_the_handle_alone() {
        let g = DagGraph::from_yaml(
            "name: p\nparameters: { host: api.example }\ntasks:\n  - name: s\n    command: [x]\n    defer:\n      kind: k\n      http:\n        url: \"https://{{ host }}/jobs/{{ handle }}\"\n        succeed_when: \"state == DONE\"\n",
        )
        .unwrap();
        let h = g.task_spec("s").unwrap().defer.as_ref().unwrap().http.clone().unwrap();
        assert_eq!(h.url, "https://api.example/jobs/{{ handle }}");
    }

    /// Every signpost in the product asserts the same four properties.
    ///
    /// A source scan rather than a shared helper crates call, because no such
    /// helper can reach them all: `dagron-crypto` has zero dagron dependencies,
    /// `dagron-core` and `dagron-source` are *upstream* of `dagron-engine`, and
    /// `dagron-api` never builds a `Seams`. One test that reads the tree covers
    /// every crate regardless of which way the dependency arrows point.
    ///
    /// The bar: a gate is a **signpost, not a dead end** — it names what was
    /// attempted, and it names what to do instead in this build.
    /// Before this test the eight signposts asserted
    /// mutually different subsets of that — `source.rs` checked the fallback and
    /// the seam but not the anchor, `fleet.rs` and `link.rs` checked the anchor
    /// and the fallback but not the seam, `crypto` checked only that the gap was
    /// named, and the engine's 403 was asserted by nothing at all. A funnel
    /// whose steps each enforce a different rule is a funnel that leaks.
    #[test]
    fn every_signpost_names_the_gap_and_a_way_forward() {
        // Split so this test does not match its own source.
        let anchor = concat!("#what-this-build", "-does-not-do");
        // The message, not its neighbourhood. A fixed character window around
        // the anchor reaches into whatever code happens to sit nearby, and in a
        // large file that almost always contains one of the markers below — so
        // the test passes for a signpost that says nothing. Measured: a
        // deliberately dead-end signpost injected into dagron-artifact passed a
        // 1400-character window and fails this one.
        //
        // A signpost is a run of consecutive non-blank lines: a `bail!(…);`, a
        // `const` with its doc comment, or a paragraph of `//!`. Bounding the
        // window at the blank lines either side is exactly that run.
        fn message_block(src: &str, at: usize) -> String {
            let strip = |l: &str| {
                l.trim()
                    .trim_start_matches("//!")
                    .trim_start_matches("///")
                    .trim_start_matches("//")
                    .trim()
                    .trim_end_matches('\\')
                    .trim()
                    .to_string()
            };
            let lines: Vec<&str> = src.lines().collect();
            let idx = src[..at].matches('\n').count();
            let mut lo = idx;
            while lo > 0 && !strip(lines[lo - 1]).is_empty() {
                lo -= 1;
            }
            let mut hi = idx;
            while hi + 1 < lines.len() && !strip(lines[hi + 1]).is_empty() {
                hi += 1;
            }
            lines[lo..=hi].iter().map(|l| strip(l)).collect::<Vec<_>>().join(" ")
        }

        let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");
        let mut checked = 0;
        let mut offenders: Vec<String> = Vec::new();

        let mut stack = vec![std::path::PathBuf::from(root).join("crates")];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("read crates/") {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    // `target/` holds build output, not source we author.
                    if path.file_name().is_some_and(|n| n == "target") {
                        continue;
                    }
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|s| s.to_str()) != Some("rs") {
                    continue;
                }
                let src = std::fs::read_to_string(&path).unwrap();
                let mut from = 0;
                while let Some(rel) = src[from..].find(anchor) {
                    let at = from + rel;
                    from = at + anchor.len();
                    // Skip the assertions in this test file and in each
                    // signpost's own unit test: they quote the anchor to check
                    // for it, and are not themselves signposts.
                    let line_start = src[..at].rfind('\n').map(|i| i + 1).unwrap_or(0);
                    let line_end = src[at..].find('\n').map(|i| at + i).unwrap_or(src.len());
                    if src[line_start..line_end].trim_start().starts_with("assert") {
                        continue;
                    }
                    checked += 1;
                    let w = message_block(&src, at);
                    let w = w.split_whitespace().collect::<Vec<_>>().join(" ");
                    let w = w.as_str();

                    // 1. Names the gap.
                    let lower = w.to_ascii_lowercase();
                    let names_gap = lower.contains("not in this build")
                        || lower.contains("not bundled in this build");
                    // 2. Names what to do INSTEAD — a working open path, a seam
                    //    to plug into, or the page that documents one. This is
                    //    the property that separates a signpost from a refusal.
                    //
                    //    The gap phrase is removed FIRST, and that is not a
                    //    detail: "this build" is the strongest marker of an
                    //    alternative ("This build streams with SOURCE=stream"),
                    //    but it is also inside the gap phrase itself, so
                    //    matching it against the whole message makes the check
                    //    vacuous — every signpost passes by saying only that it
                    //    is not in this build. Measured: a deliberately
                    //    dead-end signpost injected into dagron-artifact passed
                    //    until this line existed.
                    // Lowercased: a signpost's alternative is a new sentence
                    // ("This build runs one unit", dagron-source), so a
                    // case-sensitive match misses the capital that starts it.
                    let rest = w
                        .to_ascii_lowercase()
                        .replace("not in this build", "")
                        .replace("not bundled in this build", "");
                    let names_way_forward =
                        ["this build", "docs/", "seams", "sourcefactory", "externalpoller",
                         "unset ", "instead", "the open build"]
                            .iter()
                            .any(|m| rest.contains(m));
                    if !(names_gap && names_way_forward) {
                        let line = src[..at].matches('\n').count() + 1;
                        offenders.push(format!(
                            "{}:{line}: gap={names_gap} way_forward={names_way_forward}",
                            path.strip_prefix(root).unwrap_or(&path).display()
                        ));
                    }
                }
            }
        }

        assert!(checked >= 8, "expected the product's signposts, found {checked}");
        assert!(
            offenders.is_empty(),
            "a gate must be a signpost, not a dead end — each of these names the gap without \
             naming a way forward:\n  {}",
            offenders.join("\n  ")
        );
    }
}
