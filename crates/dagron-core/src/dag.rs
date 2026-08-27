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
    /// Conditional guard, e.g. `"{{ depth }} > 0"`. When it evaluates false the
    /// task (and any sub-DAG it would expand to) is skipped. This is what lets a
    /// recursive template terminate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<String>,
    /// Human-readable label template for fan-out instances, e.g.
    /// `instance_key: "{{ item.region }}"`. When set on a `with_items` /
    /// `with_param` task, each expanded instance is named
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
    /// (a `produces:` task succeeded, or — Enterprise — an external dataset
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
/// **Only `tasks` is enforceable today, and that is a statement about the data
/// rather than about ambition.** `AGENT_SCHEDULER_PLAN.md` §G-S3 asks for a
/// "spend/token/task budget per run". A task count is exact and knowable at
/// creation — dagron expands `with_items` fan-out and templates at submit, so
/// the number of rows a run will insert is already decided before anything
/// runs. Spend and token counts are not: nothing in the engine meters currency
/// or model tokens against a run, so a `spend:` field would be a promise with
/// no measurement behind it, which is worse than no field.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct RunBudget {
    /// Maximum tasks this run may create. `None` = no cap. Must be >= 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tasks: Option<u32>,
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
    /// succeeded, or — Enterprise — an external event was posted). The open
    /// build supports exactly **one** dataset here; subscribing to several
    /// (fan-in composition, with [`DagSpec::datasets_mode`]) ships with dagron
    /// Enterprise. Fires coalesce: updates that arrive while a fire is being
    /// processed produce one run, not one per event. Empty = not
    /// dataset-triggered.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on_datasets: Vec<String>,
    /// How multiple [`DagSpec::on_datasets`] entries combine (**Enterprise**):
    /// `"any"` — fire when any subscribed dataset updates (default); `"all"` —
    /// fire only once *every* subscribed dataset has updated since the last
    /// fire (the Airflow AND-of-datasets semantics, e.g. "refresh the join
    /// once both upstream tables landed"). Meaningless with a single dataset.
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

        // Dataset triggers (`on_datasets:`): valid, deduplicated URIs. The open
        // build fires on a single dataset; multi-dataset composition (and its
        // `datasets_mode`) is the Enterprise data-aware scheduler — reject with
        // a signpost, not a silent partial subscription (the SOURCE-connector
        // funnel pattern).
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
            if !cfg!(feature = "enterprise")
                && (spec.on_datasets.len() > 1 || spec.datasets_mode.is_some())
            {
                bail!(
                    "DAG '{}' subscribes to {} datasets{}: multi-dataset triggers and \
                     `datasets_mode` composition (any-of / all-of fan-in) ship with dagron \
                     Enterprise — https://github.com/lucheeseng827/dagron#dagron-enterprise. \
                     This build fires on a single dataset: keep exactly one `on_datasets` \
                     entry (and omit `datasets_mode`), or split consumers into one \
                     workflow per upstream dataset.",
                    spec.name,
                    spec.on_datasets.len(),
                    if spec.datasets_mode.is_some() { " with datasets_mode" } else { "" },
                );
            }
        }

        for task in &spec.tasks {
            if node_index.contains_key(&task.name) {
                bail!("duplicate task name '{}' in DAG '{}'", task.name, spec.name);
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
            // `produces:` — dataset updates are recorded on the worker-result
            // success path, which approval gates, sub-workflow triggers, and
            // wait sensors never take (they resolve via reconcile sweeps). Only
            // command tasks may declare them, so a `produces:` is never
            // silently dropped.
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
    /// and `on_datasets:` triggers — plus the OSS/Enterprise composition gate.
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

        // Composition (multi-dataset / datasets_mode) is the Enterprise line:
        // the open build rejects it with a signpost naming the edition; an
        // enterprise build accepts it.
        let multi = "name: p\non_datasets: [\"a://b\", \"a://c\"]\ndatasets_mode: all\ntasks:\n  - { name: t, command: [\"true\"] }\n";
        #[cfg(not(feature = "enterprise"))]
        {
            let err =
                DagGraph::from_yaml(multi).err().expect("multi-dataset is Enterprise").to_string();
            assert!(err.contains("dagron Enterprise"), "signpost names the edition: {err}");
        }
        #[cfg(feature = "enterprise")]
        {
            let dag = DagGraph::from_yaml(multi).unwrap();
            assert_eq!(dag.spec.on_datasets.len(), 2);
            assert_eq!(dag.spec.datasets_mode.as_deref(), Some("all"));
        }
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
}
