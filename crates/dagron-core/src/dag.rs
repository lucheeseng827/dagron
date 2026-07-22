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
    /// Arguments passed to the called template — values may reference the caller's
    /// scope via `{{ name }}`; inside the template they fill its parameters.
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
    /// Loop operator: re-run this task until `until` evaluates true (the
    /// poll-until-done pattern). See [`RepeatSpec`]. Evaluated by the engine
    /// each time the task *succeeds*; failures still follow the normal
    /// retry/failure path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repeat: Option<RepeatSpec>,
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
/// the task pod container's `resources` block.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ResourceRequirements {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub requests: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub limits: BTreeMap<String, String>,
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
/// `approval` is a human approval gate (#19).
pub const TASK_KINDS: &[&str] = &["task", "approval"];

/// Valid `approval_on_timeout:` values.
pub const APPROVAL_TIMEOUT_ACTIONS: &[&str] = &["approve", "reject"];

impl TaskSpec {
    /// Whether this task is a `type: approval` human gate (#19).
    pub fn is_approval(&self) -> bool {
        self.task_type.as_deref() == Some("approval")
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
    /// Run-level wall-clock budget in seconds. When the run has been `running` longer than
    /// this, the engine's deadline sweep marks it `failed` and cancels its
    /// remaining tasks. `None` = no run-level deadline (per-task `timeout_secs`
    /// still applies). Must be ≥ 1 when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_timeout_secs: Option<u64>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docker_image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_class: Option<String>,
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
        if let Some(d) = &spec.deadline {
            parse_duration_secs(&d.within)
                .map_err(|e| anyhow::anyhow!("invalid deadline in DAG '{}': {e}", spec.name))?;
        }
        if let Some(class) = &spec.runner_class {
            validate_runner_class(class)
                .map_err(|e| anyhow::anyhow!("invalid runner_class in DAG '{}': {e}", spec.name))?;
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
            if let Some(class) = &task.runner_class {
                validate_runner_class(class).map_err(|e| {
                    anyhow::anyhow!(
                        "invalid runner_class for task '{}' in DAG '{}': {e}",
                        task.name,
                        spec.name
                    )
                })?;
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
                if task.is_approval() {
                    bail!(
                        "task '{}' cannot combine `repeat` with an approval gate in DAG '{}'",
                        task.name,
                        spec.name
                    );
                }
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
            // A command is required for an ordinary task; an approval gate has no
            // command (it waits for a human), so it is exempt.
            if task.command.is_empty() && !task.is_approval() {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runner-class validation: charset/length rules plus the reserved `other`
    /// (the metrics tail bucket) — a spec-level `other` would collide with the
    /// aggregated `runner_class="other"` series.
    #[test]
    fn runner_class_validation_rules() {
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
    fn run_timeout_zero_is_rejected() {
        let err = DagGraph::from_yaml(
            "name: w\nrun_timeout_secs: 0\ntasks:\n  - { name: a, command: [\"true\"] }\n",
        )
        .err()
        .expect("run_timeout_secs=0 must be rejected")
        .to_string();
        assert!(err.contains("run_timeout_secs=0"), "got: {err}");
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
