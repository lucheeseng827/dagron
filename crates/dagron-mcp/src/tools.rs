//! The MCP tool catalogue and its dispatch onto `dagron-api`.
//!
//! One module owns both halves on purpose: a tool that is advertised but not
//! dispatched (or dispatched but not advertised) is the failure mode this
//! surface is most prone to, and keeping the catalogue and the `match` in the
//! same file makes the drift visible in review — [`every_advertised_tool_dispatches`]
//! makes it visible in CI.
//!
//! Two rules run through every tool here, both from `docs/MCP.md`'s
//! implementation notes:
//!
//! - **Path segments are percent-encoded, never pattern-matched.** uuid-shaped
//!   ids keep the strict `[A-Za-z0-9_-]` check ([`Args::id`]); the segments that
//!   are legitimately free-form — an artifact's `task` and `name` — go through
//!   [`Args::seg`], which encodes them so an artifact called `../../etc/passwd`
//!   is one opaque segment rather than a reshaped request path.
//! - **A state answer is not a tool error.** `404`/`409`/`410` mean "that
//!   workflow is paused", "that gate isn't awaiting approval", "that run was
//!   compacted" — things an agent should reason about, so [`render`] returns
//!   them as structured results. Tool errors stay reserved for transport and
//!   validation failures.

use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::Duration;

use crate::{ApiResponse, DagronClient, Method};

/// Whether a tool only reads dagron state or changes it.
///
/// The distinction is load-bearing rather than documentary: `DAGRON_MCP_READONLY`
/// hides every [`Access::Write`] tool from `tools/list` *and* refuses it in
/// [`call_tool`], which is what keeps the pre-P0 safe-by-default posture
/// available now that this server can create, delete and approve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// Reads only. Safe to expose to an agent whose prompt you do not control.
    Read,
    /// Changes cluster state (creates runs, edits workflows, resolves gates).
    Write,
}

// ── Argument access ───────────────────────────────────────────────────────────

/// Typed, validated access to one tool call's `arguments` object.
///
/// Every accessor fails *locally* with a message naming the argument, so a
/// malformed call costs the agent one fast tool error instead of a round trip
/// and an opaque HTTP 400.
struct Args<'a> {
    v: &'a Value,
}

impl<'a> Args<'a> {
    fn new(v: &'a Value) -> Self {
        Self { v }
    }

    /// A required string argument.
    fn str(&self, k: &str) -> Result<&'a str> {
        match self.v.get(k) {
            Some(Value::String(s)) if !s.is_empty() => Ok(s),
            Some(Value::String(_)) => anyhow::bail!("`{k}` must not be empty"),
            Some(other) if !other.is_null() => anyhow::bail!("`{k}` must be a string, got {other}"),
            _ => anyhow::bail!("missing required string argument `{k}`"),
        }
    }

    /// A required string argument that may legitimately be empty — an artifact's
    /// contents, where a zero-byte marker file is a real thing to write.
    fn raw_str(&self, k: &str) -> Result<&'a str> {
        match self.v.get(k) {
            Some(Value::String(s)) => Ok(s),
            Some(other) if !other.is_null() => anyhow::bail!("`{k}` must be a string, got {other}"),
            _ => anyhow::bail!("missing required string argument `{k}`"),
        }
    }

    /// An optional string argument. An empty string is "not set", not "match the
    /// empty string" — an agent filling in a blank must not accidentally filter.
    fn opt_str(&self, k: &str) -> Result<Option<&'a str>> {
        match self.v.get(k) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(s)) if s.is_empty() => Ok(None),
            Some(Value::String(s)) => Ok(Some(s)),
            Some(other) => anyhow::bail!("`{k}` must be a string, got {other}"),
        }
    }

    /// A uuid-shaped path-segment id, checked against `[A-Za-z0-9_-]+`.
    ///
    /// Deliberately *not* widened for the free-form segments the artifact tools
    /// introduced — see [`Args::seg`].
    fn id(&self, k: &str) -> Result<String> {
        let v = self.str(k)?;
        if !v.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
            anyhow::bail!("invalid `{k}`: only non-empty [A-Za-z0-9_-] allowed");
        }
        Ok(v.to_string())
    }

    /// A free-form path segment, percent-encoded.
    ///
    /// Artifact `task`/`name` are user-chosen strings that may legitimately hold
    /// spaces, dots and slashes, so the `id` alphabet cannot apply. Encoding —
    /// not pattern-matching — is what keeps them one segment: `../../etc/passwd`
    /// reaches dagron-api as `..%2F..%2Fetc%2Fpasswd`, which its router sees as a
    /// single `{name}` capture and its artifact store then sanitizes.
    fn seg(&self, k: &str) -> Result<String> {
        Ok(urlencode(self.str(k)?))
    }

    /// An optional integer, clamped locally to `min..=max`.
    ///
    /// Bounding here rather than trusting the server's clamp is what turns a
    /// malformed argument into a fast tool error instead of a round trip.
    fn opt_int(&self, k: &str, min: i64, max: i64) -> Result<Option<i64>> {
        match self.v.get(k) {
            None | Some(Value::Null) => Ok(None),
            Some(v) => {
                let n = v.as_i64().with_context(|| format!("`{k}` must be an integer"))?;
                if !(min..=max).contains(&n) {
                    anyhow::bail!("`{k}` must be between {min} and {max}");
                }
                Ok(Some(n))
            }
        }
    }

    /// An integer with a default, bounded as [`Args::opt_int`].
    fn int(&self, k: &str, min: i64, max: i64, default: i64) -> Result<i64> {
        Ok(self.opt_int(k, min, max)?.unwrap_or(default))
    }

    /// An enum-valued string, checked against the set the API accepts.
    ///
    /// The server validates these too (with a 400). Checking locally means the
    /// agent is told the legal values in the same breath as the rejection.
    fn one_of(&self, k: &str, allowed: &[&str]) -> Result<String> {
        let v = self.str(k)?.trim().to_ascii_lowercase();
        if !allowed.contains(&v.as_str()) {
            anyhow::bail!("invalid `{k}` {v:?}: expected one of {}", allowed.join(", "));
        }
        Ok(v)
    }

    /// A `{string: string}` map — the shape both `parameters` arguments take.
    ///
    /// dagron's `parameters:` are string-valued substitutions, so a number or a
    /// nested object is a mistake worth naming rather than coercing: silently
    /// stringifying `{"retries": 3}` would leave the agent believing it passed
    /// an integer.
    fn str_map(&self, k: &str) -> Result<Option<BTreeMap<String, String>>> {
        match self.v.get(k) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::Object(o)) if o.is_empty() => Ok(None),
            Some(Value::Object(o)) => {
                let mut out = BTreeMap::new();
                for (name, value) in o {
                    match value {
                        Value::String(s) => {
                            out.insert(name.clone(), s.clone());
                        }
                        other => anyhow::bail!(
                            "`{k}.{name}` must be a string, got {other} \
                             (dagron parameters are string substitutions)"
                        ),
                    }
                }
                Ok(Some(out))
            }
            Some(other) => anyhow::bail!("`{k}` must be an object of strings, got {other}"),
        }
    }
}

// ── Query strings ─────────────────────────────────────────────────────────────

/// Accumulates `?a=b&c=d`, percent-encoding every value.
#[derive(Default)]
struct Query(Vec<String>);

impl Query {
    fn new() -> Self {
        Self::default()
    }

    fn opt(&mut self, key: &str, value: Option<&str>) {
        if let Some(v) = value {
            self.0.push(format!("{key}={}", urlencode(v)));
        }
    }

    fn opt_int(&mut self, key: &str, value: Option<i64>) {
        if let Some(v) = value {
            self.0.push(format!("{key}={v}"));
        }
    }

    fn finish(self) -> String {
        if self.0.is_empty() {
            String::new()
        } else {
            format!("?{}", self.0.join("&"))
        }
    }
}

/// Percent-encode a query value or path segment. Only unreserved characters pass
/// through, so a regex containing `&`, `+` or `[` — or an artifact name
/// containing `/` — reaches the engine as one intact value.
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

// ── Log filter grammar ────────────────────────────────────────────────────────

/// Query params of the shared log filter grammar
/// (`dagron_logging::logfilter`), as MCP tool arguments. Listed once so the
/// per-task and whole-run log tools can't drift into offering different filters
/// over the same data — and so an agent that learns one has learned both.
const LOG_FILTER_ARGS: &[(&str, &str)] = &[
    ("q", "keep only lines containing this text"),
    ("exclude", "drop lines containing this text"),
    ("regex", "keep only lines matching this regular expression"),
    ("level", "keep only these inferred levels, comma-separated (error,warn,info,debug,trace,plain)"),
    ("case", "match case-sensitively ('1' to enable; default is insensitive)"),
    ("context", "also keep this many lines either side of each match"),
    ("limit", "maximum lines to return"),
    ("tail", "when capped, keep the last lines instead of the first ('1' to enable)"),
];

/// Build a log tool's `properties` map: `run_id`, the filter args, plus whatever
/// is specific to that tool.
fn log_tool_props(extra: &[(&str, Value)]) -> Value {
    let mut props = serde_json::Map::new();
    props.insert("run_id".into(), json!({ "type": "string" }));
    for (name, value) in extra {
        props.insert((*name).into(), value.clone());
    }
    for (name, description) in LOG_FILTER_ARGS {
        props.insert((*name).into(), json!({ "type": "string", "description": description }));
    }
    Value::Object(props)
}

/// Collect the log filter args present in a tool call into a query string.
///
/// Values are passed through as strings and validated by the engine, which owns
/// the grammar — re-implementing the parse here would give an agent two places
/// to be told a regex is invalid, with two different messages.
fn log_filter_query(args: &Value) -> Result<String> {
    let a = Args::new(args);
    let mut q = Query::new();
    for (name, _) in LOG_FILTER_ARGS {
        q.opt(name, a.opt_str(name)?);
    }
    q.opt("task", a.opt_str("task")?);
    Ok(q.finish())
}

// ── Schema helpers ────────────────────────────────────────────────────────────

fn obj(props: Value, required: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": props,
        "required": required,
        "additionalProperties": false
    })
}

/// A tool that takes no arguments.
fn no_args() -> Value {
    json!({ "type": "object", "properties": {}, "additionalProperties": false })
}

fn sstr(description: &str) -> Value {
    json!({ "type": "string", "description": description })
}

fn sint(description: &str, min: i64, max: i64) -> Value {
    json!({ "type": "integer", "description": description, "minimum": min, "maximum": max })
}

fn params_prop(what: &str) -> Value {
    json!({
        "type": "object",
        "description": format!("arguments for {what} declared `parameters:` (string values)"),
        "additionalProperties": { "type": "string" }
    })
}

fn tool(access: Access, name: &str, description: &str, input: Value) -> (Access, Value) {
    (access, json!({ "name": name, "description": description, "inputSchema": input }))
}

// ── The catalogue ─────────────────────────────────────────────────────────────

/// Workflow lifecycle states `POST /api/workflows/{id}/state` accepts.
const WORKFLOW_STATES: &[&str] = &["active", "paused", "retired"];
/// Triage decisions `POST /api/runs/{id}/triage` accepts.
const TRIAGE_STATES: &[&str] = &["acknowledged", "resolved", "ignored"];
/// Cap on an `Idempotency-Key`, matching dagron-api's own.
const MAX_IDEMPOTENCY_KEY: usize = 255;

/// The catalogue, each tool paired with whether it writes.
fn catalogue() -> Vec<(Access, Value)> {
    let mut t = Vec::new();

    // ── Runs: drive ──────────────────────────────────────────────────────────
    t.push(tool(
        Access::Read,
        "dagron_list_runs",
        "List recent workflow runs (id, status, timing), newest first. Filter with \
`status`/`name`/`trigger` and page with `limit`/`offset` rather than listing everything \
and filtering client-side.",
        obj(
            json!({
                "status": sstr("only runs in this status (pending, running, succeeded, failed, cancelled)"),
                "name": sstr("only runs of this workflow definition name (exact match)"),
                "trigger": sstr("only runs from this trigger: manual, schedule or backfill"),
                "limit": sint("page size (default 100)", 1, 500),
                "offset": sint("rows to skip", 0, i64::from(u32::MAX)),
            }),
            &[],
        ),
    ));
    t.push(tool(
        Access::Read,
        "dagron_get_run",
        "Get one workflow run's detail by id, including its task rows and (when it failed) \
a failure summary.",
        obj(json!({ "run_id": sstr("the run id") }), &["run_id"]),
    ));
    t.push(tool(
        Access::Write,
        "dagron_submit_run",
        "Submit a new workflow run from an ad-hoc DAG YAML spec. Pass `parameters` to bind the \
spec's declared `parameters:`. Pass `idempotency_key` when retrying a submit you are not sure \
landed: a repeat with the same key returns the SAME run_id instead of creating a second run.",
        obj(
            json!({
                "yaml": sstr("the DAG spec (YAML)"),
                "parameters": params_prop("the spec's"),
                "idempotency_key": sstr(
                    "retry-safety token; a repeat with this key returns the original run_id",
                ),
            }),
            &["yaml"],
        ),
    ));
    t.push(tool(
        Access::Write,
        "dagron_cancel_run",
        "Cancel a running workflow by id.",
        obj(json!({ "run_id": sstr("the run id") }), &["run_id"]),
    ));
    t.push(tool(
        Access::Read,
        "dagron_wait_run",
        "Block until a run reaches a terminal state and return its status, result and failure \
summary. Prefer this over polling dagron_get_run in a loop: the server long-polls, so one call \
replaces N round trips. A timed-out wait returns `finished: false` — call again.",
        obj(
            json!({
                "run_id": sstr("the run id"),
                "timeout_secs": sint("how long to block (default 30)", 1, 600),
            }),
            &["run_id"],
        ),
    ));

    // ── Runs: recover ────────────────────────────────────────────────────────
    t.push(tool(
        Access::Write,
        "dagron_rerun_run",
        "Cascade-rerun a failed or cancelled run from its failure frontier: failed tasks reset \
and re-run, succeeded ones stay intact. 409 when the run is not in a rerunnable state.",
        obj(
            json!({
                "run_id": sstr("the run id"),
                "from": sstr("rerun mode; only 'failed' (the default) is supported"),
            }),
            &["run_id"],
        ),
    ));
    t.push(tool(
        Access::Write,
        "dagron_resubmit_run",
        "Submit a fresh run from the same spec this run was created from. Unlike rerun, nothing \
of the original run is reused — you get a new run_id starting from the top.",
        obj(json!({ "run_id": sstr("the run id") }), &["run_id"]),
    ));
    t.push(tool(
        Access::Write,
        "dagron_retry_task",
        "Retry one failed or cancelled task in place. 409 when the task is not in a retryable \
state.",
        obj(
            json!({ "run_id": sstr("the run id"), "task_id": sstr("the task id") }),
            &["run_id", "task_id"],
        ),
    ));
    t.push(tool(
        Access::Write,
        "dagron_clear_task",
        "Clear a completed task and every task downstream of it, so that sub-DAG re-runs. Use \
this (not retry) to re-run a task that succeeded. 409 when the task is not completed.",
        obj(
            json!({ "run_id": sstr("the run id"), "task_id": sstr("the task id") }),
            &["run_id", "task_id"],
        ),
    ));

    // ── Approval gates ───────────────────────────────────────────────────────
    t.push(tool(
        Access::Read,
        "dagron_list_approvals",
        "The human-in-the-loop worklist: every `type: approval` gate currently parked, oldest \
first. A run sitting here is waiting on a decision, not on the engine.",
        no_args(),
    ));
    t.push(tool(
        Access::Write,
        "dagron_approve_task",
        "Approve a parked `type: approval` gate: the task succeeds and its dependents advance. \
409 when the task is not awaiting approval.",
        obj(
            json!({ "run_id": sstr("the run id"), "task_id": sstr("the gate's task id") }),
            &["run_id", "task_id"],
        ),
    ));
    t.push(tool(
        Access::Write,
        "dagron_reject_task",
        "Reject a parked `type: approval` gate: the task fails and its `all_success` dependents \
skip. 409 when the task is not awaiting approval.",
        obj(
            json!({ "run_id": sstr("the run id"), "task_id": sstr("the gate's task id") }),
            &["run_id", "task_id"],
        ),
    ));

    // ── Triage ───────────────────────────────────────────────────────────────
    t.push(tool(
        Access::Write,
        "dagron_triage_run",
        "Write down what you concluded about a failed run. `state` is acknowledged (seen, being \
worked on), resolved (dealt with) or ignored (a real failure we accept); `note` is the part \
worth reading in three months, so write one.",
        obj(
            json!({
                "run_id": sstr("the run id"),
                "state": json!({
                    "type": "string",
                    "enum": TRIAGE_STATES,
                    "description": "acknowledged | resolved | ignored",
                }),
                "note": sstr("why — free text"),
            }),
            &["run_id", "state"],
        ),
    ));
    t.push(tool(
        Access::Write,
        "dagron_clear_triage",
        "Undo a triage decision, putting the run back in the attention queue.",
        obj(json!({ "run_id": sstr("the run id") }), &["run_id"]),
    ));

    // ── Logs ─────────────────────────────────────────────────────────────────
    t.push(tool(
        Access::Read,
        "dagron_get_task_logs",
        "Read a task's captured logs/output within a run. Accepts the same log filter as \
dagron_get_run_logs.",
        obj(log_tool_props(&[("task_id", json!({ "type": "string" }))]), &["run_id", "task_id"]),
    ));
    t.push(tool(
        // The tool an agent should reach for first when a run failed: one call
        // returns every task's output, attributed and filtered, instead of N
        // calls to guess which task printed the error.
        Access::Read,
        "dagron_get_run_logs",
        "Read the whole run's logs as one attributed stream, filtered server-side. \
Use `level`/`q`/`regex`/`exclude` to narrow, `task` to restrict to specific tasks, and `context` \
to keep surrounding lines. Prefer this over per-task reads when diagnosing a failure.",
        obj(
            log_tool_props(&[(
                "task",
                json!({
                    "type": "string",
                    "description": "restrict to these task names or ids (comma-separated)",
                }),
            )]),
            &["run_id"],
        ),
    ));

    // ── Run structure & provenance ───────────────────────────────────────────
    t.push(tool(
        Access::Read,
        "dagron_get_run_graph",
        "The run's DAG as `{nodes[], edges[]}` — the structure behind a failure, including each \
node's status, attempt, wait state and cache hit.",
        obj(json!({ "run_id": sstr("the run id") }), &["run_id"]),
    ));
    t.push(tool(
        Access::Read,
        "dagron_get_run_spec",
        "The DAG YAML this run was actually created from. The way to recover what you submitted \
after an ad-hoc dagron_submit_run.",
        obj(json!({ "run_id": sstr("the run id") }), &["run_id"]),
    ));

    // ── Live events ──────────────────────────────────────────────────────────
    t.push(tool(
        Access::Read,
        "dagron_get_run_events",
        "Bounded read of the run's live event channel (SSE). Connects, collects events emitted \
within `wait_ms`, then returns them. `wait_ms` defaults to 2000 and is capped at 10000 so the \
call always returns promptly.",
        obj(
            json!({
                "run_id": sstr("the run id"),
                "wait_ms": sint("collection window in milliseconds (default 2000)", 100, 10000),
            }),
            &["run_id"],
        ),
    ));

    // ── Workflows: author ────────────────────────────────────────────────────
    t.push(tool(
        Access::Read,
        "dagron_list_workflows",
        "List registered workflows with their schedule and a digest of recent runs. Optionally \
filter by tag. Note: dagron-api returns the whole registry here — there is no server-side \
paging on this route.",
        obj(json!({ "tag": sstr("only workflows carrying this tag") }), &[]),
    ));
    t.push(tool(
        Access::Read,
        "dagron_get_workflow",
        "Get one registered workflow: its spec, description, state and current version.",
        obj(json!({ "workflow_id": sstr("the workflow id") }), &["workflow_id"]),
    ));
    t.push(tool(
        Access::Write,
        "dagron_create_workflow",
        "Register a named workflow from a DAG YAML spec. Registration is what makes a \
`type: workflow` task able to invoke it by name — a parent/child DAG is unreachable without \
this. The name defaults to the spec's own; 409 on a duplicate.",
        obj(
            json!({
                "spec": sstr("the DAG spec (YAML)"),
                "name": sstr("registered name; defaults to the spec's `name:`"),
                "description": sstr("what this workflow is for"),
            }),
            &["spec"],
        ),
    ));
    t.push(tool(
        Access::Write,
        "dagron_update_workflow",
        "Replace a registered workflow's spec (and optionally rename it). The prior definition \
is kept as a version, readable with dagron_list_workflow_versions.",
        obj(
            json!({
                "workflow_id": sstr("the workflow id"),
                "spec": sstr("the new DAG spec (YAML)"),
                "name": sstr("new name; defaults to the spec's `name:`"),
                "description": sstr("what this workflow is for"),
            }),
            &["workflow_id", "spec"],
        ),
    ));
    t.push(tool(
        Access::Write,
        "dagron_delete_workflow",
        "Delete a registered workflow. This cascades its schedules away — prefer \
dagron_set_workflow_state with 'paused' or 'retired' to stop it reversibly.",
        obj(json!({ "workflow_id": sstr("the workflow id") }), &["workflow_id"]),
    ));
    t.push(tool(
        Access::Write,
        "dagron_set_workflow_state",
        "Pause, retire or reactivate a workflow. 'paused' stops it firing while leaving its \
schedules intact; 'retired' is a soft delete that keeps the history; 'active' resumes.",
        obj(
            json!({
                "workflow_id": sstr("the workflow id"),
                "state": json!({
                    "type": "string",
                    "enum": WORKFLOW_STATES,
                    "description": "active | paused | retired",
                }),
            }),
            &["workflow_id", "state"],
        ),
    ));
    t.push(tool(
        Access::Write,
        "dagron_run_workflow",
        "Run a registered workflow by id, with optional arguments for its declared \
`parameters:`. This is how a stored workflow is called as a function — no need to fetch its \
spec and resubmit YAML. 409 when the workflow is paused or retired.",
        obj(
            json!({
                "workflow_id": sstr("the workflow id"),
                "parameters": params_prop("the workflow's"),
            }),
            &["workflow_id"],
        ),
    ));
    t.push(tool(
        Access::Read,
        "dagron_list_workflow_runs",
        "This workflow's run history, newest first.",
        obj(
            json!({
                "workflow_id": sstr("the workflow id"),
                "limit": sint("page size (default 50)", 1, 500),
                "offset": sint("rows to skip", 0, i64::from(u32::MAX)),
            }),
            &["workflow_id"],
        ),
    ));
    t.push(tool(
        Access::Read,
        "dagron_list_workflow_versions",
        "The workflow's append-only definition history, newest first — every spec it has had.",
        obj(json!({ "workflow_id": sstr("the workflow id") }), &["workflow_id"]),
    ));

    // ── Artifacts ────────────────────────────────────────────────────────────
    t.push(tool(
        Access::Read,
        "dagron_get_artifact",
        "Read a file a task wrote to the artifact store. Text artifacts come back inline; \
anything binary, or larger than the inline cap, comes back as size + content type + locator \
rather than inflating your context with base64.",
        obj(
            json!({
                "run_id": sstr("the run id"),
                "task": sstr("the task that produced it"),
                "name": sstr("the artifact's name"),
            }),
            &["run_id", "task", "name"],
        ),
    ));
    t.push(tool(
        Access::Read,
        "dagron_artifact_exists",
        "Check whether an artifact exists without transferring it.",
        obj(
            json!({
                "run_id": sstr("the run id"),
                "task": sstr("the task that produced it"),
                "name": sstr("the artifact's name"),
            }),
            &["run_id", "task", "name"],
        ),
    ));
    t.push(tool(
        Access::Write,
        "dagron_put_artifact",
        "Write a text artifact into the store — the way to seed an input file a DAG will read.",
        obj(
            json!({
                "run_id": sstr("the run id"),
                "task": sstr("the task name to file it under"),
                "name": sstr("the artifact's name"),
                "content": sstr("the artifact's contents (text)"),
            }),
            &["run_id", "task", "name", "content"],
        ),
    ));

    // ── Observability ────────────────────────────────────────────────────────
    t.push(tool(
        Access::Read,
        "dagron_get_metrics",
        "Cluster-internal snapshot: run/task counts by status and dead-letter total.",
        no_args(),
    ));
    t.push(tool(
        Access::Read,
        "dagron_get_metrics_timeseries",
        "Per-day run counts by outcome plus duration stats, for spotting a trend rather than a \
single failure.",
        obj(
            json!({
                "days": sint("look-back window (default 14)", 1, 90),
                "name": sstr("restrict to one workflow definition name"),
            }),
            &[],
        ),
    ));
    t.push(tool(
        Access::Read,
        "dagron_get_health",
        "Instance health: scheduler leadership, event-listener state, and the attention counters \
(active runs, awaiting approvals, dead letters).",
        no_args(),
    ));
    t.push(tool(
        Access::Read,
        "dagron_search",
        "Find workflows, runs and schedules by name or id prefix. Start here when you have a \
human name (\"the nightly ETL\") and need an id — every other tool takes ids.",
        obj(
            json!({
                "q": sstr("name or id prefix to search for"),
                "limit": sint("per-category cap (default 8)", 1, 20),
            }),
            &["q"],
        ),
    ));

    // ── Dead letters ─────────────────────────────────────────────────────────
    t.push(tool(
        Access::Read,
        "dagron_list_dead_letters",
        "Inspect the poison queue (parked submissions). `limit` defaults to 100, capped at 500.",
        obj(json!({ "limit": sint("page size (default 100)", 1, 500) }), &[]),
    ));
    t.push(tool(
        Access::Write,
        "dagron_redrive_dead_letter",
        "Re-submit a parked payload as a fresh run and drop it from the queue.",
        obj(json!({ "id": sstr("the dead-letter id") }), &["id"]),
    ));
    t.push(tool(
        Access::Write,
        "dagron_delete_dead_letter",
        "Discard a parked payload permanently.",
        obj(json!({ "id": sstr("the dead-letter id") }), &["id"]),
    ));

    // ── Lineage ──────────────────────────────────────────────────────────────
    t.push(tool(
        Access::Read,
        "dagron_list_datasets",
        "The dataset registry: each dataset's uri, when it was last updated, by which run, and \
which workflows consume it.",
        obj(json!({ "limit": sint("page size (default 100)", 1, 500) }), &[]),
    ));
    t.push(tool(
        Access::Read,
        "dagron_get_dataset_events",
        "The lineage ledger: who wrote what, when. Pass `uri` to follow one dataset.",
        obj(
            json!({
                "uri": sstr("restrict the trail to this dataset uri"),
                "limit": sint("page size (default 100)", 1, 500),
            }),
            &[],
        ),
    ));

    // ── Archive ──────────────────────────────────────────────────────────────
    t.push(tool(
        Access::Read,
        "dagron_list_archived_runs",
        "Page the archive index (runs aged out of the live tables), newest finished first.",
        obj(
            json!({
                "name": sstr("restrict to one workflow name"),
                "limit": sint("page size (default 100)", 1, 500),
                "offset": sint("rows to skip", 0, i64::from(u32::MAX)),
            }),
            &[],
        ),
    ));
    t.push(tool(
        Access::Read,
        "dagron_get_archived_run",
        "The full archive document for one run (run + definition + tasks + events). Answers 410 \
once the run has been compacted to the Parquet dataset.",
        obj(json!({ "run_id": sstr("the archived run id") }), &["run_id"]),
    ));

    t
}

/// The full MCP tool catalogue (name, description, JSON-Schema input).
///
/// Unfiltered on purpose: this is the catalogue as a *definition*. What a given
/// server advertises is [`tool_defs_for`].
pub fn tool_defs() -> Vec<Value> {
    catalogue().into_iter().map(|(_, def)| def).collect()
}

/// The catalogue a server should advertise. `readonly` hides every write tool —
/// not merely refusing it on call, because a tool an agent can see is a tool it
/// will plan around.
pub fn tool_defs_for(readonly: bool) -> Vec<Value> {
    catalogue()
        .into_iter()
        .filter(|(access, _)| !readonly || *access == Access::Read)
        .map(|(_, def)| def)
        .collect()
}

/// Whether `name` reads or writes — `None` for a tool this catalogue doesn't have.
pub fn tool_access(name: &str) -> Option<Access> {
    catalogue().into_iter().find_map(|(access, def)| (def["name"] == name).then_some(access))
}

// ── Response rendering ────────────────────────────────────────────────────────

/// Non-2xx statuses that are *answers* rather than malfunctions: the workflow is
/// paused, the gate isn't awaiting approval, the run was compacted, the id is
/// unknown. An agent should branch on these, so they come back as structured
/// results with `isError: false`.
fn state_label(status: u16) -> Option<&'static str> {
    match status {
        404 => Some("not_found"),
        409 => Some("conflict"),
        410 => Some("gone"),
        _ => None,
    }
}

/// Turn a dagron-api response into tool output.
fn render(resp: ApiResponse) -> Result<String> {
    if resp.is_success() {
        // A bodyless 2xx (a DELETE, say) still has to say *something* an agent
        // can parse — "" would read as a failed call.
        return Ok(if resp.body.trim().is_empty() {
            json!({ "status": resp.status, "ok": true }).to_string()
        } else {
            resp.body
        });
    }
    if let Some(outcome) = state_label(resp.status) {
        return Ok(json!({
            "status": resp.status,
            "outcome": outcome,
            "detail": detail(&resp.body),
        })
        .to_string());
    }
    anyhow::bail!("dagron-api returned {}: {}", resp.status, resp.body)
}

/// A response body as JSON when it is JSON, as text otherwise — so a structured
/// error keeps its structure instead of arriving as an escaped string.
fn detail(body: &str) -> Value {
    if body.trim().is_empty() {
        return Value::Null;
    }
    serde_json::from_str::<Value>(body).unwrap_or_else(|_| Value::String(body.to_string()))
}

/// An `Idempotency-Key`, validated against the same rules dagron-api applies, so
/// a bad key is a fast tool error rather than a 400 after the round trip.
fn idempotency_key(raw: &str) -> Result<String> {
    let key = raw.trim();
    if key.is_empty() {
        anyhow::bail!("`idempotency_key` must not be empty");
    }
    if key.len() > MAX_IDEMPOTENCY_KEY {
        anyhow::bail!("`idempotency_key` must be at most {MAX_IDEMPOTENCY_KEY} characters");
    }
    if !key.chars().all(|c| c.is_ascii_graphic()) {
        anyhow::bail!("`idempotency_key` must be printable ASCII without spaces");
    }
    Ok(key.to_string())
}

/// Serialize a JSON body for a POST/PUT.
fn body(v: Value) -> Option<(String, &'static str)> {
    Some((v.to_string(), "application/json"))
}

// ── Dispatch ──────────────────────────────────────────────────────────────────

/// Execute a tool against dagron-api, returning the response text.
pub async fn call_tool(client: &DagronClient, name: &str, args: &Value) -> Result<String> {
    // Fail closed: a write tool stays refused in read-only mode even if some
    // composing server advertised it anyway.
    if client.readonly() && tool_access(name) == Some(Access::Write) {
        anyhow::bail!(
            "`{name}` changes cluster state and this server is running read-only \
             (DAGRON_MCP_READONLY); unset it to enable the write tools"
        );
    }
    let a = Args::new(args);
    match name {
        // ── Runs: drive ──────────────────────────────────────────────────────
        "dagron_list_runs" => {
            let mut q = Query::new();
            q.opt("status", a.opt_str("status")?);
            q.opt("name", a.opt_str("name")?);
            q.opt("trigger", a.opt_str("trigger")?);
            q.opt_int("limit", a.opt_int("limit", 1, 500)?);
            q.opt_int("offset", a.opt_int("offset", 0, i64::from(u32::MAX))?);
            get(client, &format!("/api/runs{}", q.finish())).await
        }
        "dagron_get_run" => get(client, &format!("/api/runs/{}", a.id("run_id")?)).await,
        "dagron_submit_run" => {
            // `POST /api/runs` takes a JSON envelope (`{"yaml": "…"}`), not a raw
            // YAML body: its handler binds `Json<SubmitBody>`, which rejects any
            // request that isn't `application/json` with a 415 before the handler
            // runs. Posting the spec as `application/yaml` meant the one tool an
            // agent needs most — submit a workflow — never reached the engine.
            let mut payload = json!({ "yaml": a.str("yaml")? });
            if let Some(p) = a.str_map("parameters")? {
                payload["parameters"] = json!(p);
            }
            // The header, not a body field: idempotency is a transport-level
            // retry contract and dagron-api reads it from `Idempotency-Key`.
            let headers = match a.opt_str("idempotency_key")? {
                Some(k) => vec![("idempotency-key", idempotency_key(k)?)],
                None => vec![],
            };
            render(
                client
                    .request(Method::Post, "/api/runs", body(payload), &headers)
                    .await?,
            )
        }
        "dagron_cancel_run" => {
            post(client, &format!("/api/runs/{}/cancel", a.id("run_id")?), None).await
        }
        "dagron_wait_run" => {
            let run_id = a.id("run_id")?;
            let timeout = a.int("timeout_secs", 1, 600, 30)?;
            get(client, &format!("/api/runs/{run_id}/wait?timeout_secs={timeout}")).await
        }

        // ── Runs: recover ────────────────────────────────────────────────────
        "dagron_rerun_run" => {
            let run_id = a.id("run_id")?;
            // The route takes `Option<Json<RerunBody>>`: sending no body at all
            // is the pre-existing call shape, so only send one when asked to.
            let payload = match a.opt_str("from")? {
                Some(from) => body(json!({ "from": from })),
                None => None,
            };
            post(client, &format!("/api/runs/{run_id}/rerun"), payload).await
        }
        "dagron_resubmit_run" => {
            post(client, &format!("/api/runs/{}/resubmit", a.id("run_id")?), None).await
        }
        "dagron_retry_task" => {
            let (run, task) = (a.id("run_id")?, a.id("task_id")?);
            post(client, &format!("/api/runs/{run}/tasks/{task}/retry"), None).await
        }
        "dagron_clear_task" => {
            let (run, task) = (a.id("run_id")?, a.id("task_id")?);
            post(client, &format!("/api/runs/{run}/tasks/{task}/clear"), None).await
        }

        // ── Approval gates ───────────────────────────────────────────────────
        "dagron_list_approvals" => get(client, "/api/approvals").await,
        "dagron_approve_task" => {
            let (run, task) = (a.id("run_id")?, a.id("task_id")?);
            post(client, &format!("/api/runs/{run}/tasks/{task}/approve"), None).await
        }
        "dagron_reject_task" => {
            let (run, task) = (a.id("run_id")?, a.id("task_id")?);
            post(client, &format!("/api/runs/{run}/tasks/{task}/reject"), None).await
        }

        // ── Triage ───────────────────────────────────────────────────────────
        "dagron_triage_run" => {
            let run_id = a.id("run_id")?;
            let state = a.one_of("state", TRIAGE_STATES)?;
            let mut payload = json!({ "state": state });
            if let Some(note) = a.opt_str("note")? {
                payload["note"] = json!(note);
            }
            post(client, &format!("/api/runs/{run_id}/triage"), body(payload)).await
        }
        "dagron_clear_triage" => {
            let run_id = a.id("run_id")?;
            render(
                client
                    .request(Method::Delete, &format!("/api/runs/{run_id}/triage"), None, &[])
                    .await?,
            )
        }

        // ── Logs ─────────────────────────────────────────────────────────────
        "dagron_get_task_logs" => {
            let (run, task) = (a.id("run_id")?, a.id("task_id")?);
            get(
                client,
                &format!("/api/runs/{run}/tasks/{task}/logs{}", log_filter_query(args)?),
            )
            .await
        }
        "dagron_get_run_logs" => {
            let run = a.id("run_id")?;
            get(client, &format!("/api/runs/{run}/logs{}", log_filter_query(args)?)).await
        }

        // ── Run structure & provenance ───────────────────────────────────────
        "dagron_get_run_graph" => {
            get(client, &format!("/api/runs/{}/graph", a.id("run_id")?)).await
        }
        "dagron_get_run_spec" => get(client, &format!("/api/runs/{}/spec", a.id("run_id")?)).await,

        // ── Live events ──────────────────────────────────────────────────────
        "dagron_get_run_events" => {
            // Bounded poll over the per-run SSE channel. The budget is hard-capped
            // so an MCP tool call always returns promptly — even a never-emitting
            // run won't block the AI agent past `wait_ms`.
            let run_id = a.id("run_id")?;
            let wait_ms = a.int("wait_ms", 100, 10_000, 2000)?;
            let raw = client
                .read_sse(&format!("/api/runs/{run_id}/stream"), Duration::from_millis(wait_ms as u64))
                .await?;
            let events = parse_sse(&raw);
            Ok(json!({
                "run_id": run_id,
                "wait_ms": wait_ms,
                "event_count": events.len(),
                "events": events,
            })
            .to_string())
        }

        // ── Workflows ────────────────────────────────────────────────────────
        "dagron_list_workflows" => {
            let mut q = Query::new();
            q.opt("tag", a.opt_str("tag")?);
            get(client, &format!("/api/workflows{}", q.finish())).await
        }
        "dagron_get_workflow" => {
            get(client, &format!("/api/workflows/{}", a.id("workflow_id")?)).await
        }
        "dagron_create_workflow" => {
            let payload = upsert_body(&a)?;
            render(client.request(Method::Post, "/api/workflows", body(payload), &[]).await?)
        }
        "dagron_update_workflow" => {
            let id = a.id("workflow_id")?;
            let payload = upsert_body(&a)?;
            render(
                client
                    .request(Method::Put, &format!("/api/workflows/{id}"), body(payload), &[])
                    .await?,
            )
        }
        "dagron_delete_workflow" => {
            let id = a.id("workflow_id")?;
            render(
                client
                    .request(Method::Delete, &format!("/api/workflows/{id}"), None, &[])
                    .await?,
            )
        }
        "dagron_set_workflow_state" => {
            let id = a.id("workflow_id")?;
            let state = a.one_of("state", WORKFLOW_STATES)?;
            post(client, &format!("/api/workflows/{id}/state"), body(json!({ "state": state })))
                .await
        }
        "dagron_run_workflow" => {
            let id = a.id("workflow_id")?;
            // Bodyless when there are no arguments: the route took no body at all
            // before `parameters` existed, and that call shape stays valid.
            let payload = a.str_map("parameters")?.map(|p| json!({ "parameters": p })).and_then(body);
            post(client, &format!("/api/workflows/{id}/run"), payload).await
        }
        "dagron_list_workflow_runs" => {
            let id = a.id("workflow_id")?;
            let mut q = Query::new();
            q.opt_int("limit", a.opt_int("limit", 1, 500)?);
            q.opt_int("offset", a.opt_int("offset", 0, i64::from(u32::MAX))?);
            get(client, &format!("/api/workflows/{id}/runs{}", q.finish())).await
        }
        "dagron_list_workflow_versions" => {
            get(client, &format!("/api/workflows/{}/versions", a.id("workflow_id")?)).await
        }

        // ── Artifacts ────────────────────────────────────────────────────────
        "dagron_get_artifact" => {
            let path = artifact_path(&a)?;
            let resp = client.request_bytes(&path).await?;
            if !resp.is_success() {
                // Reuse the state rendering: a missing artifact is a 404 answer.
                return render(ApiResponse {
                    status: resp.status,
                    body: String::from_utf8_lossy(&resp.bytes).into_owned(),
                });
            }
            Ok(describe_artifact(&path, resp, client.max_artifact_bytes()))
        }
        "dagron_artifact_exists" => get(client, &format!("{}/exists", artifact_path(&a)?)).await,
        "dagron_put_artifact" => {
            let path = artifact_path(&a)?;
            let content = a.raw_str("content")?.to_string();
            render(
                client
                    .request(Method::Put, &path, Some((content, "application/octet-stream")), &[])
                    .await?,
            )
        }

        // ── Observability ────────────────────────────────────────────────────
        "dagron_get_metrics" => get(client, "/api/metrics").await,
        "dagron_get_metrics_timeseries" => {
            let mut q = Query::new();
            q.opt_int("days", a.opt_int("days", 1, 90)?);
            q.opt("name", a.opt_str("name")?);
            get(client, &format!("/api/metrics/timeseries{}", q.finish())).await
        }
        "dagron_get_health" => get(client, "/api/health").await,
        "dagron_search" => {
            let mut q = Query::new();
            q.opt("q", Some(a.str("q")?));
            q.opt_int("limit", a.opt_int("limit", 1, 20)?);
            get(client, &format!("/api/search{}", q.finish())).await
        }

        // ── Dead letters ─────────────────────────────────────────────────────
        "dagron_list_dead_letters" => {
            let mut q = Query::new();
            q.opt_int("limit", a.opt_int("limit", 1, 500)?);
            get(client, &format!("/api/dead-letters{}", q.finish())).await
        }
        "dagron_redrive_dead_letter" => {
            post(client, &format!("/api/dead-letters/{}/redrive", a.id("id")?), None).await
        }
        "dagron_delete_dead_letter" => {
            let id = a.id("id")?;
            render(
                client
                    .request(Method::Delete, &format!("/api/dead-letters/{id}"), None, &[])
                    .await?,
            )
        }

        // ── Lineage ──────────────────────────────────────────────────────────
        "dagron_list_datasets" => {
            let mut q = Query::new();
            q.opt_int("limit", a.opt_int("limit", 1, 500)?);
            get(client, &format!("/api/datasets{}", q.finish())).await
        }
        "dagron_get_dataset_events" => {
            let mut q = Query::new();
            q.opt("uri", a.opt_str("uri")?);
            q.opt_int("limit", a.opt_int("limit", 1, 500)?);
            get(client, &format!("/api/datasets/events{}", q.finish())).await
        }

        // ── Archive ──────────────────────────────────────────────────────────
        "dagron_list_archived_runs" => {
            let mut q = Query::new();
            q.opt("name", a.opt_str("name")?);
            q.opt_int("limit", a.opt_int("limit", 1, 500)?);
            q.opt_int("offset", a.opt_int("offset", 0, i64::from(u32::MAX))?);
            get(client, &format!("/api/archive/runs{}", q.finish())).await
        }
        "dagron_get_archived_run" => {
            get(client, &format!("/api/archive/runs/{}", a.id("run_id")?)).await
        }

        other => anyhow::bail!("unknown tool `{other}`"),
    }
}

/// `GET path`, rendered.
async fn get(client: &DagronClient, path: &str) -> Result<String> {
    render(client.request(Method::Get, path, None, &[]).await?)
}

/// `POST path`, rendered.
async fn post(
    client: &DagronClient,
    path: &str,
    payload: Option<(String, &'static str)>,
) -> Result<String> {
    render(client.request(Method::Post, path, payload, &[]).await?)
}

/// The `{spec, name?, description?}` envelope both workflow writes take.
fn upsert_body(a: &Args<'_>) -> Result<Value> {
    let mut payload = json!({ "spec": a.str("spec")? });
    if let Some(name) = a.opt_str("name")? {
        payload["name"] = json!(name);
    }
    if let Some(d) = a.opt_str("description")? {
        payload["description"] = json!(d);
    }
    Ok(payload)
}

/// `/api/runs/{run_id}/artifacts/{task}/{name}` with each segment encoded.
fn artifact_path(a: &Args<'_>) -> Result<String> {
    Ok(format!(
        "/api/runs/{}/artifacts/{}/{}",
        a.id("run_id")?,
        a.seg("task")?,
        a.seg("name")?
    ))
}

/// Describe a fetched artifact for an agent.
///
/// Binary bodies do not fit JSON-RPC, and base64-inflating a 40 MB artifact into
/// a context window is a denial of service the agent performs on itself. So:
/// inline the bytes only when they are valid UTF-8 *and* under the cap;
/// otherwise hand back size, content type and the locator to fetch it with
/// something that isn't a language model.
fn describe_artifact(path: &str, resp: crate::BytesResponse, cap: usize) -> String {
    let text = std::str::from_utf8(&resp.bytes).ok();
    let mut out = json!({
        "locator": path,
        "bytes": resp.bytes.len(),
        "content_type": resp.content_type,
    });
    match text {
        Some(t) if resp.bytes.len() <= cap => {
            out["encoding"] = json!("text");
            out["content"] = json!(t);
        }
        Some(_) => {
            out["encoding"] = json!("text");
            out["omitted"] = json!(true);
            out["reason"] = json!(format!(
                "artifact is {} bytes, over the {cap}-byte inline cap \
                 (DAGRON_MCP_MAX_ARTIFACT_BYTES); fetch it with the locator",
                resp.bytes.len()
            ));
        }
        None => {
            out["encoding"] = json!("binary");
            out["omitted"] = json!(true);
            out["reason"] =
                json!("artifact is not valid UTF-8; fetch it with the locator");
        }
    }
    out.to_string()
}

// ── SSE ───────────────────────────────────────────────────────────────────────

/// Minimal SSE parser: split on blank lines and gather `event:` / `data:` lines
/// per event. Multi-line `data:` is joined with `\n` per the spec. Comment
/// lines (leading `:`) and unknown fields are ignored.
fn parse_sse(raw: &str) -> Vec<Value> {
    let mut out = Vec::new();
    let mut name: Option<String> = None;
    let mut data: Vec<String> = Vec::new();
    let flush = |name: &mut Option<String>, data: &mut Vec<String>, out: &mut Vec<Value>| {
        if name.is_none() && data.is_empty() {
            return;
        }
        let joined = data.join("\n");
        let parsed = serde_json::from_str::<Value>(&joined).unwrap_or(Value::String(joined));
        out.push(json!({
            "event": name.clone().unwrap_or_else(|| "message".to_string()),
            "data": parsed,
        }));
        *name = None;
        data.clear();
    };
    for line in raw.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() {
            flush(&mut name, &mut data, &mut out);
            continue;
        }
        if line.starts_with(':') {
            continue;
        }
        let (field, value) = match line.split_once(':') {
            Some((f, v)) => (f, v.strip_prefix(' ').unwrap_or(v)),
            None => (line, ""),
        };
        match field {
            "event" => name = Some(value.to_string()),
            "data" => data.push(value.to_string()),
            _ => {}
        }
    }
    // The final event may not be terminated by a blank line if the read window
    // closed mid-frame — flush so the agent still sees it.
    flush(&mut name, &mut data, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BytesResponse;

    /// A client pointed at a closed port: every tool that reaches HTTP fails
    /// fast with a connection error instead of a DNS timeout, which is what makes
    /// [`every_advertised_tool_dispatches`] cheap enough to run in CI.
    fn client() -> DagronClient {
        DagronClient::new("http://127.0.0.1:1", None)
    }

    fn names() -> Vec<String> {
        tool_defs().iter().map(|t| t["name"].as_str().unwrap().to_string()).collect()
    }

    async fn call(name: &str, args: Value) -> Result<String> {
        call_tool(&client(), name, &args).await
    }

    // ── Catalogue ────────────────────────────────────────────────────────────

    /// The failure mode this surface is most prone to: a tool advertised in
    /// `tools/list` that `call_tool` has no arm for, so the agent picks it and
    /// gets "unknown tool" for its trouble.
    #[tokio::test]
    async fn every_advertised_tool_dispatches() {
        for name in names() {
            let err = call(&name, json!({})).await.err().map(|e| e.to_string()).unwrap_or_default();
            assert!(
                !err.contains("unknown tool"),
                "{name} is advertised but not dispatched"
            );
        }
    }

    /// Conversely: nothing dispatches that isn't advertised, or read-only mode
    /// has a hole (the gate is keyed on the catalogue).
    #[test]
    fn every_tool_declares_its_access() {
        for name in names() {
            assert!(tool_access(&name).is_some(), "{name} has no access classification");
        }
        assert!(tool_access("dagron_nonexistent").is_none());
    }

    /// The roadmap in `docs/MCP.md`: P0 (author + wait + artifacts), P1 (recover
    /// and gates), P2 (visibility and lineage). Named explicitly so removing one
    /// is a deliberate act rather than an edit nobody noticed.
    #[test]
    fn the_roadmap_tools_are_all_present() {
        let have = names();
        for expected in [
            // P0 — an agent cannot author a workflow
            "dagron_create_workflow",
            "dagron_list_workflows",
            "dagron_get_workflow",
            "dagron_update_workflow",
            "dagron_delete_workflow",
            "dagron_set_workflow_state",
            "dagron_run_workflow",
            "dagron_wait_run",
            "dagron_get_artifact",
            "dagron_artifact_exists",
            // P1 — recovery and gates
            "dagron_list_approvals",
            "dagron_approve_task",
            "dagron_reject_task",
            "dagron_rerun_run",
            "dagron_resubmit_run",
            "dagron_retry_task",
            "dagron_clear_task",
            "dagron_redrive_dead_letter",
            "dagron_delete_dead_letter",
            "dagron_triage_run",
            "dagron_clear_triage",
            // P2 — visibility and lineage
            "dagron_search",
            "dagron_get_run_graph",
            "dagron_get_run_spec",
            "dagron_get_health",
            "dagron_list_workflow_runs",
            "dagron_list_workflow_versions",
            "dagron_list_datasets",
            "dagron_get_dataset_events",
            "dagron_get_metrics_timeseries",
            "dagron_list_archived_runs",
            "dagron_get_archived_run",
            "dagron_put_artifact",
        ] {
            assert!(have.contains(&expected.to_string()), "missing roadmap tool {expected}");
        }
    }

    /// P3 stays P3: routes we decided *not* to wrap must not appear because
    /// someone found them convenient. Secrets and tokens are the sharp ones — a
    /// tool that mints a credential hands prompt-injection a credential.
    #[test]
    fn the_declined_surface_stayed_declined() {
        for banned in ["token", "secret", "login", "environment", "git_repo", "schedule", "backfill"]
        {
            for name in names() {
                assert!(
                    !name.contains(banned),
                    "{name} exposes {banned}, which docs/MCP.md tier P3 declines"
                );
            }
        }
    }

    #[test]
    fn readonly_catalogue_is_the_read_half() {
        let full = tool_defs_for(false);
        let ro = tool_defs_for(true);
        assert!(ro.len() < full.len(), "read-only mode must actually hide something");
        for t in &ro {
            let name = t["name"].as_str().unwrap();
            assert_eq!(tool_access(name), Some(Access::Read), "{name} is not a read tool");
        }
    }

    #[test]
    fn every_schema_is_a_closed_object() {
        for t in tool_defs() {
            let schema = &t["inputSchema"];
            assert_eq!(schema["type"], "object", "{} has a non-object schema", t["name"]);
            assert_eq!(
                schema["additionalProperties"], false,
                "{} accepts unknown arguments",
                t["name"]
            );
            // Every `required` entry must actually be declared, or the model is
            // told to send an argument the tool will reject as unknown.
            let props = schema["properties"].as_object().unwrap();
            for req in schema["required"].as_array().unwrap_or(&vec![]) {
                let key = req.as_str().unwrap();
                assert!(props.contains_key(key), "{} requires undeclared `{key}`", t["name"]);
            }
        }
    }

    // ── Argument validation ──────────────────────────────────────────────────

    #[tokio::test]
    async fn unsafe_ids_are_rejected_before_any_request() {
        // Crafted ids with path/query characters must not reach the HTTP client.
        for bad in ["../secrets", "1/cancel", "a?b", "x y", ""] {
            let err = call("dagron_get_run", json!({ "run_id": bad })).await.unwrap_err().to_string();
            assert!(
                err.contains("`run_id`"),
                "expected local run_id validation before any request for {bad:?}, got {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn listings_are_bounded_locally() {
        // A malformed page argument fails as a fast tool error rather than a
        // server round trip — `docs/MCP.md`, "bound every listing locally".
        for (tool, arg, bad) in [
            ("dagron_list_dead_letters", "limit", json!(0)),
            ("dagron_list_dead_letters", "limit", json!(501)),
            ("dagron_list_dead_letters", "limit", json!("ten")),
            ("dagron_list_runs", "limit", json!(9999)),
            ("dagron_list_datasets", "limit", json!(-1)),
            ("dagron_get_metrics_timeseries", "days", json!(91)),
            ("dagron_search", "limit", json!(21)),
        ] {
            let err = call(tool, json!({ arg: bad, "q": "x" })).await.unwrap_err().to_string();
            assert!(err.contains(arg), "{tool}.{arg}={bad} should fail locally, got {err:?}");
        }
    }

    #[tokio::test]
    async fn run_events_rejects_out_of_range_wait() {
        let err = call("dagron_get_run_events", json!({ "run_id": "abc", "wait_ms": 50 }))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("wait_ms"), "want wait_ms in error, got {err:?}");
    }

    #[tokio::test]
    async fn enum_arguments_name_the_legal_values() {
        let err = call("dagron_set_workflow_state", json!({ "workflow_id": "w1", "state": "off" }))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("active"), "got {err:?}");
        let err = call("dagron_triage_run", json!({ "run_id": "r1", "state": "meh" }))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("acknowledged"), "got {err:?}");
    }

    #[tokio::test]
    async fn parameters_must_be_strings() {
        // dagron `parameters:` are string substitutions. Coercing `3` to "3"
        // would leave the agent believing it passed an integer.
        let err = call("dagron_submit_run", json!({ "yaml": "name: w", "parameters": { "n": 3 } }))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("`parameters.n`"), "got {err:?}");
    }

    #[test]
    fn idempotency_keys_are_validated_the_way_dagron_api_validates_them() {
        assert!(idempotency_key("  6f1e2b9c-run-42 ").unwrap() == "6f1e2b9c-run-42");
        assert!(idempotency_key("   ").is_err());
        assert!(idempotency_key(&"k".repeat(MAX_IDEMPOTENCY_KEY + 1)).is_err());
        assert!(idempotency_key("has space").is_err());
    }

    // ── Log filters ──────────────────────────────────────────────────────────

    #[test]
    fn log_filter_query_encodes_only_what_was_passed() {
        assert_eq!(log_filter_query(&json!({ "run_id": "r1" })).unwrap(), "");
        assert_eq!(
            log_filter_query(&json!({ "run_id": "r1", "level": "error", "context": "2" })).unwrap(),
            "?level=error&context=2",
        );
        // An empty string is "not set", not "match the empty string" — an agent
        // filling in a blank argument must not accidentally filter.
        assert_eq!(log_filter_query(&json!({ "q": "" })).unwrap(), "");
        // A regex with query-significant characters must survive as one value.
        assert_eq!(
            log_filter_query(&json!({ "regex": "a&b[0-9]+" })).unwrap(),
            "?regex=a%26b%5B0-9%5D%2B",
        );
        // Wrong types fail locally with a clear message rather than as an
        // opaque HTTP 400 from dagron-api.
        assert!(log_filter_query(&json!({ "limit": 100 })).is_err());
    }

    #[test]
    fn both_log_tools_offer_the_same_filter() {
        let defs = tool_defs();
        let props = |name: &str| -> Vec<String> {
            let t = defs.iter().find(|t| t["name"] == name).expect("tool present");
            let mut k: Vec<String> =
                t["inputSchema"]["properties"].as_object().unwrap().keys().cloned().collect();
            k.sort();
            k
        };
        let task = props("dagron_get_task_logs");
        let run = props("dagron_get_run_logs");
        for (key, _) in LOG_FILTER_ARGS {
            assert!(task.contains(&key.to_string()), "task tool missing {key}");
            assert!(run.contains(&key.to_string()), "run tool missing {key}");
        }
        assert!(task.contains(&"task_id".to_string()));
        assert!(run.contains(&"task".to_string()));
    }

    // ── Response rendering ───────────────────────────────────────────────────

    #[test]
    fn a_conflict_is_an_answer_not_a_tool_error() {
        // Paused workflow, gate not awaiting approval, task not completed: states
        // the agent should reason about, so they come back as structured results.
        for (status, outcome) in [(404u16, "not_found"), (409, "conflict"), (410, "gone")] {
            let out = render(ApiResponse {
                status,
                body: r#"{"error":"workflow is paused"}"#.into(),
            })
            .expect("state answers are not tool errors");
            let v: Value = serde_json::from_str(&out).unwrap();
            assert_eq!(v["status"], status);
            assert_eq!(v["outcome"], outcome);
            assert_eq!(v["detail"]["error"], "workflow is paused");
        }
    }

    #[test]
    fn transport_and_auth_failures_stay_tool_errors() {
        for status in [401u16, 403, 422, 500, 503] {
            assert!(
                render(ApiResponse { status, body: "nope".into() }).is_err(),
                "{status} should be a tool error"
            );
        }
    }

    #[test]
    fn a_bodyless_success_still_says_something() {
        // "" would read to an agent as a failed call.
        let out = render(ApiResponse { status: 204, body: String::new() }).unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["ok"], true);
        assert_eq!(v["status"], 204);
    }

    #[test]
    fn a_non_json_error_body_survives_as_text() {
        let out = render(ApiResponse { status: 409, body: "run 'r1' not found".into() }).unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["detail"], "run 'r1' not found");
    }

    // ── Artifacts ────────────────────────────────────────────────────────────

    #[test]
    fn a_text_artifact_comes_back_inline() {
        let out = describe_artifact(
            "/api/runs/r1/artifacts/build/out.txt",
            BytesResponse {
                status: 200,
                content_type: Some("application/octet-stream".into()),
                bytes: b"hello".to_vec(),
            },
            1024,
        );
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["encoding"], "text");
        assert_eq!(v["content"], "hello");
        assert_eq!(v["bytes"], 5);
    }

    #[test]
    fn binary_and_oversized_artifacts_are_described_not_inlined() {
        // Base64-inflating a 40 MB artifact into a context window is a denial of
        // service the agent performs on itself.
        let big = describe_artifact(
            "/api/runs/r1/artifacts/build/big.txt",
            BytesResponse { status: 200, content_type: None, bytes: vec![b'a'; 4096] },
            1024,
        );
        let v: Value = serde_json::from_str(&big).unwrap();
        assert_eq!(v["omitted"], true);
        assert!(v["content"].is_null());
        assert!(v["reason"].as_str().unwrap().contains("inline cap"));

        let bin = describe_artifact(
            "/api/runs/r1/artifacts/build/blob.bin",
            BytesResponse { status: 200, content_type: None, bytes: vec![0xff, 0xfe, 0x00] },
            1024,
        );
        let v: Value = serde_json::from_str(&bin).unwrap();
        assert_eq!(v["encoding"], "binary");
        assert_eq!(v["omitted"], true);
        assert_eq!(v["locator"], "/api/runs/r1/artifacts/build/blob.bin");
    }

    // ── SSE ──────────────────────────────────────────────────────────────────

    #[test]
    fn sse_parser_handles_multi_line_and_named_events() {
        let raw = "event: task\ndata: {\"run_id\":\"r1\"}\n\n\
                   : keepalive\n\n\
                   data: line1\ndata: line2\n\n";
        let events = parse_sse(raw);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["event"], "task");
        assert_eq!(events[0]["data"]["run_id"], "r1");
        assert_eq!(events[1]["event"], "message");
        assert_eq!(events[1]["data"], "line1\nline2");
    }

    #[test]
    fn sse_parser_flushes_unterminated_tail() {
        // Bounded read may close the connection mid-frame; the last event
        // must not be dropped silently.
        let raw = "event: resync\ndata: lagged";
        let events = parse_sse(raw);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["event"], "resync");
        assert_eq!(events[0]["data"], "lagged");
    }

    // ── Wire shape ───────────────────────────────────────────────────────────

    /// Serve exactly one HTTP request on an ephemeral port and hand back the
    /// request head and body. Small enough to keep in-crate, and the only way to
    /// assert the *wire shape* of a tool call — which is where a submit that
    /// dagron-api answers with 415 hides from every other kind of test.
    async fn capture_one_request(
        response: &'static str,
    ) -> (String, tokio::task::JoinHandle<(String, String)>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut raw = Vec::new();
            let mut buf = [0u8; 1024];
            // Read the head, then exactly Content-Length bytes of body.
            loop {
                let n = sock.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                raw.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&raw).into_owned();
                let Some((head, body)) = text.split_once("\r\n\r\n") else { continue };
                let len: usize = head
                    .lines()
                    .find_map(|l| {
                        let (k, v) = l.split_once(':')?;
                        k.eq_ignore_ascii_case("content-length").then(|| v.trim().parse().ok())?
                    })
                    .unwrap_or(0);
                if body.len() >= len {
                    break;
                }
            }
            let text = String::from_utf8_lossy(&raw).into_owned();
            let (head, body) = text.split_once("\r\n\r\n").unwrap();
            sock.write_all(response.as_bytes()).await.unwrap();
            sock.flush().await.unwrap();
            (head.to_string(), body.to_string())
        });
        (base, handle)
    }

    const CREATED_RUN: &str = "HTTP/1.1 201 Created\r\ncontent-type: application/json\r\n\
                               content-length: 21\r\n\r\n{\"run_id\":\"run-0001\"}";
    /// A response that promises 64 bytes and delivers 5, then hangs up — the
    /// shape of a connection dropped mid-body.
    const TRUNCATED: &str = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                             content-length: 64\r\n\r\nshort";
    const CONFLICT: &str = "HTTP/1.1 409 Conflict\r\ncontent-type: text/plain\r\n\
                            content-length: 18\r\n\r\nworkflow is paused";

    #[tokio::test]
    async fn submit_posts_the_spec_as_json() {
        let (base, server) = capture_one_request(CREATED_RUN).await;
        let client = DagronClient::new(base, None);
        let out =
            call_tool(&client, "dagron_submit_run", &json!({ "yaml": "name: w\ntasks: []\n" }))
                .await
                .unwrap();
        assert!(out.contains("run-0001"), "got {out}");

        let (head, body) = server.await.unwrap();
        assert!(head.starts_with("POST /api/runs "), "got {head}");
        assert!(
            head.to_lowercase().contains("content-type: application/json"),
            "submit must be application/json or dagron-api answers 415; head: {head}"
        );
        let sent: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(sent["yaml"], "name: w\ntasks: []\n");
    }

    #[tokio::test]
    async fn submit_sends_parameters_in_the_body_and_the_key_in_the_header() {
        // The idempotency gap was the sharp one: the client with the least
        // ability to reason about its own retries was the one denied the safety
        // net. It is a *header*, so a body field would have been silently ignored.
        let (base, server) = capture_one_request(CREATED_RUN).await;
        let client = DagronClient::new(base, None);
        call_tool(
            &client,
            "dagron_submit_run",
            &json!({
                "yaml": "name: w\n",
                "parameters": { "region": "ap-southeast-1" },
                "idempotency_key": "retry-42",
            }),
        )
        .await
        .unwrap();

        let (head, body) = server.await.unwrap();
        assert!(head.to_lowercase().contains("idempotency-key: retry-42"), "head: {head}");
        let sent: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(sent["parameters"]["region"], "ap-southeast-1");
    }

    #[tokio::test]
    async fn artifact_segments_are_percent_encoded_not_pattern_matched() {
        // An artifact name is user-chosen and may hold slashes; encoding is what
        // keeps it one path segment instead of a reshaped request.
        let (base, server) = capture_one_request(CREATED_RUN).await;
        let client = DagronClient::new(base, None);
        let _ = call_tool(
            &client,
            "dagron_artifact_exists",
            &json!({ "run_id": "r1", "task": "build step", "name": "../../etc/passwd" }),
        )
        .await;

        let (head, _) = server.await.unwrap();
        let line = head.lines().next().unwrap();
        assert_eq!(
            line,
            "GET /api/runs/r1/artifacts/build%20step/..%2F..%2Fetc%2Fpasswd/exists HTTP/1.1",
            "artifact segments must be encoded, got {line}"
        );
    }

    #[tokio::test]
    async fn a_paused_workflow_is_reported_as_a_state_not_a_failure() {
        let (base, server) = capture_one_request(CONFLICT).await;
        let client = DagronClient::new(base, None);
        let out = call_tool(&client, "dagron_run_workflow", &json!({ "workflow_id": "w1" }))
            .await
            .expect("409 must not be a tool error");
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["outcome"], "conflict");
        assert_eq!(v["detail"], "workflow is paused");

        let (head, body) = server.await.unwrap();
        assert!(head.starts_with("POST /api/workflows/w1/run "), "got {head}");
        // The route took no body at all before `parameters` existed, and that
        // call shape has to stay valid.
        assert!(body.is_empty(), "a parameterless run must send no body, got {body:?}");
    }

    #[tokio::test]
    async fn create_workflow_sends_the_upsert_envelope() {
        let (base, server) = capture_one_request(CREATED_RUN).await;
        let client = DagronClient::new(base, None);
        let _ = call_tool(
            &client,
            "dagron_create_workflow",
            &json!({ "spec": "name: nightly\n", "description": "the nightly ETL" }),
        )
        .await;

        let (head, body) = server.await.unwrap();
        assert!(head.starts_with("POST /api/workflows "), "got {head}");
        let sent: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(sent["spec"], "name: nightly\n");
        assert_eq!(sent["description"], "the nightly ETL");
        // Absent `name` means "derive it from the spec", so it must not be sent
        // as null — serde would take that as an explicit None and 422.
        assert!(sent.get("name").is_none(), "an omitted name must not be sent: {body}");
    }

    #[tokio::test]
    async fn wait_run_sends_its_bounded_timeout() {
        let (base, server) = capture_one_request(CREATED_RUN).await;
        let client = DagronClient::new(base, None);
        let _ = call_tool(&client, "dagron_wait_run", &json!({ "run_id": "r1" })).await;
        let (head, _) = server.await.unwrap();
        assert!(head.starts_with("GET /api/runs/r1/wait?timeout_secs=30 "), "got {head}");
    }

    #[tokio::test]
    async fn a_body_that_dies_mid_read_is_a_tool_error_not_an_empty_success() {
        // Defaulting a failed body read to "" would reach `render` as a 2xx with
        // an empty body and come back as {"status":200,"ok":true}: the agent is
        // told the call succeeded and simply returned nothing, which is the one
        // answer a dropped connection must never produce.
        let (base, server) = capture_one_request(TRUNCATED).await;
        let client = DagronClient::new(base, None);
        let err = call_tool(&client, "dagron_get_run", &json!({ "run_id": "r1" }))
            .await
            .expect_err("a truncated body must not read as success");
        assert!(
            err.to_string().contains("body read failed"),
            "want a transport failure, got {err:?}"
        );
        let _ = server.await;
    }

    #[tokio::test]
    async fn a_truncated_artifact_is_a_tool_error_not_a_zero_byte_file() {
        // Worse than the case above: describe_artifact would present the empty
        // result as a complete zero-byte artifact, content and all.
        let (base, server) = capture_one_request(TRUNCATED).await;
        let client = DagronClient::new(base, None);
        let err = call_tool(
            &client,
            "dagron_get_artifact",
            &json!({ "run_id": "r1", "task": "build", "name": "out.txt" }),
        )
        .await
        .expect_err("a truncated artifact must not read as an empty one");
        assert!(
            err.to_string().contains("body read failed"),
            "want a transport failure, got {err:?}"
        );
        let _ = server.await;
    }

    #[tokio::test]
    async fn list_runs_passes_its_filters_through() {
        let (base, server) = capture_one_request(CREATED_RUN).await;
        let client = DagronClient::new(base, None);
        let _ = call_tool(
            &client,
            "dagron_list_runs",
            &json!({ "status": "failed", "name": "nightly etl", "limit": 25 }),
        )
        .await;
        let (head, _) = server.await.unwrap();
        assert!(
            head.starts_with("GET /api/runs?status=failed&name=nightly%20etl&limit=25 "),
            "got {head}"
        );
    }
}
