//! Run list + detail read endpoints (read pool, all parameterized, auth-gated).

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::auth::AuthUser;
use crate::state::AppState;

/// Default page size for `GET /api/runs`, capped at MAX_LIMIT.
const DEFAULT_LIMIT: i64 = 100;
const MAX_LIMIT: i64 = 500;

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct RunSummary {
    pub id: String,
    pub definition_id: String,
    pub status: String,
    pub created_at: String,
    pub finished_at: Option<String>,
    /// Workflow/DAG name from the run's definition (LEFT JOIN; None if the
    /// definition row is missing). Surfaced as the "Workflow" column in the UI.
    pub name: Option<String>,
    /// What started this run ([`trigger_kind_sql`]): a `schedule_backfills`
    /// row claims it as a backfill, a stamped `schedule_id` marks a cron fire,
    /// a trigger record says what the service that submitted it said (`ci`),
    /// and anything else was human-initiated.
    pub trigger_kind: String,
    /// What a human decided about this run: acknowledged / resolved / ignored,
    /// or absent for one nobody has looked at. `status` is what the engine did;
    /// this is what was done about it, and it is the difference between an
    /// attention queue that drains and one that only ages out.
    pub triage_state: Option<String>,
    pub triage_note: Option<String>,
    pub triaged_at: Option<String>,
    pub triaged_by: Option<String>,
    /// Whether the engine's wall clock was trustworthy when this run was
    /// recorded: `synced` / `drifted` / `unknown`, or `null` for a run the
    /// engine never assessed (API-submitted runs are stamped by the engine
    /// host, not this gateway). A disconnected unit that boots without a
    /// clock still keeps its schedule; this says whether its timestamps are
    /// evidence.
    pub clock_confidence: Option<String>,
    /// Set on a `running` run that was cancelled, or passed its deadline,
    /// while a worker held one of its tasks: the status it takes once that
    /// work has stopped (`cancelled` or `failed`). `null` on every other run.
    #[sqlx(default)]
    pub stopping_as: Option<String>,
    /// A run that arrived while its workflow's `max_active_runs` was full,
    /// and was made to wait (`when_full`): when it began to, and when it was
    /// let in. A `running` run with the first and not the second waits, and
    /// has started nothing. Both `null` on a run that never waited.
    #[sqlx(default)]
    pub queued_at: Option<String>,
    #[sqlx(default)]
    pub admitted_at: Option<String>,
}

/// A run's trigger kind, over `workflow_runs wr` joined to `schedule_backfills
/// sb` and `run_triggers rt`. Written once, as the value and as the filter must
/// agree: a run listed as `ci` is the run `?trigger=ci` returns.
///
/// The run's trigger record comes first: it was written with the run, by
/// whatever started it, and every run has one. A run with none predates it,
/// and is read as it was then: a backfill by its ledger row, a schedule's
/// fire by its stamp, and otherwise manual.
macro_rules! trigger_kind_sql {
    () => {
        "CASE WHEN rt.kind IS NOT NULL THEN rt.kind
              WHEN sb.run_id IS NOT NULL THEN 'backfill'
              WHEN wr.schedule_id IS NOT NULL THEN 'schedule'
              ELSE 'manual' END"
    };
}

/// The joins [`trigger_kind_sql`] reads.
macro_rules! trigger_joins_sql {
    () => {
        " LEFT JOIN schedule_backfills sb ON sb.run_id = wr.id
 LEFT JOIN run_triggers rt ON rt.run_id = wr.id"
    };
}

/// SELECT list + joins shared by the run list and per-workflow runs: summary
/// columns, the definition name, and the trigger kind.
pub(crate) const SUMMARY_SELECT: &str = concat!(
    "SELECT wr.id, wr.definition_id, wr.status, wr.created_at, wr.finished_at, d.name,
        wr.triage_state, wr.triage_note, wr.triaged_at, wr.triaged_by,
        wr.clock_confidence, wr.stopping_as, wr.queued_at, wr.admitted_at,
        ",
    trigger_kind_sql!(),
    " AS trigger_kind
 FROM workflow_runs wr
 LEFT JOIN workflow_definitions d ON d.id = wr.definition_id
",
    trigger_joins_sql!()
);

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct TaskRow {
    pub id: String,
    pub name: String,
    pub status: String,
    pub attempt: i64,
    pub output: Option<String>,
    pub scheduled_at: Option<String>,
    pub finished_at: Option<String>,
    /// Why a parked task is parked. Each park form keeps the row `running` with
    /// a NULL lease, so without these the console cannot tell "waiting on
    /// something" from "hung": the time sensor's deadline, the HTTP sensor's
    /// endpoint, the dataset sensor's URI, and the sub-workflow trigger's child
    /// run id (which also gives the console a parent → child link).
    pub wake_at: Option<String>,
    pub wait_url: Option<String>,
    pub wait_dataset: Option<String>,
    pub sub_run_id: Option<String>,
    /// Scheduling metadata worth seeing next to a task: the named pool it drew a
    /// slot from (#21), its dispatch priority (#25), and whether it was resolved
    /// from the memoization store instead of executed (#22) — the last of which
    /// explains an otherwise inexplicable 0s success.
    pub pool: Option<String>,
    pub priority: i64,
    pub cache_hit: bool,
    /// Who resolved an approval gate and why; `decided_by` is `timeout` when the
    /// sweep did. `None` on every non-approval row and on an unresolved gate.
    pub decided_by: Option<String>,
    pub decision_comment: Option<String>,
    /// Why a `ready` task is not being dispatched: its pod was refused by a
    /// full Kubernetes namespace ResourceQuota, so it went back to the queue
    /// with no attempt consumed and `scheduled_at` set to its next probe. When
    /// the wait began, the last refusal sentence, and the refusals so far. Set
    /// only while the task is waiting — `None` / `0` on every other row — so
    /// the console can tell "queued behind a quota" from "nobody is claiming
    /// this".
    pub quota_wait_since: Option<String>,
    pub quota_wait_detail: Option<String>,
    pub quota_waits: i64,
    /// When a `running` task was asked to stop: its run was cancelled or
    /// passed its deadline, or a gang sibling failed, while a worker held it.
    /// Its work is still to be seen gone, and it is `cancelled` once it has
    /// been. `null` on every other row, a cancelled one included.
    pub stop_requested_at: Option<String>,
    /// What broke the attempt that failed, as the engine read it from what
    /// the task printed: a [`dagron_core::fault::FaultClass`] in kebab case
    /// (`user-code`, `storage`, `preemption`, …). `null` where nothing has
    /// looked; `unknown` where something looked and could not tell.
    pub fault_class: Option<String>,
}

/// Why a run failed, in the run detail itself.
///
/// Without this, an agent whose run failed holds a status and a list of task
/// rows and must work out which one broke and then fetch its logs — two more
/// calls and a correlation step, from a model that has nine tools to choose
/// between. The reason belongs in the response it already has.
///
/// Present whenever this run has a failed task, or the run itself failed. That
/// is deliberately not the same as `status == "failed"`: a run whose fail-fast
/// is off can be `running` with a task already dead, and the earlier an agent
/// learns that, the less it wastes.
#[derive(Debug, Serialize, PartialEq)]
pub struct RunFailure {
    /// The failed task this summarises, when there is one. Absent when the run
    /// failed with no task failing — the `run_timeout_secs` sweep fails the run
    /// and *cancels* its tasks, so nothing is left in a `failed` state and the
    /// reason lives on the run row instead.
    pub task_id: Option<String>,
    pub task_name: Option<String>,
    pub attempt: Option<i64>,
    /// How many tasks in this run are `failed`. One failure summarised out of
    /// several is honest only if the caller can see there were several.
    pub failed_tasks: usize,
    /// The tail of the recorded failure text: the task's output, or the run's
    /// own when no task failed. dagron stores one output stream per task, not a
    /// split stdout/stderr, so this is that stream — and for a failed task it
    /// is the error the executor recorded.
    pub message: Option<String>,
    /// Whether `message` is a tail of something longer. A model that cannot
    /// tell a complete error from a clipped one will confidently explain the
    /// wrong half, so say which this is rather than leaving it to be guessed.
    pub truncated: bool,
    /// What broke the failed task's last attempt, as the engine classified it
    /// (the task row's `fault_class`); `null` where nothing classified it.
    pub fault_class: Option<String>,
    /// What another attempt is worth, given that class: `infrastructure` (the
    /// same code elsewhere likely succeeds), `application` (another attempt
    /// reproduces it), `platform` (it was taken away; retrying is a policy
    /// question) or `unknown`. A caller deciding between running the task
    /// again and reading its code reads this, and not the class's name.
    pub disposition: Option<String>,
}

/// The task rows behind a run, used by both `GET /api/runs/{id}` and the
/// failure summary on `GET /api/runs/{id}/wait`. Shared so the two cannot drift:
/// a column added for one and missed by the other would silently change what a
/// failure summary can see depending on which route asked for it.
///
/// The quota-wait columns are never cleared once written, so they are guarded
/// rather than selected bare: a row is waiting only while it is `ready` and no
/// claim has moved `version` past the one it was handed back at.
const RUN_TASK_ROWS_SQL: &str = "SELECT id, name, status, attempt, output, scheduled_at, finished_at,
        wake_at, wait_url, wait_dataset, sub_run_id,
        pool, priority, cache_hit, decided_by, decision_comment, stop_requested_at, fault_class,
        CASE WHEN status = 'ready' AND quota_wait_version = version
             THEN quota_wait_since END AS quota_wait_since,
        CASE WHEN status = 'ready' AND quota_wait_version = version
             THEN quota_wait_detail END AS quota_wait_detail,
        CASE WHEN status = 'ready' AND quota_wait_version = version
             THEN quota_waits ELSE 0 END AS quota_waits
 FROM task_runs WHERE run_id = $1 ORDER BY name";

/// How much failure text to inline. Enough for a stack trace's business end,
/// bounded so a task that logged a megabyte before dying cannot bloat every
/// poll of the run detail — `GET /api/runs/{id}/logs` remains the full view.
const FAILURE_TAIL_LINES: usize = 20;
const FAILURE_TAIL_BYTES: usize = 4096;

/// Last `max_lines` lines of `text`, further clipped to `max_bytes` from the
/// end. Returns the tail and whether anything was dropped.
fn failure_tail(text: &str, max_lines: usize, max_bytes: usize) -> (String, bool) {
    let trimmed = text.trim_end();
    let lines: Vec<&str> = trimmed.lines().collect();
    let start = lines.len().saturating_sub(max_lines);
    let mut truncated = start > 0;
    let mut out = lines[start..].join("\n");

    if out.len() > max_bytes {
        // Keep the end, not the start: the last thing a dying task said is the
        // part that explains it. Step forward to a char boundary so a clipped
        // multi-byte character cannot panic the slice.
        let mut cut = out.len() - max_bytes;
        while cut < out.len() && !out.is_char_boundary(cut) {
            cut += 1;
        }
        out = out[cut..].to_string();
        truncated = true;
    }
    (out, truncated)
}

/// Build the failure summary from rows already in hand — no extra query, and
/// pure, so the choice of *which* failure to report is testable without a
/// database.
fn summarize_failure(status: &str, run_output: Option<&str>, tasks: &[TaskRow]) -> Option<RunFailure> {
    // Earliest-finished failed task. With fail-fast off several tasks fail
    // independently, and the one that finished first is the likeliest cause
    // rather than a downstream casualty. `finished_at` is RFC3339 UTC
    // everywhere it is written, so the lexicographic minimum is the
    // chronological one; a tie falls back to name, which is the row order.
    let mut culprit: Option<&TaskRow> = None;
    let mut failed_tasks = 0usize;
    for t in tasks.iter().filter(|t| t.status == "failed") {
        failed_tasks += 1;
        let better = match culprit {
            None => true,
            Some(c) => {
                let k = |t: &TaskRow| {
                    (
                        t.finished_at.is_none(),
                        t.finished_at.clone().unwrap_or_default(),
                        t.name.clone(),
                    )
                };
                k(t) < k(c)
            }
        };
        if better {
            culprit = Some(t);
        }
    }

    // No failed task. Only the run's own failure is left to report, and only
    // if it actually failed — a cancelled or succeeded run has no failure, and
    // saying otherwise would make the field mean "look here" on every poll.
    let (task, text) = match culprit {
        Some(t) => (Some(t), t.output.as_deref()),
        None if status == "failed" => (None, run_output),
        None => return None,
    };

    let (message, truncated) = text
        .map(|t| failure_tail(t, FAILURE_TAIL_LINES, FAILURE_TAIL_BYTES))
        .unwrap_or_default();

    let fault_class = task.and_then(|t| t.fault_class.clone());
    let disposition = fault_class.as_deref().map(|c| {
        dagron_core::fault::FaultClass::parse(c)
            .disposition()
            .as_str()
            .to_string()
    });
    Some(RunFailure {
        task_id: task.map(|t| t.id.clone()),
        task_name: task.map(|t| t.name.clone()),
        attempt: task.map(|t| t.attempt),
        failed_tasks,
        message: if message.is_empty() { None } else { Some(message) },
        truncated,
        fault_class,
        disposition,
    })
}

#[derive(Debug, Serialize)]
pub struct RunDetail {
    pub id: String,
    pub definition_id: String,
    pub status: String,
    pub input: Option<String>,
    pub output: Option<String>,
    pub created_at: String,
    pub finished_at: Option<String>,
    /// Workflow/DAG name from the run's definition (for the header + backlink).
    pub name: Option<String>,
    /// Trigger kind: manual | schedule | backfill | ci.
    pub trigger_kind: String,
    /// What the run was started for, when the service that submitted it said
    /// (`run_triggers`): `null` for a run with no record.
    pub trigger: Option<RunTriggerView>,
    /// What a human decided about this run. `status` is what the engine did;
    /// this is what was done about it.
    pub triage_state: Option<String>,
    pub triage_note: Option<String>,
    pub triaged_at: Option<String>,
    pub triaged_by: Option<String>,
    /// Whether the engine's wall clock was trustworthy when this run was
    /// recorded: `synced` / `drifted` / `unknown`, or `null` for a run the
    /// engine never assessed (API-submitted runs are stamped by the engine
    /// host, not this gateway). A disconnected unit that boots without a
    /// clock still keeps its schedule; this says whether its timestamps are
    /// evidence.
    pub clock_confidence: Option<String>,
    /// Measured wall-vs-monotonic offset (ms) behind a `drifted` verdict.
    pub clock_offset_ms: Option<i64>,
    /// What produced the verdict: `sync-file`, `step`, `behind-datastore`.
    pub clock_source: Option<String>,
    /// A run that was cancelled, or passed its deadline, while a worker held
    /// one of its tasks: the status it takes once that work has stopped
    /// (`cancelled` or `failed`), and when it was asked. Both set only while
    /// the run is `running`, which it is until then; `null` otherwise.
    pub stopping_as: Option<String>,
    pub stop_requested_at: Option<String>,
    /// A run that arrived while its workflow's `max_active_runs` was full,
    /// and was made to wait (`when_full`): when it began to, and when it was
    /// let in. A `running` run with the first and not the second waits: it
    /// has started nothing, and its `run_timeout_secs` is counted from when
    /// it is let in. Both `null` on a run that never waited.
    pub queued_at: Option<String>,
    pub admitted_at: Option<String>,
    /// Why this run failed, when it did — `null` otherwise, so a
    /// caller can branch on presence without inspecting `status`.
    pub failure: Option<RunFailure>,
    /// What the run, with the runs it started, spent of a model's tokens and
    /// has reserved for its tasks still running, and its `budget: { tokens }`.
    /// Absent where nothing of it was metered.
    #[cfg(feature = "enterprise")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tokens: Option<dagron_core::tokens::RunTokens>,
    pub tasks: Vec<TaskRow>,
}

/// A run's trigger record, as a reader gets it: what started the run, as
/// whatever started it wrote it down when the run was made. Where a caller of
/// this gateway gave it (a CI service), every field but the first two is that
/// caller's word, taken because the caller may set a trigger at all.
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct RunTriggerView {
    pub kind: String,
    /// What submitted it: this gateway's caller as it authenticated them
    /// (`user:…`, `ci:…`), `engine` for a run the engine started itself, or
    /// `ops-api` for one asked of the engine's own API.
    pub principal: String,
    /// Who caused it at the source: the person who asked, the pusher, a pull
    /// request's author. A parent run's, for a run its task started.
    pub actor: Option<String>,
    pub trust: Option<String>,
    /// What it came from, where that is no repository: a schedule, a cron
    /// entry, a dataset, a parent run's task, a source, a dead letter.
    pub source: Option<String>,
    pub forge_host: Option<String>,
    /// `owner/name` on that forge.
    pub repo: Option<String>,
    #[sqlx(rename = "ref")]
    #[serde(rename = "ref")]
    pub git_ref: Option<String>,
    pub sha: Option<String>,
    pub base_sha: Option<String>,
    pub pr: Option<i64>,
    /// The id of the event at its source: a fire's time, a backfill's date,
    /// a source's position, a CI service's event.
    pub event_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ListParams {
    pub status: Option<String>,
    /// Filter to one workflow's runs (definition name, exact match).
    pub name: Option<String>,
    /// Filter by trigger kind: manual | schedule | backfill | ci.
    pub trigger: Option<String>,
    /// Filter to the runs whose trigger record names this repository
    /// (`host/owner/name`, as a listed CI pipeline's `description` has it).
    /// Two repositories may each have a pipeline of one name: `name` alone
    /// returns both's runs.
    pub repo: Option<String>,
    /// Filter to the runs of one event at its source, by the id their
    /// trigger records carry: a CI service's event, which may have made a
    /// run for each of several pipelines.
    pub event: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

/// The repository a run's trigger record names, as one text: `host/owner/name`,
/// or `owner/name` where the record has no host. Reads `run_triggers rt`. A
/// listed CI pipeline's `description` is the same text, so a row's link can
/// narrow the run list to exactly its runs.
pub(crate) const TRIGGER_REPO_SQL: &str =
    "concat_ws('/', NULLIF(rt.forge_host, ''), NULLIF(rt.repo, ''))";

/// `GET /api/runs?status=&name=&trigger=&repo=&event=&limit=&offset=` —
/// most-recent-first run list with optional status / workflow-name / trigger /
/// repository / event filters.
pub async fn list_runs(
    _auth: AuthUser,
    State(state): State<AppState>,
    Query(params): Query<ListParams>,
) -> Result<Json<Vec<RunSummary>>, StatusCode> {
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let offset = params.offset.unwrap_or(0).max(0);
    // An empty `repo=` names no repository, and filters nothing. So with an
    // empty `event=`.
    let repo = params.repo.as_deref().filter(|r| !r.is_empty());
    let event = params.event.as_deref().filter(|e| !e.is_empty());

    // Bind each filter as optional: when None, the `$n IS NULL` branch keeps
    // every row, so one parameterized query serves all filter combinations.
    let rows = sqlx::query_as::<_, RunSummary>(&format!(
        "{SUMMARY_SELECT}
         WHERE (CAST($1 AS TEXT) IS NULL OR wr.status = $1)
           AND (CAST($2 AS TEXT) IS NULL OR d.name = $2)
           AND (CAST($3 AS TEXT) IS NULL OR {} = $3)
           AND (CAST($6 AS TEXT) IS NULL OR {TRIGGER_REPO_SQL} = $6)
           AND (CAST($7 AS TEXT) IS NULL OR rt.event_id = $7)
         ORDER BY wr.created_at DESC
         LIMIT $4 OFFSET $5",
        trigger_kind_sql!()
    ))
    .bind(&params.status)
    .bind(&params.name)
    .bind(&params.trigger)
    .bind(limit)
    .bind(offset)
    .bind(repo)
    .bind(event)
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;

    Ok(Json(rows))
}

/// A repository that runs came from, as their trigger records name it.
#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct RunSource {
    /// `host/owner/name`: what `?repo=` narrows a list to.
    pub repo: String,
    /// How many runs name it.
    pub runs: i64,
    /// When the latest of them was made.
    pub last_run_at: String,
}

#[derive(Debug, Deserialize)]
pub struct SourcesParams {
    pub limit: Option<i64>,
}

/// `GET /api/runs/sources?limit=` — the repositories that runs' trigger
/// records name, the one with the latest run first. A run that names none (a
/// schedule's, a person's) is of no repository, and is counted under none.
pub async fn list_sources(
    _auth: AuthUser,
    State(state): State<AppState>,
    Query(params): Query<SourcesParams>,
) -> Result<Json<Vec<RunSource>>, StatusCode> {
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let rows = sqlx::query_as::<_, RunSource>(&format!(
        "SELECT {TRIGGER_REPO_SQL} AS repo, count(*) AS runs, max(rt.created_at) AS last_run_at
           FROM run_triggers rt
          WHERE rt.repo IS NOT NULL AND rt.repo <> ''
          GROUP BY 1
          ORDER BY last_run_at DESC, repo
          LIMIT $1"
    ))
    .bind(limit)
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;
    Ok(Json(rows))
}

/// One of a repository's refs, with the runs for its latest commit: the
/// commit of the latest run on it, and the latest run of each workflow for
/// that commit. So a branch reads as its head does, whichever event each
/// run came of: a push's runs, and a later one asked for by hand for one
/// workflow, are one row.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct RefRuns {
    #[serde(rename = "ref")]
    pub git_ref: String,
    /// The pull request's number, where the ref is one's.
    pub pr: Option<i64>,
    pub sha: Option<String>,
    /// Who caused the latest run's event at its source.
    pub actor: Option<String>,
    /// The latest run's event, by its id at its source: what
    /// `GET /api/runs?event=` narrows to.
    pub event_id: Option<String>,
    /// When the latest run was made.
    pub at: String,
    /// The commit's runs, the latest of each workflow, by name.
    pub runs: Vec<RefRun>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct RefRun {
    pub id: String,
    /// The workflow's name: a CI pipeline's.
    pub name: Option<String>,
    pub status: String,
    pub created_at: String,
    pub finished_at: Option<String>,
    pub stopping_as: Option<String>,
    /// When a run that waits for its workflow's `max_active_runs` began to,
    /// and when it was let in ([`RunDetail::queued_at`]).
    pub queued_at: Option<String>,
    pub admitted_at: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RefsParams {
    /// The repository, as [`RunSource::repo`] has it: `host/owner/name`.
    pub repo: Option<String>,
    pub limit: Option<i64>,
}

#[derive(sqlx::FromRow)]
struct RefRunRow {
    #[sqlx(rename = "ref")]
    git_ref: String,
    pr: Option<i64>,
    sha: Option<String>,
    actor: Option<String>,
    event_id: Option<String>,
    at: String,
    id: String,
    name: Option<String>,
    status: String,
    created_at: String,
    finished_at: Option<String>,
    stopping_as: Option<String>,
    queued_at: Option<String>,
    admitted_at: Option<String>,
}

/// `GET /api/runs/refs?repo=&limit=` — a repository's refs, each with the
/// runs for its latest commit, the ref with the latest run first. 400
/// without a `repo`.
///
/// A repository is matched by its record's columns, so that
/// `idx_run_triggers_repo_ref` serves it: `host/owner/name` as a host and
/// the rest, or the whole as the name of a record that has no host. Each
/// arm is one the index answers, so that no arm reads the table.
pub async fn list_refs(
    _auth: AuthUser,
    State(state): State<AppState>,
    Query(params): Query<RefsParams>,
) -> Result<Json<Vec<RefRuns>>, StatusCode> {
    let repo = params
        .repo
        .as_deref()
        .filter(|r| !r.is_empty())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let (host, name) = repo.split_once('/').unwrap_or(("", repo));
    let rows = sqlx::query_as::<_, RefRunRow>(
        "WITH mine AS (
             SELECT rt.run_id, rt.ref, rt.pr, rt.sha, rt.actor, rt.event_id, rt.created_at
               FROM run_triggers rt
              WHERE rt.ref IS NOT NULL AND rt.ref <> ''
                AND ((rt.forge_host = $1 AND rt.repo = $2)
                     OR (rt.forge_host IS NULL AND rt.repo = $3)
                     OR (rt.forge_host = '' AND rt.repo = $3))
         ), latest AS (
             -- The newest of each ref: the first row of each partition, which
             -- reads the same on Postgres and SQLite (no DISTINCT ON).
             SELECT ref, pr, sha, actor, event_id, created_at FROM (
                 SELECT ref, pr, sha, actor, event_id, created_at,
                        ROW_NUMBER() OVER (PARTITION BY ref ORDER BY created_at DESC, run_id DESC) AS rn
                   FROM mine
             ) newest WHERE rn = 1
         ), shown AS (
             SELECT * FROM latest ORDER BY created_at DESC, ref LIMIT $4
         ), runs AS (
             SELECT ref, pr, sha, actor, event_id, at, id, name, status, created_at, finished_at,
                    stopping_as, queued_at, admitted_at FROM (
                 SELECT s.ref, s.pr, s.sha, s.actor, s.event_id, s.created_at AS at,
                        wr.id, d.name, wr.status, wr.created_at, wr.finished_at, wr.stopping_as,
                        wr.queued_at, wr.admitted_at,
                        ROW_NUMBER() OVER (PARTITION BY s.ref, d.name
                                           ORDER BY wr.created_at DESC, wr.id DESC) AS rn
                   FROM shown s
                   JOIN mine m ON m.ref = s.ref AND m.sha IS NOT DISTINCT FROM s.sha
                   JOIN workflow_runs wr ON wr.id = m.run_id
                   LEFT JOIN workflow_definitions d ON d.id = wr.definition_id
             ) newest WHERE rn = 1
         )
         SELECT ref, pr, sha, actor, event_id, at, id, name, status, created_at, finished_at, stopping_as,
                queued_at, admitted_at
           FROM runs
          ORDER BY at DESC, ref, name, id",
    )
    .bind(host)
    .bind(name)
    .bind(repo)
    .bind(limit)
    .fetch_all(&state.read_pool)
    .await
    .map_err(internal)?;

    let mut refs: Vec<RefRuns> = Vec::new();
    for row in rows {
        let run = RefRun {
            id: row.id,
            name: row.name,
            status: row.status,
            created_at: row.created_at,
            finished_at: row.finished_at,
            stopping_as: row.stopping_as,
            queued_at: row.queued_at,
            admitted_at: row.admitted_at,
        };
        match refs.last_mut() {
            Some(last) if last.git_ref == row.git_ref => last.runs.push(run),
            _ => refs.push(RefRuns {
                git_ref: row.git_ref,
                pr: row.pr,
                sha: row.sha,
                actor: row.actor,
                event_id: row.event_id,
                at: row.at,
                runs: vec![run],
            }),
        }
    }
    Ok(Json(refs))
}

/// `GET /api/runs/:id` — one run plus its task rows. 404 if the run is unknown.
pub async fn get_run(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<RunDetail>, StatusCode> {
    let run = sqlx::query_as::<_, RunSummaryFull>(concat!(
        "SELECT wr.id, wr.definition_id, wr.status, wr.input, wr.output,
                wr.created_at, wr.finished_at, d.name,
                wr.triage_state, wr.triage_note, wr.triaged_at, wr.triaged_by,
                wr.clock_confidence, wr.clock_offset_ms, wr.clock_source,
                wr.stopping_as, wr.stop_requested_at, wr.queued_at, wr.admitted_at,
                ",
        trigger_kind_sql!(),
        " AS trigger_kind
         FROM workflow_runs wr
         LEFT JOIN workflow_definitions d ON d.id = wr.definition_id
",
        trigger_joins_sql!(),
        "
         WHERE wr.id = $1"
    ))
    .bind(&id)
    .fetch_optional(&state.read_pool)
    .await
    .map_err(internal)?
    .ok_or(StatusCode::NOT_FOUND)?;

    let trigger = sqlx::query_as::<_, RunTriggerView>(
        "SELECT kind, principal, actor, trust, source, forge_host, repo, ref, sha, base_sha, pr, event_id
           FROM run_triggers WHERE run_id = $1",
    )
    .bind(&id)
    .fetch_optional(&state.read_pool)
    .await
    .map_err(internal)?;

    let tasks = sqlx::query_as::<_, TaskRow>(RUN_TASK_ROWS_SQL)
        .bind(&id)
        .fetch_all(&state.read_pool)
        .await
        .map_err(internal)?;

    // Built from `tasks` and the run row, both already fetched — the summary
    // costs no additional round trip to the database.
    let failure = summarize_failure(&run.status, run.output.as_deref(), &tasks);
    #[cfg(feature = "enterprise")]
    let tokens = dagron_core::tokens::of_run(&state.read_pool, &id)
        .await
        .map_err(|e| {
            tracing::error!(error = ?e, "reading a run's tokens failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(RunDetail {
        id: run.id,
        definition_id: run.definition_id,
        status: run.status,
        input: run.input,
        output: run.output,
        created_at: run.created_at,
        finished_at: run.finished_at,
        name: run.name,
        trigger_kind: run.trigger_kind,
        trigger,
        triage_state: run.triage_state,
        triage_note: run.triage_note,
        triaged_at: run.triaged_at,
        triaged_by: run.triaged_by,
        clock_confidence: run.clock_confidence,
        clock_offset_ms: run.clock_offset_ms,
        clock_source: run.clock_source,
        stopping_as: run.stopping_as,
        stop_requested_at: run.stop_requested_at,
        queued_at: run.queued_at,
        admitted_at: run.admitted_at,
        failure,
        #[cfg(feature = "enterprise")]
        tokens,
        tasks,
    }))
}

#[derive(Debug, Serialize)]
pub struct RunSpec {
    /// The stored, un-expanded DAG YAML this run was created from.
    pub yaml: String,
    /// The workflow/DAG name from the definition row (if the column is set).
    pub name: Option<String>,
}

/// `GET /api/runs/:id/spec` — the DAG spec (YAML) this run was created from, so
/// the UI can pre-fill a "re-run with changes" editor. Read-only; mirrors the
/// SELECT used by `resubmit_run`. 404 if the run (or its definition) is unknown.
pub async fn get_run_spec(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<RunSpec>, StatusCode> {
    let row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT d.spec, d.name FROM workflow_runs r
         JOIN workflow_definitions d ON d.id = r.definition_id
         WHERE r.id = $1",
    )
    .bind(&id)
    .fetch_optional(&state.read_pool)
    .await
    .map_err(internal)?;
    let (yaml, name) = row.ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(RunSpec { yaml, name }))
}

/// Run ids accepted by one `GET /api/runs/specs` call.
///
/// Bounds the `= ANY($1)` and the response together. The console asks for a
/// page of runs at a time (25), so this is slack rather than a constraint — it
/// exists so one request cannot ask the server to materialise every spec in the
/// installation.
const MAX_RUN_SPEC_IDS: usize = 200;

/// One distinct spec, and the runs that were created from it.
#[derive(Debug, Serialize, PartialEq)]
pub struct RunSpecGroup {
    pub yaml: String,
    pub name: Option<String>,
    /// The requested runs that ran this exact spec, in the order asked for.
    pub run_ids: Vec<String>,
}

/// `GET /api/runs/specs?ids=a,b,c` — the specs behind many runs in one call,
/// **grouped by content**.
///
/// Every run snapshots its own `workflow_definitions` row at submit, so N runs
/// of a workflow nobody edited are N identical specs under N different ids.
/// Returning them per-run would send the same document twenty-five times to
/// answer "did this change?" — so identical specs collapse into one entry
/// listing the runs that used it, which is also the shape the caller wants:
/// one group means nothing changed.
///
/// Unknown run ids are simply absent from the response rather than a 404. A
/// caller asking about a page of runs should get the ones that resolved; which
/// ids came back is already the answer to which didn't, and failing the whole
/// batch over one archived run would make the endpoint useless for the case it
/// exists for. 400 only for a malformed request: no ids, or more than
/// [`MAX_RUN_SPEC_IDS`].
pub async fn get_run_specs(
    _auth: AuthUser,
    State(state): State<AppState>,
    Query(params): Query<RunSpecsParams>,
) -> Result<Json<Vec<RunSpecGroup>>, (StatusCode, String)> {
    // Deduped, because a caller repeating an id should not make the IN list
    // longer or the group's `run_ids` contain it twice.
    let mut ids: Vec<String> = Vec::new();
    for raw in params.ids.as_deref().unwrap_or_default().split(',') {
        let id = raw.trim();
        if !id.is_empty() && !ids.iter().any(|seen| seen == id) {
            ids.push(id.to_string());
        }
    }
    if ids.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "ids= is required (comma-separated run ids)".into()));
    }
    if ids.len() > MAX_RUN_SPEC_IDS {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("too many ids: {} (max {MAX_RUN_SPEC_IDS})", ids.len()),
        ));
    }

    let sql = format!(
        "SELECT r.id, d.spec, d.name FROM workflow_runs r
         JOIN workflow_definitions d ON d.id = r.definition_id
         WHERE r.id {}",
        crate::db::in_list("$1")
    );
    let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(&sql)
    .bind(crate::db::list(&ids))
    .fetch_all(&state.read_pool)
    .await
    .map_err(|e| (internal(e), "db query failed".to_string()))?;

    Ok(Json(group_run_specs(&ids, rows)))
}

#[derive(Debug, Deserialize)]
pub struct RunSpecsParams {
    /// Comma-separated run ids.
    pub ids: Option<String>,
}

/// Collapse `(run id, spec, name)` rows into one entry per distinct spec.
///
/// Ordering is by `requested` rather than by whatever the database returned:
/// the caller lines these up against its own list, and `= ANY` gives no order
/// at all. Split out from the handler so it can be tested without a database —
/// the grouping is the part with the behaviour in it.
fn group_run_specs(
    requested: &[String],
    rows: Vec<(String, String, Option<String>)>,
) -> Vec<RunSpecGroup> {
    let mut groups: Vec<RunSpecGroup> = Vec::new();
    for id in requested {
        let Some((_, spec, name)) = rows.iter().find(|(rid, _, _)| rid == id) else {
            continue; // unknown / archived run — absent, not an error
        };
        // Keyed on the pair: `name` is derived from the spec today, so a
        // mismatch is not expected — but grouping two definitions together on a
        // spec match while reporting only one of their names would be a quiet
        // lie if that ever stops holding.
        match groups.iter_mut().find(|g| g.yaml == *spec && g.name == *name) {
            Some(g) => g.run_ids.push(id.clone()),
            None => groups.push(RunSpecGroup {
                yaml: spec.clone(),
                name: name.clone(),
                run_ids: vec![id.clone()],
            }),
        }
    }
    groups
}

#[derive(sqlx::FromRow)]
struct RunSummaryFull {
    id: String,
    definition_id: String,
    status: String,
    input: Option<String>,
    output: Option<String>,
    created_at: String,
    finished_at: Option<String>,
    name: Option<String>,
    trigger_kind: String,
    triage_state: Option<String>,
    triage_note: Option<String>,
    triaged_at: Option<String>,
    triaged_by: Option<String>,
    clock_confidence: Option<String>,
    clock_offset_ms: Option<i64>,
    clock_source: Option<String>,
    #[sqlx(default)]
    stopping_as: Option<String>,
    #[sqlx(default)]
    stop_requested_at: Option<String>,
    #[sqlx(default)]
    queued_at: Option<String>,
    #[sqlx(default)]
    admitted_at: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WaitParams {
    pub timeout_secs: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct RunResult {
    pub run_id: String,
    pub status: String,
    pub finished: bool,
    /// The `result_from` task's output on success, else null (fast-win #15).
    pub result: Option<String>,
    /// Why the run failed, in the same shape `GET /api/runs/{id}` returns.
    /// `null` unless this wait ended on a failure.
    ///
    /// This route is the one a synchronous caller actually uses — an agent
    /// blocking on a run holds *this* response, not the run detail. Putting the
    /// reason only on the detail route left the summary somewhere the caller
    /// that most needed it never looked: it got `status: "failed"` and had to
    /// start a second investigation with the tools it had.
    pub failure: Option<RunFailure>,
    /// How many times the run was rerun whole (`from: all`): a reader that
    /// counts runs tells a rerun of every job from a rerun of the failed ones
    /// by it, as each ends on the same run.
    pub whole_reruns: i64,
}

/// `GET /api/runs/:id/wait?timeout_secs=` — synchronous invocation (fast-win
/// #15): long-poll a run until it reaches a terminal state (or the timeout
/// elapses) and return its status + result (the `result_from` task's output on
/// success). 404 if the run is unknown; a timed-out wait returns `finished:false`
/// with the live status so the caller re-polls. Backend-agnostic short poll — the
/// engine (which may be a separate process) drives the run to completion.
pub async fn wait_run(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(params): Query<WaitParams>,
) -> Result<Json<RunResult>, StatusCode> {
    let timeout =
        std::time::Duration::from_secs(params.timeout_secs.unwrap_or(30).clamp(1, 600));
    let deadline = tokio::time::Instant::now() + timeout;
    // Event-driven wait (LOW_LATENCY R-4): this process already holds a live
    // LISTEN-fed broadcast of task events keyed by run_id, so a waiter blocks
    // on that instead of the old 200 ms busy-poll (5 queries/second per waiter,
    // and up to 200 ms of avoidable tail latency on every sync invocation).
    // Subscribe BEFORE the first status read so a transition landing between
    // the read and the wait is never missed. A slow re-check timer stays as
    // the safety net for the listener's reconnect windows — the same
    // poll-as-backstop contract the engine's reconcile loop uses.
    let mut events = state.tx.subscribe();
    loop {
        let row: Option<(String, Option<String>, i64)> =
            sqlx::query_as("SELECT status, output, whole_reruns FROM workflow_runs WHERE id = $1")
                .bind(&id)
                .fetch_optional(&state.read_pool)
                .await
                .map_err(internal)?;
        let Some((status, output, whole_reruns)) = row else {
            return Err(StatusCode::NOT_FOUND);
        };
        let finished = matches!(status.as_str(), "succeeded" | "failed" | "cancelled");
        if finished || tokio::time::Instant::now() >= deadline {
            // The failure summary needs the task rows, so fetch them only when
            // there is a failure to explain. A succeeded run — the common case,
            // and the one that decides this endpoint's cost — pays nothing, and
            // neither does a timed-out poll of a still-running run.
            let failure = if status == "failed" {
                let tasks = sqlx::query_as::<_, TaskRow>(RUN_TASK_ROWS_SQL)
                    .bind(&id)
                    .fetch_all(&state.read_pool)
                    .await
                    .map_err(internal)?;
                summarize_failure(&status, output.as_deref(), &tasks)
            } else {
                None
            };
            // Per the contract, `result` is the run output only on success; a
            // failed/cancelled run's output is an error message, not a result.
            let result = if status == "succeeded" { output } else { None };
            return Ok(Json(RunResult {
                run_id: id,
                status,
                finished,
                result,
                failure,
                whole_reruns,
            }));
        }
        // Block until this run changes, the stream hiccups (lag/close — treat
        // as "recheck now"), the fallback tick fires, or the deadline lands.
        let recheck_at = std::cmp::min(
            deadline,
            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
        );
        loop {
            tokio::select! {
                ev = events.recv() => match ev {
                    Ok(e) if e.run_id == id => break,
                    Ok(_) => continue, // some other run's event — keep waiting
                    Err(_) => break,   // lagged or sender gone — recheck via DB
                },
                _ = tokio::time::sleep_until(recheck_at) => break,
            }
        }
    }
}

/// Map any DB error to 500 without leaking internals to the client.
fn internal(err: sqlx::Error) -> StatusCode {
    tracing::error!(error = ?err, "db query failed");
    StatusCode::INTERNAL_SERVER_ERROR
}

#[cfg(test)]
mod run_spec_group_tests {
    use super::*;

    fn row(id: &str, spec: &str) -> (String, String, Option<String>) {
        (id.to_string(), spec.to_string(), Some("wf".to_string()))
    }
    fn ids(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    /// The case the endpoint exists for: nobody edited the workflow, so N runs
    /// share one spec under N different definition ids. Sending it once is the
    /// whole saving, and "one group" is also the caller's answer to "did
    /// anything change?".
    #[test]
    fn identical_specs_collapse_into_one_group() {
        let g = group_run_specs(
            &ids(&["r1", "r2", "r3"]),
            vec![row("r1", "name: w\n"), row("r2", "name: w\n"), row("r3", "name: w\n")],
        );
        assert_eq!(g.len(), 1, "three runs of one definition are one group");
        assert_eq!(g[0].run_ids, ids(&["r1", "r2", "r3"]));
    }

    /// …and a real edit splits them, in the order the caller asked.
    #[test]
    fn distinct_specs_group_separately_in_requested_order() {
        let g = group_run_specs(
            &ids(&["r3", "r2", "r1"]),
            // Deliberately returned in a different order from the request: a
            // Postgres `= ANY` has no ordering, so the handler must not inherit
            // one from the database.
            vec![row("r1", "name: a\n"), row("r3", "name: b\n"), row("r2", "name: b\n")],
        );
        assert_eq!(g.len(), 2);
        assert_eq!(g[0].yaml, "name: b\n", "the first requested run's spec leads");
        assert_eq!(g[0].run_ids, ids(&["r3", "r2"]));
        assert_eq!(g[1].run_ids, ids(&["r1"]));
    }

    /// A spec that recurs after another one is the *same* group here — the
    /// endpoint groups by content, and "was this reverted?" is a question about
    /// adjacency that only the caller's chronology can answer. Grouping by
    /// content and ordering by request is what lets it: the caller sees r2 and
    /// r4 share a spec and r3 does not.
    #[test]
    fn a_reverted_spec_is_the_same_group_adjacency_is_the_callers_question() {
        let g = group_run_specs(
            &ids(&["r2", "r3", "r4"]),
            vec![row("r2", "name: a\n"), row("r3", "name: b\n"), row("r4", "name: a\n")],
        );
        assert_eq!(g.len(), 2, "two distinct specs, whatever order they ran in");
        assert_eq!(g[0].run_ids, ids(&["r2", "r4"]), "the revert rejoins its content group");
    }

    /// An id with no row — archived, purged, or simply wrong — is left out
    /// rather than failing the batch. A page of runs that includes one archived
    /// row must still answer for the other twenty-four.
    #[test]
    fn unknown_ids_are_absent_not_an_error() {
        let g = group_run_specs(&ids(&["gone", "r1"]), vec![row("r1", "name: w\n")]);
        assert_eq!(g.len(), 1);
        assert_eq!(g[0].run_ids, ids(&["r1"]));
    }

    /// Two definitions whose specs match but whose names don't stay apart. The
    /// name is derived from the spec today, so this should not arise — the
    /// point is that if it ever does, one group reporting a single name would
    /// be a quiet lie rather than a visible bug.
    #[test]
    fn specs_that_match_but_names_that_dont_stay_apart() {
        let g = group_run_specs(
            &ids(&["r1", "r2"]),
            vec![
                ("r1".into(), "name: w\n".into(), Some("old".into())),
                ("r2".into(), "name: w\n".into(), Some("new".into())),
            ],
        );
        assert_eq!(g.len(), 2, "the name is part of the key");
    }

    #[test]
    fn nothing_requested_is_nothing_returned() {
        assert!(group_run_specs(&[], vec![row("r1", "name: w\n")]).is_empty());
    }

    /// `/api/runs/specs` is a literal sibling of `/api/runs/{id}`. Axum panics
    /// at startup on a genuine conflict and otherwise prefers the static
    /// segment — this pins both halves of that, because the failure mode is a
    /// server that either won't boot or silently routes the bulk call into the
    /// single-run handler.
    #[test]
    fn specs_route_is_not_shadowed_by_the_run_id_route() {
        use axum::routing::get;
        // Built exactly as `main.rs` declares them, in the same order.
        let app: axum::Router<()> = axum::Router::new()
            .route("/api/runs/{id}", get(|| async { "one" }))
            .route("/api/runs/specs", get(|| async { "bulk" }))
            .route("/api/runs/{id}/spec", get(|| async { "one-spec" }));

        let hit = |path: &str| {
            let app = app.clone();
            let req = axum::http::Request::builder().uri(path).body(axum::body::Body::empty()).unwrap();
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    use tower::ServiceExt;
                    let res = app.oneshot(req).await.unwrap();
                    let status = res.status();
                    let body = axum::body::to_bytes(res.into_body(), 64).await.unwrap();
                    (status, String::from_utf8(body.to_vec()).unwrap())
                })
        };

        assert_eq!(hit("/api/runs/specs"), (StatusCode::OK, "bulk".to_string()));
        assert_eq!(
            hit("/api/runs/0191f3c2-1111-7000-8000-000000000001"),
            (StatusCode::OK, "one".to_string()),
            "a real run id still reaches the single-run handler"
        );
        assert_eq!(
            hit("/api/runs/0191f3c2-1111-7000-8000-000000000001/spec"),
            (StatusCode::OK, "one-spec".to_string()),
        );
    }
}

#[cfg(test)]
mod failure_summary_tests {
    use super::*;

    /// A task row with only the fields the summary reads; the rest are the
    /// zero values, so a test says what it is about and nothing else.
    fn task(name: &str, status: &str, finished_at: Option<&str>, output: Option<&str>) -> TaskRow {
        TaskRow {
            id: format!("t-{name}"),
            name: name.to_string(),
            status: status.to_string(),
            attempt: 1,
            output: output.map(str::to_string),
            scheduled_at: None,
            finished_at: finished_at.map(str::to_string),
            wake_at: None,
            wait_url: None,
            wait_dataset: None,
            sub_run_id: None,
            pool: None,
            priority: 0,
            cache_hit: false,
            decided_by: None,
            decision_comment: None,
            quota_wait_since: None,
            quota_wait_detail: None,
            quota_waits: 0,
            stop_requested_at: None,
            fault_class: None,
        }
    }

    /// The failure says what broke it, as the engine classified it, and what
    /// another attempt is worth: what a caller deciding whether to run it
    /// again, or to read its code, needs.
    #[test]
    fn a_failure_says_its_class_and_what_another_attempt_is_worth() {
        let mut broke = task(
            "a",
            "failed",
            Some("2026-01-01T00:00:00Z"),
            Some("No space left on device"),
        );
        broke.fault_class = Some("storage".into());
        let failure = summarize_failure("failed", None, &[broke]).unwrap();
        assert_eq!(
            (
                failure.fault_class.as_deref(),
                failure.disposition.as_deref()
            ),
            (Some("storage"), Some("infrastructure"))
        );
        let mut bug = task(
            "b",
            "failed",
            Some("2026-01-01T00:00:00Z"),
            Some("assertion failed"),
        );
        bug.fault_class = Some("user-code".into());
        let failure = summarize_failure("failed", None, &[bug]).unwrap();
        assert_eq!(failure.disposition.as_deref(), Some("application"));
        // Nothing classified it: neither is said.
        let plain = task("c", "failed", Some("2026-01-01T00:00:00Z"), Some("exit 1"));
        let failure = summarize_failure("failed", None, &[plain]).unwrap();
        assert_eq!((failure.fault_class, failure.disposition), (None, None));
        // A class this build does not know reads as unknown, never as another.
        let mut newer = task("d", "failed", Some("2026-01-01T00:00:00Z"), Some("x"));
        newer.fault_class = Some("from-a-newer-engine".into());
        let failure = summarize_failure("failed", None, &[newer]).unwrap();
        assert_eq!(failure.disposition.as_deref(), Some("unknown"));
    }

    #[test]
    fn a_run_with_nothing_wrong_has_no_failure() {
        let tasks = [task("a", "succeeded", Some("2026-01-01T00:00:00Z"), Some("fine"))];
        assert_eq!(summarize_failure("succeeded", None, &tasks), None);
    }

    /// A cancelled run is not a failed one. Reporting a failure for it would
    /// make the field mean "something to look at" on every terminal run.
    #[test]
    fn a_cancelled_run_is_not_a_failure() {
        let tasks = [task("a", "cancelled", Some("2026-01-01T00:00:00Z"), None)];
        assert_eq!(summarize_failure("cancelled", Some("cancelled by operator"), &tasks), None);
    }

    #[test]
    fn the_failed_task_and_its_output_are_reported() {
        let tasks = [
            task("build", "succeeded", Some("2026-01-01T00:00:01Z"), Some("ok")),
            task("test", "failed", Some("2026-01-01T00:00:02Z"), Some("assertion failed: 2 != 3")),
        ];
        let f = summarize_failure("failed", None, &tasks).expect("a failed task is a failure");
        assert_eq!(f.task_name.as_deref(), Some("test"));
        assert_eq!(f.task_id.as_deref(), Some("t-test"));
        assert_eq!(f.failed_tasks, 1);
        assert_eq!(f.message.as_deref(), Some("assertion failed: 2 != 3"));
        assert!(!f.truncated);
    }

    /// With fail-fast off, several tasks fail independently. The one that
    /// finished first is the likeliest cause; the later ones are usually its
    /// casualties, and pointing an agent at a casualty sends it to fix the
    /// wrong task. The count still shows there were others.
    #[test]
    fn the_earliest_failure_is_the_one_reported() {
        let tasks = [
            task("downstream", "failed", Some("2026-01-01T00:00:09Z"), Some("upstream missing")),
            task("root", "failed", Some("2026-01-01T00:00:02Z"), Some("connection refused")),
        ];
        let f = summarize_failure("failed", None, &tasks).unwrap();
        assert_eq!(f.task_name.as_deref(), Some("root"));
        assert_eq!(f.failed_tasks, 2, "the other failure is still counted");
    }

    /// `cancel_overdue_runs` fails the *run* and cancels its tasks, so a
    /// deadline overrun leaves nothing in a `failed` state. This is precisely
    /// the case an agent would otherwise flail on: a failed run whose tasks all
    /// read "cancelled" and whose reason is nowhere in the task rows.
    #[test]
    fn a_run_that_failed_with_no_failed_task_falls_back_to_the_run_reason() {
        let tasks = [task("slow", "cancelled", Some("2026-01-01T00:00:09Z"), None)];
        let f = summarize_failure("failed", Some("run deadline exceeded (run_timeout_secs)"), &tasks)
            .expect("a failed run always explains itself");
        assert_eq!(f.task_id, None, "no task failed, so none is named");
        assert_eq!(f.failed_tasks, 0);
        assert_eq!(f.message.as_deref(), Some("run deadline exceeded (run_timeout_secs)"));
    }

    /// A failure with no recorded text is still a failure. `message: null`
    /// says "nothing was recorded"; omitting the summary would say "nothing
    /// went wrong", which is a different and wrong answer.
    #[test]
    fn a_failure_with_no_output_still_reports_the_task() {
        let tasks = [task("quiet", "failed", Some("2026-01-01T00:00:02Z"), None)];
        let f = summarize_failure("failed", None, &tasks).unwrap();
        assert_eq!(f.task_name.as_deref(), Some("quiet"));
        assert_eq!(f.message, None);
        assert!(!f.truncated);
    }

    /// A task with a failed row while the run is still `running` — fail-fast
    /// off. Surfacing it early is the whole point: an agent polling the run
    /// can stop waiting on work that is already doomed.
    #[test]
    fn a_still_running_run_reports_a_task_that_already_failed() {
        let tasks = [
            task("a", "failed", Some("2026-01-01T00:00:02Z"), Some("boom")),
            task("b", "running", None, None),
        ];
        let f = summarize_failure("running", None, &tasks).unwrap();
        assert_eq!(f.task_name.as_deref(), Some("a"));
    }

    #[test]
    fn the_tail_keeps_the_last_lines_and_says_it_clipped() {
        let text = (1..=50).map(|i| format!("line {i}")).collect::<Vec<_>>().join("\n");
        let (out, truncated) = failure_tail(&text, 20, 4096);
        assert!(truncated);
        assert!(out.starts_with("line 31"), "kept the last 20 lines, got: {out}");
        assert!(out.ends_with("line 50"), "the end is the part that explains it");
    }

    #[test]
    fn a_short_message_is_not_marked_truncated() {
        let (out, truncated) = failure_tail("one\ntwo\n", 20, 4096);
        assert_eq!(out, "one\ntwo");
        assert!(!truncated);
    }

    /// The byte clip slices a `String`, so it must land on a char boundary —
    /// a task that logs UTF-8 must not be able to panic the run detail.
    #[test]
    fn the_byte_clip_cannot_split_a_multibyte_character() {
        let text = "é".repeat(100); // 200 bytes, one line
        let (out, truncated) = failure_tail(&text, 20, 51);
        assert!(truncated);
        assert!(out.len() <= 51);
        assert!(out.chars().all(|c| c == 'é'), "no replacement or partial char");
    }

    #[test]
    fn an_empty_output_is_reported_as_no_message_not_an_empty_one() {
        let tasks = [task("blank", "failed", Some("2026-01-01T00:00:02Z"), Some("   \n"))];
        let f = summarize_failure("failed", None, &tasks).unwrap();
        assert_eq!(f.message, None);
    }
}
