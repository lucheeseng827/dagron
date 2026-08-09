//! Workflow + task **log read** endpoints, with an explicit, adjustable filter.
//!
//! Two views over the same stored text (`task_runs.output`):
//!
//! - `GET /api/runs/{id}/logs` — the **workflow** view: every task's output in
//!   the run merged into one attributed stream. This is the view you want when
//!   you don't yet know which task failed, which is most of the time — clicking
//!   through fourteen task panels to find the one that printed the stack trace
//!   is the workflow-log problem, not a missing feature on any single task.
//! - `GET /api/runs/{id}/tasks/{tid}/logs` — the **task** view, with live
//!   tailing (`?offset=`). Unchanged when no filter is asked for.
//!
//! Both accept the same filter grammar
//! ([`dagron_logging::logfilter`]) — `q` / `exclude` / `regex` / `level` /
//! `case` / `context` / `limit` / `tail` — so a filter typed on one view means
//! the same thing on the other, and in a shared link. The filter is applied
//! **server-side**: shipping 400MB of output to a browser so it can grep it is
//! not a filter, it's a download.
//!
//! Filtering never mutates stored output. Every response reports `total`
//! (unfiltered lines) and `matched` (lines the filter selected, before the line
//! cap), so a view that hides most of a run always says so.

use axum::extract::{Path, RawQuery, State};
use axum::http::StatusCode;
use axum::Json;
use dagron_logging::logfilter::{self, FilterResult, LogFilter};
use serde::Serialize;

use crate::auth::AuthUser;
use crate::state::AppState;

/// A task's statuses that mean "no further output will arrive".
const TERMINAL: [&str; 4] = ["succeeded", "failed", "skipped", "cancelled"];

fn is_terminal(status: &str) -> bool {
    TERMINAL.contains(&status)
}

// ── request scoping ──────────────────────────────────────────────────────────

/// The endpoint's own params, read from the same query string as the filter.
///
/// These are *not* part of the filter grammar: they choose which task output is
/// read at all, whereas the filter chooses which lines survive. Keeping them
/// separate is what lets a filter be stored and replayed without also pinning
/// itself to one task.
#[derive(Debug, Default)]
struct Scope {
    /// Restrict to these tasks, by name or task-run id (repeatable or CSV).
    tasks: Vec<String>,
    /// Restrict to tasks in these statuses (repeatable or CSV).
    statuses: Vec<String>,
    /// Task view only: resume tailing from this char offset.
    offset: Option<usize>,
}

impl Scope {
    fn from_pairs(pairs: &[(String, String)]) -> Result<Scope, String> {
        let mut s = Scope::default();
        for (k, v) in pairs {
            match k.as_str() {
                "task" | "tasks" => s.tasks.extend(csv(v)),
                "status" | "statuses" => s.statuses.extend(csv(v)),
                "offset" => {
                    s.offset = Some(
                        v.trim()
                            .parse::<usize>()
                            .map_err(|_| format!("offset must be a number, got {v:?}"))?,
                    )
                }
                _ => {}
            }
        }
        Ok(s)
    }

    /// Does this task belong to the requested scope? An empty scope selects all.
    fn selects(&self, id: &str, name: &str, status: &str) -> bool {
        let by_task = self.tasks.is_empty()
            || self.tasks.iter().any(|t| t == id || t.eq_ignore_ascii_case(name));
        let by_status =
            self.statuses.is_empty() || self.statuses.iter().any(|s| s.eq_ignore_ascii_case(status));
        by_task && by_status
    }
}

/// Split a repeatable/CSV param value into non-empty trimmed items.
fn csv(v: &str) -> Vec<String> {
    v.split(',').map(|p| p.trim()).filter(|p| !p.is_empty()).map(|p| p.to_string()).collect()
}

/// Parse the filter + scope out of a raw query string, mapping every parse
/// failure to a 400 with the reason. A bad regex is the caller's typo, not a
/// server error, and silently ignoring it would show them an unfiltered wall of
/// text they'd read as "no matches were filtered out".
fn parse_request(raw: Option<String>) -> Result<(LogFilter, Scope, bool), (StatusCode, String)> {
    let raw = raw.unwrap_or_default();
    let pairs = logfilter::parse_pairs(&raw);
    let filter = LogFilter::from_pairs(&pairs).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let scope = Scope::from_pairs(&pairs).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let filtering = logfilter::pairs_have_filter(&pairs);
    Ok((filter, scope, filtering))
}

// ── wire types ───────────────────────────────────────────────────────────────

/// One line of a filtered view.
#[derive(Debug, Serialize)]
pub struct LogLine {
    /// 1-based line number within the owning task's **unfiltered** output, so a
    /// filtered line can still be located in the raw log.
    pub n: usize,
    /// Level inferred from the line's head — best-effort, see
    /// [`dagron_logging::logfilter::Level::infer`].
    pub level: &'static str,
    /// The RFC3339 stamp the line itself carried, if any. Absent for ordinary
    /// output, which is most output; a reader wanting a time for every line
    /// falls back to the owning task's `started_at`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts: Option<String>,
    pub text: String,
    /// False for a neighbour kept only by `context=N`, so the UI can dim it.
    pub matched: bool,
}

/// A line in the merged **workflow** view, attributed to its task.
#[derive(Debug, Serialize)]
pub struct RunLogLine {
    pub task_id: String,
    /// Task name (what a human recognizes; ids are opaque).
    pub task: String,
    pub attempt: i64,
    pub n: usize,
    pub level: &'static str,
    /// The RFC3339 stamp the line itself carried, if any. Absent for ordinary
    /// output; fall back to the owning task's `started_at` in the rollup.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts: Option<String>,
    pub text: String,
    pub matched: bool,
}

/// Per-task rollup returned alongside the merged stream: every task in the run,
/// selected or not, so the UI can render the task picker without a second call
/// and show which tasks the current filter actually hits.
#[derive(Debug, Serialize)]
pub struct RunLogTask {
    pub id: String,
    pub name: String,
    pub status: String,
    pub attempt: i64,
    /// True when this task is inside the request's `task=`/`status=` scope.
    pub selected: bool,
    /// Lines in this task's raw output (0 for an unselected task — its output
    /// isn't read).
    pub total: usize,
    /// Lines the filter matched in this task.
    pub matched: usize,
    /// When the task started and finished (RFC3339, as stored). Most output
    /// carries no time of its own, so these bound every line the task printed:
    /// a line is known to have been emitted inside this window, and the window
    /// is as tight as the task was short. The UI uses `started_at` as the
    /// per-line time when the line has no `ts`, and shows the window so the
    /// uncertainty is visible rather than implied.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
}

/// `GET /api/runs/{id}/logs` response.
#[derive(Debug, Serialize)]
pub struct RunLogs {
    pub run_id: String,
    /// Every task in the run, in execution order (the picker + per-task counts).
    pub tasks: Vec<RunLogTask>,
    /// The merged, filtered lines, in execution order then line order.
    pub lines: Vec<RunLogLine>,
    /// Raw lines across the selected tasks.
    pub total: usize,
    /// Lines matched across the selected tasks, **before** the line cap.
    pub matched: usize,
    /// True when the line cap dropped lines that matched.
    pub truncated: bool,
    /// True once every selected task is terminal: this view is final.
    pub eof: bool,
    /// Whether any filter grammar was in effect (an unfiltered view still caps).
    pub filtered: bool,
    /// The effective line cap, echoed so the UI can offer "raise the limit".
    pub limit: usize,
}

/// `GET /api/runs/{id}/tasks/{tid}/logs` response.
///
/// `output` is the full output when no `offset` is given (back-compat), or the
/// slice from `offset` when tailing; with a filter active it is the filtered
/// text. A client polls with `?offset=next_offset` until `eof`. Offsets are
/// Unicode-scalar counts, so they never split a multibyte character.
#[derive(Debug, Serialize)]
pub struct TaskLogs {
    pub task_id: String,
    pub name: String,
    pub status: String,
    pub attempt: i64,
    /// When this task started and finished (RFC3339, as stored). A line with no
    /// `ts` of its own was printed somewhere inside this window; the window is
    /// as tight as the task was short, so a caller can show the time and say
    /// how much to trust it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
    pub output: Option<String>,
    /// Offset (char count) this response starts at.
    pub offset: usize,
    /// Resume point for the next poll — the char length of the full output.
    /// Always counted on the **raw** text, so a filter can't desync tailing.
    pub next_offset: usize,
    /// True once the task is terminal: no more output will arrive.
    pub eof: bool,
    /// The filtered lines. `None` when no filter was requested, which keeps the
    /// unfiltered response exactly as it was before filtering existed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lines: Option<Vec<LogLine>>,
    /// Raw lines in the slice this response covers.
    pub total: usize,
    /// Lines the filter matched in that slice, before the line cap.
    pub matched: usize,
    pub truncated: bool,
    pub filtered: bool,
}

// ── db rows ──────────────────────────────────────────────────────────────────

#[derive(Debug, sqlx::FromRow)]
struct TaskLogRow {
    task_id: String,
    name: String,
    status: String,
    attempt: i64,
    scheduled_at: Option<String>,
    finished_at: Option<String>,
    output: Option<String>,
}

/// Task rows for one run, in execution order. `scheduled_at` is NULL for a task
/// that never started, which must sort *after* the ones that did — otherwise a
/// run's log view opens on the tasks that produced no output.
const RUN_TASKS_SQL: &str = "SELECT id AS task_id, name, status, attempt,
            scheduled_at, finished_at, output
     FROM task_runs WHERE run_id = $1
     ORDER BY scheduled_at IS NULL, scheduled_at, name";

// ── handlers ─────────────────────────────────────────────────────────────────

/// `GET /api/runs/{id}/logs[?q=&exclude=&regex=&level=&case=&context=&limit=&tail=&task=&status=]`
///
/// The whole run's task output as one attributed, filtered stream. Returns an
/// empty `lines` array (not 404) for a real run whose tasks produced nothing —
/// "this run logged nothing" is an answer, and a 404 would read as "wrong id".
pub async fn get_run_logs(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<String>,
    RawQuery(raw): RawQuery,
) -> Result<Json<RunLogs>, (StatusCode, String)> {
    let (filter, scope, filtered) = parse_request(raw)?;

    let rows = sqlx::query_as::<_, TaskLogRow>(RUN_TASKS_SQL)
        .bind(&id)
        .fetch_all(&state.read_pool)
        .await
        .map_err(internal)?;

    // A run id that matches no task rows is either unknown or has no tasks yet.
    // Distinguish the two so a mistyped id doesn't look like a quiet run.
    if rows.is_empty() {
        let exists = sqlx::query_scalar::<_, i64>("SELECT 1 FROM workflow_runs WHERE id = $1")
            .bind(&id)
            .fetch_optional(&state.read_pool)
            .await
            .map_err(internal)?
            .is_some();
        if !exists {
            return Err((StatusCode::NOT_FOUND, "run not found".into()));
        }
    }

    let limit = filter.effective_limit();
    let mut tasks = Vec::with_capacity(rows.len());
    let mut lines: Vec<RunLogLine> = Vec::new();
    let (mut total, mut matched, mut truncated, mut eof) = (0usize, 0usize, false, true);

    for row in &rows {
        let selected = scope.selects(&row.task_id, &row.name, &row.status);
        if !selected {
            tasks.push(RunLogTask {
                id: row.task_id.clone(),
                name: row.name.clone(),
                status: row.status.clone(),
                attempt: row.attempt,
                selected: false,
                total: 0,
                matched: 0,
                // Reported even for an unselected task: the picker shows when
                // each task ran, which is half of choosing one.
                started_at: row.scheduled_at.clone(),
                finished_at: row.finished_at.clone(),
            });
            continue;
        }
        if !is_terminal(&row.status) {
            eof = false;
        }
        // NOTE: only the *returned* lines are capped. `RUN_TASKS_SQL` has already
        // pulled this task's entire `output` into memory and the filter scans all
        // of it, so a 500MB task costs 500MB per request — and the run page polls
        // this. Bounding the read itself (scope pushed into the WHERE clause,
        // SQL-side slicing, per-task streaming) is tracked as known follow-up
        // work.
        let res = filter.apply(row.output.as_deref().unwrap_or_default());
        total += res.total;
        matched += res.matched;
        truncated |= res.truncated;
        tasks.push(RunLogTask {
            id: row.task_id.clone(),
            name: row.name.clone(),
            status: row.status.clone(),
            attempt: row.attempt,
            selected: true,
            total: res.total,
            matched: res.matched,
            started_at: row.scheduled_at.clone(),
            finished_at: row.finished_at.clone(),
        });
        lines.extend(res.lines.into_iter().map(|l| RunLogLine {
            task_id: row.task_id.clone(),
            task: row.name.clone(),
            attempt: row.attempt,
            n: l.n,
            level: l.level.as_str(),
            ts: l.ts,
            text: l.text,
            matched: l.matched,
        }));
    }

    // Merge-level cap. `tail` keeps the end of the run (the failure), which is
    // what a truncated log view should show by default.
    if lines.len() > limit {
        truncated = true;
        if filter.is_tail() {
            lines.drain(..lines.len() - limit);
        } else {
            lines.truncate(limit);
        }
    }

    Ok(Json(RunLogs {
        run_id: id,
        tasks,
        lines,
        total,
        matched,
        truncated,
        eof,
        filtered,
        limit,
    }))
}

/// `GET /api/runs/{id}/tasks/{tid}/logs[?offset=N][&<filter>]` — one task's
/// output, scoped to the run (so a task id can't be probed against the wrong
/// run). 404 if not found.
///
/// With `?offset=` it returns only the output past that char offset for live
/// tailing (#17): poll with `?offset=next_offset` until `eof`. A filter applies
/// **within** that slice, so a tailing client with `level=error` set appends
/// only the new error lines — while `next_offset` keeps advancing over the raw
/// text, so toggling the filter mid-tail never loses or repeats output.
pub async fn get_task_logs(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path((id, tid)): Path<(String, String)>,
    RawQuery(raw): RawQuery,
) -> Result<Json<TaskLogs>, (StatusCode, String)> {
    let (filter, scope, filtered) = parse_request(raw)?;

    let row = sqlx::query_as::<_, TaskLogRow>(
        "SELECT id AS task_id, name, status, attempt,
                scheduled_at, finished_at, output
         FROM task_runs WHERE id = $1 AND run_id = $2",
    )
    .bind(&tid)
    .bind(&id)
    .fetch_optional(&state.read_pool)
    .await
    .map_err(internal)?
    .ok_or((StatusCode::NOT_FOUND, "task not found in this run".to_string()))?;

    let full = row.output.unwrap_or_default();
    let next_offset = full.chars().count();
    let eof = is_terminal(&row.status);
    // Slice on a char boundary by skipping whole scalars — never panics on UTF-8.
    let slice = match scope.offset {
        Some(off) if off < next_offset => full.chars().skip(off).collect::<String>(),
        Some(_) => String::new(), // caller is caught up
        None => full,
    };
    let offset = scope.offset.unwrap_or(0).min(next_offset);

    let (output, lines, res) = if filtered {
        let res = filter.apply(&slice);
        let lines = res
            .lines
            .iter()
            .map(|l| LogLine {
                n: l.n,
                level: l.level.as_str(),
                ts: l.ts.clone(),
                text: l.text.clone(),
                matched: l.matched,
            })
            .collect();
        (res.to_text(), Some(lines), res)
    } else {
        // No filter asked for: return the slice untouched, byte for byte.
        let total = slice.lines().count();
        (slice, None, FilterResult { lines: Vec::new(), total, matched: total, truncated: false })
    };

    Ok(Json(TaskLogs {
        task_id: row.task_id,
        name: row.name,
        status: row.status,
        attempt: row.attempt,
        started_at: row.scheduled_at,
        finished_at: row.finished_at,
        output: Some(output),
        offset,
        next_offset,
        eof,
        lines,
        total: res.total,
        matched: res.matched,
        truncated: res.truncated,
        filtered,
    }))
}

fn internal(err: sqlx::Error) -> (StatusCode, String) {
    tracing::error!(error = ?err, "db query failed");
    (StatusCode::INTERNAL_SERVER_ERROR, "database error".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_defaults_to_everything() {
        let s = Scope::from_pairs(&logfilter::parse_pairs("")).unwrap();
        assert!(s.selects("t1", "extract", "succeeded"));
        assert_eq!(s.offset, None);
    }

    #[test]
    fn scope_matches_task_by_name_or_id() {
        let s = Scope::from_pairs(&logfilter::parse_pairs("task=extract")).unwrap();
        assert!(s.selects("t1", "extract", "succeeded"));
        assert!(!s.selects("t2", "load", "succeeded"));
        let s = Scope::from_pairs(&logfilter::parse_pairs("task=t2")).unwrap();
        assert!(s.selects("t2", "load", "succeeded"));
        // Ids are opaque and exact; names are what humans type, so those are
        // matched case-insensitively.
        let s = Scope::from_pairs(&logfilter::parse_pairs("task=EXTRACT")).unwrap();
        assert!(s.selects("t1", "extract", "succeeded"));
        assert!(!Scope::from_pairs(&logfilter::parse_pairs("task=T2")).unwrap().selects("t2", "load", "ok"));
    }

    #[test]
    fn scope_accepts_csv_and_repeats() {
        let s = Scope::from_pairs(&logfilter::parse_pairs("task=extract,load")).unwrap();
        assert!(s.selects("t1", "extract", "succeeded"));
        assert!(s.selects("t2", "load", "succeeded"));
        let s = Scope::from_pairs(&logfilter::parse_pairs("task=extract&task=load")).unwrap();
        assert!(s.selects("t2", "load", "succeeded"));
    }

    #[test]
    fn scope_ands_task_and_status() {
        let s = Scope::from_pairs(&logfilter::parse_pairs("task=extract&status=failed")).unwrap();
        assert!(s.selects("t1", "extract", "failed"));
        assert!(!s.selects("t1", "extract", "succeeded"), "status must also match");
        assert!(!s.selects("t2", "load", "failed"), "task must also match");
    }

    #[test]
    fn scope_rejects_a_non_numeric_offset() {
        assert!(Scope::from_pairs(&logfilter::parse_pairs("offset=soon")).is_err());
        assert_eq!(Scope::from_pairs(&logfilter::parse_pairs("offset=12")).unwrap().offset, Some(12));
    }

    #[test]
    fn parse_request_flags_filtering_only_when_asked() {
        let (_, _, filtering) = parse_request(Some("offset=10".into())).unwrap();
        assert!(!filtering, "tailing alone is not filtering");
        let (_, _, filtering) = parse_request(Some("offset=10&level=error".into())).unwrap();
        assert!(filtering);
        let (_, _, filtering) = parse_request(None).unwrap();
        assert!(!filtering);
    }

    #[test]
    fn parse_request_rejects_a_bad_filter_with_400() {
        let (code, msg) = parse_request(Some("regex=%5B".into())).unwrap_err();
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert!(msg.contains("invalid regex"), "the reason must reach the caller: {msg}");
        let (code, _) = parse_request(Some("level=loud".into())).unwrap_err();
        assert_eq!(code, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn csv_trims_and_drops_empties() {
        assert_eq!(csv(" a , ,b "), vec!["a".to_string(), "b".to_string()]);
        assert!(csv(",,").is_empty());
    }

    #[test]
    fn terminal_statuses_are_eof() {
        assert!(is_terminal("succeeded") && is_terminal("cancelled"));
        assert!(!is_terminal("running") && !is_terminal("awaiting_approval"));
    }
}
