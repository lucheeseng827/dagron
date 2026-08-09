// dagron-api contracts — mirror the Rust structs in dagron-api/src/routes/*.

export type TaskStatus =
  | "pending"
  | "ready"
  | "running"
  | "awaiting_approval"
  | "succeeded"
  | "failed"
  | "skipped"
  | "cancelled";

export type RunStatus = "pending" | "running" | "succeeded" | "failed" | "cancelled";

/// Outcome of resolving a `type: approval` gate — mirrors the OpenAPI
/// `resolution: { enum: [approved, rejected] }` on the approve/reject routes.
export type TaskResolution = "approved" | "rejected";

/// What started a run — derived server-side (schedule stamp / backfill ledger).
export type TriggerKind = "manual" | "schedule" | "backfill";

export interface RunSummary {
  id: string;
  definition_id: string;
  status: RunStatus;
  created_at: string;
  finished_at: string | null;
  /// Workflow/DAG name (from the run's definition); may be null for legacy rows.
  name: string | null;
  trigger_kind: TriggerKind;
  /// What a human decided about this run — acknowledged / resolved / ignored,
  /// or absent for one nobody has looked at yet. `status` is what the engine
  /// did; this is what was done about it.
  triage_state?: "acknowledged" | "resolved" | "ignored";
  triage_note?: string;
  triaged_at?: string;
  triaged_by?: string;
}

export type GitRepoState = "Synced" | "OutOfSync" | "Syncing";

/// `GET /api/git-repos` — repos plus whether a GitOps worker is alive.
///
/// Syncing runs in the separate `dagron-gitops` container, so a deployment can
/// have repositories configured and nothing to act on them. Surfacing that is
/// the point: the page used to promise "Auto-sync ON" while every sync failed.
export interface GitRepoList {
  repos: GitRepo[];
  worker_online: boolean;
  /// Whether the API can encrypt a credential at all. False means
  /// DAGRON_ENV_SECRET_KEY (or a KEK provider) is unset, so the credential form
  /// would only ever 503 — the page says so instead of offering it.
  credentials_configured: boolean;
}

/// How the GitOps worker authenticates to a repository.
///
/// `token` is HTTPS (a PAT, a GitHub App installation token, a GitLab project
/// access token); `ssh` is a private key (a deploy key). Which one is legal
/// follows from the repo's URL, and the API rejects the mismatch rather than
/// storing a credential that could never be used.
export type GitAuthKind = "none" | "token" | "ssh";

/// Write-only credential payload. Nothing here is ever returned by the API —
/// reads get `auth_kind` / `auth_username` / `auth_hint` instead.
export interface GitAuthInput {
  kind: GitAuthKind;
  username?: string;
  token?: string;
  ssh_private_key?: string;
  known_hosts?: string;
}

export interface GitRepo {
  id: string;
  name: string;
  url: string;
  branch: string;
  rev: string | null;
  state: GitRepoState;
  auto_sync: number; // 0/1
  workflow_count: number;
  drift: number;
  last_message: string | null;
  last_synced_at: string | null;
  created_at: string;
  auth_kind: GitAuthKind;
  auth_username: string | null;
  /// Non-secret identifier for the stored credential — `••••cdef` for a token,
  /// `ssh-ed25519 SHA256:…` for a key (the same fingerprint the forge shows next
  /// to the deploy key).
  auth_hint: string | null;
  auth_known_hosts: string | null;
  auth_updated_at: string | null;
}

export interface TaskRow {
  id: string;
  name: string;
  status: TaskStatus;
  attempt: number;
  output: string | null;
  scheduled_at: string | null;
  finished_at: string | null;
  /// Why a parked task is parked — at most one is set. Every park form (time /
  /// HTTP / dataset sensor, sub-workflow trigger) keeps the row `running` with
  /// no lease, so without these a parked task and a hung one look identical.
  wake_at: string | null;
  wait_url: string | null;
  wait_dataset: string | null;
  sub_run_id: string | null;
  /// Scheduling metadata: the named pool the task drew a slot from (#21), its
  /// dispatch priority (#25), and whether it was served from the memoization
  /// store rather than executed (#22) — which explains a 0s success.
  pool: string | null;
  priority: number;
  cache_hit: boolean;
}

/// A dataset in the registry: current state plus who consumes it.
export interface Dataset {
  uri: string;
  updated_at: string;
  last_run_id: string | null;
  last_task: string | null;
  updates: number;
  /// Workflows subscribed via `on_datasets:` — the runs this dataset wakes.
  consumers: string[];
}

/// One entry of the append-only lineage ledger.
export interface DatasetEvent {
  id: number;
  uri: string;
  workflow: string | null;
  run_id: string | null;
  task_id: string | null;
  task_name: string | null;
  source: string;
  at: string;
}

export interface RunDetail {
  /// What a human decided about this run — see RunSummary.triage_state.
  triage_state?: "acknowledged" | "resolved" | "ignored";
  triage_note?: string;
  triaged_at?: string;
  triaged_by?: string;
  id: string;
  definition_id: string;
  status: RunStatus;
  input: string | null;
  output: string | null;
  created_at: string;
  finished_at: string | null;
  /// Workflow/DAG name for the header + backlink; null for legacy rows.
  name: string | null;
  trigger_kind: TriggerKind;
  tasks: TaskRow[];
}

export interface GraphNode {
  id: string;
  name: string;
  status: TaskStatus;
  attempt: number;
  scheduled_at: string | null;
  finished_at: string | null;
  /// Park reason, if any — a parked node is `running` forever until the thing it
  /// waits on happens, which on a graph is indistinguishable from a stuck task.
  wake_at: string | null;
  wait_url: string | null;
  wait_dataset: string | null;
  sub_run_id: string | null;
  /// Resolved from the memoization store instead of executed (#22).
  cache_hit: boolean;
}

export interface GraphEdge {
  source: string;
  target: string;
}

export interface GraphResponse {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

/// Level the API inferred from a log line's head. Best-effort — task output is
/// whatever the command printed, so `plain` is the common case.
export type LogLevel = "error" | "warn" | "info" | "debug" | "trace" | "plain";

/// One line of a filtered log view (see crates/dagron-api/src/routes/logs.rs).
export interface LogLine {
  /// 1-based line number in the task's *unfiltered* output, so a filtered line
  /// can still be located in the raw log.
  n: number;
  level: LogLevel;
  /// The RFC3339 stamp the line itself carried, parsed off the front and
  /// removed from `text` (so filters match the message). Absent for ordinary
  /// output — fall back to the owning task's `started_at`.
  ts?: string;
  text: string;
  /// False for a neighbour kept only by `context=N` — rendered dimmed.
  matched: boolean;
}

/// A merged workflow-log line, attributed to the task that printed it.
export interface RunLogLine extends LogLine {
  task_id: string;
  task: string;
  attempt: number;
}

/// Per-task rollup returned with the workflow log view: every task in the run,
/// selected or not, so the task picker needs no second request.
export interface RunLogTask {
  id: string;
  name: string;
  status: TaskStatus;
  attempt: number;
  /// Inside the request's task/status scope.
  selected: boolean;
  /// Raw line count (0 for an unselected task — its output isn't read).
  total: number;
  /// Lines the filter matched in this task.
  matched: number;
  /// When the task started/finished (RFC3339). Most output carries no time of
  /// its own, so this window bounds every line the task printed — and is as
  /// tight as the task was short.
  started_at?: string;
  finished_at?: string;
}

/// GET /api/runs/:id/logs — the whole run's output as one filtered stream.
export interface RunLogs {
  run_id: string;
  tasks: RunLogTask[];
  lines: RunLogLine[];
  /// Raw lines across the selected tasks.
  total: number;
  /// Lines matched across the selected tasks, *before* the line cap.
  matched: number;
  /// True when the line cap dropped lines that matched.
  truncated: boolean;
  /// True once every selected task is terminal: the view is final.
  eof: boolean;
  /// Whether any filter was in effect (an unfiltered view still caps).
  filtered: boolean;
  /// Effective line cap, echoed so the UI can offer "raise the limit".
  limit: number;
}

export interface TaskLogs {
  task_id: string;
  name: string;
  status: TaskStatus;
  /// When the task started/finished (RFC3339). A line with no `ts` of its own
  /// was printed somewhere inside this window.
  started_at?: string;
  finished_at?: string;
  attempt: number;
  output: string | null;
  /// Char offset this response starts at (tail metadata, see routes/logs.rs).
  offset: number;
  /// Resume point for the next tail poll — the full output's char length.
  /// Always counted on the *raw* text, so a filter can't desync tailing.
  next_offset: number;
  /// True once the task is terminal: no more output will arrive.
  eof: boolean;
  /// Filtered lines; absent when no filter was requested.
  lines?: LogLine[];
  /// Raw lines in the slice this response covers.
  total: number;
  /// Lines the filter matched in that slice, before the line cap.
  matched: number;
  truncated: boolean;
  filtered: boolean;
}

/// SSE event payload from GET /api/runs/:id/stream (one run) and
/// GET /api/events/stream (account-wide, feeds the list pages' live mode).
export interface TaskEvent {
  run_id: string;
}

export interface StatusCount {
  status: string;
  count: number;
}

export interface MetricsResponse {
  runs_by_status: StatusCount[];
  tasks_by_status: StatusCount[];
  dead_letters: number;
}

export interface DeadLetter {
  id: string;
  payload: string;
  error: string;
  source: string;
  failures: number;
  first_seen_at: string;
  last_error_at: string;
}

export interface WorkflowSummary {
  id: string;
  name: string;
  created_at: string;
  updated_at: string;
}

export interface Workflow extends WorkflowSummary {
  spec: string;
  description: string | null;
  /// Lifecycle: active / paused / retired.
  state?: WorkflowState;
  /// Current definition version; the history is /workflows/{id}/versions.
  version?: number;
}

/// One entry in a workflow's definition history. Append-only: an edit adds a
/// version, it never rewrites one.
export interface WorkflowVersion {
  id: string;
  version: number;
  name: string;
  spec: string;
  created_at: string;
  created_by?: string;
}

/// Workflow lifecycle. `paused` and `retired` both refuse to run and neither
/// touches the workflow's schedules — that is the difference from deleting,
/// which cascades them away.
export type WorkflowState = "active" | "paused" | "retired";

/// Enriched Workflows-list row (definition + schedule + recent-run digest).
export interface WorkflowRow {
  /// Lifecycle state of the workflow itself — not `paused`, which is about a
  /// single schedule being disabled.
  state?: WorkflowState;
  id: string;
  name: string;
  description: string | null;
  source: "git" | "manual";
  created_at: string;
  updated_at: string;
  schedule_id: string | null;
  cron_expr: string | null;
  next_fire_at: string | null;
  paused: boolean;
  has_schedule: boolean;
  last_status: RunStatus | null;
  last_at: string | null;
  history: TaskStatus[];
  success_rate: number | null;
  run_count: number;
  /** Organizational labels declared in the spec (#26). */
  tags: string[];
}

export interface Schedule {
  id: string;
  workflow_id: string;
  workflow_name: string;
  cron_expr: string;
  /// IANA timezone the cron is evaluated in (default "UTC").
  timezone: string;
  /// Per-fire `when:` gate expression; null = always fire.
  when_expr: string | null;
  /// `stopStrategy` auto-stop expression; null = never auto-stop.
  stop_expr: string | null;
  /// Set when the stopStrategy tripped (read-only).
  stopped_at: string | null;
  stop_reason: string | null;
  enabled: boolean;
  catchup: boolean;
  catchup_window_secs: number | null;
  catchup_max_runs: number | null;
  next_fire_at: string | null;
  last_fired_at: string | null;
  created_at: string;
  updated_at: string;
}

/// Optional fields accepted by create/update schedule.
export interface ScheduleOptions {
  timezone?: string;
  when_expr?: string;
  stop_expr?: string;
  enabled?: boolean;
  catchup?: boolean;
  catchup_window_secs?: number | null;
  catchup_max_runs?: number | null;
}

// ── wired-backend types (approvals, backfills, archive, health, admin) ───────

export interface PendingApproval {
  run_id: string;
  task_id: string;
  task_name: string;
  workflow_name: string | null;
  since: string | null;
}

export type BackfillStatus = "running" | "completed" | "cancelled";

export interface BackfillView {
  id: string;
  schedule_id: string;
  cron_expr: string;
  timezone: string;
  range_from: string;
  range_to: string;
  cursor: string;
  status: BackfillStatus;
  max_runs: number;
  requested: number;
  fired: number;
  created_at: string;
  updated_at: string;
}

export interface ArchivedRunSummary {
  run_id: string;
  name: string;
  status: string;
  created_at: string | null;
  finished_at: string | null;
  archived_at: string;
  compacted_at: string | null;
  parquet_path: string | null;
}

/// `dagron.run-archive.v1` document served by GET /api/archive/runs/:id.
export interface ArchivedRunDoc {
  format: string;
  run: {
    id: string;
    status: string;
    created_at?: string | null;
    finished_at?: string | null;
    output?: string | null;
  };
  definition?: { name?: string | null; spec?: string | null } | null;
  tasks?: Array<{
    id: string;
    name: string;
    status: string;
    attempt?: number;
    output?: string | null;
    scheduled_at?: string | null;
    finished_at?: string | null;
  }>;
  index: ArchivedRunSummary;
}

export interface HealthResponse {
  api: string;
  /// Build edition: "oss" | "enterprise" — gates enterprise-only screens
  /// (audit log, viewer role) in the chrome.
  edition: string;
  db: string;
  scheduler_leader: boolean;
  leader_holder: string | null;
  active_runs: number;
  awaiting_approvals: number;
  dead_letters: number;
}

export interface DayBucket {
  day: string;
  succeeded: number;
  failed: number;
  cancelled: number;
  active: number;
  avg_duration_secs: number | null;
  max_duration_secs: number | null;
}

/// A personal access token as the API returns it — prefix only, never the
/// secret. The plaintext exists exactly once, in the CreatedToken below.
export interface ApiToken {
  id: string;
  name: string;
  /// The cleartext head, e.g. `dgp_D-5E8bxXpY` — enough to match a token
  /// against the one you copied, useless as a credential.
  prefix: string;
  created_at: string;
  expires_at?: string;
  last_used_at?: string;
  revoked_at?: string;
}

/// The one response that carries the secret. Shown once and never retrievable:
/// only its hash is stored.
export interface CreatedToken {
  id: string;
  name: string;
  prefix: string;
  token: string;
  expires_at?: string;
}

export interface UserView {
  id: string;
  email: string;
  name: string;
  groups: string[];
  created_at: string;
}

export interface AuditEntry {
  id: string;
  at: string;
  user_email: string;
  method: string;
  path: string;
  status: number;
}

/// Notification event names accepted by both the notify spec and the global
/// defaults' `on` lists.
export type NotifyEvent = "succeeded" | "failed" | "cancelled" | "deadline_exceeded";

/// Instance-wide notification defaults (Settings → Notifications). Empty `on`
/// lists mean each target's built-in default: Slack = incidents only, webhook =
/// every event. The engine applies these to every run on top of any
/// per-workflow `notify:` block.
export interface NotificationSettings {
  slack_enabled: boolean;
  slack_webhook_url: string;
  slack_on: NotifyEvent[];
  webhook_enabled: boolean;
  webhook_url: string;
  webhook_on: NotifyEvent[];
}

export interface NotifyTestResult {
  slack: string;
  webhook: string;
}

// ── environments (variable sets + write-only secrets) ────────────────────────

export interface EnvironmentView {
  id: string;
  name: string;
  description: string | null;
  /// Plain variables, templatable as {{ env.NAME }} in workflow specs.
  variables: Record<string, string>;
  /// Secret names only — values are write-only by design.
  secret_names: string[];
  /// Whether the server can store secrets (DAGRON_ENV_SECRET_KEY configured).
  secrets_configured: boolean;
  created_at: string;
  updated_at: string;
}

// ── global search (⌘K palette) ───────────────────────────────────────────────

export interface SearchWorkflowHit {
  id: string;
  name: string;
  description: string | null;
}

export interface SearchRunHit {
  id: string;
  name: string | null;
  status: string;
  created_at: string;
}

export interface SearchScheduleHit {
  id: string;
  workflow_id: string;
  workflow_name: string;
  cron_expr: string;
  enabled: number;
}

export interface SearchResponse {
  query: string;
  workflows: SearchWorkflowHit[];
  runs: SearchRunHit[];
  schedules: SearchScheduleHit[];
}
