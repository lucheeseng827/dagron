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
}

export type GitRepoState = "Synced" | "OutOfSync" | "Syncing";

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
}

export interface TaskRow {
  id: string;
  name: string;
  status: TaskStatus;
  attempt: number;
  output: string | null;
  scheduled_at: string | null;
  finished_at: string | null;
}

export interface RunDetail {
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
}

export interface GraphEdge {
  source: string;
  target: string;
}

export interface GraphResponse {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export interface TaskLogs {
  task_id: string;
  name: string;
  status: TaskStatus;
  attempt: number;
  output: string | null;
  /// Char offset this response starts at (tail metadata, see graph.rs).
  offset: number;
  /// Resume point for the next tail poll — the full output's char length.
  next_offset: number;
  /// True once the task is terminal: no more output will arrive.
  eof: boolean;
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
}

/// Enriched Workflows-list row (definition + schedule + recent-run digest).
export interface WorkflowRow {
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
