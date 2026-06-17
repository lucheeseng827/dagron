// dagron-api contracts — mirror the Rust structs in dagron-api/src/routes/*.

export type TaskStatus =
  | "pending"
  | "ready"
  | "running"
  | "succeeded"
  | "failed"
  | "skipped"
  | "cancelled";

export type RunStatus = "pending" | "running" | "succeeded" | "failed" | "cancelled";

export interface RunSummary {
  id: string;
  definition_id: string;
  status: RunStatus;
  created_at: string;
  finished_at: string | null;
  /// Workflow/DAG name (from the run's definition); may be null for legacy rows.
  name: string | null;
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
}

/// SSE event payload from GET /api/runs/:id/stream
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
  enabled: boolean;
  next_fire_at: string | null;
  last_fired_at: string | null;
  created_at: string;
  updated_at: string;
}
