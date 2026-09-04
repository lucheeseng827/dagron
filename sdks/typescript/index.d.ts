// Type declarations for the dagron SDK (hand-written so TS consumers get types
// without a build step).

export interface TaskOptions {
  /** Container image (maps to dagron `docker_image`). */
  image?: string;
  /** argv to run. */
  command?: string[];
  /** Names of upstream tasks this one depends on. */
  dependsOn?: string[];
  /** Runner class (pool) that may claim this task (maps to `runner_class`). */
  runnerClass?: string;
}

/** Inferred severity of a stored log line — best-effort, from the line's head. */
export type LogLevel = "error" | "warn" | "info" | "debug" | "trace" | "plain";

/**
 * The server-side log filter accepted by both log endpoints. All fields
 * optional; omitting everything returns unfiltered output (still line-capped).
 */
export interface LogFilter {
  /** Keep only lines containing this text. */
  q?: string;
  /** Drop lines containing this text. */
  exclude?: string;
  /** Keep only lines matching this regular expression. */
  regex?: string;
  /** Keep only these inferred levels. */
  level?: LogLevel | LogLevel[];
  /** Match case-sensitively (default: insensitive). */
  case?: boolean;
  /** Also keep this many lines either side of each match. */
  context?: number;
  /** Maximum lines to return (0/omitted = the server default). */
  limit?: number;
  /** When capped, keep the last lines instead of the first. */
  tail?: boolean;
}

export declare const LOG_FILTER_PARAMS: readonly string[];
export declare function logFilterParams(filter?: LogFilter): Record<string, unknown>;

export interface DagSpec {
  name: string;
  runner_class?: string;
  tasks: Array<{
    name: string;
    docker_image?: string;
    command?: string[];
    depends_on?: string[];
    runner_class?: string;
  }>;
}

export declare class Dag {
  readonly name: string;
  readonly runnerClass?: string;
  constructor(name: string, opts?: { runnerClass?: string });
  /** Add a task; returns its name (use it in a later task's `dependsOn`). */
  task(name: string, opts?: TaskOptions): string;
  /** Build the dagron spec (validates dependency references). */
  toSpec(): DagSpec;
  /** dagron spec as JSON (valid dagron input). */
  toJSON(): string;
  /** POST the DAG to dagron-api `/api/runs` (wrapped as `{yaml}`); resolves to the new run id. */
  submit(apiUrl: string, opts?: { token?: string }): Promise<string>;
}

/** A DAG, a spec object, or a YAML/JSON spec string. */
export type SpecLike = Dag | Record<string, unknown> | string;

/** One Server-Sent Event from {@link Client.streamRun}. */
export interface StreamEvent {
  event: string;
  data: unknown;
}

/**
 * Why a run failed, summarised in the run detail (`RunDetail.failure`) so an
 * agent need not fetch task rows and their logs to learn it. `null` on the run
 * detail when nothing failed.
 */
export interface RunFailure {
  /** The failed task this summarises; `null` when the run failed with no task
   *  failing (a `run_timeout_secs` overrun cancels its tasks). */
  task_id: string | null;
  task_name: string | null;
  attempt: number | null;
  /** How many tasks in this run are `failed`. */
  failed_tasks: number;
  /** The tail of the failure text, clipped — see `truncated`. */
  message: string | null;
  /** Whether `message` is a tail of something longer. */
  truncated: boolean;
}

/** Run detail as returned by {@link Client.getRun}. */
export interface RunDetail {
  id: string;
  definition_id: string;
  status: string;
  input: string | null;
  output: string | null;
  created_at: string;
  finished_at: string | null;
  /** Workflow/DAG name from the run's definition. */
  name: string | null;
  /** Derived trigger kind: `manual` | `schedule` | `backfill`. */
  trigger_kind: string;
  triage_state: string | null;
  triage_note: string | null;
  triaged_at: string | null;
  triaged_by: string | null;
  /** Whether the engine's wall clock was trustworthy when the run was recorded:
   *  `synced` | `drifted` | `unknown`, or `null` when never assessed. */
  clock_confidence: string | null;
  /** Measured wall-vs-monotonic offset (ms) behind a `drifted` verdict. */
  clock_offset_ms: number | null;
  /** What produced the verdict (`sync-file` | `step` | `behind-datastore`). */
  clock_source: string | null;
  /** Why this run failed, when it did — `null` otherwise, so a caller can
   *  branch on presence without inspecting `status`. */
  failure: RunFailure | null;
  /** Per-task rows (`{id, name, status, attempt, output, …}`). */
  tasks: Array<Record<string, unknown>>;
}

/** Error thrown by {@link Client}. */
export declare class DagronError extends Error {
  /** HTTP status code, or `0` for a transport-level failure. */
  readonly status: number;
  /** Raw response body, when available. */
  readonly body?: string;
  constructor(status: number, message: string, body?: string);
}

/** Typed client for the dagron-api gateway (`/api/...`). Zero deps, Node 18+. */
export declare class Client {
  baseUrl: string;
  token: string | null;
  timeout: number;
  constructor(baseUrl: string, opts?: { token?: string; timeout?: number });

  // auth
  login(email: string, password: string): Promise<string>;
  logout(): Promise<void>;
  me(): Promise<Record<string, unknown>>;
  createUser(
    email: string,
    password: string,
    name: string,
    groups?: string[],
  ): Promise<Record<string, unknown>>;

  // runs
  submitRun(
    spec: SpecLike,
    opts?: { parameters?: Record<string, string>; idempotencyKey?: string },
  ): Promise<string>;
  listRuns(opts?: {
    status?: string;
    limit?: number;
    offset?: number;
  }): Promise<Array<Record<string, unknown>>>;
  getRun(runId: string): Promise<RunDetail>;
  getRunGraph(runId: string): Promise<Record<string, unknown>>;
  getTaskLogs(
    runId: string,
    taskId: string,
    opts?: LogFilter & { offset?: number },
  ): Promise<Record<string, unknown>>;
  getRunLogs(
    runId: string,
    opts?: LogFilter & { tasks?: string[]; statuses?: string[] },
  ): Promise<Record<string, unknown>>;
  cancelRun(runId: string): Promise<number>;
  rerunRun(
    runId: string,
    opts?: { params?: Record<string, unknown> },
  ): Promise<Record<string, unknown>>;
  resubmitRun(runId: string): Promise<string>;
  retryTask(runId: string, taskId: string): Promise<boolean>;
  approveTask(runId: string, taskId: string): Promise<Record<string, unknown>>;
  rejectTask(runId: string, taskId: string): Promise<Record<string, unknown>>;
  streamRun(runId: string, opts?: { timeout?: number }): AsyncGenerator<StreamEvent>;
  waitForRun(
    runId: string,
    opts?: { pollInterval?: number; timeout?: number | null },
  ): Promise<Record<string, unknown>>;

  // workflows
  listWorkflows(): Promise<Array<Record<string, unknown>>>;
  getWorkflow(workflowId: string): Promise<Record<string, unknown>>;
  createWorkflow(
    spec: SpecLike,
    opts?: { name?: string; description?: string },
  ): Promise<Record<string, unknown>>;
  updateWorkflow(
    workflowId: string,
    spec: SpecLike,
    opts?: { name?: string; description?: string },
  ): Promise<Record<string, unknown>>;
  deleteWorkflow(workflowId: string): Promise<void>;
  runWorkflow(workflowId: string): Promise<Record<string, unknown>>;
  syncWorkflowToGit(workflowId: string): Promise<Record<string, unknown>>;

  // schedules
  listSchedules(opts?: { workflowId?: string }): Promise<Array<Record<string, unknown>>>;
  createSchedule(
    workflowId: string,
    cronExpr: string,
    opts?: { enabled?: boolean },
  ): Promise<Record<string, unknown>>;
  updateSchedule(
    scheduleId: string,
    opts?: { cronExpr?: string; enabled?: boolean },
  ): Promise<Record<string, unknown>>;
  deleteSchedule(scheduleId: string): Promise<void>;
  backfillSchedule(
    scheduleId: string,
    from: string,
    to: string,
    opts?: { maxRuns?: number },
  ): Promise<Record<string, unknown>>;

  // backfill jobs
  createBackfill(
    scheduleId: string,
    from: string,
    to: string,
    opts?: { maxRuns?: number },
  ): Promise<Record<string, unknown>>;
  listBackfills(opts?: {
    scheduleId?: string;
    limit?: number;
  }): Promise<Array<Record<string, unknown>>>;
  getBackfill(backfillId: string): Promise<Record<string, unknown>>;
  cancelBackfill(backfillId: string): Promise<Record<string, unknown>>;

  // dead letters
  listDeadLetters(opts?: { limit?: number }): Promise<Array<Record<string, unknown>>>;
  redriveDeadLetter(deadLetterId: string): Promise<Record<string, unknown>>;
  discardDeadLetter(deadLetterId: string): Promise<void>;

  // git repos
  listGitRepos(): Promise<Array<Record<string, unknown>>>;
  connectGitRepo(
    url: string,
    opts?: { branch?: string; autoSync?: boolean; path?: string },
  ): Promise<Record<string, unknown>>;
  syncGitRepo(repoId: string): Promise<Record<string, unknown>>;
  disconnectGitRepo(repoId: string): Promise<void>;

  // observability
  metrics(): Promise<Record<string, unknown>>;
  healthz(): Promise<string>;
}
