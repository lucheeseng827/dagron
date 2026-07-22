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
  submitRun(spec: SpecLike): Promise<string>;
  listRuns(opts?: {
    status?: string;
    limit?: number;
    offset?: number;
  }): Promise<Array<Record<string, unknown>>>;
  getRun(runId: string): Promise<Record<string, unknown>>;
  getRunGraph(runId: string): Promise<Record<string, unknown>>;
  getTaskLogs(runId: string, taskId: string): Promise<Record<string, unknown>>;
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
