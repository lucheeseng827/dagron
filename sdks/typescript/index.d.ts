// Type declarations for the dagron SDK (hand-written so TS consumers get types
// without a build step). The version tracks the dagron-api version this SDK
// covers — `0.9.x` means "speaks to the 0.9 gateway".

/** A literal env var, or one resolved from a secret at dispatch. */
export type EnvEntry =
  | { name: string; value: string | number | boolean }
  | { name: string; valueFrom: string | { secret: string }; value?: string };

/** `wait:` — a deferrable sensor. Exactly one form is set. */
export interface WaitSpec {
  /** Relative span anchored when the task is reached, e.g. `"5m"` (wire: `for`). */
  duration?: string;
  /** Absolute RFC3339 instant to wait until. */
  until?: string;
  /** HTTP(S) endpoint to poll until it answers 2xx. */
  url?: string;
  /** Dataset to wait on — succeeds on a *fresh* update, not any update. */
  dataset?: string;
}

/** `cache:` — result memoization keyed by the resolved `key`. */
export interface CacheSpec {
  key: string;
  max_age_secs?: number;
}

/**
 * `repeat:` — re-run the task until `until` holds.
 *
 * Field names are the engine's, not camelCase: this object goes to the wire as
 * written, and `task()` rejects any other key rather than emitting a spec that
 * validates locally and is refused on submit.
 */
export interface RepeatSpec {
  until: string;
  max_iterations: number;
  delay_secs?: number;
}

/** When a task runs relative to its dependencies' outcomes. */
export type TriggerRule =
  | "all_success"
  | "all_done"
  | "one_failed"
  | "all_failed"
  | "none_failed";

/** Task kinds that run no command and park instead. */
export type CommandlessTaskType = "approval" | "workflow" | "wait";

export declare const TRIGGER_RULES: readonly TriggerRule[];
export declare const COMMANDLESS_TASK_TYPES: readonly CommandlessTaskType[];

/**
 * Everything `task()` accepts, mapping one-to-one onto the engine's `TaskSpec`.
 * An unknown key throws rather than being silently dropped.
 */
/** A text file a {@link Recipe} places in the image. */
export interface RecipeFileInit {
  /** Absolute destination path inside the image. */
  path: string;
  /** The file's text. */
  content: string;
  /** `true` → mode 0755, else 0644. */
  executable?: boolean;
}

/** Everything a {@link Recipe} can say beyond its name and base image. */
export interface RecipeOptions {
  /** Debian/Ubuntu packages installed with apt-get (the base must be apt-based). */
  apt?: string[];
  /** Python requirement specifiers installed with pip. */
  pip?: string[];
  /**
   * `ENV` lines. **Baked into the image**, so visible to `docker inspect`, to
   * every task that runs it, and to anyone who can pull it. Keys must match
   * `[A-Za-z_][A-Za-z0-9_]*`; a value carrying a `user:password@` URL is
   * refused.
   */
  env?: Record<string, string>;
  /** `WORKDIR` — absolute. */
  workdir?: string;
  /** Text files copied into the image at absolute paths. */
  files?: Array<RecipeFile | RecipeFileInit>;
  /** Extra `RUN` lines, in order, after the files are in place. */
  run?: string[];
  /** Final `USER`. Omit to keep the base image's. */
  user?: string;
  /**
   * Keep the base image's `ENTRYPOINT`. Off by default: the generated
   * Dockerfile ends with `ENTRYPOINT []` so a task's argv is exec'd verbatim.
   */
  keepEntrypoint?: boolean;
  /** Target platform, e.g. `"linux/arm64"`. Omit for the builder's native one. */
  platform?: string;
  /**
   * Verbatim Dockerfile. Used as-is, and every field above except `files` and
   * `platform` is then ignored — though all of them still hash.
   */
  dockerfile?: string;
}

/**
 * Identifies the recipe format and the Dockerfile synthesised from it. Hashed
 * with the recipe, so bumping it re-tags every image on purpose.
 */
export declare const BUILD_GENERATOR_VERSION: string;

/** A text file a {@link Recipe} places in the image. */
export declare class RecipeFile {
  readonly path: string;
  readonly content: string;
  readonly executable: boolean;
  constructor(path: string, content: string, opts?: { executable?: boolean });
  /** The canonical form (`executable` omitted when false). */
  toObject(): Record<string, unknown>;
}

/**
 * What a task's image should contain, instead of a Dockerfile.
 *
 * The tag is a function of the recipe, so the image reference is known before
 * the image exists — which is what lets {@link Dag.task} pin a task to an image
 * the same workflow is about to build. Three implementations must agree on that
 * derivation byte for byte (this one, the Python SDK, and the Rust builder);
 * `sdks/recipe-vectors.json` is the fixed point they are all tested against.
 */
export declare class Recipe {
  readonly name: string;
  readonly base: string;
  constructor(name: string, base: string, opts?: RecipeOptions);
  /** The recipe as the builder serialises it: declaration order, empties omitted. */
  toObject(): Record<string, unknown>;
  /** The exact bytes that are hashed. */
  canonicalJson(): string;
  /** Lowercase hex SHA-256 over the generator version and the canonical form. */
  hash(): string;
  /** The content-addressed tag, `r-<16 hex>`. */
  tag(): string;
  /** `<prefix>/<name>:<tag>`, or `<name>:<tag>` with no prefix. */
  imageRef(repositoryPrefix?: string): string;
  /** Throw if this recipe could not produce a well-formed image. */
  validate(): void;
}

export interface TaskOptions {
  /** Container image (maps to dagron `docker_image`). */
  image?: string | Recipe;
  /** argv to run — this is what makes the task a *leaf*. */
  command?: string[];
  /** Names of upstream tasks this one depends on. */
  dependsOn?: string[];
  /** Chain another **saved** workflow as this step (inlined at run creation). */
  workflowRef?: string;
  /** Call a template declared on this spec (see {@link Dag.template}). */
  template?: string;
  /** Arguments for the callee — a `template` call or a `type: workflow` trigger. */
  arguments?: Record<string, string>;
  /** Task kind (maps to `type`): `approval` | `workflow` | `wait`. */
  taskType?: CommandlessTaskType | "task";
  /** For a `type: workflow` trigger: the registered workflow to run as a child. */
  workflow?: string;
  /** For a `type: wait` sensor: which deadline or signal to park on. */
  wait?: WaitSpec;
  /** For a `type: approval` gate: seconds before the timeout default applies. */
  approvalTimeoutSecs?: number;
  /** What an expired approval defaults to (default `reject` — a gate fails safe). */
  approvalOnTimeout?: "approve" | "reject";
  /** Arbitrary JSON handed to the task. */
  input?: unknown;
  /** Conditional gate; may read `{{ tasks.<dep>.output }}` if it depends on `<dep>`. */
  when?: string;
  /** When this task runs relative to its deps' outcomes (default `all_success`). */
  triggerRule?: TriggerRule;
  /** Lifecycle hook — auto-wired to every non-hook task. */
  hook?: "on_exit" | "on_failure";
  /** This task failing does not fail the run. */
  allowFailure?: boolean;
  /** Fan-out: expand once per item (`{{ item }}` substitutes). */
  withItems?: unknown[];
  /** Fan-out from a parameter holding a JSON array string. */
  withParam?: string;
  /** Readable label template for fan-out instances. */
  instanceKey?: string;
  /** Attempts before the task is marked failed (>= 1; 1 = no retries). */
  maxAttempts?: number;
  /** Base delay between retries; the actual delay doubles per attempt. */
  retryDelaySecs?: number;
  /** Ceiling on the exponential retry backoff. */
  retryMaxDelaySecs?: number;
  /** Whether a task killed by its deadline is retried (default true). */
  retryOnTimeout?: boolean;
  /** Per-fault-class attempt budgets, e.g. `{ "gpu-ecc": 8, "nan-loss": 0 }`. */
  retryBudgets?: Record<string, number>;
  /** Per-task subprocess timeout. */
  timeoutSecs?: number;
  /** Env vars: a `{NAME: value}` object, or entries that may name a secret. */
  env?: Record<string, string | number | boolean> | EnvEntry[];
  /** CPU/memory requests/limits for the task pod (Kubernetes executor). */
  resources?: Record<string, unknown>;
  /** ServiceAccount for the task pod — the IRSA seam. */
  serviceAccount?: string;
  /** Runner class (pool of scheduler replicas) that may claim this task. */
  runnerClass?: string;
  /** Named concurrency pool this task draws a slot from. */
  pool?: string;
  /** Dispatch priority among tasks ready at the same moment (0 = default). */
  priority?: number;
  /** Result memoization: reuse a prior success with the same resolved key. */
  cache?: CacheSpec;
  /** Loop operator: re-run until the condition on its own output holds. */
  repeat?: RepeatSpec;
  /** Datasets this task updates when it succeeds. */
  produces?: string[];
  /** Gang / co-scheduling: a member count, or the full `{ size }`. */
  gang?: number | { size: number };
  /** Trust envelope — the privileges this task runs under. */
  isolation?: Record<string, unknown>;
}

/** Spec-level properties, all optional. An unknown key throws. */
export interface DagOptions {
  /** Default runner class for tasks that don't set their own. */
  runnerClass?: string;
  /**
   * Where images built from a {@link Recipe} live, e.g.
   * `registry.example/ws-8f3a`. Empty means a daemon-local image, built and
   * used on the same socket and never pushed. When set, the build task is
   * pinned to this repository *and* told to push, so the image the tasks
   * reference is the image the build produces even against a pool configured
   * for somewhere else.
   */
  imageRepository?: string;
  /** Pool that has a builder on it (default `"build"`). */
  buildRunnerClass?: string;
  /** Ceiling on a build, in seconds (default 900). */
  buildTimeoutSecs?: number;
  /** Declared parameters and their defaults, substituted at submit. */
  parameters?: Record<string, string>;
  /** Labels for the console's workflow list (`listWorkflows({ tag })`). */
  tags?: string[];
  /** The named variable set + secrets this spec runs under. */
  environment?: string;
  /** The DRY block, merged into every task that doesn't override it. */
  taskDefaults?: Record<string, unknown>;
  /** The run's hard wall-clock budget. */
  runTimeoutSecs?: number;
  /** How many runs of this workflow may be in flight at once. */
  maxActiveRuns?: number;
  /** The task whose output becomes the run's result (must name a real task). */
  resultFrom?: string;
  /** A ceiling on what one run may expand to, e.g. `{ tasks: 500 }`. */
  budget?: { tasks?: number };
  /** The *soft* deadline that notifies rather than kills, e.g. `{ within: "2h" }`. */
  deadline?: { within: string };
  /** Per-workflow notification routing, overriding the instance defaults. */
  notify?: Record<string, unknown>;
  /** Datasets whose update triggers this workflow. */
  onDatasets?: string[];
  /** Whether `onDatasets` fires on `all` or `any` of them. */
  datasetsMode?: "all" | "any";
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

/** One task as emitted into a dagron spec (engine field names). */
export type TaskSpec = Record<string, unknown> & { name: string };

/** A reusable sub-DAG as it appears under a spec's `templates:` list. */
export interface TemplateSpec {
  name: string;
  parameters?: Record<string, string>;
  tasks: TaskSpec[];
}

/** A dagron spec as emitted by {@link Dag.toSpec}. */
export interface DagSpec {
  name: string;
  parameters?: Record<string, string>;
  tags?: string[];
  environment?: string;
  runner_class?: string;
  task_defaults?: Record<string, unknown>;
  run_timeout_secs?: number;
  max_active_runs?: number;
  result_from?: string;
  budget?: Record<string, unknown>;
  deadline?: Record<string, unknown>;
  notify?: Record<string, unknown>;
  on_datasets?: string[];
  datasets_mode?: string;
  templates?: TemplateSpec[];
  tasks: TaskSpec[];
}

/** The task-list half shared by {@link Dag} and {@link Template}. */
declare class TaskSet {
  readonly name: string;
  /** Add a task; returns its name (use it in a later task's `dependsOn`). */
  task(name: string, opts?: TaskOptions): string;
  /** Add a human approval gate (`type: approval`). */
  approval(
    name: string,
    opts?: Omit<TaskOptions, "taskType" | "approvalTimeoutSecs" | "approvalOnTimeout"> & {
      timeoutSecs?: number;
      onTimeout?: "approve" | "reject";
    },
  ): string;
  /** Add a deferrable sensor (`type: wait`) — it holds no worker slot. */
  sensor(
    name: string,
    opts?: Omit<TaskOptions, "taskType" | "wait"> & WaitSpec,
  ): string;
  /** Add a sub-workflow trigger (`type: workflow`). */
  trigger(
    name: string,
    workflow: string,
    opts?: Omit<TaskOptions, "taskType" | "workflow">,
  ): string;
}

/** A named, reusable sub-DAG declared on a {@link Dag} and called by a task. */
export declare class Template extends TaskSet {
  readonly parameters: Record<string, string>;
  constructor(name: string, opts?: { parameters?: Record<string, string> });
  /** The template as it appears under a spec's `templates:` list. */
  toSpec(): TemplateSpec;
}

export declare class Dag extends TaskSet {
  readonly runnerClass?: string;
  /** Where images built from a {@link Recipe} are pushed and pulled from. */
  readonly imageRepository: string;
  /** Pool that has a builder on it (default `"build"`). */
  readonly buildRunnerClass: string;
  /** Ceiling on a build, in seconds (default 900). */
  readonly buildTimeoutSecs: number;
  constructor(name: string, opts?: DagOptions);
  /** Declare a reusable sub-DAG and return it for filling with tasks. */
  template(name: string, opts?: { parameters?: Record<string, string> }): Template;
  /** Build the validated dagron spec (mirrors the server's `validate_graph`). */
  toSpec(): DagSpec;
  /** dagron spec as JSON (valid dagron input). */
  toJSON(): string;
  /** Submit the DAG as an ad-hoc run; resolves to the new run id. */
  submit(apiUrl: string, opts?: { token?: string; timeout?: number }): Promise<string>;
}

/** A DAG, a spec object, or a YAML/JSON spec string. */
export type SpecLike = Dag | Record<string, unknown> | string;

/** One Server-Sent Event from {@link Client.streamRun} / {@link Client.streamEvents}. */
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

/** What {@link Client.waitRun} resolves to — one synchronous invocation. */
export interface RunResult {
  run_id: string;
  status: string;
  /** `false` when the wait timed out; the caller simply calls again. */
  finished: boolean;
  /** The `result_from` task's output on success, else `null`. */
  result: string | null;
  /** Why the run failed, when this wait ended on a failure. */
  failure: RunFailure | null;
}

/** The spec a run was created from, as returned by {@link Client.getRunSpec}. */
export interface RunSpec {
  /** The stored, un-expanded DAG YAML. */
  yaml: string;
  name: string | null;
}

/** A minted access token — the only response that ever carries the plaintext. */
export interface CreatedToken {
  id: string;
  name: string;
  /** The cleartext head, e.g. `dgp_abc123…`. */
  prefix: string;
  /** Shown once. There is no way to retrieve it later. */
  token: string;
  expires_at: string | null;
}

/**
 * What `GET /api/git-repos` answers: the tracked repositories **plus** the
 * registry's own state. Deliberately not a bare array — a repo list alone
 * cannot say whether a worker is running to sync it, or whether a credential
 * can be stored at all.
 */
export interface GitRepoList {
  repos: Array<Record<string, unknown>>;
  /** Whether a `dagron-gitops` worker is alive to act on these repos. */
  worker_online: boolean;
  /** Whether encryption is configured, i.e. whether a credential can be stored. */
  credentials_configured: boolean;
}

/** A repository credential — write-only; never readable back. */
export interface GitRepoAuth {
  /** `https` or `ssh`. */
  kind?: string;
  username?: string;
  /** HTTPS deploy token / PAT. */
  token?: string;
  /** SSH private key (PEM). */
  sshPrivateKey?: string;
  /** Pinned host key for SSH remotes. */
  knownHosts?: string;
}

/** Error thrown by {@link Client}. */
export declare class DagronError extends Error {
  /** HTTP status code, or `0` for a transport-level failure. */
  readonly status: number;
  /** Raw response body, when available. */
  readonly body?: string;
  constructor(status: number, message: string, body?: string);
}

type Dict = Record<string, unknown>;

/** Typed client for the dagron-api gateway (`/api/...`). Zero deps, Node 18+. */
export declare class Client {
  baseUrl: string;
  token: string | null;
  timeout: number;
  constructor(baseUrl: string, opts?: { token?: string; timeout?: number });
  /** Build a client from `DAGRON_API_URL` / `DAGRON_TOKEN`. */
  static fromEnv(opts?: { timeout?: number }): Client;
  /** Drop the in-memory token so it does not outlive its use. */
  close(): void;

  // auth & identity
  login(email: string, password: string): Promise<string>;
  logout(): Promise<void>;
  me(): Promise<Dict>;
  createUser(email: string, password: string, name: string, groups?: string[]): Promise<Dict>;
  listUsers(): Promise<Dict[]>;
  listTokens(): Promise<Dict[]>;
  createToken(name: string, opts?: { expiresInDays?: number }): Promise<CreatedToken>;
  revokeToken(tokenId: string): Promise<void>;

  // runs
  submitRun(
    spec: SpecLike,
    opts?: { parameters?: Record<string, string>; idempotencyKey?: string },
  ): Promise<string>;
  listRuns(opts?: {
    status?: string;
    name?: string;
    trigger?: string;
    limit?: number;
    offset?: number;
  }): Promise<Dict[]>;
  /**
   * Pages through {@link Client.listRuns}. The `limit` and `offset` it drives
   * itself are refused; size the pages with `pageSize`.
   */
  iterRuns(opts?: {
    pageSize?: number;
    status?: string;
    name?: string;
    trigger?: string;
  }): AsyncGenerator<Dict>;
  getRun(runId: string): Promise<RunDetail>;
  getRunSpec(runId: string): Promise<RunSpec>;
  getRunGraph(runId: string): Promise<Dict>;
  getTaskLogs(runId: string, taskId: string, opts?: LogFilter & { offset?: number }): Promise<Dict>;
  getRunLogs(
    runId: string,
    opts?: LogFilter & { tasks?: string[]; statuses?: string[] },
  ): Promise<Dict>;
  cancelRun(runId: string): Promise<number>;
  rerunRun(runId: string, opts?: { params?: Dict }): Promise<Dict>;
  resubmitRun(runId: string): Promise<string>;
  retryTask(runId: string, taskId: string): Promise<boolean>;
  clearTask(runId: string, taskId: string): Promise<Dict>;
  approveTask(runId: string, taskId: string): Promise<Dict>;
  rejectTask(runId: string, taskId: string): Promise<Dict>;
  listApprovals(): Promise<Dict[]>;
  streamRun(runId: string, opts?: { timeout?: number }): AsyncGenerator<StreamEvent>;
  streamEvents(opts?: { timeout?: number }): AsyncGenerator<StreamEvent>;
  waitRun(runId: string, opts?: { timeoutSecs?: number }): Promise<RunResult>;
  waitForRun(
    runId: string,
    opts?: { pollInterval?: number; timeout?: number | null },
  ): Promise<RunDetail>;
  setTriage(runId: string, state: string, opts?: { note?: string }): Promise<Dict>;
  clearTriage(runId: string): Promise<Dict>;
  listArchivedRuns(opts?: { name?: string; limit?: number; offset?: number }): Promise<Dict[]>;
  getArchivedRun(runId: string): Promise<Dict>;
  archiveRun(runId: string): Promise<Dict>;

  // workflows
  listWorkflows(opts?: { tag?: string }): Promise<Dict[]>;
  getWorkflow(workflowId: string): Promise<Dict>;
  createWorkflow(spec: SpecLike, opts?: { name?: string; description?: string }): Promise<Dict>;
  updateWorkflow(
    workflowId: string,
    spec: SpecLike,
    opts?: { name?: string; description?: string },
  ): Promise<Dict>;
  deleteWorkflow(workflowId: string): Promise<void>;
  runWorkflow(workflowId: string, opts?: { parameters?: Record<string, string> }): Promise<Dict>;
  listWorkflowRuns(workflowId: string, opts?: { limit?: number; offset?: number }): Promise<Dict[]>;
  listWorkflowVersions(workflowId: string): Promise<Dict[]>;
  setWorkflowState(workflowId: string, state: "active" | "paused" | "retired"): Promise<Dict>;
  applyBundle(
    manifest: Uint8Array | string,
    signature: Uint8Array | string,
    files: Record<string, Uint8Array | string>,
  ): Promise<Dict>;
  workflowBadge(name: string): Promise<string>;
  syncWorkflowToGit(workflowId: string): Promise<Dict>;

  // schedules & backfills
  listSchedules(opts?: { workflowId?: string }): Promise<Dict[]>;
  createSchedule(
    workflowId: string,
    cronExpr: string,
    opts?: {
      enabled?: boolean;
      timezone?: string;
      whenExpr?: string;
      stopExpr?: string;
      catchup?: boolean;
      catchupWindowSecs?: number;
      catchupMaxRuns?: number;
    },
  ): Promise<Dict>;
  updateSchedule(
    scheduleId: string,
    opts?: {
      cronExpr?: string;
      enabled?: boolean;
      timezone?: string;
      whenExpr?: string;
      stopExpr?: string;
      catchup?: boolean;
      catchupWindowSecs?: number;
      catchupMaxRuns?: number;
    },
  ): Promise<Dict>;
  deleteSchedule(scheduleId: string): Promise<void>;
  backfillSchedule(
    scheduleId: string,
    from: string,
    to: string,
    opts?: { maxRuns?: number },
  ): Promise<Dict>;
  createBackfill(
    scheduleId: string,
    from: string,
    to: string,
    opts?: { maxRuns?: number },
  ): Promise<Dict>;
  listBackfills(opts?: { scheduleId?: string; limit?: number }): Promise<Dict[]>;
  getBackfill(backfillId: string): Promise<Dict>;
  cancelBackfill(backfillId: string): Promise<Dict>;

  // environments & settings
  listEnvironments(): Promise<Dict[]>;
  createEnvironment(
    name: string,
    opts?: { variables?: Record<string, string>; description?: string },
  ): Promise<Dict>;
  updateEnvironment(
    environmentId: string,
    opts?: { variables?: Record<string, string>; description?: string },
  ): Promise<Dict>;
  deleteEnvironment(environmentId: string): Promise<void>;
  setEnvironmentSecret(environmentId: string, name: string, value: string): Promise<void>;
  deleteEnvironmentSecret(environmentId: string, name: string): Promise<void>;
  getNotificationSettings(): Promise<Dict>;
  setNotificationSettings(settings: Dict): Promise<Dict>;
  testNotifications(settings: Dict): Promise<Dict>;
  getDeadLetterSettings(): Promise<Dict>;
  setDeadLetterSettings(maxAttempts: number): Promise<Dict>;

  // datasets & artifacts
  listDatasets(opts?: { limit?: number }): Promise<Dict[]>;
  listDatasetEvents(opts?: { uri?: string; limit?: number }): Promise<Dict[]>;
  putArtifact(
    runId: string,
    task: string,
    name: string,
    data: Uint8Array | string,
  ): Promise<string>;
  getArtifact(runId: string, task: string, name: string): Promise<Uint8Array>;
  artifactExists(runId: string, task: string, name: string): Promise<boolean>;
  syncArtifacts(): Promise<Dict>;

  // dead letters & GitOps
  listDeadLetters(opts?: { limit?: number }): Promise<Dict[]>;
  redriveDeadLetter(deadLetterId: string): Promise<Dict>;
  discardDeadLetter(deadLetterId: string): Promise<void>;
  listGitRepos(): Promise<GitRepoList>;
  connectGitRepo(
    url: string,
    opts?: { branch?: string; autoSync?: boolean; path?: string; auth?: GitRepoAuth },
  ): Promise<Dict>;
  setGitRepoAuth(repoId: string, opts?: GitRepoAuth): Promise<Dict>;
  clearGitRepoAuth(repoId: string): Promise<void>;
  syncGitRepo(repoId: string): Promise<Dict>;
  disconnectGitRepo(repoId: string): Promise<void>;

  // observability
  metrics(): Promise<Dict>;
  metricsTimeseries(opts?: { days?: number; name?: string }): Promise<Dict[]>;
  search(query: string, opts?: { limit?: number }): Promise<Dict>;
  health(): Promise<Dict>;
  healthz(): Promise<string>;
  readyz(): Promise<string>;
}
