// Typed dagron-api client — typed dagron-api client:
// base "/api" (Next rewrite → dagron-api), bearer from localStorage, generic fetch.

import type {
  ArchivedRunDoc,
  ArchivedRunSummary,
  AuditEntry,
  BackfillView,
  DayBucket,
  DeadLetter,
  EnvironmentView,
  GitRepo,
  GraphResponse,
  HealthResponse,
  MetricsResponse,
  NotificationSettings,
  NotifyTestResult,
  PendingApproval,
  RunDetail,
  SearchResponse,
  RunSummary,
  Schedule,
  ScheduleOptions,
  TaskLogs,
  TaskResolution,
  UserView,
  Workflow,
  WorkflowRow,
} from "@/types/dagron";

const BASE = "/api";

// The session lives in an HttpOnly `dagron_session` cookie set by dagron-api on
// login, so it is never readable by JS (XSS can't exfiltrate it). Every call
// goes same-origin through the Next `/api` rewrite, so the browser attaches the
// cookie automatically; `credentials: "same-origin"` makes that explicit.

/// Self-contained login: exchange email + password for a session cookie.
/// Throws on bad credentials (401).
export async function login(email: string, password: string): Promise<void> {
  const res = await fetch(`${BASE}/login`, {
    method: "POST",
    credentials: "same-origin",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email, password }),
  });
  if (!res.ok) {
    if (res.status === 401) throw new Error("Invalid email or password.");
    throw new Error(`${res.status}: ${(await res.text().catch(() => "")) || res.statusText}`);
  }
}

/// Clear the session server-side. The cookie is HttpOnly, so JS can't drop it —
/// the server expires it via Set-Cookie.
export async function logout(): Promise<void> {
  await fetch(`${BASE}/logout`, { method: "POST", credentials: "same-origin" }).catch(() => {});
}

/// Validated session claims, as returned by `GET /api/me`.
export interface Me {
  sub: string;
  email: string;
  name: string;
  groups: string[];
  exp: number;
}

/// Fetch the current user's claims (for the sidebar identity chip).
export const getMe = (): Promise<Me> => apiFetch(`/me`);

export type SessionState = "authed" | "unauthed" | "error";

/// Classify the current session by probing `/api/me` (the HttpOnly cookie can't
/// be inspected from JS). Only a 401 means "not signed in"; network/proxy
/// failures and 5xx map to "error" so a transient backend blip doesn't bounce an
/// already-authenticated user to the sign-in form.
export async function checkSession(): Promise<SessionState> {
  try {
    const res = await fetch(`${BASE}/me`, { credentials: "same-origin" });
    if (res.ok) return "authed";
    if (res.status === 401) return "unauthed";
    return "error";
  } catch {
    return "error";
  }
}

function defaultHeaders(): Record<string, string> {
  return { "Content-Type": "application/json" };
}

async function apiFetch<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    credentials: "same-origin",
    ...options,
    headers: { ...defaultHeaders(), ...(options?.headers as Record<string, string>) },
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`${res.status}: ${body || res.statusText}`);
  }
  if (res.status === 204) return undefined as T;
  return res.json() as Promise<T>;
}

// ── reads ──────────────────────────────────────────────────────────────────
export function listRuns(params?: {
  status?: string;
  /// Workflow (definition) name, exact match.
  name?: string;
  /// Trigger kind: manual | schedule | backfill.
  trigger?: string;
  limit?: number;
  offset?: number;
}): Promise<RunSummary[]> {
  const qs = new URLSearchParams();
  if (params?.status) qs.set("status", params.status);
  if (params?.name) qs.set("name", params.name);
  if (params?.trigger) qs.set("trigger", params.trigger);
  if (params?.limit != null) qs.set("limit", String(params.limit));
  if (params?.offset != null) qs.set("offset", String(params.offset));
  const q = qs.toString();
  return apiFetch(`/runs${q ? `?${q}` : ""}`);
}

export const getRun = (id: string): Promise<RunDetail> =>
  apiFetch(`/runs/${encodeURIComponent(id)}`);

export const getRunGraph = (id: string): Promise<GraphResponse> =>
  apiFetch(`/runs/${encodeURIComponent(id)}/graph`);

/// The stored DAG spec (YAML) this run was created from — used to pre-fill the
/// "re-run with changes" editor.
export const getRunSpec = (id: string): Promise<{ yaml: string; name: string | null }> =>
  apiFetch(`/runs/${encodeURIComponent(id)}/spec`);

/// Task logs. Pass `offset` (a prior response's `next_offset`) to tail: only
/// output past that char offset comes back, until `eof`.
export const getTaskLogs = (id: string, tid: string, offset?: number): Promise<TaskLogs> =>
  apiFetch(
    `/runs/${encodeURIComponent(id)}/tasks/${encodeURIComponent(tid)}/logs${
      offset != null ? `?offset=${offset}` : ""
    }`,
  );

// ── control ──────────────────────────────────────────────────────────────────
export const submitRun = (yaml: string): Promise<{ run_id: string }> =>
  apiFetch(`/runs`, { method: "POST", body: JSON.stringify({ yaml }) });

export const cancelRun = (id: string): Promise<{ cancelled: number }> =>
  apiFetch(`/runs/${encodeURIComponent(id)}/cancel`, { method: "POST" });

/// Re-trigger: start a fresh run from this run's stored definition (any state).
export const resubmitRun = (id: string): Promise<{ run_id: string }> =>
  apiFetch(`/runs/${encodeURIComponent(id)}/resubmit`, { method: "POST" });

export const retryTask = (id: string, tid: string): Promise<{ retried: boolean }> =>
  apiFetch(`/runs/${encodeURIComponent(id)}/tasks/${encodeURIComponent(tid)}/retry`, {
    method: "POST",
  });

/// Clear + downstream: reset a completed task (even a succeeded one) and every
/// terminal task depending on it, re-arming the run to re-run that sub-DAG.
export const clearTask = (
  id: string,
  tid: string,
): Promise<{ run_id: string; task_id: string; cleared: number }> =>
  apiFetch(`/runs/${encodeURIComponent(id)}/tasks/${encodeURIComponent(tid)}/clear`, {
    method: "POST",
  });

// Human approval gate (#19): resolve a task parked in `awaiting_approval`.
// Approve lets the DAG continue; reject fails the task (and, per trigger rules,
// its dependents). Mirrors POST /runs/:id/tasks/:tid/{approve,reject}.
export const approveTask = (
  id: string,
  tid: string,
): Promise<{ run_id: string; task_id: string; resolution: TaskResolution }> =>
  apiFetch(`/runs/${encodeURIComponent(id)}/tasks/${encodeURIComponent(tid)}/approve`, {
    method: "POST",
  });

/// Reject a gate: the task fails and `all_success` dependents skip.
export const rejectTask = (
  id: string,
  tid: string,
): Promise<{ run_id: string; task_id: string; resolution: TaskResolution }> =>
  apiFetch(`/runs/${encodeURIComponent(id)}/tasks/${encodeURIComponent(tid)}/reject`, {
    method: "POST",
  });

// Cascade rerun a failed/cancelled run from its failure frontier: only the
// failed/cancelled tasks (and what they blocked) re-run; succeeded tasks are
// left intact. Optional `params` is deep-merged into each reset task's input.
export const rerunRun = (
  id: string,
  params?: Record<string, unknown>,
): Promise<{ run_id: string; rerun: number }> =>
  apiFetch(`/runs/${encodeURIComponent(id)}/rerun`, {
    method: "POST",
    body: JSON.stringify(params ? { params } : {}),
  });

// ── observability + dead-letters ─────────────────────────────────────────────
export const getMetrics = (): Promise<MetricsResponse> => apiFetch(`/metrics`);

/// Per-day run outcomes + duration stats for the Metrics charts and the
/// workflow detail trend. Optional `name` scopes to one workflow.
export const getMetricsTimeseries = (days = 14, name?: string): Promise<DayBucket[]> =>
  apiFetch(`/metrics/timeseries?days=${days}${name ? `&name=${encodeURIComponent(name)}` : ""}`);

/// Rich health for the sidebar widget: DB, scheduler leadership, attention counters.
export const getHealth = (): Promise<HealthResponse> => apiFetch(`/health`);

/// Every approval gate parked in `awaiting_approval`, oldest first.
export const listApprovals = (): Promise<PendingApproval[]> => apiFetch(`/approvals`);

export const listDeadLetters = (limit = 100): Promise<DeadLetter[]> =>
  apiFetch(`/dead-letters?limit=${limit}`);

export const redriveDeadLetter = (
  id: string,
): Promise<{ run_id: string; redriven_from: string }> =>
  apiFetch(`/dead-letters/${encodeURIComponent(id)}/redrive`, { method: "POST" });

export const discardDeadLetter = (id: string): Promise<void> =>
  apiFetch(`/dead-letters/${encodeURIComponent(id)}`, { method: "DELETE" });

// ── archived runs (hot/cold split) ───────────────────────────────────────────
export const listArchivedRuns = (params?: {
  name?: string;
  limit?: number;
  offset?: number;
}): Promise<ArchivedRunSummary[]> => {
  const qs = new URLSearchParams();
  if (params?.name) qs.set("name", params.name);
  if (params?.limit != null) qs.set("limit", String(params.limit));
  if (params?.offset != null) qs.set("offset", String(params.offset));
  const q = qs.toString();
  return apiFetch(`/archive/runs${q ? `?${q}` : ""}`);
};

/// Full archive document for one run. 404 = never archived; 410 = compacted to
/// Parquet (the error body carries the part-file path).
export const getArchivedRun = (id: string): Promise<ArchivedRunDoc> =>
  apiFetch(`/archive/runs/${encodeURIComponent(id)}`);

// ── workflows (first-class) ──────────────────────────────────────────────────
export const listWorkflows = (): Promise<WorkflowRow[]> => apiFetch(`/workflows`);

/// One workflow's run history (matched by definition name), newest first.
export const listWorkflowRuns = (
  id: string,
  limit = 50,
  offset = 0,
): Promise<RunSummary[]> =>
  apiFetch(`/workflows/${encodeURIComponent(id)}/runs?limit=${limit}&offset=${offset}`);

export const getWorkflow = (id: string): Promise<Workflow> =>
  apiFetch(`/workflows/${encodeURIComponent(id)}`);

export const createWorkflow = (
  spec: string,
  name?: string,
  description?: string,
): Promise<Workflow> =>
  apiFetch(`/workflows`, { method: "POST", body: JSON.stringify({ spec, name, description }) });

export const updateWorkflow = (
  id: string,
  spec: string,
  name?: string,
  description?: string,
): Promise<Workflow> =>
  apiFetch(`/workflows/${encodeURIComponent(id)}`, {
    method: "PUT",
    body: JSON.stringify({ spec, name, description }),
  });

export const deleteWorkflow = (id: string): Promise<void> =>
  apiFetch(`/workflows/${encodeURIComponent(id)}`, { method: "DELETE" });

export const runWorkflow = (id: string): Promise<{ run_id: string; workflow_id: string }> =>
  apiFetch(`/workflows/${encodeURIComponent(id)}/run`, { method: "POST" });

/// Open a pull request that commits this workflow's raw DAG spec to the
/// configured GitOps repo. Returns the PR URL and branch.
export const syncWorkflowToGit = (
  id: string,
): Promise<{ pr_url: string; branch: string; path: string }> =>
  apiFetch(`/workflows/${encodeURIComponent(id)}/sync-to-git`, { method: "POST" });

// ── schedules ────────────────────────────────────────────────────────────────
export const listSchedules = (workflowId?: string): Promise<Schedule[]> =>
  apiFetch(`/schedules${workflowId ? `?workflow_id=${encodeURIComponent(workflowId)}` : ""}`);

export const createSchedule = (
  workflow_id: string,
  cron_expr: string,
  options?: ScheduleOptions,
): Promise<Schedule> =>
  apiFetch(`/schedules`, {
    method: "POST",
    body: JSON.stringify({ workflow_id, cron_expr, enabled: true, ...options }),
  });

export const updateSchedule = (
  id: string,
  patch: { cron_expr?: string } & ScheduleOptions,
): Promise<Schedule> =>
  apiFetch(`/schedules/${encodeURIComponent(id)}`, { method: "PUT", body: JSON.stringify(patch) });

export const deleteSchedule = (id: string): Promise<void> =>
  apiFetch(`/schedules/${encodeURIComponent(id)}`, { method: "DELETE" });

// ── backfills (paced jobs) ───────────────────────────────────────────────────
export const listBackfills = (scheduleId?: string, limit = 100): Promise<BackfillView[]> =>
  apiFetch(
    `/backfills?limit=${limit}${scheduleId ? `&schedule_id=${encodeURIComponent(scheduleId)}` : ""}`,
  );

export const createBackfill = (
  schedule_id: string,
  from: string,
  to: string,
  max_runs?: number,
): Promise<BackfillView> =>
  apiFetch(`/backfills`, {
    method: "POST",
    body: JSON.stringify({ schedule_id, from, to, ...(max_runs ? { max_runs } : {}) }),
  });

export const getBackfill = (id: string): Promise<BackfillView> =>
  apiFetch(`/backfills/${encodeURIComponent(id)}`);

export const cancelBackfill = (id: string): Promise<{ id: string; cancelled: boolean }> =>
  apiFetch(`/backfills/${encodeURIComponent(id)}/cancel`, { method: "POST" });

// ── admin: users + audit trail ───────────────────────────────────────────────
export const listUsers = (): Promise<UserView[]> => apiFetch(`/users`);

export const createUser = (
  email: string,
  name: string,
  password: string,
  groups: string[],
): Promise<{ id: string }> =>
  apiFetch(`/users`, { method: "POST", body: JSON.stringify({ email, name, password, groups }) });

export const listAudit = (limit = 100, offset = 0): Promise<AuditEntry[]> =>
  apiFetch(`/audit?limit=${limit}&offset=${offset}`);

// ── instance settings: notification defaults ─────────────────────────────────
export const getNotificationSettings = (): Promise<NotificationSettings> =>
  apiFetch(`/settings/notifications`);

export const saveNotificationSettings = (
  s: NotificationSettings,
): Promise<NotificationSettings> =>
  apiFetch(`/settings/notifications`, { method: "PUT", body: JSON.stringify(s) });

/// Send a test message to each enabled target in `s` (unsaved edits included).
export const testNotificationSettings = (s: NotificationSettings): Promise<NotifyTestResult> =>
  apiFetch(`/settings/notifications/test`, { method: "POST", body: JSON.stringify(s) });

// ── environments (variable sets + write-only secrets) ────────────────────────
export const listEnvironments = (): Promise<EnvironmentView[]> => apiFetch(`/environments`);

export const createEnvironment = (
  name: string,
  description?: string,
  variables?: Record<string, string>,
): Promise<EnvironmentView> =>
  apiFetch(`/environments`, { method: "POST", body: JSON.stringify({ name, description, variables }) });

export const updateEnvironment = (
  id: string,
  patch: { description?: string; variables?: Record<string, string> },
): Promise<EnvironmentView> =>
  apiFetch(`/environments/${encodeURIComponent(id)}`, { method: "PUT", body: JSON.stringify(patch) });

export const deleteEnvironment = (id: string): Promise<void> =>
  apiFetch(`/environments/${encodeURIComponent(id)}`, { method: "DELETE" });

/// Set (upsert) a secret value — write-only: never readable back.
export const putEnvironmentSecret = (id: string, name: string, value: string): Promise<void> =>
  apiFetch(`/environments/${encodeURIComponent(id)}/secrets/${encodeURIComponent(name)}`, {
    method: "PUT",
    body: JSON.stringify({ value }),
  });

export const deleteEnvironmentSecret = (id: string, name: string): Promise<void> =>
  apiFetch(`/environments/${encodeURIComponent(id)}/secrets/${encodeURIComponent(name)}`, {
    method: "DELETE",
  });

// ── global search (⌘K palette) ───────────────────────────────────────────────
export const globalSearch = (q: string, limit = 8): Promise<SearchResponse> =>
  apiFetch(`/search?q=${encodeURIComponent(q)}&limit=${limit}`);

// ── GitOps repository registry ───────────────────────────────────────────────
export const listGitRepos = (): Promise<GitRepo[]> => apiFetch(`/git-repos`);

export const connectGitRepo = (
  url: string,
  branch?: string,
  auto_sync = false,
): Promise<GitRepo> =>
  apiFetch(`/git-repos`, { method: "POST", body: JSON.stringify({ url, branch, auto_sync }) });

export const syncGitRepo = (id: string): Promise<GitRepo> =>
  apiFetch(`/git-repos/${encodeURIComponent(id)}/sync`, { method: "POST" });

export const disconnectGitRepo = (id: string): Promise<void> =>
  apiFetch(`/git-repos/${encodeURIComponent(id)}`, { method: "DELETE" });
