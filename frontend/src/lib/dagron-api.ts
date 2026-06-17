// Typed dagron-api client — typed dagron-api client:
// base "/api" (Next rewrite → dagron-api), bearer from localStorage, generic fetch.

import type {
  DeadLetter,
  GitRepo,
  GraphResponse,
  MetricsResponse,
  RunDetail,
  RunSummary,
  Schedule,
  TaskLogs,
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
  limit?: number;
  offset?: number;
}): Promise<RunSummary[]> {
  const qs = new URLSearchParams();
  if (params?.status) qs.set("status", params.status);
  if (params?.limit != null) qs.set("limit", String(params.limit));
  if (params?.offset != null) qs.set("offset", String(params.offset));
  const q = qs.toString();
  return apiFetch(`/runs${q ? `?${q}` : ""}`);
}

export const getRun = (id: string): Promise<RunDetail> =>
  apiFetch(`/runs/${encodeURIComponent(id)}`);

export const getRunGraph = (id: string): Promise<GraphResponse> =>
  apiFetch(`/runs/${encodeURIComponent(id)}/graph`);

export const getTaskLogs = (id: string, tid: string): Promise<TaskLogs> =>
  apiFetch(`/runs/${encodeURIComponent(id)}/tasks/${encodeURIComponent(tid)}/logs`);

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

export const listDeadLetters = (limit = 100): Promise<DeadLetter[]> =>
  apiFetch(`/dead-letters?limit=${limit}`);

export const redriveDeadLetter = (
  id: string,
): Promise<{ run_id: string; redriven_from: string }> =>
  apiFetch(`/dead-letters/${encodeURIComponent(id)}/redrive`, { method: "POST" });

export const discardDeadLetter = (id: string): Promise<void> =>
  apiFetch(`/dead-letters/${encodeURIComponent(id)}`, { method: "DELETE" });

// ── workflows (first-class) ──────────────────────────────────────────────────
export const listWorkflows = (): Promise<WorkflowRow[]> => apiFetch(`/workflows`);

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
  enabled = true,
): Promise<Schedule> =>
  apiFetch(`/schedules`, { method: "POST", body: JSON.stringify({ workflow_id, cron_expr, enabled }) });

export const updateSchedule = (
  id: string,
  patch: { cron_expr?: string; enabled?: boolean },
): Promise<Schedule> =>
  apiFetch(`/schedules/${encodeURIComponent(id)}`, { method: "PUT", body: JSON.stringify(patch) });

export const deleteSchedule = (id: string): Promise<void> =>
  apiFetch(`/schedules/${encodeURIComponent(id)}`, { method: "DELETE" });

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
