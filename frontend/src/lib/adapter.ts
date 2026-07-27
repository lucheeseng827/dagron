// Bridge dagron shapes ↔ dagron view types, and the shared status palette.
//
// dagron task_runs lack started_at / sort_order / message (they have output,
// attempt, scheduled_at, finished_at), so we derive them here.

import type { TaskRow, TaskStatus } from "@/types/dagron";

/// dagron Step shape consumed by StepTimeline-like components.
export interface Step {
  id: string;
  name: string;
  status: TaskStatus;
  started_at?: string;
  finished_at?: string;
  sort_order: number;
  message?: string;
}

/// Map a dagron TaskRow + its array index → a Step.
/// sort_order derives from array order (caller passes topologically/sorted rows);
/// output → message; scheduled_at → started_at fallback.
export function toStep(task: TaskRow, index: number): Step {
  return {
    id: task.id,
    name: task.name,
    status: task.status,
    started_at: task.scheduled_at ?? undefined,
    finished_at: task.finished_at ?? undefined,
    sort_order: index,
    message: task.output ?? undefined,
  };
}

/// CSS color for each task/run status — used by the timeline, DAG nodes, and
/// status dots. Accepts a plain string (any status the API may return) and falls
/// back to grey for unknown values, so callers don't need unsafe `as` casts.
/// Status hues come from the CSS tokens in globals.css rather than being
/// duplicated as hex here — the two copies had already drifted. The MAPPING is
/// deliberately unchanged: operators read these colours daily, and rotating a
/// familiar vocabulary buys nothing but re-learning.
export function statusColor(status: string): string {
  switch (status) {
    case "succeeded":
      return "var(--green)";
    case "failed":
      return "var(--red)";
    case "running":
      return "var(--blue)";
    case "ready":
      return "var(--amber)";
    case "pending":
      return "var(--dim)";
    case "cancelled":
    case "skipped":
      return "var(--muted-status)";
    case "awaiting_approval":
      return "var(--purple)"; // a human gate, distinct from machine states
    default:
      return "var(--dim)";
  }
}

/// Human label for a status (title-cased; underscores become spaces).
export function statusLabel(status: TaskStatus): string {
  if (status === "awaiting_approval") return "Awaiting approval";
  return status.charAt(0).toUpperCase() + status.slice(1);
}

/// Why a task is parked, or `null` if it isn't.
///
/// Every park form — the time / HTTP / dataset wait sensors and the
/// sub-workflow trigger — deliberately leaves the row `running` with no lease
/// (no extra status, no CHECK rebuild server-side). The cost is that a parked
/// task and a hung one are indistinguishable in a status column, so the API
/// carries the reason and this turns it into something readable. `runLink` is
/// set only for a sub-workflow trigger: the child run is navigable.
export interface WaitingOn {
  /// Short badge text, e.g. "Waiting on dataset".
  label: string;
  /// The specific thing waited on (URI, endpoint, wake time, child run id).
  detail: string;
  /// Run id to link to, when the thing waited on is a run.
  runLink?: string;
}

export function waitingOn(
  task: Pick<TaskRow, "status" | "wake_at" | "wait_url" | "wait_dataset" | "sub_run_id"> | null | undefined,
): WaitingOn | null {
  // Only a live row is parked: a resolved sensor keeps its wake_at/wait_url for
  // the record, and showing "waiting" on a succeeded task would be a lie.
  if (!task || task.status !== "running") return null;
  if (task.sub_run_id) {
    return { label: "Waiting on sub-workflow", detail: task.sub_run_id, runLink: task.sub_run_id };
  }
  if (task.wait_dataset) return { label: "Waiting on dataset", detail: task.wait_dataset };
  if (task.wait_url) return { label: "Waiting on endpoint", detail: task.wait_url };
  if (task.wake_at) return { label: "Waiting until", detail: task.wake_at };
  return null;
}
