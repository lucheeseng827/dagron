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
export function statusColor(status: string): string {
  switch (status) {
    case "succeeded":
      return "#2ea043"; // green
    case "failed":
      return "#f85149"; // red
    case "running":
      return "#2f81f7"; // blue
    case "ready":
      return "#d29922"; // amber
    case "pending":
      return "#6e7681"; // grey
    case "cancelled":
    case "skipped":
      return "#484f58"; // muted
    default:
      return "#6e7681";
  }
}

/// Human label for a status (title-cased).
export function statusLabel(status: TaskStatus): string {
  return status.charAt(0).toUpperCase() + status.slice(1);
}
