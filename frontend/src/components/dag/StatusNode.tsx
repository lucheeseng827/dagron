"use client";

import { Handle, Position, type NodeProps } from "@xyflow/react";
import { statusColor, statusLabel } from "@/lib/adapter";
import type { TaskStatus } from "@/types/dagron";

export interface StatusNodeData {
  name: string;
  status: TaskStatus;
  attempt: number;
  /// Set when this task chains another saved workflow (a `workflow_ref` call).
  /// Rendered as a sub-workflow node rather than a plain command step.
  workflowRef?: string;
  /// Container image the task runs in (editor view); shown as a badge line.
  dockerImage?: string;
  /// Run view: dispatch/finish instants, for the duration line on the node.
  scheduledAt?: string | null;
  finishedAt?: string | null;
  [key: string]: unknown;
}

/// Wall-clock line for a run-view node: elapsed for a running task, total for a
/// finished one, nothing before dispatch. Null-safe on unparseable input.
function nodeDuration(d: StatusNodeData): string | null {
  if (!d.scheduledAt) return null;
  const start = new Date(d.scheduledAt).getTime();
  if (Number.isNaN(start)) return null;
  const end = d.finishedAt ? new Date(d.finishedAt).getTime() : Date.now();
  if (Number.isNaN(end) || end < start) return null;
  const secs = Math.round((end - start) / 1000);
  if (secs < 60) return `${secs}s`;
  const m = Math.floor(secs / 60);
  if (m < 60) return `${m}m ${secs % 60}s`;
  return `${Math.floor(m / 60)}h ${m % 60}m`;
}

/// DAG node: name + status badge, colored by task status. A task that
/// chains another workflow renders with a sub-workflow badge instead.
/// Handle positions come from the layout (top/bottom for vertical and
/// diagonal flows, left/right for horizontal).
export default function StatusNode({ data, sourcePosition, targetPosition }: NodeProps) {
  const d = data as StatusNodeData;
  const color = statusColor(d.status);
  const isRef = typeof d.workflowRef === "string" && d.workflowRef.length > 0;
  return (
    <div
      style={{
        width: 190,
        background: "var(--card)",
        border: `1px solid ${color}`,
        borderLeft: `4px solid ${color}`,
        // Dashed outline distinguishes a chained sub-workflow from a leaf step.
        borderStyle: isRef ? "dashed" : "solid",
        borderRadius: 6,
        padding: "8px 10px",
        color: "var(--fg)",
        fontSize: 13,
      }}
    >
      <Handle type="target" position={targetPosition ?? Position.Top} />
      <div style={{ fontWeight: 600, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
        {d.name}
      </div>
      {isRef && (
        <div
          style={{ color: "var(--muted)", fontSize: 11, marginTop: 2, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
          title={`Runs saved workflow: ${d.workflowRef}`}
        >
          ⧉ {d.workflowRef}
        </div>
      )}
      {!isRef && d.dockerImage && (
        <div
          style={{ color: "var(--muted)", fontSize: 11, marginTop: 2, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
          title={`Runs in container image: ${d.dockerImage}`}
        >
          ◳ {d.dockerImage}
        </div>
      )}
      <div style={{ display: "flex", justifyContent: "space-between", gap: 6, marginTop: 4 }}>
        <span style={{ color, fontSize: 11 }}>{isRef ? "sub-workflow" : statusLabel(d.status)}</span>
        <span style={{ color: "var(--muted)", fontSize: 11, whiteSpace: "nowrap" }}>
          {nodeDuration(d) ?? ""}
          {d.attempt > 1 ? ` · try ${d.attempt}` : ""}
        </span>
      </div>
      <Handle type="source" position={sourcePosition ?? Position.Bottom} />
    </div>
  );
}
