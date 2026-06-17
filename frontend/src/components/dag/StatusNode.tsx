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
  [key: string]: unknown;
}

/// Argo-style node: name + status badge, colored by task status. A task that
/// chains another workflow renders with a sub-workflow badge instead.
export default function StatusNode({ data }: NodeProps) {
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
      <Handle type="target" position={Position.Top} />
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
      <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4 }}>
        <span style={{ color, fontSize: 11 }}>{isRef ? "sub-workflow" : statusLabel(d.status)}</span>
        {d.attempt > 0 && <span style={{ color: "var(--muted)", fontSize: 11 }}>try {d.attempt}</span>}
      </div>
      <Handle type="source" position={Position.Bottom} />
    </div>
  );
}
