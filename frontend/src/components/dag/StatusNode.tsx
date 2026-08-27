"use client";

import { Handle, Position, type NodeProps } from "@xyflow/react";
import { statusColor, statusLabel, type WaitingOn } from "@/lib/adapter";
import { fromNow } from "@/lib/time";
import type { TaskStatus } from "@/types/dagron";
import {
  NODE_MID_GAP,
  NODE_PAD_Y,
  NODE_ROW_GAP,
  NODE_ROW_LH,
  NODE_TITLE_LH,
} from "./layout";

/// Every row is `flexShrink: 0` with an explicit `lineHeight`. The node is a
/// fixed-height flex column, so a shrinkable row absorbs any shortfall between
/// the declared height and what the rows need — and on the name row, which
/// clips its overflow, that shows up as letters cut off mid-glyph rather than
/// as anything that looks like a layout bug. Holding the line boxes fixed means
/// a future extra row overflows visibly instead of quietly cropping text.
const ROW = { flexShrink: 0, lineHeight: `${NODE_ROW_LH}px` } as const;
/// Shared by the two subtitle rows (sub-workflow / template / image).
const SUBTITLE = {
  ...ROW,
  color: "var(--muted)",
  fontSize: 11,
  marginTop: NODE_MID_GAP,
  overflow: "hidden",
  textOverflow: "ellipsis",
  whiteSpace: "nowrap",
} as const;

export interface StatusNodeData {
  name: string;
  status: TaskStatus;
  attempt: number;
  /// Set when this task chains another saved workflow (a `workflow_ref` call).
  /// Rendered as a sub-workflow node rather than a plain command step.
  workflowRef?: string;
  /// Set when this task calls a `templates:` sub-DAG (a `template:` call).
  /// Rendered like a chained workflow — it is one node standing for several.
  templateRef?: string;
  /// How many tasks the called template declares, for the node's subtitle.
  templateTasks?: number;
  /// Container image the task runs in (editor view); shown as a badge line.
  dockerImage?: string;
  /// Run view: dispatch/finish instants, for the duration line on the node.
  scheduledAt?: string | null;
  finishedAt?: string | null;
  /// Run view: why this node is parked (wait sensor / sub-workflow trigger), or
  /// null when it isn't. A parked node is `running`, so without this its clock
  /// just keeps counting and it reads as hung.
  waiting?: WaitingOn | null;
  /// Run view: resolved from the memoization cache rather than executed.
  cacheHit?: boolean;
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

/// What a parked node shows where its duration would go. A node is ~190px wide,
/// so a dataset URI or endpoint never fits — the kind of wait is what's useful
/// at a glance, with the full value on the status badge's tooltip.
function shortDetail(w: WaitingOn): string {
  if (w.runLink) return "sub-workflow";
  if (w.label === "Waiting until") return fromNow(w.detail);
  if (w.label === "Waiting on dataset") return "dataset";
  return "endpoint";
}

/// DAG node: name + status badge, colored by task status. A task that
/// chains another workflow renders with a sub-workflow badge instead.
/// Handle positions come from the layout (top/bottom for vertical and
/// diagonal flows, left/right for horizontal).
export default function StatusNode({ data, sourcePosition, targetPosition }: NodeProps) {
  const d = data as StatusNodeData;
  const color = statusColor(d.status);
  const isWorkflowRef = typeof d.workflowRef === "string" && d.workflowRef.length > 0;
  const isTemplate = typeof d.templateRef === "string" && d.templateRef.length > 0;
  // Both kinds are *calls*: one node standing for a sub-DAG the canvas doesn't
  // draw, so they share the dashed outline and the sub-workflow subtitle.
  const isRef = isWorkflowRef || isTemplate;
  return (
    <div
      style={{
        width: "100%",
        // Fill the wrapper React Flow sized from the declared height, so the
        // border is the node's real edge and the bottom handle lands on it
        // rather than partway up the content.
        height: "100%",
        boxSizing: "border-box",
        display: "flex",
        flexDirection: "column",
        justifyContent: "center",
        background: "var(--card)",
        border: `1px solid ${color}`,
        borderLeft: `4px solid ${color}`,
        // Dashed outline distinguishes a chained sub-workflow from a leaf step.
        borderStyle: isRef ? "dashed" : "solid",
        borderRadius: 6,
        padding: `${NODE_PAD_Y}px 10px`,
        color: "var(--fg)",
        fontSize: 13,
      }}
    >
      <Handle type="target" position={targetPosition ?? Position.Top} />
      <div
        style={{
          flexShrink: 0,
          lineHeight: `${NODE_TITLE_LH}px`,
          fontWeight: 600,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}
        title={d.name}
      >
        {d.name}
      </div>
      {isWorkflowRef && (
        <div style={SUBTITLE} title={`Runs saved workflow: ${d.workflowRef}`}>
          ⧉ {d.workflowRef}
        </div>
      )}
      {isTemplate && (
        <div style={SUBTITLE} title={`Calls template: ${d.templateRef}`}>
          ⧉ {d.templateRef}
          {d.templateTasks ? ` · ${d.templateTasks} tasks` : ""}
        </div>
      )}
      {!isRef && d.dockerImage && (
        <div style={SUBTITLE} title={`Runs in container image: ${d.dockerImage}`}>
          ◳ {d.dockerImage}
        </div>
      )}
      <div
        style={{
          ...ROW,
          display: "flex",
          justifyContent: "space-between",
          gap: 6,
          marginTop: NODE_ROW_GAP,
        }}
      >
        <span
          style={{ color: d.waiting ? "var(--blue)" : color, fontSize: 11 }}
          title={d.waiting ? `${d.waiting.label}: ${d.waiting.detail}` : undefined}
        >
          {isTemplate
            ? "sub-DAG"
            : isWorkflowRef
              ? "sub-workflow"
              : d.waiting
                ? "⏸ waiting"
                : statusLabel(d.status)}
        </span>
        <span style={{ color: "var(--muted)", fontSize: 11, whiteSpace: "nowrap" }}>
          {/* A parked task's elapsed clock only measures how long it has been
              waiting, which reads as "stuck for 4h" — the reason belongs there
              instead. A cache hit has no runtime to report at all. */}
          {d.waiting ? shortDetail(d.waiting) : d.cacheHit ? "⚡ cached" : (nodeDuration(d) ?? "")}
          {d.attempt > 1 ? ` · try ${d.attempt}` : ""}
        </span>
      </div>
      <Handle type="source" position={sourcePosition ?? Position.Bottom} />
    </div>
  );
}
