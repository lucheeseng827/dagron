"use client";

import { Handle, Position, type NodeProps } from "@xyflow/react";
import { statusColor } from "@/lib/adapter";
import { SENTINEL_H, SENTINEL_W, START_COLOR, type SentinelData } from "./sentinels";

/// Glyph for the End marker, chosen by the run's terminal outcome.
function endGlyph(status: string): string {
  switch (status) {
    case "succeeded":
      return "✓";
    case "failed":
      return "✕";
    case "cancelled":
      return "⊘";
    default:
      return "■"; // not yet reached
  }
}

/// A Start/End boundary marker. Stadium-shaped (a flowchart terminator) so it
/// reads as a flow boundary rather than a task card. Start carries only a source
/// handle, End only a target handle — they can't be wired by the user.
/// Handle positions come from the layout (top/bottom for vertical and
/// diagonal flows, left/right for horizontal).
export default function SentinelNode({ data, sourcePosition, targetPosition }: NodeProps) {
  const d = data as SentinelData;
  const isStart = d.kind === "start";
  // Start lights up green once the run is underway; neutral otherwise. End takes
  // its color from the run's outcome.
  const color = isStart ? (d.active ? statusColor("succeeded") : START_COLOR) : statusColor(d.status);
  return (
    <div
      title={isStart ? "Workflow start" : "Workflow end"}
      style={{
        width: SENTINEL_W,
        minHeight: SENTINEL_H,
        boxSizing: "border-box",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        gap: 8,
        background: "var(--card)",
        border: `1.5px solid ${color}`,
        borderRadius: 999, // stadium / terminator shape
        padding: "8px 14px",
        color: "var(--fg)",
        fontSize: 13,
        fontWeight: 600,
        letterSpacing: "0.02em",
        // Keep the label on one line so the rendered height stays == SENTINEL_H
        // (the height dagre laid out for), even for "Cancelled"/"Succeeded".
        whiteSpace: "nowrap",
      }}
    >
      {!isStart && <Handle type="target" position={targetPosition ?? Position.Top} isConnectable={false} />}
      <span aria-hidden style={{ color, fontSize: 12 }}>
        {isStart ? "▶" : endGlyph(d.status)}
      </span>
      <span>{d.label}</span>
      {isStart && <Handle type="source" position={sourcePosition ?? Position.Bottom} isConnectable={false} />}
    </div>
  );
}
