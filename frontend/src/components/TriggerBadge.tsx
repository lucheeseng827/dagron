"use client";

import type { TriggerKind } from "@/types/dagron";

const STYLE: Record<TriggerKind, { label: string; color: string; bg: string }> = {
  manual: { label: "Manual", color: "var(--blue)", bg: "rgba(47,129,247,0.13)" },
  schedule: { label: "Schedule", color: "var(--green)", bg: "rgba(46,160,67,0.13)" },
  backfill: { label: "Backfill", color: "var(--amber)", bg: "rgba(210,153,34,0.13)" },
  ci: { label: "CI", color: "#3fb950", bg: "rgba(63,185,80,0.14)" },
  api: { label: "API", color: "var(--blue)", bg: "rgba(47,129,247,0.13)" },
  dataset: { label: "Dataset", color: "var(--green)", bg: "rgba(46,160,67,0.13)" },
  workflow: { label: "Workflow", color: "var(--muted)", bg: "rgba(139,148,158,0.16)" },
  source: { label: "Source", color: "var(--muted)", bg: "rgba(139,148,158,0.16)" },
};

/// Pill answering "why did this run?": a person's submit, a call on the
/// engine's API, a schedule's fire, a backfill, a dataset's update, a parent
/// run's task, a message of the engine's source, or a CI service running a
/// pipeline for an event.
export default function TriggerBadge({ kind }: { kind: TriggerKind }) {
  const s = STYLE[kind] ?? STYLE.manual;
  return (
    <span
      style={{
        fontSize: 10,
        fontWeight: 600,
        padding: "2px 8px",
        borderRadius: 999,
        color: s.color,
        background: s.bg,
        flexShrink: 0,
      }}
    >
      {s.label}
    </span>
  );
}
