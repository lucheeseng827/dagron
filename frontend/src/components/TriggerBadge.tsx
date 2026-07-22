"use client";

import type { TriggerKind } from "@/types/dagron";

const STYLE: Record<TriggerKind, { label: string; color: string; bg: string }> = {
  manual: { label: "Manual", color: "var(--blue)", bg: "rgba(47,129,247,0.13)" },
  schedule: { label: "Schedule", color: "var(--green)", bg: "rgba(46,160,67,0.13)" },
  backfill: { label: "Backfill", color: "var(--amber)", bg: "rgba(210,153,34,0.13)" },
};

/// Pill answering "why did this run?" — manual submit, cron fire, or backfill.
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
