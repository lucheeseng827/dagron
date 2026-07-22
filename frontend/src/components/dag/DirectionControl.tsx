"use client";

// Segmented ↓/→/↘ control for the DAG layout direction, styled to match the
// React Flow chrome (Controls/MiniMap). Shared by the read-only run viewer
// (DagGraph) and the editable workflow canvas (EditableDag) so both offer the
// same re-layout control.

import { DIRECTIONS, type LayoutDirection } from "./direction";

export default function DirectionControl({
  dir,
  onChange,
}: {
  dir: LayoutDirection;
  onChange: (d: LayoutDirection) => void;
}) {
  return (
    <div
      style={{
        display: "flex",
        background: "var(--panel-2)",
        border: "1px solid var(--border)",
        borderRadius: 8,
        overflow: "hidden",
        boxShadow: "0 2px 10px rgba(0, 0, 0, 0.4)",
      }}
    >
      {DIRECTIONS.map((d, i) => {
        const active = d.id === dir;
        return (
          <button
            key={d.id}
            onClick={() => onChange(d.id)}
            title={d.label}
            aria-label={d.label}
            aria-pressed={active}
            style={{
              width: 30,
              height: 28,
              border: "none",
              borderLeft: i === 0 ? "none" : "1px solid var(--border)",
              background: active ? "#222a33" : "transparent",
              color: active ? "var(--fg)" : "var(--muted)",
              fontSize: 13,
              cursor: "pointer",
            }}
          >
            {d.glyph}
          </button>
        );
      })}
    </div>
  );
}
