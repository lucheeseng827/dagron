"use client";

import { useMemo } from "react";
import { statusColor, statusLabel } from "@/lib/adapter";
import { jobRows, jobStatus, type JobRow, type JobTask } from "@/lib/ci-jobs";
import type { TaskStatus } from "@/types/dagron";

/// A run's jobs as a grid: a job a row, and a job that ran as several
/// instances (a matrix, a fan-out) a row of them, each a cell that opens its
/// task. The graph shows how jobs depend on one another; this shows, of a
/// job with twenty instances, which of them failed.
export default function JobsGrid({
  tasks,
  selected,
  onTaskClick,
}: {
  tasks: JobTask[];
  selected: string | null;
  onTaskClick: (id: string) => void;
}) {
  const rows = useMemo(() => jobRows(tasks), [tasks]);
  if (rows.length === 0) {
    return (
      <p className="dy-empty" style={{ padding: 24 }}>
        This run has no tasks yet.
      </p>
    );
  }
  return (
    <div style={{ height: "100%", overflow: "auto", padding: "18px 24px" }}>
      <div className="dy-card" style={{ padding: 0, overflow: "hidden" }}>
        {rows.map((row) => (
          <Row key={row.job} row={row} selected={selected} onTaskClick={onTaskClick} />
        ))}
      </div>
    </div>
  );
}

function Row({
  row,
  selected,
  onTaskClick,
}: {
  row: JobRow;
  selected: string | null;
  onTaskClick: (id: string) => void;
}) {
  const status = jobStatus(row);
  const color = statusColor(status);
  // A job that is one task opens that task; one with instances opens the
  // task that bears its name, where there is one.
  const own = row.cells.length === 1 && row.cells[0].label === "" ? row.cells[0] : row.head;
  const failed = row.cells.filter((c) => c.status === "failed").length;
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "minmax(160px, 280px) 1fr",
        gap: 14,
        alignItems: "start",
        padding: "10px 18px",
        borderBottom: "1px solid var(--border)",
      }}
    >
      <button
        onClick={() => own && onTaskClick(own.id)}
        disabled={!own}
        title={own ? "Open this job's task" : undefined}
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          background: "none",
          border: "none",
          padding: 0,
          color: "var(--fg)",
          font: "inherit",
          textAlign: "left",
          cursor: own ? "pointer" : "default",
          minWidth: 0,
        }}
      >
        <span className="dy-dot" style={{ width: 9, height: 9, background: color, flex: "none" }} title={status} />
        <span
          style={{
            fontWeight: 600,
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
            outline: own && own.id === selected ? "1px solid var(--accent)" : undefined,
            outlineOffset: 3,
            borderRadius: 3,
          }}
        >
          {row.job}
        </span>
        {row.cells.length > 1 && (
          <span style={{ color: "var(--dim)", fontSize: 12, whiteSpace: "nowrap" }}>
            {row.cells.length} instances{failed > 0 ? `, ${failed} failed` : ""}
          </span>
        )}
      </button>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
        {row.cells.length === 1 && row.cells[0].label === "" ? (
          <span style={{ color, fontSize: 12.5 }}>{statusLabel(status as TaskStatus)}</span>
        ) : (
          row.cells.map((cell) => {
            const c = statusColor(cell.status);
            return (
              <button
                key={cell.id}
                onClick={() => onTaskClick(cell.id)}
                className="dy-pill mono"
                title={`${row.job}.${cell.label}: ${statusLabel(cell.status as TaskStatus)}`}
                style={{
                  cursor: "pointer",
                  display: "inline-flex",
                  alignItems: "center",
                  gap: 6,
                  borderColor: cell.id === selected ? "var(--accent)" : undefined,
                }}
              >
                <span className="dy-dot dy-dot-sm" style={{ background: c }} />
                {cell.label}
              </button>
            );
          })
        )}
      </div>
    </div>
  );
}
