"use client";

// Gantt-style run timeline: one row per task, bars positioned on the run's
// wall-clock window — answers "where did the time go, what ran in parallel?"
// Pure divs (no chart lib); identity is carried by the row label + tooltip,
// never color alone (status color is reinforcement, matching the DAG view).

import { useMemo } from "react";
import { statusColor, statusLabel } from "@/lib/adapter";
import type { GraphResponse, TaskStatus } from "@/types/dagron";

export interface RunTimelineProps {
  graph: GraphResponse;
  runCreatedAt: string;
  runFinishedAt: string | null;
  onTaskClick?: (taskId: string) => void;
  selected?: string | null;
}

interface Row {
  id: string;
  name: string;
  status: TaskStatus;
  attempt: number;
  start: number | null;
  end: number | null;
}

const LABEL_W = 190;

function fmtClock(t: number): string {
  const d = new Date(t);
  return `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}:${String(d.getSeconds()).padStart(2, "0")}`;
}

function fmtDur(ms: number): string {
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  return `${Math.floor(m / 60)}h ${m % 60}m`;
}

export default function RunTimeline({
  graph,
  runCreatedAt,
  runFinishedAt,
  onTaskClick,
  selected,
}: RunTimelineProps) {
  const { rows, t0, t1 } = useMemo(() => {
    const now = Date.now();
    const rows: Row[] = graph.nodes.map((n) => {
      const start = n.scheduled_at ? new Date(n.scheduled_at).getTime() : null;
      const running = n.status === "running" || n.status === "awaiting_approval";
      const end = n.finished_at
        ? new Date(n.finished_at).getTime()
        : running && start
          ? now
          : null;
      return {
        id: n.id,
        name: n.name,
        status: n.status,
        attempt: n.attempt,
        start: start != null && !Number.isNaN(start) ? start : null,
        end: end != null && !Number.isNaN(end) ? end : null,
      };
    });
    // Window: run start → run end (or the latest task edge, or now).
    const runStart = new Date(runCreatedAt).getTime();
    const starts = rows.map((r) => r.start).filter((v): v is number => v != null);
    const ends = rows.map((r) => r.end).filter((v): v is number => v != null);
    const t0 = Math.min(Number.isNaN(runStart) ? Infinity : runStart, ...(starts.length ? starts : [now]));
    const runEnd = runFinishedAt ? new Date(runFinishedAt).getTime() : NaN;
    const t1raw = Math.max(Number.isNaN(runEnd) ? -Infinity : runEnd, ...(ends.length ? ends : [now]));
    const t1 = t1raw > t0 ? t1raw : t0 + 1000;
    // Dispatch order top→bottom; never-started tasks sink to the bottom.
    rows.sort((a, b) => (a.start ?? Infinity) - (b.start ?? Infinity) || a.name.localeCompare(b.name));
    return { rows, t0, t1 };
  }, [graph, runCreatedAt, runFinishedAt]);

  const span = t1 - t0;
  const ticks = [0, 0.25, 0.5, 0.75, 1];

  return (
    <div style={{ padding: 24, overflow: "auto", height: "100%" }}>
      <div style={{ maxWidth: 1100 }}>
        {/* time axis */}
        <div style={{ display: "flex", marginBottom: 6 }}>
          <div style={{ width: LABEL_W, flexShrink: 0 }} />
          <div style={{ flex: 1, position: "relative", height: 18 }}>
            {ticks.map((f) => (
              <span
                key={f}
                className="mono"
                style={{
                  position: "absolute",
                  left: `${f * 100}%`,
                  transform: f === 0 ? "none" : f === 1 ? "translateX(-100%)" : "translateX(-50%)",
                  fontSize: 10,
                  color: "var(--dim)",
                }}
              >
                {fmtClock(t0 + f * span)}
              </span>
            ))}
          </div>
        </div>

        {rows.map((r) => {
          const color = statusColor(r.status);
          const hasBar = r.start != null && r.end != null && r.end > r.start;
          const left = hasBar ? ((r.start! - t0) / span) * 100 : 0;
          const width = hasBar ? Math.max(((r.end! - r.start!) / span) * 100, 0.6) : 0;
          const dur = hasBar ? fmtDur(r.end! - r.start!) : null;
          const active = r.status === "running" || r.status === "awaiting_approval";
          return (
            <div
              key={r.id}
              onClick={() => onTaskClick?.(r.id)}
              title={`${r.name} — ${statusLabel(r.status)}${dur ? ` · ${dur}` : ""}${r.attempt > 1 ? ` · try ${r.attempt}` : ""}`}
              style={{
                display: "flex",
                alignItems: "center",
                padding: "3px 0",
                cursor: onTaskClick ? "pointer" : "default",
                background: selected === r.id ? "rgba(255,255,255,0.05)" : undefined,
                borderRadius: 6,
              }}
            >
              <div
                style={{
                  width: LABEL_W,
                  flexShrink: 0,
                  paddingRight: 12,
                  fontSize: 12.5,
                  whiteSpace: "nowrap",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  display: "flex",
                  alignItems: "center",
                  gap: 7,
                }}
              >
                <span className="dy-dot dy-dot-sm" style={{ background: color }} />
                {r.name}
              </div>
              <div
                style={{
                  flex: 1,
                  position: "relative",
                  height: 22,
                  background: "rgba(255,255,255,0.025)",
                  borderRadius: 4,
                }}
              >
                {hasBar ? (
                  <div
                    style={{
                      position: "absolute",
                      left: `${left}%`,
                      width: `${width}%`,
                      top: 3,
                      bottom: 3,
                      background: color,
                      borderRadius: 3,
                      opacity: active ? 0.75 : 0.95,
                      minWidth: 3,
                    }}
                  />
                ) : (
                  <span style={{ position: "absolute", left: 6, top: 3, fontSize: 10.5, color: "var(--dim)" }}>
                    {statusLabel(r.status)}
                  </span>
                )}
                {dur && (
                  <span
                    className="mono"
                    style={{
                      position: "absolute",
                      // Duration label sits after the bar unless the bar ends
                      // near the right edge, then it tucks inside.
                      left: left + width < 88 ? `calc(${left + width}% + 8px)` : undefined,
                      right: left + width >= 88 ? 6 : undefined,
                      top: 4,
                      fontSize: 10.5,
                      color: "var(--muted)",
                      whiteSpace: "nowrap",
                    }}
                  >
                    {dur}
                    {r.attempt > 1 ? ` · try ${r.attempt}` : ""}
                  </span>
                )}
              </div>
            </div>
          );
        })}
        {rows.length === 0 && <p className="dy-empty">No tasks.</p>}
      </div>
    </div>
  );
}
