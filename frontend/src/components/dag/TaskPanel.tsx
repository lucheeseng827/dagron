"use client";

import { useEffect, useState } from "react";
import { getTaskLogs } from "@/lib/dagron-api";
import { statusColor, statusLabel } from "@/lib/adapter";
import type { TaskLogs } from "@/types/dagron";

export interface TaskPanelProps {
  runId: string;
  taskId: string | null;
  onClose: () => void;
  /// Render extra controls (e.g. retry button) in the panel header.
  actions?: (logs: TaskLogs) => React.ReactNode;
}

/// Right-side drawer: task detail + logs for the clicked node.
export default function TaskPanel({ runId, taskId, onClose, actions }: TaskPanelProps) {
  const [logs, setLogs] = useState<TaskLogs | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!taskId) {
      setLogs(null);
      return;
    }
    let live = true;
    // Drop the previous task's logs immediately so stale status/output/actions
    // (and a misdirected retry) can't show while the new fetch is in flight.
    setLogs(null);
    setError(null);
    getTaskLogs(runId, taskId)
      .then((l) => live && setLogs(l))
      .catch((e) => live && setError(String(e)));
    return () => {
      live = false;
    };
  }, [runId, taskId]);

  if (!taskId) return null;

  return (
    <aside
      style={{
        width: 380,
        borderLeft: "1px solid rgba(255,255,255,0.08)",
        background: "var(--card)",
        padding: "1rem",
        overflow: "auto",
        display: "flex",
        flexDirection: "column",
        gap: "0.75rem",
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <strong>{logs?.name ?? "Task"}</strong>
        <button onClick={onClose} className="dy-pill" style={{ cursor: "pointer" }}>
          ✕
        </button>
      </div>
      {error && <p style={{ color: "#f85149" }}>{error}</p>}
      {logs && (
        <>
          <div style={{ display: "flex", gap: "1rem", fontSize: 13 }}>
            <span style={{ color: statusColor(logs.status) }}>{statusLabel(logs.status)}</span>
            <span style={{ color: "var(--muted)" }}>attempt {logs.attempt}</span>
          </div>
          {actions && <div>{actions(logs)}</div>}
          <pre
            style={{
              background: "var(--bg)",
              padding: "0.75rem",
              borderRadius: 6,
              fontSize: 12,
              whiteSpace: "pre-wrap",
              wordBreak: "break-word",
              minHeight: 80,
            }}
          >
            {logs.output ?? "(no output)"}
          </pre>
        </>
      )}
    </aside>
  );
}
