"use client";

import { useEffect, useRef, useState } from "react";
import { getTaskLogs } from "@/lib/dagron-api";
import { statusColor, statusLabel } from "@/lib/adapter";
import type { TaskLogs } from "@/types/dagron";

export interface TaskPanelProps {
  runId: string;
  taskId: string | null;
  onClose: () => void;
  /// Render extra controls (e.g. retry/approve buttons) in the panel header.
  actions?: (logs: TaskLogs) => React.ReactNode;
}

const TAIL_INTERVAL_MS = 2000;

/// Right-side drawer: task detail + logs for the clicked node. Logs tail live:
/// after the initial full fetch, output past `next_offset` is polled and
/// appended until the task is terminal (`eof`) — no full-refetch flicker.
export default function TaskPanel({ runId, taskId, onClose, actions }: TaskPanelProps) {
  const [logs, setLogs] = useState<TaskLogs | null>(null);
  const [output, setOutput] = useState("");
  const [error, setError] = useState<string | null>(null);
  const preRef = useRef<HTMLPreElement | null>(null);
  // Tail cursor lives in a ref so the poll interval reads the latest value.
  const offsetRef = useRef(0);

  useEffect(() => {
    if (!taskId) {
      setLogs(null);
      return;
    }
    let live = true;
    let timer: ReturnType<typeof setInterval> | null = null;
    // In-flight guard: if a poll outlives the 2s interval (slow endpoint), the
    // next tick must not start a second fetch from the same offset — two
    // concurrent reads of the same delta would append duplicated output.
    let inFlight = false;
    // Drop the previous task's logs immediately so stale status/output/actions
    // (and a misdirected retry) can't show while the new fetch is in flight.
    setLogs(null);
    setOutput("");
    setError(null);
    offsetRef.current = 0;

    const stop = () => {
      if (timer) clearInterval(timer);
      timer = null;
    };

    const applyFull = (l: TaskLogs) => {
      setLogs(l);
      setOutput(l.output ?? "");
      offsetRef.current = l.next_offset;
    };

    const poll = async () => {
      if (inFlight) return;
      inFlight = true;
      try {
        const l = await getTaskLogs(runId, taskId, offsetRef.current);
        if (!live) return;
        if (l.next_offset < offsetRef.current) {
          // Output shrank — the task was retried/cleared and restarted from
          // scratch. Resync with a full fetch instead of appending garbage.
          const fresh = await getTaskLogs(runId, taskId);
          if (!live) return;
          applyFull(fresh);
        } else {
          setLogs((prev) => (prev ? { ...prev, ...l, output: prev.output } : l));
          if (l.output) setOutput((o) => o + l.output);
          offsetRef.current = l.next_offset;
        }
        if (l.eof) stop();
      } catch (e) {
        if (live) setError(String(e));
        stop();
      } finally {
        inFlight = false;
      }
    };

    getTaskLogs(runId, taskId)
      .then((l) => {
        if (!live) return;
        applyFull(l);
        if (!l.eof) timer = setInterval(poll, TAIL_INTERVAL_MS);
      })
      .catch((e) => live && setError(String(e)));

    return () => {
      live = false;
      stop();
    };
  }, [runId, taskId]);

  // Follow-scroll: stick to the bottom while new output streams in, but only
  // when the user is already near the bottom — don't fight an upward scroll.
  useEffect(() => {
    const el = preRef.current;
    if (!el) return;
    const nearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 60;
    if (nearBottom) el.scrollTop = el.scrollHeight;
  }, [output]);

  if (!taskId) return null;

  const streaming = logs != null && !logs.eof;

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
          <div style={{ display: "flex", gap: "1rem", fontSize: 13, alignItems: "center" }}>
            <span style={{ color: statusColor(logs.status) }}>{statusLabel(logs.status)}</span>
            <span style={{ color: "var(--muted)" }}>attempt {logs.attempt}</span>
            {streaming && (
              <span style={{ color: "var(--blue)", fontSize: 11, display: "inline-flex", alignItems: "center", gap: 5 }}>
                <span className="dy-dot dy-dot-sm" style={{ background: "var(--blue)" }} />
                tailing
              </span>
            )}
          </div>
          {actions && <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>{actions(logs)}</div>}
          <pre
            ref={preRef}
            style={{
              background: "var(--bg)",
              padding: "0.75rem",
              borderRadius: 6,
              fontSize: 12,
              whiteSpace: "pre-wrap",
              wordBreak: "break-word",
              minHeight: 80,
              maxHeight: "60vh",
              overflow: "auto",
            }}
          >
            {output || "(no output)"}
          </pre>
        </>
      )}
    </aside>
  );
}
