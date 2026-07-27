"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import LogFilterBar from "@/components/logview/LogFilterBar";
import LogLines, { type RenderedLine } from "@/components/logview/LogLines";
import { getTaskLogs } from "@/lib/dagron-api";
import { statusColor, statusLabel, type WaitingOn } from "@/lib/adapter";
import {
  DEFAULT_LOG_LIMIT,
  isEmptyFilter,
  lineTime,
  taskDuration,
  type LogFilterState,
} from "@/lib/log-filter";
import { absTime, fromNow } from "@/lib/time";
import type { LogLine, TaskLogs } from "@/types/dagron";

export interface TaskPanelProps {
  runId: string;
  taskId: string | null;
  onClose: () => void;
  /// Render extra controls (e.g. retry/approve buttons) in the panel header.
  actions?: (logs: TaskLogs) => React.ReactNode;
  /// Why this task is parked, from the run detail (`waitingOn`). A parked task
  /// is `running` with no logs and no lease, so without this the panel shows a
  /// spinner-shaped nothing and the task reads as hung.
  waiting?: WaitingOn | null;
  /// Scheduling facts from the run detail that the logs endpoint doesn't carry:
  /// the pool the task drew a slot from, its dispatch priority, and whether it
  /// was served from the memoization cache.
  pool?: string | null;
  priority?: number;
  cacheHit?: boolean;
  /// The log filter, shared with the run's workflow log view so switching
  /// between them doesn't silently change what you're looking at.
  filter: LogFilterState;
  onFilterChange: (next: LogFilterState) => void;
}

const TAIL_INTERVAL_MS = 2000;

/// Right-side drawer: task detail + logs for the clicked node. Logs tail live:
/// after the initial full fetch, output past `next_offset` is polled and
/// appended until the task is terminal (`eof`) — no full-refetch flicker.
///
/// With a filter set, the server applies it *inside* each tailed slice, so the
/// tail keeps working: only matching new lines are appended, while `next_offset`
/// still advances over the raw text. Changing the filter restarts from offset 0
/// — the earlier slices were filtered under the old predicate, and re-deriving
/// them client-side would need the raw output the filter exists to avoid
/// fetching.
export default function TaskPanel({
  runId,
  taskId,
  onClose,
  actions,
  waiting,
  pool,
  priority,
  cacheHit,
  filter,
  onFilterChange,
}: TaskPanelProps) {
  const [logs, setLogs] = useState<TaskLogs | null>(null);
  const [output, setOutput] = useState("");
  const [lines, setLines] = useState<LogLine[]>([]);
  const [error, setError] = useState<string | null>(null);
  const paneRef = useRef<HTMLDivElement | null>(null);
  const preRef = useRef<HTMLPreElement | null>(null);
  // Tail cursor lives in a ref so the poll interval reads the latest value.
  const offsetRef = useRef(0);
  // Running totals over every slice fetched so far. The server reports these per
  // slice; a tailing pane needs them for the whole task, or the summary would
  // reset to "0 of 12 lines" on each poll.
  const [seen, setSeen] = useState({ total: 0, matched: 0, truncated: false });

  const filtering = isEmptyFilter(filter) ? false : true;
  // Re-key the fetch effect on the filter's serialized form: a new object with
  // the same fields must not restart the tail.
  const filterKey = useMemo(() => JSON.stringify(filter), [filter]);

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
    setLines([]);
    setSeen({ total: 0, matched: 0, truncated: false });
    setError(null);
    offsetRef.current = 0;

    const stop = () => {
      if (timer) clearInterval(timer);
      timer = null;
    };

    const applyFull = (l: TaskLogs) => {
      setLogs(l);
      setOutput(l.output ?? "");
      setLines(l.lines ?? []);
      setSeen({ total: l.total, matched: l.matched, truncated: l.truncated });
      offsetRef.current = l.next_offset;
    };

    const poll = async () => {
      if (inFlight) return;
      inFlight = true;
      try {
        const l = await getTaskLogs(runId, taskId, offsetRef.current, filter);
        if (!live) return;
        if (l.next_offset < offsetRef.current) {
          // Output shrank — the task was retried/cleared and restarted from
          // scratch. Resync with a full fetch instead of appending garbage.
          const fresh = await getTaskLogs(runId, taskId, undefined, filter);
          if (!live) return;
          applyFull(fresh);
        } else {
          setLogs((prev) => (prev ? { ...prev, ...l, output: prev.output } : l));
          if (l.output) setOutput((o) => o + l.output);
          if (l.lines?.length) setLines((prev) => [...prev, ...l.lines!]);
          setSeen((s) => ({
            total: s.total + l.total,
            matched: s.matched + l.matched,
            truncated: s.truncated || l.truncated,
          }));
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

    getTaskLogs(runId, taskId, undefined, filter)
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
    // `filter` is covered by `filterKey`.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runId, taskId, filterKey]);

  // Follow-scroll: stick to the bottom while new output streams in, but only
  // when the user is already near the bottom — don't fight an upward scroll.
  useEffect(() => {
    const el = filtering ? paneRef.current : preRef.current;
    if (!el) return;
    const nearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 60;
    if (nearBottom) el.scrollTop = el.scrollHeight;
  }, [output, lines, filtering]);

  const rendered: RenderedLine[] = useMemo(
    () =>
      lines.map((l, i) => ({
        key: `${l.n}:${i}`,
        n: l.n,
        level: l.level,
        text: l.text,
        matched: l.matched,
        // One task in this pane, so every fallback time is its start.
        time: lineTime(l.ts, {
          started: logs?.started_at,
          within: taskDuration(logs?.started_at, logs?.finished_at),
        }),
      })),
    [lines, logs?.started_at, logs?.finished_at],
  );

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
      {/* Once the logs load, the filter bar owns error display — a rejected
          filter belongs next to the box that caused it. This only covers the
          case where there's no pane to attach it to. */}
      {error && !logs && <p style={{ color: "var(--red)" }}>{error}</p>}
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
          {(cacheHit || pool || (priority ?? 0) !== 0) && (
            <div style={{ display: "flex", gap: 6, flexWrap: "wrap", fontSize: 11 }}>
              {cacheHit && (
                <span className="dy-pill" title="Served from the memoization cache — no worker, no secrets, no artifacts">
                  ⚡ cached
                </span>
              )}
              {pool && (
                <span className="dy-pill" title={`Drew a slot from concurrency pool "${pool}"`}>
                  pool {pool}
                </span>
              )}
              {(priority ?? 0) !== 0 && (
                <span className="dy-pill" title="Dispatch priority among simultaneously-ready tasks (higher goes first)">
                  priority {priority}
                </span>
              )}
            </div>
          )}
          {waiting && (
            <div
              style={{
                display: "flex",
                flexDirection: "column",
                gap: 4,
                padding: "0.6rem 0.7rem",
                borderRadius: 6,
                border: "1px solid rgba(88,166,255,0.35)",
                background: "rgba(88,166,255,0.10)",
                fontSize: 12.5,
              }}
            >
              <span style={{ color: "var(--blue)", display: "inline-flex", alignItems: "center", gap: 6 }}>
                <span aria-hidden>⏸</span>
                {waiting.label}
                {/* A time sensor's deadline is the one detail that reads better
                    relative than absolute — "in 4m" answers "is it stuck?". */}
                {waiting.runLink === undefined && waiting.label === "Waiting until" && (
                  <span style={{ color: "var(--muted)" }}>{fromNow(waiting.detail)}</span>
                )}
              </span>
              {waiting.runLink ? (
                <Link href={`/runs/${waiting.runLink}`} className="mono" style={{ color: "var(--blue)", wordBreak: "break-all" }}>
                  {waiting.detail}
                </Link>
              ) : (
                <span className="mono" style={{ color: "var(--muted)", wordBreak: "break-all" }} title={absTime(waiting.detail)}>
                  {waiting.detail}
                </span>
              )}
              <span style={{ color: "var(--dim)", fontSize: 11 }}>
                Parked — holds no worker slot. It stays “running” until this resolves.
              </span>
            </div>
          )}
          {actions && <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>{actions(logs)}</div>}
          <LogFilterBar
            value={filter}
            onChange={onFilterChange}
            compact
            error={error}
            summary={{ ...seen, limit: filter.limit || DEFAULT_LOG_LIMIT }}
          />
          {/* Two panes on purpose. Unfiltered output stays a plain `<pre>` — the
              raw text, copyable in one selection, exactly as before. A filtered
              view needs per-line structure (source line number, level colour,
              dimmed context), which a single text blob can't carry. */}
          {filtering ? (
            <LogLines
              ref={paneRef}
              lines={rendered}
              style={{ minHeight: 80, maxHeight: "60vh" }}
              empty="No lines match this filter."
            />
          ) : (
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
          )}
        </>
      )}
    </aside>
  );
}
