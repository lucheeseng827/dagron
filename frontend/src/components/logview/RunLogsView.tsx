"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import LogFilterBar from "@/components/logview/LogFilterBar";
import LogLines, { type RenderedLine } from "@/components/logview/LogLines";
import { getRunLogs } from "@/lib/dagron-api";
import { statusColor } from "@/lib/adapter";
import {
  isEmptyFilter,
  lineTime,
  saveFilter,
  taskDuration,
  type LogFilterState,
} from "@/lib/log-filter";
import type { RunLogs } from "@/types/dagron";

export interface RunLogsViewProps {
  runId: string;
  filter: LogFilterState;
  onFilterChange: (next: LogFilterState) => void;
  /// Tasks the view is restricted to (names). Empty = the whole run.
  tasks: string[];
  onTasksChange: (next: string[]) => void;
  /// Open a task's drawer (clicking a line's task label).
  onTaskOpen?: (taskId: string) => void;
  /// Poll while the run is live. Off when the user paused live updates.
  live?: boolean;
}

/// Refresh cadence while the run still has non-terminal tasks. The workflow view
/// re-reads the whole selection rather than tailing per task: a merged view has
/// no single cursor to advance, and at this cadence a full filtered re-read is
/// cheaper than reconciling N interleaved tails.
const POLL_MS = 3000;

/// The **workflow** log view: every task's output in one run, merged, attributed
/// and filtered server-side.
///
/// This is the view for "something in this run went wrong and I don't know
/// where" — the case where opening task drawers one at a time is the actual
/// problem. Narrowing to specific tasks is a scope control (which output is read
/// at all), separate from the filter (which lines survive).
export default function RunLogsView({
  runId,
  filter,
  onFilterChange,
  tasks,
  onTasksChange,
  onTaskOpen,
  live = true,
}: RunLogsViewProps) {
  const [data, setData] = useState<RunLogs | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const paneRef = useRef<HTMLDivElement | null>(null);

  // Serialize the scope so the fetch effect keys on its contents, not on a new
  // array identity from the parent every render.
  const taskKey = tasks.join(",");

  const load = useCallback(
    async (signal: { cancelled: boolean }) => {
      setLoading(true);
      try {
        const r = await getRunLogs(runId, filter, { tasks });
        if (signal.cancelled) return;
        setData(r);
        setError(null);
      } catch (e) {
        if (!signal.cancelled) setError(String(e));
      } finally {
        if (!signal.cancelled) setLoading(false);
      }
    },
    // `tasks` is covered by `taskKey`; depending on the array itself would
    // refetch on every parent render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [runId, filter, taskKey],
  );

  // Drop the previous run's lines and error the moment the run changes, so the
  // new run never briefly renders the old one's task pills and output.
  useEffect(() => {
    setData(null);
    setError(null);
  }, [runId]);

  useEffect(() => {
    const signal = { cancelled: false };
    void load(signal);
    return () => {
      signal.cancelled = true;
    };
  }, [load]);

  // Keep polling only while something can still produce output. A finished run's
  // logs are final — polling them forever is pure load for no new information.
  useEffect(() => {
    if (!live || !data || data.eof) return;
    const signal = { cancelled: false };
    const t = setInterval(() => void load(signal), POLL_MS);
    return () => {
      signal.cancelled = true;
      clearInterval(t);
    };
  }, [live, data, load]);

  useEffect(() => {
    saveFilter(filter);
  }, [filter]);

  // Follow-scroll: stick to the bottom while a live run appends, but only when
  // the user is already near it — don't fight someone reading further up.
  useEffect(() => {
    const el = paneRef.current;
    if (!el) return;
    const nearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 80;
    if (nearBottom) el.scrollTop = el.scrollHeight;
  }, [data]);

  // Task start times, for lines that carry no stamp of their own.
  const taskWindow = useMemo(() => {
    const m = new Map<string, { started?: string; within?: string }>();
    for (const t of data?.tasks ?? []) {
      m.set(t.id, { started: t.started_at, within: taskDuration(t.started_at, t.finished_at) });
    }
    return m;
  }, [data]);

  const lines: RenderedLine[] = useMemo(
    () =>
      (data?.lines ?? []).map((l, i) => ({
        // Task + attempt + line number identifies a line across refetches; the
        // index keeps the key unique if a task somehow repeats a number.
        key: `${l.task_id}:${l.attempt}:${l.n}:${i}`,
        n: l.n,
        level: l.level,
        text: l.text,
        matched: l.matched,
        // Prefer the line's own stamp; fall back to when its task started,
        // marked as an estimate so a derived time never passes for a recorded
        // one.
        time: lineTime(l.ts, taskWindow.get(l.task_id)),
        task: l.task,
        onTaskClick: onTaskOpen ? () => onTaskOpen(l.task_id) : undefined,
      })),
    [data, onTaskOpen, taskWindow],
  );

  const toggleTask = (name: string) => {
    onTasksChange(tasks.includes(name) ? tasks.filter((t) => t !== name) : [...tasks, name]);
  };

  const filtering = !isEmptyFilter(filter);
  const scoped = tasks.length > 0;

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        gap: 10,
        padding: "12px 16px",
        height: "100%",
        minHeight: 0,
      }}
    >
      <LogFilterBar
        value={filter}
        onChange={onFilterChange}
        error={error}
        summary={
          data
            ? {
                matched: data.matched,
                total: data.total,
                truncated: data.truncated,
                limit: data.limit,
              }
            : null
        }
      />

      {/* Task scope. Every task in the run is offered, including ones with no
          output — their absence from the picker would read as "this task doesn't
          exist" rather than "this task printed nothing". */}
      {data && data.tasks.length > 0 && (
        <div style={{ display: "flex", gap: 5, flexWrap: "wrap", alignItems: "center" }}>
          <button
            onClick={() => onTasksChange([])}
            className={`dy-pill ${scoped ? "" : "dy-pill-active"}`}
            style={{ cursor: "pointer", fontSize: 11, padding: "3px 8px" }}
            title="Show output from every task in the run"
          >
            all tasks
          </button>
          {data.tasks.map((t) => {
            const on = tasks.includes(t.name);
            return (
              <button
                key={t.id}
                onClick={() => toggleTask(t.name)}
                className={`dy-pill ${on ? "dy-pill-active" : ""}`}
                aria-pressed={on}
                style={{ cursor: "pointer", fontSize: 11, padding: "3px 8px" }}
                title={`${t.name} — ${t.status}${
                  t.selected ? ` · ${t.matched.toLocaleString()} of ${t.total.toLocaleString()} lines` : ""
                }`}
              >
                <span
                  className="dy-dot dy-dot-sm"
                  style={{ background: statusColor(t.status) }}
                  aria-hidden
                />
                {t.name}
                {t.selected && filtering && t.matched > 0 && (
                  <span style={{ color: "var(--dim)" }}>{t.matched}</span>
                )}
              </button>
            );
          })}
        </div>
      )}

      <LogLines
        ref={paneRef}
        lines={lines}
        style={{ flex: 1, minHeight: 0 }}
        empty={
          loading && !data
            ? "Loading…"
            : filtering
              ? "No lines match this filter. Widen it, or clear it to see everything."
              : scoped
                ? "The selected tasks produced no output."
                : "This run has not produced any output yet."
        }
      />
    </div>
  );
}
