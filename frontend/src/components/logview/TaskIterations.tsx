"use client";

import { useCallback, useEffect, useState } from "react";
import { getTaskAttempts } from "@/lib/dagron-api";
import { errMsg } from "@/lib/err";
import { absTime, timeAgo } from "@/lib/time";
import type { LogFilterState } from "@/lib/log-filter";
import type { TaskAttemptLog, TaskAttempts } from "@/types/dagron";

/// "What did the *other* iterations print?"
///
/// The log pane above shows one attempt, because `task_runs.output` is one
/// column that every attempt overwrites. For a `repeat:` loop that is pass N of
/// N; for a task that failed twice and passed, it is the pass that worked
/// rather than the two that explain why it had to. The superseded attempts are
/// retained as a bounded tail and read here.
///
/// **Collapsed, and fetched only when opened.** The pane above polls while a
/// task runs; this does not, and must not become something that does. Opening
/// it is one request, the same bargain the run-history diff card makes.
export default function TaskIterations({
  runId,
  taskId,
  /// The attempt currently on the row — the one the pane above is showing.
  /// Nothing is offered below 2: there is no history until something has been
  /// superseded.
  attempt,
  /// The log filter, so a filter typed above means the same thing here.
  filter,
}: {
  runId: string;
  taskId: string;
  attempt: number;
  filter: LogFilterState;
}) {
  const [open, setOpen] = useState(false);
  const [data, setData] = useState<TaskAttempts | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [shown, setShown] = useState<number | null>(null);

  // Re-fetch when the task changes, when a new attempt starts (the history just
  // grew by one), or when the filter changes. The fetch lives in the effect so
  // its cleanup owns every request: an orphaned one can still resolve after the
  // panel has moved to another task and write that task's history under this
  // one's heading.
  const key = `${runId}:${taskId}:${attempt}:${JSON.stringify(filter)}`;

  const load = useCallback(() => {
    setLoading(true);
    setError(null);
    let alive = true;
    getTaskAttempts(runId, taskId, filter)
      .then((d) => alive && setData(d))
      .catch((e) => alive && setError(errMsg(e)))
      .finally(() => alive && setLoading(false));
    return () => {
      alive = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);

  useEffect(() => {
    setShown(null);
    if (open) return load();
    setData(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, load]);

  // One attempt means nothing has been superseded yet — there is no history to
  // offer, and a control that opens onto "nothing here" is worse than no
  // control.
  if (attempt < 2) return null;

  const earlier = attempt - 1;

  return (
    <div style={{ border: "1px solid var(--border)", borderRadius: 6, overflow: "hidden" }}>
      <button
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
        className="dy-btn"
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          width: "100%",
          border: "none",
          borderRadius: 0,
          justifyContent: "flex-start",
          fontSize: 12.5,
          padding: "6px 9px",
          cursor: "pointer",
        }}
        title="The attempts before this one. The pane above shows the current attempt only — this is the rest."
      >
        <span style={{ color: "var(--blue)" }} aria-hidden>
          ⟳
        </span>
        <span>
          {earlier} earlier {earlier === 1 ? "attempt" : "attempts"}
        </span>
        <span style={{ flex: 1 }} />
        <span style={{ color: "var(--dim)" }}>{open ? "Hide" : "Show"}</span>
      </button>

      {open && (
        <div style={{ padding: "0 9px 9px" }}>
          {loading ? (
            <p className="dy-empty" style={{ margin: "8px 0 0" }}>
              Loading attempts…
            </p>
          ) : error ? (
            <p style={{ color: "var(--red)", fontSize: 12, margin: "8px 0 0" }} role="alert">
              {error}
            </p>
          ) : !data || data.attempts.length === 0 ? (
            // Not an error, and worth spelling out: retention can be off, and
            // "nothing was kept" reads exactly like "nothing happened" unless
            // the difference is named.
            <p className="dy-empty" style={{ margin: "8px 0 0", fontSize: 12 }}>
              No earlier output was retained for this task. Retention is off when
              <code style={{ margin: "0 4px" }}>DAGRON_ATTEMPT_LOG_BYTES=0</code>
              — before it existed, every attempt overwrote the one before it.
            </p>
          ) : (
            <>
              {data.evicted && (
                // `evicted` only knows the history doesn't start at 1 — which
                // is usually the window, but is also what a run looks like if
                // retention was turned on partway. Name the likely cause
                // without asserting it.
                <p style={{ color: "var(--amber)", fontSize: 11.5, margin: "8px 0 4px" }}>
                  Attempts before #{data.attempts[0].attempt} were not kept — usually the
                  retention window (<code>DAGRON_ATTEMPT_LOG_KEEP</code>).
                </p>
              )}
              <div style={{ display: "flex", flexDirection: "column", gap: 4, marginTop: 8 }}>
                {data.attempts.map((a) => (
                  <AttemptRow
                    key={a.attempt}
                    a={a}
                    expanded={shown === a.attempt}
                    onToggle={() => setShown(shown === a.attempt ? null : a.attempt)}
                  />
                ))}
                <div style={{ display: "flex", gap: 8, alignItems: "center", fontSize: 11.5, padding: "4px 2px" }}>
                  <span className="mono" style={{ color: "var(--muted)" }}>
                    #{data.current_attempt}
                  </span>
                  <span style={{ color: "var(--dim)" }}>current — shown in the pane above</span>
                </div>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}

function AttemptRow({
  a,
  expanded,
  onToggle,
}: {
  a: TaskAttemptLog;
  expanded: boolean;
  onToggle: () => void;
}) {
  // A loop pass that simply hadn't converged yet is not a failure, and colouring
  // them the same is how a healthy 30-iteration poll reads as 30 errors.
  const loop = a.reason === "iteration";
  return (
    <div style={{ border: "1px solid var(--border)", borderRadius: 5, overflow: "hidden" }}>
      <button
        onClick={onToggle}
        aria-expanded={expanded}
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          width: "100%",
          background: "transparent",
          border: "none",
          color: "var(--fg)",
          fontSize: 11.5,
          padding: "5px 8px",
          cursor: "pointer",
          textAlign: "left",
        }}
      >
        <span className="mono" style={{ color: "var(--muted)" }}>
          #{a.attempt}
        </span>
        <span style={{ color: loop ? "var(--blue)" : "var(--red)" }}>
          {loop ? "loop pass" : "failed"}
        </span>
        <span style={{ color: "var(--dim)" }} title={absTime(a.finished_at)}>
          {timeAgo(a.finished_at)}
        </span>
        <span style={{ color: "var(--dim)" }}>
          {a.total} {a.total === 1 ? "line" : "lines"}
        </span>
        {a.retention_truncated && (
          <span
            className="dy-pill"
            style={{ fontSize: 10 }}
            title="Only the end of this attempt's output was kept (DAGRON_ATTEMPT_LOG_BYTES)."
          >
            tail only
          </span>
        )}
        <span style={{ flex: 1 }} />
        <span style={{ color: "var(--dim)" }}>{expanded ? "−" : "+"}</span>
      </button>
      {expanded && (
        <pre
          style={{
            background: "var(--bg)",
            margin: 0,
            padding: "0.6rem",
            fontSize: 11.5,
            whiteSpace: "pre-wrap",
            wordBreak: "break-word",
            maxHeight: 240,
            overflow: "auto",
          }}
        >
          {a.retention_truncated && (
            <span style={{ color: "var(--dim)" }}>{"… earlier output not retained\n"}</span>
          )}
          {a.output || "(no output)"}
        </pre>
      )}
    </div>
  );
}
