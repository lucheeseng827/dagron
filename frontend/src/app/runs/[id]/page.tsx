"use client";

import { use, useCallback, useEffect, useRef, useState } from "react";
import Link from "next/link";
import DagGraph from "@/components/dag/DagGraph";
import RunTimeline from "@/components/dag/RunTimeline";
import TaskPanel from "@/components/dag/TaskPanel";
import RunLogsView from "@/components/logview/RunLogsView";
import LiveToggle from "@/components/LiveToggle";
import RerunDialog from "@/components/RerunDialog";
import RerunMenu from "@/components/RerunMenu";
import TriggerBadge from "@/components/TriggerBadge";
import { clearTriage, setTriage } from "@/lib/dagron-api";
import { useToast } from "@/components/Toasts";
import {
  approveTask,
  cancelRun,
  clearTask,
  getRun,
  getRunGraph,
  listWorkflows,
  rejectTask,
  rerunRun,
  retryTask,
} from "@/lib/dagron-api";
import { subscribeRun } from "@/lib/dagron-stream";
import { statusColor, waitingOn } from "@/lib/adapter";
import { useLiveUpdates, type ConnStatus } from "@/lib/live";
import {
  EMPTY_FILTER,
  fromParams,
  loadFilter,
  saveFilter,
  toParams,
  type LogFilterState,
} from "@/lib/log-filter";
import { absTime, duration } from "@/lib/time";
import type { GraphResponse, RunDetail } from "@/types/dagron";

type View = "graph" | "timeline" | "logs";

const TERMINAL = new Set(["succeeded", "failed", "cancelled"]);
const CLEARABLE = new Set(["succeeded", "failed", "skipped", "cancelled"]);

/// Query keys the log filter owns. Listed once so the URL writer can clear the
/// whole set before re-writing it — otherwise a param dropped from the filter
/// would linger in the URL and reload as still-set.
const FILTER_URL_KEYS = ["q", "exclude", "regex", "case", "level", "context", "limit", "tail"];

function hasAnyFilterParam(p: URLSearchParams): boolean {
  return FILTER_URL_KEYS.some((k) => p.has(k));
}

export default function RunPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = use(params);
  const toast = useToast();
  const [run, setRun] = useState<RunDetail | null>(null);
  const [graph, setGraph] = useState<GraphResponse | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [live] = useLiveUpdates();
  const [conn, setConn] = useState<ConnStatus>("offline");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [rerunOpen, setRerunOpen] = useState(false);
  const [view, setView] = useState<View>("graph");
  // Saved-workflow id matching this run's name, for the header backlink.
  const [workflowId, setWorkflowId] = useState<string | null>(null);
  // The log filter, shared by the workflow log view and the task drawer so
  // switching between them never silently changes what's being shown.
  const [filter, setFilter] = useState<LogFilterState>(EMPTY_FILTER);
  // Tasks the workflow log view is narrowed to (names). Empty = whole run.
  const [logTasks, setLogTasks] = useState<string[]>([]);
  // Hydration guard: the filter and view come from the URL (shareable) or
  // localStorage (sticky), both of which only exist in the browser. Until that
  // has run, the URL-writing effect below must not overwrite the very params
  // it's about to read.
  const [hydrated, setHydrated] = useState(false);

  // Debounced refetch so a burst of events triggers at most one reload.
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  // Auto-open the first task once per run; don't fight the user reclosing it.
  const didAutoSelect = useRef(false);
  const refetch = useCallback(() => {
    if (timer.current) clearTimeout(timer.current);
    timer.current = setTimeout(() => {
      Promise.all([getRun(id), getRunGraph(id)])
        .then(([r, g]) => {
          setRun(r);
          setGraph(g);
          // Auto-open the deep-linked task (?task=<name or id>) or the first
          // task, once on initial load — don't reopen after the user closes it.
          setSelected((s) => {
            if (s != null) return s;
            if (didAutoSelect.current) return null;
            didAutoSelect.current = true;
            const want = new URLSearchParams(window.location.search).get("task");
            if (want) {
              const hit = g.nodes.find((n) => n.id === want || n.name === want);
              if (hit) return hit.id;
            }
            return g.nodes[0]?.id ?? null;
          });
        })
        .catch((e) => setError(String(e)));
    }, 150);
  }, [id]);

  // Reset the one-time auto-open when navigating to a different run.
  useEffect(() => {
    didAutoSelect.current = false;
    setSelected(null);
  }, [id]);

  // Initial load.
  useEffect(() => {
    refetch();
  }, [refetch]);

  // Restore the view, log filter and log scope from the URL — a link to a
  // filtered log view has to reopen as that filtered log view, or "send me the
  // link" doesn't work. Falls back to the last filter this browser used, since
  // triage is repetitive and retyping it per run is the original complaint.
  useEffect(() => {
    const p = new URLSearchParams(window.location.search);
    // Assign unconditionally, never "only when present": this effect re-runs on
    // every run id, so a bare `if (x) set(x)` would carry the previous run's
    // scope across. Task names don't survive that trip, so the next run's log
    // view would show zero matches with no pill looking active — an empty
    // screen with nothing to explain it.
    const v = p.get("view");
    setView(v === "logs" || v === "timeline" ? v : "graph");
    const fromUrl = fromParams(p);
    setFilter(hasAnyFilterParam(p) ? fromUrl : loadFilter());
    const scope = p.get("logTask");
    setLogTasks(scope ? scope.split(",").filter(Boolean) : []);
    setHydrated(true);
  }, [id]);

  // Persist the filter so it carries to the next run's logs.
  useEffect(() => {
    if (hydrated) saveFilter(filter);
  }, [filter, hydrated]);

  // Keep ?task=/?view=/the filter in the URL so a log view is shareable by link.
  useEffect(() => {
    // Don't touch the URL until the graph is loaded — this effect fires on
    // mount with graph=null and would delete a deep-linked ?task= before
    // refetch() gets a chance to read it.
    if (!graph || !hydrated) return;
    const url = new URL(window.location.href);
    const node = graph.nodes.find((n) => n.id === selected);
    if (node) url.searchParams.set("task", node.name);
    else url.searchParams.delete("task");
    if (view === "graph") url.searchParams.delete("view");
    else url.searchParams.set("view", view);
    if (logTasks.length) url.searchParams.set("logTask", logTasks.join(","));
    else url.searchParams.delete("logTask");
    // Rewrite the filter params wholesale: a param dropped from the filter has
    // to leave the URL too, or a cleared filter would still share as a set one.
    for (const key of FILTER_URL_KEYS) url.searchParams.delete(key);
    for (const [k, v] of toParams(filter)) url.searchParams.set(k, v);
    window.history.replaceState(null, "", url.toString());
  }, [selected, graph, view, filter, logTasks, hydrated]);

  // Resolve the run's workflow name → saved workflow id (best-effort backlink).
  useEffect(() => {
    if (!run?.name) return;
    let alive = true;
    listWorkflows()
      .then((ws) => {
        if (!alive) return;
        const hit = ws.find((w) => w.name === run.name);
        setWorkflowId(hit?.id ?? null);
      })
      .catch(() => {});
    return () => {
      alive = false;
    };
  }, [run?.name]);

  // Cancel any queued refetch on unmount so it can't fire after navigation.
  useEffect(() => {
    return () => {
      if (timer.current) clearTimeout(timer.current);
    };
  }, []);

  // Live updates: refetch on any event for this run; resync also refetches.
  // Gated on the global live-updates toggle — paused holds no stream open, and
  // resuming refetches to catch up on whatever happened meanwhile.
  useEffect(() => {
    if (!live) {
      setConn("paused");
      return;
    }
    setConn("offline");
    const unsub = subscribeRun(id, {
      onEvent: () => refetch(),
      onResync: () => refetch(),
      onStatus: (s) => {
        setConn(s);
        // Catch up on (re)connect and on resume-after-pause; the 150ms
        // debounce coalesces this with the initial load on fresh mounts.
        if (s === "live") refetch();
      },
    });
    return unsub;
  }, [id, refetch, live]);

  const act = async (fn: () => Promise<unknown>, okMsg: string) => {
    setBusy(true);
    try {
      await fn();
      toast(okMsg);
    } catch (e) {
      setError(String(e));
      toast(String(e), "error");
    } finally {
      setBusy(false);
    }
    // SSE will refetch; nudge in case events are delayed.
    refetch();
  };

  const onCancel = () => {
    if (!confirm("Cancel this run? Non-terminal tasks will be cancelled.")) return;
    void act(() => cancelRun(id), "Run cancelled");
  };
  const onRerun = () => {
    if (!confirm("Rerun from failure? Failed/cancelled tasks re-run; succeeded tasks are kept.")) return;
    void act(() => rerunRun(id), "Rerunning from failure");
  };
  const onRetry = (tid: string) => void act(() => retryTask(id, tid), "Task retrying");
  const onClear = (tid: string, name: string) => {
    if (!confirm(`Clear "${name}" and re-run it plus everything downstream of it?`)) return;
    void act(() => clearTask(id, tid), "Task cleared — downstream re-running");
  };
  const onApprove = (tid: string) => void act(() => approveTask(id, tid), "Gate approved");
  const onReject = (tid: string) => {
    if (!confirm("Reject this approval gate? The task fails and dependents skip.")) return;
    void act(() => rejectTask(id, tid), "Gate rejected");
  };

  // The open task's row from the run detail: the park reason and scheduling
  // facts the logs endpoint doesn't carry (a parked task has no logs at all).
  const selectedTask = selected ? run?.tasks.find((t) => t.id === selected) : undefined;

  const runActive = run ? !TERMINAL.has(run.status) : false;
  // A failed/cancelled run can resume from its failure frontier.
  const runRerunnable = run ? run.status === "failed" || run.status === "cancelled" : false;

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100vh" }}>
      <header
        style={{
          display: "flex",
          alignItems: "center",
          gap: 14,
          padding: "14px 24px",
          borderBottom: "1px solid var(--border)",
          background: "var(--side)",
        }}
      >
        <strong style={{ fontSize: 15, whiteSpace: "nowrap" }}>
          {run?.name ? (
            workflowId ? (
              <Link href={`/workflows/${workflowId}/history`} style={{ color: "var(--fg)" }} title="Workflow history">
                {run.name}
              </Link>
            ) : (
              run.name
            )
          ) : (
            "Run"
          )}{" "}
          <span className="mono" style={{ color: "var(--muted)" }}>
            {id.slice(0, 8)}
          </span>
        </strong>
        {run && (
          <span style={{ display: "inline-flex", alignItems: "center", gap: 7, color: statusColor(run.status) }}>
            <span className="dy-dot" style={{ background: statusColor(run.status) }} />
            {run.status}
          </span>
        )}
        {run && <TriggerBadge kind={run.trigger_kind} />}
        {run && (
          <span
            className="mono"
            style={{ fontSize: 12, color: "var(--muted)", whiteSpace: "nowrap" }}
            title={`started ${absTime(run.created_at)}${run.finished_at ? `\nfinished ${absTime(run.finished_at)}` : ""}`}
          >
            {duration(run.created_at, run.finished_at)}
          </span>
        )}
        <LiveToggle status={conn} onRefresh={refetch} />
        <div style={{ flex: 1 }} />
        <div style={{ display: "flex", gap: 3, background: "var(--panel)", border: "1px solid var(--border)", borderRadius: 8, padding: 3 }}>
          {(["graph", "timeline", "logs"] as const).map((v) => (
            <button
              key={v}
              onClick={() => setView(v)}
              className={`dy-pill ${view === v ? "dy-pill-active" : ""}`}
              style={{ cursor: "pointer", textTransform: "capitalize" }}
              title={
                v === "logs"
                  ? "Every task's output in one filterable stream"
                  : `Show the run as a ${v}`
              }
            >
              {v}
            </button>
          ))}
        </div>
        {runActive && (
          <button onClick={onCancel} disabled={busy} className="dy-btn dy-btn-danger">
            Cancel run
          </button>
        )}
        {runRerunnable && (
          <button
            onClick={onRerun}
            disabled={busy}
            className="dy-btn dy-btn-primary"
            title="Re-run only the failed/cancelled tasks; succeeded tasks are kept"
          >
            ▶ Resume from failure
          </button>
        )}
        {run && TERMINAL.has(run.status) && (
          <RerunMenu runId={id} disabled={busy} onError={setError} onEdit={() => setRerunOpen(true)} />
        )}
        {/* Only for failures: triaging a run that succeeded would be recording
            a decision about a non-event. */}
        {run?.status === "failed" && (
          <TriageControl
            runId={id}
            state={run.triage_state}
            by={run.triaged_by}
            note={run.triage_note}
            onDone={refetch}
            onError={setError}
          />
        )}
      </header>

      {error && <p style={{ color: "var(--red)", padding: "8px 24px" }}>{error}</p>}

      <div style={{ flex: 1, minHeight: 0, display: "flex" }}>
        <div style={{ flex: 1, minHeight: 0 }}>
          {/* The log view doesn't need the graph — it's the view you reach for
              when the graph hasn't told you anything useful — so render it
              without waiting for one. */}
          {view === "logs" ? (
            <RunLogsView
              runId={id}
              filter={filter}
              onFilterChange={setFilter}
              tasks={logTasks}
              onTasksChange={setLogTasks}
              onTaskOpen={setSelected}
              live={live}
            />
          ) : (
            graph &&
            (view === "graph" ? (
              <DagGraph graph={graph} runStatus={run?.status} onNodeClick={setSelected} />
            ) : (
              run && (
                <RunTimeline
                  graph={graph}
                  runCreatedAt={run.created_at}
                  runFinishedAt={run.finished_at}
                  onTaskClick={setSelected}
                  selected={selected}
                />
              )
            ))
          )}
        </div>
        <TaskPanel
          runId={id}
          taskId={selected}
          onClose={() => setSelected(null)}
          // Park reason comes from the run detail, not the logs endpoint: a
          // parked task has no output to tail, which is exactly why it needs a
          // reason shown.
          waiting={waitingOn(selectedTask)}
          pool={selectedTask?.pool}
          priority={selectedTask?.priority}
          cacheHit={selectedTask?.cache_hit}
          filter={filter}
          onFilterChange={setFilter}
          actions={(logs) => (
            <>
              {logs.status === "awaiting_approval" && (
                <>
                  <button onClick={() => onApprove(logs.task_id)} disabled={busy} className="dy-btn dy-btn-primary">
                    ✓ Approve
                  </button>
                  <button onClick={() => onReject(logs.task_id)} disabled={busy} className="dy-btn dy-btn-danger">
                    ✕ Reject
                  </button>
                </>
              )}
              {(logs.status === "failed" || logs.status === "cancelled") && (
                <button onClick={() => onRetry(logs.task_id)} disabled={busy} className="dy-btn dy-btn-primary">
                  Retry task
                </button>
              )}
              {CLEARABLE.has(logs.status) && (
                <button
                  onClick={() => onClear(logs.task_id, logs.name)}
                  disabled={busy}
                  className="dy-btn"
                  title="Reset this task and everything downstream of it, then re-run"
                >
                  ↺ Clear + downstream
                </button>
              )}
            </>
          )}
        />
      </div>

      {rerunOpen && <RerunDialog runId={id} onClose={() => setRerunOpen(false)} />}
    </div>
  );
}

/// Record what was decided about a failed run.
///
/// Three states rather than one "handled" flag, because they are genuinely
/// different answers and the difference is the reason to keep the record at all:
/// someone being on it is not the same as it being dealt with, and neither is
/// the same as deciding to accept it. Collapsed into one this would be a
/// mark-as-read button, and nobody would trust it a month later.
function TriageControl({
  runId,
  state,
  by,
  note,
  onDone,
  onError,
}: {
  runId: string;
  state?: "acknowledged" | "resolved" | "ignored";
  by?: string;
  note?: string;
  onDone: () => void;
  onError: (m: string) => void;
}) {
  const [busy, setBusy] = useState(false);

  async function mark(next: "acknowledged" | "resolved" | "ignored") {
    // "Ignored" is the one that needs a reason: it is a decision to live with a
    // real failure, and the note is what makes that defensible later. Asked for
    // before the request so cancelling the prompt cancels the whole action.
    let why: string | undefined;
    if (next === "ignored") {
      const answer = window.prompt("Why is this being accepted rather than fixed?");
      if (answer === null) return;
      why = answer.trim() || undefined;
    }
    setBusy(true);
    try {
      await setTriage(runId, next, why);
      onDone();
    } catch (e) {
      onError(String(e));
    } finally {
      setBusy(false);
    }
  }

  async function undo() {
    setBusy(true);
    try {
      await clearTriage(runId);
      onDone();
    } catch (e) {
      onError(String(e));
    } finally {
      setBusy(false);
    }
  }

  if (state) {
    return (
      <span
        className="dy-pill"
        title={[`Triaged by ${by ?? "someone"}`, note && `\u201c${note}\u201d`]
          .filter(Boolean)
          .join(" \u2014 ")}
        style={{ display: "inline-flex", gap: 8, alignItems: "center" }}
      >
        {state}
        <button
          onClick={undo}
          disabled={busy}
          title="Put this run back in the attention queue"
          style={{
            background: "none",
            border: "none",
            color: "var(--dim)",
            cursor: "pointer",
            padding: 0,
            font: "inherit",
          }}
        >
          undo
        </button>
      </span>
    );
  }

  return (
    <span style={{ display: "inline-flex", gap: 4 }}>
      {(["acknowledged", "resolved", "ignored"] as const).map((s) => (
        <button
          key={s}
          className="dy-btn"
          disabled={busy}
          onClick={() => mark(s)}
          title={
            s === "acknowledged"
              ? "Seen \u2014 someone is looking at it. Leaves the attention queue."
              : s === "resolved"
                ? "Dealt with: rerun, fixed upstream, or the data was corrected."
                : "A real failure we have decided to accept. Asks for a reason."
          }
          style={{ fontSize: 12, padding: "5px 9px" }}
        >
          {s}
        </button>
      ))}
    </span>
  );
}
