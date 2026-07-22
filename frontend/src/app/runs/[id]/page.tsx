"use client";

import { use, useCallback, useEffect, useRef, useState } from "react";
import Link from "next/link";
import DagGraph from "@/components/dag/DagGraph";
import RunTimeline from "@/components/dag/RunTimeline";
import TaskPanel from "@/components/dag/TaskPanel";
import LiveToggle from "@/components/LiveToggle";
import RerunDialog from "@/components/RerunDialog";
import RerunMenu from "@/components/RerunMenu";
import TriggerBadge from "@/components/TriggerBadge";
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
import { statusColor } from "@/lib/adapter";
import { useLiveUpdates, type ConnStatus } from "@/lib/live";
import { absTime, duration } from "@/lib/time";
import type { GraphResponse, RunDetail } from "@/types/dagron";

type View = "graph" | "timeline";

const TERMINAL = new Set(["succeeded", "failed", "cancelled"]);
const CLEARABLE = new Set(["succeeded", "failed", "skipped", "cancelled"]);

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

  // Keep ?task= in the URL so a selected task's logs are shareable by link.
  useEffect(() => {
    // Don't touch the URL until the graph is loaded — this effect fires on
    // mount with graph=null and would delete a deep-linked ?task= before
    // refetch() gets a chance to read it.
    if (!graph) return;
    const url = new URL(window.location.href);
    const node = graph.nodes.find((n) => n.id === selected);
    if (node) url.searchParams.set("task", node.name);
    else url.searchParams.delete("task");
    window.history.replaceState(null, "", url.toString());
  }, [selected, graph]);

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
          {(["graph", "timeline"] as const).map((v) => (
            <button
              key={v}
              onClick={() => setView(v)}
              className={`dy-pill ${view === v ? "dy-pill-active" : ""}`}
              style={{ cursor: "pointer", textTransform: "capitalize" }}
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
      </header>

      {error && <p style={{ color: "var(--red)", padding: "8px 24px" }}>{error}</p>}

      <div style={{ flex: 1, minHeight: 0, display: "flex" }}>
        <div style={{ flex: 1, minHeight: 0 }}>
          {graph &&
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
            ))}
        </div>
        <TaskPanel
          runId={id}
          taskId={selected}
          onClose={() => setSelected(null)}
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
