"use client";

import { use, useCallback, useEffect, useRef, useState } from "react";
import DagGraph from "@/components/dag/DagGraph";
import TaskPanel from "@/components/dag/TaskPanel";
import { useRouter } from "next/navigation";
import { cancelRun, getRun, getRunGraph, rerunRun, resubmitRun, retryTask } from "@/lib/dagron-api";
import { subscribeRun } from "@/lib/dagron-stream";
import { statusColor } from "@/lib/adapter";
import type { GraphResponse, RunDetail } from "@/types/dagron";

type Conn = "live" | "reconnecting" | "offline";

const TERMINAL = new Set(["succeeded", "failed", "cancelled"]);

export default function RunPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = use(params);
  const router = useRouter();
  const [run, setRun] = useState<RunDetail | null>(null);
  const [graph, setGraph] = useState<GraphResponse | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [conn, setConn] = useState<Conn>("offline");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

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
          // Auto-open the first task so logs/output are visible without a click,
          // but only once on initial load — don't reopen it after the user closes it.
          setSelected((s) => {
            if (s != null) return s;
            if (didAutoSelect.current) return null;
            didAutoSelect.current = true;
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

  // Cancel any queued refetch on unmount so it can't fire after navigation.
  useEffect(() => {
    return () => {
      if (timer.current) clearTimeout(timer.current);
    };
  }, []);

  // Live updates: refetch on any event for this run; resync also refetches.
  useEffect(() => {
    const unsub = subscribeRun(id, {
      onEvent: () => refetch(),
      onResync: () => refetch(),
      onStatus: setConn,
    });
    return unsub;
  }, [id, refetch]);

  const onCancel = async () => {
    if (!confirm("Cancel this run? Non-terminal tasks will be cancelled.")) return;
    setBusy(true);
    try {
      await cancelRun(id);
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
    // SSE will refetch; nudge in case events are delayed.
    refetch();
  };

  const onRetry = async (taskId: string) => {
    setBusy(true);
    try {
      await retryTask(id, taskId);
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
    refetch();
  };

  const onRerun = async () => {
    if (!confirm("Rerun from failure? Failed/cancelled tasks re-run; succeeded tasks are kept.")) {
      return;
    }
    setBusy(true);
    try {
      await rerunRun(id);
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
    // SSE will refetch; nudge in case events are delayed.
    refetch();
  };

  const onResubmit = async () => {
    setBusy(true);
    try {
      const { run_id } = await resubmitRun(id);
      router.push(`/runs/${run_id}`);
    } catch (e) {
      setError(String(e));
      setBusy(false);
    }
  };

  const runActive = run ? !TERMINAL.has(run.status) : false;
  // A failed/cancelled run can resume from its failure frontier.
  const runRerunnable = run ? run.status === "failed" || run.status === "cancelled" : false;
  const connColor = conn === "live" ? "#2ea043" : conn === "reconnecting" ? "#d29922" : "#6e7681";

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
        <strong style={{ fontSize: 15 }}>
          Run <span className="mono">{id.slice(0, 8)}</span>
        </strong>
        {run && (
          <span style={{ display: "inline-flex", alignItems: "center", gap: 7, color: statusColor(run.status) }}>
            <span className="dy-dot" style={{ background: statusColor(run.status) }} />
            {run.status}
          </span>
        )}
        <span style={{ fontSize: 12, color: connColor, display: "inline-flex", alignItems: "center", gap: 6 }}>
          <span className="dy-dot dy-dot-sm" style={{ background: connColor }} />
          {conn}
        </span>
        <div style={{ flex: 1 }} />
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
          <button onClick={onResubmit} disabled={busy} className="dy-btn" title="Start a fresh run from this workflow definition">
            ⟳ Re-run
          </button>
        )}
      </header>

      {error && <p style={{ color: "var(--red)", padding: "8px 24px" }}>{error}</p>}

      <div style={{ flex: 1, minHeight: 0, display: "flex" }}>
        <div style={{ flex: 1, minHeight: 0 }}>
          {graph && <DagGraph graph={graph} onNodeClick={setSelected} />}
        </div>
        <TaskPanel
          runId={id}
          taskId={selected}
          onClose={() => setSelected(null)}
          actions={(logs) =>
            logs.status === "failed" || logs.status === "cancelled" ? (
              <button
                onClick={() => onRetry(logs.task_id)}
                disabled={busy}
                className="dy-btn dy-btn-primary"
              >
                Retry task
              </button>
            ) : null
          }
        />
      </div>
    </div>
  );
}
