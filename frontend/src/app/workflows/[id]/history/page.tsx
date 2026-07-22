"use client";

// Read-oriented workflow detail: run history, duration trend, and a run × task
// status grid (spot a task that's been flaky for a week at a glance) — the
// editor stays at /workflows/[id].

import { use, useCallback, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import TriggerBadge from "@/components/TriggerBadge";
import { useToast } from "@/components/Toasts";
import { getRunGraph, getWorkflow, listWorkflowRuns, runWorkflow } from "@/lib/dagron-api";
import { statusColor, statusLabel } from "@/lib/adapter";
import { errMsg } from "@/lib/err";
import { absTime, timeAgo, duration } from "@/lib/time";
import type { GraphResponse, RunSummary, TaskStatus, Workflow } from "@/types/dagron";

const GRID_RUNS = 10; // columns in the run × task grid
const PAGE_SIZE = 25;

function runSecs(r: RunSummary): number | null {
  if (!r.finished_at) return null;
  const s = new Date(r.created_at).getTime();
  const e = new Date(r.finished_at).getTime();
  if (Number.isNaN(s) || Number.isNaN(e) || e < s) return null;
  return (e - s) / 1000;
}

export default function WorkflowHistoryPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = use(params);
  const router = useRouter();
  const toast = useToast();
  const [wf, setWf] = useState<Workflow | null>(null);
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [graphs, setGraphs] = useState<Map<string, GraphResponse>>(new Map());
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [page, setPage] = useState(0);
  const [hasMore, setHasMore] = useState(false);

  useEffect(() => {
    getWorkflow(id)
      .then(setWf)
      .catch((e) => setError(errMsg(e)));
  }, [id]);

  const load = useCallback(() => {
    listWorkflowRuns(id, PAGE_SIZE + 1, page * PAGE_SIZE)
      .then((rs) => {
        setHasMore(rs.length > PAGE_SIZE);
        setRuns(rs.slice(0, PAGE_SIZE));
        setError(null);
      })
      .catch((e) => setError(errMsg(e)));
  }, [id, page]);
  useEffect(() => load(), [load]);

  // Run × task grid: fetch task graphs for the newest N runs of page one.
  const gridRuns = useMemo(() => (page === 0 ? runs.slice(0, GRID_RUNS) : []), [runs, page]);
  useEffect(() => {
    let alive = true;
    Promise.all(
      gridRuns.map((r) =>
        getRunGraph(r.id)
          .then((g) => [r.id, g] as const)
          .catch(() => null),
      ),
    ).then((pairs) => {
      if (!alive) return;
      setGraphs(new Map(pairs.filter((p): p is readonly [string, GraphResponse] => p != null)));
    });
    return () => {
      alive = false;
    };
  }, [gridRuns]);

  // Union of task names across the grid runs, in first-seen (roughly topo) order.
  const taskNames = useMemo(() => {
    const seen: string[] = [];
    for (const r of gridRuns) {
      for (const n of graphs.get(r.id)?.nodes ?? []) {
        if (!seen.includes(n.name)) seen.push(n.name);
      }
    }
    return seen;
  }, [gridRuns, graphs]);

  // KPIs over the loaded page.
  const finished = runs.filter((r) => r.status === "succeeded" || r.status === "failed");
  const successRate = finished.length
    ? Math.round((runs.filter((r) => r.status === "succeeded").length / finished.length) * 100)
    : null;
  const durations = runs.map(runSecs).filter((v): v is number => v != null);
  const avgSecs = durations.length ? durations.reduce((a, b) => a + b, 0) / durations.length : null;
  const maxSecs = durations.length ? Math.max(...durations) : null;

  // Duration trend: chronological (oldest → newest) bars for this page's runs.
  const trend = useMemo(() => [...runs].reverse(), [runs]);

  const onRun = async () => {
    setBusy(true);
    try {
      const { run_id } = await runWorkflow(id);
      toast("Run started");
      router.push(`/runs/${run_id}`);
    } catch (e) {
      toast(errMsg(e), "error");
      setBusy(false);
    }
  };

  return (
    <div className="dy-page" style={{ maxWidth: 1320 }}>
      <div className="dy-pagehead">
        <div>
          <h1 className="dy-h1" style={{ marginBottom: 0 }}>
            {wf?.name ?? "Workflow"}
          </h1>
          <p className="dy-subtitle">{wf?.description || "Run history and per-task health."}</p>
        </div>
        <div style={{ display: "flex", gap: 10 }}>
          <button onClick={onRun} disabled={busy} className="dy-btn dy-btn-primary">
            ▶ Run now
          </button>
          <Link href={`/workflows/${id}`} className="dy-btn">
            ✎ Edit
          </Link>
        </div>
      </div>
      {error && <p style={{ color: "var(--red)" }}>{error}</p>}

      {/* KPI row */}
      <div className="dy-kpis">
        <div className="dy-kpi">
          <div className="dy-kpi-label">Runs (this page)</div>
          <div className="dy-kpi-value">{runs.length}</div>
        </div>
        <div className="dy-kpi">
          <div className="dy-kpi-label">Success rate</div>
          <div className="dy-kpi-value">{successRate == null ? "—" : `${successRate}%`}</div>
          {successRate != null && (
            <div className="dy-bar">
              <div className="dy-bar-fill" style={{ width: `${successRate}%` }} />
            </div>
          )}
        </div>
        <div className="dy-kpi">
          <div className="dy-kpi-label">Avg duration</div>
          <div className="dy-kpi-value">{avgSecs == null ? "—" : humanSecs(avgSecs)}</div>
          <div className="dy-kpi-sub">{maxSecs != null ? `max ${humanSecs(maxSecs)}` : ""}</div>
        </div>
        <div className="dy-kpi">
          <div className="dy-kpi-label">Last run</div>
          <div className="dy-kpi-value" style={{ fontSize: 20, color: runs[0] ? statusColor(runs[0].status as TaskStatus) : undefined }}>
            {runs[0] ? runs[0].status : "never"}
          </div>
          <div className="dy-kpi-sub">{runs[0] ? timeAgo(runs[0].created_at) : ""}</div>
        </div>
      </div>

      {/* duration trend */}
      {trend.length > 1 && (
        <div className="dy-card" style={{ marginBottom: 18 }}>
          <div className="dy-cardhead">
            <strong>Duration trend</strong>
            <span style={{ fontSize: 12, color: "var(--dim)" }}>oldest → newest · bar height = wall-clock · color = outcome</span>
          </div>
          <div style={{ display: "flex", alignItems: "flex-end", gap: 3, height: 90 }}>
            {trend.map((r) => {
              const secs = runSecs(r);
              const h = secs != null && maxSecs ? Math.max((secs / maxSecs) * 100, 4) : 4;
              return (
                <Link
                  key={r.id}
                  href={`/runs/${r.id}`}
                  title={`${r.id.slice(0, 8)} — ${r.status}${secs != null ? ` · ${humanSecs(secs)}` : ""} · ${absTime(r.created_at)}`}
                  style={{
                    flex: 1,
                    maxWidth: 26,
                    height: `${h}%`,
                    background: statusColor(r.status as TaskStatus),
                    borderRadius: "3px 3px 0 0",
                    opacity: r.finished_at ? 0.95 : 0.55,
                  }}
                />
              );
            })}
          </div>
        </div>
      )}

      {/* run × task grid */}
      {page === 0 && taskNames.length > 0 && (
        <div className="dy-card" style={{ marginBottom: 18, overflowX: "auto" }}>
          <div className="dy-cardhead">
            <strong>Task health · last {gridRuns.length} runs</strong>
            <span style={{ fontSize: 12, color: "var(--dim)" }}>rows = tasks · columns = runs, newest first</span>
          </div>
          <table style={{ borderCollapse: "collapse" }}>
            <thead>
              <tr>
                <th style={{ textAlign: "left", fontSize: 11, color: "var(--dim)", fontWeight: 600, padding: "4px 12px 4px 0" }}>Task</th>
                {gridRuns.map((r) => (
                  <th key={r.id} style={{ padding: "4px 3px" }} title={`${r.id.slice(0, 8)} · ${absTime(r.created_at)}`}>
                    <Link href={`/runs/${r.id}`} className="mono" style={{ fontSize: 9.5, color: "var(--dim)" }}>
                      {r.id.slice(0, 4)}
                    </Link>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {taskNames.map((name) => (
                <tr key={name}>
                  <td style={{ fontSize: 12.5, padding: "3px 12px 3px 0", whiteSpace: "nowrap", maxWidth: 260, overflow: "hidden", textOverflow: "ellipsis" }}>
                    {name}
                  </td>
                  {gridRuns.map((r) => {
                    const node = graphs.get(r.id)?.nodes.find((n) => n.name === name);
                    return (
                      <td key={r.id} style={{ padding: 3, textAlign: "center" }}>
                        {node ? (
                          <Link
                            href={`/runs/${r.id}?task=${encodeURIComponent(name)}`}
                            title={`${name} — ${statusLabel(node.status)}${node.attempt > 1 ? ` · try ${node.attempt}` : ""}`}
                            style={{ display: "inline-block", width: 16, height: 16, borderRadius: 4, background: statusColor(node.status) }}
                          />
                        ) : (
                          <span style={{ display: "inline-block", width: 16, height: 16, borderRadius: 4, background: "rgba(255,255,255,0.04)" }} title="not in this run" />
                        )}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* runs table */}
      <div className="dy-card" style={{ padding: 0, overflow: "hidden" }}>
        <div style={{ display: "grid", gridTemplateColumns: "24px 1.2fr 1fr 1fr 1fr", gap: 12, padding: "11px 18px", borderBottom: "1px solid var(--border)", fontSize: 11, fontWeight: 600, color: "var(--dim)", textTransform: "uppercase", letterSpacing: "0.05em" }}>
          <div />
          <div>Run</div>
          <div>Started</div>
          <div>Duration</div>
          <div>Trigger</div>
        </div>
        {runs.map((r) => (
          <Link key={r.id} href={`/runs/${r.id}`} className="dy-runrow" style={{ display: "grid", gridTemplateColumns: "24px 1.2fr 1fr 1fr 1fr", gap: 12 }}>
            <span className="dy-dot" style={{ width: 9, height: 9, background: statusColor(r.status as TaskStatus) }} title={r.status} />
            <span className="mono" style={{ color: "var(--blue)" }}>
              {r.id.slice(0, 8)}
            </span>
            <span style={{ color: "var(--muted)" }} title={absTime(r.created_at)}>
              {timeAgo(r.created_at)}
            </span>
            <span className="mono" style={{ color: "#c9d1d9" }}>
              {duration(r.created_at, r.finished_at)}
            </span>
            <span>
              <TriggerBadge kind={r.trigger_kind} />
            </span>
          </Link>
        ))}
        {runs.length === 0 && !error && <p className="dy-empty" style={{ padding: 16 }}>No runs yet.</p>}
        <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "12px 18px" }}>
          <button className="dy-btn" disabled={page === 0} onClick={() => setPage((p) => Math.max(0, p - 1))}>
            ← Prev
          </button>
          <span style={{ fontSize: 12.5, color: "var(--muted)" }}>Page {page + 1}</span>
          <button className="dy-btn" disabled={!hasMore} onClick={() => setPage((p) => p + 1)}>
            Next →
          </button>
        </div>
      </div>
    </div>
  );
}

function humanSecs(secs: number): string {
  if (secs < 60) return `${Math.round(secs)}s`;
  const m = Math.floor(secs / 60);
  if (m < 60) return `${m}m ${Math.round(secs % 60)}s`;
  return `${Math.floor(m / 60)}h ${m % 60}m`;
}
