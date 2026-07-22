"use client";

import { useCallback, useEffect, useState } from "react";
import Link from "next/link";
import LiveToggle from "@/components/LiveToggle";
import TriggerBadge from "@/components/TriggerBadge";
import { listArchivedRuns, listRuns, listWorkflows } from "@/lib/dagron-api";
import { statusColor } from "@/lib/adapter";
import { errMsg } from "@/lib/err";
import { useLiveRefresh, useLiveUpdates } from "@/lib/live";
import { absTime, timeAgo, duration } from "@/lib/time";
import type { ArchivedRunSummary, RunSummary, TaskStatus } from "@/types/dagron";

// Comp column layout: ● | Workflow | Run | Started | Duration | Trigger
const GRID = "24px 1.4fr 1.2fr 1fr 1fr 1fr";
const PAGE_SIZE = 50;

const STATUS_FILTERS = ["all", "running", "succeeded", "failed", "cancelled"] as const;
type StatusFilter = (typeof STATUS_FILTERS)[number];
type Tab = "live" | "archive";

export default function RunsPage() {
  const [tab, setTab] = useState<Tab>("live");
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [archived, setArchived] = useState<ArchivedRunSummary[]>([]);
  const [names, setNames] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [liveUpdates] = useLiveUpdates();

  // Filters + pagination. `status` seeds from ?status= so overview KPI links
  // ("1 failed today") land pre-filtered.
  const [status, setStatus] = useState<StatusFilter>("all");
  const [name, setName] = useState("");
  const [trigger, setTrigger] = useState("");
  const [page, setPage] = useState(0);
  // One extra row is requested per page purely to detect "has next page".
  const [hasMore, setHasMore] = useState(false);

  useEffect(() => {
    const s = new URLSearchParams(window.location.search).get("status");
    if (s && (STATUS_FILTERS as readonly string[]).includes(s)) setStatus(s as StatusFilter);
    listWorkflows()
      .then((ws) => setNames(ws.map((w) => w.name).sort()))
      .catch(() => {});
  }, []);

  const load = useCallback(() => {
    if (tab === "live") {
      listRuns({
        status: status === "all" ? undefined : status,
        name: name || undefined,
        trigger: trigger || undefined,
        limit: PAGE_SIZE + 1,
        offset: page * PAGE_SIZE,
      })
        .then((rs) => {
          setHasMore(rs.length > PAGE_SIZE);
          setRuns(rs.slice(0, PAGE_SIZE));
          setError(null);
        })
        .catch((e) => setError(errMsg(e)));
    } else {
      listArchivedRuns({ name: name || undefined, limit: PAGE_SIZE + 1, offset: page * PAGE_SIZE })
        .then((rs) => {
          setHasMore(rs.length > PAGE_SIZE);
          setArchived(rs.slice(0, PAGE_SIZE));
          setError(null);
        })
        .catch((e) => setError(errMsg(e)));
    }
  }, [tab, status, name, trigger, page]);
  useEffect(() => load(), [load]);
  // Live mode: refetch on activity from the account-wide event stream. Task
  // events only move the live list — the archive tab holds no stream open.
  const conn = useLiveRefresh(liveUpdates && tab === "live", load);

  // Any filter change resets to the first page.
  const setFilter = (fn: () => void) => {
    fn();
    setPage(0);
  };

  const pager = (
    <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "12px 18px" }}>
      <button className="dy-btn" disabled={page === 0} onClick={() => setPage((p) => Math.max(0, p - 1))}>
        ← Prev
      </button>
      <span style={{ fontSize: 12.5, color: "var(--muted)" }}>Page {page + 1}</span>
      <button className="dy-btn" disabled={!hasMore} onClick={() => setPage((p) => p + 1)}>
        Next →
      </button>
    </div>
  );

  return (
    <div className="dy-page">
      <div className="dy-pagehead">
        <div>
          <h1 className="dy-h1" style={{ marginBottom: 0 }}>
            Runs
          </h1>
          <p className="dy-subtitle">Every execution across all workflows, newest first.</p>
        </div>
        <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
          {tab === "live" && <LiveToggle status={conn} onRefresh={load} />}
          <div style={{ display: "flex", gap: 3, background: "var(--panel)", border: "1px solid var(--border)", borderRadius: 8, padding: 3 }}>
            {(["live", "archive"] as const).map((t) => (
              <button
                key={t}
                onClick={() => setFilter(() => setTab(t))}
                className={`dy-pill ${tab === t ? "dy-pill-active" : ""}`}
                style={{ cursor: "pointer", textTransform: "capitalize" }}
              >
                {t}
              </button>
            ))}
          </div>
          <Link href="/submit" className="dy-btn dy-btn-primary">
            + Submit
          </Link>
        </div>
      </div>

      {/* filter row */}
      <div style={{ display: "flex", gap: 10, alignItems: "center", marginBottom: 16, flexWrap: "wrap" }}>
        {tab === "live" && (
          <div style={{ display: "flex", gap: 4 }}>
            {STATUS_FILTERS.map((f) => (
              <button
                key={f}
                onClick={() => setFilter(() => setStatus(f))}
                className={`dy-pill ${status === f ? "dy-pill-active" : ""}`}
                style={{ cursor: "pointer", textTransform: "capitalize" }}
              >
                {f}
              </button>
            ))}
          </div>
        )}
        <select
          value={name}
          onChange={(e) => setFilter(() => setName(e.target.value))}
          className="dy-btn"
          style={{ cursor: "pointer" }}
        >
          <option value="">All workflows</option>
          {names.map((n) => (
            <option key={n} value={n}>
              {n}
            </option>
          ))}
        </select>
        {tab === "live" && (
          <select
            value={trigger}
            onChange={(e) => setFilter(() => setTrigger(e.target.value))}
            className="dy-btn"
            style={{ cursor: "pointer" }}
          >
            <option value="">All triggers</option>
            <option value="manual">Manual</option>
            <option value="schedule">Schedule</option>
            <option value="backfill">Backfill</option>
          </select>
        )}
      </div>

      {error && <p style={{ color: "var(--red)" }}>{error}</p>}

      <div className="dy-card" style={{ padding: 0, overflow: "hidden" }}>
        {/* header row */}
        <div
          style={{
            display: "grid",
            gridTemplateColumns: GRID,
            gap: 12,
            padding: "11px 18px",
            borderBottom: "1px solid var(--border)",
            fontSize: 11,
            fontWeight: 600,
            color: "var(--dim)",
            textTransform: "uppercase",
            letterSpacing: "0.05em",
          }}
        >
          <div />
          <div>Workflow</div>
          <div>Run</div>
          <div>{tab === "live" ? "Started" : "Archived"}</div>
          <div>Duration</div>
          <div>{tab === "live" ? "Trigger" : "Tier"}</div>
        </div>

        {tab === "live" &&
          runs.map((r) => {
            const color = statusColor(r.status as TaskStatus);
            return (
              <Link
                key={r.id}
                href={`/runs/${r.id}`}
                className="dy-runrow"
                style={{ display: "grid", gridTemplateColumns: GRID, gap: 12 }}
              >
                <span className="dy-dot" style={{ width: 9, height: 9, background: color }} title={r.status} />
                <span style={{ fontWeight: 600, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                  {r.name ?? "—"}
                </span>
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
            );
          })}
        {tab === "live" && runs.length === 0 && !error && (
          <p className="dy-empty" style={{ padding: 16 }}>
            {page > 0 || status !== "all" || name || trigger ? "No runs match." : "No runs yet."}
          </p>
        )}

        {tab === "archive" &&
          archived.map((r) => {
            const color = statusColor(r.status as TaskStatus);
            const compacted = r.compacted_at != null;
            const row = (
              <>
                <span className="dy-dot" style={{ width: 9, height: 9, background: color }} title={r.status} />
                <span style={{ fontWeight: 600, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                  {r.name}
                </span>
                <span className="mono" style={{ color: compacted ? "var(--dim)" : "var(--blue)" }}>
                  {r.run_id.slice(0, 8)}
                </span>
                <span style={{ color: "var(--muted)" }} title={absTime(r.archived_at)}>
                  {timeAgo(r.archived_at)}
                </span>
                <span className="mono" style={{ color: "#c9d1d9" }}>
                  {r.created_at && r.finished_at ? duration(r.created_at, r.finished_at) : "—"}
                </span>
                <span style={{ color: "var(--muted)", fontSize: 12 }} title={r.parquet_path ?? undefined}>
                  {compacted ? "parquet (analytics)" : "archive"}
                </span>
              </>
            );
            // A compacted run has no per-run document to open.
            return compacted ? (
              <div key={r.run_id} className="dy-runrow" style={{ display: "grid", gridTemplateColumns: GRID, gap: 12, opacity: 0.6, cursor: "default" }}>
                {row}
              </div>
            ) : (
              <Link key={r.run_id} href={`/runs/archive/${r.run_id}`} className="dy-runrow" style={{ display: "grid", gridTemplateColumns: GRID, gap: 12 }}>
                {row}
              </Link>
            );
          })}
        {tab === "archive" && archived.length === 0 && !error && (
          <p className="dy-empty" style={{ padding: 16 }}>
            No archived runs{name ? " match" : ""}. Runs land here after the engine's archive GC moves them out of the hot store.
          </p>
        )}

        {pager}
      </div>
    </div>
  );
}
