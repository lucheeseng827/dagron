"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { listGitRepos, listRuns, listWorkflows } from "@/lib/dagron-api";
import { statusColor } from "@/lib/adapter";
import { errMsg } from "@/lib/err";
import { fromNow, timeAgo } from "@/lib/time";
import type { GitRepo, GitRepoState, RunSummary, TaskStatus, WorkflowRow } from "@/types/dagron";

const REPO_COLOR: Record<GitRepoState, string> = {
  Synced: "var(--green)",
  OutOfSync: "var(--amber)",
  Syncing: "var(--blue)",
};

function hhmm(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
}

export default function OverviewPage() {
  const [wfs, setWfs] = useState<WorkflowRow[]>([]);
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [repos, setRepos] = useState<GitRepo[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const load = () => {
      listWorkflows().then(setWfs).catch((e) => setError(errMsg(e)));
      listRuns({ limit: 200 }).then(setRuns).catch(() => {});
      listGitRepos().then(setRepos).catch(() => {});
    };
    load();
    const t = setInterval(load, 5000);
    return () => clearInterval(t);
  }, []);

  // KPIs
  const total = wfs.length;
  const active = wfs.filter((w) => w.has_schedule && !w.paused).length;

  const today = new Date().toISOString().slice(0, 10);
  const todays = runs.filter((r) => r.created_at.slice(0, 10) === today);
  const okToday = todays.filter((r) => r.status === "succeeded").length;
  const failToday = todays.filter((r) => r.status === "failed").length;

  const weekAgo = Date.now() - 7 * 86400_000;
  const week = runs.filter((r) => new Date(r.created_at).getTime() >= weekAgo);
  const weekDone = week.filter((r) => r.status === "succeeded" || r.status === "failed");
  const successRate = weekDone.length ? Math.round((week.filter((r) => r.status === "succeeded").length / weekDone.length) * 100) : 0;

  const synced = repos.filter((r) => r.state === "Synced").length;
  const outOfSync = repos.filter((r) => r.state === "OutOfSync").length;

  const nextRuns = wfs
    .filter((w) => w.has_schedule && !w.paused && w.next_fire_at)
    .sort((a, b) => new Date(a.next_fire_at!).getTime() - new Date(b.next_fire_at!).getTime())
    .slice(0, 6);
  const recent = runs.slice(0, 6);

  return (
    <div className="dy-page">
      <div className="dy-pagehead">
        <div>
          <h1 className="dy-h1" style={{ marginBottom: 0 }}>
            Overview
          </h1>
          <p className="dy-subtitle">Scheduler health, upcoming runs, and GitOps sync at a glance.</p>
        </div>
        <Link href="/workflows/new" className="dy-btn dy-btn-primary">
          + New workflow
        </Link>
      </div>
      {error && <p style={{ color: "var(--red)" }}>{error}</p>}

      {/* KPI row */}
      <div className="dy-kpis">
        <div className="dy-kpi">
          <div className="dy-kpi-label">Active workflows</div>
          <div className="dy-kpi-value">
            {active}
            <span style={{ fontSize: 14, color: "var(--dim)", fontWeight: 500 }}> / {total}</span>
          </div>
          <div className="dy-kpi-sub" style={{ color: "var(--green)" }}>{active} with active schedule</div>
        </div>
        <div className="dy-kpi">
          <div className="dy-kpi-label">Runs today</div>
          <div className="dy-kpi-value">{todays.length}</div>
          <div className="dy-kpi-sub">
            <span style={{ color: "var(--green)" }}>{okToday} ok</span> ·{" "}
            <span style={{ color: failToday ? "var(--red)" : "var(--muted)" }}>{failToday} failed</span>
          </div>
        </div>
        <div className="dy-kpi">
          <div className="dy-kpi-label">Success rate · 7d</div>
          <div className="dy-kpi-value">{successRate}%</div>
          <div className="dy-bar">
            <div className="dy-bar-fill" style={{ width: `${successRate}%` }} />
          </div>
        </div>
        <div className="dy-kpi">
          <div className="dy-kpi-label">GitOps sync</div>
          <div className="dy-kpi-value" style={{ color: outOfSync ? "var(--amber)" : undefined }}>
            {outOfSync ? `${outOfSync} drift` : "in sync"}
          </div>
          <div className="dy-kpi-sub">{synced} synced · {outOfSync} out-of-sync</div>
        </div>
      </div>

      {/* two columns */}
      <div style={{ display: "grid", gridTemplateColumns: "1.55fr 1fr", gap: 18, alignItems: "start" }}>
        {/* Next scheduled runs */}
        <div className="dy-card">
          <div className="dy-cardhead">
            <strong>Next scheduled runs</strong>
            <span style={{ fontSize: 12, color: "var(--dim)" }}>
              {nextRuns[0]?.next_fire_at ? `next ${fromNow(nextRuns[0].next_fire_at).replace("in ", "")}` : ""}
            </span>
          </div>
          {nextRuns.map((w) => (
            <Link key={w.id} href={`/workflows/${w.id}`} className="dy-row" style={{ gap: 14 }}>
              <span className="mono" style={{ width: 56, color: "var(--muted)", flexShrink: 0 }}>
                {w.next_fire_at ? hhmm(w.next_fire_at) : "—"}
              </span>
              <span style={{ minWidth: 0 }}>
                <div style={{ fontWeight: 600 }}>{w.name}</div>
                <div className="mono" style={{ fontSize: 12, color: "var(--dim)" }}>{w.cron_expr}</div>
              </span>
              <span style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 12 }}>
                <SourceBadge source={w.source} />
                <span style={{ fontSize: 12.5, color: "var(--muted)", width: 56, textAlign: "right" }}>
                  {w.next_fire_at ? fromNow(w.next_fire_at).replace("in ", "") : ""}
                </span>
              </span>
            </Link>
          ))}
          {nextRuns.length === 0 && <p className="dy-empty">No scheduled runs.</p>}
        </div>

        {/* right column */}
        <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
          <div className="dy-card">
            <div className="dy-cardhead">
              <strong>Recent runs</strong>
              <Link href="/runs">View all →</Link>
            </div>
            {recent.map((r) => (
              <Link key={r.id} href={`/runs/${r.id}`} className="dy-row">
                <span className="dy-dot" style={{ background: statusColor(r.status as TaskStatus) }} />
                <span style={{ minWidth: 0, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                  {r.name ?? r.id.slice(0, 8)}
                </span>
                <span style={{ marginLeft: "auto", fontSize: 12.5, color: "var(--muted)" }}>
                  {r.status === "running" ? "now" : timeAgo(r.created_at).replace(" ago", "")}
                </span>
              </Link>
            ))}
            {recent.length === 0 && <p className="dy-empty">No runs yet.</p>}
          </div>

          <div className="dy-card">
            <div className="dy-cardhead">
              <strong>GitOps repos</strong>
              <Link href="/gitops">View all →</Link>
            </div>
            {repos.map((r) => (
              <Link key={r.id} href="/gitops" className="dy-row">
                <span className="dy-dot" style={{ background: REPO_COLOR[r.state] }} />
                <span className="mono" style={{ fontSize: 13 }}>{r.name}</span>
                <span style={{ marginLeft: "auto", fontSize: 12.5, color: REPO_COLOR[r.state] }}>{r.state}</span>
              </Link>
            ))}
            {repos.length === 0 && <p className="dy-empty">No repos connected.</p>}
          </div>
        </div>
      </div>
    </div>
  );
}

function SourceBadge({ source }: { source: "git" | "manual" }) {
  const git = source === "git";
  return (
    <span
      style={{
        fontSize: 9.5,
        fontWeight: 600,
        padding: "2px 7px",
        borderRadius: 999,
        color: git ? "var(--accent)" : "var(--blue)",
        background: git ? "rgba(232,131,58,0.13)" : "rgba(47,129,247,0.13)",
      }}
    >
      {git ? "GitOps" : "Manual"}
    </span>
  );
}
