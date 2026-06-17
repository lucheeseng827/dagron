"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { listRuns } from "@/lib/dagron-api";
import { statusColor } from "@/lib/adapter";
import { errMsg } from "@/lib/err";
import { timeAgo, duration } from "@/lib/time";
import type { RunSummary, TaskStatus } from "@/types/dagron";

// Comp column layout: ● | Workflow | Run | Started | Duration | Trigger
const GRID = "24px 1.4fr 1.2fr 1fr 1fr 1fr";

export default function RunsPage() {
  const [runs, setRuns] = useState<RunSummary[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    listRuns({ limit: 100 })
      .then(setRuns)
      .catch((e) => setError(errMsg(e)));
  }, []);

  return (
    <div className="dy-page">
      <div className="dy-pagehead">
        <div>
          <h1 className="dy-h1" style={{ marginBottom: 0 }}>
            Runs
          </h1>
          <p className="dy-subtitle">Every execution across all workflows, newest first.</p>
        </div>
        <Link href="/submit" className="dy-btn dy-btn-primary">
          + Submit
        </Link>
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
          <div>Started</div>
          <div>Duration</div>
          <div>Trigger</div>
        </div>

        {runs.map((r) => {
          const color = statusColor(r.status as TaskStatus);
          return (
            <Link
              key={r.id}
              href={`/runs/${r.id}`}
              className="dy-runrow"
              style={{ display: "grid", gridTemplateColumns: GRID, gap: 12 }}
            >
              <span className="dy-dot" style={{ width: 9, height: 9, background: color }} />
              <span style={{ fontWeight: 600, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                {r.name ?? "—"}
              </span>
              <span className="mono" style={{ color: "var(--blue)" }}>
                {r.id.slice(0, 8)}
              </span>
              <span style={{ color: "var(--muted)" }} title={r.created_at}>
                {timeAgo(r.created_at)}
              </span>
              <span className="mono" style={{ color: "#c9d1d9" }}>
                {duration(r.created_at, r.finished_at)}
              </span>
              <span style={{ color: "var(--muted)" }}>—</span>
            </Link>
          );
        })}
        {runs.length === 0 && !error && <p className="dy-empty" style={{ padding: 16 }}>No runs yet.</p>}
      </div>
    </div>
  );
}
