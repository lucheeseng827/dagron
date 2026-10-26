"use client";

import { useCallback, useEffect, useState } from "react";
import Link from "next/link";
import { listRunSources } from "@/lib/dagron-api";
import { absTime, timeAgo } from "@/lib/time";
import type { RunSource } from "@/types/dagron";

const GRID = "minmax(260px, 1fr) 90px 150px";

/// The repositories runs came from: each one a CI service has submitted a
/// run for, the one with the latest run first. A repository is listed for
/// what its runs' own records say, so one that has had no run yet is not
/// here, and nothing here is a repository's settings.
export default function ReposPage() {
  const [sources, setSources] = useState<RunSource[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(() => {
    listRunSources(500)
      .then((rows) => {
        setSources(rows);
        setError(null);
      })
      .catch((e) => setError(String(e)));
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  return (
    <div className="dy-page dy-page-wide">
      <div className="dy-pagehead">
        <div>
          <h1 className="dy-h1" style={{ marginBottom: 0 }}>
            Repositories
          </h1>
          <p className="dy-subtitle">Each repository CI has run for, the one with the latest run first.</p>
        </div>
        <button onClick={load} className="dy-btn">
          Refresh
        </button>
      </div>

      {error && <p style={{ color: "var(--red)" }}>{error}</p>}

      <div className="dy-card" style={{ padding: 0, overflow: "hidden" }}>
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
          <div>Repository</div>
          <div>Runs</div>
          <div>Latest run</div>
        </div>
        {sources?.map((s) => (
          <Link
            key={s.repo}
            href={`/repos/detail/?repo=${encodeURIComponent(s.repo)}`}
            className="dy-runrow"
            style={{ display: "grid", gridTemplateColumns: GRID, gap: 12 }}
          >
            <span className="mono" style={{ fontWeight: 600, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {s.repo}
            </span>
            <span className="mono" style={{ color: "#c9d1d9" }}>
              {s.runs}
            </span>
            <span style={{ color: "var(--muted)" }} title={absTime(s.last_run_at)}>
              {timeAgo(s.last_run_at)}
            </span>
          </Link>
        ))}
        {sources != null && sources.length === 0 && !error && (
          <p className="dy-empty" style={{ padding: 16 }}>
            No repository yet. One is listed once a CI service has submitted a run for it.
          </p>
        )}
      </div>
    </div>
  );
}
